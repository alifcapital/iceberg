/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.actions;

import java.math.RoundingMode;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.BinPackRewriteFilePlanner;
import org.apache.iceberg.actions.FileRewritePlan;
import org.apache.iceberg.actions.FileRewriteRunner;
import org.apache.iceberg.actions.ImmutableRewriteDataFiles;
import org.apache.iceberg.actions.ImmutableRewriteDataFiles.Result.Builder;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.actions.RewriteDataFilesCommitManager;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Queues;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.math.IntMath;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RewriteDataFilesSparkAction
    extends BaseSnapshotUpdateSparkAction<RewriteDataFilesSparkAction> implements RewriteDataFiles {

  private static final Logger LOG = LoggerFactory.getLogger(RewriteDataFilesSparkAction.class);
  private static final Set<String> VALID_OPTIONS =
      ImmutableSet.of(
          MAX_CONCURRENT_FILE_GROUP_REWRITES,
          MAX_FILE_GROUP_SIZE_BYTES,
          MAX_FILE_GROUP_SIZE_NUM,
          MAX_FILE_GROUP_COUNT,
          PARTIAL_PROGRESS_ENABLED,
          PARTIAL_PROGRESS_MAX_COMMITS,
          TARGET_FILE_SIZE_BYTES,
          USE_STARTING_SEQUENCE_NUMBER,
          REWRITE_JOB_ORDER,
          OUTPUT_SPEC_ID,
          REMOVE_DANGLING_DELETES,
          BinPackRewriteFilePlanner.MAX_FILES_TO_REWRITE);

  private static final RewriteDataFilesSparkAction.Result EMPTY_RESULT =
      ImmutableRewriteDataFiles.Result.builder().rewriteResults(ImmutableList.of()).build();

  private final Table table;

  private Expression filter = Expressions.alwaysTrue();
  private int maxConcurrentFileGroupRewrites;
  private int maxCommits;
  private int maxFileGroupCount;
  private boolean partialProgressEnabled;
  private boolean removeDanglingDeletes;
  private boolean useStartingSequenceNumber;
  private boolean caseSensitive;
  private BinPackRewriteFilePlanner planner = null;
  private FileRewriteRunner<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> runner = null;

  RewriteDataFilesSparkAction(SparkSession spark, Table table) {
    super(((org.apache.spark.sql.classic.SparkSession) spark).cloneSession());
    // Disable Adaptive Query Execution as this may change the output partitioning of our write
    spark().conf().set(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), false);
    // Disable executor cache for delete files as each partition is rewritten separately.
    // Note: when compacting to a different target spec, data from multiple partitions
    // may be grouped together, but caching is still disabled to avoid connection pool issues.
    spark().conf().set(SparkSQLProperties.EXECUTOR_CACHE_DELETE_FILES_ENABLED, "false");
    this.caseSensitive = SparkUtil.caseSensitive(spark);
    this.table = table;
  }

  @Override
  protected RewriteDataFilesSparkAction self() {
    return this;
  }

  @Override
  public RewriteDataFilesSparkAction binPack() {
    ensureRunnerNotSet();
    this.runner = new SparkBinPackFileRewriteRunner(spark(), table);
    return this;
  }

  @Override
  public RewriteDataFilesSparkAction sort(SortOrder sortOrder) {
    ensureRunnerNotSet();
    this.runner = new SparkSortFileRewriteRunner(spark(), table, sortOrder);
    return this;
  }

  @Override
  public RewriteDataFilesSparkAction sort() {
    ensureRunnerNotSet();
    this.runner = new SparkSortFileRewriteRunner(spark(), table);
    return this;
  }

  @Override
  public RewriteDataFilesSparkAction zOrder(String... columnNames) {
    ensureRunnerNotSet();
    this.runner = new SparkZOrderFileRewriteRunner(spark(), table, Arrays.asList(columnNames));
    return this;
  }

  private void ensureRunnerNotSet() {
    Preconditions.checkArgument(
        runner == null,
        "Cannot set rewrite mode, it has already been set to %s",
        runner == null ? null : runner.description());
  }

  @Override
  public RewriteDataFilesSparkAction filter(Expression expression) {
    filter = Expressions.and(filter, expression);
    return this;
  }

  @Override
  public RewriteDataFiles.Result execute() {
    if (table.currentSnapshot() == null) {
      return EMPTY_RESULT;
    }

    long startingSnapshotId = table.currentSnapshot().snapshotId();

    init(startingSnapshotId);

    FileRewritePlan<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> plan = planner.plan();

    if (plan.totalGroupCount() == 0) {
      LOG.info("Nothing found to rewrite in {}", table.name());
      return EMPTY_RESULT;
    }

    if (maxFileGroupCount < plan.totalGroupCount()) {
      LOG.info(
          "Limiting rewrite to {} of {} file groups in {} due to {}",
          maxFileGroupCount,
          plan.totalGroupCount(),
          table.name(),
          MAX_FILE_GROUP_COUNT);
    }

    Builder resultBuilder =
        partialProgressEnabled
            ? doExecuteWithPartialProgress(plan, commitManager(startingSnapshotId))
            : doExecute(plan, commitManager(startingSnapshotId));
    ImmutableRewriteDataFiles.Result result = resultBuilder.build();

    if (removeDanglingDeletes) {
      RemoveDanglingDeletesSparkAction action =
          new RemoveDanglingDeletesSparkAction(spark(), table);
      int removedDeleteFiles = Iterables.size(action.execute().removedDeleteFiles());
      return result.withRemovedDeleteFilesCount(
          result.removedDeleteFilesCount() + removedDeleteFiles);
    }

    return result;
  }

  private void init(long startingSnapshotId) {
    this.planner =
        runner instanceof SparkShufflingFileRewriteRunner
            ? new SparkShufflingDataRewritePlanner(table, filter, startingSnapshotId, caseSensitive)
            : new BinPackRewriteFilePlanner(table, filter, startingSnapshotId, caseSensitive);

    // Default to BinPack if no strategy selected
    if (this.runner == null) {
      this.runner = new SparkBinPackFileRewriteRunner(spark(), table);
    }

    validateAndInitOptions();
  }

  @VisibleForTesting
  RewriteFileGroup rewriteFiles(
      FileRewritePlan<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> plan,
      RewriteFileGroup fileGroup) {
    String desc = jobDesc(fileGroup, plan);
    Set<DataFile> addedFiles =
        withJobGroupInfo(
            newJobGroupInfo("REWRITE-DATA-FILES", desc), () -> runner.rewrite(fileGroup));

    fileGroup.setOutputFiles(addedFiles);
    LOG.info("Rewrite Files Ready to be Committed - {}", desc);
    return fileGroup;
  }

  private ExecutorService rewriteService() {
    return MoreExecutors.getExitingExecutorService(
        (ThreadPoolExecutor)
            Executors.newFixedThreadPool(
                maxConcurrentFileGroupRewrites,
                new ThreadFactoryBuilder().setNameFormat("Rewrite-Service-%d").build()));
  }

  @VisibleForTesting
  RewriteDataFilesCommitManager commitManager(long startingSnapshotId) {
    return new RewriteDataFilesCommitManager(
        table, startingSnapshotId, useStartingSequenceNumber, commitSummary());
  }

  private Builder doExecute(
      FileRewritePlan<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> plan,
      RewriteDataFilesCommitManager commitManager) {
    ExecutorService rewriteService = rewriteService();

    ConcurrentLinkedQueue<RewriteFileGroup> rewrittenGroups = Queues.newConcurrentLinkedQueue();

    Iterable<RewriteFileGroup> groupsToRewrite = Iterables.limit(plan.groups(), maxFileGroupCount);
    Tasks.Builder<RewriteFileGroup> rewriteTaskBuilder =
        Tasks.foreach(groupsToRewrite)
            .executeWith(rewriteService)
            .stopOnFailure()
            .noRetry()
            .onFailure(
                (fileGroup, exception) -> {
                  LOG.warn(
                      "Failure during rewrite process for group {}", fileGroup.info(), exception);
                });

    try {
      rewriteTaskBuilder.run(
          fileGroup -> {
            rewrittenGroups.add(rewriteFiles(plan, fileGroup));
          });
    } catch (Exception e) {
      // At least one rewrite group failed, clean up all completed rewrites
      LOG.error(
          "Cannot complete rewrite, {} is not enabled and one of the file set groups failed to "
              + "be rewritten. This error occurred during the writing of new files, not during the commit process. This "
              + "indicates something is wrong that doesn't involve conflicts with other Iceberg operations. Enabling "
              + "{} may help in this case but the root cause should be investigated. Cleaning up {} groups which finished "
              + "being written.",
          PARTIAL_PROGRESS_ENABLED,
          PARTIAL_PROGRESS_ENABLED,
          rewrittenGroups.size(),
          e);

      Tasks.foreach(rewrittenGroups)
          .suppressFailureWhenFinished()
          .run(commitManager::abortFileGroup);
      throw e;
    } finally {
      rewriteService.shutdown();
    }

    try {
      commitManager.commitOrClean(Sets.newHashSet(rewrittenGroups));
    } catch (ValidationException | CommitFailedException e) {
      String errorMessage =
          String.format(
              "Cannot commit rewrite because of a ValidationException or CommitFailedException. This usually means that "
                  + "this rewrite has conflicted with another concurrent Iceberg operation. To reduce the likelihood of "
                  + "conflicts, set %s which will break up the rewrite into multiple smaller commits controlled by %s. "
                  + "Separate smaller rewrite commits can succeed independently while any commits that conflict with "
                  + "another Iceberg operation will be ignored. This mode will create additional snapshots in the table "
                  + "history, one for each commit.",
              PARTIAL_PROGRESS_ENABLED, PARTIAL_PROGRESS_MAX_COMMITS);
      throw new RuntimeException(errorMessage, e);
    }

    List<FileGroupRewriteResult> rewriteResults =
        rewrittenGroups.stream().map(RewriteFileGroup::asResult).collect(Collectors.toList());
    long removedPosDeleteRecords = calculateUniquePosDeleteRecordsCount(rewrittenGroups);
    return ImmutableRewriteDataFiles.Result.builder()
        .rewriteResults(rewriteResults)
        .removedPosDeleteRecordsCount(removedPosDeleteRecords);
  }

  private Builder doExecuteWithPartialProgress(
      FileRewritePlan<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> plan,
      RewriteDataFilesCommitManager commitManager) {
    ExecutorService rewriteService = rewriteService();

    // start commit service
    int effectiveGroupCount = Math.min(plan.totalGroupCount(), maxFileGroupCount);
    int groupsPerCommit = IntMath.divide(effectiveGroupCount, maxCommits, RoundingMode.CEILING);
    RewriteDataFilesCommitManager.CommitService commitService =
        commitManager.service(groupsPerCommit);
    commitService.start();

    Collection<FileGroupFailureResult> rewriteFailures = new ConcurrentLinkedQueue<>();

    Iterable<RewriteFileGroup> groupsToRewrite = Iterables.limit(plan.groups(), maxFileGroupCount);
    Tasks.Builder<RewriteFileGroup> rewriteTaskBuilder =
        Tasks.foreach(groupsToRewrite)
            .executeWith(rewriteService)
            .stopOnFailure()
            .noRetry()
            .onFailure(
                (fileGroup, exception) -> {
                  LOG.warn(
                      "Failure during rewrite process for group {}", fileGroup.info(), exception);
                  rewriteFailures.add(
                      ImmutableRewriteDataFiles.FileGroupFailureResult.builder()
                          .info(fileGroup.info())
                          .dataFilesCount(fileGroup.inputFileNum())
                          .build());
                });

    // start rewrite tasks
    try {
      rewriteTaskBuilder.run(fileGroup -> commitService.offer(rewriteFiles(plan, fileGroup)));
    } catch (ValidationException e) {
      // Some commit failed, lets abort future work
      LOG.error(
          "Cannot commit rewrite because of a ValidationException. This usually means that "
              + "this rewrite has conflicted with another concurrent Iceberg operation.");
      List<Runnable> tasks = rewriteService.shutdownNow();
      tasks.forEach(
          task -> {
            if (task instanceof Future) {
              ((Future<?>) task).cancel(true);
            }
          });
      // If no commits succeeded, propagate the error so caller can rescan
      if (commitService.succeededCommits() == 0) {
        throw e;
      }
    } finally {
      rewriteService.shutdown();
      commitService.close();
    }

    List<RewriteFileGroup> committedGroups = commitService.results();
    long removedPosDeleteRecords = calculateUniquePosDeleteRecordsCount(committedGroups);
    return ImmutableRewriteDataFiles.Result.builder()
        .rewriteResults(toRewriteResults(committedGroups))
        .rewriteFailures(rewriteFailures)
        .removedPosDeleteRecordsCount(removedPosDeleteRecords);
  }

  private Iterable<FileGroupRewriteResult> toRewriteResults(List<RewriteFileGroup> commitResults) {
    return commitResults.stream().map(RewriteFileGroup::asResult).collect(Collectors.toList());
  }

  private long calculateUniquePosDeleteRecordsCount(Iterable<RewriteFileGroup> groups) {
    Set<String> seenPaths = Sets.newHashSet();
    Set<DeleteFile> uniquePosDeletes = Sets.newHashSet();
    for (RewriteFileGroup group : groups) {
      for (FileScanTask task : group.fileScanTasks()) {
        for (DeleteFile deleteFile : task.deletes()) {
          if (deleteFile.content() == FileContent.POSITION_DELETES
              && seenPaths.add(deleteFile.path().toString())) {
            uniquePosDeletes.add(deleteFile);
          }
        }
      }
    }
    return uniquePosDeletes.stream().mapToLong(DeleteFile::recordCount).sum();
  }

  void validateAndInitOptions() {
    Set<String> validOptions = Sets.newHashSet(runner.validOptions());
    validOptions.addAll(VALID_OPTIONS);
    validOptions.addAll(planner.validOptions());

    Set<String> invalidKeys = Sets.newHashSet(options().keySet());
    invalidKeys.removeAll(validOptions);

    Preconditions.checkArgument(
        invalidKeys.isEmpty(),
        "Cannot use options %s, they are not supported by the action or the rewriter %s",
        invalidKeys,
        runner.description());

    planner.init(options());
    runner.init(options());

    maxConcurrentFileGroupRewrites =
        PropertyUtil.propertyAsInt(
            options(),
            MAX_CONCURRENT_FILE_GROUP_REWRITES,
            MAX_CONCURRENT_FILE_GROUP_REWRITES_DEFAULT);

    maxFileGroupCount =
        PropertyUtil.propertyAsInt(options(), MAX_FILE_GROUP_COUNT, MAX_FILE_GROUP_COUNT_DEFAULT);

    maxCommits =
        PropertyUtil.propertyAsInt(
            options(), PARTIAL_PROGRESS_MAX_COMMITS, PARTIAL_PROGRESS_MAX_COMMITS_DEFAULT);

    partialProgressEnabled =
        PropertyUtil.propertyAsBoolean(
            options(), PARTIAL_PROGRESS_ENABLED, PARTIAL_PROGRESS_ENABLED_DEFAULT);

    useStartingSequenceNumber =
        PropertyUtil.propertyAsBoolean(
            options(), USE_STARTING_SEQUENCE_NUMBER, USE_STARTING_SEQUENCE_NUMBER_DEFAULT);

    removeDanglingDeletes =
        PropertyUtil.propertyAsBoolean(
            options(), REMOVE_DANGLING_DELETES, REMOVE_DANGLING_DELETES_DEFAULT);

    Preconditions.checkArgument(
        maxConcurrentFileGroupRewrites >= 1,
        "Cannot set %s to %s, the value must be positive.",
        MAX_CONCURRENT_FILE_GROUP_REWRITES,
        maxConcurrentFileGroupRewrites);

    Preconditions.checkArgument(
        maxFileGroupCount >= 1,
        "Cannot set %s to %s, the value must be positive.",
        MAX_FILE_GROUP_COUNT,
        maxFileGroupCount);

    Preconditions.checkArgument(
        !partialProgressEnabled || maxCommits > 0,
        "Cannot set %s to %s, the value must be positive when %s is true",
        PARTIAL_PROGRESS_MAX_COMMITS,
        maxCommits,
        PARTIAL_PROGRESS_ENABLED);
  }

  private String jobDesc(
      RewriteFileGroup group,
      FileRewritePlan<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> plan) {
    StructLike partition = group.info().partition();
    if (partition.size() > 0) {
      return String.format(
          Locale.ROOT,
          "Rewriting %d files with %d delete files (%s, file group %d/%d, %s (%d/%d)) in %s",
          group.rewrittenFiles().size(),
          group.numDeletes(),
          runner.description(),
          group.info().globalIndex(),
          plan.totalGroupCount(),
          partition,
          group.info().partitionIndex(),
          plan.groupsInPartition(partition),
          table.name());
    } else {
      return String.format(
          Locale.ROOT,
          "Rewriting %d files with %d delete files (%s, file group %d/%d) in %s",
          group.rewrittenFiles().size(),
          group.numDeletes(),
          runner.description(),
          group.info().globalIndex(),
          plan.totalGroupCount(),
          table.name());
    }
  }
}
