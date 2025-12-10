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

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.ConvertEqualityDeleteFiles;
import org.apache.iceberg.actions.ImmutableConvertEqualityDeleteFiles;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.deletes.DeleteGranularity;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.SortingPositionOnlyDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.Files;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.FileWriter;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.source.SerializableTableWithSize;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.StructLikeSet;
import org.apache.iceberg.util.Tasks;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spark implementation of {@link ConvertEqualityDeleteFiles}.
 *
 * <p>This action converts equality delete files to position delete files. The conversion is done
 * by:
 *
 * <ol>
 *   <li>Finding all equality delete files in the current snapshot
 *   <li>Reading equality delete keys from delete files (build side for hash join)
 *   <li>Reading data files with position metadata (probe side for hash join)
 *   <li>Performing leftsemi join to find rows matching delete keys
 *   <li>Writing position delete files for matching rows
 *   <li>Committing: removing old equality deletes, adding new position deletes
 * </ol>
 *
 * <p>The commit validates from the starting snapshot to detect concurrent modifications. If the
 * equality delete files were modified by another operation, the commit will fail with {@link
 * ValidationException} or {@link CommitFailedException}.
 */
public class ConvertEqualityDeleteFilesSparkAction
    extends BaseSnapshotUpdateSparkAction<ConvertEqualityDeleteFilesSparkAction>
    implements ConvertEqualityDeleteFiles {

  private static final Logger LOG =
      LoggerFactory.getLogger(ConvertEqualityDeleteFilesSparkAction.class);

  private static final String LOG_PREFIX = "[ConvertEqDeletes]";

  /**
   * If enabled, the action will commit results incrementally as groups complete, allowing partial
   * progress even if the overall action fails. When disabled, all changes are committed atomically
   * at the end.
   */
  public static final String PARTIAL_PROGRESS_ENABLED = "partial-progress.enabled";

  public static final boolean PARTIAL_PROGRESS_ENABLED_DEFAULT = false;

  /**
   * Minimum size of data files (in bytes) to read before attempting a partial commit.
   * This prevents excessive commits for small groups while allowing large groups to commit
   * immediately. Default is 512MB.
   */
  public static final String PARTIAL_PROGRESS_MIN_COMMIT_SIZE_BYTES =
      "partial-progress.min-commit-size-bytes";

  public static final long PARTIAL_PROGRESS_MIN_COMMIT_SIZE_BYTES_DEFAULT = 512L * 1024 * 1024;

  /**
   * Local mount path for s3fs/goofys/mountpoint-s3 FUSE cache.
   * When set, data files will be read from this local path instead of S3.
   * Example: "/mnt/s3-cache/my-bucket"
   *
   * <p>This is useful when running convert_equality_deletes frequently on the same data files.
   * The FUSE mount provides transparent caching with partial read support (range requests),
   * which works well with Parquet bloom filters.
   */
  public static final String CACHE_MOUNT_PATH = "cache.mount-path";

  /**
   * S3 prefix to replace with the local mount path.
   * Example: "s3://my-bucket" or "s3a://my-bucket"
   *
   * <p>When reading a file like "s3://my-bucket/data/file.parquet",
   * if cache.mount-path="/mnt/s3-cache/my-bucket" and cache.s3-prefix="s3://my-bucket",
   * the file will be read from "/mnt/s3-cache/my-bucket/data/file.parquet" if it exists.
   */
  public static final String CACHE_S3_PREFIX = "cache.s3-prefix";

  private static final Result EMPTY_RESULT =
      ImmutableConvertEqualityDeleteFiles.Result.builder()
          .convertedEqualityDeleteFilesCount(0)
          .addedPositionDeleteFilesCount(0)
          .rewrittenDeleteRecordsCount(0L)
          .addedDeleteRecordsCount(0L)
          .build();

  private final Table table;
  // Unique ID for this action instance to prevent file name collisions between parallel jobs
  private final String operationUUID = UUID.randomUUID().toString().substring(0, 8);
  private Expression filter = Expressions.alwaysTrue();
  private boolean partialProgressEnabled = PARTIAL_PROGRESS_ENABLED_DEFAULT;
  private long minCommitSizeBytes = PARTIAL_PROGRESS_MIN_COMMIT_SIZE_BYTES_DEFAULT;
  private String cacheMountPath = null;
  private String cacheS3Prefix = null;

  ConvertEqualityDeleteFilesSparkAction(SparkSession spark, Table table) {
    super(spark.cloneSession());
    // Disable AQE to ensure predictable join behavior for the hash join
    spark().conf().set(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), false);
    this.table = table;
  }

  @Override
  protected ConvertEqualityDeleteFilesSparkAction self() {
    return this;
  }

  @Override
  public ConvertEqualityDeleteFilesSparkAction filter(Expression expression) {
    filter = Expressions.and(filter, expression);
    return this;
  }

  private void initOptions() {
    this.partialProgressEnabled =
        PropertyUtil.propertyAsBoolean(
            options(), PARTIAL_PROGRESS_ENABLED, PARTIAL_PROGRESS_ENABLED_DEFAULT);
    this.minCommitSizeBytes =
        PropertyUtil.propertyAsLong(
            options(),
            PARTIAL_PROGRESS_MIN_COMMIT_SIZE_BYTES,
            PARTIAL_PROGRESS_MIN_COMMIT_SIZE_BYTES_DEFAULT);

    // Cache options for s3fs/FUSE mount
    this.cacheMountPath = options().get(CACHE_MOUNT_PATH);
    this.cacheS3Prefix = options().get(CACHE_S3_PREFIX);

    if (cacheMountPath != null && cacheS3Prefix != null) {
      LOG.info(
          "{} table={} cache_enabled mount_path={} s3_prefix={}",
          LOG_PREFIX,
          table.name(),
          cacheMountPath,
          cacheS3Prefix);
    } else if (cacheMountPath != null || cacheS3Prefix != null) {
      LOG.warn(
          "{} table={} cache partially configured - both {} and {} must be set to enable caching",
          LOG_PREFIX,
          table.name(),
          CACHE_MOUNT_PATH,
          CACHE_S3_PREFIX);
      this.cacheMountPath = null;
      this.cacheS3Prefix = null;
    }
  }

  @Override
  public Result execute() {
    initOptions();

    long startTime = System.currentTimeMillis();

    if (table.currentSnapshot() == null) {
      LOG.info("{} table={} empty table, nothing to convert", LOG_PREFIX, table.name());
      return EMPTY_RESULT;
    }

    long startingSnapshotId = table.currentSnapshot().snapshotId();
    LOG.info(
        "{} table={} snapshot={} filter={} partial_progress={} min_commit_size_bytes={} starting conversion",
        LOG_PREFIX,
        table.name(),
        startingSnapshotId,
        filter,
        partialProgressEnabled,
        minCommitSizeBytes);

    // Step 1: Find all tasks that have equality delete files
    long scanStartTime = System.currentTimeMillis();
    Map<DeleteFileGroup, List<FileScanTask>> tasksWithEqDeletes =
        findTasksWithEqualityDeletes(startingSnapshotId);
    long scanDuration = System.currentTimeMillis() - scanStartTime;

    if (tasksWithEqDeletes.isEmpty()) {
      LOG.info(
          "{} table={} scan_duration_ms={} no equality delete files found",
          LOG_PREFIX,
          table.name(),
          scanDuration);
      return EMPTY_RESULT;
    }

    int totalEqDeleteFiles =
        tasksWithEqDeletes.keySet().stream().mapToInt(g -> g.deleteFiles().size()).sum();
    int totalDataFiles = tasksWithEqDeletes.values().stream().mapToInt(List::size).sum();

    LOG.info(
        "{} table={} scan_duration_ms={} eq_delete_groups={} eq_delete_files={} data_files={}",
        LOG_PREFIX,
        table.name(),
        scanDuration,
        tasksWithEqDeletes.size(),
        totalEqDeleteFiles,
        totalDataFiles);

    if (partialProgressEnabled) {
      return doExecuteWithPartialProgress(tasksWithEqDeletes, startingSnapshotId, startTime);
    } else {
      return doExecute(tasksWithEqDeletes, startingSnapshotId, startTime);
    }
  }

  private Result doExecute(
      Map<DeleteFileGroup, List<FileScanTask>> tasksWithEqDeletes,
      long startingSnapshotId,
      long startTime) {

    Set<DeleteFile> convertedEqDeleteFiles = Sets.newHashSet();
    Set<DeleteFile> addedPosDeleteFiles = Sets.newHashSet();
    long totalRewrittenRecords = 0;
    long totalAddedRecords = 0;

    int groupIndex = 0;
    for (Map.Entry<DeleteFileGroup, List<FileScanTask>> entry : tasksWithEqDeletes.entrySet()) {
      groupIndex++;
      DeleteFileGroup eqDeleteGroup = entry.getKey();
      List<FileScanTask> dataFileTasks = entry.getValue();

      LOG.info(
          "{} table={} group={}/{} eq_delete_files={} data_files={} processing",
          LOG_PREFIX,
          table.name(),
          groupIndex,
          tasksWithEqDeletes.size(),
          eqDeleteGroup.deleteFiles().size(),
          dataFileTasks.size());

      ConversionResult conversionResult =
          convertEqualityDeletes(eqDeleteGroup, dataFileTasks, startingSnapshotId, groupIndex);

      convertedEqDeleteFiles.addAll(eqDeleteGroup.deleteFiles());
      addedPosDeleteFiles.addAll(conversionResult.posDeleteFiles);
      totalRewrittenRecords += conversionResult.eqDeleteRecordsCount;
      totalAddedRecords += conversionResult.posDeleteRecordsCount;
    }

    // Commit
    if (!convertedEqDeleteFiles.isEmpty()) {
      LOG.info(
          "{} table={} eq_delete_files_to_remove={} pos_delete_files_to_add={} "
              + "eq_delete_records={} pos_delete_records={} committing",
          LOG_PREFIX,
          table.name(),
          convertedEqDeleteFiles.size(),
          addedPosDeleteFiles.size(),
          totalRewrittenRecords,
          totalAddedRecords);

      try {
        long commitStartTime = System.currentTimeMillis();
        commitChanges(convertedEqDeleteFiles, addedPosDeleteFiles, startingSnapshotId);
        long commitDuration = System.currentTimeMillis() - commitStartTime;
        LOG.info(
            "{} table={} commit_duration_ms={} commit successful",
            LOG_PREFIX,
            table.name(),
            commitDuration);
      } catch (ValidationException | CommitFailedException e) {
        // NOTE: We intentionally do NOT clean up the created position delete files here.
        // If we delete them, we might remove files that a concurrent job has already committed.
        // Orphan files will be cleaned up later by remove_orphan_files procedure.
        LOG.warn(
            "{} table={} pos_delete_files_count={} commit failed due to concurrent modification. "
                + "Files are NOT cleaned up to avoid deleting files committed by parallel jobs. "
                + "Run remove_orphan_files to clean up.",
            LOG_PREFIX,
            table.name(),
            addedPosDeleteFiles.size());
        throw new RuntimeException(
            "Cannot commit because of a concurrent modification. "
                + "The equality delete files may have been modified by another operation.",
            e);
      }
    }

    long totalDuration = System.currentTimeMillis() - startTime;
    LOG.info(
        "{} table={} total_duration_ms={} converted_eq_delete_files={} "
            + "added_pos_delete_files={} rewritten_records={} added_records={} completed",
        LOG_PREFIX,
        table.name(),
        totalDuration,
        convertedEqDeleteFiles.size(),
        addedPosDeleteFiles.size(),
        totalRewrittenRecords,
        totalAddedRecords);

    return ImmutableConvertEqualityDeleteFiles.Result.builder()
        .convertedEqualityDeleteFilesCount(convertedEqDeleteFiles.size())
        .addedPositionDeleteFilesCount(addedPosDeleteFiles.size())
        .rewrittenDeleteRecordsCount(totalRewrittenRecords)
        .addedDeleteRecordsCount(totalAddedRecords)
        .build();
  }

  private Result doExecuteWithPartialProgress(
      Map<DeleteFileGroup, List<FileScanTask>> tasksWithEqDeletes,
      long startingSnapshotId,
      long startTime) {

    // Build dependency tracking: eq_delete_path -> set of groups containing it
    Map<String, Set<DeleteFileGroup>> eqDeleteToGroups = Maps.newHashMap();
    for (DeleteFileGroup group : tasksWithEqDeletes.keySet()) {
      for (DeleteFile eqDelete : group.deleteFiles()) {
        String path = eqDelete.path().toString();
        eqDeleteToGroups.computeIfAbsent(path, k -> Sets.newHashSet()).add(group);
      }
    }

    // Sort groups by MIN sequence number of their eq deletes (ascending).
    // Eq deletes with lower sequence numbers apply to fewer data files (only D where D.seq < E.seq),
    // so they appear in fewer groups and can be fully processed and committed sooner.
    List<Map.Entry<DeleteFileGroup, List<FileScanTask>>> sortedGroups =
        tasksWithEqDeletes.entrySet().stream()
            .sorted(
                Comparator.comparingLong(
                    entry ->
                        entry.getKey().deleteFiles().stream()
                            .mapToLong(DeleteFile::dataSequenceNumber)
                            .min()
                            .orElse(Long.MAX_VALUE)))
            .collect(Collectors.toList());

    LOG.info(
        "{} table={} groups_sorted_for_partial_progress total_groups={}",
        LOG_PREFIX,
        table.name(),
        sortedGroups.size());

    // Track processed groups and results per group
    Set<DeleteFileGroup> processedGroups = Sets.newHashSet();
    Map<DeleteFileGroup, ConversionResult> groupResults = Maps.newHashMap();

    // Track committed eq deletes and pos deletes
    Set<String> committedEqDeletePaths = Sets.newHashSet();
    Set<DeleteFile> uncommittedPosDeletes = Sets.newHashSet();
    Map<DeleteFile, Set<DeleteFile>> eqDeleteToPosDeletes = Maps.newHashMap();

    // Counters for result
    int totalConvertedEqDeleteFiles = 0;
    int totalAddedPosDeleteFiles = 0;
    long totalRewrittenRecords = 0;
    long totalAddedRecords = 0;
    int commitCount = 0;

    // Track bytes read since last commit for commit threshold
    long bytesReadSinceLastCommit = 0;

    int groupIndex = 0;
    for (Map.Entry<DeleteFileGroup, List<FileScanTask>> entry : sortedGroups) {
      groupIndex++;
      DeleteFileGroup eqDeleteGroup = entry.getKey();
      List<FileScanTask> dataFileTasks = entry.getValue();

      LOG.info(
          "{} table={} group={}/{} eq_delete_files={} data_files={} processing (partial progress)",
          LOG_PREFIX,
          table.name(),
          groupIndex,
          sortedGroups.size(),
          eqDeleteGroup.deleteFiles().size(),
          dataFileTasks.size());

      ConversionResult conversionResult =
          convertEqualityDeletes(eqDeleteGroup, dataFileTasks, startingSnapshotId, groupIndex);

      processedGroups.add(eqDeleteGroup);
      groupResults.put(eqDeleteGroup, conversionResult);
      uncommittedPosDeletes.addAll(conversionResult.posDeleteFiles);

      // Track which pos deletes belong to which eq deletes
      for (DeleteFile eqDelete : eqDeleteGroup.deleteFiles()) {
        eqDeleteToPosDeletes
            .computeIfAbsent(eqDelete, k -> Sets.newHashSet())
            .addAll(conversionResult.posDeleteFiles);
      }

      totalRewrittenRecords += conversionResult.eqDeleteRecordsCount;
      totalAddedRecords += conversionResult.posDeleteRecordsCount;

      // Track bytes read from data files in this group
      long groupDataFilesSize =
          dataFileTasks.stream().mapToLong(task -> task.file().fileSizeInBytes()).sum();
      bytesReadSinceLastCommit += groupDataFilesSize;

      // Check which eq deletes are now fully processed (all their groups are done)
      Set<DeleteFile> readyToCommitEqDeletes = Sets.newHashSet();
      Set<DeleteFile> readyToCommitPosDeletes = Sets.newHashSet();

      for (DeleteFile eqDelete : eqDeleteGroup.deleteFiles()) {
        String eqPath = eqDelete.path().toString();
        if (committedEqDeletePaths.contains(eqPath)) {
          continue; // Already committed
        }

        Set<DeleteFileGroup> groupsForThisEqDelete = eqDeleteToGroups.get(eqPath);
        boolean allGroupsProcessed = processedGroups.containsAll(groupsForThisEqDelete);

        if (allGroupsProcessed) {
          readyToCommitEqDeletes.add(eqDelete);
          // Add all pos deletes from all groups that contain this eq delete
          for (DeleteFileGroup g : groupsForThisEqDelete) {
            ConversionResult result = groupResults.get(g);
            if (result != null) {
              readyToCommitPosDeletes.addAll(result.posDeleteFiles);
            }
          }
        }
      }

      // Filter out already committed pos deletes
      readyToCommitPosDeletes.retainAll(uncommittedPosDeletes);

      // Try to commit if we have ready eq deletes and accumulated enough data
      boolean shouldCommit =
          !readyToCommitEqDeletes.isEmpty() && bytesReadSinceLastCommit >= minCommitSizeBytes;

      if (shouldCommit) {
        LOG.info(
            "{} table={} partial_commit={} eq_delete_files={} pos_delete_files={} "
                + "bytes_since_last_commit={} attempting commit",
            LOG_PREFIX,
            table.name(),
            commitCount + 1,
            readyToCommitEqDeletes.size(),
            readyToCommitPosDeletes.size(),
            bytesReadSinceLastCommit);

        try {
          long commitStartTime = System.currentTimeMillis();
          commitChanges(readyToCommitEqDeletes, readyToCommitPosDeletes, startingSnapshotId);
          long commitDuration = System.currentTimeMillis() - commitStartTime;

          commitCount++;
          totalConvertedEqDeleteFiles += readyToCommitEqDeletes.size();
          totalAddedPosDeleteFiles += readyToCommitPosDeletes.size();

          // Mark as committed
          for (DeleteFile eqDelete : readyToCommitEqDeletes) {
            committedEqDeletePaths.add(eqDelete.path().toString());
          }
          uncommittedPosDeletes.removeAll(readyToCommitPosDeletes);

          // Reset bytes counter after successful commit
          bytesReadSinceLastCommit = 0;

          LOG.info(
              "{} table={} partial_commit={} commit_duration_ms={} success",
              LOG_PREFIX,
              table.name(),
              commitCount,
              commitDuration);

        } catch (ValidationException | CommitFailedException e) {
          // NOTE: We intentionally do NOT clean up the created position delete files here.
          // If we delete them, we might remove files that a concurrent job has already committed.
          // Orphan files will be cleaned up later by remove_orphan_files procedure.
          LOG.error(
              "{} table={} partial_commit failed due to concurrent modification, stopping. "
                  + "Uncommitted files ({}) are NOT cleaned up to avoid deleting files committed by parallel jobs.",
              LOG_PREFIX,
              table.name(),
              uncommittedPosDeletes.size(),
              e);

          // Return partial results
          long totalDuration = System.currentTimeMillis() - startTime;
          LOG.info(
              "{} table={} total_duration_ms={} partial_progress_stopped "
                  + "converted_eq_delete_files={} added_pos_delete_files={} commits={}",
              LOG_PREFIX,
              table.name(),
              totalDuration,
              totalConvertedEqDeleteFiles,
              totalAddedPosDeleteFiles,
              commitCount);

          return ImmutableConvertEqualityDeleteFiles.Result.builder()
              .convertedEqualityDeleteFilesCount(totalConvertedEqDeleteFiles)
              .addedPositionDeleteFilesCount(totalAddedPosDeleteFiles)
              .rewrittenDeleteRecordsCount(totalRewrittenRecords)
              .addedDeleteRecordsCount(totalAddedRecords)
              .build();
        }
      }
    }

    // Final commit for any remaining uncommitted eq deletes
    Set<DeleteFile> remainingEqDeletes = Sets.newHashSet();
    for (DeleteFileGroup group : processedGroups) {
      for (DeleteFile eqDelete : group.deleteFiles()) {
        if (!committedEqDeletePaths.contains(eqDelete.path().toString())) {
          remainingEqDeletes.add(eqDelete);
        }
      }
    }

    if (!remainingEqDeletes.isEmpty() && !uncommittedPosDeletes.isEmpty()) {
      LOG.info(
          "{} table={} final_commit eq_delete_files={} pos_delete_files={} attempting",
          LOG_PREFIX,
          table.name(),
          remainingEqDeletes.size(),
          uncommittedPosDeletes.size());

      try {
        long commitStartTime = System.currentTimeMillis();
        commitChanges(remainingEqDeletes, uncommittedPosDeletes, startingSnapshotId);
        long commitDuration = System.currentTimeMillis() - commitStartTime;

        commitCount++;
        totalConvertedEqDeleteFiles += remainingEqDeletes.size();
        totalAddedPosDeleteFiles += uncommittedPosDeletes.size();

        LOG.info(
            "{} table={} final_commit commit_duration_ms={} success",
            LOG_PREFIX,
            table.name(),
            commitDuration);

      } catch (ValidationException | CommitFailedException e) {
        // NOTE: We intentionally do NOT clean up the created position delete files here.
        // If we delete them, we might remove files that a concurrent job has already committed.
        // Orphan files will be cleaned up later by remove_orphan_files procedure.
        LOG.error(
            "{} table={} final_commit failed due to concurrent modification. "
                + "Uncommitted files ({}) are NOT cleaned up to avoid deleting files committed by parallel jobs.",
            LOG_PREFIX,
            table.name(),
            uncommittedPosDeletes.size(),
            e);

        long totalDuration = System.currentTimeMillis() - startTime;
        LOG.info(
            "{} table={} total_duration_ms={} partial_progress_stopped_at_final_commit "
                + "converted_eq_delete_files={} added_pos_delete_files={} commits={}",
            LOG_PREFIX,
            table.name(),
            totalDuration,
            totalConvertedEqDeleteFiles,
            totalAddedPosDeleteFiles,
            commitCount);

        return ImmutableConvertEqualityDeleteFiles.Result.builder()
            .convertedEqualityDeleteFilesCount(totalConvertedEqDeleteFiles)
            .addedPositionDeleteFilesCount(totalAddedPosDeleteFiles)
            .rewrittenDeleteRecordsCount(totalRewrittenRecords)
            .addedDeleteRecordsCount(totalAddedRecords)
            .build();
      }
    }

    long totalDuration = System.currentTimeMillis() - startTime;
    LOG.info(
        "{} table={} total_duration_ms={} partial_progress_completed "
            + "converted_eq_delete_files={} added_pos_delete_files={} commits={}",
        LOG_PREFIX,
        table.name(),
        totalDuration,
        totalConvertedEqDeleteFiles,
        totalAddedPosDeleteFiles,
        commitCount);

    return ImmutableConvertEqualityDeleteFiles.Result.builder()
        .convertedEqualityDeleteFilesCount(totalConvertedEqDeleteFiles)
        .addedPositionDeleteFilesCount(totalAddedPosDeleteFiles)
        .rewrittenDeleteRecordsCount(totalRewrittenRecords)
        .addedDeleteRecordsCount(totalAddedRecords)
        .build();
  }

  private void cleanUpFiles(Set<DeleteFile> files) {
    Tasks.foreach(files)
        .noRetry()
        .suppressFailureWhenFinished()
        .onFailure(
            (file, exc) ->
                LOG.warn(
                    "{} table={} file={} cleanup failed",
                    LOG_PREFIX,
                    table.name(),
                    file.path(),
                    exc))
        .run(file -> table.io().deleteFile(file.path().toString()));
  }

  private Map<DeleteFileGroup, List<FileScanTask>> findTasksWithEqualityDeletes(long snapshotId) {
    Map<DeleteFileGroup, List<FileScanTask>> result = Maps.newHashMap();

    try (CloseableIterable<CombinedScanTask> combinedTasks =
        table
            .newScan()
            .useSnapshot(snapshotId)
            .filter(filter)
            .includeColumnStats()
            .planTasks()) {

      for (CombinedScanTask combinedTask : combinedTasks) {
        for (FileScanTask task : combinedTask.files()) {
          List<DeleteFile> eqDeletes =
              task.deletes().stream()
                  .filter(d -> d.content() == FileContent.EQUALITY_DELETES)
                  .collect(Collectors.toList());

          if (!eqDeletes.isEmpty()) {
            DeleteFileGroup key = new DeleteFileGroup(eqDeletes, task.spec());
            result.computeIfAbsent(key, k -> Lists.newArrayList()).add(task);
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to plan scan tasks", e);
    }

    return result;
  }

  private ConversionResult convertEqualityDeletes(
      DeleteFileGroup eqDeleteGroup,
      List<FileScanTask> dataFileTasks,
      long snapshotId,
      int groupIndex) {

    List<DeleteFile> eqDeleteFiles = eqDeleteGroup.deleteFiles();
    PartitionSpec spec = eqDeleteGroup.spec();

    Set<Integer> allEqualityFieldIds = Sets.newHashSet();
    for (DeleteFile eqDelete : eqDeleteFiles) {
      allEqualityFieldIds.addAll(eqDelete.equalityFieldIds());
    }

    Schema deleteSchema = TypeUtil.select(table.schema(), allEqualityFieldIds);
    List<String> deleteColumnNames =
        deleteSchema.columns().stream().map(Types.NestedField::name).collect(Collectors.toList());

    StructLike partition =
        dataFileTasks.isEmpty() ? null : dataFileTasks.get(0).file().partition();
    int specId = spec.specId();

    long totalEqDeleteFileSize =
        eqDeleteFiles.stream().mapToLong(DeleteFile::fileSizeInBytes).sum();

    LOG.info(
        "{} table={} group={} eq_delete_files={} eq_delete_size_bytes={} "
            + "equality_columns={} partition={} data_files={}",
        LOG_PREFIX,
        table.name(),
        groupIndex,
        eqDeleteFiles.size(),
        totalEqDeleteFileSize,
        deleteColumnNames,
        partition,
        dataFileTasks.size());

    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark().sparkContext());

    // Step 1: Prepare eq delete file paths (will be read on executors)
    List<String> eqDeleteFilePaths = eqDeleteFiles.stream()
        .map(f -> f.path().toString())
        .collect(Collectors.toList());

    // Step 2: Build list of data files to process (with partition info for executor writes)
    int partitionSize = spec.partitionType().fields().size();
    List<DataFileInfo> dataFileInfos =
        dataFileTasks.stream()
            .map(t -> new DataFileInfo(
                t.file().path().toString(),
                t.file().format().name(),
                t.file().fileSizeInBytes(),
                specId,
                partition,
                partitionSize))
            .distinct()
            .collect(Collectors.toList());

    if (dataFileInfos.isEmpty()) {
      LOG.info("{} table={} group={} no data files to process", LOG_PREFIX, table.name(), groupIndex);
      return new ConversionResult(Sets.newHashSet(), 0, 0);
    }

    long totalDataFileSize = dataFileInfos.stream().mapToLong(DataFileInfo::fileSizeInBytes).sum();

    LOG.info(
        "{} table={} group={} data_files={} data_size_bytes={} processing on executors",
        LOG_PREFIX,
        table.name(),
        groupIndex,
        dataFileInfos.size(),
        totalDataFileSize);

    // Step 3: Distributed processing - read eq deletes and data files on executors
    long startTime = System.currentTimeMillis();

    Table serializableTable = SerializableTableWithSize.copyOf(table);
    Broadcast<Table> tableBroadcast = jsc.broadcast(serializableTable);
    Broadcast<List<String>> eqDeletePathsBroadcast = jsc.broadcast(eqDeleteFilePaths);

    // Add _pos column to projection schema for correct row position tracking
    List<Types.NestedField> projectionFields = Lists.newArrayList(deleteSchema.columns());
    projectionFields.add(MetadataColumns.ROW_POSITION);
    Schema projectionSchema = new Schema(projectionFields);

    // Accumulators - registered with names for Spark UI visibility
    org.apache.spark.util.LongAccumulator eqDeleteRecordsRead = new org.apache.spark.util.LongAccumulator();
    org.apache.spark.util.LongAccumulator eqDeleteReadTimeMs = new org.apache.spark.util.LongAccumulator();
    org.apache.spark.util.LongAccumulator dataFileReadTimeMs = new org.apache.spark.util.LongAccumulator();
    org.apache.spark.util.LongAccumulator posDeleteWriteTimeMs = new org.apache.spark.util.LongAccumulator();
    org.apache.spark.util.LongAccumulator posDeleteRecordsWritten = new org.apache.spark.util.LongAccumulator();
    org.apache.spark.util.LongAccumulator filesSkipped = new org.apache.spark.util.LongAccumulator();
    org.apache.spark.util.LongAccumulator dataFileBytesRead = new org.apache.spark.util.LongAccumulator();
    spark().sparkContext().register(eqDeleteRecordsRead, "ConvertEqDeletes.eqDeleteRecordsRead");
    spark().sparkContext().register(eqDeleteReadTimeMs, "ConvertEqDeletes.eqDeleteReadTimeMs");
    spark().sparkContext().register(dataFileReadTimeMs, "ConvertEqDeletes.dataFileReadTimeMs");
    spark().sparkContext().register(posDeleteWriteTimeMs, "ConvertEqDeletes.posDeleteWriteTimeMs");
    spark().sparkContext().register(posDeleteRecordsWritten, "ConvertEqDeletes.posDeleteRecordsWritten");
    spark().sparkContext().register(filesSkipped, "ConvertEqDeletes.filesSkipped");
    spark().sparkContext().register(dataFileBytesRead, "ConvertEqDeletes.dataFileBytesRead");

    // Distribute data files with enough parallelism
    int numPartitions = Math.max(1, Math.min(dataFileInfos.size(),
        spark().sparkContext().defaultParallelism()));
    JavaRDD<DataFileInfo> filesRDD = jsc.parallelize(dataFileInfos, numPartitions);

    // Generate unique operation ID
    String operationId = snapshotId + "-" + operationUUID + "-g" + groupIndex;

    // Process on executors: read eq deletes once per partition, then process all data files
    JavaRDD<DeleteFileInfo> deleteFileInfosRDD = filesRDD.mapPartitions(
        new ProcessPartitionFunction(
            tableBroadcast,
            eqDeletePathsBroadcast,
            deleteSchema,
            projectionSchema,
            eqDeleteRecordsRead,
            eqDeleteReadTimeMs,
            dataFileReadTimeMs,
            posDeleteWriteTimeMs,
            posDeleteRecordsWritten,
            filesSkipped,
            dataFileBytesRead,
            cacheMountPath,
            cacheS3Prefix,
            groupIndex,
            operationId));

    // Collect delete file metadata
    spark().sparkContext().setJobDescription(
        String.format("ConvertEqDeletes: %s group=%d data_files=%d eq_deletes=%d",
            table.name(), groupIndex, dataFileInfos.size(), eqDeleteFilePaths.size()));
    List<DeleteFileInfo> deleteFileInfos = deleteFileInfosRDD.collect();
    spark().sparkContext().setJobDescription(null);
    long duration = System.currentTimeMillis() - startTime;

    long eqDeleteRecordsCount = eqDeleteRecordsRead.value();
    long posDeleteRecordsCount = posDeleteRecordsWritten.value();

    LOG.info(
        "{} table={} group={} total_ms={} eq_read_ms={} data_read_ms={} pos_write_ms={} "
            + "data_files={} data_bytes_total={} data_bytes_read={} files_skipped={} "
            + "eq_delete_records={} pos_delete_files={} pos_delete_records={}",
        LOG_PREFIX,
        table.name(),
        groupIndex,
        duration,
        eqDeleteReadTimeMs.value(),
        dataFileReadTimeMs.value(),
        posDeleteWriteTimeMs.value(),
        dataFileInfos.size(),
        totalDataFileSize,
        dataFileBytesRead.value(),
        filesSkipped.value(),
        eqDeleteRecordsCount,
        deleteFileInfos.size(),
        posDeleteRecordsCount);

    // Log each created pos delete file
    for (DeleteFileInfo info : deleteFileInfos) {
      LOG.info(
          "{} table={} group={} pos_delete_file={} size_bytes={} records={}",
          LOG_PREFIX,
          table.name(),
          groupIndex,
          info.path(),
          info.fileSizeInBytes(),
          info.recordCount());
    }

    // Convert DeleteFileInfo to DeleteFile for commit
    Set<DeleteFile> posDeleteFiles = convertToDeleteFiles(deleteFileInfos, spec);

    return new ConversionResult(posDeleteFiles, eqDeleteRecordsCount, posDeleteRecordsCount);
  }

  /** Convert serializable DeleteFileInfo from executors to DeleteFile for commit. */
  private Set<DeleteFile> convertToDeleteFiles(List<DeleteFileInfo> deleteFileInfos, PartitionSpec spec) {
    Set<DeleteFile> result = Sets.newHashSet();
    for (DeleteFileInfo info : deleteFileInfos) {
      StructLike partition = info.partitionValues() != null
          ? new PartitionWrapper(info.partitionValues())
          : null;

      org.apache.iceberg.DataFiles.Builder builder = org.apache.iceberg.DataFiles.builder(spec);
      // Use FileMetadata to build DeleteFile
      DeleteFile deleteFile = org.apache.iceberg.FileMetadata.deleteFileBuilder(spec)
          .ofPositionDeletes()
          .withPath(info.path())
          .withFormat(FileFormat.fromString(info.path().substring(info.path().lastIndexOf('.') + 1).toUpperCase()))
          .withFileSizeInBytes(info.fileSizeInBytes())
          .withRecordCount(info.recordCount())
          .withPartition(partition)
          .build();
      result.add(deleteFile);
    }
    return result;
  }

  /** Serializable data file info for distribution to executors. */
  private static class DataFileInfo implements Serializable {
    private final String path;
    private final String format;
    private final long fileSizeInBytes;
    private final int specId;
    private final Object[] partitionValues;

    DataFileInfo(String path, String format, long fileSizeInBytes, int specId,
        StructLike partition, int partitionSize) {
      this.path = path;
      this.format = format;
      this.fileSizeInBytes = fileSizeInBytes;
      this.specId = specId;
      if (partition != null && partitionSize > 0) {
        this.partitionValues = new Object[partitionSize];
        for (int i = 0; i < partitionSize; i++) {
          this.partitionValues[i] = partition.get(i, Object.class);
        }
      } else {
        this.partitionValues = null;
      }
    }

    String path() {
      return path;
    }

    FileFormat format() {
      return FileFormat.fromString(format);
    }

    long fileSizeInBytes() {
      return fileSizeInBytes;
    }

    int specId() {
      return specId;
    }

    Object[] partitionValues() {
      return partitionValues;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      DataFileInfo that = (DataFileInfo) o;
      return path.equals(that.path);
    }

    @Override
    public int hashCode() {
      return path.hashCode();
    }
  }

  /** Serializable wrapper for partition values. */
  private static class PartitionWrapper implements StructLike, Serializable {
    private final Object[] values;

    PartitionWrapper(Object[] values) {
      this.values = values;
    }

    @Override
    public int size() {
      return values != null ? values.length : 0;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(values[pos]);
    }

    @Override
    public <T> void set(int pos, T value) {
      values[pos] = value;
    }
  }

  /** Serializable delete file metadata returned from executors. */
  private static class DeleteFileInfo implements Serializable {
    private final String path;
    private final String format;
    private final long fileSizeInBytes;
    private final long recordCount;
    private final int specId;
    private final Object[] partitionValues;

    DeleteFileInfo(
        String path,
        String format,
        long fileSizeInBytes,
        long recordCount,
        int specId,
        Object[] partitionValues) {
      this.path = path;
      this.format = format;
      this.fileSizeInBytes = fileSizeInBytes;
      this.recordCount = recordCount;
      this.specId = specId;
      this.partitionValues = partitionValues;
    }

    static DeleteFileInfo from(DeleteFile deleteFile, Object[] partitionValues) {
      return new DeleteFileInfo(
          deleteFile.path().toString(),
          deleteFile.format().name(),
          deleteFile.fileSizeInBytes(),
          deleteFile.recordCount(),
          deleteFile.specId(),
          partitionValues);
    }

    String path() {
      return path;
    }

    long fileSizeInBytes() {
      return fileSizeInBytes;
    }

    long recordCount() {
      return recordCount;
    }

    int specId() {
      return specId;
    }

    Object[] partitionValues() {
      return partitionValues;
    }
  }

  // ==================== Static helper methods for executor-side operations ====================

  /** Write position delete file on executor and return metadata. */
  private static List<DeleteFileInfo> writePosDeleteFileOnExecutor(
      Table table,
      DataFileInfo fileInfo,
      List<PositionDelete<Record>> posDeletes,
      int groupIndex,
      String operationId) throws IOException {

    PartitionSpec spec = table.specs().get(fileInfo.specId());
    StructLike partition = fileInfo.partitionValues() != null
        ? new PartitionWrapper(fileInfo.partitionValues())
        : null;

    String deleteFileFormatStr = table.properties().getOrDefault(
        TableProperties.DELETE_DEFAULT_FILE_FORMAT,
        TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
    FileFormat deleteFileFormat = FileFormat.fromString(deleteFileFormatStr);

    int taskId = org.apache.spark.TaskContext.getPartitionId();

    OutputFileFactory outputFileFactory =
        OutputFileFactory.builderFor(table, taskId, groupIndex)
            .format(deleteFileFormat)
            .operationId(operationId)
            .suffix("pos-deletes")
            .build();

    GenericAppenderFactory appenderFactory =
        new GenericAppenderFactory(table, table.schema(), spec, table.properties(), null, null, null);

    FileWriter<PositionDelete<Record>, DeleteWriteResult> posDeleteWriter =
        new SortingPositionOnlyDeleteWriter<>(
            () -> {
              EncryptedOutputFile outputFile = spec.isUnpartitioned()
                  ? outputFileFactory.newOutputFile()
                  : outputFileFactory.newOutputFile(spec, partition);
              return appenderFactory.newPosDeleteWriter(outputFile, deleteFileFormat, partition);
            },
            DeleteGranularity.FILE);

    for (PositionDelete<Record> posDelete : posDeletes) {
      posDeleteWriter.write(posDelete);
    }

    posDeleteWriter.close();
    DeleteWriteResult writeResult = posDeleteWriter.result();

    List<DeleteFileInfo> result = Lists.newArrayList();
    for (DeleteFile deleteFile : writeResult.deleteFiles()) {
      result.add(DeleteFileInfo.from(deleteFile, fileInfo.partitionValues()));
    }
    return result;
  }

  /** Get InputFile, using local FUSE mount path if configured. */
  private static InputFile getInputFileWithCache(
      String s3Path, Table table, String cacheMountPath, String cacheS3Prefix) {
    if (cacheMountPath != null && cacheS3Prefix != null && s3Path.startsWith(cacheS3Prefix)) {
      String localPath = s3Path.replace(cacheS3Prefix, cacheMountPath);
      return Files.localInput(new File(localPath));
    }
    return table.io().newInputFile(s3Path);
  }

  /** Open data file for reading with optional filter. */
  private static CloseableIterable<Record> openDataFileForRead(
      InputFile inputFile, Schema schema, FileFormat format, Expression filter) {
    switch (format) {
      case PARQUET:
        Parquet.ReadBuilder parquetBuilder = Parquet.read(inputFile)
            .project(schema)
            .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema));
        if (filter != null) {
          parquetBuilder.filter(filter);
        }
        return parquetBuilder.build();
      case ORC:
        ORC.ReadBuilder orcBuilder = ORC.read(inputFile)
            .project(schema)
            .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(schema, fileSchema));
        if (filter != null) {
          orcBuilder.filter(filter);
        }
        return orcBuilder.build();
      case AVRO:
        return Avro.read(inputFile)
            .project(schema)
            .createReaderFunc(DataReader::create)
            .build();
      default:
        throw new UnsupportedOperationException("Unsupported format: " + format);
    }
  }

  // ==================== Executor function for processing partitions ====================

  /**
   * Process a partition of data files on executor.
   * Reads eq delete keys once per partition, then processes all data files.
   */
  private static class ProcessPartitionFunction
      implements FlatMapFunction<Iterator<DataFileInfo>, DeleteFileInfo> {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessPartitionFunction.class);

    private final Broadcast<Table> tableBroadcast;
    private final Broadcast<List<String>> eqDeletePathsBroadcast;
    private final Schema deleteSchema;
    private final Schema projectionSchema;
    private final org.apache.spark.util.LongAccumulator eqDeleteRecordsRead;
    private final org.apache.spark.util.LongAccumulator eqDeleteReadTimeMs;
    private final org.apache.spark.util.LongAccumulator dataFileReadTimeMs;
    private final org.apache.spark.util.LongAccumulator posDeleteWriteTimeMs;
    private final org.apache.spark.util.LongAccumulator posDeleteRecordsWritten;
    private final org.apache.spark.util.LongAccumulator filesSkipped;
    private final org.apache.spark.util.LongAccumulator dataFileBytesRead;
    private final String cacheMountPath;
    private final String cacheS3Prefix;
    private final int groupIndex;
    private final String operationId;

    ProcessPartitionFunction(
        Broadcast<Table> tableBroadcast,
        Broadcast<List<String>> eqDeletePathsBroadcast,
        Schema deleteSchema,
        Schema projectionSchema,
        org.apache.spark.util.LongAccumulator eqDeleteRecordsRead,
        org.apache.spark.util.LongAccumulator eqDeleteReadTimeMs,
        org.apache.spark.util.LongAccumulator dataFileReadTimeMs,
        org.apache.spark.util.LongAccumulator posDeleteWriteTimeMs,
        org.apache.spark.util.LongAccumulator posDeleteRecordsWritten,
        org.apache.spark.util.LongAccumulator filesSkipped,
        org.apache.spark.util.LongAccumulator dataFileBytesRead,
        String cacheMountPath,
        String cacheS3Prefix,
        int groupIndex,
        String operationId) {
      this.tableBroadcast = tableBroadcast;
      this.eqDeletePathsBroadcast = eqDeletePathsBroadcast;
      this.deleteSchema = deleteSchema;
      this.projectionSchema = projectionSchema;
      this.eqDeleteRecordsRead = eqDeleteRecordsRead;
      this.eqDeleteReadTimeMs = eqDeleteReadTimeMs;
      this.dataFileReadTimeMs = dataFileReadTimeMs;
      this.posDeleteWriteTimeMs = posDeleteWriteTimeMs;
      this.posDeleteRecordsWritten = posDeleteRecordsWritten;
      this.filesSkipped = filesSkipped;
      this.dataFileBytesRead = dataFileBytesRead;
      this.cacheMountPath = cacheMountPath;
      this.cacheS3Prefix = cacheS3Prefix;
      this.groupIndex = groupIndex;
      this.operationId = operationId;
    }

    @Override
    public Iterator<DeleteFileInfo> call(Iterator<DataFileInfo> dataFiles) throws Exception {
      if (!dataFiles.hasNext()) {
        return java.util.Collections.emptyIterator();
      }

      Table table = tableBroadcast.value();
      List<String> eqDeletePaths = eqDeletePathsBroadcast.value();

      // Determine key type for optimized reading (no intermediate allocations)
      int keyColumnCount = deleteSchema.columns().size();
      boolean isSingleColumn = keyColumnCount == 1;
      Types.NestedField firstCol = deleteSchema.columns().get(0);
      org.apache.iceberg.types.Type.TypeID typeId = firstCol.type().typeId();
      boolean isSingleLongColumn = isSingleColumn
          && (typeId == org.apache.iceberg.types.Type.TypeID.LONG
              || typeId == org.apache.iceberg.types.Type.TypeID.INTEGER);
      boolean isSingleStringColumn = isSingleColumn
          && typeId == org.apache.iceberg.types.Type.TypeID.STRING;

      // Step 1: Read equality delete keys directly into optimized data structure
      long eqReadStart = System.currentTimeMillis();
      Set<Long> longKeys = null;
      Set<String> stringKeys = null;
      Set<List<Object>> deleteKeys = null;

      if (isSingleLongColumn) {
        longKeys = readEqDeleteLongKeysOnExecutor(table, eqDeletePaths);
        eqDeleteRecordsRead.add(longKeys.size());
        if (longKeys.isEmpty()) {
          eqDeleteReadTimeMs.add(System.currentTimeMillis() - eqReadStart);
          return java.util.Collections.emptyIterator();
        }
      } else if (isSingleStringColumn) {
        stringKeys = readEqDeleteStringKeysOnExecutor(table, eqDeletePaths);
        eqDeleteRecordsRead.add(stringKeys.size());
        if (stringKeys.isEmpty()) {
          eqDeleteReadTimeMs.add(System.currentTimeMillis() - eqReadStart);
          return java.util.Collections.emptyIterator();
        }
      } else {
        deleteKeys = readEqDeleteKeysOnExecutor(table, eqDeletePaths);
        eqDeleteRecordsRead.add(deleteKeys.size());
        if (deleteKeys.isEmpty()) {
          eqDeleteReadTimeMs.add(System.currentTimeMillis() - eqReadStart);
          return java.util.Collections.emptyIterator();
        }
      }
      eqDeleteReadTimeMs.add(System.currentTimeMillis() - eqReadStart);

      // Step 2: Process all data files in this partition
      List<DeleteFileInfo> results = Lists.newArrayList();
      String eqColumnName = firstCol.name();
      int posColumnIndex = projectionSchema.columns().size() - 1;

      while (dataFiles.hasNext()) {
        DataFileInfo fileInfo = dataFiles.next();
        List<PositionDelete<Record>> matches = Lists.newArrayList();

        InputFile inputFile = getInputFileWithCache(fileInfo.path(), table, cacheMountPath, cacheS3Prefix);

        // Build bloom filter for single-column optimized paths
        Expression bloomFilter = null;
        if (isSingleLongColumn && longKeys.size() <= 10000) {
          bloomFilter = Expressions.in(eqColumnName, longKeys);
        } else if (isSingleStringColumn && stringKeys.size() <= 10000) {
          bloomFilter = Expressions.in(eqColumnName, stringKeys);
        }

        boolean anyRowsRead = false;
        long dataReadStart = System.currentTimeMillis();
        try (CloseableIterable<Record> reader =
            openDataFileForRead(inputFile, projectionSchema, fileInfo.format(), bloomFilter)) {
          for (Record record : reader) {
            if (!anyRowsRead) {
              anyRowsRead = true;
            }
            boolean match = false;

            if (isSingleLongColumn) {
              Object val = record.get(0);
              long key = val instanceof Integer ? ((Integer) val).longValue() : (Long) val;
              match = longKeys.contains(key);
            } else if (isSingleStringColumn) {
              Object val = record.get(0);
              String key = val != null ? val.toString() : null;
              match = stringKeys.contains(key);
            } else {
              List<Object> recordKey = Lists.newArrayListWithCapacity(keyColumnCount);
              for (int i = 0; i < keyColumnCount; i++) {
                recordKey.add(record.get(i));
              }
              match = deleteKeys.contains(recordKey);
            }

            if (match) {
              Long pos = (Long) record.get(posColumnIndex);
              PositionDelete<Record> posDelete = PositionDelete.create();
              posDelete.set(fileInfo.path(), pos, null);
              matches.add(posDelete);
            }
          }
        }
        dataFileReadTimeMs.add(System.currentTimeMillis() - dataReadStart);

        if (!anyRowsRead) {
          // File was skipped by bloom filter (no rows read at all)
          filesSkipped.add(1);
        } else {
          // File was actually read
          dataFileBytesRead.add(fileInfo.fileSizeInBytes());

          if (!matches.isEmpty()) {
            long writeStart = System.currentTimeMillis();
            List<DeleteFileInfo> written = writePosDeleteFileOnExecutor(
                table, fileInfo, matches, groupIndex, operationId);
            posDeleteWriteTimeMs.add(System.currentTimeMillis() - writeStart);
            results.addAll(written);
            posDeleteRecordsWritten.add(matches.size());
          }
        }
      }

      return results.iterator();
    }

    /** Read equality delete keys as Long directly (no intermediate List allocation). */
    private Set<Long> readEqDeleteLongKeysOnExecutor(Table table, List<String> eqDeletePaths) {
      Set<Long> keys = Sets.newHashSet();

      for (String path : eqDeletePaths) {
        InputFile inputFile = getInputFileWithCache(path, table, cacheMountPath, cacheS3Prefix);
        FileFormat format = FileFormat.fromFileName(path);

        try (CloseableIterable<Record> reader = openDeleteFileForRead(inputFile, deleteSchema, format)) {
          for (Record record : reader) {
            Object val = record.get(0);
            keys.add(val instanceof Integer ? ((Integer) val).longValue() : (Long) val);
          }
        } catch (IOException e) {
          throw new RuntimeException("Failed to read eq delete file: " + path, e);
        }
      }

      return keys;
    }

    /** Read equality delete keys as String directly (no intermediate List allocation). */
    private Set<String> readEqDeleteStringKeysOnExecutor(Table table, List<String> eqDeletePaths) {
      Set<String> keys = Sets.newHashSet();

      for (String path : eqDeletePaths) {
        InputFile inputFile = getInputFileWithCache(path, table, cacheMountPath, cacheS3Prefix);
        FileFormat format = FileFormat.fromFileName(path);

        try (CloseableIterable<Record> reader = openDeleteFileForRead(inputFile, deleteSchema, format)) {
          for (Record record : reader) {
            Object val = record.get(0);
            keys.add(val != null ? val.toString() : null);
          }
        } catch (IOException e) {
          throw new RuntimeException("Failed to read eq delete file: " + path, e);
        }
      }

      return keys;
    }

    /** Read equality delete keys for multi-column keys. */
    private Set<List<Object>> readEqDeleteKeysOnExecutor(Table table, List<String> eqDeletePaths) {
      Set<List<Object>> keys = Sets.newHashSet();
      int keyColumnCount = deleteSchema.columns().size();

      for (String path : eqDeletePaths) {
        InputFile inputFile = getInputFileWithCache(path, table, cacheMountPath, cacheS3Prefix);
        FileFormat format = FileFormat.fromFileName(path);

        try (CloseableIterable<Record> reader = openDeleteFileForRead(inputFile, deleteSchema, format)) {
          for (Record record : reader) {
            List<Object> keyValues = Lists.newArrayListWithCapacity(keyColumnCount);
            for (int i = 0; i < keyColumnCount; i++) {
              keyValues.add(record.get(i));
            }
            keys.add(keyValues);
          }
        } catch (IOException e) {
          throw new RuntimeException("Failed to read eq delete file: " + path, e);
        }
      }

      return keys;
    }

    /** Open delete file for reading. */
    private CloseableIterable<Record> openDeleteFileForRead(
        InputFile inputFile, Schema schema, FileFormat format) {
      switch (format) {
        case PARQUET:
          return Parquet.read(inputFile)
              .project(schema)
              .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
              .build();
        case ORC:
          return ORC.read(inputFile)
              .project(schema)
              .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(schema, fileSchema))
              .build();
        case AVRO:
          return Avro.read(inputFile)
              .project(schema)
              .createReaderFunc(DataReader::create)
              .build();
        default:
          throw new UnsupportedOperationException("Unsupported format: " + format);
      }
    }
  }

  /**
   * Read equality delete keys using Iceberg's DeleteLoader.
   *
   * <p>Uses BaseDeleteLoader which handles S3/cloud storage via table's FileIO.
   */
  private Dataset<Row> readEqualityDeleteKeys(
      List<DeleteFile> eqDeleteFiles, Schema deleteSchema, int groupIndex) {

    DeleteLoader deleteLoader =
        new BaseDeleteLoader(deleteFile -> table.io().newInputFile(deleteFile.path().toString()));

    StructLikeSet deleteKeys = deleteLoader.loadEqualityDeletes(eqDeleteFiles, deleteSchema);

    LOG.info(
        "{} table={} group={} read_eq_delete_records={} from {} files",
        LOG_PREFIX,
        table.name(),
        groupIndex,
        deleteKeys.size(),
        eqDeleteFiles.size());

    StructType sparkSchema = SparkSchemaUtil.convert(deleteSchema);
    List<Row> rows = Lists.newArrayListWithCapacity(deleteKeys.size());

    for (StructLike key : deleteKeys) {
      Object[] values = new Object[deleteSchema.columns().size()];
      for (int i = 0; i < deleteSchema.columns().size(); i++) {
        values[i] = key.get(i, Object.class);
      }
      rows.add(RowFactory.create(values));
    }

    if (rows.isEmpty()) {
      return spark().createDataFrame(rows, sparkSchema);
    }

    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark().sparkContext());
    JavaRDD<Row> rdd = jsc.parallelize(rows);
    return spark().createDataFrame(rdd, sparkSchema);
  }

  /**
   * Read data files directly via FileIO WITHOUT applying deletes.
   *
   * <p>This is necessary because when reading via Iceberg source, equality deletes are
   * automatically applied, which means the rows we want to convert are already filtered out.
   */
  private Dataset<Row> readDataFilesWithPosition(
      List<FileScanTask> dataFileTasks, List<String> equalityColumns, long snapshotId) {

    Schema eqColumnsSchema = TypeUtil.select(table.schema(),
        table.schema().columns().stream()
            .filter(f -> equalityColumns.contains(f.name()))
            .map(Types.NestedField::fieldId)
            .collect(Collectors.toSet()));

    List<Types.NestedField> projectionFields = Lists.newArrayList(eqColumnsSchema.columns());
    Schema projectionSchema = new Schema(projectionFields);

    StructType sparkSchema = SparkSchemaUtil.convert(projectionSchema);
    // Add _file and _pos columns
    sparkSchema = sparkSchema.add(MetadataColumns.FILE_PATH.name(),
        org.apache.spark.sql.types.DataTypes.StringType, false);
    sparkSchema = sparkSchema.add(MetadataColumns.ROW_POSITION.name(),
        org.apache.spark.sql.types.DataTypes.LongType, false);

    List<Row> allRows = Lists.newArrayList();

    // Get unique data files
    Set<String> processedFiles = Sets.newHashSet();
    for (FileScanTask task : dataFileTasks) {
      String filePath = task.file().path().toString();
      if (processedFiles.contains(filePath)) {
        continue;
      }
      processedFiles.add(filePath);

      InputFile inputFile = table.io().newInputFile(filePath);
      FileFormat format = task.file().format();

      long rowPosition = 0;
      try (CloseableIterable<Record> reader = openDataFile(inputFile, projectionSchema, format)) {
        for (Record record : reader) {
          Object[] values = new Object[equalityColumns.size() + 2];
          for (int i = 0; i < equalityColumns.size(); i++) {
            values[i] = record.get(i);
          }
          values[equalityColumns.size()] = filePath;
          values[equalityColumns.size() + 1] = rowPosition;
          allRows.add(RowFactory.create(values));
          rowPosition++;
        }
      } catch (IOException e) {
        throw new RuntimeException("Failed to read data file: " + filePath, e);
      }
    }

    if (allRows.isEmpty()) {
      return spark().createDataFrame(allRows, sparkSchema);
    }

    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark().sparkContext());
    JavaRDD<Row> rdd = jsc.parallelize(allRows);
    return spark().createDataFrame(rdd, sparkSchema);
  }

  private CloseableIterable<Record> openDataFile(
      InputFile inputFile, Schema projectionSchema, FileFormat format) {
    switch (format) {
      case PARQUET:
        return Parquet.read(inputFile)
            .project(projectionSchema)
            .createReaderFunc(
                fileSchema -> GenericParquetReaders.buildReader(projectionSchema, fileSchema))
            .build();
      case ORC:
        return ORC.read(inputFile)
            .project(projectionSchema)
            .createReaderFunc(
                fileSchema -> GenericOrcReader.buildReader(projectionSchema, fileSchema))
            .build();
      case AVRO:
        return Avro.read(inputFile)
            .project(projectionSchema)
            .createReaderFunc(DataReader::create)
            .build();
      default:
        throw new UnsupportedOperationException("Unsupported data file format: " + format);
    }
  }

  private ConversionResult writePosDeleteFiles(
      Dataset<Row> positionDeletes,
      int specId,
      StructLike partition,
      long snapshotId,
      long eqDeleteRecordsCount,
      int groupIndex) {

    Set<DeleteFile> result = Sets.newHashSet();

    List<Row> rows = positionDeletes.collectAsList();
    if (rows.isEmpty()) {
      LOG.info(
          "{} table={} group={} pos_delete_records=0 nothing to write",
          LOG_PREFIX,
          table.name(),
          groupIndex);
      return new ConversionResult(result, eqDeleteRecordsCount, 0);
    }

    long posDeleteRecordsCount = rows.size();

    LOG.info(
        "{} table={} group={} pos_delete_records={} partition={} writing",
        LOG_PREFIX,
        table.name(),
        groupIndex,
        posDeleteRecordsCount,
        partition);

    long writeStartTime = System.currentTimeMillis();

    PartitionSpec spec = table.specs().get(specId);

    String deleteFileFormatStr =
        table
            .properties()
            .getOrDefault(
                TableProperties.DELETE_DEFAULT_FILE_FORMAT,
                TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
    FileFormat deleteFileFormat = FileFormat.fromString(deleteFileFormatStr);

    OutputFileFactory outputFileFactory =
        OutputFileFactory.builderFor(table, 0, groupIndex)
            .format(deleteFileFormat)
            .operationId(String.valueOf(snapshotId) + "-" + operationUUID + "-g" + groupIndex)
            .suffix("pos-deletes")
            .build();

    GenericAppenderFactory appenderFactory =
        new GenericAppenderFactory(table, table.schema(), spec, table.properties(), null, null, null);

    FileWriter<PositionDelete<Record>, DeleteWriteResult> posDeleteWriter =
        new SortingPositionOnlyDeleteWriter<>(
            () -> {
              EncryptedOutputFile outputFile =
                  spec.isUnpartitioned()
                      ? outputFileFactory.newOutputFile()
                      : outputFileFactory.newOutputFile(spec, partition);
              return appenderFactory.newPosDeleteWriter(outputFile, deleteFileFormat, partition);
            },
            DeleteGranularity.FILE);

    try {
      PositionDelete<Record> posDelete = PositionDelete.create();
      for (Row row : rows) {
        String filePath = row.getString(0);
        long pos = row.getLong(1);
        posDelete.set(filePath, pos, null);
        posDeleteWriter.write(posDelete);
      }

      posDeleteWriter.close();
      DeleteWriteResult writeResult = posDeleteWriter.result();
      result.addAll(writeResult.deleteFiles());

    } catch (IOException e) {
      throw new RuntimeException("Failed to write position delete files", e);
    }

    long writeDuration = System.currentTimeMillis() - writeStartTime;
    long totalWrittenBytes = result.stream().mapToLong(DeleteFile::fileSizeInBytes).sum();

    LOG.info(
        "{} table={} group={} write_duration_ms={} pos_delete_files={} written_bytes={}",
        LOG_PREFIX,
        table.name(),
        groupIndex,
        writeDuration,
        result.size(),
        totalWrittenBytes);

    for (DeleteFile file : result) {
      LOG.info(
          "{} table={} group={} pos_delete_file={} size_bytes={} records={}",
          LOG_PREFIX,
          table.name(),
          groupIndex,
          file.path(),
          file.fileSizeInBytes(),
          file.recordCount());
    }

    return new ConversionResult(result, eqDeleteRecordsCount, posDeleteRecordsCount);
  }

  private void commitChanges(
      Set<DeleteFile> eqDeleteFilesToRemove,
      Set<DeleteFile> posDeleteFilesToAdd,
      long startingSnapshotId) {

    long maxSequenceNumber =
        eqDeleteFilesToRemove.stream().mapToLong(DeleteFile::dataSequenceNumber).max().orElse(0);

    LOG.info(
        "{} table={} starting_snapshot={} max_sequence_number={} committing",
        LOG_PREFIX,
        table.name(),
        startingSnapshotId,
        maxSequenceNumber);

    RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(startingSnapshotId);

    for (DeleteFile eqDelete : eqDeleteFilesToRemove) {
      rewrite.deleteFile(eqDelete);
    }

    for (DeleteFile posDelete : posDeleteFilesToAdd) {
      rewrite.addFile(posDelete, maxSequenceNumber);
    }

    commitSummary().forEach(rewrite::set);
    rewrite.commit();
  }

  private static class ConversionResult {
    final Set<DeleteFile> posDeleteFiles;
    final long eqDeleteRecordsCount;
    final long posDeleteRecordsCount;

    ConversionResult(
        Set<DeleteFile> posDeleteFiles, long eqDeleteRecordsCount, long posDeleteRecordsCount) {
      this.posDeleteFiles = posDeleteFiles;
      this.eqDeleteRecordsCount = eqDeleteRecordsCount;
      this.posDeleteRecordsCount = posDeleteRecordsCount;
    }
  }

  private static class DeleteFileGroup {
    private final List<DeleteFile> deleteFiles;
    private final PartitionSpec spec;

    DeleteFileGroup(List<DeleteFile> deleteFiles, PartitionSpec spec) {
      this.deleteFiles = ImmutableList.copyOf(deleteFiles);
      this.spec = spec;
    }

    List<DeleteFile> deleteFiles() {
      return deleteFiles;
    }

    PartitionSpec spec() {
      return spec;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      DeleteFileGroup that = (DeleteFileGroup) o;
      Set<String> thisPaths =
          deleteFiles.stream().map(f -> f.path().toString()).collect(Collectors.toSet());
      Set<String> thatPaths =
          that.deleteFiles.stream().map(f -> f.path().toString()).collect(Collectors.toSet());
      return thisPaths.equals(thatPaths) && spec.specId() == that.spec.specId();
    }

    @Override
    public int hashCode() {
      Set<String> paths =
          deleteFiles.stream().map(f -> f.path().toString()).collect(Collectors.toSet());
      return paths.hashCode() * 31 + spec.specId();
    }
  }
}
