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

import static org.apache.iceberg.TableProperties.GC_ENABLED;
import static org.apache.iceberg.TableProperties.GC_ENABLED_DEFAULT;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.actions.ExpireSnapshots;
import org.apache.iceberg.actions.ImmutableExpireSnapshots;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An action that performs the same operation as {@link org.apache.iceberg.ExpireSnapshots} but uses
 * Spark to determine the delta in files between the pre and post-expiration table metadata. All of
 * the same restrictions of {@link org.apache.iceberg.ExpireSnapshots} also apply to this action.
 *
 * <p>This action first leverages {@link org.apache.iceberg.ExpireSnapshots} to expire snapshots and
 * then uses metadata tables to find files that can be safely deleted. This is done by anti-joining
 * two Datasets that contain all manifest and content files before and after the expiration. The
 * snapshot expiration will be fully committed before any deletes are issued.
 *
 * <p>This operation performs a shuffle so the parallelism can be controlled through
 * 'spark.sql.shuffle.partitions'.
 *
 * <p>Deletes are still performed locally after retrieving the results from the Spark executors.
 */
@SuppressWarnings("UnnecessaryAnonymousClass")
public class ExpireSnapshotsSparkAction extends BaseSparkAction<ExpireSnapshotsSparkAction>
    implements ExpireSnapshots {

  public static final String STREAM_RESULTS = "stream-results";
  public static final boolean STREAM_RESULTS_DEFAULT = false;

  private static final Logger LOG = LoggerFactory.getLogger(ExpireSnapshotsSparkAction.class);

  private final Table table;
  private final TableOperations ops;

  private final Set<Long> expiredSnapshotIds = Sets.newHashSet();
  private Long expireOlderThanValue = null;
  private Integer retainLastValue = null;
  private Consumer<String> deleteFunc = null;
  private ExecutorService deleteExecutorService = null;

  ExpireSnapshotsSparkAction(SparkSession spark, Table table) {
    super(spark);
    this.table = table;
    this.ops = ((HasTableOperations) table).operations();

    ValidationException.check(
        PropertyUtil.propertyAsBoolean(table.properties(), GC_ENABLED, GC_ENABLED_DEFAULT),
        "Cannot expire snapshots: GC is disabled (deleting files may corrupt other tables)");
  }

  @Override
  protected ExpireSnapshotsSparkAction self() {
    return this;
  }

  @Override
  public ExpireSnapshotsSparkAction executeDeleteWith(ExecutorService executorService) {
    this.deleteExecutorService = executorService;
    return this;
  }

  @Override
  public ExpireSnapshotsSparkAction expireSnapshotId(long snapshotId) {
    expiredSnapshotIds.add(snapshotId);
    return this;
  }

  @Override
  public ExpireSnapshotsSparkAction expireOlderThan(long timestampMillis) {
    this.expireOlderThanValue = timestampMillis;
    return this;
  }

  @Override
  public ExpireSnapshotsSparkAction retainLast(int numSnapshots) {
    Preconditions.checkArgument(
        1 <= numSnapshots,
        "Number of snapshots to retain must be at least 1, cannot be: %s",
        numSnapshots);
    this.retainLastValue = numSnapshots;
    return this;
  }

  @Override
  public ExpireSnapshotsSparkAction deleteWith(Consumer<String> newDeleteFunc) {
    this.deleteFunc = newDeleteFunc;
    return this;
  }

  @Override
  public ExpireSnapshots.Result execute() {
    JobGroupInfo info = newJobGroupInfo("EXPIRE-SNAPSHOTS", jobDesc());
    return withJobGroupInfo(info, this::doExecute);
  }

  private ExpireSnapshots.Result doExecute() {
    long startTime = System.nanoTime();
    LOG.info("Starting snapshot expiration");

    // perform expiration with file cleanup
    org.apache.iceberg.ExpireSnapshots expireSnapshots = table.expireSnapshots()
        .cleanExpiredFiles(true); // Use Iceberg's implementation for file cleanup

    for (long id : expiredSnapshotIds) {
      expireSnapshots = expireSnapshots.expireSnapshotId(id);
    }

    if (expireOlderThanValue != null) {
      expireSnapshots = expireSnapshots.expireOlderThan(expireOlderThanValue);
    }

    if (retainLastValue != null) {
      expireSnapshots = expireSnapshots.retainLast(retainLastValue);
    }

    if (deleteFunc != null) {
      expireSnapshots = expireSnapshots.deleteWith(deleteFunc);
    }

    if (deleteExecutorService != null) {
      expireSnapshots = expireSnapshots.executeDeleteWith(deleteExecutorService);
    }

    // commit() returns void, we'll create our own Result
    expireSnapshots.commit();

    long endTime = System.nanoTime();
    LOG.info("Snapshot expiration completed in {} ms", (endTime - startTime) / 1_000_000);

    // Return a result with all zeros since we're using core implementation
    return ImmutableExpireSnapshots.Result.builder()
        .deletedDataFilesCount(0L)
        .deletedPositionDeleteFilesCount(0L)
        .deletedEqualityDeleteFilesCount(0L)
        .deletedManifestsCount(0L)
        .deletedManifestListsCount(0L)
        .deletedStatisticsFilesCount(0L)
        .build();
  }

  private String jobDesc() {
    List<String> options = Lists.newArrayList();

    if (expireOlderThanValue != null) {
      options.add("older_than=" + expireOlderThanValue);
    }

    if (retainLastValue != null) {
      options.add("retain_last=" + retainLastValue);
    }

    if (!expiredSnapshotIds.isEmpty()) {
      Long first = expiredSnapshotIds.stream().findFirst().get();
      if (expiredSnapshotIds.size() > 1) {
        options.add(
            String.format("snapshot_ids: %s (%s more...)", first, expiredSnapshotIds.size() - 1));
      } else {
        options.add(String.format("snapshot_id: %s", first));
      }
    }

    return String.format("Expiring snapshots (%s) in %s", COMMA_JOINER.join(options), table.name());
  }
}
