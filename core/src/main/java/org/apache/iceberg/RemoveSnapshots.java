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
package org.apache.iceberg;

import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.GC_ENABLED;
import static org.apache.iceberg.TableProperties.GC_ENABLED_DEFAULT;
import static org.apache.iceberg.TableProperties.MAX_REF_AGE_MS;
import static org.apache.iceberg.TableProperties.MAX_REF_AGE_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.MAX_SNAPSHOT_AGE_MS;
import static org.apache.iceberg.TableProperties.MAX_SNAPSHOT_AGE_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.MIN_SNAPSHOTS_TO_KEEP;
import static org.apache.iceberg.TableProperties.MIN_SNAPSHOTS_TO_KEEP_DEFAULT;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("UnnecessaryAnonymousClass")
class RemoveSnapshots implements ExpireSnapshots {
  private static final Logger LOG = LoggerFactory.getLogger(RemoveSnapshots.class);

  // Creates an executor service that runs each task in the thread that invokes execute/submit.
  private static final ExecutorService DEFAULT_DELETE_EXECUTOR_SERVICE =
      MoreExecutors.newDirectExecutorService();

  private final TableOperations ops;
  private final Set<Long> idsToRemove = Sets.newHashSet();
  private final long now;
  private final long defaultMaxRefAgeMs;
  private TableMetadata base;
  private long defaultExpireOlderThan;
  private int defaultMinNumSnapshots;
  private BiConsumer<String, String> deleteFunc = null;
  private ExecutorService deleteExecutorService = DEFAULT_DELETE_EXECUTOR_SERVICE;
  private ExecutorService planExecutorService;
  private Boolean incrementalCleanup;
  private boolean specifiedSnapshotId = false;
  private boolean cleanExpiredMetadata = false;
  private boolean cleanExpiredFiles = true;
  private CleanupLevel cleanupLevel = CleanupLevel.ALL;

  RemoveSnapshots(TableOperations ops) {
    this.ops = ops;
    this.base = ops.current();
    ValidationException.check(
        PropertyUtil.propertyAsBoolean(base.properties(), GC_ENABLED, GC_ENABLED_DEFAULT),
        "Cannot expire snapshots: GC is disabled (deleting files may corrupt other tables)");

    long defaultMaxSnapshotAgeMs =
        PropertyUtil.propertyAsLong(
            base.properties(), MAX_SNAPSHOT_AGE_MS, MAX_SNAPSHOT_AGE_MS_DEFAULT);

    this.now = System.currentTimeMillis();
    this.defaultExpireOlderThan = now - defaultMaxSnapshotAgeMs;
    this.defaultMinNumSnapshots =
        PropertyUtil.propertyAsInt(
            base.properties(), MIN_SNAPSHOTS_TO_KEEP, MIN_SNAPSHOTS_TO_KEEP_DEFAULT);

    this.defaultMaxRefAgeMs =
        PropertyUtil.propertyAsLong(base.properties(), MAX_REF_AGE_MS, MAX_REF_AGE_MS_DEFAULT);
  }

  @Override
  public ExpireSnapshots cleanExpiredFiles(boolean clean) {
    Preconditions.checkArgument(
        cleanupLevel == CleanupLevel.ALL,
        "Cannot set cleanExpiredFiles when cleanupLevel has already been set to: %s",
        cleanupLevel);
    this.cleanExpiredFiles = clean;
    this.cleanupLevel = clean ? CleanupLevel.ALL : CleanupLevel.NONE;
    return this;
  }

  @Override
  public ExpireSnapshots expireSnapshotId(long expireSnapshotId) {
    LOG.info("Expiring snapshot with id: {}", expireSnapshotId);
    idsToRemove.add(expireSnapshotId);
    specifiedSnapshotId = true;
    return this;
  }

  @Override
  public ExpireSnapshots expireOlderThan(long timestampMillis) {
    LOG.info(
        "Expiring snapshots older than: {} ({})",
        DateTimeUtil.formatTimestampMillis(timestampMillis),
        timestampMillis);
    this.defaultExpireOlderThan = timestampMillis;
    return this;
  }

  @Override
  public ExpireSnapshots retainLast(int numSnapshots) {
    Preconditions.checkArgument(
        1 <= numSnapshots,
        "Number of snapshots to retain must be at least 1, cannot be: %s",
        numSnapshots);
    this.defaultMinNumSnapshots = numSnapshots;
    return this;
  }

  @Override
  public ExpireSnapshots deleteWith(BiConsumer<String, String> newDeleteFunc) {
    this.deleteFunc = newDeleteFunc;
    return this;
  }

  @Override
  public ExpireSnapshots executeDeleteWith(ExecutorService executorService) {
    this.deleteExecutorService = executorService;
    return this;
  }

  @Override
  public ExpireSnapshots planWith(ExecutorService executorService) {
    this.planExecutorService = executorService;
    return this;
  }

  protected ExecutorService planExecutorService() {
    if (planExecutorService == null) {
      this.planExecutorService = ThreadPools.getWorkerPool();
    }

    return planExecutorService;
  }

  @Override
  public ExpireSnapshots cleanExpiredMetadata(boolean clean) {
    this.cleanExpiredMetadata = clean;
    return this;
  }

  @Override
  public ExpireSnapshots cleanupLevel(CleanupLevel level) {
    Preconditions.checkArgument(null != level, "Invalid cleanup level: null");
    Preconditions.checkArgument(
        cleanExpiredFiles || level == CleanupLevel.NONE,
        "Cannot set cleanupLevel to %s when cleanExpiredFiles was explicitly set to false",
        level);
    this.cleanupLevel = level;
    return this;
  }

  @Override
  public List<Snapshot> apply() {
    TableMetadata updated = internalApply();
    List<Snapshot> removed = Lists.newArrayList(base.snapshots());
    removed.removeAll(updated.snapshots());

    return removed;
  }

  private TableMetadata internalApply() {
    this.base = ops.refresh();
    // attempt to clean expired metadata even if there are no snapshots to expire
    // table metadata builder takes care of the case when this should actually be a no-op
    if (base.snapshots().isEmpty() && !cleanExpiredMetadata) {
      return base;
    }

    long startTime = System.nanoTime();
    LOG.info("Starting metadata update phase");

    Set<Long> idsToRetain = Sets.newHashSet();
    // Identify refs that should be removed
    long refsStartTime = System.nanoTime();
    LOG.info("Starting to compute retained refs");
    Map<String, SnapshotRef> retainedRefs = computeRetainedRefs(base.refs());
    Map<Long, List<String>> retainedIdToRefs = Maps.newHashMap();
    for (Map.Entry<String, SnapshotRef> retainedRefEntry : retainedRefs.entrySet()) {
      long snapshotId = retainedRefEntry.getValue().snapshotId();
      retainedIdToRefs.putIfAbsent(snapshotId, Lists.newArrayList());
      retainedIdToRefs.get(snapshotId).add(retainedRefEntry.getKey());
      idsToRetain.add(snapshotId);
    }
    long refsEndTime = System.nanoTime();
    LOG.info("Completed computing retained refs in {} ms", (refsEndTime - refsStartTime) / 1_000_000);

    for (long idToRemove : idsToRemove) {
      List<String> refsForId = retainedIdToRefs.get(idToRemove);
      Preconditions.checkArgument(
          refsForId == null,
          "Cannot expire %s. Still referenced by refs: %s",
          idToRemove,
          refsForId);
    }

    long branchStartTime = System.nanoTime();
    LOG.info("Starting to compute retained branch snapshots");
    idsToRetain.addAll(computeAllBranchSnapshotsToRetain(retainedRefs.values()));
    long branchEndTime = System.nanoTime();
    LOG.info("Completed computing retained branch snapshots in {} ms", (branchEndTime - branchStartTime) / 1_000_000);

    long unrefStartTime = System.nanoTime();
    LOG.info("Starting to compute unreferenced snapshots to retain");
    idsToRetain.addAll(unreferencedSnapshotsToRetain(retainedRefs.values()));
    long unrefEndTime = System.nanoTime();
    LOG.info("Completed computing unreferenced snapshots in {} ms", (unrefEndTime - unrefStartTime) / 1_000_000);

    TableMetadata.Builder updatedMetaBuilder = TableMetadata.buildFrom(base);

    long removeRefsStartTime = System.nanoTime();
    LOG.info("Starting to remove expired refs");
    base.refs().keySet().stream()
        .filter(ref -> !retainedRefs.containsKey(ref))
        .forEach(updatedMetaBuilder::removeRef);
    long removeRefsEndTime = System.nanoTime();
    LOG.info("Completed removing expired refs in {} ms", (removeRefsEndTime - removeRefsStartTime) / 1_000_000);

    long removeSnapsStartTime = System.nanoTime();
    LOG.info("Starting to remove expired snapshots");
    base.snapshots().stream()
        .map(Snapshot::snapshotId)
        .filter(snapshot -> !idsToRetain.contains(snapshot))
        .forEach(idsToRemove::add);
    updatedMetaBuilder.removeSnapshots(idsToRemove);
    long removeSnapsEndTime = System.nanoTime();
    LOG.info("Completed removing {} expired snapshots in {} ms",
        idsToRemove.size(), (removeSnapsEndTime - removeSnapsStartTime) / 1_000_000);

    long totalTime = System.nanoTime() - startTime;
    LOG.info("Completed metadata update phase in {} ms", totalTime / 1_000_000);

    if (cleanExpiredMetadata) {
      Set<Integer> reachableSpecs = Sets.newConcurrentHashSet();
      reachableSpecs.add(base.defaultSpecId());
      Set<Integer> reachableSchemas = Sets.newConcurrentHashSet();
      reachableSchemas.add(base.currentSchemaId());

      Tasks.foreach(idsToRetain)
          .executeWith(planExecutorService())
          .run(
              snapshotId -> {
                Snapshot snapshot = base.snapshot(snapshotId);
                snapshot.allManifests(ops.io()).stream()
                    .map(ManifestFile::partitionSpecId)
                    .forEach(reachableSpecs::add);
                reachableSchemas.add(snapshot.schemaId());
              });

      Set<Integer> specsToRemove =
          base.specs().stream()
              .map(PartitionSpec::specId)
              .filter(specId -> !reachableSpecs.contains(specId))
              .collect(Collectors.toSet());
      updatedMetaBuilder.removeSpecs(specsToRemove);

      Set<Integer> schemasToRemove =
          base.schemas().stream()
              .map(Schema::schemaId)
              .filter(schemaId -> !reachableSchemas.contains(schemaId))
              .collect(Collectors.toSet());
      updatedMetaBuilder.removeSchemas(schemasToRemove);
    }

    return updatedMetaBuilder.build();
  }

  private Map<String, SnapshotRef> computeRetainedRefs(Map<String, SnapshotRef> refs) {
    Map<String, SnapshotRef> retainedRefs = Maps.newHashMap();
    for (Map.Entry<String, SnapshotRef> refEntry : refs.entrySet()) {
      String name = refEntry.getKey();
      SnapshotRef ref = refEntry.getValue();
      if (name.equals(SnapshotRef.MAIN_BRANCH)) {
        retainedRefs.put(name, ref);
        continue;
      }

      Snapshot snapshot = base.snapshot(ref.snapshotId());
      long maxRefAgeMs = ref.maxRefAgeMs() != null ? ref.maxRefAgeMs() : defaultMaxRefAgeMs;
      if (snapshot != null) {
        long refAgeMs = now - snapshot.timestampMillis();
        if (refAgeMs <= maxRefAgeMs) {
          retainedRefs.put(name, ref);
        }
      } else {
        LOG.warn("Removing invalid ref {}: snapshot {} does not exist", name, ref.snapshotId());
      }
    }

    return retainedRefs;
  }

  private Set<Long> computeAllBranchSnapshotsToRetain(Collection<SnapshotRef> refs) {
    Set<Long> branchSnapshotsToRetain = Sets.newHashSet();
    for (SnapshotRef ref : refs) {
      if (ref.isBranch()) {
        long expireSnapshotsOlderThan =
            ref.maxSnapshotAgeMs() != null ? now - ref.maxSnapshotAgeMs() : defaultExpireOlderThan;
        int minSnapshotsToKeep =
            ref.minSnapshotsToKeep() != null ? ref.minSnapshotsToKeep() : defaultMinNumSnapshots;
        branchSnapshotsToRetain.addAll(
            computeBranchSnapshotsToRetain(
                ref.snapshotId(), expireSnapshotsOlderThan, minSnapshotsToKeep));
      }
    }

    return branchSnapshotsToRetain;
  }

  private Set<Long> computeBranchSnapshotsToRetain(
      long snapshot, long expireSnapshotsOlderThan, int minSnapshotsToKeep) {
    Set<Long> idsToRetain = Sets.newHashSet();
    for (Snapshot ancestor : SnapshotUtil.ancestorsOf(snapshot, base::snapshot)) {
      if (idsToRetain.size() < minSnapshotsToKeep
          || ancestor.timestampMillis() >= expireSnapshotsOlderThan) {
        idsToRetain.add(ancestor.snapshotId());
      } else {
        return idsToRetain;
      }
    }

    return idsToRetain;
  }

  private Set<Long> unreferencedSnapshotsToRetain(Collection<SnapshotRef> refs) {
    Set<Long> referencedSnapshots = Sets.newHashSet();
    for (SnapshotRef ref : refs) {
      if (ref.isBranch()) {
        for (Snapshot snapshot : SnapshotUtil.ancestorsOf(ref.snapshotId(), base::snapshot)) {
          referencedSnapshots.add(snapshot.snapshotId());
        }
      } else {
        referencedSnapshots.add(ref.snapshotId());
      }
    }

    Set<Long> snapshotsToRetain = Sets.newHashSet();
    for (Snapshot snapshot : base.snapshots()) {
      if (!referencedSnapshots.contains(snapshot.snapshotId())
          && // unreferenced
          snapshot.timestampMillis() >= defaultExpireOlderThan) { // not old enough to expire
        snapshotsToRetain.add(snapshot.snapshotId());
      }
    }

    return snapshotsToRetain;
  }

  @Override
  public void commit() {
    long startTime = System.nanoTime();
    LOG.info("Starting commit");

    Tasks.foreach(ops)
        .retry(base.propertyAsInt(COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))
        .exponentialBackoff(
            base.propertyAsInt(COMMIT_MIN_RETRY_WAIT_MS, COMMIT_MIN_RETRY_WAIT_MS_DEFAULT),
            base.propertyAsInt(COMMIT_MAX_RETRY_WAIT_MS, COMMIT_MAX_RETRY_WAIT_MS_DEFAULT),
            base.propertyAsInt(COMMIT_TOTAL_RETRY_TIME_MS, COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT),
            2.0 /* exponential */)
        .onlyRetryOn(CommitFailedException.class)
        .run(
            item -> {
              TableMetadata updated = internalApply();
              ops.commit(base, updated);
            });

    long commitEndTime = System.nanoTime();
    LOG.info("Completed committing metadata changes in {} ms", (commitEndTime - startTime) / 1_000_000);
    LOG.info(
        "Committed snapshot changes and prepare to clean up files at level={}",
        cleanupLevel.name());

    if (CleanupLevel.NONE != cleanupLevel && !base.snapshots().isEmpty()) {
      long cleanupStartTime = System.nanoTime();
      LOG.info("Starting expired files cleanup");
      cleanExpiredSnapshots();
      LOG.info("Completed expired files cleanup in {} ms", (System.nanoTime() - cleanupStartTime) / 1_000_000);
    }

    long totalTime = System.nanoTime() - startTime;
    LOG.info("Completed expire snapshots operation in {} ms", totalTime / 1_000_000);
  }

  ExpireSnapshots withIncrementalCleanup(boolean useIncrementalCleanup) {
    this.incrementalCleanup = useIncrementalCleanup;
    return this;
  }

  private void cleanExpiredSnapshots() {
    TableMetadata current = ops.refresh();

    if (Boolean.TRUE.equals(incrementalCleanup)) {
      validateCleanupCanBeIncremental(current);
    } else if (incrementalCleanup == null) {
      incrementalCleanup =
          !specifiedSnapshotId
              && !hasRemovedNonMainAncestors(base, current)
              && !hasNonMainSnapshots(current);
    }

    LOG.info(
        "Cleaning up expired files (local, {})", incrementalCleanup ? "incremental" : "reachable");

    FileCleanupStrategy cleanupStrategy =
        incrementalCleanup
            ? new IncrementalFileCleanup(
                ops.io(), deleteExecutorService, planExecutorService(), deleteFunc)
            : new ReachableFileCleanup(
                ops.io(), deleteExecutorService, planExecutorService(), deleteFunc);

    cleanupStrategy.cleanFiles(base, current, cleanupLevel);
  }

  private void validateCleanupCanBeIncremental(TableMetadata current) {
    if (specifiedSnapshotId) {
      throw new UnsupportedOperationException(
          "Cannot clean files incrementally when snapshot IDs are specified");
    }

    if (hasRemovedNonMainAncestors(base, current)) {
      throw new UnsupportedOperationException(
          "Cannot incrementally clean files when snapshots outside of main ancestry were removed");
    }

    if (hasNonMainSnapshots(current)) {
      throw new UnsupportedOperationException(
          "Cannot incrementally clean files when there are snapshots outside of main");
    }
  }

  private boolean hasRemovedNonMainAncestors(
      TableMetadata beforeExpiration, TableMetadata afterExpiration) {
    Set<Long> mainAncestors = mainAncestors(beforeExpiration);
    for (Snapshot snapshotBeforeExpiration : beforeExpiration.snapshots()) {
      boolean removedSnapshot =
          afterExpiration.snapshot(snapshotBeforeExpiration.snapshotId()) == null;
      boolean snapshotInMainAncestry =
          mainAncestors.contains(snapshotBeforeExpiration.snapshotId());
      if (removedSnapshot && !snapshotInMainAncestry) {
        return true;
      }
    }

    return false;
  }

  private boolean hasNonMainSnapshots(TableMetadata metadata) {
    if (metadata.currentSnapshot() == null) {
      return !metadata.snapshots().isEmpty();
    }

    Set<Long> mainAncestors = mainAncestors(metadata);

    for (Snapshot snapshot : metadata.snapshots()) {
      if (!mainAncestors.contains(snapshot.snapshotId())) {
        return true;
      }
    }

    return false;
  }

  private Set<Long> mainAncestors(TableMetadata metadata) {
    Set<Long> ancestors = Sets.newHashSet();
    if (metadata.currentSnapshot() == null) {
      return ancestors;
    }

    for (Snapshot ancestor :
        SnapshotUtil.ancestorsOf(metadata.currentSnapshot().snapshotId(), metadata::snapshot)) {
      ancestors.add(ancestor.snapshotId());
    }

    return ancestors;
  }
}
