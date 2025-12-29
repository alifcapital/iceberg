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

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * File cleanup strategy for snapshot expiration which determines, via an in-memory reference set,
 * metadata and data files that are not reachable given the previous and current table states.
 */
class ReachableFileCleanup extends FileCleanupStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(ReachableFileCleanup.class);

  ReachableFileCleanup(
      FileIO fileIO,
      ExecutorService deleteExecutorService,
      ExecutorService planExecutorService,
      BiConsumer<String, String> deleteFunc) {
    super(fileIO, deleteExecutorService, planExecutorService, deleteFunc);
  }

  @Override
  public void cleanFiles(
      TableMetadata beforeExpiration,
      TableMetadata afterExpiration,
      ExpireSnapshots.CleanupLevel cleanupLevel) {
    long startTime = System.nanoTime();
    LOG.info("Starting reachable file cleanup");

    if (ExpireSnapshots.CleanupLevel.NONE == cleanupLevel) {
      LOG.info("Nothing to clean.");
      return;
    }

    Set<String> manifestListsToDelete = Sets.newHashSet();

    Set<Snapshot> snapshotsBeforeExpiration = Sets.newHashSet(beforeExpiration.snapshots());
    Set<Snapshot> snapshotsAfterExpiration = Sets.newHashSet(afterExpiration.snapshots());
    Set<Snapshot> expiredSnapshots = Sets.newHashSet();
    for (Snapshot snapshot : snapshotsBeforeExpiration) {
      if (!snapshotsAfterExpiration.contains(snapshot)) {
        expiredSnapshots.add(snapshot);
        if (snapshot.manifestListLocation() != null) {
          manifestListsToDelete.add(snapshot.manifestListLocation());
        }
      }
    }

    long readManifestsStartTime = System.nanoTime();
    LOG.info("Starting to read manifests from {} expired snapshots", expiredSnapshots.size());
    Set<ManifestFile> deletionCandidates = readManifests(expiredSnapshots);
    long readManifestsEndTime = System.nanoTime();
    LOG.info("Completed reading manifests in {} ms, found {} candidates for deletion",
        (readManifestsEndTime - readManifestsStartTime) / 1_000_000, deletionCandidates.size());

    if (!deletionCandidates.isEmpty()) {
      Set<ManifestFile> currentManifests = ConcurrentHashMap.newKeySet();

      long pruneStartTime = System.nanoTime();
      LOG.info("Starting to prune {} manifests", deletionCandidates.size());
      Set<ManifestFile> manifestsToDelete =
          pruneReferencedManifests(
              snapshotsAfterExpiration, deletionCandidates, currentManifests::add);
      long pruneEndTime = System.nanoTime();
      LOG.info("Completed pruning in {} ms, {} manifests remain after pruning",
          (pruneEndTime - pruneStartTime) / 1_000_000, manifestsToDelete.size());

      if (!manifestsToDelete.isEmpty()) {
        if (ExpireSnapshots.CleanupLevel.ALL == cleanupLevel) {
          long findFilesStartTime = System.nanoTime();
          LOG.info("Starting to find files to delete from {} manifests", manifestsToDelete.size());
          Set<String> dataFilesToDelete = findFilesToDelete(manifestsToDelete, currentManifests);
          LOG.info("Completed finding files in {} ms, found {} files to delete",
              (System.nanoTime() - findFilesStartTime) / 1_000_000, dataFilesToDelete.size());

          long deleteStartTime = System.nanoTime();
          boolean supportsBulkDeletes = fileIO instanceof SupportsBulkOperations;
          LOG.info("File IO {} bulk deletes", supportsBulkDeletes ? "supports" : "does not support");

          deleteFiles(dataFilesToDelete, "data");
          LOG.info("Deleted {} data files in {} ms", dataFilesToDelete.size(),
              (System.nanoTime() - deleteStartTime) / 1_000_000);
        }

        Set<String> manifestPathsToDelete =
            manifestsToDelete.stream().map(ManifestFile::path).collect(Collectors.toSet());
        long manifestDeleteStartTime = System.nanoTime();
        deleteFiles(manifestPathsToDelete, "manifest");
        long manifestDeleteEndTime = System.nanoTime();
        LOG.info("Deleted {} manifest files in {} ms", manifestPathsToDelete.size(),
            (manifestDeleteEndTime - manifestDeleteStartTime) / 1_000_000);
      }
    }

    long manifestListDeleteStartTime = System.nanoTime();
    deleteFiles(manifestListsToDelete, "manifest list");
    LOG.info("Deleted {} manifest list files in {} ms", manifestListsToDelete.size(),
        (System.nanoTime() - manifestListDeleteStartTime) / 1_000_000);

    if (hasAnyStatisticsFiles(beforeExpiration)) {
      long statsDeleteStartTime = System.nanoTime();
      Set<String> expiredStatisticsFiles = expiredStatisticsFilesLocations(beforeExpiration, afterExpiration);
      deleteFiles(expiredStatisticsFiles, "statistics files");
      LOG.info("Deleted {} statistics files in {} ms", expiredStatisticsFiles.size(),
          (System.nanoTime() - statsDeleteStartTime) / 1_000_000);
    }

    long totalTime = System.nanoTime() - startTime;
    LOG.info("Completed reachable file cleanup in {} ms", totalTime / 1_000_000);
  }

  private Set<ManifestFile> pruneReferencedManifests(
      Set<Snapshot> snapshots,
      Set<ManifestFile> deletionCandidates,
      Consumer<ManifestFile> currentManifestCallback) {
    Set<ManifestFile> candidateSet = ConcurrentHashMap.newKeySet();
    candidateSet.addAll(deletionCandidates);

    long pruneStartTime = System.nanoTime();
    LOG.info("Starting to prune referenced manifests from {} snapshots", snapshots.size());
    Tasks.foreach(snapshots)
        .retry(3)
        .stopOnFailure()
        .throwFailureWhenFinished()
        .executeWith(planExecutorService)
        .onFailure(
            (snapshot, exc) ->
                LOG.warn(
                    "Failed to determine manifests for snapshot {}", snapshot.snapshotId(), exc))
        .run(
            snapshot -> {
              try (CloseableIterable<ManifestFile> manifestFiles = readManifests(snapshot)) {
                for (ManifestFile manifestFile : manifestFiles) {
                  candidateSet.remove(manifestFile);
                  if (candidateSet.isEmpty()) {
                    return;
                  }

                  currentManifestCallback.accept(manifestFile.copy());
                }
              } catch (IOException e) {
                throw new RuntimeIOException(
                    e, "Failed to close manifest list: %s", snapshot.manifestListLocation());
              }
            });
    long pruneEndTime = System.nanoTime();
    LOG.info("Completed pruning referenced manifests in {} ms, {} candidates remain",
        (pruneEndTime - pruneStartTime) / 1_000_000, candidateSet.size());

    return candidateSet;
  }

  private Set<ManifestFile> readManifests(Set<Snapshot> snapshots) {
    Set<ManifestFile> manifestFiles = ConcurrentHashMap.newKeySet();

    long readStartTime = System.nanoTime();
    LOG.info("Starting to read manifests from {} snapshots", snapshots.size());
    Tasks.foreach(snapshots)
        .retry(3)
        .stopOnFailure()
        .throwFailureWhenFinished()
        .executeWith(planExecutorService)
        .onFailure(
            (snapshot, exc) ->
                LOG.warn(
                    "Failed to determine manifests for snapshot {}", snapshot.snapshotId(), exc))
        .run(
            snapshot -> {
              try (CloseableIterable<ManifestFile> manifests = readManifests(snapshot)) {
                for (ManifestFile manifestFile : manifests) {
                  manifestFiles.add(manifestFile.copy());
                }
              } catch (IOException e) {
                throw new RuntimeIOException(
                    e, "Failed to close manifest list: %s", snapshot.manifestListLocation());
              }
            });
    long readEndTime = System.nanoTime();
    LOG.info("Completed reading manifests in {} ms, found {} manifests",
        (readEndTime - readStartTime) / 1_000_000, manifestFiles.size());

    return manifestFiles;
  }

  // Helper to determine data files to delete
  private Set<String> findFilesToDelete(
      Set<ManifestFile> manifestFilesToDelete, Set<ManifestFile> currentManifestFiles) {
    Set<String> filesToDelete = ConcurrentHashMap.newKeySet();

    Tasks.foreach(manifestFilesToDelete)
        .retry(3)
        .suppressFailureWhenFinished()
        .executeWith(planExecutorService)
        .onFailure(
            (item, exc) ->
                LOG.warn(
                    "Failed to determine live files in manifest {}. Retrying", item.path(), exc))
        .run(
            manifest -> {
              try (CloseableIterable<String> paths = ManifestFiles.readPaths(manifest, fileIO)) {
                paths.forEach(filesToDelete::add);
              } catch (IOException e) {
                throw new RuntimeIOException(e, "Failed to read manifest file: %s", manifest);
              }
            });

    if (filesToDelete.isEmpty()) {
      return filesToDelete;
    }

    try {
      Tasks.foreach(currentManifestFiles)
          .retry(3)
          .stopOnFailure()
          .throwFailureWhenFinished()
          .executeWith(planExecutorService)
          .onFailure(
              (item, exc) ->
                  LOG.warn(
                      "Failed to determine live files in manifest {}. Retrying", item.path(), exc))
          .run(
              manifest -> {
                if (filesToDelete.isEmpty()) {
                  return;
                }

                // Remove all the live files from the candidate deletion set
                try (CloseableIterable<String> paths = ManifestFiles.readPaths(manifest, fileIO)) {
                  paths.forEach(filesToDelete::remove);
                } catch (IOException e) {
                  throw new RuntimeIOException(e, "Failed to read manifest file: %s", manifest);
                }
              });

    } catch (Throwable e) {
      LOG.warn("Failed to list all reachable files", e);
      return Sets.newHashSet();
    }

    return filesToDelete;
  }
}
