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

import java.io.IOException;
import java.net.URI;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ReachableFileUtil;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.actions.DeleteOrphanFiles;
import org.apache.iceberg.actions.ImmutableDeleteOrphanFiles;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.util.FileSystemWalker;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An action that removes orphan metadata, data and delete files by listing a given location and
 * comparing the actual files in that location with content and metadata files referenced by all
 * valid snapshots. The location must be accessible for listing via the Hadoop {@link FileSystem}.
 *
 * <p>By default, this action cleans up the table location returned by {@link Table#location()} and
 * removes unreachable files that are older than 3 days using {@link Table#io()}. The behavior can
 * be modified by passing a custom location to {@link #location} and a custom timestamp to {@link
 * #olderThan(long)}. For example, someone might point this action to the data folder to clean up
 * only orphan data files.
 *
 * <p>Configure an alternative delete method using {@link #deleteWith(Consumer)}.
 *
 * <p>For full control of the set of files being evaluated, use the {@link
 * #compareToFileList(Dataset)} argument. This skips the directory listing - any files in the
 * dataset provided which are not found in table metadata will be deleted, using the same {@link
 * Table#location()} and {@link #olderThan(long)} filtering as above.
 *
 * <p><em>Note:</em> It is dangerous to call this action with a short retention interval as it might
 * corrupt the state of the table if another operation is writing at the same time.
 */
public class DeleteOrphanFilesSparkAction extends BaseSparkAction<DeleteOrphanFilesSparkAction>
    implements DeleteOrphanFiles {

  private static final Logger LOG = LoggerFactory.getLogger(DeleteOrphanFilesSparkAction.class);
  private static final Map<String, String> EQUAL_SCHEMES_DEFAULT = ImmutableMap.of("s3n,s3a", "s3");

  private final Configuration hadoopConf;
  private final Table table;
  private Map<String, String> equalSchemes = flattenMap(EQUAL_SCHEMES_DEFAULT);
  private Map<String, String> equalAuthorities = Collections.emptyMap();
  private PrefixMismatchMode prefixMismatchMode = PrefixMismatchMode.ERROR;
  private String location;
  private long olderThanTimestamp = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(3);
  private Dataset<Row> compareToFileList;
  private Consumer<String> deleteFunc = null;
  private ExecutorService deleteExecutorService = null;
  private ExecutorService planExecutorService = null;
  private boolean usePrefixListing = false;

  DeleteOrphanFilesSparkAction(SparkSession spark, Table table) {
    super(spark);

    this.hadoopConf = spark.sessionState().newHadoopConf();
    this.table = table;
    this.location = table.location();
    // Auto-detect: use FileIO prefix listing if supported (e.g. S3), otherwise use Hadoop
    this.usePrefixListing = table.io() instanceof SupportsPrefixOperations;
    // Create executor for parallel manifest reading based on available processors
    int parallelism = Math.max(1, Runtime.getRuntime().availableProcessors());
    this.planExecutorService = Executors.newFixedThreadPool(parallelism);

    ValidationException.check(
        PropertyUtil.propertyAsBoolean(table.properties(), GC_ENABLED, GC_ENABLED_DEFAULT),
        "Cannot delete orphan files: GC is disabled (deleting files may corrupt other tables)");
  }

  @Override
  protected DeleteOrphanFilesSparkAction self() {
    return this;
  }

  @Override
  public DeleteOrphanFilesSparkAction executeDeleteWith(ExecutorService executorService) {
    this.deleteExecutorService = executorService;
    return this;
  }

  @Override
  public DeleteOrphanFilesSparkAction prefixMismatchMode(PrefixMismatchMode newPrefixMismatchMode) {
    this.prefixMismatchMode = newPrefixMismatchMode;
    return this;
  }

  @Override
  public DeleteOrphanFilesSparkAction equalSchemes(Map<String, String> newEqualSchemes) {
    this.equalSchemes = Maps.newHashMap();
    equalSchemes.putAll(flattenMap(EQUAL_SCHEMES_DEFAULT));
    equalSchemes.putAll(flattenMap(newEqualSchemes));
    return this;
  }

  @Override
  public DeleteOrphanFilesSparkAction equalAuthorities(Map<String, String> newEqualAuthorities) {
    this.equalAuthorities = Maps.newHashMap();
    equalAuthorities.putAll(flattenMap(newEqualAuthorities));
    return this;
  }

  @Override
  public DeleteOrphanFilesSparkAction location(String newLocation) {
    this.location = newLocation;
    return this;
  }

  @Override
  public DeleteOrphanFilesSparkAction olderThan(long newOlderThanTimestamp) {
    this.olderThanTimestamp = newOlderThanTimestamp;
    return this;
  }

  @Override
  public DeleteOrphanFilesSparkAction deleteWith(Consumer<String> newDeleteFunc) {
    this.deleteFunc = newDeleteFunc;
    return this;
  }

  public DeleteOrphanFilesSparkAction compareToFileList(Dataset<Row> files) {
    StructType schema = files.schema();

    StructField filePathField = schema.apply(FILE_PATH);
    Preconditions.checkArgument(
        filePathField.dataType() == DataTypes.StringType,
        "Invalid %s column: %s is not a string",
        FILE_PATH,
        filePathField.dataType());

    StructField lastModifiedField = schema.apply(LAST_MODIFIED);
    Preconditions.checkArgument(
        lastModifiedField.dataType() == DataTypes.TimestampType,
        "Invalid %s column: %s is not a timestamp",
        LAST_MODIFIED,
        lastModifiedField.dataType());

    this.compareToFileList = files;
    return this;
  }

  public DeleteOrphanFilesSparkAction usePrefixListing(boolean newUsePrefixListing) {
    this.usePrefixListing = newUsePrefixListing;
    return this;
  }

  private List<String> filteredCompareToFileList() {
    Dataset<Row> files = compareToFileList;
    if (location != null) {
      files = files.filter(files.col(FILE_PATH).startsWith(location));
    }
    return files
        .filter(files.col(LAST_MODIFIED).lt(new Timestamp(olderThanTimestamp)))
        .select(files.col(FILE_PATH))
        .as(Encoders.STRING())
        .collectAsList();
  }

  @Override
  public DeleteOrphanFiles.Result execute() {
    JobGroupInfo info = newJobGroupInfo("DELETE-ORPHAN-FILES", jobDesc());
    return withJobGroupInfo(info, this::doExecute);
  }

  private String jobDesc() {
    List<String> options = Lists.newArrayList();
    options.add("older_than=" + olderThanTimestamp);
    if (location != null) {
      options.add("location=" + location);
    }
    String optionsAsString = COMMA_JOINER.join(options);
    return String.format("Deleting orphan files (%s) from %s", optionsAsString, table.name());
  }

  private void deleteBulk(SupportsBulkOperations io, List<String> paths) {
    try {
      io.deleteFiles(paths);
      LOG.info("Deleted {} files using bulk deletes", paths.size());
    } catch (BulkDeletionFailureException e) {
      int deletedFilesCount = paths.size() - e.numberFailedObjects();
      LOG.warn(
          "Deleted only {} of {} files using bulk deletes", deletedFilesCount, paths.size(), e);
    }
  }

  private DeleteOrphanFiles.Result doExecute() {
    long startTime = System.nanoTime();
    // All operations happen on the driver to avoid serialization overhead
    // and spark.driver.maxResultSize issues

    try {
      LOG.info("Starting orphan file detection for table {}", table.name());

      // 1. List actual files on filesystem
      long listStartTime = System.nanoTime();
      LOG.info("Listing actual files from location: {}", location);
      Map<String, String> actualFiles = listActualFiles();
      long listEndTime = System.nanoTime();
      LOG.info("Found {} actual files in {} ms", actualFiles.size(),
          (listEndTime - listStartTime) / 1_000_000);

      // 2. Collect valid files from metadata
      Set<String> validFilePaths = collectValidFilePaths();

      // 3. Summary comparison
      LOG.info("=== File comparison summary ===");
      LOG.info("Files on filesystem: {}", actualFiles.size());
      LOG.info("Valid files in metadata: {}", validFilePaths.size());

      // 4. Find orphans
      long findStartTime = System.nanoTime();
      List<String> orphanFiles =
          findOrphanFiles(actualFiles, validFilePaths, prefixMismatchMode, equalSchemes, equalAuthorities);
      long findEndTime = System.nanoTime();
      LOG.info("Found {} orphan files in {} ms", orphanFiles.size(),
          (findEndTime - findStartTime) / 1_000_000);

      // 5. Delete orphans
      if (!orphanFiles.isEmpty()) {
        long deleteStartTime = System.nanoTime();
        if (deleteFunc == null && table.io() instanceof SupportsBulkOperations) {
          deleteBulk((SupportsBulkOperations) table.io(), orphanFiles);
        } else {
          Tasks.Builder<String> deleteTasks =
              Tasks.foreach(orphanFiles)
                  .noRetry()
                  .executeWith(deleteExecutorService)
                  .suppressFailureWhenFinished()
                  .onFailure((file, exc) -> LOG.warn("Failed to delete file: {}", file, exc));

          if (deleteFunc == null) {
            LOG.info(
                "Table IO {} does not support bulk operations. Using non-bulk deletes.",
                table.io().getClass().getName());
            deleteTasks.run(table.io()::deleteFile);
          } else {
            LOG.info("Custom delete function provided. Using non-bulk deletes");
            deleteTasks.run(deleteFunc::accept);
          }
        }
        long deleteEndTime = System.nanoTime();
        LOG.info("Deleted {} orphan files in {} ms", orphanFiles.size(),
            (deleteEndTime - deleteStartTime) / 1_000_000);
      }

      long totalTime = System.nanoTime() - startTime;
      LOG.info("Completed orphan file cleanup in {} ms", totalTime / 1_000_000);

      return ImmutableDeleteOrphanFiles.Result.builder().orphanFileLocations(orphanFiles).build();
    } finally {
      if (planExecutorService != null) {
        planExecutorService.shutdown();
      }
    }
  }

  /**
   * Collects all valid file paths from table metadata.
   * Iterates through snapshots and manifests with progress logging.
   * Includes content files, manifests, manifest lists, and other metadata files.
   */
  private Set<String> collectValidFilePaths() {
    long startTime = System.nanoTime();
    Set<String> validPaths = ConcurrentHashMap.newKeySet();

    // 1. Add metadata files (quick, on driver)
    List<String> manifestLists = ReachableFileUtil.manifestListLocations(table);
    validPaths.addAll(manifestLists);

    Set<String> metadataFiles = ReachableFileUtil.metadataFileLocations(table, false);
    validPaths.addAll(metadataFiles);

    validPaths.add(ReachableFileUtil.versionHintLocation(table));

    List<String> statisticsFiles = ReachableFileUtil.statisticsFilesLocations(table);
    validPaths.addAll(statisticsFiles);

    LOG.info("Added {} metadata files (manifest lists: {}, metadata: {}, statistics: {})",
        manifestLists.size() + metadataFiles.size() + 1 + statisticsFiles.size(),
        manifestLists.size(), metadataFiles.size(), statisticsFiles.size());

    // 2. Collect manifests from all snapshots
    List<Snapshot> snapshots = Lists.newArrayList(table.snapshots());
    LOG.info("Scanning {} snapshots for manifests...", snapshots.size());

    Set<ManifestFile> allManifests = ConcurrentHashMap.newKeySet();
    AtomicInteger processedSnapshots = new AtomicInteger(0);
    long snapshotScanStart = System.nanoTime();

    Tasks.foreach(snapshots)
        .executeWith(planExecutorService)
        .suppressFailureWhenFinished()
        .onFailure((snapshot, exc) -> LOG.warn("Failed to read manifests for snapshot {}",
            snapshot.snapshotId(), exc))
        .run(snapshot -> {
          for (ManifestFile manifest : snapshot.allManifests(table.io())) {
            allManifests.add(manifest);
            validPaths.add(manifest.path());
          }
          int processed = processedSnapshots.incrementAndGet();
          if (processed % 100 == 0 || processed == snapshots.size()) {
            LOG.info("Processed {} / {} snapshots", processed, snapshots.size());
          }
        });

    long snapshotScanEnd = System.nanoTime();
    LOG.info("Completed scanning {} snapshots in {} ms, found {} unique manifests",
        snapshots.size(), (snapshotScanEnd - snapshotScanStart) / 1_000_000, allManifests.size());

    // 3. Read content files from manifests
    LOG.info("Scanning {} manifests for content files...", allManifests.size());
    AtomicInteger processedManifests = new AtomicInteger(0);
    AtomicLong totalContentFiles = new AtomicLong(0);
    long manifestScanStart = System.nanoTime();

    Tasks.foreach(allManifests)
        .executeWith(planExecutorService)
        .suppressFailureWhenFinished()
        .onFailure((manifest, exc) -> LOG.warn("Failed to read manifest {}", manifest.path(), exc))
        .run(manifest -> {
          long filesInManifest = 0;
          try (CloseableIterable<? extends ContentFile<?>> files = readManifestFiles(manifest)) {
            for (ContentFile<?> file : files) {
              validPaths.add(file.location());
              filesInManifest++;
            }
          } catch (IOException e) {
            LOG.warn("Error closing manifest reader for {}", manifest.path(), e);
          }
          totalContentFiles.addAndGet(filesInManifest);
          int processed = processedManifests.incrementAndGet();
          if (processed % 100 == 0 || processed == allManifests.size()) {
            LOG.info("Processed {} / {} manifests, found {} content files so far",
                processed, allManifests.size(), totalContentFiles.get());
          }
        });

    long manifestScanEnd = System.nanoTime();
    LOG.info("Completed scanning {} manifests in {} ms, found {} content files",
        allManifests.size(), (manifestScanEnd - manifestScanStart) / 1_000_000, totalContentFiles.get());

    long totalTime = System.nanoTime() - startTime;
    LOG.info("Completed collecting valid files in {} ms: {} total valid paths",
        totalTime / 1_000_000, validPaths.size());

    return validPaths;
  }

  private CloseableIterable<? extends ContentFile<?>> readManifestFiles(ManifestFile manifest) {
    switch (manifest.content()) {
      case DATA:
        return ManifestFiles.read(manifest, table.io(), table.specs());
      case DELETES:
        return ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs());
      default:
        throw new IllegalArgumentException("Unsupported manifest content: " + manifest.content());
    }
  }

  /**
   * Lists actual files on the filesystem. Returns a map from normalized path to original URI string.
   */
  private Map<String, String> listActualFiles() {
    if (compareToFileList != null) {
      List<String> files = filteredCompareToFileList();
      Map<String, String> result = Maps.newHashMapWithExpectedSize(files.size());
      for (String file : files) {
        result.put(normalizePath(file), file);
      }
      return result;
    }

    Map<String, String> actualFiles = Maps.newHashMap();
    Consumer<String> fileConsumer = path -> actualFiles.put(normalizePath(path), path);

    if (usePrefixListing) {
      Preconditions.checkArgument(
          table.io() instanceof SupportsPrefixOperations,
          "Cannot use prefix listing with FileIO %s which does not support prefix operations.",
          table.io());

      Predicate<org.apache.iceberg.io.FileInfo> predicate =
          fileInfo -> fileInfo.createdAtMillis() < olderThanTimestamp;

      FileSystemWalker.listDirRecursivelyWithFileIO(
          (SupportsPrefixOperations) table.io(),
          location,
          table.specs(),
          predicate,
          fileConsumer);
    } else {
      Predicate<FileStatus> predicate = file -> file.getModificationTime() < olderThanTimestamp;

      // Run entirely on driver - no depth/subdirs limits, ignore directoryConsumer
      List<String> ignoredSubDirs = Lists.newArrayList();
      FileSystemWalker.listDirRecursivelyWithHadoop(
          location,
          table.specs(),
          predicate,
          hadoopConf,
          Integer.MAX_VALUE,  // maxDepth - no limit, run everything on driver
          Integer.MAX_VALUE,  // maxDirectSubDirs - no limit
          ignoredSubDirs::add,
          fileConsumer);
    }

    return actualFiles;
  }

  /**
   * Normalizes a file path for comparison by extracting just the path component.
   */
  private String normalizePath(String uriString) {
    URI uri = new Path(uriString).toUri();
    return uri.getPath();
  }

  @VisibleForTesting
  static List<String> findOrphanFiles(
      Map<String, String> actualFiles,
      Set<String> validFilePaths,
      PrefixMismatchMode prefixMismatchMode,
      Map<String, String> equalSchemes,
      Map<String, String> equalAuthorities) {

    // Normalize valid paths for comparison
    Set<String> normalizedValidPaths = Sets.newHashSetWithExpectedSize(validFilePaths.size());
    Map<String, Pair<String, String>> validPathPrefixes = Maps.newHashMap();

    for (String validPath : validFilePaths) {
      URI uri = new Path(validPath).toUri();
      String normalizedPath = uri.getPath();
      normalizedValidPaths.add(normalizedPath);
      String scheme = equalSchemes.getOrDefault(uri.getScheme(), uri.getScheme());
      String authority = equalAuthorities.getOrDefault(uri.getAuthority(), uri.getAuthority());
      validPathPrefixes.put(normalizedPath, Pair.of(scheme, authority));
    }

    List<String> orphanFiles = Lists.newArrayList();
    Set<Pair<String, String>> conflicts = Sets.newHashSet();

    for (Map.Entry<String, String> entry : actualFiles.entrySet()) {
      String normalizedActualPath = entry.getKey();
      String originalUri = entry.getValue();

      if (!normalizedValidPaths.contains(normalizedActualPath)) {
        // Path not in valid set - it's an orphan
        orphanFiles.add(originalUri);
      } else {
        // Path exists in valid set - check scheme/authority
        URI actualUri = new Path(originalUri).toUri();
        String actualScheme = equalSchemes.getOrDefault(actualUri.getScheme(), actualUri.getScheme());
        String actualAuthority =
            equalAuthorities.getOrDefault(actualUri.getAuthority(), actualUri.getAuthority());

        Pair<String, String> validPrefix = validPathPrefixes.get(normalizedActualPath);
        String validScheme = validPrefix.first();
        String validAuthority = validPrefix.second();

        boolean schemeMatch = java.util.Objects.equals(actualScheme, validScheme);
        boolean authorityMatch = java.util.Objects.equals(actualAuthority, validAuthority);

        if (!schemeMatch || !authorityMatch) {
          if (prefixMismatchMode == PrefixMismatchMode.DELETE) {
            orphanFiles.add(originalUri);
          } else {
            if (!schemeMatch) {
              conflicts.add(Pair.of(validScheme, actualScheme));
            }
            if (!authorityMatch) {
              conflicts.add(Pair.of(validAuthority, actualAuthority));
            }
          }
        }
      }
    }

    if (prefixMismatchMode == PrefixMismatchMode.ERROR && !conflicts.isEmpty()) {
      throw new ValidationException(
          "Unable to determine whether certain files are orphan. "
              + "Metadata references files that match listed/provided files except for authority/scheme. "
              + "Please, inspect the conflicting authorities/schemes and provide which of them are equal "
              + "by further configuring the action via equalSchemes() and equalAuthorities() methods. "
              + "Set the prefix mismatch mode to 'NONE' to ignore remaining locations with conflicting "
              + "authorities/schemes or to 'DELETE' iff you are ABSOLUTELY confident that remaining conflicting "
              + "authorities/schemes are different. It will be impossible to recover deleted files. "
              + "Conflicting authorities/schemes: %s.",
          conflicts);
    }

    return orphanFiles;
  }

  private static Map<String, String> flattenMap(Map<String, String> map) {
    Map<String, String> flattenedMap = Maps.newHashMap();
    if (map != null) {
      for (String key : map.keySet()) {
        String value = map.get(key);
        for (String splitKey : COMMA_SPLITTER.split(key)) {
          flattenedMap.put(splitKey.trim(), value.trim());
        }
      }
    }
    return flattenedMap;
  }
}
