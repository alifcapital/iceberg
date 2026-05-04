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
package org.apache.iceberg.actions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.actions.RewriteDataFiles.FileGroupInfo;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.SortOrderUtil;
import org.apache.iceberg.util.UuidBucketUtil;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.BinPacking;
import org.apache.iceberg.util.BoundsPacking;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.StructLikeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A file rewrite planner that merges small files only in "clean zones" - gaps between large files
 * where no large file bounds exist.
 *
 * <p>This planner scans ALL files to understand the full bounds landscape:
 *
 * <pre>
 * Large files:
 * ├─ A: [100 ─────── 500]
 * ├─ B: [600 ─────── 900]
 * └─ C: [1500 ────── 2000]
 *
 * Clean zones (gaps):
 * ├─ (-∞, 100)       ← before first large file
 * ├─ (500, 600)      ← gap between A and B
 * ├─ (900, 1500)     ← gap between B and C
 * └─ (2000, +∞)      ← after last large file
 * </pre>
 *
 * <p>A small file is safe to merge only if its entire bounds [lower, upper] fit within a single
 * clean zone. Small files that overlap with large files are skipped - they should be handled by
 * the OVERLAP strategy.
 */
public class SmallFilesRewritePlanner
    extends SizeBasedFileRewritePlanner<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> {

  private static final Logger LOG = LoggerFactory.getLogger(SmallFilesRewritePlanner.class);

  public static final String COLUMNS = "columns";
  public static final String USE_IDENTIFIER_KEYS = "use-identifier-keys";
  public static final String USE_UUID_PREFIX_BUCKETING = "use-uuid-prefix-bucketing";

  /** Target file size for loner files (files that don't overlap with other small files). */
  public static final String LONER_TARGET_FILE_SIZE_BYTES = "loner-target-file-size-bytes";

  public static final long LONER_TARGET_FILE_SIZE_BYTES_DEFAULT = 4 * 1024 * 1024; // 4 MB

  /** When true, only files with delete files are selected for rewrite. */
  public static final String DELETE_FILES_ONLY = "delete-files-only";

  public static final boolean DELETE_FILES_ONLY_DEFAULT = false;

  /** Minimum number of delete files to consider a file for rewrite. */
  public static final String DELETE_FILE_THRESHOLD = "delete-file-threshold";

  public static final int DELETE_FILE_THRESHOLD_DEFAULT = Integer.MAX_VALUE;

  /** Minimum delete ratio (deleted records / total records) to consider a file for rewrite. */
  public static final String DELETE_RATIO_THRESHOLD = "delete-ratio-threshold";

  public static final double DELETE_RATIO_THRESHOLD_DEFAULT = 0.3;

  /**
   * When set, files older than this many seconds bypass the min-input-files check.
   * Uses dataSequenceNumber to determine file age via snapshot timestamps.
   * Value of 0 disables this feature (default).
   */
  public static final String MERGE_OLDER_THAN = "merge-older-than";

  public static final long MERGE_OLDER_THAN_DEFAULT = 0;

  private final Expression filter;
  private final Long snapshotId;
  private final boolean caseSensitive;

  private List<Integer> columnFieldIds;
  private List<Type> columnTypes;
  private long maxGroupSize;
  private long maxGroupInputFiles;
  private long lonerWriteMaxFileSize;
  private boolean deleteFilesOnly;
  private int deleteFileThreshold;
  private double deleteRatioThreshold;
  private Integer sortOrderId;
  private boolean useIdentifierKeys;
  private boolean useUuidPrefixBucketing;
  private int numBuckets;
  private long mergeOlderThanMs;
  private Map<Long, Long> seqToTimestamp;

  public SmallFilesRewritePlanner(Table table) {
    this(table, Expressions.alwaysTrue());
  }

  public SmallFilesRewritePlanner(Table table, Expression filter) {
    this(
        table,
        filter,
        table.currentSnapshot() != null ? table.currentSnapshot().snapshotId() : null,
        false);
  }

  public SmallFilesRewritePlanner(
      Table table, Expression filter, Long snapshotId, boolean caseSensitive) {
    super(table);
    this.filter = filter;
    this.snapshotId = snapshotId;
    this.caseSensitive = caseSensitive;
  }

  @Override
  public Set<String> validOptions() {
    return ImmutableSet.<String>builder()
        .addAll(super.validOptions())
        .add(COLUMNS)
        .add(USE_IDENTIFIER_KEYS)
        .add(USE_UUID_PREFIX_BUCKETING)
        .add(LONER_TARGET_FILE_SIZE_BYTES)
        .add(DELETE_FILES_ONLY)
        .add(DELETE_FILE_THRESHOLD)
        .add(DELETE_RATIO_THRESHOLD)
        .add(MERGE_OLDER_THAN)
        .build();
  }

  @Override
  public void init(Map<String, String> options) {
    super.init(options);

    String columnsOption = options.get(COLUMNS);
    this.useIdentifierKeys =
        Boolean.parseBoolean(options.getOrDefault(USE_IDENTIFIER_KEYS, "false"));

    Preconditions.checkArgument(
        columnsOption != null || useIdentifierKeys,
        "SMALL_FILES strategy requires either '%s' option or '%s=true'",
        COLUMNS,
        USE_IDENTIFIER_KEYS);

    Preconditions.checkArgument(
        columnsOption == null || !useIdentifierKeys,
        "Cannot specify both '%s' and '%s=true'",
        COLUMNS,
        USE_IDENTIFIER_KEYS);

    // Initialize options that are needed even for unsorted fallback
    this.maxGroupSize =
        PropertyUtil.propertyAsLong(
            options, MAX_FILE_GROUP_SIZE_BYTES, MAX_FILE_GROUP_SIZE_BYTES_DEFAULT);
    this.maxGroupInputFiles =
        PropertyUtil.propertyAsLong(
            options, MAX_FILE_GROUP_INPUT_FILES, MAX_FILE_GROUP_INPUT_FILES_DEFAULT);
    this.lonerWriteMaxFileSize =
        PropertyUtil.propertyAsLong(
            options, LONER_TARGET_FILE_SIZE_BYTES, LONER_TARGET_FILE_SIZE_BYTES_DEFAULT);
    this.deleteFilesOnly =
        PropertyUtil.propertyAsBoolean(options, DELETE_FILES_ONLY, DELETE_FILES_ONLY_DEFAULT);
    this.deleteFileThreshold =
        PropertyUtil.propertyAsInt(options, DELETE_FILE_THRESHOLD, DELETE_FILE_THRESHOLD_DEFAULT);
    this.deleteRatioThreshold =
        PropertyUtil.propertyAsDouble(
            options, DELETE_RATIO_THRESHOLD, DELETE_RATIO_THRESHOLD_DEFAULT);

    long mergeOlderThanSec =
        PropertyUtil.propertyAsLong(options, MERGE_OLDER_THAN, MERGE_OLDER_THAN_DEFAULT);
    this.mergeOlderThanMs = mergeOlderThanSec * 1000;
    if (mergeOlderThanMs > 0) {
      this.seqToTimestamp = buildSeqToTimestamp();
      LOG.info(
          "SMALL_FILES [{}]: merge-older-than={}s, seqToTimestamp entries={}",
          table().name(),
          mergeOlderThanSec,
          seqToTimestamp.size());
    }

    List<String> columns;
    if (useIdentifierKeys) {
      Set<Integer> identifierFieldIds = table().schema().identifierFieldIds();
      if (identifierFieldIds.isEmpty()) {
        LOG.info("SMALL_FILES [{}]: table has no identifier keys, using unsorted fallback", table().name());
        this.columnFieldIds = ImmutableList.of();
        this.columnTypes = ImmutableList.of();
        this.useUuidPrefixBucketing = false;
        return;
      }
      // Sort by fieldId for stable ordering (same as SparkSmallFilesRewriteRunner)
      List<Integer> sortedFieldIds =
          identifierFieldIds.stream().sorted().collect(Collectors.toList());
      columns =
          sortedFieldIds.stream()
              .map(table().schema()::findColumnName)
              .collect(Collectors.toList());
    } else {
      columns =
          Arrays.stream(columnsOption.split(","))
              .map(String::trim)
              .filter(s -> !s.isEmpty())
              .collect(Collectors.toList());
      Preconditions.checkArgument(
          !columns.isEmpty(), "'%s' option must specify at least one column", COLUMNS);
    }

    // Validate and resolve columns to field IDs
    this.columnFieldIds = new ArrayList<>();
    this.columnTypes = new ArrayList<>();

    for (String column : columns) {
      Types.NestedField field = table().schema().findField(column);
      Preconditions.checkArgument(field != null, "Column '%s' not found in table schema", column);
      columnFieldIds.add(field.fieldId());
      columnTypes.add(field.type());
      LOG.info(
          "SMALL_FILES [{}]: init column='{}' fieldId={} type={}",
          table().name(),
          column,
          field.fieldId(),
          field.type());
    }

    this.useUuidPrefixBucketing =
        PropertyUtil.propertyAsBoolean(options, USE_UUID_PREFIX_BUCKETING, false);
    if (useUuidPrefixBucketing) {
      long totalSize = getTotalFilesSize();
      this.numBuckets = UuidBucketUtil.computeBuckets(totalSize, targetFileSize());
    }

    // Initialize sort order when identifier keys are available
    if (useIdentifierKeys && !columnFieldIds.isEmpty()) {
      initSortOrder();
    }
  }

  /**
   * Initialize sort order. Sorting is applied only when group splits into multiple files
   * (for good bounds).
   */
  private void initSortOrder() {
    // Build sort order for identifier keys (used when group splits into multiple files)
    SortOrder identifierKeysSortOrder = buildSortOrderFromFieldIds(columnFieldIds);
    this.sortOrderId = findMatchingSortOrder(identifierKeysSortOrder);

    LOG.info(
        "SMALL_FILES [{}]: will sort only when group splits into multiple files (sortOrderId={})",
        table().name(),
        sortOrderId);
  }

  /**
   * Find matching sort order in table without registering a new one.
   * Returns the sort order ID if found, null otherwise.
   */
  private Integer findMatchingSortOrder(SortOrder newSortOrder) {
    SortOrder existing = SortOrderUtil.maybeFindTableSortOrder(table(), newSortOrder);
    if (existing.isSorted()) {
      return existing.orderId();
    }
    return null;
  }

  private SortOrder buildSortOrderFromFieldIds(List<Integer> fieldIds) {
    Schema schema = table().schema();
    SortOrder.Builder builder = SortOrder.builderFor(schema);
    for (Integer fieldId : fieldIds) {
      String columnName = schema.findColumnName(fieldId);
      builder.asc(columnName, NullOrder.NULLS_LAST);
    }
    return builder.build();
  }

  /**
   * Determines sort order ID for a file group based on its size and target file size.
   *
   * <p>Sort order is needed when group will split into multiple files (for good bounds).
   *
   * @param inputSize total size of input files in the group
   * @param maxFileSize max file size for this group (affects split calculation)
   * @return sort order ID or null if sorting is not needed
   */
  private Integer getSortOrderIdForGroup(long inputSize, long maxFileSize) {
    if (sortOrderId == null) {
      return null; // no identifier keys configured
    }

    // Sort only if group will split into multiple files (for good bounds)
    // Use the actual maxFileSize for this group (loners have smaller maxFileSize)
    if (inputSize > maxFileSize) {
      return sortOrderId;
    }

    return null; // single output file - sorting won't improve bounds
  }

  /** Returns true if file has too many delete files (>= threshold). */
  private boolean tooManyDeletes(FileScanTask task) {
    return task.deletes() != null && task.deletes().size() >= deleteFileThreshold;
  }

  /** Returns true if file has too high delete ratio (>= threshold). */
  private boolean tooHighDeleteRatio(FileScanTask task) {
    if (task.deletes() == null || task.deletes().isEmpty()) {
      return false;
    }

    long knownDeletedRecordCount =
        task.deletes().stream()
            .filter(ContentFileUtil::isFileScoped)
            .mapToLong(ContentFile::recordCount)
            .sum();

    double deletedRecords = (double) Math.min(knownDeletedRecordCount, task.file().recordCount());
    double deleteRatio = deletedRecords / task.file().recordCount();
    return deleteRatio >= deleteRatioThreshold;
  }

  /** Returns true if file has delete files that need processing. */
  private boolean hasDeletes(FileScanTask task) {
    return tooManyDeletes(task) || tooHighDeleteRatio(task);
  }

  /** Builds a map from data sequence number to snapshot timestamp. */
  private Map<Long, Long> buildSeqToTimestamp() {
    Map<Long, Long> map = new HashMap<>();
    for (Snapshot snapshot : table().snapshots()) {
      map.put(snapshot.sequenceNumber(), snapshot.timestampMillis());
    }
    return map;
  }

  /** Returns true if a file is older than merge-older-than threshold. */
  private boolean isFileOld(FileScanTask task, long now) {
    Long seq = task.file().dataSequenceNumber();
    if (seq == null) {
      return true; // v1 table or unknown sequence — treat as old
    }

    Long timestampMs = seqToTimestamp.get(seq);
    if (timestampMs == null) {
      return true; // snapshot expired — file is definitely old
    }

    return now - timestampMs >= mergeOlderThanMs;
  }

  /**
   * Returns true if a group contains at least one file older than merge-older-than threshold.
   */
  private boolean oldEnough(List<FileScanTask> group) {
    if (group.size() < 2 || mergeOlderThanMs <= 0 || seqToTimestamp == null) {
      return false;
    }

    long now = System.currentTimeMillis();
    for (FileScanTask task : group) {
      if (isFileOld(task, now)) {
        return true;
      }
    }

    return false;
  }

  /** Counts files older than merge-older-than threshold. */
  private long countOldFiles(List<FileScanTask> files) {
    if (mergeOlderThanMs <= 0 || seqToTimestamp == null) {
      return 0;
    }

    long now = System.currentTimeMillis();
    return files.stream().filter(t -> isFileOld(t, now)).count();
  }

  @Override
  protected long defaultTargetFileSize() {
    return PropertyUtil.propertyAsLong(
        table().properties(),
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
  }

  @Override
  protected Iterable<FileScanTask> filterFiles(Iterable<FileScanTask> tasks) {
    // We don't filter here - we need ALL files to find the clean zone
    // Filtering happens in plan() after we determine the clean zone
    return tasks;
  }

  @Override
  protected Iterable<List<FileScanTask>> filterFileGroups(List<List<FileScanTask>> groups) {
    return groups;
  }

  @Override
  public FileRewritePlan<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> plan() {
    StructLikeMap<List<FileScanTask>> filesByPartition = scanFiles();

    List<RewriteFileGroup> selectedGroups = new ArrayList<>();
    RewriteExecutionContext ctx = new RewriteExecutionContext();

    // When no columns specified (unsorted fallback), skip clean zone filtering
    boolean unsortedFallback = columnFieldIds.isEmpty();

    for (Map.Entry<StructLike, List<FileScanTask>> entry : filesByPartition.entrySet()) {
      StructLike partition = entry.getKey();
      List<FileScanTask> partitionFiles = entry.getValue();

      List<FileScanTask> filesWithBounds;
      if (unsortedFallback) {
        // Unsorted fallback: use all files
        filesWithBounds = partitionFiles;
      } else {
        // Filter files that have bounds for all columns
        filesWithBounds =
            partitionFiles.stream().filter(this::hasAllBounds).collect(Collectors.toList());
      }

      if (filesWithBounds.isEmpty()) {
        continue;
      }

      if (deleteFilesOnly) {
        // delete-files-only mode: process files with delete files + tiny files
        planDeleteFilesOnly(ctx, partition, filesWithBounds, selectedGroups);
        // Also merge tiny files (< 4 MB) using standard small files logic, no sorting
        planSmallFiles(
            ctx, partition, filesWithBounds, unsortedFallback, selectedGroups,
            lonerWriteMaxFileSize, lonerWriteMaxFileSize, true);
      } else {
        // Normal mode: merge small files
        planSmallFiles(
            ctx, partition, filesWithBounds, unsortedFallback, selectedGroups,
            minFileSize(), writeMaxFileSize(), false);
      }
    }

    int totalGroupCount = selectedGroups.size();
    Map<StructLike, Integer> groupsInPartition =
        selectedGroups.stream()
            .collect(
                Collectors.groupingBy(g -> g.info().partition(), Collectors.summingInt(g -> 1)));

    LOG.debug("SMALL_FILES [{}]: created {} groups for rewrite", table().name(), totalGroupCount);

    return new FileRewritePlan<>(
        CloseableIterable.of(selectedGroups), totalGroupCount, groupsInPartition);
  }

  /**
   * Plan for delete-files-only mode: rewrite files >= 4 MB with delete files.
   * Tiny files (< 4 MB) are handled separately by planSmallFiles.
   */
  private void planDeleteFilesOnly(
      RewriteExecutionContext ctx,
      StructLike partition,
      List<FileScanTask> filesWithBounds,
      List<RewriteFileGroup> selectedGroups) {

    // Files with delete files (>= 4 MB, meeting delete threshold)
    // Tiny files with deletes will be grouped with other tiny files in planSmallFiles
    List<FileScanTask> filesWithDeletes =
        filesWithBounds.stream()
            .filter(t -> t.length() >= lonerWriteMaxFileSize)
            .filter(this::hasDeletes)
            .collect(Collectors.toList());

    if (filesWithDeletes.isEmpty()) {
      return;
    }

    LOG.info(
        "SMALL_FILES [{}]: delete-files-only: partition={} filesWithDeletes={}",
        table().name(),
        partition,
        filesWithDeletes.size());

    // Files with deletes: each in its own group
    // No sorting needed for single file groups (sorting only helps with bounds for multi-file groups)
    for (FileScanTask fileWithDeletes : filesWithDeletes) {
      selectedGroups.add(
          createRewriteGroup(
              ctx,
              partition,
              ImmutableList.of(fileWithDeletes),
              writeMaxFileSize(),
              null));
    }
  }

  /**
   * Plan for small files mode.
   *
   * @param smallFileThreshold files below this size are considered "small"
   * @param targetFileSize target size for output files
   * @param skipSorting if true, do not apply sort order to output files
   */
  private void planSmallFiles(
      RewriteExecutionContext ctx,
      StructLike partition,
      List<FileScanTask> filesWithBounds,
      boolean unsortedFallback,
      List<RewriteFileGroup> selectedGroups,
      long smallFileThreshold,
      long targetFileSize,
      boolean skipSorting) {

    // Separate into large and small files based on threshold
    List<FileScanTask> largeFiles =
        filesWithBounds.stream()
            .filter(t -> t.length() >= smallFileThreshold)
            .collect(Collectors.toList());

    List<FileScanTask> smallFiles =
        filesWithBounds.stream()
            .filter(t -> t.length() < smallFileThreshold)
            .collect(Collectors.toList());

    LOG.debug(
        "SMALL_FILES [{}]: partition={} total={} large={} small={} threshold={}",
        table().name(),
        partition,
        filesWithBounds.size(),
        largeFiles.size(),
        smallFiles.size(),
        smallFileThreshold);

    if (smallFiles.size() < 2) {
      return; // Need at least 2 small files to merge
    }

    planSmallFilesWithSortOrderId(
        ctx, partition, smallFiles, largeFiles, unsortedFallback, selectedGroups,
        targetFileSize, skipSorting);
  }

  /**
   * Plan small files - sortOrderId is determined per-group based on expected output files.
   *
   * @param targetFileSize target size for output files (cleanZone and overlap groups)
   * @param skipSorting if true, do not apply sort order to output files
   */
  private void planSmallFilesWithSortOrderId(
      RewriteExecutionContext ctx,
      StructLike partition,
      List<FileScanTask> smallFiles,
      List<FileScanTask> largeFiles,
      boolean unsortedFallback,
      List<RewriteFileGroup> selectedGroups,
      long targetFileSize,
      boolean skipSorting) {

    // For loners, use the smaller of targetFileSize and lonerWriteMaxFileSize
    long lonerTargetFileSize = Math.min(targetFileSize, lonerWriteMaxFileSize);

    if (unsortedFallback) {
      // Unsorted fallback: group all small files without overlap analysis
      for (List<FileScanTask> group : groupFiles(smallFiles, targetFileSize)) {
        if (enoughInputFiles(group) || oldEnough(group)) {
          long inputSize = inputSize(group);
          Integer groupSortOrderId = skipSorting ? null : getSortOrderIdForGroup(inputSize, targetFileSize);
          selectedGroups.add(
              createRewriteGroup(ctx, partition, group, targetFileSize, groupSortOrderId));
        }
      }
      return;
    }

    // Check if UUID bucketing should be used
    if (useUuidPrefixBucketing && numBuckets > 0 && !smallFiles.isEmpty()) {
      int firstBucket = getFileBucket(smallFiles.get(0));
      if (firstBucket >= 0) {
        // UUID detected - process per bucket
        planSmallFilesPerBucket(
            ctx, partition, smallFiles, largeFiles, selectedGroups,
            targetFileSize, lonerTargetFileSize, skipSorting);
        return;
      } else {
        LOG.info(
            "SMALL_FILES [{}]: UUID prefix bucketing skipped: bounds don't look like UUID",
            table().name());
      }
    }

    // Standard processing without UUID bucketing
    planSmallFilesCleanZoneOverlap(
        ctx, partition, smallFiles, largeFiles, selectedGroups,
        targetFileSize, lonerTargetFileSize, skipSorting);
  }

  /**
   * Plan small files with UUID bucket awareness - process each bucket independently.
   */
  private void planSmallFilesPerBucket(
      RewriteExecutionContext ctx,
      StructLike partition,
      List<FileScanTask> smallFiles,
      List<FileScanTask> largeFiles,
      List<RewriteFileGroup> selectedGroups,
      long targetFileSize,
      long lonerTargetFileSize,
      boolean skipSorting) {

    // Group files by bucket
    Map<Integer, List<FileScanTask>> smallByBucket = new java.util.HashMap<>();
    Map<Integer, List<FileScanTask>> largeByBucket = new java.util.HashMap<>();

    for (FileScanTask task : smallFiles) {
      int bucket = getFileBucket(task);
      if (bucket >= 0) {
        smallByBucket.computeIfAbsent(bucket, k -> new ArrayList<>()).add(task);
      }
    }

    for (FileScanTask task : largeFiles) {
      int bucket = getFileBucket(task);
      if (bucket >= 0) {
        largeByBucket.computeIfAbsent(bucket, k -> new ArrayList<>()).add(task);
      }
    }

    LOG.info(
        "SMALL_FILES [{}]: partition={} | UUID bucketing: {} small files in {} buckets, numBuckets={}",
        table().name(),
        partition,
        smallFiles.size(),
        smallByBucket.size(),
        numBuckets);

    // Process each bucket independently
    for (Map.Entry<Integer, List<FileScanTask>> entry : smallByBucket.entrySet()) {
      int bucket = entry.getKey();
      List<FileScanTask> bucketSmallFiles = entry.getValue();
      List<FileScanTask> bucketLargeFiles = largeByBucket.getOrDefault(bucket, ImmutableList.of());

      if (bucketSmallFiles.size() < 2) {
        continue; // Need at least 2 small files to merge within a bucket
      }

      planSmallFilesCleanZoneOverlap(
          ctx, partition, bucketSmallFiles, bucketLargeFiles, selectedGroups,
          targetFileSize, lonerTargetFileSize, skipSorting);
    }
  }

  /**
   * Core logic for cleanZone/overlap/loners grouping.
   */
  private void planSmallFilesCleanZoneOverlap(
      RewriteExecutionContext ctx,
      StructLike partition,
      List<FileScanTask> smallFiles,
      List<FileScanTask> largeFiles,
      List<RewriteFileGroup> selectedGroups,
      long targetFileSize,
      long lonerTargetFileSize,
      boolean skipSorting) {

    // Build covered ranges from large files (merged overlapping ranges)
    List<CoveredRange> coveredRanges = buildCoveredRanges(largeFiles);

    // Categorize small files into 3 groups:
    // 1. Clean zone files - entirely inside one gap between large file bounds (keyed by gap index)
    // 2. Overlap files - inside large bounds but overlap with other small files
    // 3. Loners - inside large bounds, no overlap with other small files
    Map<Integer, List<FileScanTask>> cleanZoneFilesByGap = new java.util.TreeMap<>();
    List<FileScanTask> insideLargeFiles = new ArrayList<>();
    int cleanZoneFilesCount = 0;

    for (FileScanTask task : smallFiles) {
      int gapIndex = cleanZoneGapIndex(task, coveredRanges);
      if (gapIndex >= 0) {
        cleanZoneFilesByGap.computeIfAbsent(gapIndex, k -> new ArrayList<>()).add(task);
        cleanZoneFilesCount++;
      } else {
        insideLargeFiles.add(task);
      }
    }

    // Bin pack each clean zone gap independently to avoid spanning across large bounds
    List<List<FileScanTask>> cleanZoneGroups = new ArrayList<>();
    for (List<FileScanTask> filesInGap : cleanZoneFilesByGap.values()) {
      cleanZoneGroups.addAll(groupFiles(filesInGap, targetFileSize));
    }

    // Group files inside large bounds by overlap clusters
    // Returns: list of clusters (overlapping files) + list of loners
    OverlapResult overlapResult = groupByOverlapClusters(insideLargeFiles);

    // Bin pack each overlap cluster
    List<List<FileScanTask>> overlapGroups = new ArrayList<>();
    for (List<FileScanTask> cluster : overlapResult.clusters) {
      overlapGroups.addAll(groupFiles(cluster, targetFileSize));
    }

    // Group loners together
    List<List<FileScanTask>> lonerGroups = groupFiles(overlapResult.loners, lonerTargetFileSize);

    int overlapFilesCount = overlapResult.clusters.stream().mapToInt(List::size).sum();
    long oldFilesCount = countOldFiles(smallFiles);
    LOG.info(
        "SMALL_FILES [{}]: partition={} | cleanZone: {} files in {} gaps -> {} groups | "
            + "overlap: {} files ({} clusters) -> {} groups | loners: {} files -> {} groups"
            + " | old: {}/{}",
        table().name(),
        partition,
        cleanZoneFilesCount,
        cleanZoneFilesByGap.size(),
        cleanZoneGroups.size(),
        overlapFilesCount,
        overlapResult.clusters.size(),
        overlapGroups.size(),
        overlapResult.loners.size(),
        lonerGroups.size(),
        oldFilesCount,
        smallFiles.size());

    // Create groups for cleanZone and overlap files
    for (List<FileScanTask> group : cleanZoneGroups) {
      if (enoughInputFiles(group) || oldEnough(group)) {
        long inputSize = inputSize(group);
        Integer groupSortOrderId = skipSorting ? null : getSortOrderIdForGroup(inputSize, targetFileSize);
        selectedGroups.add(
            createRewriteGroup(ctx, partition, group, targetFileSize, groupSortOrderId));
      }
    }
    for (List<FileScanTask> group : overlapGroups) {
      if (enoughInputFiles(group) || oldEnough(group)) {
        long inputSize = inputSize(group);
        Integer groupSortOrderId = skipSorting ? null : getSortOrderIdForGroup(inputSize, targetFileSize);
        selectedGroups.add(
            createRewriteGroup(ctx, partition, group, targetFileSize, groupSortOrderId));
      }
    }
    // Create groups for loners (smaller target so they grow gradually)
    for (List<FileScanTask> group : lonerGroups) {
      if (enoughInputFiles(group) || oldEnough(group)) {
        long inputSize = inputSize(group);
        Integer groupSortOrderId = skipSorting ? null : getSortOrderIdForGroup(inputSize, lonerTargetFileSize);
        selectedGroups.add(
            createRewriteGroup(ctx, partition, group, lonerTargetFileSize, groupSortOrderId));
      }
    }
  }

  /** Represents a covered range from large files [lower, upper]. */
  private static class CoveredRange {
    Comparable<Object> lower;
    Comparable<Object> upper;

    CoveredRange(Comparable<Object> lower, Comparable<Object> upper) {
      this.lower = lower;
      this.upper = upper;
    }
  }

  /** Result of grouping files by overlap - clusters of overlapping files and lone files. */
  private static class OverlapResult {
    final List<List<FileScanTask>> clusters;
    final List<FileScanTask> loners;

    OverlapResult(List<List<FileScanTask>> clusters, List<FileScanTask> loners) {
      this.clusters = clusters;
      this.loners = loners;
    }
  }

  /**
   * Build merged covered ranges from large files. Overlapping ranges are merged into single
   * ranges.
   */
  @SuppressWarnings("unchecked")
  private List<CoveredRange> buildCoveredRanges(List<FileScanTask> largeFiles) {
    if (largeFiles.isEmpty() || columnFieldIds.isEmpty()) {
      return ImmutableList.of();
    }

    int fieldId = columnFieldIds.get(0);
    Type type = columnTypes.get(0);

    // Extract bounds from large files
    List<CoveredRange> ranges = new ArrayList<>();
    for (FileScanTask task : largeFiles) {
      ByteBuffer lowerBuf = task.file().lowerBounds().get(fieldId);
      ByteBuffer upperBuf = task.file().upperBounds().get(fieldId);
      if (lowerBuf != null && upperBuf != null) {
        Comparable<Object> lower = (Comparable<Object>) Conversions.fromByteBuffer(type, lowerBuf);
        Comparable<Object> upper = (Comparable<Object>) Conversions.fromByteBuffer(type, upperBuf);
        ranges.add(new CoveredRange(lower, upper));
      }
    }

    if (ranges.isEmpty()) {
      return ImmutableList.of();
    }

    // Sort by lower bound
    ranges.sort((a, b) -> a.lower.compareTo(b.lower));

    // Merge overlapping ranges
    List<CoveredRange> merged = new ArrayList<>();
    CoveredRange current = ranges.get(0);

    for (int i = 1; i < ranges.size(); i++) {
      CoveredRange next = ranges.get(i);
      // If next.lower <= current.upper, ranges overlap or touch - merge them
      if (next.lower.compareTo(current.upper) <= 0) {
        // Extend current range if next.upper is greater
        if (next.upper.compareTo(current.upper) > 0) {
          current.upper = next.upper;
        }
      } else {
        // Gap between ranges - save current and start new
        merged.add(current);
        current = next;
      }
    }
    merged.add(current);

    LOG.debug("SMALL_FILES [{}]: {} large files -> {} covered ranges", table().name(), largeFiles.size(), merged.size());

    return merged;
  }

  /**
   * Returns the index of the clean zone gap that fully contains the file, or -1 if the file
   * overlaps a large file.
   *
   * <p>Gap indexing: 0 = (-∞, first.lower), i = (range[i-1].upper, range[i].lower) for 1..N-1,
   * N = (last.upper, +∞). When there are no large files, all files are in gap 0.
   */
  @SuppressWarnings("unchecked")
  private int cleanZoneGapIndex(FileScanTask task, List<CoveredRange> coveredRanges) {
    if (coveredRanges.isEmpty()) {
      return 0; // No large files - everything is in the single open gap
    }

    if (columnFieldIds.isEmpty()) {
      return -1;
    }

    int fieldId = columnFieldIds.get(0);
    Type type = columnTypes.get(0);

    ByteBuffer lowerBuf = task.file().lowerBounds().get(fieldId);
    ByteBuffer upperBuf = task.file().upperBounds().get(fieldId);
    if (lowerBuf == null || upperBuf == null) {
      return -1;
    }

    Comparable<Object> fileLower =
        (Comparable<Object>) Conversions.fromByteBuffer(type, lowerBuf);
    Comparable<Object> fileUpper =
        (Comparable<Object>) Conversions.fromByteBuffer(type, upperBuf);

    // Gap 0: before first covered range
    CoveredRange first = coveredRanges.get(0);
    if (fileUpper.compareTo(first.lower) < 0) {
      return 0;
    }

    // Gap N: after last covered range
    int n = coveredRanges.size();
    CoveredRange last = coveredRanges.get(n - 1);
    if (fileLower.compareTo(last.upper) > 0) {
      return n;
    }

    // Gaps 1..N-1: between range[i-1] and range[i]
    for (int i = 0; i < n - 1; i++) {
      CoveredRange current = coveredRanges.get(i);
      CoveredRange next = coveredRanges.get(i + 1);

      if (fileLower.compareTo(current.upper) > 0 && fileUpper.compareTo(next.lower) < 0) {
        return i + 1;
      }
    }

    return -1; // overlaps a large file
  }

  /**
   * Groups files by overlap clusters. Files that overlap with each other form a cluster. Files that
   * don't overlap with any other file are considered loners.
   *
   * <p>Algorithm: sort by lower bound, then sweep through. If current file overlaps with the
   * running max upper bound, it joins the current cluster. Otherwise, start a new cluster.
   */
  @SuppressWarnings("unchecked")
  private OverlapResult groupByOverlapClusters(List<FileScanTask> files) {
    if (files.isEmpty()) {
      return new OverlapResult(ImmutableList.of(), ImmutableList.of());
    }

    if (columnFieldIds.isEmpty()) {
      // No columns - can't determine overlap, treat all as one cluster
      return new OverlapResult(ImmutableList.of(files), ImmutableList.of());
    }

    int fieldId = columnFieldIds.get(0);
    Type type = columnTypes.get(0);

    // Sort by lower bound
    List<FileScanTask> sorted = new ArrayList<>(files);
    sorted.sort(
        Comparator.comparing(
            task -> {
              ByteBuffer buf = task.file().lowerBounds().get(fieldId);
              return (Comparable<Object>) Conversions.fromByteBuffer(type, buf);
            }));

    List<List<FileScanTask>> clusters = new ArrayList<>();
    List<FileScanTask> loners = new ArrayList<>();

    List<FileScanTask> currentCluster = new ArrayList<>();
    Comparable<Object> maxUpper = null;

    for (FileScanTask task : sorted) {
      ByteBuffer lowerBuf = task.file().lowerBounds().get(fieldId);
      ByteBuffer upperBuf = task.file().upperBounds().get(fieldId);
      Comparable<Object> lower = (Comparable<Object>) Conversions.fromByteBuffer(type, lowerBuf);
      Comparable<Object> upper = (Comparable<Object>) Conversions.fromByteBuffer(type, upperBuf);

      if (maxUpper == null) {
        // First file - start new cluster
        currentCluster.add(task);
        maxUpper = upper;
      } else if (lower.compareTo(maxUpper) <= 0) {
        // Overlaps with current cluster
        currentCluster.add(task);
        if (upper.compareTo(maxUpper) > 0) {
          maxUpper = upper;
        }
      } else {
        // No overlap - check if adjacent (small gap)
        double distance = BoundsPacking.calculateDistance(/* lower= */ maxUpper, /* upper= */ lower);
        if (BoundsPacking.isWithinGapThreshold(distance)) {
          // Adjacent - add to current cluster
          currentCluster.add(task);
          if (upper.compareTo(maxUpper) > 0) {
            maxUpper = upper;
          }
        } else {
          // Gap too large - finalize current cluster
          if (currentCluster.size() == 1) {
            loners.add(currentCluster.get(0));
          } else {
            clusters.add(ImmutableList.copyOf(currentCluster));
          }
          // Start new cluster
          currentCluster = new ArrayList<>();
          currentCluster.add(task);
          maxUpper = upper;
        }
      }
    }

    // Don't forget the last cluster
    if (!currentCluster.isEmpty()) {
      if (currentCluster.size() == 1) {
        loners.add(currentCluster.get(0));
      } else {
        clusters.add(ImmutableList.copyOf(currentCluster));
      }
    }

    return new OverlapResult(clusters, loners);
  }

  /**
   * Group files using bin packing, sorted by lower bound if columns specified.
   *
   * @param targetFileSize target size for bin packing (max size per group)
   */
  @SuppressWarnings("unchecked")
  private List<List<FileScanTask>> groupFiles(List<FileScanTask> files, long targetFileSize) {
    if (files.isEmpty()) {
      return ImmutableList.of();
    }

    List<FileScanTask> filesToPack;
    if (columnFieldIds.isEmpty()) {
      // Unsorted fallback: use files as-is
      filesToPack = files;
    } else {
      int fieldId = columnFieldIds.get(0);
      Type type = columnTypes.get(0);

      // Sort by lower bound
      filesToPack = new ArrayList<>(files);
      filesToPack.sort(
          Comparator.comparing(
              task -> {
                ByteBuffer buf = task.file().lowerBounds().get(fieldId);
                return (Comparable<Object>) Conversions.fromByteBuffer(type, buf);
              }));
    }

    // Use bin packing to group files - use min of maxGroupSize and targetFileSize
    long binSize = Math.min(maxGroupSize, targetFileSize);
    BinPacking.ListPacker<FileScanTask> packer =
        new BinPacking.ListPacker<>(binSize, 1, false, maxGroupInputFiles);

    return packer.pack(filesToPack, FileScanTask::length);
  }

  private boolean hasAllBounds(FileScanTask task) {
    Map<Integer, ByteBuffer> lowerBounds = task.file().lowerBounds();
    Map<Integer, ByteBuffer> upperBounds = task.file().upperBounds();

    if (lowerBounds == null || upperBounds == null) {
      return false;
    }

    for (int fieldId : columnFieldIds) {
      if (!lowerBounds.containsKey(fieldId) || !upperBounds.containsKey(fieldId)) {
        return false;
      }
    }
    return true;
  }

  private StructLikeMap<List<FileScanTask>> scanFiles() {
    TableScan scan =
        table()
            .newScan()
            .filter(filter)
            .caseSensitive(caseSensitive)
            .ignoreResiduals()
            .includeColumnStats();

    if (snapshotId != null) {
      scan = scan.useSnapshot(snapshotId);
    }

    CloseableIterable<FileScanTask> fileScanTasks = scan.planFiles();
    Types.StructType partitionType = table().spec().partitionType();
    StructLikeMap<List<FileScanTask>> filesByPartition = StructLikeMap.create(partitionType);
    StructLike emptyStruct = GenericRecord.create(partitionType);

    try {
      for (FileScanTask task : fileScanTasks) {
        StructLike partition =
            task.file().specId() == table().spec().specId()
                ? task.file().partition()
                : emptyStruct;
        filesByPartition.computeIfAbsent(partition, k -> Lists.newArrayList()).add(task);
      }
    } finally {
      try {
        fileScanTasks.close();
      } catch (IOException e) {
        LOG.error("Error closing file scan tasks", e);
      }
    }

    return filesByPartition;
  }

  private RewriteFileGroup createRewriteGroup(
      RewriteExecutionContext ctx,
      StructLike partition,
      List<FileScanTask> tasks,
      long maxFileSize,
      Integer sortOrderId) {
    long inputSize = inputSize(tasks);
    FileGroupInfo info =
        ImmutableRewriteDataFiles.FileGroupInfo.builder()
            .globalIndex(ctx.currentGlobalIndex())
            .partitionIndex(ctx.currentPartitionIndex(partition))
            .partition(partition)
            .build();
    return new RewriteFileGroup(
        info,
        Lists.newArrayList(tasks),
        outputSpecId(),
        maxFileSize,
        inputSplitSize(inputSize),
        expectedOutputFiles(inputSize),
        sortOrderId);
  }

  /** Gets total files size from snapshot summary. */
  private long getTotalFilesSize() {
    if (table().currentSnapshot() == null) {
      return 0;
    }

    String totalSizeStr = table().currentSnapshot().summary().get("total-files-size");
    if (totalSizeStr == null) {
      return 0;
    }

    try {
      return Long.parseLong(totalSizeStr);
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  /**
   * Get the bucket for a file based on its lower bound.
   * Returns -1 if bucket cannot be determined.
   */
  private int getFileBucket(FileScanTask task) {
    if (numBuckets <= 0 || columnFieldIds.isEmpty()) {
      return -1;
    }

    int fieldId = columnFieldIds.get(0);
    Type type = columnTypes.get(0);

    ByteBuffer lowerBuf = task.file().lowerBounds() != null
        ? task.file().lowerBounds().get(fieldId)
        : null;
    if (lowerBuf == null) {
      return -1;
    }

    Object lower = Conversions.fromByteBuffer(type, lowerBuf);
    if (!(lower instanceof CharSequence) || !UuidBucketUtil.looksLikeUuid(lower.toString())) {
      return -1;
    }

    int lowerHex = UuidBucketUtil.extractHexPrefix(lower);
    return UuidBucketUtil.hexToBucket(lowerHex, numBuckets);
  }
}
