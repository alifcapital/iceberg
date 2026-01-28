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
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
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
  private Integer largeSortOrderId;
  private Integer smallSortOrderId;
  private boolean useIdentifierKeys;
  private boolean useUuidPrefixBucketing;

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

    List<String> columns;
    if (useIdentifierKeys) {
      Set<Integer> identifierFieldIds = table().schema().identifierFieldIds();
      if (identifierFieldIds.isEmpty()) {
        LOG.info("SMALL_FILES [{}]: table has no identifier keys, using unsorted fallback", table().name());
        this.columnFieldIds = ImmutableList.of();
        this.columnTypes = ImmutableList.of();
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

    this.useUuidPrefixBucketing =
        PropertyUtil.propertyAsBoolean(options, USE_UUID_PREFIX_BUCKETING, false);

    // Initialize sort order IDs for delete-files-only mode
    if (deleteFilesOnly && useIdentifierKeys && !columnFieldIds.isEmpty()) {
      initSortOrderIds();
    }
  }

  /**
   * Initialize sort order IDs for delete-files-only mode. Large files use single PK sort (if
   * applicable) for eq delete convert optimization. Small files use full identifier key sort for
   * good bounds.
   */
  private void initSortOrderIds() {
    Set<Integer> identifierFieldIds = table().schema().identifierFieldIds();

    // Sort order for small files: full identifier keys sort
    SortOrder smallFilesSortOrder = buildSortOrderFromFieldIds(columnFieldIds);
    this.smallSortOrderId = ensureSortOrderRegistered(smallFilesSortOrder);

    // Sort order for large files: single Long/Integer/Decimal PK or unsorted
    if (isSingleLongOrDecimalIdentifierKey(identifierFieldIds)) {
      // Single Long/Integer/Decimal PK - sort helps ROW_GROUP_MERGE_JOIN in eq delete convert
      SortOrder largeSortOrder = buildSortOrderFromFieldIds(columnFieldIds);
      this.largeSortOrderId = ensureSortOrderRegistered(largeSortOrder);
      LOG.info(
          "SMALL_FILES [{}]: delete-files-only: single Long/Integer/Decimal PK detected, "
              + "large files will be sorted (sortOrderId={})",
          table().name(),
          largeSortOrderId);
    } else {
      // Composite PK or non-numeric type - sorting won't help eq delete convert
      this.largeSortOrderId = null; // null means unsorted
      LOG.info(
          "SMALL_FILES [{}]: delete-files-only: composite or non-numeric PK, large files will be unsorted",
          table().name());
    }
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
   * Checks if identifier key is a single Long/Integer/Decimal column. These types benefit from
   * ROW_GROUP_MERGE_JOIN optimization in eq delete convert.
   */
  private boolean isSingleLongOrDecimalIdentifierKey(Set<Integer> identifierFieldIds) {
    if (identifierFieldIds.size() != 1) {
      return false;
    }
    Integer fieldId = identifierFieldIds.iterator().next();
    Types.NestedField field = table().schema().findField(fieldId);
    if (field == null) {
      return false;
    }
    Type.TypeID typeId = field.type().typeId();
    return typeId == Type.TypeID.LONG
        || typeId == Type.TypeID.INTEGER
        || typeId == Type.TypeID.DECIMAL;
  }

  /**
   * Ensures the sort order is registered in the table and returns its ID. Returns the sort order ID
   * if found or registered.
   */
  private Integer ensureSortOrderRegistered(SortOrder newSortOrder) {
    // Check if matching sort order already exists
    SortOrder existing = SortOrderUtil.maybeFindTableSortOrder(table(), newSortOrder);
    if (existing.isSorted()) {
      return existing.orderId();
    }

    // Need to add the sort order
    if (table().sortOrder().isUnsorted()) {
      // No default sort order - use replaceSortOrder (becomes default)
      LOG.info("SMALL_FILES [{}]: registering sort order as table default", table().name());
      ReplaceSortOrder replace = table().replaceSortOrder();
      for (SortField field : newSortOrder.fields()) {
        String columnName = table().schema().findColumnName(field.sourceId());
        if (field.direction() == SortDirection.ASC) {
          replace.asc(columnName, field.nullOrder());
        } else {
          replace.desc(columnName, field.nullOrder());
        }
      }
      replace.commit();
    } else {
      // Has different default - use addSortOrder (don't change default)
      LOG.info("SMALL_FILES [{}]: adding sort order without changing table default", table().name());
      TableOperations ops = ((HasTableOperations) table()).operations();
      TableMetadata current = ops.current();
      TableMetadata updated = TableMetadata.buildFrom(current).addSortOrder(newSortOrder).build();
      ops.commit(current, updated);
    }

    // Refresh to pick up the new sort order
    table().refresh();
    SortOrder registered = SortOrderUtil.maybeFindTableSortOrder(table(), newSortOrder);
    LOG.info("SMALL_FILES [{}]: sort order registered with orderId={}", table().name(), registered.orderId());
    return registered.orderId();
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
        // delete-files-only mode: process files with delete files
        planDeleteFilesOnly(ctx, partition, filesWithBounds, unsortedFallback, selectedGroups);
      } else {
        // Normal mode: merge small files
        planSmallFiles(ctx, partition, filesWithBounds, unsortedFallback, selectedGroups);
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

  /** Plan for delete-files-only mode. */
  private void planDeleteFilesOnly(
      RewriteExecutionContext ctx,
      StructLike partition,
      List<FileScanTask> filesWithBounds,
      boolean unsortedFallback,
      List<RewriteFileGroup> selectedGroups) {

    // Large files: only those with delete files
    List<FileScanTask> largeFilesWithDeletes =
        filesWithBounds.stream()
            .filter(t -> !isSmallFile(t))
            .filter(this::hasDeletes)
            .collect(Collectors.toList());

    // Small files: ALL of them (they need merging regardless of delete files)
    List<FileScanTask> smallFiles =
        filesWithBounds.stream().filter(this::isSmallFile).collect(Collectors.toList());

    if (largeFilesWithDeletes.isEmpty() && smallFiles.isEmpty()) {
      return;
    }

    LOG.info(
        "SMALL_FILES [{}]: delete-files-only: partition={} largeWithDeletes={} smallFiles={}",
        table().name(),
        partition,
        largeFilesWithDeletes.size(),
        smallFiles.size());

    // Large files: each in its own group with largeSortOrderId
    for (FileScanTask largeFile : largeFilesWithDeletes) {
      selectedGroups.add(
          createRewriteGroup(
              ctx,
              partition,
              ImmutableList.of(largeFile),
              writeMaxFileSize(),
              largeSortOrderId));
    }

    // Small files: use normal small files logic with smallSortOrderId
    if (smallFiles.size() >= 2) {
      // Get ALL large files for covered ranges calculation
      List<FileScanTask> largeFilesForRanges =
          filesWithBounds.stream()
              .filter(t -> !isSmallFile(t))
              .collect(Collectors.toList());

      planSmallFilesWithSortOrderId(
          ctx,
          partition,
          smallFiles,
          largeFilesForRanges,
          unsortedFallback,
          selectedGroups,
          smallSortOrderId);
    }
  }

  /** Plan for normal small files mode. */
  private void planSmallFiles(
      RewriteExecutionContext ctx,
      StructLike partition,
      List<FileScanTask> filesWithBounds,
      boolean unsortedFallback,
      List<RewriteFileGroup> selectedGroups) {

    // Separate into large and small files
    List<FileScanTask> largeFiles =
        filesWithBounds.stream().filter(t -> !isSmallFile(t)).collect(Collectors.toList());

    List<FileScanTask> smallFiles =
        filesWithBounds.stream().filter(this::isSmallFile).collect(Collectors.toList());

    LOG.debug(
        "SMALL_FILES [{}]: partition={} total={} large={} small={}",
        table().name(),
        partition,
        filesWithBounds.size(),
        largeFiles.size(),
        smallFiles.size());

    if (smallFiles.size() < 2) {
      return; // Need at least 2 small files to merge
    }

    planSmallFilesWithSortOrderId(
        ctx, partition, smallFiles, largeFiles, unsortedFallback, selectedGroups, null);
  }

  /** Plan small files with optional sortOrderId. */
  private void planSmallFilesWithSortOrderId(
      RewriteExecutionContext ctx,
      StructLike partition,
      List<FileScanTask> smallFiles,
      List<FileScanTask> largeFiles,
      boolean unsortedFallback,
      List<RewriteFileGroup> selectedGroups,
      Integer sortOrderId) {

    if (unsortedFallback) {
      // Unsorted fallback: group all small files without overlap analysis
      for (List<FileScanTask> group : groupFiles(smallFiles)) {
        if (enoughInputFiles(group)) {
          selectedGroups.add(
              createRewriteGroup(ctx, partition, group, writeMaxFileSize(), sortOrderId));
        }
      }
    } else {
      // Build covered ranges from large files (merged overlapping ranges)
      List<CoveredRange> coveredRanges = buildCoveredRanges(largeFiles);

      // Categorize small files into 3 groups:
      // 1. Clean zone files - entirely outside large file bounds
      // 2. Overlap files - inside large bounds but overlap with other small files
      // 3. Loners - inside large bounds, no overlap with other small files
      List<FileScanTask> cleanZoneFiles = new ArrayList<>();
      List<FileScanTask> insideLargeFiles = new ArrayList<>();

      for (FileScanTask task : smallFiles) {
        if (isInCleanZone(task, coveredRanges)) {
          cleanZoneFiles.add(task);
        } else {
          insideLargeFiles.add(task);
        }
      }

      // Group clean zone files using bin packing
      List<List<FileScanTask>> cleanZoneGroups = groupFiles(cleanZoneFiles);

      // Group files inside large bounds by overlap clusters
      // Returns: list of clusters (overlapping files) + list of loners
      OverlapResult overlapResult = groupByOverlapClusters(insideLargeFiles);

      // Bin pack each overlap cluster
      List<List<FileScanTask>> overlapGroups = new ArrayList<>();
      for (List<FileScanTask> cluster : overlapResult.clusters) {
        overlapGroups.addAll(groupFiles(cluster));
      }

      // Group loners together
      List<List<FileScanTask>> lonerGroups = groupFiles(overlapResult.loners);

      int overlapFilesCount = overlapResult.clusters.stream().mapToInt(List::size).sum();
      LOG.info(
          "SMALL_FILES [{}]: partition={} | cleanZone: {} files -> {} groups | "
              + "overlap: {} files ({} clusters) -> {} groups | loners: {} files -> {} groups",
          table().name(),
          partition,
          cleanZoneFiles.size(),
          cleanZoneGroups.size(),
          overlapFilesCount,
          overlapResult.clusters.size(),
          overlapGroups.size(),
          overlapResult.loners.size(),
          lonerGroups.size());

      // Create groups for cleanZone and overlap files (standard writeMaxFileSize)
      for (List<FileScanTask> group : cleanZoneGroups) {
        if (enoughInputFiles(group)) {
          selectedGroups.add(
              createRewriteGroup(ctx, partition, group, writeMaxFileSize(), sortOrderId));
        }
      }
      for (List<FileScanTask> group : overlapGroups) {
        if (enoughInputFiles(group)) {
          selectedGroups.add(
              createRewriteGroup(ctx, partition, group, writeMaxFileSize(), sortOrderId));
        }
      }
      // Create groups for loners (smaller writeMaxFileSize so they grow gradually)
      // Skip loners if UUID prefix bucketing is enabled and bounds look like UUID
      boolean skipLoners = useUuidPrefixBucketing && hasUuidBounds(overlapResult.loners);
      if (skipLoners) {
        LOG.info(
            "SMALL_FILES [{}]: skipping {} loner files due to UUID prefix bucketing",
            table().name(),
            overlapResult.loners.size());
      } else {
        for (List<FileScanTask> group : lonerGroups) {
          if (enoughInputFiles(group)) {
            selectedGroups.add(
                createRewriteGroup(ctx, partition, group, lonerWriteMaxFileSize, sortOrderId));
          }
        }
      }
    }
  }

  /** Check if file is considered "small" based on min file size threshold. */
  private boolean isSmallFile(FileScanTask task) {
    return task.length() < minFileSize();
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
   * Check if a small file fits entirely within a clean zone (gap between covered ranges).
   *
   * <p>Clean zones are: (-∞, first.lower), (range1.upper, range2.lower), ..., (last.upper, +∞)
   */
  @SuppressWarnings("unchecked")
  private boolean isInCleanZone(FileScanTask task, List<CoveredRange> coveredRanges) {
    if (coveredRanges.isEmpty()) {
      return true; // No large files - everything is clean
    }

    if (columnFieldIds.isEmpty()) {
      return false;
    }

    int fieldId = columnFieldIds.get(0);
    Type type = columnTypes.get(0);

    ByteBuffer lowerBuf = task.file().lowerBounds().get(fieldId);
    ByteBuffer upperBuf = task.file().upperBounds().get(fieldId);
    if (lowerBuf == null || upperBuf == null) {
      return false;
    }

    Comparable<Object> fileLower =
        (Comparable<Object>) Conversions.fromByteBuffer(type, lowerBuf);
    Comparable<Object> fileUpper =
        (Comparable<Object>) Conversions.fromByteBuffer(type, upperBuf);

    // Check if file is before first covered range
    CoveredRange first = coveredRanges.get(0);
    if (fileUpper.compareTo(first.lower) < 0) {
      return true; // File is entirely before first large file
    }

    // Check if file is after last covered range
    CoveredRange last = coveredRanges.get(coveredRanges.size() - 1);
    if (fileLower.compareTo(last.upper) > 0) {
      return true; // File is entirely after last large file
    }

    // Check if file fits in a gap between covered ranges
    for (int i = 0; i < coveredRanges.size() - 1; i++) {
      CoveredRange current = coveredRanges.get(i);
      CoveredRange next = coveredRanges.get(i + 1);

      // Gap is (current.upper, next.lower)
      // File fits if fileLower > current.upper AND fileUpper < next.lower
      if (fileLower.compareTo(current.upper) > 0 && fileUpper.compareTo(next.lower) < 0) {
        return true;
      }
    }

    return false; // File overlaps with some large file
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

  /** Group files using bin packing, sorted by lower bound if columns specified. */
  @SuppressWarnings("unchecked")
  private List<List<FileScanTask>> groupFiles(List<FileScanTask> files) {
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

    // Use bin packing to group files
    BinPacking.ListPacker<FileScanTask> packer =
        new BinPacking.ListPacker<>(maxGroupSize, 1, false, maxGroupInputFiles);

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

  /**
   * Checks if loner files have UUID-like bounds.
   * Only checks the first file - if it has UUID bounds, all do.
   */
  private boolean hasUuidBounds(List<FileScanTask> loners) {
    if (loners.isEmpty() || columnFieldIds.isEmpty()) {
      return false;
    }

    int fieldId = columnFieldIds.get(0);
    Type type = columnTypes.get(0);

    FileScanTask firstLoner = loners.get(0);
    ByteBuffer lowerBuf = firstLoner.file().lowerBounds().get(fieldId);
    if (lowerBuf == null) {
      return false;
    }

    Object lower = Conversions.fromByteBuffer(type, lowerBuf);
    if (lower instanceof CharSequence) {
      return UuidBucketUtil.looksLikeUuid(lower.toString());
    }
    return false;
  }
}
