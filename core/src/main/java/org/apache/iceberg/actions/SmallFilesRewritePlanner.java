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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.actions.RewriteDataFiles.FileGroupInfo;
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

  private final Expression filter;
  private final Long snapshotId;
  private final boolean caseSensitive;

  private List<Integer> columnFieldIds;
  private List<Type> columnTypes;
  private long maxGroupSize;
  private long maxGroupInputFiles;
  private boolean skipRewrite;

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
        .build();
  }

  @Override
  public void init(Map<String, String> options) {
    super.init(options);

    String columnsOption = options.get(COLUMNS);
    boolean useIdentifierKeys =
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
        LOG.info("SMALL_FILES: table has no identifier keys, skipping");
        this.skipRewrite = true;
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
          "SMALL_FILES init: column='{}' fieldId={} type={}",
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
    if (skipRewrite) {
      return new FileRewritePlan<>(CloseableIterable.of(ImmutableList.of()), 0, ImmutableMap.of());
    }

    StructLikeMap<List<FileScanTask>> filesByPartition = scanFiles();

    List<RewriteFileGroup> selectedGroups = new ArrayList<>();
    RewriteExecutionContext ctx = new RewriteExecutionContext();

    for (Map.Entry<StructLike, List<FileScanTask>> entry : filesByPartition.entrySet()) {
      StructLike partition = entry.getKey();
      List<FileScanTask> partitionFiles = entry.getValue();

      // Filter files that have bounds for all columns
      List<FileScanTask> filesWithBounds =
          partitionFiles.stream().filter(this::hasAllBounds).collect(Collectors.toList());

      if (filesWithBounds.isEmpty()) {
        continue;
      }

      // Separate into large and small files
      List<FileScanTask> largeFiles =
          filesWithBounds.stream()
              .filter(t -> !isSmallFile(t))
              .collect(Collectors.toList());

      List<FileScanTask> smallFiles =
          filesWithBounds.stream()
              .filter(this::isSmallFile)
              .collect(Collectors.toList());

      LOG.debug(
          "SMALL_FILES: partition={} total={} large={} small={}",
          partition,
          filesWithBounds.size(),
          largeFiles.size(),
          smallFiles.size());

      if (smallFiles.size() < 2) {
        continue; // Need at least 2 small files to merge
      }

      // Build covered ranges from large files (merged overlapping ranges)
      List<CoveredRange> coveredRanges = buildCoveredRanges(largeFiles);

      // Filter small files to only those entirely within clean zones
      List<FileScanTask> cleanZoneFiles = filterToCleanZones(smallFiles, coveredRanges);

      if (cleanZoneFiles.size() < 2) {
        continue;
      }

      // Sort by lower bound and group using bin packing
      List<List<FileScanTask>> groups = groupFiles(cleanZoneFiles);

      for (List<FileScanTask> group : groups) {
        if (group.size() >= 2) {
          long inputSize = inputSize(group);
          RewriteFileGroup rewriteGroup =
              newRewriteGroup(
                  ctx,
                  partition,
                  group,
                  inputSplitSize(inputSize),
                  expectedOutputFiles(inputSize));
          selectedGroups.add(rewriteGroup);
        }
      }
    }

    int totalGroupCount = selectedGroups.size();
    Map<StructLike, Integer> groupsInPartition =
        selectedGroups.stream()
            .collect(
                Collectors.groupingBy(g -> g.info().partition(), Collectors.summingInt(g -> 1)));

    LOG.debug("SMALL_FILES: created {} groups for rewrite", totalGroupCount);

    return new FileRewritePlan<>(
        CloseableIterable.of(selectedGroups), totalGroupCount, groupsInPartition);
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

    LOG.debug("SMALL_FILES: {} large files -> {} covered ranges", largeFiles.size(), merged.size());

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

  /** Filter small files to only those entirely within clean zones. */
  private List<FileScanTask> filterToCleanZones(
      List<FileScanTask> smallFiles, List<CoveredRange> coveredRanges) {
    List<FileScanTask> cleanFiles =
        smallFiles.stream()
            .filter(task -> isInCleanZone(task, coveredRanges))
            .collect(Collectors.toList());

    LOG.debug(
        "SMALL_FILES: {} small files in clean zones (out of {} total)",
        cleanFiles.size(),
        smallFiles.size());

    return cleanFiles;
  }

  /** Group files using bin packing, sorted by lower bound. */
  @SuppressWarnings("unchecked")
  private List<List<FileScanTask>> groupFiles(List<FileScanTask> files) {
    if (columnFieldIds.isEmpty() || files.isEmpty()) {
      return ImmutableList.of();
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

    // Use bin packing to group files
    BinPacking.ListPacker<FileScanTask> packer =
        new BinPacking.ListPacker<>(maxGroupSize, 1, false, maxGroupInputFiles);

    return packer.pack(sorted, FileScanTask::length);
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

  private RewriteFileGroup newRewriteGroup(
      RewriteExecutionContext ctx,
      StructLike partition,
      List<FileScanTask> tasks,
      long inputSplitSize,
      int expectedOutputFiles) {
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
        writeMaxFileSize(),
        inputSplitSize,
        expectedOutputFiles);
  }
}
