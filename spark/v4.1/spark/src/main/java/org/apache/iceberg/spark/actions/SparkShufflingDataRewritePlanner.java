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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.BinPackRewriteFilePlanner;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.BoundsPacking;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Produces plans for shuffling rewrites. Since shuffle and sort could considerably improve the
 * compression ratio, the planner introduces an additional {@link #COMPRESSION_FACTOR} option which
 * is used when calculating the {@link #expectedOutputFiles(long)}.
 *
 * <p>When {@link #USE_IDENTIFIER_KEYS} is enabled and the table has identifier keys, files are
 * grouped sequentially by bounds to preserve non-overlapping properties after merge.
 */
class SparkShufflingDataRewritePlanner extends BinPackRewriteFilePlanner {

  private static final Logger LOG = LoggerFactory.getLogger(SparkShufflingDataRewritePlanner.class);
  /**
   * The number of shuffle partitions and consequently the number of output files created by the
   * Spark sort is based on the size of the input data files used in this file rewriter. Due to
   * compression, the disk file sizes may not accurately represent the size of files in the output.
   * This parameter lets the user adjust the file size used for estimating actual output data size.
   * A factor greater than 1.0 would generate more files than we would expect based on the on-disk
   * file size. A value less than 1.0 would create fewer files than we would expect based on the
   * on-disk size.
   */
  public static final String COMPRESSION_FACTOR = "compression-factor";

  public static final double COMPRESSION_FACTOR_DEFAULT = 1.0;

  public static final String USE_IDENTIFIER_KEYS = "use-identifier-keys";
  public static final String COLUMNS = "columns";

  private double compressionFactor;
  private boolean useIdentifierKeys;
  private List<Integer> columnFieldIds;
  private List<Type> columnTypes;
  private long maxGroupSize;
  private long maxGroupCount;

  SparkShufflingDataRewritePlanner(
      Table table, Expression filter, Long snapshotId, boolean caseSensitive) {
    super(table, filter, snapshotId, caseSensitive);
  }

  @Override
  public Set<String> validOptions() {
    return ImmutableSet.<String>builder()
        .addAll(super.validOptions())
        .add(COMPRESSION_FACTOR)
        .add(USE_IDENTIFIER_KEYS)
        .add(COLUMNS)
        .build();
  }

  @Override
  public void init(Map<String, String> options) {
    super.init(options);
    this.compressionFactor = compressionFactor(options);

    this.useIdentifierKeys =
        Boolean.parseBoolean(options.getOrDefault(USE_IDENTIFIER_KEYS, "false"));

    if (useIdentifierKeys) {
      Set<Integer> identifierFieldIds = table().schema().identifierFieldIds();
      if (identifierFieldIds.isEmpty()) {
        // No identifier keys - will use regular binpack grouping
        this.columnFieldIds = ImmutableList.of();
        this.columnTypes = ImmutableList.of();
      } else {
        // Sort by field ID for deterministic ordering (same as SparkSortFileRewriteRunner)
        List<Integer> sortedFieldIds =
            identifierFieldIds.stream().sorted().collect(Collectors.toList());
        initColumnsFromFieldIds(sortedFieldIds);
      }
    } else {
      this.columnFieldIds = ImmutableList.of();
      this.columnTypes = ImmutableList.of();
    }

    this.maxGroupSize =
        PropertyUtil.propertyAsLong(
            options, MAX_FILE_GROUP_SIZE_BYTES, MAX_FILE_GROUP_SIZE_BYTES_DEFAULT);
    this.maxGroupCount =
        PropertyUtil.propertyAsLong(
            options, MAX_FILE_GROUP_INPUT_FILES, MAX_FILE_GROUP_INPUT_FILES_DEFAULT);
  }

  private void initColumns(List<String> columns) {
    this.columnFieldIds = new ArrayList<>();
    this.columnTypes = new ArrayList<>();

    for (String column : columns) {
      Types.NestedField field = table().schema().findField(column);
      Preconditions.checkArgument(field != null, "Column '%s' not found in table schema", column);
      columnFieldIds.add(field.fieldId());
      columnTypes.add(field.type());
    }
  }

  private void initColumnsFromFieldIds(List<Integer> fieldIds) {
    this.columnFieldIds = new ArrayList<>();
    this.columnTypes = new ArrayList<>();

    for (Integer fieldId : fieldIds) {
      Types.NestedField field = table().schema().findField(fieldId);
      Preconditions.checkArgument(
          field != null, "Field with ID '%s' not found in table schema", fieldId);
      columnFieldIds.add(field.fieldId());
      columnTypes.add(field.type());
    }
  }

  @Override
  protected int expectedOutputFiles(long inputSize) {
    return Math.max(1, super.expectedOutputFiles((long) (inputSize * compressionFactor)));
  }

  @Override
  protected boolean shouldIncludeColumnStats() {
    return useIdentifierKeys && !columnFieldIds.isEmpty();
  }

  private double compressionFactor(Map<String, String> options) {
    double value =
        PropertyUtil.propertyAsDouble(options, COMPRESSION_FACTOR, COMPRESSION_FACTOR_DEFAULT);
    Preconditions.checkArgument(
        value > 0, "'%s' is set to %s but must be > 0", COMPRESSION_FACTOR, value);
    return value;
  }

  @Override
  protected Iterable<List<FileScanTask>> planFileGroups(Iterable<FileScanTask> tasks) {
    // If using identifier keys with columns defined, use bounds-aware grouping
    if (useIdentifierKeys && !columnFieldIds.isEmpty()) {
      return planFileGroupsWithBounds(tasks);
    }

    // Otherwise use default binpack grouping
    return super.planFileGroups(tasks);
  }

  /**
   * Plans file groups based on overlapping bounds. Files are sorted by lower bound and grouped
   * only with overlapping files. Consecutive files (no overlap) start new groups to avoid
   * expanding bounds.
   */
  @SuppressWarnings("unchecked")
  private Iterable<List<FileScanTask>> planFileGroupsWithBounds(Iterable<FileScanTask> tasks) {
    List<FileScanTask> allTasks = Lists.newArrayList(tasks);

    if (allTasks.isEmpty()) {
      return ImmutableList.of();
    }

    // Filter to files that have bounds for identifier key columns
    List<FileScanTask> tasksWithBounds =
        allTasks.stream().filter(this::hasAllBounds).collect(Collectors.toList());

    if (tasksWithBounds.isEmpty()) {
      LOG.info("No files with bounds for identifier keys, using regular binpack");
      return super.planFileGroups(tasks);
    }

    // Filter files using parent's filterFiles (size, delete-file-threshold, delete-ratio)
    List<FileScanTask> filteredTasks = Lists.newArrayList(filterFiles(tasksWithBounds));

    if (filteredTasks.isEmpty()) {
      LOG.info("No files to group after filtering");
      return ImmutableList.of();
    }

    // Sort by lower bound of first column
    int fieldId = columnFieldIds.get(0);
    Type type = columnTypes.get(0);

    filteredTasks.sort(
        Comparator.comparing(
            task -> {
              ByteBuffer buf = task.file().lowerBounds().get(fieldId);
              return (Comparable<Object>) Conversions.fromByteBuffer(type, buf);
            }));

    // Group by overlapping bounds - consecutive files start new groups
    BoundsPacking.ListPacker<FileScanTask> packer =
        new BoundsPacking.ListPacker<>(maxGroupSize, maxGroupCount);
    List<List<FileScanTask>> groups =
        packer.packWithBounds(
            filteredTasks,
            ContentScanTask::length,
            task -> {
              ByteBuffer buf = task.file().lowerBounds().get(fieldId);
              return (Comparable<?>) Conversions.fromByteBuffer(type, buf);
            },
            task -> {
              ByteBuffer buf = task.file().upperBounds().get(fieldId);
              return (Comparable<?>) Conversions.fromByteBuffer(type, buf);
            });

    // Apply parent's filterFileGroups (min-input-files, content size, delete thresholds)
    List<List<FileScanTask>> validGroups = Lists.newArrayList(filterFileGroups(groups));

    LOG.info(
        "Bounds-aware grouping: {} files -> {} groups ({} valid)",
        filteredTasks.size(),
        groups.size(),
        validGroups.size());

    return validGroups;
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

}
