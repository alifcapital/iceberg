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

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.util.PropertyUtil;

public abstract class SizeBasedDataRewriter extends SizeBasedFileRewriter<FileScanTask, DataFile> {

  /**
   * The minimum number of deletes that needs to be associated with a data file for it to be
   * considered for rewriting. If a data file has this number of deletes or more, it will be
   * rewritten regardless of its file size determined by {@link #MIN_FILE_SIZE_BYTES} and {@link
   * #MAX_FILE_SIZE_BYTES}. If a file group contains a file that satisfies this condition, the file
   * group will be rewritten regardless of the number of files in the file group determined by
   * {@link #MIN_INPUT_FILES}.
   *
   * <p>Defaults to Integer.MAX_VALUE, which means this feature is not enabled by default.
   */
  public static final String DELETE_FILE_THRESHOLD = "delete-file-threshold";

  public static final int DELETE_FILE_THRESHOLD_DEFAULT = Integer.MAX_VALUE;

  private int deleteFileThreshold;


  /**
 * The threshold for the minimum number of deleted rows that a data file must have to be considered for rewriting.
 * If a data file has a row count greater than or equal to the specified threshold, the file will be considered
 * for rewriting regardless of other size or delete-based criteria.
 *
 * <p>This property allows for controlling the rewrite of files based on deleted row count, which can be useful when 
 * the size of a file isn't the sole criterion, and large files with excessive rows should also be rewritten
 * for performance reasons or other optimization goals.
 *
 * <p>Defaults to 0, meaning this feature is disabled unless a specific threshold is set.
 */
  public static final String DELETED_ROW_COUNT_THRESHOLD = "row-count-threshold";
  
  public static final int DELETED_ROW_COUNT_THRESHOLD_DEFAULT = Integer.MAX_VALUE;
  
  private int deletedRowCountThreshold;


    /**
   * The threshold for the ratio of deleted rows to total rows in a data file. This ratio is used
   * to determine if a file should be considered for rewriting based on the proportion of deleted rows
   * relative to the total number of rows in the file.
   * <p>
   * The value of this ratio should be between 0 and 1 (inclusive). If the ratio exceeds this threshold,
   * the file will be flagged for rewriting.
   * 
   * <p>Defaults to 0.05, which means that files with more than 5% of their rows deleted will be considered
   * for rewriting.
   */
  public static final String DELETE_ROW_RATIO = "delete-row-ratio";
  
  public static final double DELETE_ROW_RATIO_DEFAULT = 1;

  private double deleteRowRatio;


  protected SizeBasedDataRewriter(Table table) {
    super(table);
  }

  @Override
  public Set<String> validOptions() {
    return ImmutableSet.<String>builder()
        .addAll(super.validOptions())
        .add(DELETE_FILE_THRESHOLD)
        .add(DELETED_ROW_COUNT_THRESHOLD)
        .add(DELETE_ROW_RATIO)
        .build();
  }

  @Override
  public void init(Map<String, String> options) {
    super.init(options);
    this.deleteFileThreshold = deleteFileThreshold(options);
    this.deletedRowCountThreshold = deletedRowCountThreshold(options);
    this.deleteRowRatio = deleteRowRatio(options);
  }

  @Override
  protected Iterable<FileScanTask> filterFiles(Iterable<FileScanTask> tasks) {
    return Iterables.filter(tasks, task -> wronglySized(task)
        || tooManyDeletes(task)
        || tooManyDeletedRows(task)
        || exceedsDeleteRowRatio(task));
  }

  /**
   * Checks if the ratio of deleted rows to total rows in the given file exceeds the threshold 
   * defined by {@link #DELETE_ROW_RATIO}. A file will be flagged for rewriting if the ratio 
   * exceeds this threshold.
   * 
   * @param task the file scan task representing the data file
   * @return true if the file has a delete-to-row ratio greater than or equal to the threshold
   */
  private boolean exceedsDeleteRowRatio(FileScanTask task) {
    long totalRows = task.file().recordCount();
    long deletedRows = task.deletedRowCount();
    System.out.println(String.format(
            "Checking delete row ratio: file=%s, totalRows=%d, deletedRows=%d, ratio=%.4f, threshold=%.2f, exceeds=%b"
    return totalRows > 0 && ((double) deletedRows / totalRows) >= deleteRowRatio;
  }

  private boolean tooManyDeletedRows(FileScanTask task) {
    System.out.println(String.format(
            "Checking deleted rows: file=%s, deletedRows=%d, threshold=%d, exceeds=%b",
            task.file().path(), deletedRows, deletedRowCountThreshold, result
    ));
    return task.deletedRowCount() >= deletedRowCountThreshold;
  }

  private boolean tooManyDeletes(FileScanTask task) {
    return task.deletes() != null && task.deletes().size() >= deleteFileThreshold;
  }

  @Override
  protected Iterable<List<FileScanTask>> filterFileGroups(List<List<FileScanTask>> groups) {
    return Iterables.filter(groups, this::shouldRewrite);
  }

  private boolean shouldRewrite(List<FileScanTask> group) {
    return enoughInputFiles(group)
        || enoughContent(group)
        || tooMuchContent(group)
        || anyTaskHasTooManyDeletes(group)
        || anyTaskExceedsDeleteRowRatio(group)
        || anyTaskHasTooManyDeletedRows(group);
  }

  private boolean anyTaskHasTooManyDeletedRows(List<FileScanTask> group) {
    return group.stream().anyMatch(this::tooManyDeletedRows);
  }

  private boolean anyTaskHasTooManyDeletes(List<FileScanTask> group) {
    return group.stream().anyMatch(this::tooManyDeletes);
  }

    private boolean anyTaskExceedsDeleteRowRatio(List<FileScanTask> group) {
    return group.stream().anyMatch(this::exceedsDeleteRowRatio);
  }

  @Override
  protected long defaultTargetFileSize() {
    return PropertyUtil.propertyAsLong(
        table().properties(),
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
  }

  private int deleteFileThreshold(Map<String, String> options) {
    int value =
        PropertyUtil.propertyAsInt(options, DELETE_FILE_THRESHOLD, DELETE_FILE_THRESHOLD_DEFAULT);
    Preconditions.checkArgument(
        value >= 0, "'%s' is set to %s but must be >= 0", DELETE_FILE_THRESHOLD, value);
    return value;
  }

  private int deletedRowCountThreshold(Map<String, String> options) {
    int value =
        PropertyUtil.propertyAsInt(options, DELETED_ROW_COUNT_THRESHOLD, DELETED_ROW_COUNT_THRESHOLD_DEFAULT);
    Preconditions.checkArgument(
        value >= 0, "'%s' is set to %s but must be >= 0", DELETED_ROW_COUNT_THRESHOLD, value);
    return value;
  }

    private double deleteRowRatio(Map<String, String> options) {
    double value =
        PropertyUtil.propertyAsDouble(options, DELETE_ROW_RATIO, DELETE_ROW_RATIO_DEFAULT);
    Preconditions.checkArgument(
        value >= 0 && value <= 1, "'%s' is set to %s but must be between 0 and 1", DELETE_ROW_RATIO, value);
    return value;
  }
}
