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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.ParquetValueReader;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.schema.MessageType;

/**
 * Row-group level merge join for Parquet files.
 *
 * <p>This class performs merge join with row group level control, allowing:
 *
 * <ul>
 *   <li>Skipping row groups with no overlap with delete keys
 *   <li>Early termination when delete keys exceed row group upper bound
 *   <li>Binary search for initial delete pointer position
 * </ul>
 */
class ParquetRowGroupMergeJoin {

  private ParquetRowGroupMergeJoin() {}

  /** Result of merge join with row group level processing. */
  static class Result {
    final List<PositionDelete<Record>> matches;
    final long recordsScanned;

    Result(List<PositionDelete<Record>> matches, long recordsScanned) {
      this.matches = matches;
      this.recordsScanned = recordsScanned;
    }
  }

  /**
   * Perform merge join with row group level control.
   *
   * @param inputFile the Parquet input file
   * @param projectionSchema schema for projection (with ROW_POSITION)
   * @param sortedDeleteKeys sorted list of delete keys
   * @param eqDeleteFieldId field ID of the equality delete column
   * @param eqColumnName name of the equality delete column
   * @param dataFilePath path of the data file (for position delete output)
   * @return merge join result with matches and statistics
   */
  @SuppressWarnings("unchecked")
  static Result execute(
      InputFile inputFile,
      Schema projectionSchema,
      List<Long> sortedDeleteKeys,
      int eqDeleteFieldId,
      String eqColumnName,
      String dataFilePath)
      throws IOException {

    List<PositionDelete<Record>> matches = Lists.newArrayList();
    long recordsScanned = 0;

    if (sortedDeleteKeys.isEmpty()) {
      return new Result(matches, 0);
    }

    long minDeleteKey = sortedDeleteKeys.get(0);
    long maxDeleteKey = sortedDeleteKeys.get(sortedDeleteKeys.size() - 1);

    // Build schema without ROW_POSITION since we track position manually
    List<Types.NestedField> readFields = Lists.newArrayList();
    for (Types.NestedField field : projectionSchema.columns()) {
      if (field.fieldId() != MetadataColumns.ROW_POSITION.fieldId()) {
        readFields.add(field);
      }
    }
    Schema readSchema = new Schema(readFields);

    // Open low-level Parquet reader
    ParquetReadOptions options = ParquetReadOptions.builder().build();
    org.apache.parquet.io.InputFile parquetInputFile =
        new org.apache.parquet.io.InputFile() {
          @Override
          public long getLength() throws IOException {
            return inputFile.getLength();
          }

          @Override
          public org.apache.parquet.io.SeekableInputStream newStream() throws IOException {
            org.apache.iceberg.io.SeekableInputStream stream = inputFile.newStream();
            return new org.apache.parquet.io.DelegatingSeekableInputStream(stream) {
              @Override
              public long getPos() throws IOException {
                return stream.getPos();
              }

              @Override
              public void seek(long newPos) throws IOException {
                stream.seek(newPos);
              }
            };
          }
        };

    try (ParquetFileReader reader = ParquetFileReader.open(parquetInputFile, options)) {
      MessageType fileSchema = reader.getFileMetaData().getSchema();
      List<BlockMetaData> rowGroups = reader.getRowGroups();

      // Create Iceberg reader for records (without ROW_POSITION)
      ParquetValueReader<Record> model =
          (ParquetValueReader<Record>) GenericParquetReaders.buildReader(readSchema, fileSchema);

      int deletePtr = 0; // Shared across row groups for efficiency
      long rowPosition = 0; // Track row position manually

      for (int rgIdx = 0; rgIdx < rowGroups.size(); rgIdx++) {
        BlockMetaData rowGroup = rowGroups.get(rgIdx);
        long rgRowCount = rowGroup.getRowCount();

        // Get row group bounds for eq delete column
        ColumnChunkMetaData colMeta = null;
        for (ColumnChunkMetaData col : rowGroup.getColumns()) {
          if (col.getPath().toDotString().equals(eqColumnName)) {
            colMeta = col;
            break;
          }
        }

        // Check if we have valid statistics for this row group
        org.apache.parquet.column.statistics.Statistics<?> stats =
            (colMeta != null) ? colMeta.getStatistics() : null;
        boolean hasValidStats = stats != null && stats.hasNonNullValue();

        if (!hasValidStats) {
          // No stats, must read this row group
          ProcessRowGroupResult result =
              processRowGroupNoStats(
                  reader, model, rgRowCount, sortedDeleteKeys, deletePtr, rowPosition, dataFilePath);
          recordsScanned += result.recordsScanned;
          matches.addAll(result.matches);
          deletePtr = result.deletePtr;
          rowPosition = result.rowPosition;
          if (result.earlyTermination) break;
          continue;
        }

        // Get row group bounds
        Object minObj = stats.genericGetMin();
        Object maxObj = stats.genericGetMax();
        if (minObj == null
            || maxObj == null
            || !(minObj instanceof Number)
            || !(maxObj instanceof Number)) {
          // Statistics type doesn't match expected numeric type
          ProcessRowGroupResult result =
              processRowGroupNoStats(
                  reader, model, rgRowCount, sortedDeleteKeys, deletePtr, rowPosition, dataFilePath);
          recordsScanned += result.recordsScanned;
          matches.addAll(result.matches);
          deletePtr = result.deletePtr;
          rowPosition = result.rowPosition;
          if (result.earlyTermination) break;
          continue;
        }
        long rgMin = ((Number) minObj).longValue();
        long rgMax = ((Number) maxObj).longValue();

        // Skip row group if all delete keys < row group min (and we're done)
        if (maxDeleteKey < rgMin) {
          reader.skipNextRowGroup();
          rowPosition += rgRowCount;
          break;
        }

        // Skip row group if all delete keys > row group max
        if (minDeleteKey > rgMax) {
          reader.skipNextRowGroup();
          rowPosition += rgRowCount;
          continue;
        }

        // Check if current delete pointer is already past this row group
        if (deletePtr < sortedDeleteKeys.size() && sortedDeleteKeys.get(deletePtr) > rgMax) {
          reader.skipNextRowGroup();
          rowPosition += rgRowCount;
          continue;
        }

        // Binary search to find starting deletePtr for this row group
        if (deletePtr < sortedDeleteKeys.size() && sortedDeleteKeys.get(deletePtr) < rgMin) {
          int searchResult = Collections.binarySearch(sortedDeleteKeys, rgMin);
          if (searchResult < 0) {
            deletePtr = -(searchResult + 1);
          } else {
            deletePtr = searchResult;
          }
        }

        // Check again after binary search
        if (deletePtr >= sortedDeleteKeys.size()) {
          reader.skipNextRowGroup();
          rowPosition += rgRowCount;
          break;
        }

        if (sortedDeleteKeys.get(deletePtr) > rgMax) {
          reader.skipNextRowGroup();
          rowPosition += rgRowCount;
          continue;
        }

        // Read row group
        ProcessRowGroupResult result =
            processRowGroupWithStats(
                reader,
                model,
                rgRowCount,
                sortedDeleteKeys,
                deletePtr,
                rowPosition,
                dataFilePath,
                rgMax,
                rgIdx == rowGroups.size() - 1);
        recordsScanned += result.recordsScanned;
        matches.addAll(result.matches);
        deletePtr = result.deletePtr;
        rowPosition = result.rowPosition;
        if (result.earlyTermination) break;
      }
    }

    return new Result(matches, recordsScanned);
  }

  private static class ProcessRowGroupResult {
    final List<PositionDelete<Record>> matches;
    final long recordsScanned;
    final int deletePtr;
    final long rowPosition;
    final boolean earlyTermination;

    ProcessRowGroupResult(
        List<PositionDelete<Record>> matches,
        long recordsScanned,
        int deletePtr,
        long rowPosition,
        boolean earlyTermination) {
      this.matches = matches;
      this.recordsScanned = recordsScanned;
      this.deletePtr = deletePtr;
      this.rowPosition = rowPosition;
      this.earlyTermination = earlyTermination;
    }
  }

  private static ProcessRowGroupResult processRowGroupNoStats(
      ParquetFileReader reader,
      ParquetValueReader<Record> model,
      long rgRowCount,
      List<Long> sortedDeleteKeys,
      int deletePtr,
      long rowPosition,
      String dataFilePath)
      throws IOException {

    List<PositionDelete<Record>> matches = Lists.newArrayList();
    long recordsScanned = 0;
    boolean earlyTermination = false;

    PageReadStore pages = reader.readNextRowGroup();
    model.setPageSource(pages);

    for (long i = 0; i < rgRowCount; i++) {
      Record record = model.read(null);
      if (record == null) {
        rowPosition++;
        continue;
      }
      recordsScanned++;
      Object val = record.get(0);
      if (val == null) {
        rowPosition++;
        continue;
      }

      long dataKey = val instanceof Integer ? ((Integer) val).longValue() : (Long) val;

      while (deletePtr < sortedDeleteKeys.size() && sortedDeleteKeys.get(deletePtr) < dataKey) {
        deletePtr++;
      }

      if (deletePtr >= sortedDeleteKeys.size()) {
        earlyTermination = true;
        rowPosition++;
        break;
      }

      if (sortedDeleteKeys.get(deletePtr).equals(dataKey)) {
        PositionDelete<Record> posDelete = PositionDelete.create();
        posDelete.set(dataFilePath, rowPosition, null);
        matches.add(posDelete);
      }
      rowPosition++;
    }

    return new ProcessRowGroupResult(
        matches, recordsScanned, deletePtr, rowPosition, earlyTermination);
  }

  private static ProcessRowGroupResult processRowGroupWithStats(
      ParquetFileReader reader,
      ParquetValueReader<Record> model,
      long rgRowCount,
      List<Long> sortedDeleteKeys,
      int deletePtr,
      long rowPosition,
      String dataFilePath,
      long rgMax,
      boolean isLastRowGroup)
      throws IOException {

    List<PositionDelete<Record>> matches = Lists.newArrayList();
    long recordsScanned = 0;
    boolean earlyTermination = false;

    PageReadStore pages = reader.readNextRowGroup();
    model.setPageSource(pages);

    for (long i = 0; i < rgRowCount; i++) {
      Record record = model.read(null);
      if (record == null) {
        rowPosition++;
        continue;
      }
      recordsScanned++;
      Object val = record.get(0);
      if (val == null) {
        rowPosition++;
        continue;
      }

      long dataKey = val instanceof Integer ? ((Integer) val).longValue() : (Long) val;

      // Move delete pointer while deleteKey < dataKey
      while (deletePtr < sortedDeleteKeys.size() && sortedDeleteKeys.get(deletePtr) < dataKey) {
        deletePtr++;
      }

      // Early termination if all delete keys exhausted
      if (deletePtr >= sortedDeleteKeys.size()) {
        earlyTermination = true;
        rowPosition++;
        break;
      }

      // Early termination if current delete key > row group max
      if (sortedDeleteKeys.get(deletePtr) > rgMax) {
        // Skip remaining rows in this row group
        rowPosition += (rgRowCount - i);
        // If this is the last row group, mark as file-level early termination
        if (isLastRowGroup) {
          earlyTermination = true;
        }
        break;
      }

      // Match: deleteKey == dataKey
      if (sortedDeleteKeys.get(deletePtr).equals(dataKey)) {
        PositionDelete<Record> posDelete = PositionDelete.create();
        posDelete.set(dataFilePath, rowPosition, null);
        matches.add(posDelete);
      }
      rowPosition++;
    }

    return new ProcessRowGroupResult(
        matches, recordsScanned, deletePtr, rowPosition, earlyTermination);
  }
}
