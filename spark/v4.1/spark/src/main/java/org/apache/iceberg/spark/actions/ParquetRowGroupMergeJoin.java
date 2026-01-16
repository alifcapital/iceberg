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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.ParquetBloomRowGroupFilter;
import org.apache.iceberg.parquet.ParquetDictionaryRowGroupFilter;
import org.apache.iceberg.parquet.ParquetValueReader;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.DictionaryPageReadStore;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.BloomFilterReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

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
   * Perform merge join with row group level control for Long keys.
   */
  @SuppressWarnings("unchecked")
  static Result execute(
      InputFile inputFile,
      Schema projectionSchema,
      List<Long> sortedDeleteKeys,
      int eqDeleteFieldId,
      String eqColumnName,
      String dataFilePath,
      Expression filter)
      throws IOException {

    return executeGeneric(
        inputFile,
        projectionSchema,
        sortedDeleteKeys,
        eqColumnName,
        dataFilePath,
        filter,
        val -> val instanceof Integer ? ((Integer) val).longValue() : (Long) val,
        (stats, pt) -> {
          Object minObj = stats.genericGetMin();
          Object maxObj = stats.genericGetMax();
          if (minObj instanceof Number && maxObj instanceof Number) {
            return new Long[] {((Number) minObj).longValue(), ((Number) maxObj).longValue()};
          }
          return null;
        });
  }

  /**
   * Perform merge join with row group level control for BigDecimal keys.
   */
  @SuppressWarnings("unchecked")
  static Result executeDecimal(
      InputFile inputFile,
      Schema projectionSchema,
      List<BigDecimal> sortedDeleteKeys,
      int eqDeleteFieldId,
      String eqColumnName,
      String dataFilePath,
      Expression filter)
      throws IOException {

    return executeGeneric(
        inputFile,
        projectionSchema,
        sortedDeleteKeys,
        eqColumnName,
        dataFilePath,
        filter,
        val -> (BigDecimal) val,
        (stats, pt) -> {
          BigDecimal min = convertDecimalStatistic(stats.genericGetMin(), pt);
          BigDecimal max = convertDecimalStatistic(stats.genericGetMax(), pt);
          if (min != null && max != null) {
            return new BigDecimal[] {min, max};
          }
          return null;
        });
  }

  /** Convert Parquet statistic value to BigDecimal. */
  private static BigDecimal convertDecimalStatistic(Object value, PrimitiveType primitiveType) {
    if (value == null || primitiveType == null) {
      return null;
    }

    LogicalTypeAnnotation logicalType = primitiveType.getLogicalTypeAnnotation();
    if (!(logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation)) {
      return null;
    }

    int scale = ((LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) logicalType).getScale();

    switch (primitiveType.getPrimitiveTypeName()) {
      case INT32:
      case INT64:
        return BigDecimal.valueOf(((Number) value).longValue(), scale);
      case FIXED_LEN_BYTE_ARRAY:
      case BINARY:
        return new BigDecimal(new BigInteger(((Binary) value).getBytes()), scale);
      default:
        return null;
    }
  }

  /** Functional interface for extracting row group bounds from statistics. */
  @FunctionalInterface
  private interface StatsExtractor<T> {
    T[] extract(org.apache.parquet.column.statistics.Statistics<?> stats, PrimitiveType primitiveType);
  }

  /**
   * Generic merge join implementation for any Comparable type.
   */
  @SuppressWarnings("unchecked")
  private static <T extends Comparable<T>> Result executeGeneric(
      InputFile inputFile,
      Schema projectionSchema,
      List<T> sortedDeleteKeys,
      String eqColumnName,
      String dataFilePath,
      Expression filter,
      Function<Object, T> keyExtractor,
      StatsExtractor<T> statsExtractor)
      throws IOException {

    List<PositionDelete<Record>> matches = Lists.newArrayList();
    long recordsScanned = 0;

    if (sortedDeleteKeys.isEmpty()) {
      return new Result(matches, 0);
    }

    T minDeleteKey = sortedDeleteKeys.get(0);
    T maxDeleteKey = sortedDeleteKeys.get(sortedDeleteKeys.size() - 1);

    // Build schema without ROW_POSITION since we track position manually
    List<Types.NestedField> readFields = Lists.newArrayList();
    for (Types.NestedField field : projectionSchema.columns()) {
      if (field.fieldId() != MetadataColumns.ROW_POSITION.fieldId()) {
        readFields.add(field);
      }
    }
    Schema readSchema = new Schema(readFields);

    // Create bloom and dictionary filters if filter expression is provided
    ParquetBloomRowGroupFilter bloomFilter =
        filter != null ? new ParquetBloomRowGroupFilter(readSchema, filter, true) : null;
    ParquetDictionaryRowGroupFilter dictFilter =
        filter != null ? new ParquetDictionaryRowGroupFilter(readSchema, filter, true) : null;

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

        // Check bloom filter - skip row group if no values can match
        if (bloomFilter != null) {
          BloomFilterReader bloomReader = reader.getBloomFilterDataReader(rowGroup);
          if (bloomReader != null && !bloomFilter.shouldRead(fileSchema, rowGroup, bloomReader)) {
            reader.skipNextRowGroup();
            rowPosition += rgRowCount;
            continue;
          }
        }

        // Check dictionary filter - skip row group if dictionary doesn't contain matching values
        if (dictFilter != null) {
          DictionaryPageReadStore dictReader = reader.getDictionaryReader(rowGroup);
          if (dictReader != null && !dictFilter.shouldRead(fileSchema, rowGroup, dictReader)) {
            reader.skipNextRowGroup();
            rowPosition += rgRowCount;
            continue;
          }
        }

        // Get row group bounds for eq delete column
        ColumnChunkMetaData colMeta = null;
        PrimitiveType primitiveType = null;
        for (ColumnChunkMetaData col : rowGroup.getColumns()) {
          if (col.getPath().toDotString().equals(eqColumnName)) {
            colMeta = col;
            primitiveType = fileSchema.getColumnDescription(col.getPath().toArray()).getPrimitiveType();
            break;
          }
        }

        // Check if we have valid statistics for this row group
        org.apache.parquet.column.statistics.Statistics<?> stats =
            (colMeta != null) ? colMeta.getStatistics() : null;
        boolean hasValidStats = stats != null && stats.hasNonNullValue();

        if (!hasValidStats) {
          // No stats, must read this row group
          ProcessRowGroupResult<T> result =
              processRowGroupNoStats(
                  reader, model, rgRowCount, sortedDeleteKeys, deletePtr, rowPosition, dataFilePath, keyExtractor);
          recordsScanned += result.recordsScanned;
          matches.addAll(result.matches);
          deletePtr = result.deletePtr;
          rowPosition = result.rowPosition;
          if (result.earlyTermination) break;
          continue;
        }

        // Get row group bounds using type-specific extractor
        T[] bounds = statsExtractor.extract(stats, primitiveType);
        if (bounds == null) {
          // Statistics type doesn't match expected type
          ProcessRowGroupResult<T> result =
              processRowGroupNoStats(
                  reader, model, rgRowCount, sortedDeleteKeys, deletePtr, rowPosition, dataFilePath, keyExtractor);
          recordsScanned += result.recordsScanned;
          matches.addAll(result.matches);
          deletePtr = result.deletePtr;
          rowPosition = result.rowPosition;
          if (result.earlyTermination) break;
          continue;
        }
        T rgMin = bounds[0];
        T rgMax = bounds[1];

        // Skip row group if all delete keys < row group min (and we're done)
        if (maxDeleteKey.compareTo(rgMin) < 0) {
          reader.skipNextRowGroup();
          rowPosition += rgRowCount;
          break;
        }

        // Skip row group if all delete keys > row group max
        if (minDeleteKey.compareTo(rgMax) > 0) {
          reader.skipNextRowGroup();
          rowPosition += rgRowCount;
          continue;
        }

        // Check if current delete pointer is already past this row group
        if (deletePtr < sortedDeleteKeys.size() && sortedDeleteKeys.get(deletePtr).compareTo(rgMax) > 0) {
          reader.skipNextRowGroup();
          rowPosition += rgRowCount;
          continue;
        }

        // Binary search to find starting deletePtr for this row group
        if (deletePtr < sortedDeleteKeys.size() && sortedDeleteKeys.get(deletePtr).compareTo(rgMin) < 0) {
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

        if (sortedDeleteKeys.get(deletePtr).compareTo(rgMax) > 0) {
          reader.skipNextRowGroup();
          rowPosition += rgRowCount;
          continue;
        }

        // Read row group
        ProcessRowGroupResult<T> result =
            processRowGroupWithStats(
                reader,
                model,
                rgRowCount,
                sortedDeleteKeys,
                deletePtr,
                rowPosition,
                dataFilePath,
                rgMax,
                rgIdx == rowGroups.size() - 1,
                keyExtractor);
        recordsScanned += result.recordsScanned;
        matches.addAll(result.matches);
        deletePtr = result.deletePtr;
        rowPosition = result.rowPosition;
        if (result.earlyTermination) break;
      }
    }

    return new Result(matches, recordsScanned);
  }

  private static class ProcessRowGroupResult<T> {
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

  private static <T extends Comparable<T>> ProcessRowGroupResult<T> processRowGroupNoStats(
      ParquetFileReader reader,
      ParquetValueReader<Record> model,
      long rgRowCount,
      List<T> sortedDeleteKeys,
      int deletePtr,
      long rowPosition,
      String dataFilePath,
      Function<Object, T> keyExtractor)
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

      T dataKey = keyExtractor.apply(val);

      while (deletePtr < sortedDeleteKeys.size() && sortedDeleteKeys.get(deletePtr).compareTo(dataKey) < 0) {
        deletePtr++;
      }

      if (deletePtr >= sortedDeleteKeys.size()) {
        earlyTermination = true;
        rowPosition++;
        break;
      }

      if (sortedDeleteKeys.get(deletePtr).compareTo(dataKey) == 0) {
        PositionDelete<Record> posDelete = PositionDelete.create();
        posDelete.set(dataFilePath, rowPosition, null);
        matches.add(posDelete);
      }
      rowPosition++;
    }

    return new ProcessRowGroupResult<>(
        matches, recordsScanned, deletePtr, rowPosition, earlyTermination);
  }

  private static <T extends Comparable<T>> ProcessRowGroupResult<T> processRowGroupWithStats(
      ParquetFileReader reader,
      ParquetValueReader<Record> model,
      long rgRowCount,
      List<T> sortedDeleteKeys,
      int deletePtr,
      long rowPosition,
      String dataFilePath,
      T rgMax,
      boolean isLastRowGroup,
      Function<Object, T> keyExtractor)
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

      T dataKey = keyExtractor.apply(val);

      // Move delete pointer while deleteKey < dataKey
      while (deletePtr < sortedDeleteKeys.size() && sortedDeleteKeys.get(deletePtr).compareTo(dataKey) < 0) {
        deletePtr++;
      }

      // Early termination if all delete keys exhausted
      if (deletePtr >= sortedDeleteKeys.size()) {
        earlyTermination = true;
        rowPosition++;
        break;
      }

      // Early termination if current delete key > row group max
      if (sortedDeleteKeys.get(deletePtr).compareTo(rgMax) > 0) {
        // Skip remaining rows in this row group
        rowPosition += (rgRowCount - i);
        // If this is the last row group, mark as file-level early termination
        if (isLastRowGroup) {
          earlyTermination = true;
        }
        break;
      }

      // Match: deleteKey == dataKey
      if (sortedDeleteKeys.get(deletePtr).compareTo(dataKey) == 0) {
        PositionDelete<Record> posDelete = PositionDelete.create();
        posDelete.set(dataFilePath, rowPosition, null);
        matches.add(posDelete);
      }
      rowPosition++;
    }

    return new ProcessRowGroupResult<>(
        matches, recordsScanned, deletePtr, rowPosition, earlyTermination);
  }
}
