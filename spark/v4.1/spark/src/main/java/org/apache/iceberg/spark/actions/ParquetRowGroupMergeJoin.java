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
import java.util.Arrays;
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
import org.apache.iceberg.parquet.ParquetSchemaUtil;
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
   * Perform merge join with row group level control for Long keys using primitive array.
   */
  static Result execute(
      InputFile inputFile,
      Schema projectionSchema,
      long[] sortedDeleteKeys,
      int fromIndex,
      int toIndex,
      int eqDeleteFieldId,
      String eqColumnName,
      String dataFilePath,
      Expression filter)
      throws IOException {

    return new LongMergeJoiner(
            projectionSchema,
            eqColumnName,
            dataFilePath,
            filter,
            sortedDeleteKeys,
            fromIndex,
            toIndex)
        .execute(inputFile);
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

    BigDecimal[] keys = sortedDeleteKeys.toArray(new BigDecimal[0]);
    return executeDecimal(
        inputFile,
        projectionSchema,
        keys,
        0,
        keys.length,
        eqDeleteFieldId,
        eqColumnName,
        dataFilePath,
        filter);
  }

  /**
   * Perform merge join with row group level control for BigDecimal keys using array.
   */
  static Result executeDecimal(
      InputFile inputFile,
      Schema projectionSchema,
      BigDecimal[] sortedDeleteKeys,
      int fromIndex,
      int toIndex,
      int eqDeleteFieldId,
      String eqColumnName,
      String dataFilePath,
      Expression filter)
      throws IOException {

    return new DecimalMergeJoiner(
            projectionSchema,
            eqColumnName,
            dataFilePath,
            filter,
            sortedDeleteKeys,
            fromIndex,
            toIndex)
        .execute(inputFile);
  }

  private static class Bounds<T extends Comparable<T>> {
    final T min;
    final T max;

    Bounds(T min, T max) {
      this.min = min;
      this.max = max;
    }
  }

  private abstract static class BaseMergeJoiner<T extends Comparable<T>> {
    protected final Schema projectionSchema;
    protected final String eqColumnName;
    protected final String dataFilePath;
    protected final Expression filter;

    BaseMergeJoiner(
        Schema projectionSchema, String eqColumnName, String dataFilePath, Expression filter) {
      this.projectionSchema = projectionSchema;
      this.eqColumnName = eqColumnName;
      this.dataFilePath = dataFilePath;
      this.filter = filter;
    }

    abstract int startIndex();

    abstract int endIndex();

    abstract T minKey();

    abstract T maxKey();

    abstract Bounds<T> extractBounds(
        org.apache.parquet.column.statistics.Statistics<?> stats, PrimitiveType primitiveType);

    abstract int compareKeyToValue(int keyIndex, T value);

    abstract int binarySearch(int fromIndex, int toIndex, T value);

    abstract RowGroupResult processRowGroupNoStats(
        ParquetFileReader reader,
        ParquetValueReader<Record> model,
        long rgRowCount,
        int deletePtr,
        long rowPosition)
        throws IOException;

    abstract RowGroupResult processRowGroupWithStats(
        ParquetFileReader reader,
        ParquetValueReader<Record> model,
        long rgRowCount,
        int deletePtr,
        long rowPosition,
        T rgMax,
        boolean isLastRowGroup)
        throws IOException;

    Result execute(InputFile inputFile) throws IOException {
      List<PositionDelete<Record>> matches = Lists.newArrayList();
      long recordsScanned = 0;

      if (startIndex() >= endIndex()) {
        return new Result(matches, 0);
      }

      T minDeleteKey = minKey();
      T maxDeleteKey = maxKey();

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

        // Project only the columns we need (eq delete column) to avoid reading entire file
        MessageType projectedSchema = ParquetSchemaUtil.pruneColumns(fileSchema, readSchema);
        reader.setRequestedSchema(projectedSchema);

        // Create Iceberg reader for records (without ROW_POSITION)
        ParquetValueReader<Record> model =
            (ParquetValueReader<Record>) GenericParquetReaders.buildReader(readSchema, projectedSchema);

        int deletePtr = startIndex(); // Shared across row groups for efficiency
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
            RowGroupResult result = processRowGroupNoStats(reader, model, rgRowCount, deletePtr, rowPosition);
            recordsScanned += result.recordsScanned;
            matches.addAll(result.matches);
            deletePtr = result.deletePtr;
            rowPosition = result.rowPosition;
            if (result.earlyTermination) break;
            continue;
          }

          Bounds<T> bounds = extractBounds(stats, primitiveType);
          if (bounds == null) {
            // Statistics type doesn't match expected type
            RowGroupResult result = processRowGroupNoStats(reader, model, rgRowCount, deletePtr, rowPosition);
            recordsScanned += result.recordsScanned;
            matches.addAll(result.matches);
            deletePtr = result.deletePtr;
            rowPosition = result.rowPosition;
            if (result.earlyTermination) break;
            continue;
          }
          T rgMin = bounds.min;
          T rgMax = bounds.max;

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
          if (deletePtr < endIndex() && compareKeyToValue(deletePtr, rgMax) > 0) {
            reader.skipNextRowGroup();
            rowPosition += rgRowCount;
            continue;
          }

          // Binary search to find starting deletePtr for this row group
          if (deletePtr < endIndex() && compareKeyToValue(deletePtr, rgMin) < 0) {
            int searchResult = binarySearch(deletePtr, endIndex(), rgMin);
            if (searchResult < 0) {
              deletePtr = -(searchResult + 1);
            } else {
              deletePtr = searchResult;
            }
          }

          // Check again after binary search
          if (deletePtr >= endIndex()) {
            reader.skipNextRowGroup();
            rowPosition += rgRowCount;
            break;
          }

          if (compareKeyToValue(deletePtr, rgMax) > 0) {
            reader.skipNextRowGroup();
            rowPosition += rgRowCount;
            continue;
          }

          // Read row group
          RowGroupResult result =
              processRowGroupWithStats(
                  reader,
                  model,
                  rgRowCount,
                  deletePtr,
                  rowPosition,
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
  }

  private static final class GenericMergeJoiner<T extends Comparable<T>> extends BaseMergeJoiner<T> {
    private final List<T> sortedDeleteKeys;
    private final Function<Object, T> keyExtractor;
    private final StatsExtractor<T> statsExtractor;

    GenericMergeJoiner(
        Schema projectionSchema,
        String eqColumnName,
        String dataFilePath,
        Expression filter,
        List<T> sortedDeleteKeys,
        Function<Object, T> keyExtractor,
        StatsExtractor<T> statsExtractor) {
      super(projectionSchema, eqColumnName, dataFilePath, filter);
      this.sortedDeleteKeys = sortedDeleteKeys;
      this.keyExtractor = keyExtractor;
      this.statsExtractor = statsExtractor;
    }

    @Override
    int startIndex() {
      return 0;
    }

    @Override
    int endIndex() {
      return sortedDeleteKeys.size();
    }

    @Override
    T minKey() {
      return sortedDeleteKeys.get(0);
    }

    @Override
    T maxKey() {
      return sortedDeleteKeys.get(sortedDeleteKeys.size() - 1);
    }

    @Override
    Bounds<T> extractBounds(org.apache.parquet.column.statistics.Statistics<?> stats, PrimitiveType primitiveType) {
      T[] bounds = statsExtractor.extract(stats, primitiveType);
      if (bounds == null) {
        return null;
      }
      return new Bounds<>(bounds[0], bounds[1]);
    }

    @Override
    int compareKeyToValue(int keyIndex, T value) {
      return sortedDeleteKeys.get(keyIndex).compareTo(value);
    }

    @Override
    int binarySearch(int fromIndex, int toIndex, T value) {
      return binarySearchList(sortedDeleteKeys, fromIndex, toIndex, value);
    }

    @Override
    RowGroupResult processRowGroupNoStats(
        ParquetFileReader reader,
        ParquetValueReader<Record> model,
        long rgRowCount,
        int deletePtr,
        long rowPosition)
        throws IOException {
      return ParquetRowGroupMergeJoin.processRowGroupNoStats(
          reader,
          model,
          rgRowCount,
          sortedDeleteKeys,
          deletePtr,
          endIndex(),
          rowPosition,
          dataFilePath,
          keyExtractor);
    }

    @Override
    RowGroupResult processRowGroupWithStats(
        ParquetFileReader reader,
        ParquetValueReader<Record> model,
        long rgRowCount,
        int deletePtr,
        long rowPosition,
        T rgMax,
        boolean isLastRowGroup)
        throws IOException {
      return ParquetRowGroupMergeJoin.processRowGroupWithStats(
          reader,
          model,
          rgRowCount,
          sortedDeleteKeys,
          deletePtr,
          endIndex(),
          rowPosition,
          dataFilePath,
          rgMax,
          isLastRowGroup,
          keyExtractor);
    }
  }

  private static final class DecimalMergeJoiner extends BaseMergeJoiner<BigDecimal> {
    private static final Function<Object, BigDecimal> DECIMAL_EXTRACTOR = val -> (BigDecimal) val;

    private final BigDecimal[] sortedDeleteKeys;
    private final int fromIndex;
    private final int toIndex;

    DecimalMergeJoiner(
        Schema projectionSchema,
        String eqColumnName,
        String dataFilePath,
        Expression filter,
        BigDecimal[] sortedDeleteKeys,
        int fromIndex,
        int toIndex) {
      super(projectionSchema, eqColumnName, dataFilePath, filter);
      this.sortedDeleteKeys = sortedDeleteKeys;
      this.fromIndex = fromIndex;
      this.toIndex = toIndex;
    }

    @Override
    int startIndex() {
      return fromIndex;
    }

    @Override
    int endIndex() {
      return toIndex;
    }

    @Override
    BigDecimal minKey() {
      return sortedDeleteKeys[fromIndex];
    }

    @Override
    BigDecimal maxKey() {
      return sortedDeleteKeys[toIndex - 1];
    }

    @Override
    Bounds<BigDecimal> extractBounds(
        org.apache.parquet.column.statistics.Statistics<?> stats, PrimitiveType primitiveType) {
      return extractDecimalBounds(stats, primitiveType);
    }

    @Override
    int compareKeyToValue(int keyIndex, BigDecimal value) {
      return sortedDeleteKeys[keyIndex].compareTo(value);
    }

    @Override
    int binarySearch(int fromIndex, int toIndex, BigDecimal value) {
      return Arrays.binarySearch(sortedDeleteKeys, fromIndex, toIndex, value);
    }

    @Override
    RowGroupResult processRowGroupNoStats(
        ParquetFileReader reader,
        ParquetValueReader<Record> model,
        long rgRowCount,
        int deletePtr,
        long rowPosition)
        throws IOException {
      return ParquetRowGroupMergeJoin.processRowGroupNoStatsArray(
          reader,
          model,
          rgRowCount,
          sortedDeleteKeys,
          deletePtr,
          toIndex,
          rowPosition,
          dataFilePath,
          DECIMAL_EXTRACTOR);
    }

    @Override
    RowGroupResult processRowGroupWithStats(
        ParquetFileReader reader,
        ParquetValueReader<Record> model,
        long rgRowCount,
        int deletePtr,
        long rowPosition,
        BigDecimal rgMax,
        boolean isLastRowGroup)
        throws IOException {
      return ParquetRowGroupMergeJoin.processRowGroupWithStatsArray(
          reader,
          model,
          rgRowCount,
          sortedDeleteKeys,
          deletePtr,
          toIndex,
          rowPosition,
          dataFilePath,
          rgMax,
          isLastRowGroup,
          DECIMAL_EXTRACTOR);
    }
  }

  private static final class LongMergeJoiner extends BaseMergeJoiner<Long> {
    private final long[] sortedDeleteKeys;
    private final int fromIndex;
    private final int toIndex;

    LongMergeJoiner(
        Schema projectionSchema,
        String eqColumnName,
        String dataFilePath,
        Expression filter,
        long[] sortedDeleteKeys,
        int fromIndex,
        int toIndex) {
      super(projectionSchema, eqColumnName, dataFilePath, filter);
      this.sortedDeleteKeys = sortedDeleteKeys;
      this.fromIndex = fromIndex;
      this.toIndex = toIndex;
    }

    @Override
    int startIndex() {
      return fromIndex;
    }

    @Override
    int endIndex() {
      return toIndex;
    }

    @Override
    Long minKey() {
      return sortedDeleteKeys[fromIndex];
    }

    @Override
    Long maxKey() {
      return sortedDeleteKeys[toIndex - 1];
    }

    @Override
    Bounds<Long> extractBounds(
        org.apache.parquet.column.statistics.Statistics<?> stats, PrimitiveType primitiveType) {
      return extractLongBounds(stats);
    }

    @Override
    int compareKeyToValue(int keyIndex, Long value) {
      return Long.compare(sortedDeleteKeys[keyIndex], value);
    }

    @Override
    int binarySearch(int fromIndex, int toIndex, Long value) {
      return Arrays.binarySearch(sortedDeleteKeys, fromIndex, toIndex, value);
    }

    @Override
    RowGroupResult processRowGroupNoStats(
        ParquetFileReader reader,
        ParquetValueReader<Record> model,
        long rgRowCount,
        int deletePtr,
        long rowPosition)
        throws IOException {
      return ParquetRowGroupMergeJoin.processRowGroupNoStatsLong(
          reader, model, rgRowCount, sortedDeleteKeys, deletePtr, toIndex, rowPosition, dataFilePath);
    }

    @Override
    RowGroupResult processRowGroupWithStats(
        ParquetFileReader reader,
        ParquetValueReader<Record> model,
        long rgRowCount,
        int deletePtr,
        long rowPosition,
        Long rgMax,
        boolean isLastRowGroup)
        throws IOException {
      return ParquetRowGroupMergeJoin.processRowGroupWithStatsLong(
          reader,
          model,
          rgRowCount,
          sortedDeleteKeys,
          deletePtr,
          toIndex,
          rowPosition,
          dataFilePath,
          rgMax,
          isLastRowGroup);
    }
  }

  private static Bounds<BigDecimal> extractDecimalBounds(
      org.apache.parquet.column.statistics.Statistics<?> stats, PrimitiveType primitiveType) {
    BigDecimal min = convertDecimalStatistic(stats.genericGetMin(), primitiveType);
    BigDecimal max = convertDecimalStatistic(stats.genericGetMax(), primitiveType);
    if (min != null && max != null) {
      return new Bounds<>(min, max);
    }
    return null;
  }

  private static <T extends Comparable<T>> int binarySearchList(
      List<T> list, int fromIndex, int toIndex, T value) {
    int low = fromIndex;
    int high = toIndex - 1;
    while (low <= high) {
      int mid = (low + high) >>> 1;
      int cmp = list.get(mid).compareTo(value);
      if (cmp < 0) {
        low = mid + 1;
      } else if (cmp > 0) {
        high = mid - 1;
      } else {
        return mid;
      }
    }
    return -(low + 1);
  }

  private static Bounds<Long> extractLongBounds(
      org.apache.parquet.column.statistics.Statistics<?> stats) {
    Object minObj = stats.genericGetMin();
    Object maxObj = stats.genericGetMax();
    if (minObj instanceof Number && maxObj instanceof Number) {
      return new Bounds<>(((Number) minObj).longValue(), ((Number) maxObj).longValue());
    }
    return null;
  }

  private static <T extends Comparable<T>> RowGroupResult processRowGroupNoStatsArray(
      ParquetFileReader reader,
      ParquetValueReader<Record> model,
      long rgRowCount,
      T[] sortedDeleteKeys,
      int deletePtr,
      int endIndex,
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

      while (deletePtr < endIndex && sortedDeleteKeys[deletePtr].compareTo(dataKey) < 0) {
        deletePtr++;
      }

      if (deletePtr >= endIndex) {
        earlyTermination = true;
        rowPosition++;
        break;
      }

      if (sortedDeleteKeys[deletePtr].compareTo(dataKey) == 0) {
        PositionDelete<Record> posDelete = PositionDelete.create();
        posDelete.set(dataFilePath, rowPosition, null);
        matches.add(posDelete);
      }
      rowPosition++;
    }

    return new RowGroupResult(matches, recordsScanned, deletePtr, rowPosition, earlyTermination);
  }

  private static <T extends Comparable<T>> RowGroupResult processRowGroupWithStatsArray(
      ParquetFileReader reader,
      ParquetValueReader<Record> model,
      long rgRowCount,
      T[] sortedDeleteKeys,
      int deletePtr,
      int endIndex,
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
      while (deletePtr < endIndex && sortedDeleteKeys[deletePtr].compareTo(dataKey) < 0) {
        deletePtr++;
      }

      // Early termination if all delete keys exhausted
      if (deletePtr >= endIndex) {
        earlyTermination = true;
        rowPosition++;
        break;
      }

      // Early termination if current delete key > row group max
      if (sortedDeleteKeys[deletePtr].compareTo(rgMax) > 0) {
        // Skip remaining rows in this row group
        rowPosition += (rgRowCount - i);
        // If this is the last row group, mark as file-level early termination
        if (isLastRowGroup) {
          earlyTermination = true;
        }
        break;
      }

      // Match: deleteKey == dataKey
      if (sortedDeleteKeys[deletePtr].compareTo(dataKey) == 0) {
        PositionDelete<Record> posDelete = PositionDelete.create();
        posDelete.set(dataFilePath, rowPosition, null);
        matches.add(posDelete);
      }
      rowPosition++;
    }

    return new RowGroupResult(matches, recordsScanned, deletePtr, rowPosition, earlyTermination);
  }

  private static RowGroupResult processRowGroupNoStatsLong(
      ParquetFileReader reader,
      ParquetValueReader<Record> model,
      long rgRowCount,
      long[] sortedDeleteKeys,
      int deletePtr,
      int toIndex,
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

      while (deletePtr < toIndex && sortedDeleteKeys[deletePtr] < dataKey) {
        deletePtr++;
      }

      if (deletePtr >= toIndex) {
        earlyTermination = true;
        rowPosition++;
        break;
      }

      if (sortedDeleteKeys[deletePtr] == dataKey) {
        PositionDelete<Record> posDelete = PositionDelete.create();
        posDelete.set(dataFilePath, rowPosition, null);
        matches.add(posDelete);
      }
      rowPosition++;
    }

    return new RowGroupResult(matches, recordsScanned, deletePtr, rowPosition, earlyTermination);
  }

  private static RowGroupResult processRowGroupWithStatsLong(
      ParquetFileReader reader,
      ParquetValueReader<Record> model,
      long rgRowCount,
      long[] sortedDeleteKeys,
      int deletePtr,
      int toIndex,
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
      while (deletePtr < toIndex && sortedDeleteKeys[deletePtr] < dataKey) {
        deletePtr++;
      }

      // Early termination if all delete keys exhausted
      if (deletePtr >= toIndex) {
        earlyTermination = true;
        rowPosition++;
        break;
      }

      // Early termination if current delete key > row group max
      if (sortedDeleteKeys[deletePtr] > rgMax) {
        // Skip remaining rows in this row group
        rowPosition += (rgRowCount - i);
        // If this is the last row group, mark as file-level early termination
        if (isLastRowGroup) {
          earlyTermination = true;
        }
        break;
      }

      // Match: deleteKey == dataKey
      if (sortedDeleteKeys[deletePtr] == dataKey) {
        PositionDelete<Record> posDelete = PositionDelete.create();
        posDelete.set(dataFilePath, rowPosition, null);
        matches.add(posDelete);
      }
      rowPosition++;
    }

    return new RowGroupResult(matches, recordsScanned, deletePtr, rowPosition, earlyTermination);
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
    return new GenericMergeJoiner<>(
            projectionSchema,
            eqColumnName,
            dataFilePath,
            filter,
            sortedDeleteKeys,
            keyExtractor,
            statsExtractor)
        .execute(inputFile);
  }

  private static class RowGroupResult {
    final List<PositionDelete<Record>> matches;
    final long recordsScanned;
    final int deletePtr;
    final long rowPosition;
    final boolean earlyTermination;

    RowGroupResult(
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

  private static <T extends Comparable<T>> RowGroupResult processRowGroupNoStats(
      ParquetFileReader reader,
      ParquetValueReader<Record> model,
      long rgRowCount,
      List<T> sortedDeleteKeys,
      int deletePtr,
      int endIndex,
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

      while (deletePtr < endIndex && sortedDeleteKeys.get(deletePtr).compareTo(dataKey) < 0) {
        deletePtr++;
      }

      if (deletePtr >= endIndex) {
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

    return new RowGroupResult(matches, recordsScanned, deletePtr, rowPosition, earlyTermination);
  }

  private static <T extends Comparable<T>> RowGroupResult processRowGroupWithStats(
      ParquetFileReader reader,
      ParquetValueReader<Record> model,
      long rgRowCount,
      List<T> sortedDeleteKeys,
      int deletePtr,
      int endIndex,
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
      while (deletePtr < endIndex && sortedDeleteKeys.get(deletePtr).compareTo(dataKey) < 0) {
        deletePtr++;
      }

      // Early termination if all delete keys exhausted
      if (deletePtr >= endIndex) {
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

    return new RowGroupResult(matches, recordsScanned, deletePtr, rowPosition, earlyTermination);
  }
}
