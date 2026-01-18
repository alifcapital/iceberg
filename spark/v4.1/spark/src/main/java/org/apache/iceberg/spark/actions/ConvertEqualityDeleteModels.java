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

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Model classes for convert equality delete files action.
 *
 * <p>These classes are serializable and used to transfer data between driver and executors.
 */
final class ConvertEqualityDeleteModels {

  private ConvertEqualityDeleteModels() {}

  /** Serializable data file info for distribution to executors. */
  static class DataFileInfo implements Serializable {
    private final String path;
    private final String format;
    private final long fileSizeInBytes;
    private final long recordCount;
    private final int specId;
    private final Object[] partitionValues;
    private final Integer sortOrderId;
    private final Map<Integer, byte[]> lowerBounds;
    private final Map<Integer, byte[]> upperBounds;

    DataFileInfo(
        String path,
        String format,
        long fileSizeInBytes,
        long recordCount,
        int specId,
        StructLike partition,
        int partitionSize,
        Integer sortOrderId,
        Map<Integer, byte[]> lowerBounds,
        Map<Integer, byte[]> upperBounds) {
      this.path = path;
      this.format = format;
      this.fileSizeInBytes = fileSizeInBytes;
      this.recordCount = recordCount;
      this.specId = specId;
      this.sortOrderId = sortOrderId;
      this.lowerBounds = lowerBounds;
      this.upperBounds = upperBounds;
      if (partition != null && partitionSize > 0) {
        this.partitionValues = new Object[partitionSize];
        for (int i = 0; i < partitionSize; i++) {
          this.partitionValues[i] = partition.get(i, Object.class);
        }
      } else {
        this.partitionValues = null;
      }
    }

    String path() {
      return path;
    }

    FileFormat format() {
      return FileFormat.fromString(format);
    }

    long fileSizeInBytes() {
      return fileSizeInBytes;
    }

    long recordCount() {
      return recordCount;
    }

    int specId() {
      return specId;
    }

    Object[] partitionValues() {
      return partitionValues;
    }

    Integer sortOrderId() {
      return sortOrderId;
    }

    Map<Integer, byte[]> lowerBounds() {
      return lowerBounds;
    }

    Map<Integer, byte[]> upperBounds() {
      return upperBounds;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      DataFileInfo that = (DataFileInfo) o;
      return path.equals(that.path);
    }

    @Override
    public int hashCode() {
      return path.hashCode();
    }
  }

  /** Serializable wrapper for partition values. */
  static class PartitionWrapper implements StructLike, Serializable {
    private final Object[] values;

    PartitionWrapper(Object[] values) {
      this.values = values;
    }

    @Override
    public int size() {
      return values != null ? values.length : 0;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(values[pos]);
    }

    @Override
    public <T> void set(int pos, T value) {
      values[pos] = value;
    }
  }

  /** Serializable delete file metadata returned from executors. */
  static class DeleteFileInfo implements Serializable {
    private final String path;
    private final String format;
    private final long fileSizeInBytes;
    private final long recordCount;
    private final int specId;
    private final Object[] partitionValues;
    private final Map<Integer, byte[]> lowerBounds;
    private final Map<Integer, byte[]> upperBounds;

    DeleteFileInfo(
        String path,
        String format,
        long fileSizeInBytes,
        long recordCount,
        int specId,
        Object[] partitionValues,
        Map<Integer, byte[]> lowerBounds,
        Map<Integer, byte[]> upperBounds) {
      this.path = path;
      this.format = format;
      this.fileSizeInBytes = fileSizeInBytes;
      this.recordCount = recordCount;
      this.specId = specId;
      this.partitionValues = partitionValues;
      this.lowerBounds = lowerBounds;
      this.upperBounds = upperBounds;
    }

    static DeleteFileInfo from(DeleteFile deleteFile, Object[] partitionValues) {
      Map<Integer, byte[]> lowerBounds = null;
      Map<Integer, byte[]> upperBounds = null;

      if (deleteFile.lowerBounds() != null) {
        lowerBounds = Maps.newHashMap();
        for (Map.Entry<Integer, ByteBuffer> entry : deleteFile.lowerBounds().entrySet()) {
          ByteBuffer buffer = entry.getValue().duplicate();
          byte[] bytes = new byte[buffer.remaining()];
          buffer.get(bytes);
          lowerBounds.put(entry.getKey(), bytes);
        }
      }

      if (deleteFile.upperBounds() != null) {
        upperBounds = Maps.newHashMap();
        for (Map.Entry<Integer, ByteBuffer> entry : deleteFile.upperBounds().entrySet()) {
          ByteBuffer buffer = entry.getValue().duplicate();
          byte[] bytes = new byte[buffer.remaining()];
          buffer.get(bytes);
          upperBounds.put(entry.getKey(), bytes);
        }
      }

      return new DeleteFileInfo(
          deleteFile.path().toString(),
          deleteFile.format().name(),
          deleteFile.fileSizeInBytes(),
          deleteFile.recordCount(),
          deleteFile.specId(),
          partitionValues,
          lowerBounds,
          upperBounds);
    }

    String path() {
      return path;
    }

    long fileSizeInBytes() {
      return fileSizeInBytes;
    }

    long recordCount() {
      return recordCount;
    }

    int specId() {
      return specId;
    }

    Object[] partitionValues() {
      return partitionValues;
    }

    Map<Integer, byte[]> lowerBounds() {
      return lowerBounds;
    }

    Map<Integer, byte[]> upperBounds() {
      return upperBounds;
    }
  }

  /** Group of delete files sharing the same partition spec. */
  static class DeleteFileGroup {
    private final List<DeleteFile> deleteFiles;
    private final PartitionSpec spec;

    DeleteFileGroup(List<DeleteFile> deleteFiles, PartitionSpec spec) {
      this.deleteFiles = ImmutableList.copyOf(deleteFiles);
      this.spec = spec;
    }

    List<DeleteFile> deleteFiles() {
      return deleteFiles;
    }

    PartitionSpec spec() {
      return spec;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      DeleteFileGroup that = (DeleteFileGroup) o;
      Set<String> thisPaths =
          deleteFiles.stream().map(f -> f.path().toString()).collect(Collectors.toSet());
      Set<String> thatPaths =
          that.deleteFiles.stream().map(f -> f.path().toString()).collect(Collectors.toSet());
      return thisPaths.equals(thatPaths) && spec.specId() == that.spec.specId();
    }

    @Override
    public int hashCode() {
      Set<String> paths =
          deleteFiles.stream().map(f -> f.path().toString()).collect(Collectors.toSet());
      return paths.hashCode() * 31 + spec.specId();
    }
  }

  /** Result of a single group conversion. */
  static class ConversionResult {
    final Set<DeleteFile> posDeleteFiles;
    final long eqDeleteRecordsCount;
    final long posDeleteRecordsCount;

    ConversionResult(
        Set<DeleteFile> posDeleteFiles, long eqDeleteRecordsCount, long posDeleteRecordsCount) {
      this.posDeleteFiles = posDeleteFiles;
      this.eqDeleteRecordsCount = eqDeleteRecordsCount;
      this.posDeleteRecordsCount = posDeleteRecordsCount;
    }
  }

  /**
   * Container for equality delete keys read on driver and broadcast to executors.
   *
   * <p>Supports LONG, DECIMAL, STRING single-column keys and MULTI_COLUMN composite keys.
   */
  static class EqDeleteKeys implements Serializable {

    enum KeyType {
      LONG,
      DECIMAL,
      STRING,
      MULTI_COLUMN
    }

    private final KeyType keyType;
    private final Set<Long> longKeys;
    private final Set<BigDecimal> decimalKeys;
    private final Set<String> stringKeys;
    private final Set<List<Object>> multiColumnKeys;
    // Transient - not serialized, only used on driver for metrics
    private transient long readTimeMs;

    private EqDeleteKeys(
        KeyType keyType,
        Set<Long> longKeys,
        Set<BigDecimal> decimalKeys,
        Set<String> stringKeys,
        Set<List<Object>> multiColumnKeys) {
      this.keyType = keyType;
      this.longKeys = longKeys;
      this.decimalKeys = decimalKeys;
      this.stringKeys = stringKeys;
      this.multiColumnKeys = multiColumnKeys;
      this.readTimeMs = 0;
    }

    static EqDeleteKeys ofLong(Set<Long> keys) {
      return new EqDeleteKeys(KeyType.LONG, keys, null, null, null);
    }

    static EqDeleteKeys ofDecimal(Set<BigDecimal> keys) {
      return new EqDeleteKeys(KeyType.DECIMAL, null, keys, null, null);
    }

    static EqDeleteKeys ofString(Set<String> keys) {
      return new EqDeleteKeys(KeyType.STRING, null, null, keys, null);
    }

    static EqDeleteKeys ofMultiColumn(Set<List<Object>> keys) {
      return new EqDeleteKeys(KeyType.MULTI_COLUMN, null, null, null, keys);
    }

    void setReadTimeMs(long readTimeMs) {
      this.readTimeMs = readTimeMs;
    }

    long readTimeMs() {
      return readTimeMs;
    }

    KeyType keyType() {
      return keyType;
    }

    Set<Long> longKeys() {
      return longKeys;
    }

    Set<BigDecimal> decimalKeys() {
      return decimalKeys;
    }

    Set<String> stringKeys() {
      return stringKeys;
    }

    Set<List<Object>> multiColumnKeys() {
      return multiColumnKeys;
    }

    int size() {
      switch (keyType) {
        case LONG:
          return longKeys.size();
        case DECIMAL:
          return decimalKeys.size();
        case STRING:
          return stringKeys.size();
        case MULTI_COLUMN:
          return multiColumnKeys.size();
        default:
          return 0;
      }
    }

    boolean isEmpty() {
      return size() == 0;
    }
  }

  /**
   * Result from a single partition task containing delete files and metrics.
   * Used to return metrics from executors without accumulators.
   */
  static class TaskResult implements Serializable {
    private final List<DeleteFileInfo> deleteFiles;
    private final long dataFileReadTimeMs;
    private final long posDeleteWriteTimeMs;
    private final long posDeleteRecordsWritten;
    private final long filesProcessed;
    private final long filesSkipped;
    private final long dataFileBytesRead;
    private final long dataRecordsScanned;
    private final long dataRecordsTotal;

    TaskResult(
        List<DeleteFileInfo> deleteFiles,
        long dataFileReadTimeMs,
        long posDeleteWriteTimeMs,
        long posDeleteRecordsWritten,
        long filesProcessed,
        long filesSkipped,
        long dataFileBytesRead,
        long dataRecordsScanned,
        long dataRecordsTotal) {
      this.deleteFiles = deleteFiles;
      this.dataFileReadTimeMs = dataFileReadTimeMs;
      this.posDeleteWriteTimeMs = posDeleteWriteTimeMs;
      this.posDeleteRecordsWritten = posDeleteRecordsWritten;
      this.filesProcessed = filesProcessed;
      this.filesSkipped = filesSkipped;
      this.dataFileBytesRead = dataFileBytesRead;
      this.dataRecordsScanned = dataRecordsScanned;
      this.dataRecordsTotal = dataRecordsTotal;
    }

    List<DeleteFileInfo> deleteFiles() {
      return deleteFiles;
    }

    long dataFileReadTimeMs() {
      return dataFileReadTimeMs;
    }

    long posDeleteWriteTimeMs() {
      return posDeleteWriteTimeMs;
    }

    long posDeleteRecordsWritten() {
      return posDeleteRecordsWritten;
    }

    long filesProcessed() {
      return filesProcessed;
    }

    long filesSkipped() {
      return filesSkipped;
    }

    long dataFileBytesRead() {
      return dataFileBytesRead;
    }

    long dataRecordsScanned() {
      return dataRecordsScanned;
    }

    long dataRecordsTotal() {
      return dataRecordsTotal;
    }
  }
}
