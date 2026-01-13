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
import java.nio.ByteBuffer;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds per-partition boundaries for global range partitioning.
 *
 * <p>Collects total size from all files in a partition, detects UUID format, and computes the
 * number of buckets for hex prefix bucketing.
 *
 * <p>V1 supports only UUID string columns. Non-UUID types fall back to standard Spark partitioning.
 */
class PartitionBoundaries implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionBoundaries.class);

  private static final int MIN_BUCKETS = 2;
  private static final int MAX_BUCKETS = 1024;

  private long totalSize;
  private int numBuckets;
  private int filesWithMissingStats;
  private String sampleBound;
  private boolean uuidDetected;

  PartitionBoundaries() {
    this.totalSize = 0;
    this.filesWithMissingStats = 0;
    this.uuidDetected = false;
  }

  /**
   * Adds a file's bounds and size to this partition's statistics.
   *
   * <p>This method is thread-safe.
   *
   * @param file the data file
   * @param fieldId the field ID of the first sort column
   * @param type the type of the first sort column
   */
  synchronized void addFile(DataFile file, int fieldId, Type type) {
    this.totalSize += file.fileSizeInBytes();

    ByteBuffer lowerBuf = file.lowerBounds() != null ? file.lowerBounds().get(fieldId) : null;

    if (lowerBuf != null) {
      Object lower = Conversions.fromByteBuffer(type, lowerBuf);

      // Save first bound for UUID detection
      if (sampleBound == null && lower instanceof CharSequence) {
        sampleBound = lower.toString();
        uuidDetected = looksLikeUuid(sampleBound);
        if (uuidDetected) {
          LOG.debug("Detected UUID format in bounds: {}", sampleBound);
        }
      }
    } else {
      filesWithMissingStats++;
    }
  }

  /**
   * Computes the number of buckets based on total size and target file size. Rounds to the nearest
   * power of two for stability between runs.
   *
   * <p>This method is thread-safe.
   *
   * @param targetFileSize the target file size in bytes
   */
  synchronized void computeNumBuckets(long targetFileSize) {
    if (!uuidDetected) {
      this.numBuckets = 0;
      LOG.debug("Non-UUID type detected, global range partitioning not applicable");
      return;
    }

    if (filesWithMissingStats > 0) {
      LOG.info(
          "Computing buckets for UUID with {} files missing column statistics",
          filesWithMissingStats);
    }

    long optimalFileCount = Math.max(1, totalSize / targetFileSize);
    int power = Math.max(0, (int) Math.ceil(Math.log(optimalFileCount) / Math.log(2)));
    this.numBuckets = Math.max(MIN_BUCKETS, Math.min(MAX_BUCKETS, 1 << power));

    LOG.debug(
        "Computed {} buckets for UUID partition, totalSize={}, targetFileSize={}",
        numBuckets,
        totalSize,
        targetFileSize);
  }

  /** Returns true if this partition has valid boundaries for UUID bucketing. */
  boolean isValid() {
    return uuidDetected && numBuckets > 0;
  }

  /** Returns true if UUID format was detected in bounds. */
  boolean isUuidType() {
    return uuidDetected;
  }

  /**
   * Detects UUID format in a bound string.
   *
   * <p>UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx Truncated bounds are supported (minimum 9
   * chars: 8 hex + dash)
   */
  private static boolean looksLikeUuid(String bound) {
    if (bound == null || bound.length() < 9) {
      return false;
    }

    // Check for dash at position 8 (UUID format: xxxxxxxx-xxxx-...)
    if (bound.charAt(8) != '-') {
      return false;
    }

    // Check that first 8 characters are hex
    for (int i = 0; i < 8; i++) {
      char c = bound.charAt(i);
      if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) {
        return false;
      }
    }

    return true;
  }

  int numBuckets() {
    return numBuckets;
  }
}
