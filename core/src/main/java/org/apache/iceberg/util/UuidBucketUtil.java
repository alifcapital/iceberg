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
package org.apache.iceberg.util;

/**
 * Utility methods for UUID prefix bucketing.
 *
 * <p>UUID prefix bucketing partitions data by the first 3 hex characters of UUID values (000-fff =
 * 0-4095). This provides deterministic distribution without sampling, which works better than
 * Spark's range partitioner for UUID data.
 *
 * <p>Bucket formula: bucket = floor(hexValue * numBuckets / 4096)
 */
public class UuidBucketUtil {

  public static final int MIN_BUCKETS = 2;
  public static final int MAX_BUCKETS = 1024;
  public static final int HEX_SPACE = 4096;

  private UuidBucketUtil() {}

  /**
   * Computes the number of buckets based on table size and target file size.
   *
   * <p>Returns the smallest power of 2 that is >= optimalFileCount, clamped to [MIN_BUCKETS,
   * MAX_BUCKETS].
   *
   * @param totalSize total size of files in bytes
   * @param targetFileSize target file size in bytes
   * @return number of buckets, or 0 if inputs are invalid
   */
  public static int computeBuckets(long totalSize, long targetFileSize) {
    if (totalSize <= 0 || targetFileSize <= 0) {
      return 0;
    }

    long optimalFileCount = Math.max(1, totalSize / targetFileSize);
    int power = Math.max(0, (int) Math.ceil(Math.log((double) optimalFileCount) / Math.log(2)));
    return Math.max(MIN_BUCKETS, Math.min(MAX_BUCKETS, 1 << power));
  }

  /**
   * Returns the lower hex boundary (0-4095) for a bucket.
   *
   * <p>For numBuckets=16: bucket 0 -> 0, bucket 1 -> 256, bucket 15 -> 3840
   *
   * @param bucket bucket number (0 to numBuckets-1)
   * @param numBuckets total number of buckets
   * @return lower hex boundary (inclusive)
   */
  public static int bucketLowerHex(int bucket, int numBuckets) {
    return bucket * HEX_SPACE / numBuckets;
  }

  /**
   * Returns the upper hex boundary (0-4095) for a bucket.
   *
   * <p>For numBuckets=16: bucket 0 -> 255, bucket 1 -> 511, bucket 15 -> 4095
   *
   * @param bucket bucket number (0 to numBuckets-1)
   * @param numBuckets total number of buckets
   * @return upper hex boundary (inclusive)
   */
  public static int bucketUpperHex(int bucket, int numBuckets) {
    return (bucket + 1) * HEX_SPACE / numBuckets - 1;
  }

  /**
   * Returns the bucket number for a given hex prefix value.
   *
   * <p>For numBuckets=16: hex 0x000-0x0ff -> bucket 0, hex 0x100-0x1ff -> bucket 1, etc.
   *
   * @param hexValue hex prefix value (0-4095)
   * @param numBuckets total number of buckets
   * @return bucket number (0 to numBuckets-1)
   */
  public static int hexToBucket(int hexValue, int numBuckets) {
    return hexValue * numBuckets / HEX_SPACE;
  }

  /**
   * Extracts the hex prefix (first 3 characters) from a UUID string as an integer 0-4095.
   *
   * <p>Example: "01958413-cc3a-70" -> 0x019 = 25, "e061d06e-96b5-45" -> 0xe06 = 3590
   *
   * @param bound the UUID value (as Object, will be converted to String)
   * @return hex prefix as integer 0-4095, or 0 if parsing fails
   */
  public static int extractHexPrefix(Object bound) {
    if (bound == null) {
      return 0;
    }

    String s = bound.toString();
    if (s.length() < 3) {
      return 0;
    }

    try {
      return Integer.parseInt(s.substring(0, 3), 16);
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  /**
   * Checks if a string value looks like a UUID.
   *
   * <p>UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx Checks for 8 hex characters followed by a
   * dash at position 8.
   *
   * @param value the string to check
   * @return true if the value looks like a UUID
   */
  public static boolean looksLikeUuid(String value) {
    if (value == null || value.length() < 9) {
      return false;
    }

    if (value.charAt(8) != '-') {
      return false;
    }

    for (int i = 0; i < 8; i++) {
      char c = value.charAt(i);
      if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) {
        return false;
      }
    }

    return true;
  }
}
