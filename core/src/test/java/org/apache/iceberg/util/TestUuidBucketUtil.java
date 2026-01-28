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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class TestUuidBucketUtil {

  @Test
  public void testComputeBucketsBasic() {
    // 1GB total, 64MB target -> ~16 optimal files -> 16 buckets (power of 2)
    long totalSize = 1024L * 1024L * 1024L; // 1GB
    long targetFileSize = 64L * 1024L * 1024L; // 64MB
    int buckets = UuidBucketUtil.computeBuckets(totalSize, targetFileSize);
    assertThat(buckets).isEqualTo(16);
  }

  @Test
  public void testComputeBucketsSmallTable() {
    // Small table should get MIN_BUCKETS
    long totalSize = 100L * 1024L * 1024L; // 100MB
    long targetFileSize = 64L * 1024L * 1024L; // 64MB
    int buckets = UuidBucketUtil.computeBuckets(totalSize, targetFileSize);
    assertThat(buckets).isEqualTo(UuidBucketUtil.MIN_BUCKETS);
  }

  @Test
  public void testComputeBucketsLargeTable() {
    // Very large table should be capped at MAX_BUCKETS (1024)
    long totalSize = 1024L * 1024L * 1024L * 1024L; // 1TB
    long targetFileSize = 64L * 1024L * 1024L; // 64MB
    int buckets = UuidBucketUtil.computeBuckets(totalSize, targetFileSize);
    assertThat(buckets).isEqualTo(UuidBucketUtil.MAX_BUCKETS);
  }

  @Test
  public void testComputeBucketsInvalidInput() {
    assertThat(UuidBucketUtil.computeBuckets(0, 64L * 1024L * 1024L)).isEqualTo(0);
    assertThat(UuidBucketUtil.computeBuckets(1024L * 1024L * 1024L, 0)).isEqualTo(0);
    assertThat(UuidBucketUtil.computeBuckets(-1, 64L * 1024L * 1024L)).isEqualTo(0);
  }

  @Test
  public void testBucketBoundaries16Buckets() {
    int numBuckets = 16;
    // 4096 / 16 = 256 hex values per bucket

    // Bucket 0: hex 000-0ff (0-255)
    assertThat(UuidBucketUtil.bucketLowerHex(0, numBuckets)).isEqualTo(0);
    assertThat(UuidBucketUtil.bucketUpperHex(0, numBuckets)).isEqualTo(255);

    // Bucket 1: hex 100-1ff (256-511)
    assertThat(UuidBucketUtil.bucketLowerHex(1, numBuckets)).isEqualTo(256);
    assertThat(UuidBucketUtil.bucketUpperHex(1, numBuckets)).isEqualTo(511);

    // Bucket 15: hex f00-fff (3840-4095)
    assertThat(UuidBucketUtil.bucketLowerHex(15, numBuckets)).isEqualTo(3840);
    assertThat(UuidBucketUtil.bucketUpperHex(15, numBuckets)).isEqualTo(4095);
  }

  @Test
  public void testBucketBoundaries1024Buckets() {
    int numBuckets = 1024;
    // 4096 / 1024 = 4 hex values per bucket

    // Bucket 0: hex 000-003 (0-3)
    assertThat(UuidBucketUtil.bucketLowerHex(0, numBuckets)).isEqualTo(0);
    assertThat(UuidBucketUtil.bucketUpperHex(0, numBuckets)).isEqualTo(3);

    // Bucket 512: hex 800-803 (2048-2051)
    assertThat(UuidBucketUtil.bucketLowerHex(512, numBuckets)).isEqualTo(2048);
    assertThat(UuidBucketUtil.bucketUpperHex(512, numBuckets)).isEqualTo(2051);

    // Bucket 1023: hex ffc-fff (4092-4095)
    assertThat(UuidBucketUtil.bucketLowerHex(1023, numBuckets)).isEqualTo(4092);
    assertThat(UuidBucketUtil.bucketUpperHex(1023, numBuckets)).isEqualTo(4095);
  }

  @Test
  public void testHexToBucket() {
    int numBuckets = 16;
    // 4096 / 16 = 256 hex values per bucket

    // hex 000-0ff -> bucket 0
    assertThat(UuidBucketUtil.hexToBucket(0x000, numBuckets)).isEqualTo(0);
    assertThat(UuidBucketUtil.hexToBucket(0x0ff, numBuckets)).isEqualTo(0);

    // hex 100-1ff -> bucket 1
    assertThat(UuidBucketUtil.hexToBucket(0x100, numBuckets)).isEqualTo(1);
    assertThat(UuidBucketUtil.hexToBucket(0x1ff, numBuckets)).isEqualTo(1);

    // hex f00-fff -> bucket 15
    assertThat(UuidBucketUtil.hexToBucket(0xf00, numBuckets)).isEqualTo(15);
    assertThat(UuidBucketUtil.hexToBucket(0xfff, numBuckets)).isEqualTo(15);
  }

  @Test
  public void testExtractHexPrefix() {
    // UUID v4 examples - extracts first 3 hex chars
    assertThat(UuidBucketUtil.extractHexPrefix("00000000-1bdf-47ab-b123-456789abcdef"))
        .isEqualTo(0); // 000
    assertThat(UuidBucketUtil.extractHexPrefix("0f123456-abcd-4def-9012-3456789abcdef"))
        .isEqualTo(0x0f1); // 0f1 = 241
    assertThat(UuidBucketUtil.extractHexPrefix("fff99999-0000-4000-8000-000000000000"))
        .isEqualTo(0xfff); // fff = 4095

    // UUID v7 examples
    assertThat(UuidBucketUtil.extractHexPrefix("01958413-cc3a-70ab-b123-456789abcdef"))
        .isEqualTo(0x019); // 019 = 25
    assertThat(UuidBucketUtil.extractHexPrefix("e061d06e-96b5-45ab-b123-456789abcdef"))
        .isEqualTo(0xe06); // e06 = 3590

    // Edge cases
    assertThat(UuidBucketUtil.extractHexPrefix(null)).isEqualTo(0);
    assertThat(UuidBucketUtil.extractHexPrefix("ab")).isEqualTo(0); // too short
    assertThat(UuidBucketUtil.extractHexPrefix("zzz")).isEqualTo(0); // invalid hex
  }

  @Test
  public void testLooksLikeUuid() {
    // Valid UUIDs
    assertThat(UuidBucketUtil.looksLikeUuid("00000000-1bdf-47ab-b123-456789abcdef")).isTrue();
    assertThat(UuidBucketUtil.looksLikeUuid("01958413-cc3a-70ab-b123-456789abcdef")).isTrue();
    assertThat(UuidBucketUtil.looksLikeUuid("FFFFFFFF-AAAA-BBBB-CCCC-DDDDDDDDDDDD")).isTrue();

    // Truncated but valid (min 9 chars: 8 hex + dash)
    assertThat(UuidBucketUtil.looksLikeUuid("01958413-")).isTrue();
    assertThat(UuidBucketUtil.looksLikeUuid("01958413-c")).isTrue();

    // Invalid
    assertThat(UuidBucketUtil.looksLikeUuid(null)).isFalse();
    assertThat(UuidBucketUtil.looksLikeUuid("")).isFalse();
    assertThat(UuidBucketUtil.looksLikeUuid("12345678")).isFalse(); // no dash
    assertThat(UuidBucketUtil.looksLikeUuid("1234567-8")).isFalse(); // dash in wrong position
    assertThat(UuidBucketUtil.looksLikeUuid("ghijklmn-opqr")).isFalse(); // non-hex chars
    assertThat(UuidBucketUtil.looksLikeUuid("not-a-uuid")).isFalse();
  }

  @Test
  public void testRoundTripBucketBoundaries() {
    // For any bucket, hexToBucket(bucketLowerHex) should return the same bucket
    for (int numBuckets : new int[] {2, 16, 128, 256, 1024}) {
      for (int bucket = 0; bucket < numBuckets; bucket++) {
        int lowerHex = UuidBucketUtil.bucketLowerHex(bucket, numBuckets);
        int upperHex = UuidBucketUtil.bucketUpperHex(bucket, numBuckets);

        assertThat(UuidBucketUtil.hexToBucket(lowerHex, numBuckets))
            .as("bucket %d lower hex %d with %d buckets", bucket, lowerHex, numBuckets)
            .isEqualTo(bucket);
        assertThat(UuidBucketUtil.hexToBucket(upperHex, numBuckets))
            .as("bucket %d upper hex %d with %d buckets", bucket, upperHex, numBuckets)
            .isEqualTo(bucket);
      }
    }
  }
}
