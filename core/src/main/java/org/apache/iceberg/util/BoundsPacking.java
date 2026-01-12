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

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

/**
 * Sequential packing that preserves order. Unlike {@link BinPacking} which optimizes for bin
 * utilization, this packer groups items sequentially to preserve their original order.
 *
 * <p>This is useful when items are sorted by bounds and must remain consecutive after grouping.
 * Files can be consecutive (non-overlapping) or one can contain another (full containment).
 * Only partial overlaps are not allowed.
 */
public class BoundsPacking {

  /** Maximum gap between consecutive values to still be considered "adjacent" (no hole). */
  private static final double GAP_THRESHOLD = 1000;

  private BoundsPacking() {}

  /**
   * Calculate the distance between two values.
   *
   * <p>For numbers: simple subtraction. For strings: difference in UTF-8 byte representation. For
   * other types: returns MAX_VALUE (always considered a gap).
   *
   * @param lower the lower value
   * @param upper the upper value (should be >= lower)
   * @return the numeric distance between values
   */
  public static double calculateDistance(Object lower, Object upper) {
    if (lower instanceof Number && upper instanceof Number) {
      return ((Number) upper).doubleValue() - ((Number) lower).doubleValue();
    } else if (lower instanceof CharSequence && upper instanceof CharSequence) {
      return calculateStringDistance((CharSequence) lower, (CharSequence) upper);
    } else if (lower instanceof ByteBuffer && upper instanceof ByteBuffer) {
      return calculateBytesDistance((ByteBuffer) lower, (ByteBuffer) upper);
    } else {
      // Unknown type - treat as gap to be safe
      return Double.MAX_VALUE;
    }
  }

  /**
   * Calculate distance for strings using UTF-8 byte representation.
   *
   * <p>UTF-8 preserves lexicographic ordering when compared byte-by-byte, so we convert strings to
   * bytes and compute the numeric difference.
   */
  private static double calculateStringDistance(CharSequence lower, CharSequence upper) {
    byte[] lowerBytes = lower.toString().getBytes(StandardCharsets.UTF_8);
    byte[] upperBytes = upper.toString().getBytes(StandardCharsets.UTF_8);

    // Limit to 16 bytes and pad to same length
    int maxLen = Math.min(Math.max(lowerBytes.length, upperBytes.length), 16);

    byte[] lowerPadded = new byte[maxLen];
    byte[] upperPadded = new byte[maxLen];
    System.arraycopy(lowerBytes, 0, lowerPadded, 0, Math.min(lowerBytes.length, maxLen));
    System.arraycopy(upperBytes, 0, upperPadded, 0, Math.min(upperBytes.length, maxLen));

    BigInteger lowerBig = new BigInteger(1, lowerPadded);
    BigInteger upperBig = new BigInteger(1, upperPadded);

    return upperBig.subtract(lowerBig).doubleValue();
  }

  /** Calculate distance for binary types. */
  private static double calculateBytesDistance(ByteBuffer lower, ByteBuffer upper) {
    int minLen = Math.min(lower.remaining(), upper.remaining());
    minLen = Math.min(minLen, 8); // Limit to 8 bytes for double precision

    long lowerVal = 0;
    long upperVal = 0;

    for (int i = 0; i < minLen; i++) {
      lowerVal = (lowerVal << 8) | (lower.get(lower.position() + i) & 0xFF);
      upperVal = (upperVal << 8) | (upper.get(upper.position() + i) & 0xFF);
    }

    return upperVal - lowerVal;
  }

  /**
   * Checks if two items have partial overlap. Items are assumed to be sorted by lower bound.
   *
   * <p>Returns false (OK) for:
   * <ul>
   *   <li>Consecutive: B.lower >= A.upper</li>
   *   <li>Full containment: B.upper <= A.upper (B is inside A)</li>
   * </ul>
   *
   * <p>Returns true (BAD) for partial overlap: B.lower < A.upper AND B.upper > A.upper
   *
   * @param aUpper upper bound of first item (sorted before second by lower bound)
   * @param bLower lower bound of second item
   * @param bUpper upper bound of second item
   * @return true if partial overlap exists (bad), false if consecutive or containment (ok)
   */
  @SuppressWarnings("unchecked")
  public static boolean hasPartialOverlap(
      Comparable<?> aUpper, Comparable<?> bLower, Comparable<?> bUpper) {
    Comparable<Object> aUpperCmp = (Comparable<Object>) aUpper;
    Comparable<Object> bLowerCmp = (Comparable<Object>) bLower;
    Comparable<Object> bUpperCmp = (Comparable<Object>) bUpper;

    // B contained in A: B.upper <= A.upper → OK
    if (bUpperCmp.compareTo(aUpperCmp) <= 0) {
      return false;
    }
    // Consecutive: B.lower >= A.upper → OK
    if (bLowerCmp.compareTo(aUpperCmp) >= 0) {
      return false;
    }
    // Partial overlap: B.lower < A.upper AND B.upper > A.upper → BAD
    return true;
  }

  /**
   * Packs items sequentially into groups, preserving order.
   *
   * @param <T> the item type
   */
  public static class ListPacker<T> {
    private final long maxGroupSize;
    private final long maxGroupCount;

    public ListPacker(long maxGroupSize, long maxGroupCount) {
      this.maxGroupSize = maxGroupSize;
      this.maxGroupCount = maxGroupCount;
    }

    /**
     * Packs items sequentially into groups.
     *
     * <p>Items are added to the current group until maxGroupSize is reached, then a new group is
     * started. Order is preserved - items appear in groups in the same order as input.
     *
     * @param items items to pack (should be pre-sorted if order matters)
     * @param weightFunc function to get weight of each item
     * @return list of groups, each group is a list of items
     */
    public List<List<T>> pack(Iterable<T> items, Function<T, Long> weightFunc) {
      List<List<T>> groups = new ArrayList<>();
      List<T> currentGroup = new ArrayList<>();
      long currentGroupSize = 0;

      for (T item : items) {
        long itemWeight = weightFunc.apply(item);

        // If adding this item would exceed maxGroupSize and group is not empty, start new group
        if (currentGroupSize + itemWeight > maxGroupSize && !currentGroup.isEmpty()) {
          groups.add(ImmutableList.copyOf(currentGroup));
          currentGroup = new ArrayList<>();
          currentGroupSize = 0;
        }

        // Check max items per group
        if (currentGroup.size() >= maxGroupCount && !currentGroup.isEmpty()) {
          groups.add(ImmutableList.copyOf(currentGroup));
          currentGroup = new ArrayList<>();
          currentGroupSize = 0;
        }

        currentGroup.add(item);
        currentGroupSize += itemWeight;
      }

      // Don't forget the last group
      if (!currentGroup.isEmpty()) {
        groups.add(ImmutableList.copyOf(currentGroup));
      }

      return groups;
    }

    /**
     * Packs items into groups based on overlapping or adjacent bounds.
     *
     * <p>Items must be pre-sorted by lower bound. Items are added to the current group if they:
     *
     * <ul>
     *   <li>Overlap with the group (itemLower <= maxUpperInGroup)
     *   <li>Are adjacent/consecutive (distance between maxUpperInGroup and itemLower <=
     *       GAP_THRESHOLD)
     * </ul>
     *
     * <p>A new group is started only when there is a significant gap (distance > GAP_THRESHOLD)
     * between the current group's upper bound and the next item's lower bound.
     *
     * @param items items to pack (must be sorted by lower bound)
     * @param weightFunc function to get weight of each item
     * @param lowerFunc function to get lower bound of each item
     * @param upperFunc function to get upper bound of each item
     * @return list of groups, each group contains overlapping or adjacent items
     */
    @SuppressWarnings("unchecked")
    public List<List<T>> packWithBounds(
        Iterable<T> items,
        Function<T, Long> weightFunc,
        Function<T, Comparable<?>> lowerFunc,
        Function<T, Comparable<?>> upperFunc) {
      List<List<T>> groups = new ArrayList<>();
      List<T> currentGroup = new ArrayList<>();
      long currentGroupSize = 0;
      Comparable<Object> maxUpperInGroup = null;

      for (T item : items) {
        long itemWeight = weightFunc.apply(item);
        Comparable<Object> itemLower = (Comparable<Object>) lowerFunc.apply(item);
        Comparable<Object> itemUpper = (Comparable<Object>) upperFunc.apply(item);

        // Check if there is a gap between current group and this item
        // Gap: distance between maxUpperInGroup and itemLower > GAP_THRESHOLD
        // No gap (overlap or adjacent): distance <= GAP_THRESHOLD
        boolean hasGap = false;
        if (maxUpperInGroup != null && !currentGroup.isEmpty()) {
          // Only check for gap if itemLower > maxUpperInGroup (no overlap)
          if (itemLower.compareTo(maxUpperInGroup) > 0) {
            double distance = calculateDistance(maxUpperInGroup, itemLower);
            hasGap = distance > GAP_THRESHOLD;
          }
          // If itemLower <= maxUpperInGroup, they overlap - no gap
        }

        // Start new group if: gap exists OR size exceeded OR count exceeded
        boolean sizeExceeded =
            currentGroupSize + itemWeight > maxGroupSize && !currentGroup.isEmpty();
        boolean countExceeded = currentGroup.size() >= maxGroupCount && !currentGroup.isEmpty();

        if (hasGap || sizeExceeded || countExceeded) {
          if (!currentGroup.isEmpty()) {
            groups.add(ImmutableList.copyOf(currentGroup));
          }
          currentGroup = new ArrayList<>();
          currentGroupSize = 0;
          maxUpperInGroup = null;
        }

        currentGroup.add(item);
        currentGroupSize += itemWeight;

        // Update max upper bound in group
        if (maxUpperInGroup == null || itemUpper.compareTo(maxUpperInGroup) > 0) {
          maxUpperInGroup = itemUpper;
        }
      }

      // Don't forget the last group
      if (!currentGroup.isEmpty()) {
        groups.add(ImmutableList.copyOf(currentGroup));
      }

      return groups;
    }
  }
}
