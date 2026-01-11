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

  private BoundsPacking() {}

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
  }
}
