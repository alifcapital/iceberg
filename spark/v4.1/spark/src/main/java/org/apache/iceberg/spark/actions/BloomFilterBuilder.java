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

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;

/**
 * Builds bloom filter expressions for equality delete key filtering.
 *
 * <p>Creates IN expressions for each column in the delete schema to filter data files during
 * reading.
 */
class BloomFilterBuilder {

  private BloomFilterBuilder() {}

  /**
   * Build bloom filter expression for multi-column keys.
   *
   * @param keys set of multi-column keys
   * @param deleteSchema schema of the delete columns
   * @param maxKeysPerColumn maximum number of unique values per column to include in filter
   * @return expression to filter data, or null if no filter can be built
   */
  static Expression buildMultiColumnFilter(
      Set<List<Object>> keys, Schema deleteSchema, int maxKeysPerColumn) {
    if (keys.isEmpty()) {
      return null;
    }

    int colCount = deleteSchema.columns().size();
    List<Expression> columnFilters = Lists.newArrayListWithCapacity(colCount);

    for (int colIdx = 0; colIdx < colCount; colIdx++) {
      Types.NestedField col = deleteSchema.columns().get(colIdx);
      org.apache.iceberg.types.Type.TypeID colTypeId = col.type().typeId();
      String colName = col.name();

      // Extract unique values for this column
      Set<Object> uniqueValues = Sets.newHashSet();
      for (List<Object> key : keys) {
        Object val = key.get(colIdx);
        if (val != null) {
          uniqueValues.add(val);
        }
      }

      if (uniqueValues.isEmpty() || uniqueValues.size() > maxKeysPerColumn) {
        continue; // Skip this column - too many values or all nulls
      }

      // Build IN expression based on column type
      Expression colFilter = null;
      switch (colTypeId) {
        case LONG:
        case INTEGER:
          Set<Long> longVals = Sets.newHashSetWithExpectedSize(uniqueValues.size());
          for (Object v : uniqueValues) {
            longVals.add(v instanceof Integer ? ((Integer) v).longValue() : (Long) v);
          }
          colFilter = Expressions.in(colName, longVals);
          break;
        case STRING:
          Set<String> strVals = Sets.newHashSetWithExpectedSize(uniqueValues.size());
          for (Object v : uniqueValues) {
            strVals.add(v.toString());
          }
          colFilter = Expressions.in(colName, strVals);
          break;
        case DECIMAL:
          @SuppressWarnings("unchecked")
          Set<BigDecimal> decVals = (Set<BigDecimal>) (Set<?>) uniqueValues;
          colFilter = Expressions.in(colName, decVals);
          break;
        default:
          // Unsupported type for bloom filter
          break;
      }

      if (colFilter != null) {
        columnFilters.add(colFilter);
      }
    }

    if (columnFilters.isEmpty()) {
      return null;
    } else if (columnFilters.size() == 1) {
      return columnFilters.get(0);
    } else {
      // AND all column filters together
      Expression result = columnFilters.get(0);
      for (int i = 1; i < columnFilters.size(); i++) {
        result = Expressions.and(result, columnFilters.get(i));
      }
      return result;
    }
  }

  /**
   * Build bloom filter expression for single Long column.
   *
   * @param keys set of Long keys (nulls excluded)
   * @param columnName name of the column
   * @param maxKeys maximum number of keys to include in filter
   * @return expression to filter data, or null if no filter can be built
   */
  static Expression buildLongFilter(Set<Long> keys, String columnName, int maxKeys) {
    Set<Long> nonNullKeys = Sets.newHashSetWithExpectedSize(keys.size());
    for (Long key : keys) {
      if (key != null) {
        nonNullKeys.add(key);
      }
    }
    if (nonNullKeys.isEmpty() || nonNullKeys.size() > maxKeys) {
      return null;
    }
    return Expressions.in(columnName, nonNullKeys);
  }

  /**
   * Build bloom filter expression for single String column.
   *
   * @param keys set of String keys (nulls excluded)
   * @param columnName name of the column
   * @param maxKeys maximum number of keys to include in filter
   * @return expression to filter data, or null if no filter can be built
   */
  static Expression buildStringFilter(Set<String> keys, String columnName, int maxKeys) {
    Set<String> nonNullKeys = Sets.newHashSetWithExpectedSize(keys.size());
    for (String key : keys) {
      if (key != null) {
        nonNullKeys.add(key);
      }
    }
    if (nonNullKeys.isEmpty() || nonNullKeys.size() > maxKeys) {
      return null;
    }
    return Expressions.in(columnName, nonNullKeys);
  }

  /**
   * Build bloom filter expression for single BigDecimal column.
   *
   * @param keys set of BigDecimal keys (nulls excluded)
   * @param columnName name of the column
   * @param maxKeys maximum number of keys to include in filter
   * @return expression to filter data, or null if no filter can be built
   */
  static Expression buildDecimalFilter(Set<BigDecimal> keys, String columnName, int maxKeys) {
    Set<BigDecimal> nonNullKeys = Sets.newHashSetWithExpectedSize(keys.size());
    for (BigDecimal key : keys) {
      if (key != null) {
        nonNullKeys.add(key);
      }
    }
    if (nonNullKeys.isEmpty() || nonNullKeys.size() > maxKeys) {
      return null;
    }
    return Expressions.in(columnName, nonNullKeys);
  }
}
