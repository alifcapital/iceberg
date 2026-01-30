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

import static org.apache.spark.sql.functions.array;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ZOrderByteUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runner for the OVERLAP rewrite strategy. This runner reads overlapping files, sorts them, and
 * writes them back.
 *
 * <p>For single-column keys: uses ORDER BY and sets SORT_ORDER_ID in file metadata.
 *
 * <p>For multi-column keys: uses ZORDER (Z-order curve) for better multi-dimensional clustering,
 * improving data skipping for queries on any combination of columns. ZORDER files do not have
 * SORT_ORDER_ID set because Z-order is not representable as a linear sort order.
 */
class SparkOverlapFileRewriteRunner extends SparkShufflingFileRewriteRunner {

  private static final Logger LOG = LoggerFactory.getLogger(SparkOverlapFileRewriteRunner.class);
  private static final String Z_COLUMN = "ICEBERG_ZORDER_VALUE";
  private static final Schema Z_SCHEMA =
      new Schema(Types.NestedField.required(0, Z_COLUMN, Types.BinaryType.get()));
  private static final SortOrder Z_SORT_ORDER =
      SortOrder.builderFor(Z_SCHEMA)
          .sortBy(Z_COLUMN, SortDirection.ASC, NullOrder.NULLS_LAST)
          .build();

  private SortOrder sortOrder;
  private List<String> columns;
  private boolean useZOrder;

  SparkOverlapFileRewriteRunner(SparkSession spark, Table table) {
    super(spark, table);
  }

  @Override
  public void init(Map<String, String> options) {
    super.init(options);

    String columnsOption = options.get(COLUMNS);
    boolean useIdentifierKeys =
        Boolean.parseBoolean(options.getOrDefault(USE_IDENTIFIER_KEYS, "false"));

    Preconditions.checkArgument(
        columnsOption != null || useIdentifierKeys,
        "OVERLAP strategy requires either '%s' option or '%s=true'",
        COLUMNS,
        USE_IDENTIFIER_KEYS);

    Preconditions.checkArgument(
        columnsOption == null || !useIdentifierKeys,
        "Cannot specify both '%s' and '%s=true'",
        COLUMNS,
        USE_IDENTIFIER_KEYS);

    List<String> rawColumns;
    if (useIdentifierKeys) {
      Set<Integer> identifierFieldIds = table().schema().identifierFieldIds();
      if (identifierFieldIds.isEmpty()) {
        LOG.info("No identifier keys found, using unsorted fallback");
        this.columns = ImmutableList.of();
        this.sortOrder = SortOrder.unsorted();
        return;
      }
      List<Integer> sortedFieldIds =
          identifierFieldIds.stream().sorted().collect(Collectors.toList());
      rawColumns =
          sortedFieldIds.stream()
              .map(table().schema()::findColumnName)
              .collect(Collectors.toList());
    } else {
      rawColumns = parseColumnsOption(columnsOption);
      Preconditions.checkArgument(
          !rawColumns.isEmpty(), "'%s' option must specify at least one column", COLUMNS);
      validateColumnsExist(rawColumns);
    }

    // Filter out identity partition columns (constant within partition, useless for sorting)
    this.columns = filterIdentityPartitionColumns(rawColumns);

    if (columns.isEmpty()) {
      LOG.info(
          "All columns are identity partition columns, using unsorted fallback. "
              + "Original columns: {}",
          rawColumns);
      this.sortOrder = SortOrder.unsorted();
      return;
    }

    // Build sort order from filtered columns
    List<Integer> filteredFieldIds =
        columns.stream()
            .map(col -> table().schema().findField(col).fieldId())
            .collect(Collectors.toList());
    this.sortOrder = buildSortOrderFromFieldIds(filteredFieldIds);

    this.useZOrder = columns.size() > 1;
    if (!columns.isEmpty()) {
      if (useZOrder) {
        LOG.info(
            "OVERLAP [{}]: using ZORDER for {} columns: {}",
            table().name(),
            columns.size(),
            columns);
      } else {
        LOG.info("OVERLAP [{}]: using ORDER BY for column: {}", table().name(), columns.get(0));
      }
    }
  }

  @Override
  public String description() {
    return "OVERLAP";
  }

  @Override
  protected SortOrder sortOrder() {
    return useZOrder ? Z_SORT_ORDER : sortOrder;
  }

  @Override
  protected Schema sortSchema() {
    if (useZOrder) {
      return new Schema(
          new ImmutableList.Builder<Types.NestedField>()
              .addAll(table().schema().columns())
              .addAll(Z_SCHEMA.columns())
              .build());
    }
    return table().schema();
  }

  @Override
  protected Dataset<Row> sortedDF(
      Dataset<Row> df, Function<Dataset<Row>, Dataset<Row>> sortFunc, SortOrder groupSortOrder) {
    // For single-column or empty: just apply sortFunc (ORDER BY or no-op)
    if (!useZOrder || columns.isEmpty()) {
      return sortFunc.apply(df);
    }

    // For multi-column: use ZORDER
    // Create new UDF for each call to avoid state accumulation
    SparkZOrderUDF zOrderUDF =
        new SparkZOrderUDF(
            columns.size(), ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE, Integer.MAX_VALUE);

    // Convert each column to Z-order bytes and interleave them
    Column[] zOrderCols =
        columns.stream()
            .map(
                col -> {
                  org.apache.spark.sql.types.DataType type = df.schema().apply(col).dataType();
                  return zOrderUDF.sortedLexicographically(df.col(col), type);
                })
            .toArray(Column[]::new);

    Column zValue = zOrderUDF.interleaveBytes(array(zOrderCols));

    // Add Z-value column, sort by it, then drop it
    Dataset<Row> zValueDF = df.withColumn(Z_COLUMN, zValue);
    Dataset<Row> sortedDF = sortFunc.apply(zValueDF);
    return sortedDF.drop(Z_COLUMN);
  }

  List<String> columns() {
    return columns;
  }

  @Override
  protected boolean useUuidPrefixBucketing() {
    return useUuidPrefixBucketingOption() && sortOrder != null && sortOrder.isSorted();
  }
}
