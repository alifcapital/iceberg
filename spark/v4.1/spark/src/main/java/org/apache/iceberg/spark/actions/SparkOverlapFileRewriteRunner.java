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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Runner for the OVERLAP rewrite strategy. This runner reads files, sorts them by the specified
 * columns, and writes them back. The sorting ensures that output files have non-overlapping bounds.
 */
class SparkOverlapFileRewriteRunner extends SparkShufflingFileRewriteRunner {

  public static final String COLUMNS = "columns";
  public static final String USE_IDENTIFIER_KEYS = "use-identifier-keys";

  private SortOrder sortOrder;
  private List<String> columns;

  SparkOverlapFileRewriteRunner(SparkSession spark, Table table) {
    super(spark, table);
  }

  @Override
  public Set<String> validOptions() {
    return ImmutableSet.<String>builder()
        .addAll(super.validOptions())
        .add(COLUMNS)
        .add(USE_IDENTIFIER_KEYS)
        .build();
  }

  @Override
  public void init(Map<String, String> options) {
    super.init(options);

    String columnsOption = options.get(COLUMNS);
    boolean useIdentifierKeys = Boolean.parseBoolean(options.getOrDefault(USE_IDENTIFIER_KEYS, "false"));

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

    if (useIdentifierKeys) {
      Set<String> identifierFieldNames = table().schema().identifierFieldNames();
      // Empty identifier keys handled by planner (returns empty plan)
      this.columns = identifierFieldNames.stream().sorted().collect(Collectors.toList());
    } else {
      this.columns =
          Arrays.stream(columnsOption.split(","))
              .map(String::trim)
              .filter(s -> !s.isEmpty())
              .collect(Collectors.toList());
      Preconditions.checkArgument(
          !columns.isEmpty(), "'%s' option must specify at least one column", COLUMNS);
    }

    // Validate columns exist in schema
    Schema schema = table().schema();
    for (String column : columns) {
      Preconditions.checkArgument(
          schema.findField(column) != null,
          "Column '%s' not found in table schema",
          column);
    }

    // Build sort order from columns
    SortOrder.Builder builder = SortOrder.builderFor(schema);
    for (String column : columns) {
      builder.asc(column, NullOrder.NULLS_LAST);
    }
    this.sortOrder = builder.build();
  }

  @Override
  public String description() {
    return "OVERLAP";
  }

  @Override
  protected SortOrder sortOrder() {
    return sortOrder;
  }

  @Override
  protected Dataset<Row> sortedDF(Dataset<Row> df, Function<Dataset<Row>, Dataset<Row>> sortFunc) {
    return sortFunc.apply(df);
  }

  List<String> columns() {
    return columns;
  }
}
