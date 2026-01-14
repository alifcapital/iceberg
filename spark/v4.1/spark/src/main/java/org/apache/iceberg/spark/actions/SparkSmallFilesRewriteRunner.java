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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runner for the SMALL_FILES rewrite strategy. This runner reads small files from clean zones,
 * sorts them by the specified columns, and writes them back. The sorting ensures that output files
 * have non-overlapping bounds within the clean zone.
 */
class SparkSmallFilesRewriteRunner extends SparkShufflingFileRewriteRunner {

  private static final Logger LOG = LoggerFactory.getLogger(SparkSmallFilesRewriteRunner.class);

  private SortOrder sortOrder;
  private List<String> columns;

  SparkSmallFilesRewriteRunner(SparkSession spark, Table table) {
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
        "SMALL_FILES strategy requires either '%s' option or '%s=true'",
        COLUMNS,
        USE_IDENTIFIER_KEYS);

    Preconditions.checkArgument(
        columnsOption == null || !useIdentifierKeys,
        "Cannot specify both '%s' and '%s=true'",
        COLUMNS,
        USE_IDENTIFIER_KEYS);

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
      this.columns =
          sortedFieldIds.stream()
              .map(table().schema()::findColumnName)
              .collect(Collectors.toList());
      this.sortOrder = buildSortOrderFromFieldIds(sortedFieldIds);
      ensureSortOrderRegistered(this.sortOrder);
    } else {
      this.columns = parseColumnsOption(columnsOption);
      Preconditions.checkArgument(
          !columns.isEmpty(), "'%s' option must specify at least one column", COLUMNS);
      validateColumnsExist(columns);
      this.sortOrder = buildSortOrderFromColumns(columns);
    }
  }

  @Override
  public String description() {
    return "SMALL_FILES";
  }

  @Override
  protected boolean useUuidPrefixBucketing() {
    return useUuidPrefixBucketingOption() && sortOrder != null && sortOrder.isSorted();
  }

  @Override
  protected SortOrder sortOrder() {
    return sortOrder;
  }

  @Override
  protected SortOrder effectiveSortOrder(RewriteFileGroup fileGroup) {
    Integer sortOrderId = fileGroup.sortOrderId();
    if (sortOrderId != null) {
      SortOrder groupSortOrder = table().sortOrders().get(sortOrderId);
      if (groupSortOrder != null) {
        LOG.debug("Using sort order from group sortOrderId={}", sortOrderId);
        return groupSortOrder;
      } else {
        LOG.warn("Sort order with id {} not found in table, using default", sortOrderId);
      }
    }
    return sortOrder();
  }

  @Override
  protected Dataset<Row> sortedDF(Dataset<Row> df, Function<Dataset<Row>, Dataset<Row>> sortFunc) {
    return sortFunc.apply(df);
  }

  List<String> columns() {
    return columns;
  }
}
