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
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SparkSortFileRewriteRunner extends SparkShufflingFileRewriteRunner {

  private static final Logger LOG = LoggerFactory.getLogger(SparkSortFileRewriteRunner.class);

  public static final String DELETE_FILES_ONLY = "delete-files-only";

  private SortOrder sortOrder;

  SparkSortFileRewriteRunner(SparkSession spark, Table table) {
    super(spark, table);
  }

  SparkSortFileRewriteRunner(SparkSession spark, Table table, SortOrder sortOrder) {
    super(spark, table);
    Preconditions.checkArgument(
        sortOrder != null && sortOrder.isSorted(),
        "Cannot sort data without a valid sort order, the provided sort order is null or empty");
    this.sortOrder = sortOrder;
  }

  @Override
  public Set<String> validOptions() {
    return ImmutableSet.<String>builder()
        .addAll(super.validOptions())
        .add(DELETE_FILES_ONLY)
        .build();
  }

  @Override
  public void init(Map<String, String> options) {
    super.init(options);

    // If sort order was set via constructor, skip options parsing
    if (sortOrder != null) {
      return;
    }

    String columnsOption = options.get(COLUMNS);
    boolean useIdentifierKeys =
        Boolean.parseBoolean(options.getOrDefault(USE_IDENTIFIER_KEYS, "false"));
    boolean deleteFilesOnly =
        Boolean.parseBoolean(options.getOrDefault(DELETE_FILES_ONLY, "false"));

    Preconditions.checkArgument(
        columnsOption == null || !useIdentifierKeys,
        "Cannot specify both '%s' and '%s=true'",
        COLUMNS,
        USE_IDENTIFIER_KEYS);

    if (useIdentifierKeys) {
      Set<Integer> identifierFieldIds = table().schema().identifierFieldIds();
      if (identifierFieldIds.isEmpty()) {
        LOG.info("No identifier keys found, using unsorted order");
        this.sortOrder = SortOrder.unsorted();
        return;
      }
      if (deleteFilesOnly) {
        if (!isSingleLongIdentifierKey(identifierFieldIds)) {
          LOG.info(
              "delete-files-only=true but PK is not single long column (size={}) - "
                  + "sorting won't help eq delete convert, using unsorted order",
              identifierFieldIds.size());
          this.sortOrder = SortOrder.unsorted();
          return;
        }
      }
      List<Integer> sortedFieldIds =
          identifierFieldIds.stream().sorted().collect(Collectors.toList());
      this.sortOrder = buildSortOrderFromFieldIds(sortedFieldIds);
      ensureSortOrderRegistered(this.sortOrder);
    } else if (columnsOption != null) {
      List<String> columns = parseColumnsOption(columnsOption);
      Preconditions.checkArgument(
          !columns.isEmpty(), "'%s' option must specify at least one column", COLUMNS);
      validateColumnsExist(columns);
      this.sortOrder = buildSortOrderFromColumns(columns);
    } else {
      Preconditions.checkArgument(
          table().sortOrder().isSorted(),
          "Cannot sort data without a valid sort order, table '%s' is unsorted. "
              + "Specify '%s' or '%s=true' option, or define a sort order on the table",
          table().name(),
          COLUMNS,
          USE_IDENTIFIER_KEYS);
      this.sortOrder = table().sortOrder();
    }
  }

  private boolean isSingleLongIdentifierKey(Set<Integer> identifierFieldIds) {
    if (identifierFieldIds.size() != 1) {
      return false;
    }
    Integer fieldId = identifierFieldIds.iterator().next();
    Types.NestedField field = table().schema().findField(fieldId);
    if (field == null) {
      return false;
    }
    Type.TypeID typeId = field.type().typeId();
    return typeId == Type.TypeID.LONG || typeId == Type.TypeID.INTEGER;
  }

  @Override
  public String description() {
    return "SORT";
  }

  @Override
  protected SortOrder sortOrder() {
    return sortOrder;
  }

  @Override
  protected Dataset<Row> sortedDF(Dataset<Row> df, Function<Dataset<Row>, Dataset<Row>> sortFunc) {
    return sortFunc.apply(df);
  }

  @Override
  protected boolean useUuidPrefixBucketing() {
    return useUuidPrefixBucketingOption() && sortOrder != null && sortOrder.isSorted();
  }
}
