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
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.util.SortOrderUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SparkSortFileRewriteRunner extends SparkShufflingFileRewriteRunner {

  private static final Logger LOG = LoggerFactory.getLogger(SparkSortFileRewriteRunner.class);

  public static final String COLUMNS = "columns";
  public static final String USE_IDENTIFIER_KEYS = "use-identifier-keys";
  public static final String USE_GLOBAL_RANGE_PARTITIONING = "use-global-range-partitioning";
  public static final String DELETE_FILES_ONLY = "delete-files-only";

  private SortOrder sortOrder;
  private boolean sortOrderFromOptions = false;
  private boolean useGlobalRangePartitioning = false;

  SparkSortFileRewriteRunner(SparkSession spark, Table table) {
    super(spark, table);
    // Sort order will be set in init() from options or table default
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
        .add(COLUMNS)
        .add(USE_IDENTIFIER_KEYS)
        .add(USE_GLOBAL_RANGE_PARTITIONING)
        .build();
  }

  @Override
  public void init(Map<String, String> options) {
    super.init(options);

    this.useGlobalRangePartitioning =
        Boolean.parseBoolean(options.getOrDefault(USE_GLOBAL_RANGE_PARTITIONING, "false"));

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
        // delete-files-only=true: eq delete convert mode
        // Sorting only helps if single Long/Integer PK (ROW_GROUP_MERGE_JOIN optimization)
        if (!isSingleLongIdentifierKey(identifierFieldIds)) {
          LOG.info("delete-files-only=true but PK is not single long column (size={}) - sorting won't help eq delete convert, using unsorted order",
              identifierFieldIds.size());
          this.sortOrder = SortOrder.unsorted();
          return;
        }
      }
      // delete-files-only=false: merge small files mode - sort by all identifier keys for good bounds
      // delete-files-only=true + single long PK: sort for eq delete convert optimization
      this.sortOrder = buildSortOrderFromIdentifierKeys(identifierFieldIds);
      ensureSortOrderRegistered(this.sortOrder);
      this.sortOrderFromOptions = true;
    } else if (columnsOption != null) {
      List<String> columns =
          Arrays.stream(columnsOption.split(","))
              .map(String::trim)
              .filter(s -> !s.isEmpty())
              .collect(Collectors.toList());
      Preconditions.checkArgument(
          !columns.isEmpty(), "'%s' option must specify at least one column", COLUMNS);
      this.sortOrder = buildSortOrder(columns);
      this.sortOrderFromOptions = true;
    } else {
      // No options specified - use table's sort order
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

  private SortOrder buildSortOrder(List<String> columns) {
    Schema schema = table().schema();

    // Validate columns exist in schema
    for (String column : columns) {
      Preconditions.checkArgument(
          schema.findField(column) != null, "Column '%s' not found in table schema", column);
    }

    // Build sort order from columns
    SortOrder.Builder builder = SortOrder.builderFor(schema);
    for (String column : columns) {
      builder.asc(column, NullOrder.NULLS_LAST);
    }
    return builder.build();
  }

  /**
   * Checks if identifier key is a single Long/Integer column.
   * Only single long columns benefit from ROW_GROUP_MERGE_JOIN optimization in eq delete convert.
   */
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

  /**
   * Builds a sort order from identifier field IDs, sorted by field ID for deterministic ordering.
   */
  private SortOrder buildSortOrderFromIdentifierKeys(Set<Integer> identifierFieldIds) {
    Schema schema = table().schema();
    List<Integer> sortedFieldIds =
        identifierFieldIds.stream().sorted().collect(Collectors.toList());

    SortOrder.Builder builder = SortOrder.builderFor(schema);
    for (Integer fieldId : sortedFieldIds) {
      String columnName = schema.findColumnName(fieldId);
      builder.asc(columnName, NullOrder.NULLS_LAST);
    }
    return builder.build();
  }

  /**
   * Ensures the sort order is registered in the table so files can reference it by sort_order_id.
   * If no default sort order exists, this becomes the default. Otherwise, it's added without
   * changing the default.
   */
  private void ensureSortOrderRegistered(SortOrder newSortOrder) {
    // Check if matching sort order already exists
    SortOrder existing = SortOrderUtil.maybeFindTableSortOrder(table(), newSortOrder);
    if (existing.isSorted()) {
      LOG.info("Sort order already registered in table with orderId={}", existing.orderId());
      return;
    }

    // Need to add the sort order
    if (table().sortOrder().isUnsorted()) {
      // No default sort order - use replaceSortOrder (becomes default)
      LOG.info("Registering sort order as table default");
      ReplaceSortOrder replace = table().replaceSortOrder();
      for (SortField field : newSortOrder.fields()) {
        String columnName = table().schema().findColumnName(field.sourceId());
        if (field.direction() == SortDirection.ASC) {
          replace.asc(columnName, field.nullOrder());
        } else {
          replace.desc(columnName, field.nullOrder());
        }
      }
      replace.commit();
    } else {
      // Has different default - use addSortOrder (don't change default)
      LOG.info("Adding sort order without changing table default");
      TableOperations ops = ((HasTableOperations) table()).operations();
      TableMetadata current = ops.current();
      TableMetadata updated = TableMetadata.buildFrom(current).addSortOrder(newSortOrder).build();
      ops.commit(current, updated);
    }

    // Refresh to pick up the new sort order
    table().refresh();
    LOG.info(
        "Sort order registered with orderId={}",
        SortOrderUtil.maybeFindTableSortOrder(table(), newSortOrder).orderId());
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

  /** Returns true if global range partitioning is enabled and applicable. */
  boolean useGlobalRangePartitioning() {
    return useGlobalRangePartitioning && sortOrder != null && sortOrder.isSorted();
  }

  /** Returns the field ID of the first sort column, or -1 if not applicable. */
  int firstSortFieldId() {
    if (sortOrder == null || !sortOrder.isSorted()) {
      return -1;
    }
    return sortOrder.fields().get(0).sourceId();
  }
}
