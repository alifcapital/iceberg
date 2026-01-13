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
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.spark.Spark3Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.iceberg.spark.SparkFunctionCatalog;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SortOrderUtil;
import org.apache.iceberg.util.StructLikeMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Cast;
import org.apache.spark.sql.catalyst.expressions.DirectShufflePartitionID;
import org.apache.spark.sql.catalyst.expressions.Divide;
import org.apache.spark.sql.catalyst.expressions.EvalMode;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Floor;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.Multiply;
import org.apache.spark.sql.catalyst.expressions.V2ExpressionUtils;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.OrderAwareCoalesce;
import org.apache.spark.sql.catalyst.plans.logical.OrderAwareCoalescer;
import org.apache.spark.sql.catalyst.plans.logical.RepartitionByExpression;
import org.apache.spark.sql.catalyst.plans.logical.Sort;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.distributions.Distributions;
import org.apache.spark.sql.connector.distributions.OrderedDistribution;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.write.RequiresDistributionAndOrdering;
import org.apache.spark.sql.execution.datasources.v2.DistributionAndOrderingUtils$;
import org.apache.spark.sql.types.DataTypes;
import scala.Option;
import scala.Some;
import scala.collection.JavaConverters;
import scala.collection.immutable.Seq;

abstract class SparkShufflingFileRewriteRunner extends SparkDataFileRewriteRunner {

  private static final Logger LOG = LoggerFactory.getLogger(SparkShufflingFileRewriteRunner.class);

  /**
   * The number of shuffle partitions to use for each output file. By default, this file rewriter
   * assumes each shuffle partition would become a separate output file. Attempting to generate
   * large output files of 512 MB or higher may strain the memory resources of the cluster as such
   * rewrites would require lots of Spark memory. This parameter can be used to further divide up
   * the data which will end up in a single file. For example, if the target file size is 2 GB, but
   * the cluster can only handle shuffles of 512 MB, this parameter could be set to 4. Iceberg will
   * use a custom coalesce operation to stitch these sorted partitions back together into a single
   * sorted file.
   *
   * <p>Note using this parameter requires enabling Iceberg Spark session extensions.
   */
  public static final String SHUFFLE_PARTITIONS_PER_FILE = "shuffle-partitions-per-file";

  public static final int SHUFFLE_PARTITIONS_PER_FILE_DEFAULT = 1;

  public static final String TARGET_ROW_GROUP_SIZE_BYTES = "target-row-group-size-bytes";

  private int numShufflePartitionsPerFile;
  private String rowGroupSizeBytes;
  private StructLikeMap<PartitionBoundaries> globalBoundaries;

  protected SparkShufflingFileRewriteRunner(SparkSession spark, Table table) {
    super(spark, table);
  }

  protected abstract org.apache.iceberg.SortOrder sortOrder();

  /**
   * Retrieves and returns the schema for the rewrite using the current table schema.
   *
   * <p>The schema with all columns required for correctly sorting the table. This may include
   * additional computed columns which are not written to the table but are used for sorting.
   */
  protected Schema sortSchema() {
    return table().schema();
  }

  protected abstract Dataset<Row> sortedDF(
      Dataset<Row> df, Function<Dataset<Row>, Dataset<Row>> sortFunc);

  @Override
  public Set<String> validOptions() {
    return ImmutableSet.<String>builder()
        .addAll(super.validOptions())
        .add(SHUFFLE_PARTITIONS_PER_FILE)
        .add(TARGET_ROW_GROUP_SIZE_BYTES)
        .build();
  }

  @Override
  public void init(Map<String, String> options) {
    super.init(options);
    this.numShufflePartitionsPerFile = numShufflePartitionsPerFile(options);
    this.rowGroupSizeBytes = options.get(TARGET_ROW_GROUP_SIZE_BYTES);
  }

  /** Sets per-partition boundaries for global range partitioning. */
  void setGlobalBoundaries(StructLikeMap<PartitionBoundaries> boundaries) {
    this.globalBoundaries = boundaries;
  }

  @Override
  public void doRewrite(String groupId, RewriteFileGroup fileGroup) {
    Dataset<Row> scanDF =
        spark()
            .read()
            .format("iceberg")
            .option(SparkReadOptions.SCAN_TASK_SET_ID, groupId)
            .load(groupId);

    Dataset<Row> sortedDF =
        sortedDF(
            scanDF,
            sortFunction(
                fileGroup.fileScanTasks(),
                spec(fileGroup.outputSpecId()),
                fileGroup.expectedOutputFiles(),
                fileGroup.info().partition()));

    org.apache.spark.sql.DataFrameWriter<Row> writer =
        sortedDF
            .write()
            .format("iceberg")
            .option(SparkWriteOptions.REWRITTEN_FILE_SCAN_TASK_SET_ID, groupId)
            .option(SparkWriteOptions.TARGET_FILE_SIZE_BYTES, fileGroup.maxOutputFileSize())
            .option(SparkWriteOptions.USE_TABLE_DISTRIBUTION_AND_ORDERING, "false")
            .option(SparkWriteOptions.OUTPUT_SPEC_ID, fileGroup.outputSpecId());

    // Try to find matching sort order in table to set sort_order_id in data files
    org.apache.iceberg.SortOrder matchingTableSortOrder =
        SortOrderUtil.maybeFindTableSortOrder(table(), sortOrder());
    if (matchingTableSortOrder.isSorted()) {
      writer.option(SparkWriteOptions.OUTPUT_SORT_ORDER_ID, matchingTableSortOrder.orderId());
    }

    if (rowGroupSizeBytes != null) {
      writer.option(SparkWriteOptions.TARGET_ROW_GROUP_SIZE_BYTES, rowGroupSizeBytes);
    }

    writer.mode("append").save(groupId);
  }

  private Function<Dataset<Row>, Dataset<Row>> sortFunction(
      List<FileScanTask> group,
      PartitionSpec outputSpec,
      int expectedOutputFiles,
      StructLike partition) {
    SortOrder[] ordering = Spark3Util.toOrdering(outputSortOrder(group, outputSpec));
    int numShufflePartitions = Math.max(1, expectedOutputFiles * numShufflePartitionsPerFile);

    // Check if we have global boundaries for this partition
    PartitionBoundaries bounds =
        globalBoundaries != null ? globalBoundaries.get(partition) : null;

    if (bounds != null && bounds.isValid()) {
      LOG.debug("Using global range partitioning for partition {} with {} buckets",
          partition, bounds.numBuckets());
      return df ->
          transformPlan(
              df, plan -> globalRangeSortPlan(plan, ordering, bounds, numShufflePartitions));
    } else {
      if (globalBoundaries != null) {
        LOG.debug("Falling back to standard sort for partition {}: bounds not valid", partition);
      }
      return df -> transformPlan(df, plan -> sortPlan(plan, ordering, numShufflePartitions));
    }
  }

  private LogicalPlan sortPlan(LogicalPlan plan, SortOrder[] ordering, int numShufflePartitions) {
    // If ordering is empty (unsorted), skip shuffle/sort to avoid Spark 4.x error:
    // "The number of partitions can't be specified with unspecified distribution"
    if (ordering.length == 0) {
      return plan;
    }

    SparkFunctionCatalog catalog = SparkFunctionCatalog.get();
    OrderedWrite write = new OrderedWrite(ordering, numShufflePartitions);
    LogicalPlan sortPlan =
        DistributionAndOrderingUtils$.MODULE$.prepareQuery(write, plan, Option.apply(catalog));

    if (numShufflePartitionsPerFile == 1) {
      return sortPlan;
    } else {
      OrderAwareCoalescer coalescer = new OrderAwareCoalescer(numShufflePartitionsPerFile);
      int numOutputPartitions = numShufflePartitions / numShufflePartitionsPerFile;
      return new OrderAwareCoalesce(numOutputPartitions, coalescer, sortPlan);
    }
  }

  /**
   * Creates a logical plan for global range partitioning using pre-computed boundaries.
   *
   * <p>Uses DirectShufflePartitionID to partition data by computed bucket IDs based on global
   * min/max bounds, avoiding Spark's sampling-based range partitioner.
   *
   * <p>For UUID columns (auto-detected), uses hex prefix bucketing instead of range partitioning.
   */
  private LogicalPlan globalRangeSortPlan(
      LogicalPlan plan, SortOrder[] ordering, PartitionBoundaries bounds, int numShufflePartitions) {
    // If ordering is empty (unsorted), skip shuffle/sort
    if (ordering.length == 0) {
      return plan;
    }

    // Check that sort order has fields
    if (sortOrder() == null || sortOrder().fields().isEmpty()) {
      LOG.warn("Cannot use global range partitioning: sort order has no fields");
      return sortPlan(plan, ordering, numShufflePartitions);
    }

    // Get the first sort column from the plan's output
    String firstSortColumnName = table().schema().findColumnName(sortOrder().fields().get(0).sourceId());
    Expression firstSortCol = findColumnExpression(plan, firstSortColumnName);

    if (firstSortCol == null) {
      LOG.warn("Cannot use global range partitioning: column '{}' not found in plan output",
          firstSortColumnName);
      return sortPlan(plan, ordering, numShufflePartitions);
    }

    // V1 only supports UUID - non-UUID falls back to standard sort
    if (!bounds.isUuidType()) {
      LOG.debug("Non-UUID type detected, falling back to standard sort");
      return sortPlan(plan, ordering, numShufflePartitions);
    }

    // Build bucket expression for UUID hex prefix bucketing
    Expression bucketExpr = buildUuidBucketExpression(firstSortCol, bounds);
    LOG.debug("Using UUID prefix bucketing with {} buckets", bounds.numBuckets());

    // Wrap in DirectShufflePartitionID for direct partition assignment
    Expression partitionExpr = new DirectShufflePartitionID(bucketExpr);

    // Create RepartitionByExpression with the partition expression
    Seq<Expression> partitionExprs =
        JavaConverters.asScalaIteratorConverter(List.of(partitionExpr).iterator())
            .asScala()
            .toSeq();

    LogicalPlan repartitionPlan =
        new RepartitionByExpression(
            partitionExprs, plan, new Some<>(bounds.numBuckets()), Option.empty());

    // Build sort expressions for within-partition sorting
    SparkFunctionCatalog catalog = SparkFunctionCatalog.get();
    Seq<org.apache.spark.sql.catalyst.expressions.SortOrder> sortExprs =
        V2ExpressionUtils.toCatalystOrdering(ordering, repartitionPlan, Option.apply(catalog));

    // Add Sort for within-partition ordering (global=false)
    LogicalPlan sortPlan = new Sort(sortExprs, false, repartitionPlan, Option.empty());

    if (numShufflePartitionsPerFile == 1) {
      return sortPlan;
    } else {
      OrderAwareCoalescer coalescer = new OrderAwareCoalescer(numShufflePartitionsPerFile);
      int numOutputPartitions = numShufflePartitions / numShufflePartitionsPerFile;
      return new OrderAwareCoalesce(numOutputPartitions, coalescer, sortPlan);
    }
  }

  /** Finds the column expression by name in the plan's output. */
  private Expression findColumnExpression(LogicalPlan plan, String columnName) {
    scala.collection.Iterator<org.apache.spark.sql.catalyst.expressions.Attribute> iter =
        plan.output().iterator();
    while (iter.hasNext()) {
      org.apache.spark.sql.catalyst.expressions.Attribute attr = iter.next();
      if (attr.name().equalsIgnoreCase(columnName)) {
        return attr;
      }
    }
    return null;
  }

  /**
   * Builds a Catalyst expression for UUID bucket ID computation.
   *
   * <p>Uses hex prefix bucketing: floor(conv(substring(uuid, 1, 2), 16, 10) * numBuckets / 256)
   *
   * <p>This extracts first 2 hex characters (00-ff = 0-255), then maps to bucket range. For
   * numBuckets=16, this groups by first hex char: 00-0f→0, 10-1f→1, ..., f0-ff→15.
   */
  private Expression buildUuidBucketExpression(Expression col, PartitionBoundaries bounds) {
    int numBuckets = bounds.numBuckets();

    // substring(col, 1, 2) - get first 2 characters (1-based indexing in SQL)
    Expression prefix = new org.apache.spark.sql.catalyst.expressions.Substring(
        col,
        Literal.create(1, DataTypes.IntegerType),
        Literal.create(2, DataTypes.IntegerType));

    // conv(prefix, 16, 10) - convert hex to decimal (00-ff → 0-255)
    Expression hexToDecimal = new org.apache.spark.sql.catalyst.expressions.Conv(
        prefix,
        Literal.create(16, DataTypes.IntegerType),
        Literal.create(10, DataTypes.IntegerType));

    // Cast to long for arithmetic
    Expression asLong = new Cast(hexToDecimal, DataTypes.LongType, Option.empty(), EvalMode.LEGACY());

    // bucket = floor(hexValue * numBuckets / 256)
    // For numBuckets=16: 00-0f→0, 10-1f→1, ..., f0-ff→15
    // For numBuckets=32: 00-07→0, 08-0f→1, ..., f8-ff→31
    Expression scaled = new Multiply(asLong, Literal.create((long) numBuckets, DataTypes.LongType));
    Expression divided = new Divide(scaled, Literal.create(256L, DataTypes.LongType));
    Expression bucket = new Floor(divided);

    // Cast to int for partition ID
    return new Cast(bucket, DataTypes.IntegerType, Option.empty(), EvalMode.LEGACY());
  }

  private Dataset<Row> transformPlan(Dataset<Row> df, Function<LogicalPlan, LogicalPlan> func) {
    Preconditions.checkArgument(
        spark() instanceof org.apache.spark.sql.classic.SparkSession,
        "Expected instance of org.apache.spark.sql.classic.SparkSession, but got: %s",
        spark().getClass().getName());

    Preconditions.checkArgument(
        df instanceof org.apache.spark.sql.classic.Dataset,
        "df is supposed to be org.apache.spark.sql.classic.Dataset, but got: %s",
        df.getClass().getName());

    return new org.apache.spark.sql.classic.Dataset<>(
        ((org.apache.spark.sql.classic.SparkSession) spark()),
        func.apply(((org.apache.spark.sql.classic.Dataset<Row>) df).logicalPlan()),
        df.encoder());
  }

  private org.apache.iceberg.SortOrder outputSortOrder(
      List<FileScanTask> group, PartitionSpec outputSpec) {
    boolean requiresRepartitioning = !group.get(0).spec().equals(outputSpec);
    if (requiresRepartitioning) {
      // build in the requirement for partition sorting into our sort order
      // as the original spec for this group does not match the output spec
      return SortOrderUtil.buildSortOrder(sortSchema(), outputSpec, sortOrder());
    } else {
      return sortOrder();
    }
  }

  private int numShufflePartitionsPerFile(Map<String, String> options) {
    int value =
        PropertyUtil.propertyAsInt(
            options, SHUFFLE_PARTITIONS_PER_FILE, SHUFFLE_PARTITIONS_PER_FILE_DEFAULT);
    Preconditions.checkArgument(
        value > 0, "'%s' is set to %s but must be > 0", SHUFFLE_PARTITIONS_PER_FILE, value);
    Preconditions.checkArgument(
        value == 1 || Spark3Util.extensionsEnabled(spark()),
        "Using '%s' requires enabling Iceberg Spark session extensions",
        SHUFFLE_PARTITIONS_PER_FILE);
    return value;
  }

  private static class OrderedWrite implements RequiresDistributionAndOrdering {
    private final OrderedDistribution distribution;
    private final SortOrder[] ordering;
    private final int numShufflePartitions;

    OrderedWrite(SortOrder[] ordering, int numShufflePartitions) {
      this.distribution = Distributions.ordered(ordering);
      this.ordering = ordering;
      this.numShufflePartitions = numShufflePartitions;
    }

    @Override
    public Distribution requiredDistribution() {
      return distribution;
    }

    @Override
    public boolean distributionStrictlyRequired() {
      return true;
    }

    @Override
    public int requiredNumPartitions() {
      return numShufflePartitions;
    }

    @Override
    public SortOrder[] requiredOrdering() {
      return ordering;
    }
  }
}
