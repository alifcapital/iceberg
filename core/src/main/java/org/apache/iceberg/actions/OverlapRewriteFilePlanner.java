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
package org.apache.iceberg.actions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.actions.RewriteDataFiles.FileGroupInfo;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.StructLikeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A file rewrite planner that groups files based on overlapping bounds to minimize data skipping
 * inefficiency.
 *
 * <p>This planner calculates an "overlap cost" for pairs of files. The cost represents the extra
 * records that must be read when querying due to overlapping bounds. Files are grouped to maximize
 * improvement (cost reduction) after merge+sort+split.
 *
 * <p>Cost formula: cost(A,B) = A.records + B.records - overlap × (card_A + card_B)
 *
 * <p>where: - overlap = max(0, min(A.upper, B.upper) - max(A.lower, B.lower)) - cardinality =
 * records / (upper - lower)
 *
 * <p>For multi-column: total_cost = Σ cost_by_column
 */
public class OverlapRewriteFilePlanner
    extends SizeBasedFileRewritePlanner<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> {

  private static final Logger LOG = LoggerFactory.getLogger(OverlapRewriteFilePlanner.class);

  public static final String COLUMNS = "columns";
  public static final String USE_IDENTIFIER_KEYS = "use-identifier-keys";

  private final Expression filter;
  private final Long snapshotId;
  private final boolean caseSensitive;

  private List<Integer> columnFieldIds;
  private List<Type> columnTypes;
  private long maxGroupSize;
  private long maxGroupInputFiles;
  private boolean skipRewrite;

  public OverlapRewriteFilePlanner(Table table) {
    this(table, Expressions.alwaysTrue());
  }

  public OverlapRewriteFilePlanner(Table table, Expression filter) {
    this(
        table,
        filter,
        table.currentSnapshot() != null ? table.currentSnapshot().snapshotId() : null,
        false);
  }

  public OverlapRewriteFilePlanner(
      Table table, Expression filter, Long snapshotId, boolean caseSensitive) {
    super(table);
    this.filter = filter;
    this.snapshotId = snapshotId;
    this.caseSensitive = caseSensitive;
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

    List<String> columns;
    if (useIdentifierKeys) {
      Set<String> identifierFieldNames = table().schema().identifierFieldNames();
      if (identifierFieldNames.isEmpty()) {
        LOG.info("OVERLAP: table has no identifier keys, skipping");
        this.skipRewrite = true;
        this.columnFieldIds = ImmutableList.of();
        this.columnTypes = ImmutableList.of();
        return;
      }
      columns = identifierFieldNames.stream().sorted().collect(Collectors.toList());
    } else {
      columns =
          Arrays.stream(columnsOption.split(","))
              .map(String::trim)
              .filter(s -> !s.isEmpty())
              .collect(Collectors.toList());
      Preconditions.checkArgument(
          !columns.isEmpty(), "'%s' option must specify at least one column", COLUMNS);
    }

    // Validate and resolve columns to field IDs
    this.columnFieldIds = new ArrayList<>();
    this.columnTypes = new ArrayList<>();

    for (String column : columns) {
      Types.NestedField field = table().schema().findField(column);
      Preconditions.checkArgument(field != null, "Column '%s' not found in table schema", column);
      columnFieldIds.add(field.fieldId());
      columnTypes.add(field.type());
      LOG.info("OVERLAP init: column='{}' fieldId={} type={}", column, field.fieldId(), field.type());
    }

    // Parse group limits
    this.maxGroupSize =
        PropertyUtil.propertyAsLong(
            options, MAX_FILE_GROUP_SIZE_BYTES, MAX_FILE_GROUP_SIZE_BYTES_DEFAULT);
    this.maxGroupInputFiles =
        PropertyUtil.propertyAsLong(
            options, MAX_FILE_GROUP_INPUT_FILES, MAX_FILE_GROUP_INPUT_FILES_DEFAULT);

  }

  @Override
  protected long defaultTargetFileSize() {
    return PropertyUtil.propertyAsLong(
        table().properties(),
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
  }

  @Override
  protected Iterable<FileScanTask> filterFiles(Iterable<FileScanTask> tasks) {
    // Filter out files that don't have bounds for all specified columns
    return () ->
        Lists.newArrayList(tasks).stream()
            .filter(this::hasAllBounds)
            .collect(Collectors.toList())
            .iterator();
  }

  @Override
  protected Iterable<List<FileScanTask>> filterFileGroups(List<List<FileScanTask>> groups) {
    // Not used - we do custom grouping in plan()
    return groups;
  }

  @Override
  public FileRewritePlan<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> plan() {
    if (skipRewrite) {
      return new FileRewritePlan<>(CloseableIterable.of(ImmutableList.of()), 0, ImmutableMap.of());
    }

    StructLikeMap<List<FileScanTask>> filesByPartition = scanFiles();

    List<RewriteFileGroup> selectedGroups = new ArrayList<>();
    RewriteExecutionContext ctx = new RewriteExecutionContext();

    for (Map.Entry<StructLike, List<FileScanTask>> entry : filesByPartition.entrySet()) {
      StructLike partition = entry.getKey();
      List<FileScanTask> partitionFiles = entry.getValue();

      // Filter files that have bounds for all columns
      List<FileScanTask> validFiles =
          partitionFiles.stream().filter(this::hasAllBounds).collect(Collectors.toList());

      LOG.info("OVERLAP: partition={} totalFiles={}", partition, partitionFiles.size());

      if (validFiles.size() < 2) {
        continue; // Need at least 2 files to have overlap
      }

      // Find the best group to rewrite based on overlap improvement
      List<FileScanTask> bestGroup = findBestGroup(validFiles);

      if (bestGroup != null && bestGroup.size() >= 2) {
        long inputSize = inputSize(bestGroup);
        RewriteFileGroup group =
            newRewriteGroup(
                ctx,
                partition,
                bestGroup,
                inputSplitSize(inputSize),
                expectedOutputFiles(inputSize));
        selectedGroups.add(group);
        // Only return ONE group - external iteration handles multiple passes
        break;
      }
    }

    int totalGroupCount = selectedGroups.size();
    Map<StructLike, Integer> groupsInPartition =
        selectedGroups.stream()
            .collect(
                Collectors.groupingBy(
                    g -> g.info().partition(), Collectors.summingInt(g -> 1)));

    return new FileRewritePlan<>(
        CloseableIterable.of(selectedGroups), totalGroupCount, groupsInPartition);
  }

  private List<FileScanTask> findBestGroup(List<FileScanTask> files) {
    if (files.size() < 2) {
      return null;
    }

    // Calculate initial total cost
    double initialTotalCost = calculateTotalCost(files);
    LOG.info("OVERLAP: {} files, totalOverlapCost={}", files.size(), (long) initialTotalCost);

    if (initialTotalCost == 0) {
      LOG.info("OVERLAP: no overlapping files, already optimized");
      return null; // Already optimal
    }

    // Find the best pair to start with (highest improvement after merge+sort+split)
    double bestImprovement = 0;
    List<FileScanTask> bestPair = null;
    int pairsChecked = 0;
    int pairsWithOverlap = 0;

    for (int i = 0; i < files.size(); i++) {
      for (int j = i + 1; j < files.size(); j++) {
        FileScanTask a = files.get(i);
        FileScanTask b = files.get(j);

        // Check if pair fits within group size limit
        long pairSize = a.length() + b.length();
        if (pairSize > maxGroupSize) {
          continue; // Pair exceeds max group size
        }

        pairsChecked++;
        double pairCost = calculatePairCost(a, b);

        LOG.debug("OVERLAP: pair {}: pairCost={} pairSize={}", pairsChecked, pairCost, pairSize);

        if (pairCost <= 0) {
          continue; // No overlap between this pair
        }

        pairsWithOverlap++;

        // After merge+sort+split, cost between these two becomes 0
        double improvement = calculateMergeImprovement(files, Arrays.asList(a, b));

        LOG.debug("OVERLAP: overlapping pair {}: improvement={}", pairsWithOverlap, improvement);

        if (improvement > bestImprovement) {
          bestImprovement = improvement;
          bestPair = Arrays.asList(a, b);
        }
      }
    }

    LOG.info(
        "OVERLAP: checked {} pairs, {} overlapping, bestImprovement={}",
        pairsChecked,
        pairsWithOverlap,
        (long) bestImprovement);

    if (bestPair == null || bestImprovement <= 0) {
      LOG.info("OVERLAP: no improvement possible");
      return null;
    }

    LOG.info(
        "OVERLAP: best pair {} + {} records, improvement={}",
        bestPair.get(0).file().recordCount(),
        bestPair.get(1).file().recordCount(),
        (long) bestImprovement);

    // Try to expand the group by adding more files
    List<FileScanTask> group = new ArrayList<>(bestPair);
    List<FileScanTask> remaining =
        files.stream().filter(f -> !group.contains(f)).collect(Collectors.toList());

    // Keep adding files while it improves the cost and within limits
    boolean improved = true;
    while (improved && !remaining.isEmpty()) {
      // Check if we've reached the file count limit
      if (group.size() >= maxGroupInputFiles) {
        break;
      }

      long currentGroupSize = group.stream().mapToLong(FileScanTask::length).sum();

      improved = false;
      FileScanTask bestAddition = null;
      double bestAdditionImprovement = 0;

      for (FileScanTask candidate : remaining) {
        // Check if adding this file would exceed size limit
        if (currentGroupSize + candidate.length() > maxGroupSize) {
          continue; // Would exceed max group size
        }

        List<FileScanTask> testGroup = new ArrayList<>(group);
        testGroup.add(candidate);

        double newImprovement = calculateMergeImprovement(files, testGroup);
        double additionalImprovement = newImprovement - bestImprovement;

        if (additionalImprovement > 0 && newImprovement > bestAdditionImprovement) {
          bestAdditionImprovement = newImprovement;
          bestAddition = candidate;
        }
      }

      if (bestAddition != null) {
        group.add(bestAddition);
        remaining.remove(bestAddition);
        bestImprovement = bestAdditionImprovement;
        improved = true;
        LOG.info(
            "OVERLAP: added file, groupSize={} totalBytes={} improvement={}",
            group.size(),
            group.stream().mapToLong(FileScanTask::length).sum(),
            (long) bestImprovement);
      }
    }

    return group;
  }

  private double calculateTotalCost(List<FileScanTask> files) {
    double totalCost = 0;
    for (int i = 0; i < files.size(); i++) {
      for (int j = i + 1; j < files.size(); j++) {
        totalCost += calculatePairCost(files.get(i), files.get(j));
      }
    }
    return totalCost;
  }

  private double calculateMergeImprovement(List<FileScanTask> allFiles, List<FileScanTask> group) {
    List<FileScanTask> remaining =
        allFiles.stream().filter(f -> !group.contains(f)).collect(Collectors.toList());

    // Calculate cost BEFORE merge: internal (within group) + external (group vs remaining)
    double costBefore = 0;

    // Internal costs within group
    for (int i = 0; i < group.size(); i++) {
      for (int j = i + 1; j < group.size(); j++) {
        costBefore += calculatePairCost(group.get(i), group.get(j));
      }
    }

    // External costs between group and remaining
    for (FileScanTask groupFile : group) {
      for (FileScanTask remainingFile : remaining) {
        costBefore += calculatePairCost(groupFile, remainingFile);
      }
    }

    // Simulate merge+sort+split
    SimulationResult simulation = simulateMergeSplit(group);

    // Calculate cost AFTER merge
    double costAfter = 0;

    // Internal cost between simulated files (they may overlap on secondary columns)
    for (int i = 0; i < simulation.files.size(); i++) {
      for (int j = i + 1; j < simulation.files.size(); j++) {
        costAfter +=
            calculateCostBetweenSimulated(
                simulation.files.get(i), simulation.files.get(j), simulation.context);
      }
    }

    // External cost: simulated files vs remaining
    for (SimulatedFile simFile : simulation.files) {
      for (FileScanTask remainingFile : remaining) {
        costAfter += calculateCostWithSimulated(simFile, remainingFile, simulation.context);
      }
    }

    return costBefore - costAfter;
  }

  /** Context for simulation containing union bounds of the group. */
  private static class SimulationContext {
    final Object[] minBounds;
    final Object[] maxBounds;
    final double[] ranges;

    SimulationContext(int numColumns) {
      this.minBounds = new Object[numColumns];
      this.maxBounds = new Object[numColumns];
      this.ranges = new double[numColumns];
    }
  }

  /** Represents a hypothetical file after merge+sort+split with normalized positions (0..1). */
  private static class SimulatedFile {
    final double[] lowerPos; // normalized position 0..1
    final double[] upperPos; // normalized position 0..1
    final long records;

    SimulatedFile(double[] lowerPos, double[] upperPos, long records) {
      this.lowerPos = lowerPos;
      this.upperPos = upperPos;
      this.records = records;
    }
  }

  /** Result of simulation containing context and files. */
  private static class SimulationResult {
    final SimulationContext context;
    final List<SimulatedFile> files;

    SimulationResult(SimulationContext context, List<SimulatedFile> files) {
      this.context = context;
      this.files = files;
    }
  }

  /** Simulates merge+sort+split and returns list of hypothetical output files. */
  @SuppressWarnings("unchecked")
  private SimulationResult simulateMergeSplit(List<FileScanTask> group) {
    SimulationContext ctx = new SimulationContext(columnFieldIds.size());

    long totalRecords = 0;
    long totalBytes = 0;

    // Find union bounds for each column
    for (FileScanTask task : group) {
      totalRecords += task.file().recordCount();
      totalBytes += task.length();

      for (int i = 0; i < columnFieldIds.size(); i++) {
        int fieldId = columnFieldIds.get(i);
        Type type = columnTypes.get(i);

        ByteBuffer lowerBuf = task.file().lowerBounds().get(fieldId);
        ByteBuffer upperBuf = task.file().upperBounds().get(fieldId);

        if (lowerBuf != null && upperBuf != null) {
          Comparable<Object> lower =
              (Comparable<Object>) Conversions.fromByteBuffer(type, lowerBuf);
          Comparable<Object> upper =
              (Comparable<Object>) Conversions.fromByteBuffer(type, upperBuf);

          if (ctx.minBounds[i] == null || lower.compareTo(ctx.minBounds[i]) < 0) {
            ctx.minBounds[i] = lower;
          }
          if (ctx.maxBounds[i] == null || upper.compareTo(ctx.maxBounds[i]) > 0) {
            ctx.maxBounds[i] = upper;
          }
        }
      }
    }

    // Calculate ranges
    for (int i = 0; i < columnFieldIds.size(); i++) {
      if (ctx.minBounds[i] != null && ctx.maxBounds[i] != null) {
        ctx.ranges[i] = calculateRange(ctx.minBounds[i], ctx.maxBounds[i], columnTypes.get(i));
      }
    }

    // Calculate number of output files using the same logic as actual rewrite
    int numOutputFiles = expectedOutputFiles(totalBytes);

    // Create simulated files with non-overlapping normalized positions
    List<SimulatedFile> result = new ArrayList<>();
    long recordsPerFile = totalRecords / numOutputFiles;

    for (int f = 0; f < numOutputFiles; f++) {
      double[] fileLowerPos = new double[columnFieldIds.size()];
      double[] fileUpperPos = new double[columnFieldIds.size()];

      // First column: split evenly (non-overlapping)
      fileLowerPos[0] = (double) f / numOutputFiles;
      fileUpperPos[0] = (double) (f + 1) / numOutputFiles;

      // Other columns: full range (conservative estimate)
      for (int i = 1; i < columnFieldIds.size(); i++) {
        fileLowerPos[i] = 0.0;
        fileUpperPos[i] = 1.0;
      }

      long fileRecords =
          (f == numOutputFiles - 1)
              ? totalRecords - recordsPerFile * f // Last file gets remainder
              : recordsPerFile;

      result.add(new SimulatedFile(fileLowerPos, fileUpperPos, fileRecords));
    }

    return new SimulationResult(ctx, result);
  }

  /** Calculate overlap cost between a simulated file and a real file. */
  @SuppressWarnings("unchecked")
  private double calculateCostWithSimulated(
      SimulatedFile simFile, FileScanTask realFile, SimulationContext ctx) {
    double totalCost = 0;

    for (int i = 0; i < columnFieldIds.size(); i++) {
      int fieldId = columnFieldIds.get(i);
      Type type = columnTypes.get(i);

      if (ctx.minBounds[i] == null || ctx.maxBounds[i] == null || ctx.ranges[i] <= 0) {
        continue;
      }

      ByteBuffer realLowerBuf = realFile.file().lowerBounds().get(fieldId);
      ByteBuffer realUpperBuf = realFile.file().upperBounds().get(fieldId);

      if (realLowerBuf == null || realUpperBuf == null) {
        continue;
      }

      Comparable<Object> realLower =
          (Comparable<Object>) Conversions.fromByteBuffer(type, realLowerBuf);
      Comparable<Object> realUpper =
          (Comparable<Object>) Conversions.fromByteBuffer(type, realUpperBuf);

      double realRange = calculateRange(realLower, realUpper, type);
      if (realRange <= 0) {
        continue;
      }

      // Normalize real file bounds to 0..1 relative to union bounds
      double realLowerPos = normalizePosition(realLower, ctx.minBounds[i], ctx.ranges[i], type);
      double realUpperPos = normalizePosition(realUpper, ctx.minBounds[i], ctx.ranges[i], type);

      // Clamp to 0..1
      realLowerPos = Math.max(0, Math.min(1, realLowerPos));
      realUpperPos = Math.max(0, Math.min(1, realUpperPos));

      double simLowerPos = simFile.lowerPos[i];
      double simUpperPos = simFile.upperPos[i];

      // Calculate overlap in normalized space
      double overlapLowerPos = Math.max(simLowerPos, realLowerPos);
      double overlapUpperPos = Math.min(simUpperPos, realUpperPos);

      if (overlapLowerPos >= overlapUpperPos) {
        continue; // No overlap
      }

      // Convert overlap back to actual range
      double overlapRange = (overlapUpperPos - overlapLowerPos) * ctx.ranges[i];
      double simRange = (simUpperPos - simLowerPos) * ctx.ranges[i];

      if (simRange <= 0) {
        continue;
      }

      double simCardinality = simFile.records / simRange;
      double realCardinality = realFile.file().recordCount() / realRange;

      totalCost += overlapRange * (simCardinality + realCardinality);
    }

    return totalCost;
  }

  /** Normalize a value to 0..1 position relative to min bound and range. */
  private double normalizePosition(Object value, Object minBound, double range, Type type) {
    if (range <= 0) {
      return 0;
    }
    double distanceFromMin = calculateRange(minBound, value, type);
    return distanceFromMin / range;
  }

  /** Calculate overlap cost between two simulated files. */
  private double calculateCostBetweenSimulated(
      SimulatedFile a, SimulatedFile b, SimulationContext ctx) {
    double totalCost = 0;

    for (int i = 0; i < columnFieldIds.size(); i++) {
      if (ctx.ranges[i] <= 0) {
        continue;
      }

      // Calculate overlap in normalized space
      double overlapLowerPos = Math.max(a.lowerPos[i], b.lowerPos[i]);
      double overlapUpperPos = Math.min(a.upperPos[i], b.upperPos[i]);

      if (overlapLowerPos >= overlapUpperPos) {
        continue; // No overlap
      }

      // Convert to actual range
      double overlapRange = (overlapUpperPos - overlapLowerPos) * ctx.ranges[i];
      double aRange = (a.upperPos[i] - a.lowerPos[i]) * ctx.ranges[i];
      double bRange = (b.upperPos[i] - b.lowerPos[i]) * ctx.ranges[i];

      if (aRange <= 0 || bRange <= 0) {
        continue;
      }

      double aCardinality = a.records / aRange;
      double bCardinality = b.records / bRange;

      totalCost += overlapRange * (aCardinality + bCardinality);
    }

    return totalCost;
  }

  private double calculatePairCost(FileScanTask a, FileScanTask b) {
    double totalCost = 0;

    for (int i = 0; i < columnFieldIds.size(); i++) {
      int fieldId = columnFieldIds.get(i);
      Type type = columnTypes.get(i);

      double cost = calculatePairCostForColumn(a, b, fieldId, type);
      totalCost += cost;
    }

    return totalCost;
  }

  @SuppressWarnings("unchecked")
  private <T extends Comparable<T>> double calculatePairCostForColumn(
      FileScanTask a, FileScanTask b, int fieldId, Type type) {
    ByteBuffer aLowerBuf = a.file().lowerBounds().get(fieldId);
    ByteBuffer aUpperBuf = a.file().upperBounds().get(fieldId);
    ByteBuffer bLowerBuf = b.file().lowerBounds().get(fieldId);
    ByteBuffer bUpperBuf = b.file().upperBounds().get(fieldId);

    if (aLowerBuf == null || aUpperBuf == null || bLowerBuf == null || bUpperBuf == null) {
      return 0; // No bounds, can't calculate
    }

    T aLower = (T) Conversions.fromByteBuffer(type, aLowerBuf);
    T aUpper = (T) Conversions.fromByteBuffer(type, aUpperBuf);
    T bLower = (T) Conversions.fromByteBuffer(type, bLowerBuf);
    T bUpper = (T) Conversions.fromByteBuffer(type, bUpperBuf);

    LOG.debug(
        "OVERLAP: fieldId={} aLower={} aUpper={} bLower={} bUpper={}",
        fieldId,
        aLower,
        aUpper,
        bLower,
        bUpper);

    // Calculate overlap
    T overlapLower = max(aLower, bLower);
    T overlapUpper = min(aUpper, bUpper);

    if (overlapLower.compareTo(overlapUpper) >= 0) {
      return 0; // No overlap
    }

    // Calculate ranges and cardinalities
    double aRange = calculateRange(aLower, aUpper, type);
    double bRange = calculateRange(bLower, bUpper, type);
    double overlap = calculateRange(overlapLower, overlapUpper, type);

    if (aRange <= 0 || bRange <= 0) {
      return 0; // Can't calculate cardinality
    }

    long aRecords = a.file().recordCount();
    long bRecords = b.file().recordCount();

    double aCardinality = aRecords / aRange;
    double bCardinality = bRecords / bRange;

    // Cost = overlap * (card_A + card_B) is the "penalty" for overlapping
    return overlap * (aCardinality + bCardinality);
  }

  private <T extends Comparable<T>> T max(T a, T b) {
    return a.compareTo(b) >= 0 ? a : b;
  }

  private <T extends Comparable<T>> T min(T a, T b) {
    return a.compareTo(b) <= 0 ? a : b;
  }

  /**
   * Calculate the numeric range between two values.
   * For numbers: simple subtraction.
   * For strings: difference of first differing code point.
   */
  private double calculateRange(Object lower, Object upper, Type type) {
    if (lower instanceof Boolean && upper instanceof Boolean) {
      int lowerVal = (Boolean) lower ? 1 : 0;
      int upperVal = (Boolean) upper ? 1 : 0;
      return upperVal - lowerVal;
    } else if (lower instanceof Number && upper instanceof Number) {
      return ((Number) upper).doubleValue() - ((Number) lower).doubleValue();
    } else if (lower instanceof CharSequence && upper instanceof CharSequence) {
      return calculateStringRange((CharSequence) lower, (CharSequence) upper);
    } else {
      // For other types (UUID, binary, etc.) - compare bytes
      return calculateBytesRange(lower, upper, type);
    }
  }

  /**
   * Calculate range for strings based on first differing code point.
   */
  private double calculateStringRange(CharSequence lower, CharSequence upper) {
    String lowerStr = lower.toString();
    String upperStr = upper.toString();

    int i = 0;
    int minLen = Math.min(lowerStr.length(), upperStr.length());

    while (i < minLen) {
      int lowerCp = lowerStr.codePointAt(i);
      int upperCp = upperStr.codePointAt(i);

      if (lowerCp != upperCp) {
        return upperCp - lowerCp;
      }

      i += Character.charCount(lowerCp);
    }

    // One string is prefix of another
    if (lowerStr.length() != upperStr.length()) {
      return 1; // Minimal range
    }

    return 0; // Identical strings
  }

  /**
   * Calculate range for binary/UUID types based on first differing byte.
   */
  private double calculateBytesRange(Object lower, Object upper, Type type) {
    ByteBuffer lowerBuf = Conversions.toByteBuffer(type, lower);
    ByteBuffer upperBuf = Conversions.toByteBuffer(type, upper);

    int minLen = Math.min(lowerBuf.remaining(), upperBuf.remaining());

    for (int i = 0; i < minLen; i++) {
      int lowerByte = lowerBuf.get(lowerBuf.position() + i) & 0xFF;
      int upperByte = upperBuf.get(upperBuf.position() + i) & 0xFF;

      if (lowerByte != upperByte) {
        return upperByte - lowerByte;
      }
    }

    // One is prefix of another
    if (lowerBuf.remaining() != upperBuf.remaining()) {
      return 1;
    }

    return 0;
  }

  private boolean hasAllBounds(FileScanTask task) {
    Map<Integer, ByteBuffer> lowerBounds = task.file().lowerBounds();
    Map<Integer, ByteBuffer> upperBounds = task.file().upperBounds();

    if (lowerBounds == null || upperBounds == null) {
      return false;
    }

    for (int fieldId : columnFieldIds) {
      if (!lowerBounds.containsKey(fieldId) || !upperBounds.containsKey(fieldId)) {
        return false;
      }
    }
    return true;
  }

  private StructLikeMap<List<FileScanTask>> scanFiles() {
    TableScan scan =
        table()
            .newScan()
            .filter(filter)
            .caseSensitive(caseSensitive)
            .ignoreResiduals()
            .includeColumnStats();

    if (snapshotId != null) {
      scan = scan.useSnapshot(snapshotId);
    }

    CloseableIterable<FileScanTask> fileScanTasks = scan.planFiles();
    Types.StructType partitionType = table().spec().partitionType();
    StructLikeMap<List<FileScanTask>> filesByPartition = StructLikeMap.create(partitionType);
    StructLike emptyStruct = GenericRecord.create(partitionType);

    try {
      for (FileScanTask task : fileScanTasks) {
        StructLike partition =
            task.file().specId() == table().spec().specId()
                ? task.file().partition()
                : emptyStruct;
        filesByPartition.computeIfAbsent(partition, k -> Lists.newArrayList()).add(task);
      }
    } finally {
      try {
        fileScanTasks.close();
      } catch (IOException e) {
        LOG.error("Error closing file scan tasks", e);
      }
    }

    return filesByPartition;
  }

  private RewriteFileGroup newRewriteGroup(
      RewriteExecutionContext ctx,
      StructLike partition,
      List<FileScanTask> tasks,
      long inputSplitSize,
      int expectedOutputFiles) {
    FileGroupInfo info =
        ImmutableRewriteDataFiles.FileGroupInfo.builder()
            .globalIndex(ctx.currentGlobalIndex())
            .partitionIndex(ctx.currentPartitionIndex(partition))
            .partition(partition)
            .build();
    return new RewriteFileGroup(
        info,
        Lists.newArrayList(tasks),
        outputSpecId(),
        writeMaxFileSize(),
        inputSplitSize,
        expectedOutputFiles);
  }
}
