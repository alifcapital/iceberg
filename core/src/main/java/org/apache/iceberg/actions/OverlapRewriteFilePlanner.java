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
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
import org.apache.iceberg.util.UuidBucketUtil;
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
  public static final String USE_UUID_PREFIX_BUCKETING = "use-uuid-prefix-bucketing";

  private final Expression filter;
  private final Long snapshotId;
  private final boolean caseSensitive;

  private List<Integer> columnFieldIds;
  private List<Type> columnTypes;
  private long maxGroupSize;
  private long maxGroupInputFiles;
  private boolean skipRewrite;
  private boolean useUuidPrefixBucketing;
  private int numBuckets;

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
        .add(USE_UUID_PREFIX_BUCKETING)
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
      Set<Integer> identifierFieldIds = table().schema().identifierFieldIds();
      if (identifierFieldIds.isEmpty()) {
        LOG.info("OVERLAP [{}]: table has no identifier keys, skipping", table().name());
        this.skipRewrite = true;
        this.columnFieldIds = ImmutableList.of();
        this.columnTypes = ImmutableList.of();
        return;
      }
      // Sort by field ID to match the sort order used in SparkOverlapFileRewriteRunner
      List<Integer> sortedFieldIds =
          identifierFieldIds.stream().sorted().collect(Collectors.toList());
      columns =
          sortedFieldIds.stream()
              .map(table().schema()::findColumnName)
              .collect(Collectors.toList());
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
      LOG.info("OVERLAP [{}]: init column='{}' fieldId={} type={}", table().name(), column, field.fieldId(), field.type());
    }

    // Parse group limits
    this.maxGroupSize =
        PropertyUtil.propertyAsLong(
            options, MAX_FILE_GROUP_SIZE_BYTES, MAX_FILE_GROUP_SIZE_BYTES_DEFAULT);
    this.maxGroupInputFiles =
        PropertyUtil.propertyAsLong(
            options, MAX_FILE_GROUP_INPUT_FILES, MAX_FILE_GROUP_INPUT_FILES_DEFAULT);

    // UUID prefix bucketing
    this.useUuidPrefixBucketing =
        PropertyUtil.propertyAsBoolean(options, USE_UUID_PREFIX_BUCKETING, false);
    if (useUuidPrefixBucketing) {
      long totalSize = getTotalFilesSize();
      this.numBuckets = UuidBucketUtil.computeBuckets(totalSize, targetFileSize());
      LOG.info(
          "OVERLAP [{}]: UUID prefix bucketing enabled, totalSize={}, numBuckets={}",
          table().name(),
          totalSize,
          numBuckets);
    }
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

      // Filter files that have bounds for all columns and are above min size
      List<FileScanTask> validFiles =
          partitionFiles.stream()
              .filter(this::hasAllBounds)
              .filter(task -> task.length() >= minFileSize())
              .collect(Collectors.toList());

      LOG.info("OVERLAP [{}]: partition={} totalFiles={}", table().name(), partition, partitionFiles.size());

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

  private static final int PAIRS_BATCH_SIZE = 1000;
  private static final int MIN_IMPROVEMENT = 1000;

  private List<FileScanTask> findBestGroup(List<FileScanTask> files) {
    if (files.size() < 2) {
      return null;
    }

    // Find overlapping pairs efficiently using sweep line algorithm
    List<int[]> overlappingPairs = findOverlappingPairs(files);

    LOG.info("OVERLAP [{}]: {} files, {} overlapping pairs", table().name(), files.size(), overlappingPairs.size());

    if (overlappingPairs.isEmpty()) {
      LOG.info("OVERLAP [{}]: no overlapping files, already optimized", table().name());
      return null;
    }

    // Cache bucket ranges for all files - used by sort and improvement calculation
    Map<FileScanTask, int[]> bucketRanges = new java.util.HashMap<>();
    for (FileScanTask task : files) {
      bucketRanges.put(task, getBucketRange(task));
    }

    if (Thread.interrupted()) {
      Thread.currentThread().interrupt();
      return null;
    }

    // Sort pairs by (bucketSpan × records) descending
    // Files spanning more buckets with more records have higher potential improvement
    // Without UUID bucketing: bucketSpan=1, so this degrades to sorting by records
    overlappingPairs.sort(
        (p1, p2) -> {
          int[] r1a = bucketRanges.get(files.get(p1[0]));
          int[] r1b = bucketRanges.get(files.get(p1[1]));
          int[] r2a = bucketRanges.get(files.get(p2[0]));
          int[] r2b = bucketRanges.get(files.get(p2[1]));
          long score1 = (long) (r1a[1] - r1a[0] + 1) * files.get(p1[0]).file().recordCount()
                      + (long) (r1b[1] - r1b[0] + 1) * files.get(p1[1]).file().recordCount();
          long score2 = (long) (r2a[1] - r2a[0] + 1) * files.get(p2[0]).file().recordCount()
                      + (long) (r2b[1] - r2b[0] + 1) * files.get(p2[1]).file().recordCount();
          return Long.compare(score2, score1); // descending
        });

    // Stage 1: Try to find pair with positive improvement, checking in batches
    List<FileScanTask> bestPair = findBestPairIncremental(files, overlappingPairs, bucketRanges);

    if (bestPair != null) {
      return expandGroup(files, bestPair, bucketRanges);
    }

    // Stage 2: No positive improvement found - skip fallback
    // Fallback was causing inefficient "wave" behavior on consecutive UUID files
    // where each iteration only merged 1-2 files at a time
    LOG.info("OVERLAP [{}]: no positive improvement found, skipping rewrite", table().name());
    return null;
  }

  /**
   * Stage 1: Find best pair with positive improvement, checking batches of pairs.
   */
  private List<FileScanTask> findBestPairIncremental(
      List<FileScanTask> files, List<int[]> overlappingPairs, Map<FileScanTask, int[]> bucketRanges) {
    int totalPairs = overlappingPairs.size();
    int batchStart = 0;

    while (batchStart < totalPairs) {
      if (Thread.interrupted()) {
        LOG.info(
            "OVERLAP [{}]: findBestPairIncremental interrupted at batch {}",
            table().name(),
            batchStart);
        Thread.currentThread().interrupt();
        return null;
      }

      int batchEnd = Math.min(batchStart + PAIRS_BATCH_SIZE, totalPairs);

      double bestImprovement = 0;
      List<FileScanTask> bestPair = null;
      int pairsChecked = 0;

      for (int i = batchStart; i < batchEnd; i++) {
        int[] pair = overlappingPairs.get(i);
        FileScanTask a = files.get(pair[0]);
        FileScanTask b = files.get(pair[1]);

        pairsChecked++;
        double improvement = calculateMergeImprovement(files, Arrays.asList(a, b), bucketRanges);

        if (improvement > bestImprovement) {
          bestImprovement = improvement;
          bestPair = Arrays.asList(a, b);
        }
      }

      LOG.info(
          "OVERLAP [{}]: batch {}-{} of {}, checked {}, bestImprovement={}",
          table().name(),
          batchStart,
          batchEnd,
          totalPairs,
          pairsChecked,
          (long) bestImprovement);

      if (bestPair != null && bestImprovement >= MIN_IMPROVEMENT) {
        LOG.info(
            "OVERLAP [{}]: best pair {} + {} records, improvement={}",
            table().name(),
            bestPair.get(0).file().recordCount(),
            bestPair.get(1).file().recordCount(),
            (long) bestImprovement);
        return bestPair;
      }

      batchStart = batchEnd;
    }

    LOG.info("OVERLAP [{}]: no positive improvement found in any batch", table().name());
    return null;
  }

  /**
   * Stage 2: Build a fallback group starting from min lower bound among overlapping files.
   * This forces a full sort of a region with overlaps.
   *
   * <p>NOTE: Currently disabled due to inefficient "wave" behavior on consecutive UUID files.
   * Kept for potential future use with improved logic.
   */
  @SuppressWarnings({"unchecked", "UnusedMethod"})
  private List<FileScanTask> buildFallbackGroup(
      List<FileScanTask> files, List<int[]> overlappingPairs) {
    if (columnFieldIds.isEmpty()) {
      return null;
    }

    // Collect indices of files that have overlaps
    Set<Integer> overlappingFileIndices = new java.util.HashSet<>();
    for (int[] pair : overlappingPairs) {
      overlappingFileIndices.add(pair[0]);
      overlappingFileIndices.add(pair[1]);
    }

    int fieldId = columnFieldIds.get(0);
    Type type = columnTypes.get(0);

    // Get overlapping files and sort by lower bound
    List<FileScanTask> overlappingFiles = new ArrayList<>();
    for (int idx : overlappingFileIndices) {
      overlappingFiles.add(files.get(idx));
    }

    if (overlappingFiles.isEmpty()) {
      return null;
    }

    overlappingFiles.sort(
        (f1, f2) -> {
          ByteBuffer buf1 = f1.file().lowerBounds().get(fieldId);
          ByteBuffer buf2 = f2.file().lowerBounds().get(fieldId);
          if (buf1 == null && buf2 == null) return 0;
          if (buf1 == null) return 1;
          if (buf2 == null) return -1;
          Comparable<Object> v1 = (Comparable<Object>) Conversions.fromByteBuffer(type, buf1);
          Comparable<Object> v2 = (Comparable<Object>) Conversions.fromByteBuffer(type, buf2);
          return v1.compareTo(v2);
        });

    // Build group: add overlapping files in order until we hit limits
    List<FileScanTask> group = new ArrayList<>();
    long groupSize = 0;

    for (FileScanTask task : overlappingFiles) {
      if (group.size() >= maxGroupInputFiles) {
        break;
      }
      if (groupSize + task.length() > maxGroupSize && !group.isEmpty()) {
        break;
      }
      group.add(task);
      groupSize += task.length();
    }

    if (group.size() < 2) {
      return null;
    }

    // Get lower bound from first file for logging
    ByteBuffer firstLowerBuf = group.get(0).file().lowerBounds().get(fieldId);
    Object firstLower = firstLowerBuf != null ? Conversions.fromByteBuffer(type, firstLowerBuf) : null;

    LOG.info(
        "OVERLAP [{}]: fallback group starting at lower={}, {} files, {} bytes",
        table().name(),
        firstLower,
        group.size(),
        groupSize);

    return group;
  }

  /**
   * Expand a group by adding more files while it improves the cost.
   */
  private List<FileScanTask> expandGroup(
      List<FileScanTask> files, List<FileScanTask> initialPair, Map<FileScanTask, int[]> bucketRanges) {
    List<FileScanTask> group = new ArrayList<>(initialPair);
    List<FileScanTask> remaining =
        files.stream().filter(f -> !group.contains(f)).collect(Collectors.toList());

    double bestImprovement = calculateMergeImprovement(files, group, bucketRanges);

    boolean improved = true;
    while (improved && !remaining.isEmpty()) {
      if (Thread.interrupted()) {
        LOG.info("OVERLAP [{}]: expandGroup interrupted, returning current group", table().name());
        Thread.currentThread().interrupt();
        return group;
      }

      if (group.size() >= maxGroupInputFiles) {
        break;
      }

      long currentGroupSize = group.stream().mapToLong(FileScanTask::length).sum();

      improved = false;
      FileScanTask bestAddition = null;
      double bestAdditionImprovement = 0;

      for (FileScanTask candidate : remaining) {
        if (currentGroupSize + candidate.length() > maxGroupSize) {
          continue;
        }

        List<FileScanTask> testGroup = new ArrayList<>(group);
        testGroup.add(candidate);

        double newImprovement = calculateMergeImprovement(files, testGroup, bucketRanges);
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
            "OVERLAP [{}]: added file, groupSize={} totalBytes={} improvement={}",
            table().name(),
            group.size(),
            group.stream().mapToLong(FileScanTask::length).sum(),
            (long) bestImprovement);
      }
    }

    return group;
  }

  /**
   * Find overlapping file pairs using sweep line algorithm. Average case O(n log n + k) where k is
   * number of overlapping pairs. Worst case O(n²) when all files overlap.
   */
  @SuppressWarnings("unchecked")
  private List<int[]> findOverlappingPairs(List<FileScanTask> files) {
    if (columnFieldIds.isEmpty()) {
      return ImmutableList.of();
    }

    // Use first column for sweep line
    int fieldId = columnFieldIds.get(0);
    Type type = columnTypes.get(0);

    // Build list of (index, lower, upper) for sorting
    List<int[]> fileIndices = new ArrayList<>();
    List<Comparable<Object>> lowers = new ArrayList<>();
    List<Comparable<Object>> uppers = new ArrayList<>();

    for (int i = 0; i < files.size(); i++) {
      FileScanTask task = files.get(i);
      ByteBuffer lowerBuf = task.file().lowerBounds().get(fieldId);
      ByteBuffer upperBuf = task.file().upperBounds().get(fieldId);

      if (lowerBuf != null && upperBuf != null) {
        Comparable<Object> lower = (Comparable<Object>) Conversions.fromByteBuffer(type, lowerBuf);
        Comparable<Object> upper = (Comparable<Object>) Conversions.fromByteBuffer(type, upperBuf);
        fileIndices.add(new int[] {i});
        lowers.add(lower);
        uppers.add(upper);
      }
    }

    // Sort by lower bound
    Integer[] sortedIdx = new Integer[fileIndices.size()];
    for (int i = 0; i < sortedIdx.length; i++) {
      sortedIdx[i] = i;
    }
    Arrays.sort(sortedIdx, (a, b) -> lowers.get(a).compareTo(lowers.get(b)));

    // Sweep line: for each file, find all files that overlap with it
    List<int[]> overlappingPairs = new ArrayList<>();

    for (int i = 0; i < sortedIdx.length; i++) {
      if (Thread.interrupted()) {
        LOG.info(
            "OVERLAP [{}]: findOverlappingPairs interrupted at file {}",
            table().name(),
            i);
        Thread.currentThread().interrupt();
        return ImmutableList.of();
      }

      int idxI = sortedIdx[i];
      int fileI = fileIndices.get(idxI)[0];
      Comparable<Object> upperI = uppers.get(idxI);

      // Check following files until their lower > our upper
      for (int j = i + 1; j < sortedIdx.length; j++) {
        int idxJ = sortedIdx[j];
        Comparable<Object> lowerJ = lowers.get(idxJ);

        // If lowerJ >= upperI, no overlap (files just touch at boundary)
        if (lowerJ.compareTo(upperI) >= 0) {
          break;
        }

        int fileJ = fileIndices.get(idxJ)[0];
        overlappingPairs.add(new int[] {fileI, fileJ});
      }
    }

    return overlappingPairs;
  }

  /**
   * Calculate improvement from merging a group of files.
   *
   * <p>Unified logic for both UUID and non-UUID tables:
   * 1. Determine which buckets the group spans (without UUID = 1 bucket)
   * 2. For each bucket, calculate improvement among files intersecting that bucket
   * 3. Sum improvements across all buckets
   *
   * <p>Within each bucket, we use honest string-level cardinality-based cost.
   * After merge+sort+split, files within a bucket are sorted and non-overlapping.
   */
  private double calculateMergeImprovement(
      List<FileScanTask> allFiles, List<FileScanTask> group, Map<FileScanTask, int[]> bucketRanges) {
    Set<FileScanTask> groupSet = ImmutableSet.copyOf(group);
    List<FileScanTask> remaining =
        allFiles.stream().filter(f -> !groupSet.contains(f)).collect(Collectors.toList());

    // Determine bucket range for the group
    int minBucket = Integer.MAX_VALUE;
    int maxBucket = Integer.MIN_VALUE;
    for (FileScanTask task : group) {
      int[] range = bucketRanges.get(task);
      minBucket = Math.min(minBucket, range[0]);
      maxBucket = Math.max(maxBucket, range[1]);
    }

    if (minBucket > maxBucket) {
      return 0;
    }

    // Fast path: if group spans multiple buckets, improvement is obvious
    // No need for expensive per-bucket calculation
    if (maxBucket > minBucket) {
      long totalRecords = group.stream().mapToLong(t -> t.file().recordCount()).sum();
      int totalSpan = group.stream().mapToInt(t -> {
        int[] r = bucketRanges.get(t);
        return r[1] - r[0] + 1;
      }).sum();
      return (double) totalSpan * totalRecords;
    }

    // Slow path: all files in same bucket - calculate honest improvement
    double totalImprovement = 0;

    for (int bucket = minBucket; bucket <= maxBucket; bucket++) {
      if (Thread.interrupted()) {
        Thread.currentThread().interrupt();
        return 0;
      }

      // Find group files intersecting this bucket
      List<FileScanTask> groupInBucket = new ArrayList<>();
      for (FileScanTask task : group) {
        int[] range = bucketRanges.get(task);
        if (range[0] <= bucket && bucket <= range[1]) {
          groupInBucket.add(task);
        }
      }

      // Find remaining files intersecting this bucket
      List<FileScanTask> remainingInBucket = new ArrayList<>();
      for (FileScanTask task : remaining) {
        int[] range = bucketRanges.get(task);
        if (range[0] <= bucket && bucket <= range[1]) {
          remainingInBucket.add(task);
        }
      }

      if (groupInBucket.size() < 2 && remainingInBucket.isEmpty()) {
        continue; // Nothing to improve in this bucket
      }

      // Calculate costBefore: honest string-level cost within this bucket
      double costBefore = 0;

      // Internal costs within group (in this bucket)
      for (int i = 0; i < groupInBucket.size(); i++) {
        for (int j = i + 1; j < groupInBucket.size(); j++) {
          costBefore += calculatePairCost(groupInBucket.get(i), groupInBucket.get(j));
        }
      }

      // External costs: group vs remaining (in this bucket)
      for (FileScanTask groupFile : groupInBucket) {
        for (FileScanTask remainingFile : remainingInBucket) {
          costBefore += calculatePairCost(groupFile, remainingFile);
        }
      }

      // Calculate costAfter: after merge+sort+split within bucket
      // Files are sorted by primary column → no overlap on primary column
      // Use simulation to estimate costAfter
      double costAfter = 0;
      if (!groupInBucket.isEmpty()) {
        SimulationResult simulation = simulateMergeSplitForBucket(groupInBucket);

        // Internal cost between simulated files
        for (int i = 0; i < simulation.files.size(); i++) {
          for (int j = i + 1; j < simulation.files.size(); j++) {
            costAfter += calculateCostBetweenSimulated(
                simulation.files.get(i), simulation.files.get(j), simulation.context);
          }
        }

        // External cost: simulated files vs remaining in bucket
        for (SimulatedFile simFile : simulation.files) {
          for (FileScanTask remainingFile : remainingInBucket) {
            costAfter += calculateCostWithSimulated(simFile, remainingFile, simulation.context);
          }
        }
      }

      totalImprovement += (costBefore - costAfter);
    }

    return totalImprovement;
  }

  /**
   * Simulate merge+sort+split for files within a single bucket.
   * Uses regular positional splitting (not UUID bucket boundaries).
   */
  @SuppressWarnings("unchecked")
  private SimulationResult simulateMergeSplitForBucket(List<FileScanTask> bucketFiles) {
    SimulationContext ctx = new SimulationContext(columnFieldIds.size());

    long totalRecords = 0;
    long totalBytes = 0;

    // Find union bounds for each column
    for (FileScanTask task : bucketFiles) {
      totalRecords += task.file().recordCount();
      totalBytes += task.length();

      for (int i = 0; i < columnFieldIds.size(); i++) {
        int fieldId = columnFieldIds.get(i);
        Type type = columnTypes.get(i);

        ByteBuffer lowerBuf = task.file().lowerBounds().get(fieldId);
        ByteBuffer upperBuf = task.file().upperBounds().get(fieldId);

        if (lowerBuf != null && upperBuf != null) {
          Comparable<Object> lower = (Comparable<Object>) Conversions.fromByteBuffer(type, lowerBuf);
          Comparable<Object> upper = (Comparable<Object>) Conversions.fromByteBuffer(type, upperBuf);

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

    // Calculate number of output files (don't create more files than records)
    int numOutputFiles = (int) Math.min(expectedOutputFiles(totalBytes), Math.max(1, totalRecords));
    if (numOutputFiles == 0) {
      return new SimulationResult(ctx, ImmutableList.of());
    }

    // Create simulated files with non-overlapping normalized positions
    List<SimulatedFile> result = new ArrayList<>();
    long recordsPerFile = Math.max(1, totalRecords / numOutputFiles);

    for (int f = 0; f < numOutputFiles; f++) {
      double[] fileLowerPos = new double[columnFieldIds.size()];
      double[] fileUpperPos = new double[columnFieldIds.size()];

      // First column: split evenly (non-overlapping after sort)
      fileLowerPos[0] = (double) f / numOutputFiles;
      fileUpperPos[0] = (double) (f + 1) / numOutputFiles;

      // Other columns: full range (conservative estimate)
      for (int i = 1; i < columnFieldIds.size(); i++) {
        fileLowerPos[i] = 0.0;
        fileUpperPos[i] = 1.0;
      }

      long fileRecords = (f == numOutputFiles - 1)
          ? totalRecords - recordsPerFile * f
          : recordsPerFile;

      result.add(new SimulatedFile(fileLowerPos, fileUpperPos, fileRecords));
    }

    return new SimulationResult(ctx, result);
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
      // Normalize real file bounds to 0..1 relative to union bounds
      double realLowerPos = normalizePosition(realLower, ctx.minBounds[i], ctx.ranges[i], type);
      double realUpperPos = normalizePosition(realUpper, ctx.minBounds[i], ctx.ranges[i], type);

      if (realRange <= 0) {
        continue;
      }

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
   * Calculate range for strings using byte representation.
   *
   * <p>UTF-8 preserves lexicographic ordering when compared byte-by-byte, so we convert strings to
   * bytes and compute the numeric difference. This correctly handles position weight - a difference
   * at position 0 has more weight than at position 1.
   */
  private double calculateStringRange(CharSequence lower, CharSequence upper) {
    byte[] lowerBytes = lower.toString().getBytes(StandardCharsets.UTF_8);
    byte[] upperBytes = upper.toString().getBytes(StandardCharsets.UTF_8);

    // Limit to 16 bytes and pad to same length
    int maxLen = Math.min(Math.max(lowerBytes.length, upperBytes.length), 16);

    byte[] lowerPadded = new byte[maxLen];
    byte[] upperPadded = new byte[maxLen];
    System.arraycopy(lowerBytes, 0, lowerPadded, 0, Math.min(lowerBytes.length, maxLen));
    System.arraycopy(upperBytes, 0, upperPadded, 0, Math.min(upperBytes.length, maxLen));

    // Use BigInteger for correct arithmetic with large numbers
    BigInteger lowerBig = new BigInteger(1, lowerPadded);
    BigInteger upperBig = new BigInteger(1, upperPadded);

    return upperBig.subtract(lowerBig).doubleValue();
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

  /** Gets total files size from snapshot summary. */
  private long getTotalFilesSize() {
    if (table().currentSnapshot() == null) {
      return 0;
    }

    String totalSizeStr = table().currentSnapshot().summary().get("total-files-size");
    if (totalSizeStr == null) {
      return 0;
    }

    try {
      return Long.parseLong(totalSizeStr);
    } catch (NumberFormatException e) {
      LOG.warn(
          "OVERLAP [{}]: Cannot parse total-files-size: {}", table().name(), totalSizeStr, e);
      return 0;
    }
  }

  /**
   * Get the bucket range [minBucket, maxBucket] for a file.
   *
   * <p>Without UUID bucketing, returns [0, 0] (single bucket for all files).
   */
  private int[] getBucketRange(FileScanTask task) {
    if (!useUuidPrefixBucketing || numBuckets <= 0 || columnFieldIds.isEmpty()) {
      return new int[] {0, 0};
    }

    int fieldId = columnFieldIds.get(0);
    Type type = columnTypes.get(0);

    ByteBuffer lowerBuf = task.file().lowerBounds().get(fieldId);
    ByteBuffer upperBuf = task.file().upperBounds().get(fieldId);

    if (lowerBuf == null || upperBuf == null) {
      return new int[] {0, 0};
    }

    Object lower = Conversions.fromByteBuffer(type, lowerBuf);
    Object upper = Conversions.fromByteBuffer(type, upperBuf);

    // Verify bounds are actually UUIDs
    if (!(lower instanceof CharSequence) || !UuidBucketUtil.looksLikeUuid(lower.toString())) {
      return new int[] {0, 0};
    }

    int lowerHex = UuidBucketUtil.extractHexPrefix(lower);
    int upperHex = UuidBucketUtil.extractHexPrefix(upper);

    int lowerBucket = UuidBucketUtil.hexToBucket(lowerHex, numBuckets);
    int upperBucket = UuidBucketUtil.hexToBucket(upperHex, numBuckets);

    return new int[] {lowerBucket, upperBucket};
  }
}
