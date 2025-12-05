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

import static org.apache.spark.sql.functions.col;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.ConvertEqualityDeleteFiles;
import org.apache.iceberg.actions.ImmutableConvertEqualityDeleteFiles;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.deletes.DeleteGranularity;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.SortingPositionOnlyDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.FileWriter;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.source.SerializableTableWithSize;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeSet;
import org.apache.iceberg.util.Tasks;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spark implementation of {@link ConvertEqualityDeleteFiles}.
 *
 * <p>This action converts equality delete files to position delete files. The conversion is done
 * by:
 *
 * <ol>
 *   <li>Finding all equality delete files in the current snapshot
 *   <li>Reading equality delete keys from delete files (build side for hash join)
 *   <li>Reading data files with position metadata (probe side for hash join)
 *   <li>Performing leftsemi join to find rows matching delete keys
 *   <li>Writing position delete files for matching rows
 *   <li>Committing: removing old equality deletes, adding new position deletes
 * </ol>
 *
 * <p>The commit validates from the starting snapshot to detect concurrent modifications. If the
 * equality delete files were modified by another operation, the commit will fail with {@link
 * ValidationException} or {@link CommitFailedException}.
 */
public class ConvertEqualityDeleteFilesSparkAction
    extends BaseSnapshotUpdateSparkAction<ConvertEqualityDeleteFilesSparkAction>
    implements ConvertEqualityDeleteFiles {

  private static final Logger LOG =
      LoggerFactory.getLogger(ConvertEqualityDeleteFilesSparkAction.class);

  private static final String LOG_PREFIX = "[ConvertEqDeletes]";

  private static final Result EMPTY_RESULT =
      ImmutableConvertEqualityDeleteFiles.Result.builder()
          .convertedEqualityDeleteFilesCount(0)
          .addedPositionDeleteFilesCount(0)
          .rewrittenDeleteRecordsCount(0L)
          .addedDeleteRecordsCount(0L)
          .build();

  private final Table table;
  private Expression filter = Expressions.alwaysTrue();

  ConvertEqualityDeleteFilesSparkAction(SparkSession spark, Table table) {
    super(spark.cloneSession());
    // Disable AQE to ensure predictable join behavior for the hash join
    spark().conf().set(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), false);
    this.table = table;
  }

  @Override
  protected ConvertEqualityDeleteFilesSparkAction self() {
    return this;
  }

  @Override
  public ConvertEqualityDeleteFilesSparkAction filter(Expression expression) {
    filter = Expressions.and(filter, expression);
    return this;
  }

  @Override
  public Result execute() {
    long startTime = System.currentTimeMillis();

    if (table.currentSnapshot() == null) {
      LOG.info("{} table={} empty table, nothing to convert", LOG_PREFIX, table.name());
      return EMPTY_RESULT;
    }

    long startingSnapshotId = table.currentSnapshot().snapshotId();
    LOG.info(
        "{} table={} snapshot={} filter={} starting conversion",
        LOG_PREFIX,
        table.name(),
        startingSnapshotId,
        filter);

    // Step 1: Find all tasks that have equality delete files
    long scanStartTime = System.currentTimeMillis();
    Map<DeleteFileGroup, List<FileScanTask>> tasksWithEqDeletes =
        findTasksWithEqualityDeletes(startingSnapshotId);
    long scanDuration = System.currentTimeMillis() - scanStartTime;

    if (tasksWithEqDeletes.isEmpty()) {
      LOG.info(
          "{} table={} scan_duration_ms={} no equality delete files found",
          LOG_PREFIX,
          table.name(),
          scanDuration);
      return EMPTY_RESULT;
    }

    int totalEqDeleteFiles =
        tasksWithEqDeletes.keySet().stream().mapToInt(g -> g.deleteFiles().size()).sum();
    int totalDataFiles = tasksWithEqDeletes.values().stream().mapToInt(List::size).sum();

    LOG.info(
        "{} table={} scan_duration_ms={} eq_delete_groups={} eq_delete_files={} data_files={}",
        LOG_PREFIX,
        table.name(),
        scanDuration,
        tasksWithEqDeletes.size(),
        totalEqDeleteFiles,
        totalDataFiles);

    Set<DeleteFile> convertedEqDeleteFiles = Sets.newHashSet();
    Set<DeleteFile> addedPosDeleteFiles = Sets.newHashSet();
    long totalRewrittenRecords = 0;
    long totalAddedRecords = 0;

    int groupIndex = 0;
    for (Map.Entry<DeleteFileGroup, List<FileScanTask>> entry : tasksWithEqDeletes.entrySet()) {
      groupIndex++;
      DeleteFileGroup eqDeleteGroup = entry.getKey();
      List<FileScanTask> dataFileTasks = entry.getValue();

      LOG.info(
          "{} table={} group={}/{} eq_delete_files={} data_files={} processing",
          LOG_PREFIX,
          table.name(),
          groupIndex,
          tasksWithEqDeletes.size(),
          eqDeleteGroup.deleteFiles().size(),
          dataFileTasks.size());

      ConversionResult conversionResult =
          convertEqualityDeletes(eqDeleteGroup, dataFileTasks, startingSnapshotId, groupIndex);

      convertedEqDeleteFiles.addAll(eqDeleteGroup.deleteFiles());
      addedPosDeleteFiles.addAll(conversionResult.posDeleteFiles);
      totalRewrittenRecords += conversionResult.eqDeleteRecordsCount;
      totalAddedRecords += conversionResult.posDeleteRecordsCount;
    }

    // Commit
    if (!convertedEqDeleteFiles.isEmpty()) {
      LOG.info(
          "{} table={} eq_delete_files_to_remove={} pos_delete_files_to_add={} "
              + "eq_delete_records={} pos_delete_records={} committing",
          LOG_PREFIX,
          table.name(),
          convertedEqDeleteFiles.size(),
          addedPosDeleteFiles.size(),
          totalRewrittenRecords,
          totalAddedRecords);

      try {
        long commitStartTime = System.currentTimeMillis();
        commitChanges(convertedEqDeleteFiles, addedPosDeleteFiles, startingSnapshotId);
        long commitDuration = System.currentTimeMillis() - commitStartTime;
        LOG.info(
            "{} table={} commit_duration_ms={} commit successful",
            LOG_PREFIX,
            table.name(),
            commitDuration);
      } catch (ValidationException | CommitFailedException e) {
        LOG.warn(
            "{} table={} pos_delete_files_to_cleanup={} commit failed, cleaning up",
            LOG_PREFIX,
            table.name(),
            addedPosDeleteFiles.size());
        cleanUpFiles(addedPosDeleteFiles);
        throw new RuntimeException(
            "Cannot commit because of a concurrent modification. "
                + "The equality delete files may have been modified by another operation.",
            e);
      }
    }

    long totalDuration = System.currentTimeMillis() - startTime;
    LOG.info(
        "{} table={} total_duration_ms={} converted_eq_delete_files={} "
            + "added_pos_delete_files={} rewritten_records={} added_records={} completed",
        LOG_PREFIX,
        table.name(),
        totalDuration,
        convertedEqDeleteFiles.size(),
        addedPosDeleteFiles.size(),
        totalRewrittenRecords,
        totalAddedRecords);

    return ImmutableConvertEqualityDeleteFiles.Result.builder()
        .convertedEqualityDeleteFilesCount(convertedEqDeleteFiles.size())
        .addedPositionDeleteFilesCount(addedPosDeleteFiles.size())
        .rewrittenDeleteRecordsCount(totalRewrittenRecords)
        .addedDeleteRecordsCount(totalAddedRecords)
        .build();
  }

  private void cleanUpFiles(Set<DeleteFile> files) {
    Tasks.foreach(files)
        .noRetry()
        .suppressFailureWhenFinished()
        .onFailure(
            (file, exc) ->
                LOG.warn(
                    "{} table={} file={} cleanup failed",
                    LOG_PREFIX,
                    table.name(),
                    file.path(),
                    exc))
        .run(file -> table.io().deleteFile(file.path().toString()));
  }

  private Map<DeleteFileGroup, List<FileScanTask>> findTasksWithEqualityDeletes(long snapshotId) {
    Map<DeleteFileGroup, List<FileScanTask>> result = Maps.newHashMap();

    try (CloseableIterable<CombinedScanTask> combinedTasks =
        table
            .newScan()
            .useSnapshot(snapshotId)
            .filter(filter)
            .includeColumnStats()
            .planTasks()) {

      for (CombinedScanTask combinedTask : combinedTasks) {
        for (FileScanTask task : combinedTask.files()) {
          List<DeleteFile> eqDeletes =
              task.deletes().stream()
                  .filter(d -> d.content() == FileContent.EQUALITY_DELETES)
                  .collect(Collectors.toList());

          if (!eqDeletes.isEmpty()) {
            DeleteFileGroup key = new DeleteFileGroup(eqDeletes, task.spec());
            result.computeIfAbsent(key, k -> Lists.newArrayList()).add(task);
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to plan scan tasks", e);
    }

    return result;
  }

  private ConversionResult convertEqualityDeletes(
      DeleteFileGroup eqDeleteGroup,
      List<FileScanTask> dataFileTasks,
      long snapshotId,
      int groupIndex) {

    List<DeleteFile> eqDeleteFiles = eqDeleteGroup.deleteFiles();
    PartitionSpec spec = eqDeleteGroup.spec();

    Set<Integer> allEqualityFieldIds = Sets.newHashSet();
    for (DeleteFile eqDelete : eqDeleteFiles) {
      allEqualityFieldIds.addAll(eqDelete.equalityFieldIds());
    }

    Schema deleteSchema = TypeUtil.select(table.schema(), allEqualityFieldIds);
    List<String> deleteColumnNames =
        deleteSchema.columns().stream().map(Types.NestedField::name).collect(Collectors.toList());

    StructLike partition =
        dataFileTasks.isEmpty() ? null : dataFileTasks.get(0).file().partition();
    int specId = spec.specId();

    long totalEqDeleteFileSize =
        eqDeleteFiles.stream().mapToLong(DeleteFile::fileSizeInBytes).sum();

    LOG.info(
        "{} table={} group={} eq_delete_files={} eq_delete_size_bytes={} "
            + "equality_columns={} partition={} reading eq deletes",
        LOG_PREFIX,
        table.name(),
        groupIndex,
        eqDeleteFiles.size(),
        totalEqDeleteFileSize,
        deleteColumnNames,
        partition);

    // Step 1: Read equality delete keys on driver and broadcast
    long readEqStartTime = System.currentTimeMillis();
    DeleteLoader deleteLoader =
        new BaseDeleteLoader(deleteFile -> table.io().newInputFile(deleteFile.path().toString()));
    StructLikeSet eqDeleteKeys = deleteLoader.loadEqualityDeletes(eqDeleteFiles, deleteSchema);
    long eqDeleteRecordsCount = eqDeleteKeys.size();
    long readEqDuration = System.currentTimeMillis() - readEqStartTime;

    LOG.info(
        "{} table={} group={} read_eq_duration_ms={} eq_delete_records={}",
        LOG_PREFIX,
        table.name(),
        groupIndex,
        readEqDuration,
        eqDeleteRecordsCount);

    if (eqDeleteRecordsCount == 0) {
      LOG.info(
          "{} table={} group={} no equality delete records found",
          LOG_PREFIX,
          table.name(),
          groupIndex);
      return new ConversionResult(Sets.newHashSet(), 0, 0);
    }

    // Convert to serializable Set for broadcast
    Set<List<Object>> deleteKeysSet = Sets.newHashSetWithExpectedSize(eqDeleteKeys.size());
    for (StructLike key : eqDeleteKeys) {
      List<Object> keyValues = Lists.newArrayListWithCapacity(deleteSchema.columns().size());
      for (int i = 0; i < deleteSchema.columns().size(); i++) {
        keyValues.add(key.get(i, Object.class));
      }
      deleteKeysSet.add(keyValues);
    }

    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark().sparkContext());
    Broadcast<Set<List<Object>>> broadcastKeys = jsc.broadcast(deleteKeysSet);

    // Step 2: Build list of data files to process
    List<DataFileInfo> dataFileInfos =
        dataFileTasks.stream()
            .map(t -> new DataFileInfo(t.file().path().toString(), t.file().format().name()))
            .distinct()
            .collect(Collectors.toList());

    long totalDataFileSize =
        dataFileTasks.stream()
            .map(FileScanTask::file)
            .distinct()
            .mapToLong(f -> f.fileSizeInBytes())
            .sum();

    LOG.info(
        "{} table={} group={} data_files={} data_size_bytes={} reading on executors",
        LOG_PREFIX,
        table.name(),
        groupIndex,
        dataFileInfos.size(),
        totalDataFileSize);

    // Step 3: Distributed read on executors - read files and filter by broadcast keys
    long readDataStartTime = System.currentTimeMillis();

    Table serializableTable = SerializableTableWithSize.copyOf(table);
    Broadcast<Table> tableBroadcast = jsc.broadcast(serializableTable);
    Schema projectionSchema = deleteSchema;

    JavaRDD<DataFileInfo> filesRDD = jsc.parallelize(dataFileInfos, dataFileInfos.size());

    JavaRDD<PositionDeleteRecord> posDeletesRDD =
        filesRDD.flatMap(
            new ReadAndFilterFunction(tableBroadcast, projectionSchema, broadcastKeys));

    // Collect results
    List<PositionDeleteRecord> posDeletes = posDeletesRDD.collect();
    long readDataDuration = System.currentTimeMillis() - readDataStartTime;

    LOG.info(
        "{} table={} group={} read_and_filter_duration_ms={} matched_rows={}",
        LOG_PREFIX,
        table.name(),
        groupIndex,
        readDataDuration,
        posDeletes.size());

    // Step 4: Write position delete files
    return writePosDeleteFilesFromRecords(
        posDeletes, specId, partition, snapshotId, eqDeleteRecordsCount, groupIndex);
  }

  /** Serializable data file info for distribution to executors. */
  private static class DataFileInfo implements Serializable {
    private final String path;
    private final String format;

    DataFileInfo(String path, String format) {
      this.path = path;
      this.format = format;
    }

    String path() {
      return path;
    }

    FileFormat format() {
      return FileFormat.fromString(format);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      DataFileInfo that = (DataFileInfo) o;
      return path.equals(that.path);
    }

    @Override
    public int hashCode() {
      return path.hashCode();
    }
  }

  /** Serializable position delete record. */
  private static class PositionDeleteRecord implements Serializable {
    private final String filePath;
    private final long pos;

    PositionDeleteRecord(String filePath, long pos) {
      this.filePath = filePath;
      this.pos = pos;
    }

    String filePath() {
      return filePath;
    }

    long pos() {
      return pos;
    }
  }

  /** Function to read data file and filter by broadcast keys - runs on executors. */
  private static class ReadAndFilterFunction
      implements FlatMapFunction<DataFileInfo, PositionDeleteRecord> {

    private final Broadcast<Table> tableBroadcast;
    private final Schema projectionSchema;
    private final Broadcast<Set<List<Object>>> broadcastKeys;

    ReadAndFilterFunction(
        Broadcast<Table> tableBroadcast,
        Schema projectionSchema,
        Broadcast<Set<List<Object>>> broadcastKeys) {
      this.tableBroadcast = tableBroadcast;
      this.projectionSchema = projectionSchema;
      this.broadcastKeys = broadcastKeys;
    }

    @Override
    public Iterator<PositionDeleteRecord> call(DataFileInfo fileInfo) throws Exception {
      Table table = tableBroadcast.value();
      Set<List<Object>> deleteKeys = broadcastKeys.value();

      List<PositionDeleteRecord> matches = Lists.newArrayList();
      InputFile inputFile = table.io().newInputFile(fileInfo.path());

      long pos = 0;
      try (CloseableIterable<Record> reader =
          openDataFile(inputFile, projectionSchema, fileInfo.format())) {
        for (Record record : reader) {
          List<Object> recordKey =
              Lists.newArrayListWithCapacity(projectionSchema.columns().size());
          for (int i = 0; i < projectionSchema.columns().size(); i++) {
            recordKey.add(record.get(i));
          }
          if (deleteKeys.contains(recordKey)) {
            matches.add(new PositionDeleteRecord(fileInfo.path(), pos));
          }
          pos++;
        }
      }

      return matches.iterator();
    }

    private CloseableIterable<Record> openDataFile(
        InputFile inputFile, Schema schema, FileFormat format) {
      switch (format) {
        case PARQUET:
          return Parquet.read(inputFile)
              .project(schema)
              .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
              .build();
        case ORC:
          return ORC.read(inputFile)
              .project(schema)
              .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(schema, fileSchema))
              .build();
        case AVRO:
          return Avro.read(inputFile)
              .project(schema)
              .createReaderFunc(DataReader::create)
              .build();
        default:
          throw new UnsupportedOperationException("Unsupported format: " + format);
      }
    }
  }

  /**
   * Read equality delete keys using Iceberg's DeleteLoader.
   *
   * <p>Uses BaseDeleteLoader which handles S3/cloud storage via table's FileIO.
   */
  private Dataset<Row> readEqualityDeleteKeys(
      List<DeleteFile> eqDeleteFiles, Schema deleteSchema, int groupIndex) {

    DeleteLoader deleteLoader =
        new BaseDeleteLoader(deleteFile -> table.io().newInputFile(deleteFile.path().toString()));

    StructLikeSet deleteKeys = deleteLoader.loadEqualityDeletes(eqDeleteFiles, deleteSchema);

    LOG.info(
        "{} table={} group={} read_eq_delete_records={} from {} files",
        LOG_PREFIX,
        table.name(),
        groupIndex,
        deleteKeys.size(),
        eqDeleteFiles.size());

    StructType sparkSchema = SparkSchemaUtil.convert(deleteSchema);
    List<Row> rows = Lists.newArrayListWithCapacity(deleteKeys.size());

    for (StructLike key : deleteKeys) {
      Object[] values = new Object[deleteSchema.columns().size()];
      for (int i = 0; i < deleteSchema.columns().size(); i++) {
        values[i] = key.get(i, Object.class);
      }
      rows.add(RowFactory.create(values));
    }

    if (rows.isEmpty()) {
      return spark().createDataFrame(rows, sparkSchema);
    }

    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark().sparkContext());
    JavaRDD<Row> rdd = jsc.parallelize(rows);
    return spark().createDataFrame(rdd, sparkSchema);
  }

  /**
   * Read data files directly via FileIO WITHOUT applying deletes.
   *
   * <p>This is necessary because when reading via Iceberg source, equality deletes are
   * automatically applied, which means the rows we want to convert are already filtered out.
   */
  private Dataset<Row> readDataFilesWithPosition(
      List<FileScanTask> dataFileTasks, List<String> equalityColumns, long snapshotId) {

    Schema eqColumnsSchema = TypeUtil.select(table.schema(),
        table.schema().columns().stream()
            .filter(f -> equalityColumns.contains(f.name()))
            .map(Types.NestedField::fieldId)
            .collect(Collectors.toSet()));

    List<Types.NestedField> projectionFields = Lists.newArrayList(eqColumnsSchema.columns());
    Schema projectionSchema = new Schema(projectionFields);

    StructType sparkSchema = SparkSchemaUtil.convert(projectionSchema);
    // Add _file and _pos columns
    sparkSchema = sparkSchema.add(MetadataColumns.FILE_PATH.name(),
        org.apache.spark.sql.types.DataTypes.StringType, false);
    sparkSchema = sparkSchema.add(MetadataColumns.ROW_POSITION.name(),
        org.apache.spark.sql.types.DataTypes.LongType, false);

    List<Row> allRows = Lists.newArrayList();

    // Get unique data files
    Set<String> processedFiles = Sets.newHashSet();
    for (FileScanTask task : dataFileTasks) {
      String filePath = task.file().path().toString();
      if (processedFiles.contains(filePath)) {
        continue;
      }
      processedFiles.add(filePath);

      InputFile inputFile = table.io().newInputFile(filePath);
      FileFormat format = task.file().format();

      long rowPosition = 0;
      try (CloseableIterable<Record> reader = openDataFile(inputFile, projectionSchema, format)) {
        for (Record record : reader) {
          Object[] values = new Object[equalityColumns.size() + 2];
          for (int i = 0; i < equalityColumns.size(); i++) {
            values[i] = record.get(i);
          }
          values[equalityColumns.size()] = filePath;
          values[equalityColumns.size() + 1] = rowPosition;
          allRows.add(RowFactory.create(values));
          rowPosition++;
        }
      } catch (IOException e) {
        throw new RuntimeException("Failed to read data file: " + filePath, e);
      }
    }

    if (allRows.isEmpty()) {
      return spark().createDataFrame(allRows, sparkSchema);
    }

    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark().sparkContext());
    JavaRDD<Row> rdd = jsc.parallelize(allRows);
    return spark().createDataFrame(rdd, sparkSchema);
  }

  private CloseableIterable<Record> openDataFile(
      InputFile inputFile, Schema projectionSchema, FileFormat format) {
    switch (format) {
      case PARQUET:
        return Parquet.read(inputFile)
            .project(projectionSchema)
            .createReaderFunc(
                fileSchema -> GenericParquetReaders.buildReader(projectionSchema, fileSchema))
            .build();
      case ORC:
        return ORC.read(inputFile)
            .project(projectionSchema)
            .createReaderFunc(
                fileSchema -> GenericOrcReader.buildReader(projectionSchema, fileSchema))
            .build();
      case AVRO:
        return Avro.read(inputFile)
            .project(projectionSchema)
            .createReaderFunc(DataReader::create)
            .build();
      default:
        throw new UnsupportedOperationException("Unsupported data file format: " + format);
    }
  }

  private ConversionResult writePosDeleteFiles(
      Dataset<Row> positionDeletes,
      int specId,
      StructLike partition,
      long snapshotId,
      long eqDeleteRecordsCount,
      int groupIndex) {

    Set<DeleteFile> result = Sets.newHashSet();

    List<Row> rows = positionDeletes.collectAsList();
    if (rows.isEmpty()) {
      LOG.info(
          "{} table={} group={} pos_delete_records=0 nothing to write",
          LOG_PREFIX,
          table.name(),
          groupIndex);
      return new ConversionResult(result, eqDeleteRecordsCount, 0);
    }

    long posDeleteRecordsCount = rows.size();

    LOG.info(
        "{} table={} group={} pos_delete_records={} partition={} writing",
        LOG_PREFIX,
        table.name(),
        groupIndex,
        posDeleteRecordsCount,
        partition);

    long writeStartTime = System.currentTimeMillis();

    PartitionSpec spec = table.specs().get(specId);

    String deleteFileFormatStr =
        table
            .properties()
            .getOrDefault(
                TableProperties.DELETE_DEFAULT_FILE_FORMAT,
                TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
    FileFormat deleteFileFormat = FileFormat.fromString(deleteFileFormatStr);

    OutputFileFactory outputFileFactory =
        OutputFileFactory.builderFor(table, 0, 0)
            .format(deleteFileFormat)
            .operationId(String.valueOf(snapshotId))
            .suffix("pos-deletes")
            .build();

    GenericAppenderFactory appenderFactory = new GenericAppenderFactory(table.schema(), spec);
    appenderFactory.setAll(table.properties());

    FileWriter<PositionDelete<Record>, DeleteWriteResult> posDeleteWriter =
        new SortingPositionOnlyDeleteWriter<>(
            () -> {
              EncryptedOutputFile outputFile =
                  spec.isUnpartitioned()
                      ? outputFileFactory.newOutputFile()
                      : outputFileFactory.newOutputFile(spec, partition);
              return appenderFactory.newPosDeleteWriter(outputFile, deleteFileFormat, partition);
            },
            DeleteGranularity.FILE);

    try {
      PositionDelete<Record> posDelete = PositionDelete.create();
      for (Row row : rows) {
        String filePath = row.getString(0);
        long pos = row.getLong(1);
        posDelete.set(filePath, pos, null);
        posDeleteWriter.write(posDelete);
      }

      posDeleteWriter.close();
      DeleteWriteResult writeResult = posDeleteWriter.result();
      result.addAll(writeResult.deleteFiles());

    } catch (IOException e) {
      throw new RuntimeException("Failed to write position delete files", e);
    }

    long writeDuration = System.currentTimeMillis() - writeStartTime;
    long totalWrittenBytes = result.stream().mapToLong(DeleteFile::fileSizeInBytes).sum();

    LOG.info(
        "{} table={} group={} write_duration_ms={} pos_delete_files={} written_bytes={}",
        LOG_PREFIX,
        table.name(),
        groupIndex,
        writeDuration,
        result.size(),
        totalWrittenBytes);

    for (DeleteFile file : result) {
      LOG.info(
          "{} table={} group={} pos_delete_file={} size_bytes={} records={}",
          LOG_PREFIX,
          table.name(),
          groupIndex,
          file.path(),
          file.fileSizeInBytes(),
          file.recordCount());
    }

    return new ConversionResult(result, eqDeleteRecordsCount, posDeleteRecordsCount);
  }

  private ConversionResult writePosDeleteFilesFromRecords(
      List<PositionDeleteRecord> posDeletes,
      int specId,
      StructLike partition,
      long snapshotId,
      long eqDeleteRecordsCount,
      int groupIndex) {

    Set<DeleteFile> result = Sets.newHashSet();

    if (posDeletes.isEmpty()) {
      LOG.info(
          "{} table={} group={} pos_delete_records=0 nothing to write",
          LOG_PREFIX,
          table.name(),
          groupIndex);
      return new ConversionResult(result, eqDeleteRecordsCount, 0);
    }

    // Copy to mutable list and sort by file path and position for efficient writing
    List<PositionDeleteRecord> sortedDeletes = Lists.newArrayList(posDeletes);
    sortedDeletes.sort((a, b) -> {
      int cmp = a.filePath().compareTo(b.filePath());
      return cmp != 0 ? cmp : Long.compare(a.pos(), b.pos());
    });

    long posDeleteRecordsCount = sortedDeletes.size();

    LOG.info(
        "{} table={} group={} pos_delete_records={} partition={} writing",
        LOG_PREFIX,
        table.name(),
        groupIndex,
        posDeleteRecordsCount,
        partition);

    long writeStartTime = System.currentTimeMillis();

    PartitionSpec spec = table.specs().get(specId);

    String deleteFileFormatStr =
        table
            .properties()
            .getOrDefault(
                TableProperties.DELETE_DEFAULT_FILE_FORMAT,
                TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
    FileFormat deleteFileFormat = FileFormat.fromString(deleteFileFormatStr);

    OutputFileFactory outputFileFactory =
        OutputFileFactory.builderFor(table, 0, 0)
            .format(deleteFileFormat)
            .operationId(String.valueOf(snapshotId))
            .suffix("pos-deletes")
            .build();

    GenericAppenderFactory appenderFactory = new GenericAppenderFactory(table.schema(), spec);
    appenderFactory.setAll(table.properties());

    FileWriter<PositionDelete<Record>, DeleteWriteResult> posDeleteWriter =
        new SortingPositionOnlyDeleteWriter<>(
            () -> {
              EncryptedOutputFile outputFile =
                  spec.isUnpartitioned()
                      ? outputFileFactory.newOutputFile()
                      : outputFileFactory.newOutputFile(spec, partition);
              return appenderFactory.newPosDeleteWriter(outputFile, deleteFileFormat, partition);
            },
            DeleteGranularity.FILE);

    try {
      PositionDelete<Record> posDelete = PositionDelete.create();
      for (PositionDeleteRecord rec : sortedDeletes) {
        posDelete.set(rec.filePath(), rec.pos(), null);
        posDeleteWriter.write(posDelete);
      }

      posDeleteWriter.close();
      DeleteWriteResult writeResult = posDeleteWriter.result();
      result.addAll(writeResult.deleteFiles());

    } catch (IOException e) {
      throw new RuntimeException("Failed to write position delete files", e);
    }

    long writeDuration = System.currentTimeMillis() - writeStartTime;
    long totalWrittenBytes = result.stream().mapToLong(DeleteFile::fileSizeInBytes).sum();

    LOG.info(
        "{} table={} group={} write_duration_ms={} pos_delete_files={} written_bytes={}",
        LOG_PREFIX,
        table.name(),
        groupIndex,
        writeDuration,
        result.size(),
        totalWrittenBytes);

    for (DeleteFile file : result) {
      LOG.info(
          "{} table={} group={} pos_delete_file={} size_bytes={} records={}",
          LOG_PREFIX,
          table.name(),
          groupIndex,
          file.path(),
          file.fileSizeInBytes(),
          file.recordCount());
    }

    return new ConversionResult(result, eqDeleteRecordsCount, posDeleteRecordsCount);
  }

  private void commitChanges(
      Set<DeleteFile> eqDeleteFilesToRemove,
      Set<DeleteFile> posDeleteFilesToAdd,
      long startingSnapshotId) {

    long maxSequenceNumber =
        eqDeleteFilesToRemove.stream().mapToLong(DeleteFile::dataSequenceNumber).max().orElse(0);

    LOG.info(
        "{} table={} starting_snapshot={} max_sequence_number={} committing",
        LOG_PREFIX,
        table.name(),
        startingSnapshotId,
        maxSequenceNumber);

    RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(startingSnapshotId);

    for (DeleteFile eqDelete : eqDeleteFilesToRemove) {
      rewrite.deleteFile(eqDelete);
    }

    for (DeleteFile posDelete : posDeleteFilesToAdd) {
      rewrite.addFile(posDelete, maxSequenceNumber);
    }

    commitSummary().forEach(rewrite::set);
    rewrite.commit();
  }

  private static class ConversionResult {
    final Set<DeleteFile> posDeleteFiles;
    final long eqDeleteRecordsCount;
    final long posDeleteRecordsCount;

    ConversionResult(
        Set<DeleteFile> posDeleteFiles, long eqDeleteRecordsCount, long posDeleteRecordsCount) {
      this.posDeleteFiles = posDeleteFiles;
      this.eqDeleteRecordsCount = eqDeleteRecordsCount;
      this.posDeleteRecordsCount = posDeleteRecordsCount;
    }
  }

  private static class DeleteFileGroup {
    private final List<DeleteFile> deleteFiles;
    private final PartitionSpec spec;

    DeleteFileGroup(List<DeleteFile> deleteFiles, PartitionSpec spec) {
      this.deleteFiles = ImmutableList.copyOf(deleteFiles);
      this.spec = spec;
    }

    List<DeleteFile> deleteFiles() {
      return deleteFiles;
    }

    PartitionSpec spec() {
      return spec;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      DeleteFileGroup that = (DeleteFileGroup) o;
      Set<String> thisPaths =
          deleteFiles.stream().map(f -> f.path().toString()).collect(Collectors.toSet());
      Set<String> thatPaths =
          that.deleteFiles.stream().map(f -> f.path().toString()).collect(Collectors.toSet());
      return thisPaths.equals(thatPaths) && spec.specId() == that.spec.specId();
    }

    @Override
    public int hashCode() {
      Set<String> paths =
          deleteFiles.stream().map(f -> f.path().toString()).collect(Collectors.toSet());
      return paths.hashCode() * 31 + spec.specId();
    }
  }
}
