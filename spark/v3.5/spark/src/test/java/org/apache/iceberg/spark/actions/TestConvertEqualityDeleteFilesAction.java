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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.ConvertEqualityDeleteFiles;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.spark.data.TestHelpers;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public class TestConvertEqualityDeleteFilesAction extends TestBase {

  @TempDir private File tableDir;

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "id", Types.IntegerType.get()),
          optional(2, "data", Types.StringType.get()));

  @Parameter private int formatVersion;

  @Parameters(name = "formatVersion = {0}")
  protected static List<Integer> parameters() {
    // V3 requires DVs (delete vectors) for position deletes, not traditional pos delete files
    return Lists.newArrayList(2);
  }

  private String tableLocation = null;

  @BeforeEach
  public void setupTableLocation() {
    this.tableLocation = tableDir.toURI().toString();
  }

  @TestTemplate
  public void testPartialProgressPreservesDataCorrectness() {
    // Create table with V2 format (supports equality deletes)
    Table table =
        TABLES.create(
            SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(
                TableProperties.FORMAT_VERSION, String.valueOf(formatVersion),
                TableProperties.DEFAULT_FILE_FORMAT, "parquet"),
            tableLocation);

    // Insert data: ids 1-20
    spark
        .range(1, 21)
        .selectExpr("cast(id as int) as id", "'value' as data")
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);

    // Insert more data: ids 21-40 (creates second data file with higher sequence number)
    spark
        .range(21, 41)
        .selectExpr("cast(id as int) as id", "'value' as data")
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);

    table.refresh();

    // Write equality delete for ids 5, 10, 15 (applies to first data file only)
    writeEqualityDelete(table, Lists.newArrayList(5, 10, 15));

    // Insert more data: ids 41-50
    spark
        .range(41, 51)
        .selectExpr("cast(id as int) as id", "'value' as data")
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);

    // Write equality delete for ids 25, 30 (applies to first two data files)
    writeEqualityDelete(table, Lists.newArrayList(25, 30));

    table.refresh();

    // Verify we have equality delete files
    List<DeleteFile> eqDeletesBefore =
        TestHelpers.deleteFiles(table).stream()
            .filter(f -> f.content() == FileContent.EQUALITY_DELETES)
            .collect(Collectors.toList());
    assertThat(eqDeletesBefore).hasSize(2);

    // Read data BEFORE conversion - this is the "correct" answer
    List<Row> dataBefore = spark.read().format("iceberg").load(tableLocation).collectAsList();
    // Should have 50 records minus deleted (5, 10, 15, 25, 30) = 45 records
    assertThat(dataBefore).hasSize(45);

    // Run conversion with partial progress enabled
    ConvertEqualityDeleteFiles.Result result =
        SparkActions.get()
            .convertEqualityDeletes(table)
            .option(ConvertEqualityDeleteFilesSparkAction.PARTIAL_PROGRESS_ENABLED, "true")
            .option(
                ConvertEqualityDeleteFilesSparkAction.PARTIAL_PROGRESS_MIN_COMMIT_SIZE_BYTES, "0")
            .execute();

    assertThat(result.convertedEqualityDeleteFilesCount()).isEqualTo(2);
    assertThat(result.addedPositionDeleteFilesCount()).isGreaterThan(0);

    // Refresh and verify
    table.refresh();

    // Verify no equality deletes remain
    List<DeleteFile> eqDeletesAfter =
        TestHelpers.deleteFiles(table).stream()
            .filter(f -> f.content() == FileContent.EQUALITY_DELETES)
            .collect(Collectors.toList());
    assertThat(eqDeletesAfter).isEmpty();

    // Verify position deletes were added
    List<DeleteFile> posDeletesAfter =
        TestHelpers.deleteFiles(table).stream()
            .filter(f -> f.content() == FileContent.POSITION_DELETES)
            .collect(Collectors.toList());
    assertThat(posDeletesAfter).isNotEmpty();

    // CRITICAL: Read data AFTER conversion - must match data BEFORE
    List<Row> dataAfter = spark.read().format("iceberg").load(tableLocation).collectAsList();

    // Data must be identical
    assertThat(dataAfter).hasSize(dataBefore.size());

    // Verify deleted records are still deleted (not resurrected)
    List<Integer> idsAfter =
        dataAfter.stream().map(r -> r.getInt(0)).sorted().collect(Collectors.toList());
    assertThat(idsAfter).doesNotContain(5, 10, 15, 25, 30);
  }

  @TestTemplate
  public void testPartialProgressWithOverlappingGroups() {
    // Test scenario where eq deletes create overlapping groups
    Table table =
        TABLES.create(
            SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(
                TableProperties.FORMAT_VERSION, String.valueOf(formatVersion),
                TableProperties.DEFAULT_FILE_FORMAT, "parquet"),
            tableLocation);

    // Insert data in multiple commits to create multiple data files with different sequence numbers
    for (int batch = 0; batch < 3; batch++) {
      int start = batch * 10 + 1;
      int end = start + 10;
      spark
          .range(start, end)
          .selectExpr("cast(id as int) as id", "'batch" + batch + "' as data")
          .write()
          .format("iceberg")
          .mode("append")
          .save(tableLocation);
    }

    table.refresh();

    // Write eq delete E1 (seq=4) - applies to all 3 data files
    writeEqualityDelete(table, Lists.newArrayList(5));

    // Write eq delete E2 (seq=5) - applies to all 3 data files
    writeEqualityDelete(table, Lists.newArrayList(15));

    // Write eq delete E3 (seq=6) - applies to all 3 data files
    writeEqualityDelete(table, Lists.newArrayList(25));

    table.refresh();

    // Read expected data
    List<Row> dataBefore = spark.read().format("iceberg").load(tableLocation).collectAsList();

    // Convert with partial progress (min size 0 to allow commits after each group)
    ConvertEqualityDeleteFiles.Result result =
        SparkActions.get()
            .convertEqualityDeletes(table)
            .option(ConvertEqualityDeleteFilesSparkAction.PARTIAL_PROGRESS_ENABLED, "true")
            .option(
                ConvertEqualityDeleteFilesSparkAction.PARTIAL_PROGRESS_MIN_COMMIT_SIZE_BYTES, "0")
            .execute();

    assertThat(result.convertedEqualityDeleteFilesCount()).isEqualTo(3);

    table.refresh();

    // Verify data correctness
    List<Row> dataAfter = spark.read().format("iceberg").load(tableLocation).collectAsList();
    assertThat(dataAfter).hasSize(dataBefore.size());

    List<Integer> idsAfter =
        dataAfter.stream().map(r -> r.getInt(0)).sorted().collect(Collectors.toList());
    assertThat(idsAfter).doesNotContain(5, 15, 25);
  }

  @TestTemplate
  public void testWithoutPartialProgress() {
    // Baseline test without partial progress
    Table table =
        TABLES.create(
            SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(
                TableProperties.FORMAT_VERSION, String.valueOf(formatVersion),
                TableProperties.DEFAULT_FILE_FORMAT, "parquet"),
            tableLocation);

    spark
        .range(1, 51)
        .selectExpr("cast(id as int) as id", "'value' as data")
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);

    writeEqualityDelete(table, Lists.newArrayList(10, 20, 30, 40));

    table.refresh();

    List<Row> dataBefore = spark.read().format("iceberg").load(tableLocation).collectAsList();
    assertThat(dataBefore).hasSize(46); // 50 - 4 deleted

    // Convert WITHOUT partial progress (default)
    ConvertEqualityDeleteFiles.Result result =
        SparkActions.get().convertEqualityDeletes(table).execute();

    assertThat(result.convertedEqualityDeleteFilesCount()).isEqualTo(1);

    table.refresh();

    List<Row> dataAfter = spark.read().format("iceberg").load(tableLocation).collectAsList();
    assertThat(dataAfter).hasSize(dataBefore.size());

    List<Integer> idsAfter =
        dataAfter.stream().map(r -> r.getInt(0)).sorted().collect(Collectors.toList());
    assertThat(idsAfter).doesNotContain(10, 20, 30, 40);
  }

  private void writeEqualityDelete(Table table, List<Integer> idsToDelete) {
    table.refresh();
    List<Integer> equalityFieldIds = Lists.newArrayList(table.schema().findField("id").fieldId());
    Schema eqDeleteRowSchema = table.schema().select("id");

    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, System.nanoTime())
            .format(FileFormat.PARQUET)
            .build();

    GenericAppenderFactory appenderFactory =
        new GenericAppenderFactory(
            table.schema(),
            table.spec(),
            ArrayUtil.toIntArray(equalityFieldIds),
            eqDeleteRowSchema,
            null);

    OutputFile outputFile = fileFactory.newOutputFile().encryptingOutputFile();

    EqualityDeleteWriter<Record> eqDeleteWriter =
        appenderFactory.newEqDeleteWriter(
            EncryptedFiles.encryptedOutput(outputFile, EncryptionKeyMetadata.EMPTY),
            FileFormat.PARQUET,
            null);

    try (EqualityDeleteWriter<Record> writer = eqDeleteWriter) {
      for (Integer id : idsToDelete) {
        Record deleteRecord =
            GenericRecord.create(eqDeleteRowSchema).copy(ImmutableMap.of("id", id));
        writer.write(deleteRecord);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    table.newRowDelta().addDeletes(eqDeleteWriter.toDeleteFile()).commit();
  }

  @TestTemplate
  public void testNullValueInEqualityDeleteKey() {
    // Test that NULL values in equality delete keys match NULL values in data rows
    // per Iceberg spec: "A null value in a delete column matches a row if the row's value is null"

    // Create table with nullable id column
    Table table =
        TABLES.create(
            SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(
                TableProperties.FORMAT_VERSION, String.valueOf(formatVersion),
                TableProperties.DEFAULT_FILE_FORMAT, "parquet"),
            tableLocation);

    // Insert data including NULL values
    // ids: 1, 2, NULL, NULL, 5 (two NULL rows)
    spark
        .sql(
            "SELECT * FROM VALUES "
                + "(1, 'a'), (2, 'b'), (NULL, 'c'), (NULL, 'd'), (5, 'e') "
                + "AS t(id, data)")
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);

    table.refresh();

    // Verify we have 5 rows including 2 with NULL id
    List<Row> allData = spark.read().format("iceberg").load(tableLocation).collectAsList();
    assertThat(allData).hasSize(5);
    long nullCount = allData.stream().filter(r -> r.isNullAt(0)).count();
    assertThat(nullCount).isEqualTo(2);

    // Write equality delete for NULL id (should delete both rows with NULL id)
    writeEqualityDeleteWithNull(table);

    table.refresh();

    // Verify we have equality delete file
    List<DeleteFile> eqDeletesBefore =
        TestHelpers.deleteFiles(table).stream()
            .filter(f -> f.content() == FileContent.EQUALITY_DELETES)
            .collect(Collectors.toList());
    assertThat(eqDeletesBefore).hasSize(1);

    // Read data BEFORE conversion - should have 3 rows (5 - 2 with NULL id)
    List<Row> dataBefore = spark.read().format("iceberg").load(tableLocation).collectAsList();
    assertThat(dataBefore).hasSize(3);
    assertThat(dataBefore.stream().filter(r -> r.isNullAt(0)).count()).isEqualTo(0);

    // Convert equality deletes to position deletes
    ConvertEqualityDeleteFiles.Result result =
        SparkActions.get().convertEqualityDeletes(table).execute();

    assertThat(result.convertedEqualityDeleteFilesCount()).isEqualTo(1);
    assertThat(result.addedPositionDeleteFilesCount()).isGreaterThan(0);
    // Should have 2 position delete records (one for each NULL row)
    assertThat(result.addedDeleteRecordsCount()).isEqualTo(2);

    table.refresh();

    // Verify data AFTER conversion - still 3 rows, no NULL ids
    List<Row> dataAfter = spark.read().format("iceberg").load(tableLocation).collectAsList();
    assertThat(dataAfter).hasSize(3);
    assertThat(dataAfter.stream().filter(r -> r.isNullAt(0)).count()).isEqualTo(0);

    // Verify remaining ids are 1, 2, 5
    List<Integer> idsAfter =
        dataAfter.stream()
            .filter(r -> !r.isNullAt(0))
            .map(r -> r.getInt(0))
            .sorted()
            .collect(Collectors.toList());
    assertThat(idsAfter).containsExactly(1, 2, 5);

    // Verify no more equality delete files after conversion
    List<DeleteFile> eqDeletesAfter =
        TestHelpers.deleteFiles(table).stream()
            .filter(f -> f.content() == FileContent.EQUALITY_DELETES)
            .collect(Collectors.toList());
    assertThat(eqDeletesAfter).isEmpty();

    // Verify position delete files exist
    List<DeleteFile> posDeletesAfter =
        TestHelpers.deleteFiles(table).stream()
            .filter(f -> f.content() == FileContent.POSITION_DELETES)
            .collect(Collectors.toList());
    assertThat(posDeletesAfter).isNotEmpty();
  }

  private void writeEqualityDeleteWithNull(Table table) {
    table.refresh();
    List<Integer> equalityFieldIds = Lists.newArrayList(table.schema().findField("id").fieldId());
    Schema eqDeleteRowSchema = table.schema().select("id");

    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, System.nanoTime())
            .format(FileFormat.PARQUET)
            .build();

    GenericAppenderFactory appenderFactory =
        new GenericAppenderFactory(
            table.schema(),
            table.spec(),
            ArrayUtil.toIntArray(equalityFieldIds),
            eqDeleteRowSchema,
            null);

    OutputFile outputFile = fileFactory.newOutputFile().encryptingOutputFile();

    EqualityDeleteWriter<Record> eqDeleteWriter =
        appenderFactory.newEqDeleteWriter(
            EncryptedFiles.encryptedOutput(outputFile, EncryptionKeyMetadata.EMPTY),
            FileFormat.PARQUET,
            null);

    try (EqualityDeleteWriter<Record> writer = eqDeleteWriter) {
      // Write a delete record with NULL id
      Record deleteRecord = GenericRecord.create(eqDeleteRowSchema);
      deleteRecord.setField("id", null);
      writer.write(deleteRecord);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    table.newRowDelta().addDeletes(eqDeleteWriter.toDeleteFile()).commit();
  }
}
