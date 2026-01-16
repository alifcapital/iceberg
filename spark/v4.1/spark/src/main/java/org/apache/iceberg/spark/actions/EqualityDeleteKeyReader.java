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

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;

/**
 * Reads equality delete keys from delete files.
 *
 * <p>Supports reading Long, String, BigDecimal, and multi-column keys using the shared
 * delete worker pool (configured via iceberg.worker.delete-num-threads).
 *
 * <p>Files are read directly from table.io() without cache - eq delete files are small
 * and read only once on driver.
 */
class EqualityDeleteKeyReader {

  private final Schema deleteSchema;

  EqualityDeleteKeyReader(Schema deleteSchema) {
    this.deleteSchema = deleteSchema;
  }

  /** Read equality delete keys as Long directly (no intermediate List allocation). */
  Set<Long> readLongKeys(Table table, List<String> eqDeletePaths) {
    Queue<Set<Long>> results = new ConcurrentLinkedQueue<>();

    Tasks.foreach(eqDeletePaths)
        .executeWith(ThreadPools.getDeleteWorkerPool())
        .stopOnFailure()
        .run(path -> {
          Set<Long> localKeys = new HashSet<>();
          readLongKeysFromFile(table, path, localKeys);
          results.add(localKeys);
        });

    Set<Long> keys = new HashSet<>();
    for (Set<Long> result : results) {
      keys.addAll(result);
    }
    return keys;
  }

  private void readLongKeysFromFile(Table table, String path, Set<Long> keys) {
    InputFile inputFile = table.io().newInputFile(path);
    FileFormat format = FileFormat.fromFileName(path);

    try (CloseableIterable<Record> reader = openDeleteFileForRead(inputFile, deleteSchema, format)) {
      for (Record record : reader) {
        Object val = record.get(0);
        if (val == null) {
          keys.add(null);
        } else {
          keys.add(val instanceof Integer ? ((Integer) val).longValue() : (Long) val);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to read eq delete file: " + path, e);
    }
  }

  /** Read equality delete keys as String directly (no intermediate List allocation). */
  Set<String> readStringKeys(Table table, List<String> eqDeletePaths) {
    Queue<Set<String>> results = new ConcurrentLinkedQueue<>();

    Tasks.foreach(eqDeletePaths)
        .executeWith(ThreadPools.getDeleteWorkerPool())
        .stopOnFailure()
        .run(path -> {
          Set<String> localKeys = new HashSet<>();
          readStringKeysFromFile(table, path, localKeys);
          results.add(localKeys);
        });

    Set<String> keys = new HashSet<>();
    for (Set<String> result : results) {
      keys.addAll(result);
    }
    return keys;
  }

  private void readStringKeysFromFile(Table table, String path, Set<String> keys) {
    InputFile inputFile = table.io().newInputFile(path);
    FileFormat format = FileFormat.fromFileName(path);

    try (CloseableIterable<Record> reader = openDeleteFileForRead(inputFile, deleteSchema, format)) {
      for (Record record : reader) {
        Object val = record.get(0);
        keys.add(val != null ? val.toString() : null);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to read eq delete file: " + path, e);
    }
  }

  /** Read equality delete keys as BigDecimal directly (no intermediate List allocation). */
  Set<BigDecimal> readDecimalKeys(Table table, List<String> eqDeletePaths) {
    Queue<Set<BigDecimal>> results = new ConcurrentLinkedQueue<>();

    Tasks.foreach(eqDeletePaths)
        .executeWith(ThreadPools.getDeleteWorkerPool())
        .stopOnFailure()
        .run(path -> {
          Set<BigDecimal> localKeys = new HashSet<>();
          readDecimalKeysFromFile(table, path, localKeys);
          results.add(localKeys);
        });

    Set<BigDecimal> keys = new HashSet<>();
    for (Set<BigDecimal> result : results) {
      keys.addAll(result);
    }
    return keys;
  }

  private void readDecimalKeysFromFile(Table table, String path, Set<BigDecimal> keys) {
    InputFile inputFile = table.io().newInputFile(path);
    FileFormat format = FileFormat.fromFileName(path);

    try (CloseableIterable<Record> reader = openDeleteFileForRead(inputFile, deleteSchema, format)) {
      for (Record record : reader) {
        Object val = record.get(0);
        keys.add((BigDecimal) val);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to read eq delete file: " + path, e);
    }
  }

  /** Read equality delete keys for multi-column keys. */
  Set<List<Object>> readMultiColumnKeys(Table table, List<String> eqDeletePaths) {
    int keyColumnCount = deleteSchema.columns().size();
    Queue<Set<List<Object>>> results = new ConcurrentLinkedQueue<>();

    Tasks.foreach(eqDeletePaths)
        .executeWith(ThreadPools.getDeleteWorkerPool())
        .stopOnFailure()
        .run(path -> {
          Set<List<Object>> localKeys = new HashSet<>();
          readMultiColumnKeysFromFile(table, path, localKeys, keyColumnCount);
          results.add(localKeys);
        });

    Set<List<Object>> keys = new HashSet<>();
    for (Set<List<Object>> result : results) {
      keys.addAll(result);
    }
    return keys;
  }

  private void readMultiColumnKeysFromFile(
      Table table, String path, Set<List<Object>> keys, int keyColumnCount) {
    InputFile inputFile = table.io().newInputFile(path);
    FileFormat format = FileFormat.fromFileName(path);

    try (CloseableIterable<Record> reader = openDeleteFileForRead(inputFile, deleteSchema, format)) {
      for (Record record : reader) {
        List<Object> keyValues = Lists.newArrayListWithCapacity(keyColumnCount);
        for (int i = 0; i < keyColumnCount; i++) {
          keyValues.add(record.get(i));
        }
        keys.add(keyValues);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to read eq delete file: " + path, e);
    }
  }

  /** Open delete file for reading. */
  private CloseableIterable<Record> openDeleteFileForRead(
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
        return Avro.read(inputFile).project(schema).createReaderFunc(DataReader::create).build();
      default:
        throw new UnsupportedOperationException("Unsupported format: " + format);
    }
  }
}
