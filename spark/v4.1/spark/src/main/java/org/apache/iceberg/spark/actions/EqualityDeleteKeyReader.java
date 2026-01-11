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

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
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

/**
 * Reads equality delete keys from delete files.
 *
 * <p>Supports reading Long, String, BigDecimal, and multi-column keys with optional parallel
 * reading for multiple files.
 */
class EqualityDeleteKeyReader {

  /** Minimum number of eq delete files to use parallel reading. */
  private static final int MIN_FILES_FOR_PARALLEL_READ = 3;

  /** Maximum number of threads for parallel eq delete file reading. */
  private static final int MAX_PARALLEL_READ_THREADS = 16;

  private final Schema deleteSchema;
  private final String cacheMountPath;
  private final String cacheS3Prefix;

  EqualityDeleteKeyReader(Schema deleteSchema, String cacheMountPath, String cacheS3Prefix) {
    this.deleteSchema = deleteSchema;
    this.cacheMountPath = cacheMountPath;
    this.cacheS3Prefix = cacheS3Prefix;
  }

  /** Read equality delete keys as Long directly (no intermediate List allocation). */
  Set<Long> readLongKeys(Table table, List<String> eqDeletePaths) {
    if (eqDeletePaths.size() < MIN_FILES_FOR_PARALLEL_READ) {
      Set<Long> keys = new HashSet<>();
      for (String path : eqDeletePaths) {
        readLongKeysFromFile(table, path, keys);
      }
      return keys;
    }

    int threads = Math.min(eqDeletePaths.size(), MAX_PARALLEL_READ_THREADS);
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    try {
      List<Future<Set<Long>>> futures = new ArrayList<>(eqDeletePaths.size());
      for (String path : eqDeletePaths) {
        futures.add(
            executor.submit(
                () -> {
                  Set<Long> localKeys = new HashSet<>();
                  readLongKeysFromFile(table, path, localKeys);
                  return localKeys;
                }));
      }

      Set<Long> keys = new HashSet<>();
      for (Future<Set<Long>> future : futures) {
        keys.addAll(future.get());
      }
      return keys;
    } catch (ExecutionException e) {
      throw new RuntimeException("Failed to read eq delete files", e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while reading eq delete files", e);
    } finally {
      executor.shutdownNow();
    }
  }

  private void readLongKeysFromFile(Table table, String path, Set<Long> keys) {
    InputFile inputFile = getInputFileWithCache(path, table);
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
    if (eqDeletePaths.size() < MIN_FILES_FOR_PARALLEL_READ) {
      Set<String> keys = new HashSet<>();
      for (String path : eqDeletePaths) {
        readStringKeysFromFile(table, path, keys);
      }
      return keys;
    }

    int threads = Math.min(eqDeletePaths.size(), MAX_PARALLEL_READ_THREADS);
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    try {
      List<Future<Set<String>>> futures = new ArrayList<>(eqDeletePaths.size());
      for (String path : eqDeletePaths) {
        futures.add(
            executor.submit(
                () -> {
                  Set<String> localKeys = new HashSet<>();
                  readStringKeysFromFile(table, path, localKeys);
                  return localKeys;
                }));
      }

      Set<String> keys = new HashSet<>();
      for (Future<Set<String>> future : futures) {
        keys.addAll(future.get());
      }
      return keys;
    } catch (ExecutionException e) {
      throw new RuntimeException("Failed to read eq delete files", e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while reading eq delete files", e);
    } finally {
      executor.shutdownNow();
    }
  }

  private void readStringKeysFromFile(Table table, String path, Set<String> keys) {
    InputFile inputFile = getInputFileWithCache(path, table);
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
    if (eqDeletePaths.size() < MIN_FILES_FOR_PARALLEL_READ) {
      Set<BigDecimal> keys = new HashSet<>();
      for (String path : eqDeletePaths) {
        readDecimalKeysFromFile(table, path, keys);
      }
      return keys;
    }

    int threads = Math.min(eqDeletePaths.size(), MAX_PARALLEL_READ_THREADS);
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    try {
      List<Future<Set<BigDecimal>>> futures = new ArrayList<>(eqDeletePaths.size());
      for (String path : eqDeletePaths) {
        futures.add(
            executor.submit(
                () -> {
                  Set<BigDecimal> localKeys = new HashSet<>();
                  readDecimalKeysFromFile(table, path, localKeys);
                  return localKeys;
                }));
      }

      Set<BigDecimal> keys = new HashSet<>();
      for (Future<Set<BigDecimal>> future : futures) {
        keys.addAll(future.get());
      }
      return keys;
    } catch (ExecutionException e) {
      throw new RuntimeException("Failed to read eq delete files", e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while reading eq delete files", e);
    } finally {
      executor.shutdownNow();
    }
  }

  private void readDecimalKeysFromFile(Table table, String path, Set<BigDecimal> keys) {
    InputFile inputFile = getInputFileWithCache(path, table);
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

    if (eqDeletePaths.size() < MIN_FILES_FOR_PARALLEL_READ) {
      Set<List<Object>> keys = new HashSet<>();
      for (String path : eqDeletePaths) {
        readMultiColumnKeysFromFile(table, path, keys, keyColumnCount);
      }
      return keys;
    }

    int threads = Math.min(eqDeletePaths.size(), MAX_PARALLEL_READ_THREADS);
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    try {
      List<Future<Set<List<Object>>>> futures = new ArrayList<>(eqDeletePaths.size());
      for (String path : eqDeletePaths) {
        futures.add(
            executor.submit(
                () -> {
                  Set<List<Object>> localKeys = new HashSet<>();
                  readMultiColumnKeysFromFile(table, path, localKeys, keyColumnCount);
                  return localKeys;
                }));
      }

      Set<List<Object>> keys = new HashSet<>();
      for (Future<Set<List<Object>>> future : futures) {
        keys.addAll(future.get());
      }
      return keys;
    } catch (ExecutionException e) {
      throw new RuntimeException("Failed to read eq delete files", e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while reading eq delete files", e);
    } finally {
      executor.shutdownNow();
    }
  }

  private void readMultiColumnKeysFromFile(
      Table table, String path, Set<List<Object>> keys, int keyColumnCount) {
    InputFile inputFile = getInputFileWithCache(path, table);
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

  /** Get InputFile, using local FUSE mount path if configured. */
  private InputFile getInputFileWithCache(String s3Path, Table table) {
    if (cacheMountPath != null && cacheS3Prefix != null && s3Path.startsWith(cacheS3Prefix)) {
      String localPath = s3Path.replace(cacheS3Prefix, cacheMountPath);
      return Files.localInput(new File(localPath));
    }
    return table.io().newInputFile(s3Path);
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
