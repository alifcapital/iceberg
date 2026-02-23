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
package org.apache.iceberg;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinCompressionCodec;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/** Utilities for Rewrite table path action. */
public class RewriteTablePathUtil {
  private static final Logger LOG = LoggerFactory.getLogger(RewriteTablePathUtil.class);
  public static final String FILE_SEPARATOR = "/";
  private RewriteTablePathUtil() {}
  public static class RewriteResult<T> implements Serializable {
    private final Set<T> toRewrite = Sets.newHashSet();
    private final Set<Pair<String, String>> copyPlan = Sets.newHashSet();
    public RewriteResult() {}
    public RewriteResult<T> append(RewriteResult<T> r1) {
      toRewrite.addAll(r1.toRewrite);
      copyPlan.addAll(r1.copyPlan);
      return this;
    }
    public Set<T> toRewrite() {
      return toRewrite;
    }
    public Set<Pair<String, String>> copyPlan() {
      return copyPlan;
    }
  }
  public static TableMetadata replacePaths(
      TableMetadata metadata, String sourcePrefix, String targetPrefix) {
    String newLocation = metadata.location().replaceFirst(sourcePrefix, targetPrefix);
    List<Snapshot> newSnapshots = updatePathInSnapshots(metadata, sourcePrefix, targetPrefix);
    List<TableMetadata.MetadataLogEntry> metadataLogEntries =
        updatePathInMetadataLogs(metadata, sourcePrefix, targetPrefix);
    long snapshotId =
        metadata.currentSnapshot() == null ? -1 : metadata.currentSnapshot().snapshotId();
    Map<String, String> properties =
        updateProperties(metadata.properties(), sourcePrefix, targetPrefix);
    return new TableMetadata(
        null,
        metadata.formatVersion(),
        metadata.uuid(),
        newLocation,
        metadata.lastSequenceNumber(),
        metadata.lastUpdatedMillis(),
        metadata.lastColumnId(),
        metadata.currentSchemaId(),
        metadata.schemas(),
        metadata.defaultSpecId(),
        metadata.specs(),
        metadata.lastAssignedPartitionId(),
        metadata.defaultSortOrderId(),
        metadata.sortOrders(),
        properties,
        snapshotId,
        newSnapshots,
        null,
        metadata.snapshotLog(),
        metadataLogEntries,
        metadata.refs(),
        updatePathInStatisticsFiles(metadata.statisticsFiles(), sourcePrefix, targetPrefix),
        updatePathInPartitionStatisticsFiles(
            metadata.partitionStatisticsFiles(), sourcePrefix, targetPrefix),
        metadata.nextRowId(),
        metadata.encryptionKeys(),
        metadata.changes());
  }
  private static Map<String, String> updateProperties(
      Map<String, String> tableProperties, String sourcePrefix, String targetPrefix) {
    Map<String, String> properties = Maps.newHashMap(tableProperties);
    updatePathInProperty(properties, sourcePrefix, targetPrefix, TableProperties.OBJECT_STORE_PATH);
    updatePathInProperty(
        properties, sourcePrefix, targetPrefix, TableProperties.WRITE_FOLDER_STORAGE_LOCATION);
    updatePathInProperty(
        properties, sourcePrefix, targetPrefix, TableProperties.WRITE_DATA_LOCATION);
    updatePathInProperty(
        properties, sourcePrefix, targetPrefix, TableProperties.WRITE_METADATA_LOCATION);
    return properties;
  }
  private static void updatePathInProperty(
      Map<String, String> properties,
      String sourcePrefix,
      String targetPrefix,
      String propertyName) {
    if (properties.containsKey(propertyName)) {
      properties.put(
          propertyName, newPath(properties.get(propertyName), sourcePrefix, targetPrefix));
    }
  }
  private static List<StatisticsFile> updatePathInStatisticsFiles(
      List<StatisticsFile> statisticsFiles, String sourcePrefix, String targetPrefix) {
    return statisticsFiles.stream()
        .map(
            existing ->
                new GenericStatisticsFile(
                    existing.snapshotId(),
                    newPath(existing.path(), sourcePrefix, targetPrefix),
                    existing.fileSizeInBytes(),
                    existing.fileFooterSizeInBytes(),
                    existing.blobMetadata()))
        .collect(Collectors.toList());
  }
  private static List<PartitionStatisticsFile> updatePathInPartitionStatisticsFiles(
      List<PartitionStatisticsFile> partitionStatisticsFiles,
      String sourcePrefix,
      String targetPrefix) {
    return partitionStatisticsFiles.stream()
        .map(
            existing ->
                ImmutableGenericPartitionStatisticsFile.builder()
                    .snapshotId(existing.snapshotId())
                    .path(newPath(existing.path(), sourcePrefix, targetPrefix))
                    .fileSizeInBytes(existing.fileSizeInBytes())
                    .build())
        .collect(Collectors.toList());
  }
  private static List<TableMetadata.MetadataLogEntry> updatePathInMetadataLogs(
      TableMetadata metadata, String sourcePrefix, String targetPrefix) {
    List<TableMetadata.MetadataLogEntry> metadataLogEntries =
        Lists.newArrayListWithCapacity(metadata.previousFiles().size());
    for (TableMetadata.MetadataLogEntry metadataLog : metadata.previousFiles()) {
      TableMetadata.MetadataLogEntry newMetadataLog =
          new TableMetadata.MetadataLogEntry(
              metadataLog.timestampMillis(),
              newPath(metadataLog.file(), sourcePrefix, targetPrefix));
      metadataLogEntries.add(newMetadataLog);
    }
    return metadataLogEntries;
  }
  private static List<Snapshot> updatePathInSnapshots(
      TableMetadata metadata, String sourcePrefix, String targetPrefix) {
    List<Snapshot> newSnapshots = Lists.newArrayListWithCapacity(metadata.snapshots().size());
    for (Snapshot snapshot : metadata.snapshots()) {
      String newManifestListLocation =
          newPath(snapshot.manifestListLocation(), sourcePrefix, targetPrefix);
      Snapshot newSnapshot =
          new BaseSnapshot(
              snapshot.sequenceNumber(),
              snapshot.snapshotId(),
              snapshot.parentId(),
              snapshot.timestampMillis(),
              snapshot.operation(),
              snapshot.summary(),
              snapshot.schemaId(),
              newManifestListLocation,
              snapshot.firstRowId(),
              snapshot.addedRows(),
              snapshot.keyId());
      newSnapshots.add(newSnapshot);
    }
    return newSnapshots;
  }
  public static RewriteResult<ManifestFile> rewriteManifestList(
      Snapshot snapshot,
      FileIO io,
      TableMetadata tableMetadata,
      Set<String> manifestsToRewrite,
      String sourcePrefix,
      String targetPrefix,
      String stagingDir,
      String outputPath) {
    RewriteResult<ManifestFile> result = new RewriteResult<>();
    OutputFile outputFile = io.newOutputFile(outputPath);
    List<ManifestFile> manifestFiles = manifestFilesInSnapshot(io, snapshot);
    manifestFiles.forEach(
        mf ->
            Preconditions.checkArgument(
                mf.path().startsWith(sourcePrefix),
                "Encountered manifest file %s not under the source prefix %s",
                mf.path(),
                sourcePrefix));
    EncryptionManager encryptionManager =
        (io instanceof EncryptingFileIO)
            ? ((EncryptingFileIO) io).encryptionManager()
            : PlaintextEncryptionManager.instance();
    try (FileAppender<ManifestFile> writer =
        ManifestLists.write(
            tableMetadata.formatVersion(),
            outputFile,
            encryptionManager,
            snapshot.snapshotId(),
            snapshot.parentId(),
            snapshot.sequenceNumber(),
            snapshot.firstRowId())) {
      for (ManifestFile file : manifestFiles) {
        ManifestFile newFile = file.copy();
        ((StructLike) newFile).set(0, newPath(newFile.path(), sourcePrefix, targetPrefix));
        writer.add(newFile);
        if (manifestsToRewrite.contains(file.path())) {
          result.toRewrite().add(file);
          result
              .copyPlan()
              .add(Pair.of(stagingPath(file.path(), sourcePrefix, stagingDir), newFile.path()));
        }
      }
      return result;
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to rewrite the manifest list file " + snapshot.manifestListLocation(), e);
    }
  }
  private static List<ManifestFile> manifestFilesInSnapshot(FileIO io, Snapshot snapshot) {
    String path = snapshot.manifestListLocation();
    List<ManifestFile> manifestFiles = Lists.newLinkedList();
    try {
      manifestFiles = ManifestLists.read(io.newInputFile(path));
    } catch (RuntimeIOException e) {
      LOG.warn("Failed to read manifest list {}", path, e);
    }
    return manifestFiles;
  }
  public static RewriteResult<DataFile> rewriteDataManifest(
      ManifestFile manifestFile,
      Set<Long> snapshotIds,
      OutputFile outputFile,
      FileIO io,
      int format,
      Map<Integer, PartitionSpec> specsById,
      String sourcePrefix,
      String targetPrefix)
      throws IOException {
    PartitionSpec spec = specsById.get(manifestFile.partitionSpecId());
    try (ManifestWriter<DataFile> writer =
            ManifestFiles.write(format, spec, outputFile, manifestFile.snapshotId());
        ManifestReader<DataFile> reader =
            ManifestFiles.read(manifestFile, io, specsById).select(Arrays.asList("*"))) {
      return StreamSupport.stream(reader.entries().spliterator(), false)
          .map(
              entry ->
                  writeDataFileEntry(entry, snapshotIds, spec, sourcePrefix, targetPrefix, writer))
          .reduce(new RewriteResult<>(), RewriteResult::append);
    }
  }
  /**
   * Rewrite a delete manifest, replacing path references.
   *
   * <p>Position delete files are written inline (before the manifest entry) so that the actual
   * file_size_in_bytes recorded in the manifest matches the physical file on disk. This fixes a
   * bug where Spark's Parquet footer seek (seek(size-8)) landed in the middle of path data
   * because the manifest carried the original file size instead of the rewritten one.
   */
  public static RewriteResult<DeleteFile> rewriteDeleteManifest(
      ManifestFile manifestFile,
      Set<Long> snapshotIds,
      OutputFile outputFile,
      FileIO io,
      int format,
      Map<Integer, PartitionSpec> specsById,
      String sourcePrefix,
      String targetPrefix,
      String stagingLocation,
      PositionDeleteReaderWriter posDeleteReaderWriter)
      throws IOException {
    PartitionSpec spec = specsById.get(manifestFile.partitionSpecId());
    try (ManifestWriter<DeleteFile> writer =
            ManifestFiles.writeDeleteManifest(format, spec, outputFile, manifestFile.snapshotId());
        ManifestReader<DeleteFile> reader =
            ManifestFiles.readDeleteManifest(manifestFile, io, specsById)
                .select(Arrays.asList("*"))) {
      return StreamSupport.stream(reader.entries().spliterator(), false)
          .map(
              entry ->
                  writeDeleteFileEntry(
                      entry,
                      snapshotIds,
                      spec,
                      sourcePrefix,
                      targetPrefix,
                      stagingLocation,
                      writer,
                      io,
                      posDeleteReaderWriter))
          .reduce(new RewriteResult<>(), RewriteResult::append);
    }
  }
  private static RewriteResult<DataFile> writeDataFileEntry(
      ManifestEntry<DataFile> entry,
      Set<Long> snapshotIds,
      PartitionSpec spec,
      String sourcePrefix,
      String targetPrefix,
      ManifestWriter<DataFile> writer) {
    RewriteResult<DataFile> result = new RewriteResult<>();
    DataFile dataFile = entry.file();
    String sourceDataFilePath = dataFile.location();
    Preconditions.checkArgument(
        sourceDataFilePath.startsWith(sourcePrefix),
        "Encountered data file %s not under the source prefix %s",
        sourceDataFilePath,
        sourcePrefix);
    String targetDataFilePath = newPath(sourceDataFilePath, sourcePrefix, targetPrefix);
    DataFile newDataFile =
        DataFiles.builder(spec).copy(entry.file()).withPath(targetDataFilePath).build();
    appendEntryWithFile(entry, writer, newDataFile);
    if (entry.isLive() && snapshotIds.contains(entry.snapshotId())) {
      result.copyPlan().add(Pair.of(sourceDataFilePath, newDataFile.location()));
    }
    return result;
  }
  /**
   * Write a single delete manifest entry.
   *
   * <p>For POSITION_DELETES: the physical file is rewritten first so its actual size is known,
   * then the manifest entry is recorded with the correct file_size_in_bytes. Writing the manifest
   * with the original size would cause Spark to seek to the wrong offset when reading the Parquet
   * footer, resulting in a "Not a Parquet file" error (magic bytes read as e.g. "2d29" != "PAR1").
   */
  private static RewriteResult<DeleteFile> writeDeleteFileEntry(
      ManifestEntry<DeleteFile> entry,
      Set<Long> snapshotIds,
      PartitionSpec spec,
      String sourcePrefix,
      String targetPrefix,
      String stagingLocation,
      ManifestWriter<DeleteFile> writer,
      FileIO io,
      PositionDeleteReaderWriter posDeleteReaderWriter) {
    DeleteFile file = entry.file();
    RewriteResult<DeleteFile> result = new RewriteResult<>();
    switch (file.content()) {
      case POSITION_DELETES:
        String stagingFilePath = stagingPath(file.location(), sourcePrefix, stagingLocation);
        OutputFile stagingOutputFile = io.newOutputFile(stagingFilePath);
        try {
          rewritePositionDeleteFile(
              file, stagingOutputFile, io, spec, sourcePrefix, targetPrefix, posDeleteReaderWriter);
        } catch (IOException e) {
          throw new UncheckedIOException(
              "Failed to rewrite position delete file " + file.location(), e);
        }
        long actualFileSize = io.newInputFile(stagingFilePath).getLength();
        DeleteFile posDeleteFile =
            newPositionDeleteEntry(file, spec, sourcePrefix, targetPrefix, actualFileSize);
        appendEntryWithFile(entry, writer, posDeleteFile);
        if (entry.isLive() && snapshotIds.contains(entry.snapshotId())) {
          result.copyPlan().add(Pair.of(stagingFilePath, posDeleteFile.location()));
        }
        result.toRewrite().add(file);
        return result;
      case EQUALITY_DELETES:
        DeleteFile eqDeleteFile = newEqualityDeleteEntry(file, spec, sourcePrefix, targetPrefix);
        appendEntryWithFile(entry, writer, eqDeleteFile);
        if (entry.isLive() && snapshotIds.contains(entry.snapshotId())) {
          result.copyPlan().add(Pair.of(file.location(), eqDeleteFile.location()));
        }
        return result;
      default:
        throw new UnsupportedOperationException("Unsupported delete file type: " + file.content());
    }
  }
  private static <F extends ContentFile<F>> void appendEntryWithFile(
      ManifestEntry<F> entry, ManifestWriter<F> writer, F file) {
    switch (entry.status()) {
      case ADDED:
        writer.add(file);
        break;
      case EXISTING:
        writer.existing(
            file, entry.snapshotId(), entry.dataSequenceNumber(), entry.fileSequenceNumber());
        break;
      case DELETED:
        writer.delete(file, entry.dataSequenceNumber(), entry.fileSequenceNumber());
        break;
    }
  }
  private static DeleteFile newEqualityDeleteEntry(
      DeleteFile file, PartitionSpec spec, String sourcePrefix, String targetPrefix) {
    String path = file.location();
    if (!path.startsWith(sourcePrefix)) {
      throw new UnsupportedOperationException(
          "Expected delete file to be under the source prefix: " + sourcePrefix + " but was " + path);
    }
    int[] equalityFieldIds = file.equalityFieldIds().stream().mapToInt(Integer::intValue).toArray();
    return FileMetadata.deleteFileBuilder(spec)
        .ofEqualityDeletes(equalityFieldIds)
        .copy(file)
        .withPath(newPath(path, sourcePrefix, targetPrefix))
        .withSplitOffsets(file.splitOffsets())
        .build();
  }
  private static DeleteFile newPositionDeleteEntry(
      DeleteFile file, PartitionSpec spec, String sourcePrefix, String targetPrefix) {
    String path = file.location();
    Preconditions.checkArgument(
        path.startsWith(sourcePrefix),
        "Expected delete file %s to start with prefix: %s", path, sourcePrefix);
    FileMetadata.Builder builder =
        FileMetadata.deleteFileBuilder(spec)
            .copy(file)
            .withPath(newPath(path, sourcePrefix, targetPrefix))
            .withMetrics(ContentFileUtil.replacePathBounds(file, sourcePrefix, targetPrefix));
    String newReferencedDataFile = rewriteReferencedDataFilePathForDV(file, sourcePrefix, targetPrefix);
    if (newReferencedDataFile != null) {
      builder.withReferencedDataFile(newReferencedDataFile);
    }
    return builder.build();
  }
  /** Build a position delete manifest entry with an explicit file size known after physical write. */
  private static DeleteFile newPositionDeleteEntry(
      DeleteFile file,
      PartitionSpec spec,
      String sourcePrefix,
      String targetPrefix,
      long actualFileSize) {
    String path = file.location();
    Preconditions.checkArgument(
        path.startsWith(sourcePrefix),
        "Expected delete file %s to start with prefix: %s", path, sourcePrefix);
    FileMetadata.Builder builder =
        FileMetadata.deleteFileBuilder(spec)
            .copy(file)
            .withPath(newPath(path, sourcePrefix, targetPrefix))
            .withFileSizeInBytes(actualFileSize)
            .withMetrics(ContentFileUtil.replacePathBounds(file, sourcePrefix, targetPrefix));
    String newReferencedDataFile = rewriteReferencedDataFilePathForDV(file, sourcePrefix, targetPrefix);
    if (newReferencedDataFile != null) {
      builder.withReferencedDataFile(newReferencedDataFile);
    }
    return builder.build();
  }
  private static String rewriteReferencedDataFilePathForDV(
      DeleteFile deleteFile, String sourcePrefix, String targetPrefix) {
    if (!ContentFileUtil.isDV(deleteFile) || deleteFile.referencedDataFile() == null) {
      return null;
    }
    String oldReferencedDataFile = deleteFile.referencedDataFile();
    if (oldReferencedDataFile.startsWith(sourcePrefix)) {
      return newPath(oldReferencedDataFile, sourcePrefix, targetPrefix);
    }
    return oldReferencedDataFile;
  }
  /** Class providing engine-specific methods to read and write position delete files. */
  public interface PositionDeleteReaderWriter extends Serializable {
    CloseableIterable<Record> reader(InputFile inputFile, FileFormat format, PartitionSpec spec);
    default PositionDeleteWriter<Record> writer(
        OutputFile outputFile, FileFormat format, PartitionSpec spec, StructLike partition)
        throws IOException {
      return writer(outputFile, format, spec, partition, null);
    }
    @Deprecated
    PositionDeleteWriter<Record> writer(
        OutputFile outputFile,
        FileFormat format,
        PartitionSpec spec,
        StructLike partition,
        Schema rowSchema)
        throws IOException;
  }
  public static void rewritePositionDeleteFile(
      DeleteFile deleteFile,
      OutputFile outputFile,
      FileIO io,
      PartitionSpec spec,
      String sourcePrefix,
      String targetPrefix,
      PositionDeleteReaderWriter posDeleteReaderWriter)
      throws IOException {
    String path = deleteFile.location();
    if (!path.startsWith(sourcePrefix)) {
      throw new UnsupportedOperationException(
          String.format("Expected delete file %s to start with prefix: %s", path, sourcePrefix));
    }
    if (ContentFileUtil.isDV(deleteFile)) {
      rewriteDVFile(deleteFile, outputFile, io, sourcePrefix, targetPrefix);
      return;
    }
    InputFile sourceFile = io.newInputFile(path);
    try (CloseableIterable<Record> reader =
        posDeleteReaderWriter.reader(sourceFile, deleteFile.format(), spec)) {
      Record record = null;
      Schema rowSchema = null;
      CloseableIterator<Record> recordIt = reader.iterator();
      if (recordIt.hasNext()) {
        record = recordIt.next();
        rowSchema = record.get(2) != null ? spec.schema() : null;
      }
      if (record != null) {
        try (PositionDeleteWriter<Record> writer =
            posDeleteReaderWriter.writer(
                outputFile, deleteFile.format(), spec, deleteFile.partition(), rowSchema)) {
          writer.write(newPositionDeleteRecord(record, sourcePrefix, targetPrefix));
          while (recordIt.hasNext()) {
            record = recordIt.next();
            if (record != null) {
              writer.write(newPositionDeleteRecord(record, sourcePrefix, targetPrefix));
            }
          }
        }
      }
    }
  }
  private static void rewriteDVFile(
      DeleteFile deleteFile,
      OutputFile outputFile,
      FileIO io,
      String sourcePrefix,
      String targetPrefix)
      throws IOException {
    List<Blob> rewrittenBlobs = Lists.newArrayList();
    try (PuffinReader reader = Puffin.read(io.newInputFile(deleteFile.location())).build()) {
      for (Pair<org.apache.iceberg.puffin.BlobMetadata, ByteBuffer> blobPair :
          reader.readAll(reader.fileMetadata().blobs())) {
        org.apache.iceberg.puffin.BlobMetadata blobMetadata = blobPair.first();
        ByteBuffer blobData = blobPair.second();
        Map<String, String> properties = Maps.newHashMap(blobMetadata.properties());
        String referencedDataFile = properties.get("referenced-data-file");
        if (referencedDataFile != null && referencedDataFile.startsWith(sourcePrefix)) {
          properties.put("referenced-data-file", newPath(referencedDataFile, sourcePrefix, targetPrefix));
        }
        rewrittenBlobs.add(
            new Blob(
                blobMetadata.type(),
                blobMetadata.inputFields(),
                blobMetadata.snapshotId(),
                blobMetadata.sequenceNumber(),
                blobData,
                PuffinCompressionCodec.forName(blobMetadata.compressionCodec()),
                properties));
      }
    }
    try (PuffinWriter writer =
        Puffin.write(outputFile).createdBy(IcebergBuild.fullVersion()).build()) {
      rewrittenBlobs.forEach(writer::write);
    }
  }
  private static PositionDelete newPositionDeleteRecord(
      Record record, String sourcePrefix, String targetPrefix) {
    PositionDelete delete = PositionDelete.create();
    String oldPath = (String) record.get(0);
    if (!oldPath.startsWith(sourcePrefix)) {
      throw new UnsupportedOperationException(
          "Expected delete file to be under the source prefix: " + sourcePrefix + " but was " + oldPath);
    }
    delete.set(newPath(oldPath, sourcePrefix, targetPrefix), (Long) record.get(1), record.get(2));
    return delete;
  }
  public static String newPath(String path, String sourcePrefix, String targetPrefix) {
    return combinePaths(targetPrefix, relativize(path, sourcePrefix));
  }
  public static String combinePaths(String absolutePath, String relativePath) {
    return maybeAppendFileSeparator(absolutePath) + relativePath;
  }
  public static String fileName(String path) {
    String filename = path;
    int lastIndex = path.lastIndexOf(FILE_SEPARATOR);
    if (lastIndex != -1) {
      filename = path.substring(lastIndex + 1);
    }
    return filename;
  }
  public static String relativize(String path, String prefix) {
    String toRemove = maybeAppendFileSeparator(prefix);
    if (!path.startsWith(toRemove)) {
      throw new IllegalArgumentException(
          String.format("Path %s does not start with %s", path, toRemove));
    }
    return path.substring(toRemove.length());
  }
  public static String maybeAppendFileSeparator(String path) {
    return path.endsWith(FILE_SEPARATOR) ? path : path + FILE_SEPARATOR;
  }
  public static String stagingPath(String originalPath, String sourcePrefix, String stagingDir) {
    return combinePaths(stagingDir, relativize(originalPath, sourcePrefix));
  }
}
