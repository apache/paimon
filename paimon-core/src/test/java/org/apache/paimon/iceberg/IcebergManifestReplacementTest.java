/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.iceberg;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.iceberg.manifest.IcebergDataFileMeta;
import org.apache.paimon.iceberg.manifest.IcebergManifestEntry;
import org.apache.paimon.iceberg.manifest.IcebergManifestFile;
import org.apache.paimon.iceberg.manifest.IcebergManifestFileMeta;
import org.apache.paimon.iceberg.manifest.IcebergManifestList;
import org.apache.paimon.iceberg.metadata.IcebergMetadata;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.operation.FileStoreCommitImpl;
import org.apache.paimon.operation.RemoveUnexistingManifests;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Regression tests for Iceberg metadata after manifest replacement. */
public class IcebergManifestReplacementTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testManifestReplacementPreservesPublishedFilesWithOverlappingLevelZero()
            throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.INT()},
                        new String[] {"pt", "k", "v"});
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "false");
        options.put(CoreOptions.NUM_LEVELS.key(), "3");
        FileStoreTable table =
                createPaimonTable(
                        LocalFileIO.create(),
                        rowType,
                        Collections.singletonList("pt"),
                        Arrays.asList("pt", "k"),
                        1,
                        options);
        String commitUser = UUID.randomUUID().toString();
        try (TableWriteImpl<?> write =
                        table.newWrite(commitUser)
                                .withIOManager(new IOManagerImpl(tempDir.toString() + "/tmp"));
                TableCommitImpl commit = table.newCommit(commitUser)) {
            for (int pt = 1; pt <= 2; pt++) {
                write.write(GenericRow.of(pt, 1, 10));
                write.write(GenericRow.of(pt, 2, 20));
            }
            commit.commit(1, write.prepareCommit(false, 1));

            for (int pt = 1; pt <= 2; pt++) {
                BinaryRow partition = new BinaryRow(1);
                BinaryRowWriter partitionWriter = new BinaryRowWriter(partition);
                partitionWriter.writeInt(0, pt);
                partitionWriter.complete();
                write.compact(partition, 0, true);
            }
            commit.commit(2, write.prepareCommit(true, 2));
            assertThat(table.store().newScan().plan().files())
                    .hasSize(2)
                    .allSatisfy(
                            entry ->
                                    assertThat(entry.file().level())
                                            .isEqualTo(table.coreOptions().numLevels() - 1));

            // The L0 update overlaps a published file containing an untouched key as well.
            write.write(GenericRow.of(1, 1, 11));
            commit.commit(3, write.prepareCommit(false, 3));
        }

        Snapshot before = table.snapshotManager().latestSnapshot();
        List<ManifestEntry> liveFiles =
                table.store().newScan().withSnapshot(before.id()).plan().files();
        assertThat(liveFiles).hasSize(3);
        assertThat(table.newSnapshotReader().withSnapshot(before.id()).read().dataSplits())
                .anySatisfy(
                        split -> {
                            assertThat(split.partition().getInt(0)).isEqualTo(1);
                            assertThat(split.rawConvertible()).isFalse();
                            assertThat(split.dataFiles())
                                    .extracting(file -> file.level())
                                    .containsExactlyInAnyOrder(
                                            0, table.coreOptions().numLevels() - 1);
                        });
        List<GenericRow> paimonRows =
                Arrays.asList(
                        GenericRow.of(1, 1, 11),
                        GenericRow.of(1, 2, 20),
                        GenericRow.of(2, 1, 10),
                        GenericRow.of(2, 2, 20));
        assertThat(getPaimonResult(table)).containsExactlyInAnyOrderElementsOf(paimonRows);
        List<String> icebergRows = getIcebergResult();
        assertThat(icebergRows)
                .containsExactlyInAnyOrder(
                        "Record(1, 1, 10)",
                        "Record(1, 2, 20)",
                        "Record(2, 1, 10)",
                        "Record(2, 2, 20)");

        // Replace only the manifest lists, preserving every live Paimon data file.
        replaceManifestLists(table, before);

        Snapshot after = table.snapshotManager().latestSnapshot();
        assertThat(after.id()).isEqualTo(before.id() + 1);
        assertThat(table.store().manifestListFactory().create().readDeltaManifests(after))
                .isEmpty();
        assertThat(table.store().newScan().withSnapshot(after.id()).plan().files())
                .containsExactlyInAnyOrderElementsOf(liveFiles);
        assertThat(getPaimonResult(table)).containsExactlyInAnyOrderElementsOf(paimonRows);
        assertThat(getIcebergResult())
                .as("manifest replacement must preserve published files that are still live")
                .containsExactlyInAnyOrderElementsOf(icebergRows);
    }

    @Test
    public void testManifestReplacementPreservesDeletionVectorInSplitRequiringMerge()
            throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        options.put(CoreOptions.DELETION_VECTORS_MERGE_ON_READ.key(), "true");
        options.put(CoreOptions.DELETION_VECTOR_BITMAP64.key(), "true");
        options.put(CoreOptions.NUM_LEVELS.key(), "3");
        options.put(IcebergOptions.FORMAT_VERSION.key(), "3");
        FileStoreTable table =
                createPaimonTable(
                        LocalFileIO.create(),
                        rowType,
                        Collections.emptyList(),
                        Collections.singletonList("k"),
                        1,
                        options);
        String commitUser = UUID.randomUUID().toString();
        try (TableWriteImpl<?> write =
                        table.newWrite(commitUser)
                                .withIOManager(new IOManagerImpl(tempDir.toString() + "/tmp"));
                TableCommitImpl commit = table.newCommit(commitUser)) {
            write.write(GenericRow.of(1, 10));
            write.write(GenericRow.of(2, 20));
            commit.commit(1, write.prepareCommit(false, 1));
            write.write(GenericRow.ofKind(RowKind.DELETE, 2, 20));
            write.compact(BinaryRow.EMPTY_ROW, 0, false);
            commit.commit(2, write.prepareCommit(true, 2));
        }
        // Leave the update at L0 even if background compaction would otherwise finish quickly.
        FileStoreTable writeOnlyTable =
                table.copy(Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "true"));
        try (TableWriteImpl<?> write =
                        writeOnlyTable
                                .newWrite(commitUser)
                                .withIOManager(new IOManagerImpl(tempDir.toString() + "/tmp"));
                TableCommitImpl commit = table.newCommit(commitUser)) {
            write.write(GenericRow.of(1, 11));
            commit.commit(3, write.prepareCommit(false, 3));
        }
        Snapshot before = table.snapshotManager().latestSnapshot();
        assertThat(table.newSnapshotReader().withSnapshot(before.id()).read().dataSplits())
                .anySatisfy(
                        split -> {
                            assertThat(split.rawConvertible()).isFalse();
                            assertThat(split.deletionFiles()).isPresent();
                            assertThat(split.dataFiles())
                                    .anySatisfy(file -> assertThat(file.level()).isZero())
                                    .anySatisfy(file -> assertThat(file.level()).isPositive());
                        });
        assertThat(getPaimonResult(table)).containsExactly(GenericRow.of(1, 11));
        List<IcebergDataFileMeta> icebergFiles = getIcebergFiles(table, before.id());
        assertThat(icebergFiles)
                .hasSize(2)
                .anySatisfy(
                        file -> {
                            assertThat(file.content())
                                    .isEqualTo(IcebergDataFileMeta.Content.POSITION_DELETES);
                            assertThat(file.recordCount()).isEqualTo(1);
                            assertThat(file.referencedDataFile()).isNotNull();
                        });

        replaceManifestLists(table, before);

        assertThat(getPaimonResult(table)).containsExactly(GenericRow.of(1, 11));
        // Split-based reconstruction omits the physical DV index size but must retain its payload
        // range, cardinality and referenced data file.
        assertThat(getIcebergFiles(table, before.id() + 1))
                .as("rebuilding metadata must retain deletes on published files in merging splits")
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("fileSizeInBytes")
                .containsExactlyInAnyOrderElementsOf(icebergFiles);
        IcebergPathFactory pathFactory =
                new IcebergPathFactory(IcebergCommitCallback.catalogTableMetadataPath(table));
        IcebergMetadata metadata =
                IcebergMetadata.fromPath(
                        table.fileIO(), pathFactory.toMetadataPath(before.id() + 1));
        assertThat(metadata.currentSnapshot().summary().getSummary())
                .containsEntry("total-delete-files", "1")
                .containsEntry("total-position-deletes", "1");
    }

    @Test
    public void testRepairProcedureRetryRecoversFailedIcebergMetadataWrite() throws Exception {
        assertRepairProcedureRetryRecoversFailedPublication(false);
    }

    @Test
    public void testRepairProcedureRetryRecoversFailedIcebergCatalogCommit() throws Exception {
        RecordingIcebergMetadataCommitter.COMMITS.clear();
        RecordingIcebergMetadataCommitter.BASES.clear();
        try {
            assertRepairProcedureRetryRecoversFailedPublication(true);
        } finally {
            RecordingIcebergMetadataCommitter.failNextCommit = false;
            RecordingIcebergMetadataCommitter.COMMITS.clear();
            RecordingIcebergMetadataCommitter.BASES.clear();
        }
    }

    private void assertRepairProcedureRetryRecoversFailedPublication(boolean failCatalogCommit)
            throws Exception {
        MetadataWriteFailingFileIO fileIO = new MetadataWriteFailingFileIO();
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()},
                        new String[] {"pt", "v"});
        Map<String, String> options = new HashMap<>();
        if (failCatalogCommit) {
            options.put(
                    IcebergOptions.METADATA_ICEBERG_STORAGE.key(),
                    IcebergOptions.StorageType.HADOOP_CATALOG.toString());
            options.put(
                    IcebergOptions.METADATA_ICEBERG_STORAGE_LOCATION.key(),
                    IcebergOptions.StorageLocation.TABLE_LOCATION.toString());
        }
        FileStoreTable table =
                createPaimonTable(
                        fileIO,
                        rowType,
                        Collections.singletonList("pt"),
                        Collections.emptyList(),
                        -1,
                        options);
        String commitUser = UUID.randomUUID().toString();
        try (TableWriteImpl<?> write = table.newWrite(commitUser);
                TableCommitImpl commit = table.newCommit(commitUser)) {
            write.write(GenericRow.of(1, 10));
            commit.commit(1, write.prepareCommit(false, 1));
            write.write(GenericRow.of(2, 20));
            commit.commit(2, write.prepareCommit(false, 2));
        }

        Snapshot before = table.snapshotManager().latestSnapshot();
        List<ManifestFileMeta> manifests =
                table.store().newScan().manifestsReader().read(before, ScanMode.ALL).allManifests;
        assertThat(manifests).hasSize(2);
        for (ManifestFileMeta manifest : manifests) {
            if (table.store().newScan().readManifest(manifest).stream()
                    .anyMatch(entry -> entry.partition().getInt(0) == 1)) {
                assertThat(
                                fileIO.delete(
                                        table.store()
                                                .pathFactory()
                                                .toManifestFilePath(manifest.fileName()),
                                        false))
                        .isTrue();
            }
        }

        if (failCatalogCommit) {
            RecordingIcebergMetadataCommitter.COMMITS.clear();
            RecordingIcebergMetadataCommitter.BASES.clear();
            RecordingIcebergMetadataCommitter.failNextCommit = true;
        } else {
            fileIO.failNextMetadataWrite = true;
        }
        assertThatThrownBy(() -> new RemoveUnexistingManifests(table).execute())
                .hasStackTraceContaining(
                        failCatalogCommit
                                ? "injected catalog failure"
                                : "injected Iceberg metadata write failure");
        Snapshot repaired = table.snapshotManager().latestSnapshot();
        assertThat(repaired.id()).isEqualTo(before.id() + 1);
        assertThat(getPaimonResult(table)).containsExactly(GenericRow.of(2, 20));
        IcebergPathFactory pathFactory =
                new IcebergPathFactory(IcebergCommitCallback.catalogTableMetadataPath(table));
        Path repairedMetadataPath = pathFactory.toMetadataPath(repaired.id());
        assertThat(fileIO.exists(repairedMetadataPath)).isEqualTo(failCatalogCommit);
        if (failCatalogCommit) {
            assertThat(RecordingIcebergMetadataCommitter.COMMITS).isEmpty();
        } else {
            assertThat(getIcebergResult())
                    .containsExactlyInAnyOrder("Record(1, 10)", "Record(2, 20)");
        }

        // Recover through the public repair path after reloading the table, without the repair
        // user.
        FileStoreTable reloaded = FileStoreTableFactory.create(fileIO, table.location());
        assertThat(new RemoveUnexistingManifests(reloaded).execute()).isFalse();
        assertThat(reloaded.snapshotManager().latestSnapshotId()).isEqualTo(repaired.id());
        assertThat(getPaimonResult(reloaded)).containsExactly(GenericRow.of(2, 20));
        assertThat(getIcebergResult())
                .as("retrying the repair procedure must finish publishing the repaired snapshot")
                .containsExactly("Record(2, 20)");
        assertThat(IcebergMetadata.fromPath(fileIO, repairedMetadataPath).currentSnapshotId())
                .isEqualTo(repaired.id());
        if (failCatalogCommit) {
            assertThat(RecordingIcebergMetadataCommitter.COMMITS)
                    .containsExactly(repairedMetadataPath);
            assertThat(RecordingIcebergMetadataCommitter.BASES)
                    .containsExactly(pathFactory.toMetadataPath(before.id()));
        }

        assertThat(new RemoveUnexistingManifests(reloaded).execute()).isFalse();
        assertThat(reloaded.snapshotManager().latestSnapshotId()).isEqualTo(repaired.id());
        assertThat(getIcebergResult()).containsExactly("Record(2, 20)");
    }

    private void replaceManifestLists(FileStoreTable table, Snapshot before) throws Exception {
        ManifestList manifestList = table.store().manifestListFactory().create();
        Pair<String, Long> baseManifestList =
                manifestList.write(manifestList.readDataManifests(before));
        Pair<String, Long> emptyManifestList = manifestList.write(Collections.emptyList());
        try (FileStoreCommitImpl commit =
                (FileStoreCommitImpl)
                        table.store().newCommit(UUID.randomUUID().toString(), table)) {
            assertThat(
                            commit.replaceManifestList(
                                    before,
                                    before.totalRecordCount(),
                                    baseManifestList,
                                    emptyManifestList))
                    .isTrue();
        }
    }

    private List<IcebergDataFileMeta> getIcebergFiles(FileStoreTable table, long snapshotId)
            throws IOException {
        IcebergPathFactory pathFactory =
                new IcebergPathFactory(IcebergCommitCallback.catalogTableMetadataPath(table));
        IcebergMetadata metadata =
                IcebergMetadata.fromPath(table.fileIO(), pathFactory.toMetadataPath(snapshotId));
        IcebergManifestFile manifestFile = IcebergManifestFile.create(table, pathFactory);
        List<IcebergDataFileMeta> files = new ArrayList<>();
        for (IcebergManifestFileMeta manifest :
                IcebergManifestList.create(table, pathFactory)
                        .read(new Path(metadata.currentSnapshot().manifestList()).getName())) {
            for (IcebergManifestEntry entry : manifestFile.read(manifest)) {
                if (entry.isLive()) {
                    files.add(entry.file());
                }
            }
        }
        return files;
    }

    private FileStoreTable createPaimonTable(
            FileIO fileIO,
            RowType rowType,
            List<String> partitionKeys,
            List<String> primaryKeys,
            int numBuckets,
            Map<String, String> customOptions)
            throws Exception {
        Path path = new Path(tempDir.toString());

        Options options = new Options(customOptions);
        options.set(CoreOptions.BUCKET, numBuckets);
        if (!options.contains(IcebergOptions.METADATA_ICEBERG_STORAGE)) {
            options.set(
                    IcebergOptions.METADATA_ICEBERG_STORAGE,
                    IcebergOptions.StorageType.TABLE_LOCATION);
        }
        options.set(CoreOptions.FILE_FORMAT, "avro");
        options.set(CoreOptions.TARGET_FILE_SIZE, MemorySize.ofKibiBytes(32));
        options.set(IcebergOptions.COMPACT_MIN_FILE_NUM, 8);
        options.set(IcebergOptions.METADATA_DELETE_AFTER_COMMIT, true);
        options.set(IcebergOptions.METADATA_PREVIOUS_VERSIONS_MAX, 1);
        options.set(CoreOptions.MANIFEST_TARGET_FILE_SIZE, MemorySize.ofKibiBytes(8));
        Schema schema =
                new Schema(rowType.getFields(), partitionKeys, primaryKeys, options.toMap(), "");

        try (FileSystemCatalog paimonCatalog = new FileSystemCatalog(fileIO, path)) {
            paimonCatalog.createDatabase("mydb", false);
            Identifier paimonIdentifier = Identifier.create("mydb", "t");
            paimonCatalog.createTable(paimonIdentifier, schema, false);
            return (FileStoreTable) paimonCatalog.getTable(paimonIdentifier);
        }
    }

    private List<GenericRow> getPaimonResult(FileStoreTable table) throws Exception {
        ReadBuilder readBuilder = table.newReadBuilder();
        RowDataToObjectArrayConverter converter =
                new RowDataToObjectArrayConverter(table.rowType());
        List<GenericRow> result = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan())) {
            reader.forEachRemaining(row -> result.add(converter.toGenericRow(row)));
        }
        return result;
    }

    private List<String> getIcebergResult() throws Exception {
        List<String> result = new ArrayList<>();
        try (HadoopCatalog catalog = new HadoopCatalog(new Configuration(), tempDir.toString());
                CloseableIterable<Record> records =
                        IcebergGenerics.read(catalog.loadTable(TableIdentifier.of("mydb.db", "t")))
                                .build()) {
            for (Record record : records) {
                result.add(record.toString());
            }
        }
        return result;
    }

    /** A {@link LocalFileIO} that fails one Iceberg metadata write. */
    private static class MetadataWriteFailingFileIO extends LocalFileIO {

        private boolean failNextMetadataWrite;

        @Override
        public boolean tryToWriteAtomic(Path path, String content) throws IOException {
            if (failNextMetadataWrite && path.getName().endsWith(".metadata.json")) {
                failNextMetadataWrite = false;
                throw new IOException("injected Iceberg metadata write failure");
            }
            return super.tryToWriteAtomic(path, content);
        }
    }
}
