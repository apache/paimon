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
import org.apache.paimon.deletionvectors.Bitmap64DeletionVector;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.iceberg.manifest.IcebergDataFileMeta;
import org.apache.paimon.iceberg.manifest.IcebergManifestEntry;
import org.apache.paimon.iceberg.manifest.IcebergManifestFile;
import org.apache.paimon.iceberg.manifest.IcebergManifestFileMeta;
import org.apache.paimon.iceberg.manifest.IcebergManifestList;
import org.apache.paimon.iceberg.metadata.IcebergMetadata;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.operation.FileStoreCommitImpl;
import org.apache.paimon.operation.RemoveUnexistingManifests;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;

/** Regression tests for Iceberg metadata after manifest replacement. */
public class IcebergManifestReplacementTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testRepairSkipsDataEvolutionFilesRequiringColumnMerging() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.INT()},
                        new String[] {"pt", "id", "v"});
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        options.put(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        options.put(CoreOptions.WRITE_ONLY.key(), "true");
        options.put(CoreOptions.MANIFEST_MERGE_MIN_COUNT.key(), "1000");
        options.put(IcebergOptions.METADATA_ICEBERG_STORAGE.key(), "disabled");
        FileStoreTable table =
                createPaimonTable(
                        LocalFileIO.create(),
                        rowType,
                        Collections.singletonList("pt"),
                        Collections.emptyList(),
                        -1,
                        options);
        String commitUser = UUID.randomUUID().toString();
        try (TableWriteImpl<?> write = table.newWrite(commitUser);
                TableCommitImpl commit = table.newCommit(commitUser)) {
            write.write(GenericRow.of(1, 10, 100));
            write.write(GenericRow.of(1, 20, 200));
            commit.commit(1, write.prepareCommit(false, 1));
            write.write(GenericRow.of(3, 40, 400));
            commit.commit(2, write.prepareCommit(false, 2));
        }

        long firstRowId =
                table.store().newScan().plan().files().stream()
                        .filter(entry -> entry.partition().getInt(0) == 1)
                        .findFirst()
                        .get()
                        .file()
                        .firstRowId();
        // Update v over the original file's complete row-ID range, retaining id in that file.
        try (TableWriteImpl<?> write =
                        table.newWrite(commitUser)
                                .withWriteType(rowType.project(Arrays.asList("pt", "v")));
                TableCommitImpl commit = table.newCommit(commitUser)) {
            write.write(GenericRow.of(1, 111));
            write.write(GenericRow.of(1, 222));
            List<CommitMessage> messages = write.prepareCommit(false, 3);
            assertThat(messages).hasSize(1);
            DataIncrement increment = ((CommitMessageImpl) messages.get(0)).newFilesIncrement();
            assertThat(increment.newFiles())
                    .singleElement()
                    .satisfies(file -> assertThat(file.rowCount()).isEqualTo(2));
            increment.newFiles().replaceAll(file -> file.assignFirstRowId(firstRowId));
            commit.commit(3, messages);
        }

        // Enable Iceberg on the existing table. Initial publication must exclude the files
        // requiring column merging, while publishing the independently readable partitions.
        table.schemaManager()
                .commitChanges(
                        SchemaChange.setOption(
                                IcebergOptions.METADATA_ICEBERG_STORAGE.key(), "table-location"));
        table = FileStoreTableFactory.create(table.fileIO(), table.location());
        try (TableWriteImpl<?> write = table.newWrite(commitUser);
                TableCommitImpl commit = table.newCommit(commitUser)) {
            write.write(GenericRow.of(2, 30, 300));
            commit.commit(4, write.prepareCommit(false, 4));
        }
        Snapshot before = table.snapshotManager().latestSnapshot();
        assertThat(table.newSnapshotReader().withSnapshot(before.id()).read().dataSplits())
                .anySatisfy(
                        split -> {
                            assertThat(split.partition().getInt(0)).isEqualTo(1);
                            assertThat(split.rawConvertible()).isFalse();
                            assertThat(split.dataFiles()).hasSize(2);
                        });
        assertThat(getPaimonResult(table))
                .containsExactlyInAnyOrder(
                        GenericRow.of(1, 10, 111),
                        GenericRow.of(1, 20, 222),
                        GenericRow.of(2, 30, 300),
                        GenericRow.of(3, 40, 400));
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrder("Record(2, 30, 300)", "Record(3, 40, 400)");

        // Lose only the last commit's manifest, leaving the partial-column files and an
        // unaffected raw-convertible partition in the repaired snapshot.
        List<ManifestFileMeta> lostManifests =
                table.store().manifestListFactory().create().readDeltaManifests(before);
        assertThat(lostManifests).hasSize(1);
        ManifestFileMeta lostManifest = lostManifests.get(0);
        assertThat(table.store().newScan().readManifest(lostManifest))
                .singleElement()
                .satisfies(entry -> assertThat(entry.partition().getInt(0)).isEqualTo(2));
        assertThat(
                        table.fileIO()
                                .delete(
                                        table.store()
                                                .pathFactory()
                                                .toManifestFilePath(lostManifest.fileName()),
                                        false))
                .isTrue();

        assertThat(new RemoveUnexistingManifests(table).execute()).isTrue();

        Snapshot repaired = table.snapshotManager().latestSnapshot();
        assertThat(repaired.id()).isEqualTo(before.id() + 1);
        assertThat(getPaimonResult(table))
                .containsExactlyInAnyOrder(
                        GenericRow.of(1, 10, 111),
                        GenericRow.of(1, 20, 222),
                        GenericRow.of(3, 40, 400));
        assertThat(getIcebergResult()).containsExactly("Record(3, 40, 400)");
        IcebergPathFactory pathFactory =
                new IcebergPathFactory(IcebergCommitCallback.catalogTableMetadataPath(table));
        assertThat(
                        IcebergMetadata.fromPath(
                                        table.fileIO(), pathFactory.toMetadataPath(repaired.id()))
                                .currentSnapshotId())
                .isEqualTo(repaired.id());

        assertThat(new RemoveUnexistingManifests(table).execute()).isFalse();
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(repaired.id());
        assertThat(getIcebergResult()).containsExactly("Record(3, 40, 400)");
    }

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
    public void testRepairWithoutIcebergBasePreservesPublishedFiles() throws Exception {
        MetadataWriteFailingFileIO fileIO = new MetadataWriteFailingFileIO();
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.INT()},
                        new String[] {"pt", "k", "v"});
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "false");
        options.put(CoreOptions.NUM_LEVELS.key(), "3");
        options.put(CoreOptions.MANIFEST_MERGE_MIN_COUNT.key(), "1000");
        FileStoreTable table =
                createPaimonTable(
                        fileIO,
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
        }

        // Keep the overlapping update at L0 so its split still contains a published file.
        FileStoreTable writeOnlyTable =
                table.copy(Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "true"));
        List<String> icebergRows;
        long publishedSnapshotId;
        try (TableWriteImpl<?> write =
                        writeOnlyTable
                                .newWrite(commitUser)
                                .withIOManager(new IOManagerImpl(tempDir.toString() + "/tmp"));
                TableCommitImpl commit = table.newCommit(commitUser)) {
            write.write(GenericRow.of(1, 1, 11));
            commit.commit(3, write.prepareCommit(false, 3));
            publishedSnapshotId = table.snapshotManager().latestSnapshotId();
            icebergRows = getIcebergResult();
            assertThat(icebergRows)
                    .containsExactlyInAnyOrder(
                            "Record(1, 1, 10)",
                            "Record(1, 2, 20)",
                            "Record(2, 1, 10)",
                            "Record(2, 2, 20)");

            // Fail Iceberg publication after the Paimon snapshot has been committed.
            write.write(GenericRow.of(3, 1, 30));
            fileIO.failNextMetadataWrite = true;
            assertThatThrownBy(() -> commit.commit(4, write.prepareCommit(false, 4)))
                    .hasStackTraceContaining("injected Iceberg metadata write failure");
        }

        Snapshot failed = table.snapshotManager().latestSnapshot();
        assertThat(failed.id()).isEqualTo(publishedSnapshotId + 1);
        IcebergPathFactory pathFactory =
                new IcebergPathFactory(IcebergCommitCallback.catalogTableMetadataPath(table));
        assertThat(fileIO.exists(pathFactory.toMetadataPath(publishedSnapshotId))).isTrue();
        assertThat(fileIO.exists(pathFactory.toMetadataPath(failed.id()))).isFalse();
        assertThat(getIcebergResult()).containsExactlyInAnyOrderElementsOf(icebergRows);

        // Lose only the new partition's manifest; the published files are unrelated to the repair.
        List<ManifestFileMeta> lostManifests =
                table.store().manifestListFactory().create().readDeltaManifests(failed);
        assertThat(lostManifests).hasSize(1);
        ManifestFileMeta lostManifest = lostManifests.get(0);
        assertThat(table.store().newScan().readManifest(lostManifest))
                .hasSize(1)
                .allSatisfy(entry -> assertThat(entry.partition().getInt(0)).isEqualTo(3));
        assertThat(
                        fileIO.delete(
                                table.store()
                                        .pathFactory()
                                        .toManifestFilePath(lostManifest.fileName()),
                                false))
                .isTrue();

        FileStoreTable reloaded = FileStoreTableFactory.create(fileIO, table.location());
        assertThat(new RemoveUnexistingManifests(reloaded).execute()).isTrue();
        Snapshot repaired = reloaded.snapshotManager().latestSnapshot();
        assertThat(repaired.id()).isEqualTo(failed.id() + 1);
        IcebergMetadata repairedMetadata =
                IcebergMetadata.fromPath(fileIO, pathFactory.toMetadataPath(repaired.id()));
        assertThat(repairedMetadata.currentSnapshotId()).isEqualTo(repaired.id());
        assertThat(repairedMetadata.currentSnapshot().summary().getSummary())
                .containsEntry("total-data-files", "2")
                .containsEntry("total-records", "4")
                .containsEntry("total-delete-files", "0");
        assertThat(getPaimonResult(reloaded))
                .containsExactlyInAnyOrder(
                        GenericRow.of(1, 1, 11),
                        GenericRow.of(1, 2, 20),
                        GenericRow.of(2, 1, 10),
                        GenericRow.of(2, 2, 20));
        assertThat(reloaded.newSnapshotReader().withSnapshot(repaired.id()).read().dataSplits())
                .anySatisfy(
                        split -> {
                            assertThat(split.partition().getInt(0)).isEqualTo(1);
                            assertThat(split.rawConvertible()).isFalse();
                            assertThat(split.dataFiles())
                                    .extracting(file -> file.level())
                                    .containsExactlyInAnyOrder(
                                            0, reloaded.coreOptions().numLevels() - 1);
                        });
        List<String> repairedIcebergRows = getIcebergResult();
        assertThat(new RemoveUnexistingManifests(reloaded).execute()).isFalse();
        assertThat(reloaded.snapshotManager().latestSnapshotId()).isEqualTo(repaired.id());

        assertThat(repairedIcebergRows)
                .as("repair without Iceberg base metadata must preserve still-live published files")
                .containsExactlyInAnyOrderElementsOf(icebergRows);
        assertThat(getIcebergResult())
                .as("retrying the repair must preserve still-live published files")
                .containsExactlyInAnyOrderElementsOf(icebergRows);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testRepairWithoutIcebergBasePreservesInitiallyPublishedLevelZeroFiles(
            boolean overlappingUpdate) throws Exception {
        MetadataWriteFailingFileIO fileIO = new MetadataWriteFailingFileIO();
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.INT()},
                        new String[] {"pt", "k", "v"});
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "false");
        options.put(CoreOptions.NUM_LEVELS.key(), "3");
        options.put(CoreOptions.WRITE_ONLY.key(), "true");
        options.put(CoreOptions.MANIFEST_MERGE_MIN_COUNT.key(), "1000");
        FileStoreTable table =
                createPaimonTable(
                        fileIO,
                        rowType,
                        Collections.singletonList("pt"),
                        Arrays.asList("pt", "k"),
                        1,
                        options);
        String commitUser = UUID.randomUUID().toString();
        List<String> publishedIcebergRows;
        List<ManifestEntry> liveFiles;
        long publishedSnapshotId;
        try (TableWriteImpl<?> write =
                        table.newWrite(commitUser)
                                .withIOManager(new IOManagerImpl(tempDir.toString() + "/tmp"));
                TableCommitImpl commit = table.newCommit(commitUser)) {
            write.write(GenericRow.of(1, 1, 10));
            write.write(GenericRow.of(1, 2, 20));
            commit.commit(1, write.prepareCommit(false, 1));

            // Initial publication can expose a raw-convertible L0 file before any compaction.
            assertThat(table.store().newScan().plan().files())
                    .singleElement()
                    .satisfies(entry -> assertThat(entry.file().level()).isZero());
            publishedIcebergRows = getIcebergResult();
            assertThat(publishedIcebergRows)
                    .containsExactlyInAnyOrder("Record(1, 1, 10)", "Record(1, 2, 20)");

            if (overlappingUpdate) {
                // This update makes the published L0 file's split require merging.
                write.write(GenericRow.of(1, 1, 11));
                commit.commit(2, write.prepareCommit(false, 2));
                assertThat(table.newSnapshotReader().read().dataSplits())
                        .singleElement()
                        .satisfies(
                                split -> {
                                    assertThat(split.rawConvertible()).isFalse();
                                    assertThat(split.dataFiles())
                                            .extracting(file -> file.level())
                                            .containsExactly(0, 0);
                                });
                assertThat(getIcebergResult())
                        .containsExactlyInAnyOrderElementsOf(publishedIcebergRows);
            }
            publishedSnapshotId = table.snapshotManager().latestSnapshotId();
            liveFiles = table.store().newScan().plan().files();

            // Commit an unrelated partition while failing its Iceberg metadata write.
            write.write(GenericRow.of(2, 3, 30));
            fileIO.failNextMetadataWrite = true;
            long failedIdentifier = overlappingUpdate ? 3 : 2;
            assertThatThrownBy(
                            () ->
                                    commit.commit(
                                            failedIdentifier,
                                            write.prepareCommit(false, failedIdentifier)))
                    .hasStackTraceContaining("injected Iceberg metadata write failure");
        }

        Snapshot failed = table.snapshotManager().latestSnapshot();
        assertThat(failed.id()).isEqualTo(publishedSnapshotId + 1);
        IcebergPathFactory pathFactory =
                new IcebergPathFactory(IcebergCommitCallback.catalogTableMetadataPath(table));
        assertThat(fileIO.exists(pathFactory.toMetadataPath(publishedSnapshotId))).isTrue();
        assertThat(fileIO.exists(pathFactory.toMetadataPath(failed.id()))).isFalse();

        // Lose only the new partition's manifest, leaving the published L0 file untouched.
        List<ManifestFileMeta> lostManifests =
                table.store().manifestListFactory().create().readDeltaManifests(failed);
        assertThat(lostManifests).hasSize(1);
        ManifestFileMeta lostManifest = lostManifests.get(0);
        assertThat(table.store().newScan().readManifest(lostManifest))
                .hasSize(1)
                .allSatisfy(entry -> assertThat(entry.partition().getInt(0)).isEqualTo(2));
        assertThat(
                        fileIO.delete(
                                table.store()
                                        .pathFactory()
                                        .toManifestFilePath(lostManifest.fileName()),
                                false))
                .isTrue();

        FileStoreTable reloaded = FileStoreTableFactory.create(fileIO, table.location());
        assertThat(new RemoveUnexistingManifests(reloaded).execute()).isTrue();
        assertThat(reloaded.snapshotManager().latestSnapshotId()).isEqualTo(failed.id() + 1);
        assertThat(reloaded.store().newScan().plan().files())
                .containsExactlyInAnyOrderElementsOf(liveFiles);
        assertThat(getPaimonResult(reloaded))
                .containsExactlyInAnyOrder(
                        GenericRow.of(1, 1, overlappingUpdate ? 11 : 10), GenericRow.of(1, 2, 20));
        assertThat(getIcebergResult())
                .as("repair without Iceberg base metadata must retain initially published L0 files")
                .containsExactlyInAnyOrderElementsOf(publishedIcebergRows);
    }

    @Test
    public void testRepairWithoutIcebergBasePreservesIdentityAndRowIdAllocation() throws Exception {
        MetadataWriteFailingFileIO fileIO = new MetadataWriteFailingFileIO();
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()},
                        new String[] {"pt", "v"});
        Map<String, String> options = new HashMap<>();
        options.put(IcebergOptions.FORMAT_VERSION.key(), "3");
        options.put(CoreOptions.WRITE_ONLY.key(), "true");
        options.put(CoreOptions.MANIFEST_MERGE_MIN_COUNT.key(), "1000");
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
        Snapshot published = table.snapshotManager().latestSnapshot();
        IcebergPathFactory pathFactory =
                new IcebergPathFactory(IcebergCommitCallback.catalogTableMetadataPath(table));
        IcebergMetadata publishedMetadata =
                IcebergMetadata.fromPath(fileIO, pathFactory.toMetadataPath(published.id()));
        assertThat(publishedMetadata.nextRowId()).isEqualTo(2L);

        // Leave a gap in Iceberg metadata while committing the next Paimon snapshot.
        try (TableWriteImpl<?> write = table.newWrite(commitUser);
                TableCommitImpl commit = table.newCommit(commitUser)) {
            write.write(GenericRow.of(3, 30));
            fileIO.failNextMetadataWrite = true;
            assertThatThrownBy(() -> commit.commit(3, write.prepareCommit(false, 3)))
                    .hasStackTraceContaining("injected Iceberg metadata write failure");
        }
        Snapshot failed = table.snapshotManager().latestSnapshot();
        assertThat(failed.id()).isEqualTo(published.id() + 1);
        assertThat(fileIO.exists(pathFactory.toMetadataPath(published.id()))).isTrue();
        assertThat(fileIO.exists(pathFactory.toMetadataPath(failed.id()))).isFalse();

        // Lose the first partition and the failed commit's partition. The surviving row was
        // assigned ID 1; rebuilding it at ID 0 would reuse the lost first partition's row ID.
        List<ManifestFileMeta> manifests =
                table.store().newScan().manifestsReader().read(failed, ScanMode.ALL).allManifests;
        assertThat(manifests).hasSize(3);
        for (ManifestFileMeta manifest : manifests) {
            if (table.store().newScan().readManifest(manifest).stream()
                    .anyMatch(entry -> entry.partition().getInt(0) != 2)) {
                assertThat(
                                fileIO.delete(
                                        table.store()
                                                .pathFactory()
                                                .toManifestFilePath(manifest.fileName()),
                                        false))
                        .isTrue();
            }
        }

        FileStoreTable reloaded = FileStoreTableFactory.create(fileIO, table.location());
        assertThat(new RemoveUnexistingManifests(reloaded).execute()).isTrue();
        Snapshot repaired = reloaded.snapshotManager().latestSnapshot();
        assertThat(repaired.id()).isEqualTo(failed.id() + 1);
        assertThat(getPaimonResult(reloaded)).containsExactly(GenericRow.of(2, 20));
        assertThat(getIcebergFiles(reloaded, repaired.id()))
                .singleElement()
                .satisfies(file -> assertThat(file.partition().getInt(0)).isEqualTo(2));
        IcebergMetadata repairedMetadata =
                IcebergMetadata.fromPath(fileIO, pathFactory.toMetadataPath(repaired.id()));
        assertAll(
                () ->
                        assertThat(repairedMetadata.currentSnapshot().parentSnapshotId())
                                .as("repair must extend the surviving Iceberg publication")
                                .isEqualTo(published.id()),
                () ->
                        assertThat(repairedMetadata.tableUuid())
                                .as("repair must preserve the previously published table identity")
                                .isEqualTo(publishedMetadata.tableUuid()),
                () ->
                        assertThat(repairedMetadata.currentSnapshot().firstRowId())
                                .as("repair must allocate new row IDs above the published range")
                                .isGreaterThanOrEqualTo(publishedMetadata.nextRowId()),
                () ->
                        assertThat(repairedMetadata.nextRowId())
                                .as("repair must not decrease the published next row ID")
                                .isGreaterThanOrEqualTo(publishedMetadata.nextRowId()));
        assertThat(new RemoveUnexistingManifests(reloaded).execute()).isFalse();
        assertThat(reloaded.snapshotManager().latestSnapshotId()).isEqualTo(repaired.id());
        assertThat(IcebergMetadata.fromPath(fileIO, pathFactory.toMetadataPath(repaired.id())))
                .isEqualTo(repairedMetadata);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testManifestReplacementPreservesDeletionVectorInSplitRequiringMerge(
            boolean missingBase) throws Exception {
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

        IcebergPathFactory pathFactory =
                new IcebergPathFactory(IcebergCommitCallback.catalogTableMetadataPath(table));
        if (missingBase) {
            assertThat(table.fileIO().delete(pathFactory.toMetadataPath(before.id()), false))
                    .isTrue();
        }
        replaceManifestLists(table, before);

        assertThat(getPaimonResult(table)).containsExactly(GenericRow.of(1, 11));
        // Split-based reconstruction omits the physical DV index size but must retain its payload
        // range, cardinality and referenced data file.
        assertThat(getIcebergFiles(table, before.id() + 1))
                .as("rebuilding metadata must retain deletes on published files in merging splits")
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("fileSizeInBytes")
                .containsExactlyInAnyOrderElementsOf(icebergFiles);
        IcebergMetadata metadata =
                IcebergMetadata.fromPath(
                        table.fileIO(), pathFactory.toMetadataPath(before.id() + 1));
        assertThat(metadata.currentSnapshot().summary().getSummary())
                .containsEntry("total-delete-files", "1")
                .containsEntry("total-position-deletes", "1");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testRepairDeduplicatesDeletionVectorsForFileAtMultipleLevels(boolean missingBase)
            throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        options.put(CoreOptions.DELETION_VECTORS_MERGE_ON_READ.key(), "true");
        options.put(CoreOptions.DELETION_VECTOR_BITMAP64.key(), "true");
        options.put(CoreOptions.NUM_LEVELS.key(), "3");
        options.put(CoreOptions.MANIFEST_MERGE_MIN_COUNT.key(), "1000");
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

        Snapshot before = table.snapshotManager().latestSnapshot();
        List<IcebergDataFileMeta> deleteFiles =
                getIcebergFiles(table, before.id()).stream()
                        .filter(
                                file ->
                                        file.content()
                                                == IcebergDataFileMeta.Content.POSITION_DELETES)
                        .collect(Collectors.toList());
        assertThat(deleteFiles).hasSize(1);
        String dataFileName = new Path(deleteFiles.get(0).referencedDataFile()).getName();

        // Keep the same entries, separating the old-level DELETE from the promoted ADD so
        // losing that manifest leaves the physical file live at both levels.
        ManifestList manifestList = table.store().manifestListFactory().create();
        List<ManifestFileMeta> splitManifests = new ArrayList<>();
        List<Path> manifestsToDelete = new ArrayList<>();
        for (ManifestFileMeta manifest : manifestList.readDataManifests(before)) {
            for (ManifestEntry entry : table.store().newScan().readManifest(manifest)) {
                List<ManifestFileMeta> entryManifests =
                        table.store()
                                .manifestFileFactory()
                                .create()
                                .write(Collections.singletonList(entry));
                splitManifests.addAll(entryManifests);
                if (entry.kind() == FileKind.DELETE
                        && entry.file().level() == 0
                        && entry.file().fileName().equals(dataFileName)) {
                    assertThat(entryManifests).hasSize(1);
                    manifestsToDelete.add(
                            table.store()
                                    .pathFactory()
                                    .toManifestFilePath(entryManifests.get(0).fileName()));
                }
            }
        }
        assertThat(manifestsToDelete).hasSize(1);
        Pair<String, Long> baseManifestList = manifestList.write(splitManifests);
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
        assertThat(getPaimonResult(table)).containsExactly(GenericRow.of(1, 10));
        assertThat(table.fileIO().delete(manifestsToDelete.get(0), false)).isTrue();
        if (missingBase) {
            IcebergPathFactory pathFactory =
                    new IcebergPathFactory(IcebergCommitCallback.catalogTableMetadataPath(table));
            assertThat(
                            table.fileIO()
                                    .delete(
                                            pathFactory.toMetadataPath(
                                                    table.snapshotManager().latestSnapshotId()),
                                            false))
                    .isTrue();
        }

        assertThat(new RemoveUnexistingManifests(table).execute()).isTrue();

        Snapshot repaired = table.snapshotManager().latestSnapshot();
        assertThat(table.store().newScan().withSnapshot(repaired.id()).plan().files())
                .filteredOn(entry -> entry.file().fileName().equals(dataFileName))
                .extracting(entry -> entry.file().level())
                .containsExactlyInAnyOrder(0, table.coreOptions().numLevels() - 1);
        assertThat(getPaimonResult(table)).containsExactly(GenericRow.of(1, 10));
        List<IcebergDataFileMeta> repairedFiles = getIcebergFiles(table, repaired.id());
        assertThat(repairedFiles)
                .filteredOn(file -> file.content() == IcebergDataFileMeta.Content.DATA)
                .extracting(IcebergDataFileMeta::filePath)
                .containsExactly(deleteFiles.get(0).referencedDataFile());
        assertThat(repairedFiles)
                .filteredOn(file -> file.content() == IcebergDataFileMeta.Content.POSITION_DELETES)
                .as("Iceberg must contain one deletion vector per physical data file")
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("fileSizeInBytes")
                .containsExactlyElementsOf(deleteFiles);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testDeletionVectorOnlyCommitDoesNotReadUnchangedDataManifests(boolean retry)
            throws Exception {
        ManifestReadTrackingFileIO fileIO = new ManifestReadTrackingFileIO();
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()},
                        new String[] {"pt", "v"});
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        options.put(CoreOptions.DELETION_VECTOR_BITMAP64.key(), "true");
        options.put(CoreOptions.WRITE_ONLY.key(), "true");
        options.put(IcebergOptions.FORMAT_VERSION.key(), "3");
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
            write.write(GenericRow.of(1, 20));
            write.write(GenericRow.of(2, 30));
            write.write(GenericRow.of(2, 40));
            commit.commit(1, write.prepareCommit(false, 1));
        }

        Snapshot before = table.snapshotManager().latestSnapshot();
        List<ManifestEntry> dataFiles = table.store().newScan().plan().files();
        assertThat(dataFiles).hasSize(2);
        ManifestEntry deletedFrom =
                dataFiles.stream()
                        .filter(entry -> entry.partition().getInt(0) == 1)
                        .findFirst()
                        .get();
        Set<Path> unchangedManifests = new HashSet<>();
        for (ManifestFileMeta manifest :
                table.store().manifestListFactory().create().readDataManifests(before)) {
            unchangedManifests.add(
                    table.store().pathFactory().toManifestFilePath(manifest.fileName()));
        }
        IcebergPathFactory pathFactory =
                new IcebergPathFactory(IcebergCommitCallback.catalogTableMetadataPath(table));
        IcebergMetadata metadata =
                IcebergMetadata.fromPath(fileIO, pathFactory.toMetadataPath(before.id()));
        for (IcebergManifestFileMeta manifest :
                IcebergManifestList.create(table, pathFactory)
                        .read(metadata.currentSnapshot().manifestList())) {
            if (manifest.content() == IcebergManifestFileMeta.Content.DATA) {
                unchangedManifests.add(new Path(manifest.manifestPath()));
            }
        }

        // Stage a real index-only overwrite with the mirror disabled, then isolate the
        // callback's reads from the commit's own conflict checks and manifest preparation.
        FileStoreTable disabled =
                table.copy(
                        Collections.singletonMap(
                                IcebergOptions.METADATA_ICEBERG_STORAGE.key(), "disabled"));
        Bitmap64DeletionVector deletionVector = new Bitmap64DeletionVector();
        deletionVector.delete(0);
        IndexFileMeta indexFile =
                disabled.store()
                        .newIndexFileHandler()
                        .dvIndex(deletedFrom.partition(), deletedFrom.bucket())
                        .writeSingleFile(
                                Collections.singletonMap(
                                        deletedFrom.file().fileName(), deletionVector));
        CommitMessage message =
                new CommitMessageImpl(
                        deletedFrom.partition(),
                        deletedFrom.bucket(),
                        null,
                        DataIncrement.indexIncrement(Collections.singletonList(indexFile)),
                        CompactIncrement.emptyIncrement());
        try (TableCommitImpl commit = disabled.newCommit(commitUser)) {
            commit.commit(2, Collections.singletonList(message));
        }

        // Reload to avoid satisfying a full scan from the preparation's manifest cache.
        FileStoreTable reloaded = FileStoreTableFactory.create(fileIO, table.location());
        Snapshot after = reloaded.snapshotManager().latestSnapshot();
        assertThat(after.commitKind()).isEqualTo(Snapshot.CommitKind.OVERWRITE);
        assertThat(reloaded.store().manifestListFactory().create().readDeltaManifests(after))
                .isEmpty();
        fileIO.watchFiles(unchangedManifests);
        try (IcebergCommitCallback callback = new IcebergCommitCallback(reloaded, commitUser)) {
            if (retry) {
                callback.retry(new ManifestCommittable(after.commitIdentifier()));
            } else {
                callback.call(
                        new CommitCallback.Context(
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Collections.singletonList(
                                        new IndexManifestEntry(
                                                FileKind.ADD,
                                                deletedFrom.partition(),
                                                deletedFrom.bucket(),
                                                indexFile)),
                                after,
                                after.commitIdentifier()));
            }
        }
        fileIO.watchFiles(Collections.emptySet());

        assertThat(getPaimonResult(reloaded))
                .containsExactlyInAnyOrder(
                        GenericRow.of(1, 20), GenericRow.of(2, 30), GenericRow.of(2, 40));
        assertThat(getIcebergFiles(reloaded, after.id()))
                .filteredOn(file -> file.content() == IcebergDataFileMeta.Content.POSITION_DELETES)
                .singleElement()
                .satisfies(file -> assertThat(file.recordCount()).isEqualTo(1));
        assertThat(fileIO.readFiles)
                .as("DV-only publication must not scan unchanged Paimon or Iceberg data manifests")
                .isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testRepairProcedureRetryRecoversFailedIcebergMetadataWrite(boolean missingBase)
            throws Exception {
        assertRepairProcedureRetryRecoversFailedPublication(false, missingBase);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testRepairProcedureRetryRecoversFailedIcebergCatalogCommit(boolean missingBase)
            throws Exception {
        RecordingIcebergMetadataCommitter.COMMITS.clear();
        RecordingIcebergMetadataCommitter.BASES.clear();
        try {
            assertRepairProcedureRetryRecoversFailedPublication(true, missingBase);
        } finally {
            RecordingIcebergMetadataCommitter.failNextCommit = false;
            RecordingIcebergMetadataCommitter.COMMITS.clear();
            RecordingIcebergMetadataCommitter.BASES.clear();
        }
    }

    private void assertRepairProcedureRetryRecoversFailedPublication(
            boolean failCatalogCommit, boolean missingBase) throws Exception {
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
        long publishedSnapshotId;
        try (TableWriteImpl<?> write = table.newWrite(commitUser);
                TableCommitImpl commit = table.newCommit(commitUser)) {
            write.write(GenericRow.of(1, 10));
            commit.commit(1, write.prepareCommit(false, 1));
            write.write(GenericRow.of(2, 20));
            commit.commit(2, write.prepareCommit(false, 2));
            publishedSnapshotId = table.snapshotManager().latestSnapshotId();
            if (missingBase) {
                write.write(GenericRow.of(3, 30));
                fileIO.failNextMetadataWrite = true;
                assertThatThrownBy(() -> commit.commit(3, write.prepareCommit(false, 3)))
                        .hasStackTraceContaining("injected Iceberg metadata write failure");
            }
        }

        Snapshot before = table.snapshotManager().latestSnapshot();
        List<ManifestFileMeta> manifests =
                table.store().newScan().manifestsReader().read(before, ScanMode.ALL).allManifests;
        assertThat(manifests).hasSize(missingBase ? 3 : 2);
        for (ManifestFileMeta manifest : manifests) {
            if (table.store().newScan().readManifest(manifest).stream()
                    .anyMatch(entry -> entry.partition().getInt(0) != 2)) {
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
                    .containsExactly(pathFactory.toMetadataPath(publishedSnapshotId));
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

    /** Tracks reads of selected manifests, including reads made by scan threads. */
    private static class ManifestReadTrackingFileIO extends LocalFileIO {

        private final Set<Path> readFiles = ConcurrentHashMap.newKeySet();
        private volatile Set<Path> watchedFiles = Collections.emptySet();

        private void watchFiles(Set<Path> paths) {
            watchedFiles = new HashSet<>(paths);
        }

        @Override
        public SeekableInputStream newInputStream(Path path) throws IOException {
            if (watchedFiles.contains(path)) {
                readFiles.add(path);
            }
            return super.newInputStream(path);
        }
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
