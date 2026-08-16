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

package org.apache.paimon.core;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.iceberg.IcebergOptions;
import org.apache.paimon.iceberg.IcebergPathFactory;
import org.apache.paimon.iceberg.manifest.IcebergManifestEntry;
import org.apache.paimon.iceberg.manifest.IcebergManifestFile;
import org.apache.paimon.iceberg.manifest.IcebergManifestFileMeta;
import org.apache.paimon.iceberg.manifest.IcebergManifestList;
import org.apache.paimon.iceberg.metadata.IcebergMetadata;
import org.apache.paimon.iceberg.metadata.IcebergSnapshot;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for Iceberg format-version 3 row-lineage metadata fields. */
public class IcebergRowLineageCompatibilityTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testFreshV3MetadataHasRowLineageFields() throws Exception {
        FileStoreTable table = createPaimonTable(defaultRowType(), formatVersionOptions(3), "avro");
        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write =
                table.newWrite(commitUser)
                        .withIOManager(new IOManagerImpl(tempDir.toString() + "/tmp"));
        TableCommitImpl commit = table.newCommit(commitUser);

        write.write(GenericRow.of(1, 10));
        write.write(GenericRow.of(2, 20));
        commit.commit(1, write.prepareCommit(false, 1));
        write.close();
        commit.close();

        IcebergMetadata metadata = readIcebergMetadata(table, 1);
        assertThat(metadata.formatVersion()).isEqualTo(3);
        assertThat(metadata.nextRowId()).isEqualTo(2L);
        IcebergSnapshot snapshot = metadata.currentSnapshot();
        assertThat(snapshot.firstRowId()).isEqualTo(0L);
        assertThat(snapshot.addedRows()).isEqualTo(2L);

        // the bundled Iceberg parser must still accept the metadata
        TableMetadata parsed = TableMetadataParser.fromJson(readMetadataJson(table, 1));
        assertThat(parsed.formatVersion()).isEqualTo(3);
    }

    @Test
    public void testV2MetadataHasNoRowLineageFields() throws Exception {
        FileStoreTable table = createPaimonTable(defaultRowType(), formatVersionOptions(2), "avro");
        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write =
                table.newWrite(commitUser)
                        .withIOManager(new IOManagerImpl(tempDir.toString() + "/tmp"));
        TableCommitImpl commit = table.newCommit(commitUser);

        write.write(GenericRow.of(1, 10));
        commit.commit(1, write.prepareCommit(false, 1));
        write.close();
        commit.close();

        String json = readMetadataJson(table, 1);
        assertThat(json)
                .doesNotContain("next-row-id")
                .doesNotContain("first-row-id")
                .doesNotContain("added-rows");
        IcebergMetadata metadata = IcebergMetadata.fromJson(json);
        assertThat(metadata.nextRowId()).isNull();
        assertThat(metadata.currentSnapshot().firstRowId()).isNull();
        assertThat(metadata.currentSnapshot().addedRows()).isNull();
    }

    @Test
    public void testNextRowIdAdvancesAcrossCommits() throws Exception {
        FileStoreTable table = createPaimonTable(defaultRowType(), formatVersionOptions(3), "avro");
        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write =
                table.newWrite(commitUser)
                        .withIOManager(new IOManagerImpl(tempDir.toString() + "/tmp"));
        TableCommitImpl commit = table.newCommit(commitUser);

        write.write(GenericRow.of(1, 10));
        write.write(GenericRow.of(2, 20));
        commit.commit(1, write.prepareCommit(false, 1));

        write.write(GenericRow.of(3, 30));
        commit.commit(2, write.prepareCommit(false, 2));
        write.close();
        commit.close();

        IcebergMetadata metadata = readIcebergMetadata(table, 2);
        assertThat(metadata.nextRowId()).isEqualTo(3L);
        IcebergSnapshot snapshot = metadata.currentSnapshot();
        assertThat(snapshot.firstRowId()).isEqualTo(2L);
        assertThat(snapshot.addedRows()).isEqualTo(1L);

        TableMetadata parsed = TableMetadataParser.fromJson(readMetadataJson(table, 2));
        assertThat(parsed.formatVersion()).isEqualTo(3);
    }

    @Test
    public void testRegenerateWhenBaseHasNoNextRowId() throws Exception {
        FileStoreTable table = createPaimonTable(defaultRowType(), formatVersionOptions(3), "avro");
        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write =
                table.newWrite(commitUser)
                        .withIOManager(new IOManagerImpl(tempDir.toString() + "/tmp"));
        TableCommitImpl commit = table.newCommit(commitUser);

        write.write(GenericRow.of(1, 10));
        write.write(GenericRow.of(2, 20));
        commit.commit(1, write.prepareCommit(false, 1));

        // Strip next-row-id from the committed metadata, simulating a v3 file written by
        // Paimon before this fix.
        IcebergMetadata base = readIcebergMetadata(table, 1);
        IcebergMetadata stripped =
                new IcebergMetadata(
                        base.formatVersion(),
                        base.tableUuid(),
                        base.location(),
                        base.lastSequenceNumber(),
                        base.lastColumnId(),
                        base.schemas(),
                        base.currentSchemaId(),
                        base.partitionSpecs(),
                        base.lastPartitionId(),
                        base.snapshots(),
                        base.currentSnapshotId(),
                        null,
                        base.refs());
        LocalFileIO.create().overwriteFileUtf8(metadataPath(table, 1), stripped.toJson());
        assertThat(readIcebergMetadata(table, 1).nextRowId()).isNull();

        write.write(GenericRow.of(3, 30));
        commit.commit(2, write.prepareCommit(false, 2));
        write.close();
        commit.close();

        // metadata must be regenerated from scratch with valid lineage fields
        IcebergMetadata regenerated = readIcebergMetadata(table, 2);
        assertThat(regenerated.nextRowId()).isEqualTo(3L);
        assertThat(regenerated.snapshots()).hasSize(1);
        assertThat(regenerated.currentSnapshot().firstRowId()).isEqualTo(0L);
        assertThat(regenerated.currentSnapshot().addedRows()).isEqualTo(3L);
    }

    @Test
    public void testTagPreservesNextRowId() throws Exception {
        FileStoreTable table = createPaimonTable(defaultRowType(), formatVersionOptions(3), "avro");
        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write =
                table.newWrite(commitUser)
                        .withIOManager(new IOManagerImpl(tempDir.toString() + "/tmp"));
        TableCommitImpl commit = table.newCommit(commitUser);

        write.write(GenericRow.of(1, 10));
        write.write(GenericRow.of(2, 20));
        commit.commit(1, write.prepareCommit(false, 1));

        write.write(GenericRow.of(3, 30));
        commit.commit(2, write.prepareCommit(false, 2));
        write.close();
        commit.close();

        table.createTag("t1", 1);

        IcebergMetadata metadata = readIcebergMetadata(table, 2);
        assertThat(metadata.refs()).containsKey("t1");
        assertThat(metadata.nextRowId()).isEqualTo(3L);
    }

    @Test
    public void testManifestListCarriesFirstRowIdColumn() throws Exception {
        FileStoreTable table = createPaimonTable(defaultRowType(), formatVersionOptions(3), "avro");
        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write =
                table.newWrite(commitUser)
                        .withIOManager(new IOManagerImpl(tempDir.toString() + "/tmp"));
        TableCommitImpl commit = table.newCommit(commitUser);
        write.write(GenericRow.of(1, 10));
        write.write(GenericRow.of(2, 20));
        commit.commit(1, write.prepareCommit(false, 1));
        write.close();
        commit.close();

        // the v3 manifest list must round-trip the first_row_id column
        IcebergPathFactory paths = new IcebergPathFactory(new Path(table.location(), "metadata"));
        IcebergManifestList manifestList = IcebergManifestList.create(table, paths);
        List<IcebergManifestFileMeta> metas =
                manifestList.read(
                        new Path(readIcebergMetadata(table, 1).currentSnapshot().manifestList())
                                .getName());
        assertThat(metas).isNotEmpty();
        // Task 2 only adds the column; assignment arrives in Task 4 — value still null here
        for (IcebergManifestFileMeta meta : metas) {
            assertThat(meta.firstRowId()).isNull();
        }
    }

    @Test
    public void testReadsManifestListWrittenWithoutFirstRowIdColumn() throws Exception {
        // A Layer-1 manifest list physically lacks column 520. The v3 reader must resolve
        // the missing column to null (Avro schema resolution), not fail.
        FileStoreTable table = createPaimonTable(defaultRowType(), formatVersionOptions(3), "avro");
        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write =
                table.newWrite(commitUser)
                        .withIOManager(new IOManagerImpl(tempDir.toString() + "/tmp"));
        TableCommitImpl commit = table.newCommit(commitUser);
        write.write(GenericRow.of(1, 10));
        commit.commit(1, write.prepareCommit(false, 1));
        write.close();
        commit.close();

        IcebergPathFactory paths = new IcebergPathFactory(new Path(table.location(), "metadata"));
        String listName =
                new Path(readIcebergMetadata(table, 1).currentSnapshot().manifestList()).getName();

        // rewrite the manifest list through the OLD (14-column) serializer to simulate Layer 1
        FileStoreTable v2SchemaView =
                table.copy(Collections.singletonMap(IcebergOptions.FORMAT_VERSION.key(), "2"));
        IcebergManifestList oldWriter = IcebergManifestList.create(v2SchemaView, paths);
        IcebergManifestList newReader = IcebergManifestList.create(table, paths);
        List<IcebergManifestFileMeta> metas = newReader.read(listName);
        String rewritten = oldWriter.writeWithoutRolling(metas);
        LocalFileIO.create().deleteQuietly(paths.toManifestListPath(listName));
        LocalFileIO.create()
                .rename(paths.toManifestListPath(rewritten), paths.toManifestListPath(listName));

        // the v3 reader resolves the absent column to null for every meta
        for (IcebergManifestFileMeta meta : newReader.read(listName)) {
            assertThat(meta.firstRowId()).isNull();
        }
    }

    @Test
    public void testManifestEntriesCarryFirstRowIdColumn() throws Exception {
        FileStoreTable table = createPaimonTable(defaultRowType(), formatVersionOptions(3), "avro");
        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write =
                table.newWrite(commitUser)
                        .withIOManager(new IOManagerImpl(tempDir.toString() + "/tmp"));
        TableCommitImpl commit = table.newCommit(commitUser);
        write.write(GenericRow.of(1, 10));
        commit.commit(1, write.prepareCommit(false, 1));
        write.close();
        commit.close();

        IcebergPathFactory paths = new IcebergPathFactory(new Path(table.location(), "metadata"));
        IcebergManifestList manifestList = IcebergManifestList.create(table, paths);
        IcebergManifestFile manifestFile = IcebergManifestFile.create(table, paths);
        List<IcebergManifestFileMeta> metas =
                manifestList.read(
                        new Path(readIcebergMetadata(table, 1).currentSnapshot().manifestList())
                                .getName());
        for (IcebergManifestFileMeta meta : metas) {
            for (IcebergManifestEntry entry : manifestFile.read(meta)) {
                // ADDED entries are unassigned by definition; the column must round-trip as null
                assertThat(entry.file().firstRowId()).isNull();
            }
        }
    }

    // ------------------------------------------------------------------------
    //  helpers
    // ------------------------------------------------------------------------

    private RowType defaultRowType() {
        return RowType.of(
                new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
    }

    private Map<String, String> formatVersionOptions(int formatVersion) {
        Map<String, String> options = new HashMap<>();
        options.put(IcebergOptions.FORMAT_VERSION.key(), String.valueOf(formatVersion));
        return options;
    }

    @Test
    public void testRollbackDoesNotReuseRowIds() throws Exception {
        FileStoreTable table = createPaimonTable(defaultRowType(), formatVersionOptions(3), "avro");
        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write =
                table.newWrite(commitUser)
                        .withIOManager(new IOManagerImpl(tempDir.toString() + "/tmp"));
        TableCommitImpl commit = table.newCommit(commitUser);
        write.write(GenericRow.of(1, 10));
        write.write(GenericRow.of(2, 20));
        commit.commit(1, write.prepareCommit(false, 1));
        write.write(GenericRow.of(3, 30));
        commit.commit(2, write.prepareCommit(false, 2));
        write.close();
        commit.close();
        assertThat(readIcebergMetadata(table, 2).nextRowId()).isEqualTo(3L);

        // deletes snapshot 2; its Iceberg metadata stays behind as an abandoned twin
        table.rollbackTo(1);

        // the next commit reuses snapshot id 2; ids 0..2 were already handed out by the
        // abandoned timeline, so the replacement snapshot must start above them
        TableWriteImpl<?> write2 =
                table.newWrite(commitUser)
                        .withIOManager(new IOManagerImpl(tempDir.toString() + "/tmp"));
        TableCommitImpl commit2 = table.newCommit(commitUser);
        write2.write(GenericRow.of(4, 40));
        commit2.commit(3, write2.prepareCommit(false, 3));
        write2.close();
        commit2.close();

        IcebergMetadata regenerated =
                readIcebergMetadata(table, table.snapshotManager().latestSnapshotId());
        // the rollback triggers a from-scratch rebuild re-exporting all 3 live rows; they
        // start above the abandoned watermark instead of reusing ids 0..2
        assertThat(regenerated.currentSnapshot().firstRowId()).isEqualTo(3L);
        assertThat(regenerated.currentSnapshot().addedRows()).isEqualTo(3L);
        assertThat(regenerated.nextRowId()).isEqualTo(6L);
    }

    @Test
    public void testRollbackRegenerationWithoutBaseKeepsRowIdWatermark() throws Exception {
        FileStoreTable table = createPaimonTable(defaultRowType(), formatVersionOptions(3), "avro");
        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write =
                table.newWrite(commitUser)
                        .withIOManager(new IOManagerImpl(tempDir.toString() + "/tmp"));
        TableCommitImpl commit = table.newCommit(commitUser);
        write.write(GenericRow.of(1, 10));
        write.write(GenericRow.of(2, 20));
        commit.commit(1, write.prepareCommit(false, 1));
        write.write(GenericRow.of(3, 30));
        commit.commit(2, write.prepareCommit(false, 2));
        write.close();
        commit.close();

        table.rollbackTo(1);

        // no base metadata survives, so regeneration must run from scratch and still
        // respect the abandoned watermark
        LocalFileIO.create().deleteQuietly(metadataPath(table, 1));

        TableWriteImpl<?> write2 =
                table.newWrite(commitUser)
                        .withIOManager(new IOManagerImpl(tempDir.toString() + "/tmp"));
        TableCommitImpl commit2 = table.newCommit(commitUser);
        write2.write(GenericRow.of(4, 40));
        commit2.commit(3, write2.prepareCommit(false, 3));
        write2.close();
        commit2.close();

        IcebergMetadata regenerated =
                readIcebergMetadata(table, table.snapshotManager().latestSnapshotId());
        // the regenerated snapshot re-exports all 3 live rows starting at the watermark
        assertThat(regenerated.currentSnapshot().firstRowId()).isEqualTo(3L);
        assertThat(regenerated.currentSnapshot().addedRows()).isEqualTo(3L);
        assertThat(regenerated.nextRowId()).isEqualTo(6L);
    }

    private FileStoreTable createPaimonTable(
            RowType rowType, Map<String, String> customOptions, String fileFormat)
            throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path path = new Path(tempDir.toString());

        Options options = new Options(customOptions);
        options.set(CoreOptions.BUCKET, -1);
        options.set(
                IcebergOptions.METADATA_ICEBERG_STORAGE, IcebergOptions.StorageType.TABLE_LOCATION);
        options.set(CoreOptions.FILE_FORMAT, fileFormat);
        options.set(CoreOptions.TARGET_FILE_SIZE, MemorySize.ofKibiBytes(32));

        Schema schema =
                new Schema(
                        rowType.getFields(),
                        Collections.<String>emptyList(),
                        Collections.<String>emptyList(),
                        options.toMap(),
                        "");

        try (FileSystemCatalog paimonCatalog = new FileSystemCatalog(fileIO, path)) {
            paimonCatalog.createDatabase("mydb", false);
            Identifier paimonIdentifier = Identifier.create("mydb", "t");
            paimonCatalog.createTable(paimonIdentifier, schema, false);
            return (FileStoreTable) paimonCatalog.getTable(paimonIdentifier);
        }
    }

    private Path metadataPath(FileStoreTable table, long snapshotId) {
        return new Path(table.location(), String.format("metadata/v%d.metadata.json", snapshotId));
    }

    private IcebergMetadata readIcebergMetadata(FileStoreTable table, long snapshotId) {
        return IcebergMetadata.fromPath(LocalFileIO.create(), metadataPath(table, snapshotId));
    }

    private String readMetadataJson(FileStoreTable table, long snapshotId) throws Exception {
        return LocalFileIO.create().readFileUtf8(metadataPath(table, snapshotId));
    }
}
