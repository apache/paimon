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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
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
import org.apache.paimon.table.sink.FixedBucketRowKeyExtractor;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
        // the v3 manifest list round-trips the first_row_id column; data manifests are assigned
        // at manifest-list write time (this table's single commit starts at row id 0)
        for (IcebergManifestFileMeta meta : metas) {
            if (meta.content() == IcebergManifestFileMeta.Content.DATA) {
                assertThat(meta.firstRowId()).isEqualTo(0L);
            } else {
                assertThat(meta.firstRowId()).isNull();
            }
        }
    }

    @Test
    public void testManifestFirstRowIdAssignment() throws Exception {
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

        IcebergPathFactory paths = new IcebergPathFactory(new Path(table.location(), "metadata"));
        IcebergManifestList manifestList = IcebergManifestList.create(table, paths);
        List<IcebergManifestFileMeta> metas =
                manifestList.read(
                        new Path(readIcebergMetadata(table, 2).currentSnapshot().manifestList())
                                .getName());

        // every data manifest is assigned; watermark walks addedRowsCount in list order
        long watermark = -1;
        long totalAssigned = 0;
        for (IcebergManifestFileMeta meta : metas) {
            if (meta.content() == IcebergManifestFileMeta.Content.DATA) {
                assertThat(meta.firstRowId()).isNotNull();
                assertThat(meta.firstRowId()).isGreaterThan(watermark);
                watermark = meta.firstRowId();
                totalAssigned += meta.addedRowsCount();
            } else {
                assertThat(meta.firstRowId()).isNull();
            }
        }
        // commit1 assigned rows [0,2), commit2 assigned [2,3): manifests carry 0 and 2
        assertThat(
                        metas.stream()
                                .filter(m -> m.content() == IcebergManifestFileMeta.Content.DATA)
                                .map(IcebergManifestFileMeta::firstRowId))
                .containsExactlyInAnyOrder(0L, 2L);
        assertThat(totalAssigned).isEqualTo(3L);
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
    public void testExpireKeepsManifestSharedAcrossRowIdAssignment() throws Exception {
        // A manifest written before manifest-level lineage is re-listed with an assigned
        // first_row_id but the same physical path. Expiring the pre-assignment manifest list
        // must not delete the shared file, so liveness is decided by path, not value equality.
        Map<String, String> options = formatVersionOptions(3);
        options.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(), "1");
        options.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(), "2");
        FileStoreTable table = createPaimonTable(defaultRowType(), options, "avro");
        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write =
                table.newWrite(commitUser)
                        .withIOManager(new IOManagerImpl(tempDir.toString() + "/tmp"));
        TableCommitImpl commit = table.newCommit(commitUser);

        write.write(GenericRow.of(1, 10));
        commit.commit(1, write.prepareCommit(false, 1));

        IcebergPathFactory paths = new IcebergPathFactory(new Path(table.location(), "metadata"));
        String listName =
                new Path(readIcebergMetadata(table, 1).currentSnapshot().manifestList()).getName();

        // strip column 520 from snapshot 1's manifest list to simulate a pre-assignment writer
        FileStoreTable v2SchemaView =
                table.copy(Collections.singletonMap(IcebergOptions.FORMAT_VERSION.key(), "2"));
        IcebergManifestList oldWriter = IcebergManifestList.create(v2SchemaView, paths);
        IcebergManifestList reader = IcebergManifestList.create(table, paths);
        String rewritten = oldWriter.writeWithoutRolling(reader.read(listName));
        LocalFileIO.create().deleteQuietly(paths.toManifestListPath(listName));
        LocalFileIO.create()
                .rename(paths.toManifestListPath(rewritten), paths.toManifestListPath(listName));
        List<String> sharedManifestPaths = new ArrayList<>();
        for (IcebergManifestFileMeta meta : reader.read(listName)) {
            sharedManifestPaths.add(meta.manifestPath());
        }
        assertThat(sharedManifestPaths).isNotEmpty();

        // commit 2 carries the manifest over, assigning first_row_id at the same path;
        // commit 3 expires snapshot 1's manifest list against snapshot 2's
        write.write(GenericRow.of(2, 20));
        commit.commit(2, write.prepareCommit(false, 2));
        write.write(GenericRow.of(3, 30));
        commit.commit(3, write.prepareCommit(false, 3));
        write.close();
        commit.close();

        IcebergMetadata metadata = readIcebergMetadata(table, 3);
        assertThat(metadata.snapshots()).hasSize(2);
        for (String manifestPath : sharedManifestPaths) {
            assertThat(LocalFileIO.create().exists(new Path(manifestPath))).isTrue();
        }
        // every retained snapshot must stay readable end to end
        IcebergManifestFile manifestFile = IcebergManifestFile.create(table, paths);
        for (IcebergSnapshot snapshot : metadata.snapshots()) {
            for (IcebergManifestFileMeta meta :
                    reader.read(new Path(snapshot.manifestList()).getName())) {
                assertThat(manifestFile.read(meta)).isNotEmpty();
            }
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

    @Test
    public void testFirstRowIdStableAcrossManifestRewrite() throws Exception {
        // A single-bucket table always rewrites its one-and-only file whole, so no entry ever
        // survives a rewrite unchanged (the same-path invariant this test targets never fires).
        // Use two buckets instead, and only touch one of them: the other bucket's file must
        // then be carried, byte-for-byte unchanged, as an EXISTING entry into the manifest that
        // gets rewritten because its sibling entry was removed.
        RowType rowType = defaultRowType();
        Map<String, String> customOptions = formatVersionOptions(3);
        customOptions.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        customOptions.put(CoreOptions.DELETION_VECTOR_BITMAP64.key(), "true");
        FileStoreTable table = createPkPaimonTable(rowType, customOptions);

        int keyBucket0 = findKeyForBucket(table, 0);
        int keyBucket1 = findKeyForBucket(table, 1);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write =
                table.newWrite(commitUser)
                        .withIOManager(new IOManagerImpl(tempDir.toString() + "/tmp"));
        TableCommitImpl commit = table.newCommit(commitUser);

        write.write(GenericRow.of(keyBucket0, 10));
        write.write(GenericRow.of(keyBucket1, 20));
        write.compact(BinaryRow.EMPTY_ROW, 0, true);
        write.compact(BinaryRow.EMPTY_ROW, 1, true);
        commit.commit(1, write.prepareCommit(true, 1));

        // an append bundled with a full compaction is committed as two separate physical
        // snapshots (append, then compact), so the Iceberg metadata id to inspect is whatever
        // snapshot id is actually latest now, not the external commit identifier used above
        Map<String, Long> idsBefore =
                effectiveFileFirstRowIds(table, table.snapshotManager().latestSnapshotId());
        assertThat(idsBefore).isNotEmpty();

        // delete the bucket-0 key and compact only bucket 0: bucket 1's file is left entirely
        // untouched, so the shared manifest is rewritten (bucket 0's entry removed) while
        // bucket 1's entry must survive, under the same file path, as a materialized EXISTING
        // entry
        write.write(GenericRow.ofKind(RowKind.DELETE, keyBucket0, 10));
        write.compact(BinaryRow.EMPTY_ROW, 0, true);
        commit.commit(2, write.prepareCommit(true, 2));
        write.close();
        commit.close();

        Map<String, Long> idsAfter =
                effectiveFileFirstRowIds(table, table.snapshotManager().latestSnapshotId());
        assertThat(idsAfter).isNotEmpty();
        boolean checkedAtLeastOneSurvivor = false;
        for (Map.Entry<String, Long> e : idsAfter.entrySet()) {
            Long before = idsBefore.get(e.getKey());
            if (before != null) {
                checkedAtLeastOneSurvivor = true;
                // a file carried across the rewrite keeps its effective first row id
                assertThat(e.getValue()).as("file %s", e.getKey()).isEqualTo(before);
            }
        }
        assertThat(checkedAtLeastOneSurvivor)
                .as("expected bucket 1's file to survive the manifest rewrite")
                .isTrue();
    }

    @Test
    public void testGaReaderSeesAssignedManifests() throws Exception {
        assumeGaRowLineageReader();
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

        HadoopCatalog icebergCatalog = new HadoopCatalog(new Configuration(), tempDir.toString());
        Table icebergTable = icebergCatalog.loadTable(TableIdentifier.of("mydb.db", "t"));
        assertThat(icebergTable.currentSnapshot().firstRowId()).isEqualTo(0L);
        for (ManifestFile manifest :
                icebergTable.currentSnapshot().dataManifests(icebergTable.io())) {
            assertThat(manifestFirstRowId(manifest)).isNotNull();
        }
    }

    @Test
    public void testLayer1TableUpgradesOnNextCommit() throws Exception {
        FileStoreTable table = createPaimonTable(defaultRowType(), formatVersionOptions(3), "avro");
        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write =
                table.newWrite(commitUser)
                        .withIOManager(new IOManagerImpl(tempDir.toString() + "/tmp"));
        TableCommitImpl commit = table.newCommit(commitUser);
        write.write(GenericRow.of(1, 10));
        commit.commit(1, write.prepareCommit(false, 1));

        // simulate a Layer-1-written manifest list: strip the assigned first_row_id
        IcebergPathFactory paths = new IcebergPathFactory(new Path(table.location(), "metadata"));
        IcebergManifestList manifestList = IcebergManifestList.create(table, paths);
        String listName =
                new Path(readIcebergMetadata(table, 1).currentSnapshot().manifestList()).getName();
        List<IcebergManifestFileMeta> stripped = new ArrayList<>();
        for (IcebergManifestFileMeta meta : manifestList.read(listName)) {
            stripped.add(
                    new IcebergManifestFileMeta(
                            meta.manifestPath(),
                            meta.manifestLength(),
                            meta.partitionSpecId(),
                            meta.content(),
                            meta.sequenceNumber(),
                            meta.minSequenceNumber(),
                            meta.addedSnapshotId(),
                            meta.addedFilesCount(),
                            meta.existingFilesCount(),
                            meta.deletedFilesCount(),
                            meta.addedRowsCount(),
                            meta.existingRowsCount(),
                            meta.deletedRowsCount(),
                            meta.partitions(),
                            null));
        }
        // overwrite the manifest list in place with unassigned metas
        LocalFileIO.create().deleteQuietly(paths.toManifestListPath(listName));
        // writeWithoutRolling creates a new file; rename it over the original
        String rewritten = manifestList.writeWithoutRolling(stripped);
        LocalFileIO.create()
                .rename(paths.toManifestListPath(rewritten), paths.toManifestListPath(listName));
        assertThat(manifestList.read(listName).get(0).firstRowId()).isNull();
        // the stripped Layer-1 manifest has no existing/deleted entries, so its addedRowsCount()
        // is an exact count of the legacy rows it carries and still needs a real id for
        long legacyManifestRows = stripped.get(0).addedRowsCount();
        assertThat(legacyManifestRows).isEqualTo(1L);

        // next commit re-lists carried-over manifests: unassigned metas get assigned now
        write.write(GenericRow.of(2, 20));
        commit.commit(2, write.prepareCommit(false, 2));

        IcebergMetadata metadataAfterCommit2 = readIcebergMetadata(table, 2);
        IcebergSnapshot snapshotAfterCommit2 = metadataAfterCommit2.currentSnapshot();
        List<IcebergManifestFileMeta> dataManifestsAfterCommit2 = new ArrayList<>();
        for (IcebergManifestFileMeta meta :
                manifestList.read(new Path(snapshotAfterCommit2.manifestList()).getName())) {
            if (meta.content() == IcebergManifestFileMeta.Content.DATA) {
                assertThat(meta.firstRowId()).isNotNull();
                dataManifestsAfterCommit2.add(meta);
            }
        }
        // the re-assigned legacy manifest (M1) and this commit's freshly written manifest (M2)
        // stay separate: only 2 data manifests, well under the metadata-compaction threshold
        assertThat(dataManifestsAfterCommit2).hasSize(2);

        // this is the corruption scenario from the review: added-rows/next-row-id must count
        // the legacy manifest's re-assigned rows in addition to this commit's own new rows, not
        // just this commit's `metrics.addedRecords` (which is only the new row)
        long newRowsThisCommit = 1L;
        long expectedAddedRows = legacyManifestRows + newRowsThisCommit;
        assertThat(snapshotAfterCommit2.addedRows())
                .as("snapshot added-rows must include the re-assigned legacy manifest's rows")
                .isEqualTo(expectedAddedRows);
        assertThat(metadataAfterCommit2.nextRowId())
                .as("next-row-id must equal first-row-id plus the true assigned-rows total")
                .isEqualTo(snapshotAfterCommit2.firstRowId() + snapshotAfterCommit2.addedRows());
        for (IcebergManifestFileMeta meta : dataManifestsAfterCommit2) {
            assertThat(metadataAfterCommit2.nextRowId())
                    .as(
                            "next-row-id must be at or beyond every manifest's assigned range "
                                    + "(manifest %s)",
                            meta.manifestPath())
                    .isGreaterThanOrEqualTo(meta.firstRowId() + meta.addedRowsCount());
        }

        // a third commit: its first-row-id must continue exactly where commit 2 left off, and no
        // manifest's assigned id range may overlap another's (i.e. no file gets a duplicate
        // effective first-row-id)
        write.write(GenericRow.of(3, 30));
        commit.commit(3, write.prepareCommit(false, 3));
        write.close();
        commit.close();

        IcebergMetadata metadataAfterCommit3 = readIcebergMetadata(table, 3);
        IcebergSnapshot snapshotAfterCommit3 = metadataAfterCommit3.currentSnapshot();
        assertThat(snapshotAfterCommit3.firstRowId()).isEqualTo(metadataAfterCommit2.nextRowId());

        Map<String, Long> effectiveIdsAfterCommit3 = effectiveFileFirstRowIds(table, 3);
        assertThat(effectiveIdsAfterCommit3).hasSize(3);
        assertThat(new HashSet<>(effectiveIdsAfterCommit3.values()))
                .as("no two files may share an effective first-row-id")
                .hasSameSizeAs(effectiveIdsAfterCommit3.values());
    }

    @Test
    public void testRowTrackingTableUsesSyntheticIds() throws Exception {
        Map<String, String> options = formatVersionOptions(3);
        options.put(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        FileStoreTable table = createPaimonTable(defaultRowType(), options, "avro");
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

        // documented independence: assignment behaves exactly as for any other table
        IcebergMetadata metadata = readIcebergMetadata(table, 1);
        assertThat(metadata.nextRowId()).isEqualTo(2L);
        assertThat(effectiveFileFirstRowIds(table, 1).values()).containsExactly(0L);
    }

    @Test
    public void testCompactMetadataIfNeededMaterializesRowLineageUnderV3() throws Exception {
        // exercises the `compactMetadataIfNeeded` manifest-metadata-merge call site under v3,
        // which no existing test hits (all format-version-3 tests leave COMPACT_MIN_FILE_NUM
        // at its default of 10, and all tests that force compaction stay on format version 2).
        RowType rowType = defaultRowType();
        Map<String, String> customOptions = formatVersionOptions(3);
        customOptions.put(IcebergOptions.COMPACT_MIN_FILE_NUM.key(), "2");
        customOptions.put(IcebergOptions.COMPACT_MAX_FILE_NUM.key(), "2");
        // large enough that manifests are never excluded as "already big enough", so the
        // min/max file-count thresholds above are what actually triggers the merge
        customOptions.put(CoreOptions.MANIFEST_TARGET_FILE_SIZE.key(), "64 mb");
        FileStoreTable table = createPaimonTable(rowType, customOptions, "avro");

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write =
                table.newWrite(commitUser)
                        .withIOManager(new IOManagerImpl(tempDir.toString() + "/tmp"));
        TableCommitImpl commit = table.newCommit(commitUser);

        // commit 1: single manifest M1 (candidates=1 < COMPACT_MIN_FILE_NUM, no compaction)
        write.write(GenericRow.of(1, 10));
        write.write(GenericRow.of(2, 20));
        commit.commit(1, write.prepareCommit(false, 1));
        Map<String, Long> idsAfterCommit1 =
                effectiveFileFirstRowIds(table, table.snapshotManager().latestSnapshotId());
        assertThat(idsAfterCommit1).hasSize(1);

        // commit 2: M1 (already assigned) + fresh M2 => 2 candidates, meets both thresholds,
        // manifest metadata compaction merges them into a single manifest this same commit
        write.write(GenericRow.of(3, 30));
        commit.commit(2, write.prepareCommit(false, 2));
        assertThat(dataManifestCount(table, table.snapshotManager().latestSnapshotId()))
                .as("commit 2 should have merged M1+M2 into a single data manifest")
                .isEqualTo(1);
        Map<String, Long> idsAfterCommit2 =
                effectiveFileFirstRowIds(table, table.snapshotManager().latestSnapshotId());
        assertThat(idsAfterCommit2).hasSize(2);
        for (Map.Entry<String, Long> e : idsAfterCommit1.entrySet()) {
            // every file's effective first-row-id survives the metadata-compaction commit
            // unchanged, whether it was inherited or already explicit before the merge
            assertThat(idsAfterCommit2)
                    .as("file %s", e.getKey())
                    .containsEntry(e.getKey(), e.getValue());
        }
        assertMergedManifestExistingEntriesHaveExplicitFirstRowId(table);

        // commit 3: merges again, this time the base manifest already contains an
        // EXISTING entry with an explicit field 142 from commit 2's merge (file 1/2) sitting
        // alongside an ADDED entry with inherited-only field 142 (file from commit 2) -- this
        // is the "explicit-142 passthrough" branch of materializeFirstRowIds that no other
        // test reaches
        write.write(GenericRow.of(4, 40));
        commit.commit(3, write.prepareCommit(false, 3));
        write.close();
        commit.close();
        assertThat(dataManifestCount(table, table.snapshotManager().latestSnapshotId()))
                .as("commit 3 should merge again into a single data manifest")
                .isEqualTo(1);
        Map<String, Long> idsAfterCommit3 =
                effectiveFileFirstRowIds(table, table.snapshotManager().latestSnapshotId());
        assertThat(idsAfterCommit3).hasSize(3);
        for (Map.Entry<String, Long> e : idsAfterCommit2.entrySet()) {
            assertThat(idsAfterCommit3)
                    .as("file %s", e.getKey())
                    .containsEntry(e.getKey(), e.getValue());
        }
        assertMergedManifestExistingEntriesHaveExplicitFirstRowId(table);
    }

    @Test
    public void testGaReaderResolvesPerFileRowLineage() throws Exception {
        assumeGaRowLineageReader();
        // standard two-commit 2+1-row setup (matches testNextRowIdAdvancesAcrossCommits):
        // commit 1's file gets effective first-row-id 0, commit 2's file gets 2
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

        HadoopCatalog icebergCatalog = new HadoopCatalog(new Configuration(), tempDir.toString());
        Table icebergTable = icebergCatalog.loadTable(TableIdentifier.of("mydb.db", "t"));

        // Attempt 1: resolve a per-row `_row_id` value through iceberg-data's GA generics
        // reader. `select(...)` does not throw, but the requested metadata column is silently
        // dropped from the projected schema: GenericReader/InternalRecordWrapper in
        // iceberg-data 1.11.0 have no wiring for MetadataColumns.ROW_ID (unlike _pos/_file/
        // _deleted/_spec_id, which the same reader stack does resolve), so every record's
        // "_row_id" field comes back null instead of the assigned/inherited value. This is
        // verified here rather than assumed: if a future Iceberg release adds real support,
        // this loop will start observing non-null values and the assertion below must change.
        boolean anyRowIdResolved = false;
        try (CloseableIterable<Record> records =
                IcebergGenerics.read(icebergTable).select("k", "v", "_row_id").build()) {
            for (Record record : records) {
                if (record.getField("_row_id") != null) {
                    anyRowIdResolved = true;
                }
            }
        }
        assertThat(anyRowIdResolved)
                .as(
                        "iceberg-data 1.11.0 GA generics do not resolve the _row_id metadata "
                                + "column; if this starts failing, generics gained support and "
                                + "the fallback below can be simplified/removed")
                .isFalse();

        // Fallback: GA's ManifestFiles/DataFile APIs DO resolve the per-file first_row_id
        // (including inheritance from the manifest-level value), so assert actual values,
        // not just non-nullity.
        List<Long> resolvedFirstRowIds = new ArrayList<>();
        for (ManifestFile manifest :
                icebergTable.currentSnapshot().dataManifests(icebergTable.io())) {
            try (ManifestReader<DataFile> reader =
                    ManifestFiles.read(manifest, icebergTable.io(), icebergTable.specs())) {
                for (DataFile file : reader) {
                    resolvedFirstRowIds.add(dataFileFirstRowId(file));
                }
            }
        }
        assertThat(resolvedFirstRowIds).containsExactlyInAnyOrder(0L, 2L);
    }

    @Test
    public void testV2ManifestListSchemaHasNoFirstRowIdColumn() throws Exception {
        // pins the v2 manifest-list shape: 14 columns, no first_row_id anywhere, so a future
        // change to the v3 schema construction cannot silently leak into v2's byte-identical
        // output
        assertThat(IcebergManifestFileMeta.schema(false).getFieldCount()).isEqualTo(14);
        assertThat(IcebergManifestFileMeta.schema(false).getFields().stream().map(DataField::name))
                .doesNotContain("first_row_id");
        assertThat(IcebergManifestFileMeta.schema(true).getFieldCount()).isEqualTo(14);
        assertThat(IcebergManifestFileMeta.schema(true).getFields().stream().map(DataField::name))
                .doesNotContain("first_row_id");

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

        // the written manifest-list file's raw bytes must not contain "first_row_id" anywhere;
        // Avro embeds the writer schema as JSON in the file header, so a plain byte-scan of the
        // whole file is a valid (and stronger-than-parsed) check of the physical shape
        Path manifestListPath =
                new Path(readIcebergMetadata(table, 1).currentSnapshot().manifestList());
        byte[] bytes;
        try (SeekableInputStream in = table.fileIO().newInputStream(manifestListPath)) {
            bytes = IOUtils.readFully(in, false);
        }
        String content = new String(bytes, StandardCharsets.ISO_8859_1);
        assertThat(content).doesNotContain("first_row_id");
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

    private FileStoreTable createPkPaimonTable(RowType rowType, Map<String, String> customOptions)
            throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path path = new Path(tempDir.toString());
        Options options = new Options(customOptions);
        // two fixed buckets so a manifest rewrite can leave one bucket's file untouched
        // (see testFirstRowIdStableAcrossManifestRewrite)
        options.set(CoreOptions.BUCKET, 2);
        options.set(
                IcebergOptions.METADATA_ICEBERG_STORAGE, IcebergOptions.StorageType.TABLE_LOCATION);
        options.set(CoreOptions.FILE_FORMAT, "avro");
        Schema schema =
                new Schema(
                        rowType.getFields(),
                        Collections.<String>emptyList(),
                        Collections.singletonList("k"),
                        options.toMap(),
                        "");
        try (FileSystemCatalog paimonCatalog = new FileSystemCatalog(fileIO, path)) {
            paimonCatalog.createDatabase("mydb2", false);
            Identifier id = Identifier.create("mydb2", "t");
            paimonCatalog.createTable(id, schema, false);
            return (FileStoreTable) paimonCatalog.getTable(id);
        }
    }

    /** Finds the smallest positive key whose fixed-bucket hash lands in {@code targetBucket}. */
    private int findKeyForBucket(FileStoreTable table, int targetBucket) {
        FixedBucketRowKeyExtractor extractor = new FixedBucketRowKeyExtractor(table.schema());
        for (int k = 1; k < 10_000; k++) {
            extractor.setRecord(GenericRow.of(k, 0));
            if (extractor.bucket() == targetBucket) {
                return k;
            }
        }
        throw new IllegalStateException("No key found for bucket " + targetBucket);
    }

    /** Effective per-file first row id: explicit field 142, or inherited per the spec rules. */
    private Map<String, Long> effectiveFileFirstRowIds(FileStoreTable table, long snapshotId)
            throws Exception {
        IcebergPathFactory paths = new IcebergPathFactory(new Path(table.location(), "metadata"));
        IcebergManifestList manifestList = IcebergManifestList.create(table, paths);
        IcebergManifestFile manifestFile = IcebergManifestFile.create(table, paths);
        Map<String, Long> result = new HashMap<>();
        for (IcebergManifestFileMeta meta :
                manifestList.read(
                        new Path(
                                        readIcebergMetadata(table, snapshotId)
                                                .currentSnapshot()
                                                .manifestList())
                                .getName())) {
            if (meta.content() != IcebergManifestFileMeta.Content.DATA) {
                continue;
            }
            long watermark = meta.firstRowId() == null ? -1 : meta.firstRowId();
            for (IcebergManifestEntry entry : manifestFile.read(meta)) {
                long effective;
                if (entry.file().firstRowId() != null) {
                    effective = entry.file().firstRowId();
                } else {
                    effective = watermark;
                    watermark += entry.file().recordCount();
                }
                if (entry.isLive()) {
                    result.put(entry.file().filePath(), effective);
                }
            }
        }
        return result;
    }

    /** Number of DATA-content manifests referenced by the given snapshot's manifest list. */
    private int dataManifestCount(FileStoreTable table, long snapshotId) throws Exception {
        return dataManifestMetas(table, snapshotId).size();
    }

    private List<IcebergManifestFileMeta> dataManifestMetas(FileStoreTable table, long snapshotId)
            throws Exception {
        IcebergPathFactory paths = new IcebergPathFactory(new Path(table.location(), "metadata"));
        IcebergManifestList manifestList = IcebergManifestList.create(table, paths);
        List<IcebergManifestFileMeta> result = new ArrayList<>();
        for (IcebergManifestFileMeta meta :
                manifestList.read(
                        new Path(
                                        readIcebergMetadata(table, snapshotId)
                                                .currentSnapshot()
                                                .manifestList())
                                .getName())) {
            if (meta.content() == IcebergManifestFileMeta.Content.DATA) {
                result.add(meta);
            }
        }
        return result;
    }

    /**
     * After a manifest-metadata-compaction merge, every EXISTING entry in the merged manifest(s)
     * for the table's current snapshot must carry an explicit (non-null) field 142: EXISTING
     * entries are, by definition, carried-over/rewritten entries, so their first_row_id must have
     * been materialized by {@code materializeFirstRowIds} rather than left to be inherited from the
     * (now-merged-away) original manifest.
     */
    private void assertMergedManifestExistingEntriesHaveExplicitFirstRowId(FileStoreTable table)
            throws Exception {
        IcebergPathFactory paths = new IcebergPathFactory(new Path(table.location(), "metadata"));
        IcebergManifestFile manifestFile = IcebergManifestFile.create(table, paths);
        long snapshotId = table.snapshotManager().latestSnapshotId();
        boolean checkedAtLeastOneExistingEntry = false;
        for (IcebergManifestFileMeta meta : dataManifestMetas(table, snapshotId)) {
            for (IcebergManifestEntry entry : manifestFile.read(meta)) {
                if (entry.status() == IcebergManifestEntry.Status.EXISTING) {
                    checkedAtLeastOneExistingEntry = true;
                    assertThat(entry.file().firstRowId())
                            .as("EXISTING entry for file %s", entry.file().filePath())
                            .isNotNull();
                }
            }
        }
        assertThat(checkedAtLeastOneExistingEntry)
                .as("expected at least one materialized EXISTING entry after the merge")
                .isTrue();
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

    /**
     * Iceberg exposes per-manifest / per-file {@code firstRowId()} only from the GA row-lineage
     * line (1.10+). The module compiles against 1.8.1 by default, so GA reader assertions look the
     * method up reflectively and the tests skip when the API is absent (run with -Piceberg-ga).
     */
    private static final boolean GA_ROW_LINEAGE_READER = detectGaRowLineageReader();

    private static boolean detectGaRowLineageReader() {
        try {
            ManifestFile.class.getMethod("firstRowId");
            return true;
        } catch (NoSuchMethodException e) {
            return false;
        }
    }

    private static void assumeGaRowLineageReader() {
        Assumptions.assumeTrue(
                GA_ROW_LINEAGE_READER,
                "Iceberg on the test classpath predates GA row lineage; run with -Piceberg-ga");
    }

    private static Long manifestFirstRowId(ManifestFile manifest) {
        try {
            return (Long) ManifestFile.class.getMethod("firstRowId").invoke(manifest);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    private static Long dataFileFirstRowId(DataFile file) {
        try {
            return (Long) DataFile.class.getMethod("firstRowId").invoke(file);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }
}
