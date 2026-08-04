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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.iceberg.metadata.IcebergMetadata;
import org.apache.paimon.iceberg.metadata.IcebergSnapshot;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link IcebergOptions#SYNC_FULL_HISTORY}: when Iceberg metadata is created from
 * scratch, the whole retained Paimon history is replayed instead of only the latest snapshot. See
 * <a href="https://github.com/apache/paimon/issues/6107">apache/paimon#6107</a>.
 */
public class IcebergSyncFullHistoryTest {

    @TempDir java.nio.file.Path tempDir;

    private static final String VERSION_HINT_FILENAME = "version-hint.text";

    private FileStoreTable table;
    private TableWriteImpl<?> write;
    private TableCommitImpl commit;
    private String commitUser;

    @Test
    public void testDefaultRebuildOnlyExposesLatestSnapshot() throws Exception {
        createAppendTableWithoutIceberg();
        writeCommit(1, GenericRow.of(1, 10));
        writeCommit(2, GenericRow.of(2, 20));
        writeCommit(3, GenericRow.of(3, 30));

        enableIceberg(false);
        writeCommit(4, GenericRow.of(4, 40));

        IcebergMetadata metadata = readMetadata(4);
        assertThat(metadata.snapshots()).hasSize(1);
        assertThat(metadata.currentSnapshotId()).isEqualTo(4);
        // even though it exposes only one Iceberg snapshot, it contains all live files
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrder(
                        "Record(1, 10)", "Record(2, 20)", "Record(3, 30)", "Record(4, 40)");
    }

    @Test
    public void testSyncFullHistoryReplaysRetainedSnapshots() throws Exception {
        createAppendTableWithoutIceberg();
        writeCommit(1, GenericRow.of(1, 10));
        writeCommit(2, GenericRow.of(2, 20));
        writeCommit(3, GenericRow.of(3, 30));
        table.createTag("tag-2", 2);

        enableIceberg(true);
        writeCommit(4, GenericRow.of(4, 40));

        IcebergMetadata metadata = readMetadata(4);
        assertThat(
                        metadata.snapshots().stream()
                                .map(IcebergSnapshot::snapshotId)
                                .collect(Collectors.toList()))
                .containsExactly(1L, 2L, 3L, 4L);
        assertThat(metadata.currentSnapshotId()).isEqualTo(4);

        // replayed snapshots keep the original Paimon commit timestamps
        for (IcebergSnapshot icebergSnapshot : metadata.snapshots()) {
            Snapshot paimonSnapshot =
                    table.snapshotManager().snapshot(icebergSnapshot.snapshotId());
            assertThat(icebergSnapshot.timestampMs()).isEqualTo(paimonSnapshot.timeMillis());
        }

        // a pre-existing tag becomes an Iceberg ref because its snapshot now exists
        assertThat(metadata.refs()).containsOnlyKeys("tag-2");
        assertThat(metadata.refs().get("tag-2").snapshotId()).isEqualTo(2);

        // only the final replay step publishes the version hint
        assertThat(
                        table.fileIO()
                                .readFileUtf8(
                                        new Path(
                                                table.location(),
                                                "metadata/" + VERSION_HINT_FILENAME)))
                .isEqualTo("4");

        // an Iceberg client can read the current state, time travel, and resolve the tag
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrder(
                        "Record(1, 10)", "Record(2, 20)", "Record(3, 30)", "Record(4, 40)");
        assertThat(
                        getIcebergResult(
                                icebergTable ->
                                        IcebergGenerics.read(icebergTable).useSnapshot(2).build(),
                                Record::toString))
                .containsExactlyInAnyOrder("Record(1, 10)", "Record(2, 20)");
        assertThat(
                        getIcebergResult(
                                icebergTable ->
                                        IcebergGenerics.read(icebergTable)
                                                .useSnapshot(
                                                        icebergTable
                                                                .refs()
                                                                .get("tag-2")
                                                                .snapshotId())
                                                .build(),
                                Record::toString))
                .containsExactlyInAnyOrder("Record(1, 10)", "Record(2, 20)");
    }

    @Test
    public void testInterruptedReplayResumesFromNewestMetadata() throws Exception {
        createAppendTableWithoutIceberg();
        writeCommit(1, GenericRow.of(1, 10));
        writeCommit(2, GenericRow.of(2, 20));
        writeCommit(3, GenericRow.of(3, 30));

        enableIceberg(true);
        writeCommit(4, GenericRow.of(4, 40));
        assertThat(readMetadata(4).snapshots()).hasSize(4);

        // Simulate an interrupted replay / failed Iceberg commit: the newest metadata is missing,
        // but earlier replay steps survived.
        IcebergPathFactory pathFactory =
                new IcebergPathFactory(new Path(table.location(), "metadata"));
        String metadata3Before = table.fileIO().readFileUtf8(pathFactory.toMetadataPath(3));
        table.fileIO().deleteQuietly(pathFactory.toMetadataPath(4));

        writeCommit(5, GenericRow.of(5, 50));

        IcebergMetadata metadata = readMetadata(5);
        assertThat(
                        metadata.snapshots().stream()
                                .map(IcebergSnapshot::snapshotId)
                                .collect(Collectors.toList()))
                .containsExactly(1L, 2L, 3L, 4L, 5L);
        // metadata of already-replayed snapshots is reused, not rebuilt
        assertThat(table.fileIO().readFileUtf8(pathFactory.toMetadataPath(3)))
                .isEqualTo(metadata3Before);
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrder(
                        "Record(1, 10)",
                        "Record(2, 20)",
                        "Record(3, 30)",
                        "Record(4, 40)",
                        "Record(5, 50)");
    }

    @Test
    public void testResumeRejectsBaseWithoutRetainedPrefix() throws Exception {
        createAppendTableWithoutIceberg();
        writeCommit(1, GenericRow.of(1, 10));
        writeCommit(2, GenericRow.of(2, 20));
        writeCommit(3, GenericRow.of(3, 30));

        // Iceberg was first enabled WITHOUT full history sync: the metadata only contains the
        // latest snapshot.
        enableIceberg(false);
        writeCommit(4, GenericRow.of(4, 40));
        assertThat(readMetadata(4).snapshots()).hasSize(1);

        // Full history sync is enabled later, and the newest metadata is lost (e.g. a failed
        // Iceberg commit). The single-snapshot metadata of snapshot 4 is NOT a valid replay
        // prefix: resuming from it would silently drop snapshots 1-3 forever.
        Map<String, String> options = new HashMap<>();
        options.put(IcebergOptions.SYNC_FULL_HISTORY.key(), "true");
        reopen(options);
        writeCommit(5, GenericRow.of(5, 50));
        IcebergPathFactory pathFactory =
                new IcebergPathFactory(new Path(table.location(), "metadata"));
        table.fileIO().deleteQuietly(pathFactory.toMetadataPath(5));

        writeCommit(6, GenericRow.of(6, 60));

        IcebergMetadata metadata = readMetadata(6);
        assertThat(
                        metadata.snapshots().stream()
                                .map(IcebergSnapshot::snapshotId)
                                .collect(Collectors.toList()))
                .containsExactly(1L, 2L, 3L, 4L, 5L, 6L);
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrder(
                        "Record(1, 10)",
                        "Record(2, 20)",
                        "Record(3, 30)",
                        "Record(4, 40)",
                        "Record(5, 50)",
                        "Record(6, 60)");
    }

    @Test
    public void testFormatVersionChangeRebuildsHistoryWithRowLineage() throws Exception {
        // Iceberg (v2) is enabled from the start, with full history sync on.
        Map<String, String> options = new HashMap<>();
        options.put(
                IcebergOptions.METADATA_ICEBERG_STORAGE.key(),
                IcebergOptions.StorageType.TABLE_LOCATION.toString());
        options.put(IcebergOptions.SYNC_FULL_HISTORY.key(), "true");
        options.put(IcebergOptions.METADATA_DELETE_AFTER_COMMIT.key(), "false");
        createAppendTable(options);
        writeCommit(1, GenericRow.of(1, 10));
        writeCommit(2, GenericRow.of(2, 20), GenericRow.of(3, 30));
        assertThat(readMetadata(2).formatVersion()).isEqualTo(IcebergMetadata.FORMAT_VERSION_V2);

        // Switching to format version 3 makes the v2 base unusable, which triggers a full-history
        // rebuild; the stale v2 metadata files must be cleaned up so the replay can write in their
        // place.
        Map<String, String> upgrade = new HashMap<>();
        upgrade.put(IcebergOptions.FORMAT_VERSION.key(), "3");
        reopen(upgrade);
        writeCommit(3, GenericRow.of(4, 40));

        IcebergMetadata metadata = readMetadata(3);
        assertThat(metadata.formatVersion()).isEqualTo(IcebergMetadata.FORMAT_VERSION_V3);
        assertThat(
                        metadata.snapshots().stream()
                                .map(IcebergSnapshot::snapshotId)
                                .collect(Collectors.toList()))
                .containsExactly(1L, 2L, 3L);

        // v3 row lineage accumulates consistently across the replayed history
        List<Long> firstRowIds = new ArrayList<>();
        for (IcebergSnapshot icebergSnapshot : metadata.snapshots()) {
            assertThat(icebergSnapshot.firstRowId()).isNotNull();
            assertThat(icebergSnapshot.addedRows()).isNotNull();
            firstRowIds.add(icebergSnapshot.firstRowId());
        }
        assertThat(firstRowIds).containsExactly(0L, 1L, 3L);
        assertThat(metadata.nextRowId()).isEqualTo(4);
    }

    // ------------------------------------------------------------------------
    //  Utils
    // ------------------------------------------------------------------------

    private void createAppendTableWithoutIceberg() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(
                IcebergOptions.METADATA_ICEBERG_STORAGE.key(),
                IcebergOptions.StorageType.DISABLED.toString());
        createAppendTable(options);
    }

    private void createAppendTable(Map<String, String> customOptions) throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});

        LocalFileIO fileIO = LocalFileIO.create();
        Path path = new Path(tempDir.toString());

        Options options = new Options(customOptions);
        options.set(CoreOptions.BUCKET, -1);
        options.set(CoreOptions.FILE_FORMAT, "avro");
        Schema schema =
                new Schema(
                        rowType.getFields(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        options.toMap(),
                        "");

        try (FileSystemCatalog paimonCatalog = new FileSystemCatalog(fileIO, path)) {
            paimonCatalog.createDatabase("mydb", false);
            Identifier paimonIdentifier = Identifier.create("mydb", "t");
            paimonCatalog.createTable(paimonIdentifier, schema, false);
            table = (FileStoreTable) paimonCatalog.getTable(paimonIdentifier);
        }

        commitUser = UUID.randomUUID().toString();
        write = table.newWrite(commitUser);
        commit = table.newCommit(commitUser);
    }

    private void enableIceberg(boolean syncFullHistory) throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(
                IcebergOptions.METADATA_ICEBERG_STORAGE.key(),
                IcebergOptions.StorageType.TABLE_LOCATION.toString());
        options.put(IcebergOptions.SYNC_FULL_HISTORY.key(), String.valueOf(syncFullHistory));
        options.put(IcebergOptions.METADATA_DELETE_AFTER_COMMIT.key(), "false");
        reopen(options);
    }

    private void reopen(Map<String, String> options) throws Exception {
        table = table.copy(options);
        write.close();
        write = table.newWrite(commitUser);
        commit.close();
        commit = table.newCommit(commitUser);
    }

    private void writeCommit(long identifier, GenericRow... rows) throws Exception {
        for (GenericRow row : rows) {
            write.write(row);
        }
        commit.commit(identifier, write.prepareCommit(false, identifier));
    }

    private IcebergMetadata readMetadata(long snapshotId) {
        return IcebergMetadata.fromPath(
                table.fileIO(),
                new Path(table.location(), "metadata/v" + snapshotId + ".metadata.json"));
    }

    private List<String> getIcebergResult() throws Exception {
        return getIcebergResult(
                icebergTable -> IcebergGenerics.read(icebergTable).build(), Record::toString);
    }

    private List<String> getIcebergResult(
            Function<org.apache.iceberg.Table, CloseableIterable<Record>> query,
            Function<Record, String> icebergRecordToString)
            throws Exception {
        HadoopCatalog icebergCatalog = new HadoopCatalog(new Configuration(), tempDir.toString());
        TableIdentifier icebergIdentifier = TableIdentifier.of("mydb.db", "t");
        org.apache.iceberg.Table icebergTable = icebergCatalog.loadTable(icebergIdentifier);
        CloseableIterable<Record> result = query.apply(icebergTable);
        List<String> actual = new ArrayList<>();
        for (Record record : result) {
            actual.add(icebergRecordToString.apply(record));
        }
        result.close();
        return actual;
    }
}
