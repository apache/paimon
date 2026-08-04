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
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.iceberg.IcebergOptions;
import org.apache.paimon.iceberg.IcebergPathFactory;
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
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for {@link IcebergOptions#SYNC_FULL_HISTORY} on an Iceberg v3 primary-key table
 * with deletion vectors: enabling Iceberg compatibility on a table that already has snapshots must
 * rebuild the full retained history with a consistent row-id space, readable (including time
 * travel) by a real Apache Iceberg client. See <a
 * href="https://github.com/apache/paimon/issues/6107">apache/paimon#6107</a>.
 */
public class IcebergFullHistoryCompatibilityTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testEnableOnExistingV3DvTableRebuildsHistory() throws Exception {
        FileStoreTable table = createPaimonTableWithoutIceberg();

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write =
                table.newWrite(commitUser)
                        .withIOManager(new IOManagerImpl(tempDir.toString() + "/tmp"));
        TableCommitImpl commit = table.newCommit(commitUser);

        write.write(row(RowKind.INSERT, 1, 1, "a"));
        write.write(row(RowKind.INSERT, 1, 2, "b"));
        commit.commit(1, write.prepareCommit(false, 1));

        write.compact(partition(1), 0, true);
        commit.commit(2, write.prepareCommit(true, 2));

        write.write(row(RowKind.INSERT, 1, 3, "c"));
        write.write(row(RowKind.DELETE, 1, 2, "b"));
        commit.commit(3, write.prepareCommit(false, 3));
        write.close();
        commit.close();

        // no Iceberg metadata was produced so far
        assertThat(table.fileIO().exists(new Path(table.location(), "metadata/v3.metadata.json")))
                .isFalse();

        // enable Iceberg v3 with full history sync; the next commit rebuilds everything
        Map<String, String> options = new HashMap<>();
        options.put(
                IcebergOptions.METADATA_ICEBERG_STORAGE.key(),
                IcebergOptions.StorageType.TABLE_LOCATION.toString());
        options.put(IcebergOptions.FORMAT_VERSION.key(), "3");
        options.put(IcebergOptions.SYNC_FULL_HISTORY.key(), "true");
        options.put(IcebergOptions.METADATA_DELETE_AFTER_COMMIT.key(), "false");
        table = table.copy(options);
        write =
                table.newWrite(commitUser)
                        .withIOManager(new IOManagerImpl(tempDir.toString() + "/tmp"));
        commit = table.newCommit(commitUser);

        // this compaction merges the delete and produces a deletion vector
        write.compact(partition(1), 0, false);
        commit.commit(4, write.prepareCommit(true, 4));
        write.close();
        commit.close();

        long latestSnapshotId = table.snapshotManager().latestSnapshotId();
        IcebergMetadata metadata =
                IcebergMetadata.fromPath(
                        table.fileIO(),
                        new Path(
                                table.location(),
                                "metadata/v" + latestSnapshotId + ".metadata.json"));

        // the whole retained history is exposed
        assertThat(metadata.formatVersion()).isEqualTo(IcebergMetadata.FORMAT_VERSION_V3);
        List<Long> snapshotIds =
                metadata.snapshots().stream()
                        .map(IcebergSnapshot::snapshotId)
                        .collect(Collectors.toList());
        assertThat(snapshotIds)
                .isEqualTo(
                        java.util.stream.LongStream.rangeClosed(1, latestSnapshotId)
                                .boxed()
                                .collect(Collectors.toList()));

        // the v3 row-id space accumulates monotonically across the replayed history
        Long previousFirstRowId = null;
        for (IcebergSnapshot icebergSnapshot : metadata.snapshots()) {
            assertThat(icebergSnapshot.firstRowId()).isNotNull();
            assertThat(icebergSnapshot.addedRows()).isNotNull();
            if (previousFirstRowId != null) {
                assertThat(icebergSnapshot.firstRowId()).isGreaterThanOrEqualTo(previousFirstRowId);
            }
            previousFirstRowId = icebergSnapshot.firstRowId();
        }
        assertThat(metadata.nextRowId()).isNotNull();

        // every replayed snapshot's data manifests carry a non-null first_row_id (required by
        // strict v3 readers like Snowflake)
        IcebergPathFactory paths = new IcebergPathFactory(new Path(table.location(), "metadata"));
        IcebergManifestList manifestList = IcebergManifestList.create(table, paths);
        for (IcebergSnapshot icebergSnapshot : metadata.snapshots()) {
            List<IcebergManifestFileMeta> metas =
                    manifestList.read(new Path(icebergSnapshot.manifestList()).getName());
            assertThat(
                            metas.stream()
                                    .filter(
                                            m ->
                                                    m.content()
                                                            == IcebergManifestFileMeta.Content
                                                                    .DATA))
                    .allMatch(m -> m.firstRowId() != null);
        }

        // a real Iceberg client sees the full history, reads the current state with the deletion
        // vector applied, and can time travel to a replayed snapshot
        HadoopCatalog icebergCatalog = new HadoopCatalog(new Configuration(), tempDir.toString());
        Table icebergTable = icebergCatalog.loadTable(TableIdentifier.of("mydb.db", "t"));
        assertThat(
                        java.util.stream.StreamSupport.stream(
                                        icebergTable.snapshots().spliterator(), false)
                                .count())
                .isEqualTo(latestSnapshotId);

        assertThat(readIceberg(icebergTable, null)).containsExactlyInAnyOrder("1|1|a", "1|3|c");
        // snapshot 2 is the first compaction: only the first two rows existed
        assertThat(readIceberg(icebergTable, 2L)).containsExactlyInAnyOrder("1|1|a", "1|2|b");
    }

    @Test
    public void testEnableOnUncompactedDvBucketExportsCompactedFiles() throws Exception {
        FileStoreTable table = createPaimonTableWithoutIceberg();

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write =
                table.newWrite(commitUser)
                        .withIOManager(new IOManagerImpl(tempDir.toString() + "/tmp"));
        TableCommitImpl commit = table.newCommit(commitUser);

        write.write(row(RowKind.INSERT, 1, 1, "a"));
        write.write(row(RowKind.INSERT, 1, 2, "b"));
        commit.commit(1, write.prepareCommit(false, 1));

        write.compact(partition(1), 0, true);
        commit.commit(2, write.prepareCommit(true, 2));

        write.write(row(RowKind.INSERT, 1, 3, "c"));
        write.write(row(RowKind.DELETE, 1, 2, "b"));
        commit.commit(3, write.prepareCommit(false, 3));

        // this compaction produces a deletion vector against the max level file
        write.compact(partition(1), 0, false);
        commit.commit(4, write.prepareCommit(true, 4));

        // a level-0 file on top of the compacted levels: the bucket's batch split is now NOT
        // raw-convertible (level-0 file + overlapping key ranges + an active deletion vector)
        write.write(row(RowKind.INSERT, 1, 4, "d"));
        commit.commit(5, write.prepareCommit(false, 5));
        write.close();
        commit.close();

        // enable Iceberg v3 WITHOUT full history sync; the next commit creates metadata from
        // scratch while the bucket still has uncompacted level-0 files
        Map<String, String> options = new HashMap<>();
        options.put(
                IcebergOptions.METADATA_ICEBERG_STORAGE.key(),
                IcebergOptions.StorageType.TABLE_LOCATION.toString());
        options.put(IcebergOptions.FORMAT_VERSION.key(), "3");
        options.put(IcebergOptions.METADATA_DELETE_AFTER_COMMIT.key(), "false");
        table = table.copy(options);
        write =
                table.newWrite(commitUser)
                        .withIOManager(new IOManagerImpl(tempDir.toString() + "/tmp"));
        commit = table.newCommit(commitUser);

        write.write(row(RowKind.INSERT, 1, 5, "e"));
        commit.commit(6, write.prepareCommit(false, 6));

        // The non-raw-convertible split must not be dropped wholesale: the files above level 0
        // (with their deletion vector) are exactly what live incremental commits would have
        // published, so Iceberg sees the data as of the last compaction. Only the level-0 rows
        // (d, e) stay invisible until a compaction rewrites them.
        HadoopCatalog icebergCatalog = new HadoopCatalog(new Configuration(), tempDir.toString());
        Table icebergTable = icebergCatalog.loadTable(TableIdentifier.of("mydb.db", "t"));
        assertThat(readIceberg(icebergTable, null)).containsExactlyInAnyOrder("1|1|a", "1|3|c");

        IcebergMetadata metadata =
                IcebergMetadata.fromPath(
                        table.fileIO(), new Path(table.location(), "metadata/v6.metadata.json"));
        assertThat(metadata.nextRowId()).isNotNull();
        IcebergPathFactory paths = new IcebergPathFactory(new Path(table.location(), "metadata"));
        IcebergManifestList manifestList = IcebergManifestList.create(table, paths);
        assertThat(
                        manifestList
                                .read(new Path(metadata.currentSnapshot().manifestList()).getName())
                                .stream()
                                .filter(m -> m.content() == IcebergManifestFileMeta.Content.DATA))
                .allMatch(m -> m.firstRowId() != null);

        // a full compaction exports the level-0 rows through the incremental path
        write.compact(partition(1), 0, true);
        commit.commit(7, write.prepareCommit(true, 7));
        write.close();
        commit.close();

        icebergTable.refresh();
        assertThat(readIceberg(icebergTable, null))
                .containsExactlyInAnyOrder("1|1|a", "1|3|c", "1|4|d", "1|5|e");
    }

    private static List<String> readIceberg(Table icebergTable, Long snapshotId) throws Exception {
        IcebergGenerics.ScanBuilder builder = IcebergGenerics.read(icebergTable);
        if (snapshotId != null) {
            builder = builder.useSnapshot(snapshotId);
        }
        List<String> actual = new ArrayList<>();
        try (CloseableIterable<Record> reader = builder.build()) {
            // compare only the projected columns: Iceberg's generic reader may append materialized
            // metadata columns (e.g. _pos while applying a deletion vector) to the output record
            reader.forEach(
                    record ->
                            actual.add(record.get(0) + "|" + record.get(1) + "|" + record.get(2)));
        }
        return actual;
    }

    private FileStoreTable createPaimonTableWithoutIceberg() throws Exception {
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "pt", DataTypes.INT().notNull()),
                                new DataField(1, "k", DataTypes.INT().notNull()),
                                new DataField(2, "v", DataTypes.STRING())));

        LocalFileIO fileIO = LocalFileIO.create();
        Path path = new Path(tempDir.toString());

        Options options = new Options();
        options.set(CoreOptions.BUCKET, 1);
        options.set(CoreOptions.FILE_FORMAT, "parquet");
        options.set(CoreOptions.TARGET_FILE_SIZE, MemorySize.ofKibiBytes(32));
        options.set(CoreOptions.DELETION_VECTORS_ENABLED, true);
        options.set(CoreOptions.DELETION_VECTOR_BITMAP64, true);

        Schema schema =
                new Schema(
                        rowType.getFields(),
                        Collections.singletonList("pt"),
                        Arrays.asList("pt", "k"),
                        options.toMap(),
                        "");

        try (FileSystemCatalog paimonCatalog = new FileSystemCatalog(fileIO, path)) {
            paimonCatalog.createDatabase("mydb", false);
            Identifier paimonIdentifier = Identifier.create("mydb", "t");
            paimonCatalog.createTable(paimonIdentifier, schema, false);
            return (FileStoreTable) paimonCatalog.getTable(paimonIdentifier);
        }
    }

    private static GenericRow row(RowKind kind, int pt, int k, String v) {
        return GenericRow.ofKind(kind, pt, k, BinaryString.fromString(v));
    }

    private static BinaryRow partition(int pt) {
        BinaryRow partition = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(partition);
        writer.writeInt(0, pt);
        writer.complete();
        return partition;
    }
}
