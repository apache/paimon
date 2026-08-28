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
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.iceberg.manifest.IcebergManifestFileMeta;
import org.apache.paimon.iceberg.manifest.IcebergManifestList;
import org.apache.paimon.iceberg.metadata.IcebergMetadata;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for replaying the Iceberg metadata a mirror gap left behind. */
public class IcebergMirrorReplayTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testMirrorWalksPastBaseWithExpiredManifestList() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        Map<String, String> options = new HashMap<>();
        options.put(IcebergOptions.METADATA_PREVIOUS_VERSIONS_MAX.key(), "10");
        FileStoreTable table =
                createPaimonTable(
                        rowType, Collections.emptyList(), Collections.emptyList(), -1, options);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);
        write.write(GenericRow.of(1, 10));
        commit.commit(1, write.prepareCommit(false, 1));
        write.write(GenericRow.of(2, 20));
        commit.commit(2, write.prepareCommit(false, 2));
        write.write(GenericRow.of(3, 30));
        commit.commit(3, write.prepareCommit(false, 3));

        IcebergPathFactory pathFactory =
                new IcebergPathFactory(new Path(table.location(), "metadata"));

        IcebergMetadata v2 =
                IcebergMetadata.fromPath(table.fileIO(), pathFactory.toMetadataPath(2));
        table.fileIO().deleteQuietly(new Path(v2.currentSnapshot().manifestList()));
        table.fileIO().deleteQuietly(pathFactory.toMetadataPath(3));

        write.write(GenericRow.of(4, 40));
        commit.commit(4, write.prepareCommit(false, 4));
        write.close();
        commit.close();

        IcebergMetadata rebuilt =
                IcebergMetadata.fromPath(table.fileIO(), pathFactory.toMetadataPath(4));
        assertThat(rebuilt.currentSnapshotId()).isEqualTo(4L);
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrder(
                        "Record(1, 10)", "Record(2, 20)", "Record(3, 30)", "Record(4, 40)");
    }

    @Test
    public void testReplayedSnapshotKeepsCommitTime() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        FileStoreTable table =
                createPaimonTable(
                        rowType,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        -1,
                        Collections.singletonMap(IcebergOptions.FORMAT_VERSION.key(), "3"));

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);
        write.write(GenericRow.of(1, 10));
        commit.commit(1, write.prepareCommit(false, 1));
        write.write(GenericRow.of(2, 20));
        commit.commit(2, write.prepareCommit(false, 2));

        long paimonSnapshot2Time = table.snapshotManager().snapshot(2).timeMillis();

        IcebergPathFactory pathFactory =
                new IcebergPathFactory(new Path(table.location(), "metadata"));
        table.fileIO().deleteQuietly(pathFactory.toMetadataPath(2));

        write.write(GenericRow.of(3, 30));
        commit.commit(3, write.prepareCommit(false, 3));
        write.close();
        commit.close();

        IcebergMetadata rebuiltV2 =
                IcebergMetadata.fromPath(table.fileIO(), pathFactory.toMetadataPath(2));
        assertThat(rebuiltV2.currentSnapshot().timestampMs()).isEqualTo(paimonSnapshot2Time);
    }

    @Test
    public void testReplayedSnapshotWithoutDeletionVectorsDropsDeleteManifests() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        Map<String, String> customOptions = new HashMap<>();
        customOptions.put(IcebergOptions.FORMAT_VERSION.key(), "3");
        customOptions.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        customOptions.put(CoreOptions.DELETION_VECTOR_BITMAP64.key(), "true");
        customOptions.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(), "100");
        customOptions.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(), "100");
        FileStoreTable table =
                createPaimonTable(
                        rowType,
                        Collections.emptyList(),
                        Collections.singletonList("k"),
                        1,
                        customOptions);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write =
                table.newWrite(commitUser)
                        .withIOManager(new IOManagerImpl(tempDir.toString() + "/tmp"));
        TableCommitImpl commit = table.newCommit(commitUser);

        write.write(GenericRow.of(1, 10));
        write.write(GenericRow.of(2, 20));
        commit.commit(1, write.prepareCommit(false, 1));
        table.createTag("predelete", 1);
        write.write(GenericRow.ofKind(RowKind.DELETE, 2, 20));
        commit.commit(2, write.prepareCommit(false, 2));
        write.compact(BinaryRow.EMPTY_ROW, 0, false);
        commit.commit(3, write.prepareCommit(true, 3));
        long dvSnapshotId = table.snapshotManager().latestSnapshotId();
        write.close();
        commit.close();

        TableCommitImpl rollbackCommit = table.newCommit(commitUser);
        rollbackCommit.rollbackToAsLatest(table.tagManager().getOrThrow("predelete"));
        rollbackCommit.close();
        long rolledBackSnapshotId = table.snapshotManager().latestSnapshotId();

        IcebergPathFactory pathFactory =
                new IcebergPathFactory(new Path(table.location(), "metadata"));
        assertThat(countDeleteManifests(table, pathFactory, dvSnapshotId)).isGreaterThan(0L);
        assertThat(countDeleteManifests(table, pathFactory, rolledBackSnapshotId)).isEqualTo(0L);

        table.fileIO().deleteQuietly(pathFactory.toMetadataPath(rolledBackSnapshotId));

        TableWriteImpl<?> write2 =
                table.newWrite(commitUser)
                        .withIOManager(new IOManagerImpl(tempDir.toString() + "/tmp"));
        TableCommitImpl commit2 = table.newCommit(commitUser);
        write2.write(GenericRow.of(3, 30));
        commit2.commit(4, write2.prepareCommit(false, 4));
        write2.close();
        commit2.close();

        assertThat(countDeleteManifests(table, pathFactory, rolledBackSnapshotId)).isEqualTo(0L);
    }

    @Test
    public void testReplayedSnapshotKeepsHistoricalSchema() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        FileStoreTable table =
                createPaimonTable(
                        rowType,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        -1,
                        Collections.singletonMap(IcebergOptions.FORMAT_VERSION.key(), "3"));

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);
        write.write(GenericRow.of(1, 10));
        commit.commit(1, write.prepareCommit(false, 1));
        write.close();
        commit.close();

        SchemaManager schemaManager = new SchemaManager(table.fileIO(), table.location());
        schemaManager.commitChanges(SchemaChange.addColumn("w", DataTypes.INT()));
        table = table.copyWithLatestSchema();
        write = table.newWrite(commitUser);
        commit = table.newCommit(commitUser);
        write.write(GenericRow.of(2, 20, 200));
        commit.commit(2, write.prepareCommit(false, 2));
        write.close();
        commit.close();
        long schema2 = table.snapshotManager().snapshot(2).schemaId();

        IcebergPathFactory pathFactory =
                new IcebergPathFactory(new Path(table.location(), "metadata"));
        table.fileIO().deleteQuietly(pathFactory.toMetadataPath(2));

        schemaManager.commitChanges(SchemaChange.addColumn("x", DataTypes.INT()));
        table = table.copyWithLatestSchema();
        write = table.newWrite(commitUser);
        commit = table.newCommit(commitUser);
        write.write(GenericRow.of(3, 30, 300, 3000));
        commit.commit(3, write.prepareCommit(false, 3));
        write.close();
        commit.close();

        IcebergMetadata rebuiltV2 =
                IcebergMetadata.fromPath(table.fileIO(), pathFactory.toMetadataPath(2));
        assertThat(rebuiltV2.currentSnapshot().schemaId()).isEqualTo((int) schema2);
    }

    private FileStoreTable createPaimonTable(
            RowType rowType, List<String> partitionKeys, List<String> primaryKeys, int numBuckets)
            throws Exception {
        return createPaimonTable(rowType, partitionKeys, primaryKeys, numBuckets, new HashMap<>());
    }

    private FileStoreTable createPaimonTable(
            RowType rowType,
            List<String> partitionKeys,
            List<String> primaryKeys,
            int numBuckets,
            Map<String, String> customOptions)
            throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path path = new Path(tempDir.toString());

        Options options = new Options(customOptions);
        options.set(CoreOptions.BUCKET, numBuckets);
        options.set(
                IcebergOptions.METADATA_ICEBERG_STORAGE, IcebergOptions.StorageType.TABLE_LOCATION);
        if (!customOptions.containsKey(CoreOptions.FILE_FORMAT.key())) {
            options.set(CoreOptions.FILE_FORMAT, "avro");
        }
        options.set(CoreOptions.TARGET_FILE_SIZE, MemorySize.ofKibiBytes(32));
        if (!customOptions.containsKey(IcebergOptions.COMPACT_MIN_FILE_NUM.key())) {
            options.set(IcebergOptions.COMPACT_MIN_FILE_NUM, 8);
        }
        options.set(IcebergOptions.METADATA_DELETE_AFTER_COMMIT, true);
        if (!customOptions.containsKey(IcebergOptions.METADATA_PREVIOUS_VERSIONS_MAX.key())) {
            options.set(IcebergOptions.METADATA_PREVIOUS_VERSIONS_MAX, 1);
        }
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

    private org.apache.iceberg.Table getIcebergTable() {
        HadoopCatalog icebergCatalog = new HadoopCatalog(new Configuration(), tempDir.toString());
        TableIdentifier icebergIdentifier = TableIdentifier.of("mydb.db", "t");
        return icebergCatalog.loadTable(icebergIdentifier);
    }

    private List<String> getIcebergResult() throws Exception {
        return getIcebergResult(
                icebergTable -> IcebergGenerics.read(icebergTable).build(), Record::toString);
    }

    private List<String> getIcebergResult(
            Function<org.apache.iceberg.Table, CloseableIterable<Record>> query,
            Function<Record, String> icebergRecordToString)
            throws Exception {
        org.apache.iceberg.Table icebergTable = getIcebergTable();
        CloseableIterable<Record> result = query.apply(icebergTable);
        List<String> actual = new ArrayList<>();
        for (Record record : result) {
            actual.add(icebergRecordToString.apply(record));
        }
        result.close();
        return actual;
    }

    private static long countDeleteManifests(
            FileStoreTable table, IcebergPathFactory pathFactory, long snapshotId)
            throws IOException {
        IcebergMetadata metadata =
                IcebergMetadata.fromPath(table.fileIO(), pathFactory.toMetadataPath(snapshotId));
        IcebergManifestList manifestList = IcebergManifestList.create(table, pathFactory);
        return manifestList.read(new Path(metadata.currentSnapshot().manifestList()).getName())
                .stream()
                .filter(m -> m.content() == IcebergManifestFileMeta.Content.DELETES)
                .count();
    }
}
