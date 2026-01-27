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
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.iceberg.metadata.IcebergMetadata;
import org.apache.paimon.iceberg.metadata.IcebergSnapshot;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTCatalogServer;
import org.apache.iceberg.rest.RESTServerExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.iceberg.IcebergCommitCallback.catalogTableMetadataPath;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link IcebergRestMetadataCommitter}. */
public class IcebergRestMetadataCommitterTest {

    @RegisterExtension
    private static final RESTServerExtension REST_SERVER_EXTENSION =
            new RESTServerExtension(
                    Map.of(
                            RESTCatalogServer.REST_PORT,
                            RESTServerExtension.FREE_PORT,
                            CatalogProperties.CLIENT_POOL_SIZE,
                            "1",
                            CatalogProperties.CATALOG_IMPL,
                            HadoopCatalog.class.getName()));

    @TempDir public java.nio.file.Path tempDir;

    protected static RESTCatalog restCatalog;

    @BeforeEach
    public void setUp() {
        restCatalog = REST_SERVER_EXTENSION.client();
    }

    @Test
    public void testUnPartitionedPrimaryKeyTable() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT(), DataTypes.STRING(), DataTypes.INT(), DataTypes.BIGINT()
                        },
                        new String[] {"k1", "k2", "v1", "v2"});

        int numRounds = 20;
        int numRecords = 1000;
        ThreadLocalRandom random = ThreadLocalRandom.current();
        List<List<TestRecord>> testRecords = new ArrayList<>();
        List<List<String>> expected = new ArrayList<>();
        Map<String, String> expectedMap = new LinkedHashMap<>();
        for (int r = 0; r < numRounds; r++) {
            List<TestRecord> round = new ArrayList<>();
            for (int i = 0; i < numRecords; i++) {
                int k1 = random.nextInt(0, 100);
                String k2 = String.valueOf(random.nextInt(1000, 1010));
                int v1 = random.nextInt();
                long v2 = random.nextLong();
                round.add(
                        new TestRecord(
                                BinaryRow.EMPTY_ROW,
                                GenericRow.of(k1, BinaryString.fromString(k2), v1, v2)));
                expectedMap.put(String.format("%d, %s", k1, k2), String.format("%d, %d", v1, v2));
            }
            testRecords.add(round);
            expected.add(
                    expectedMap.entrySet().stream()
                            .map(e -> String.format("Record(%s, %s)", e.getKey(), e.getValue()))
                            .collect(Collectors.toList()));
        }

        runCompatibilityTest(
                rowType,
                Collections.emptyList(),
                Arrays.asList("k1", "k2"),
                testRecords,
                expected,
                Record::toString);
    }

    @Test
    public void testPartitionedPrimaryKeyTable() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.INT(),
                            DataTypes.BIGINT()
                        },
                        new String[] {"pt1", "pt2", "k", "v1", "v2"});

        BiFunction<Integer, String, BinaryRow> binaryRow =
                (pt1, pt2) -> {
                    BinaryRow b = new BinaryRow(2);
                    BinaryRowWriter writer = new BinaryRowWriter(b);
                    writer.writeInt(0, pt1);
                    writer.writeString(1, BinaryString.fromString(pt2));
                    writer.complete();
                    return b;
                };

        int numRounds = 20;
        int numRecords = 500;
        ThreadLocalRandom random = ThreadLocalRandom.current();
        boolean samePartitionEachRound = random.nextBoolean();

        List<List<TestRecord>> testRecords = new ArrayList<>();
        List<List<String>> expected = new ArrayList<>();
        Map<String, String> expectedMap = new LinkedHashMap<>();
        for (int r = 0; r < numRounds; r++) {
            List<TestRecord> round = new ArrayList<>();
            for (int i = 0; i < numRecords; i++) {
                int pt1 = (random.nextInt(0, samePartitionEachRound ? 1 : 2) + r) % 3;
                String pt2 = String.valueOf(random.nextInt(10, 12));
                String k = String.valueOf(random.nextInt(0, 100));
                int v1 = random.nextInt();
                long v2 = random.nextLong();
                round.add(
                        new TestRecord(
                                binaryRow.apply(pt1, pt2),
                                GenericRow.of(
                                        pt1,
                                        BinaryString.fromString(pt2),
                                        BinaryString.fromString(k),
                                        v1,
                                        v2)));
                expectedMap.put(
                        String.format("%d, %s, %s", pt1, pt2, k), String.format("%d, %d", v1, v2));
            }
            testRecords.add(round);
            expected.add(
                    expectedMap.entrySet().stream()
                            .map(e -> String.format("Record(%s, %s)", e.getKey(), e.getValue()))
                            .collect(Collectors.toList()));
        }

        runCompatibilityTest(
                rowType,
                Arrays.asList("pt1", "pt2"),
                Arrays.asList("pt1", "pt2", "k"),
                testRecords,
                expected,
                Record::toString);
    }

    @Test
    public void testPartitionedPrimaryKeyTableWithNonZeroFieldId() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.STRING(),
                            DataTypes.INT(),
                            DataTypes.STRING(),
                            DataTypes.INT(),
                            DataTypes.BIGINT()
                        },
                        new String[] {
                            "k", "pt1", "pt2", "v1", "v2"
                        }); // partition starts from fieldId 1

        BiFunction<Integer, String, BinaryRow> binaryRow =
                (pt1, pt2) -> {
                    BinaryRow b = new BinaryRow(2);
                    BinaryRowWriter writer = new BinaryRowWriter(b);
                    writer.writeInt(0, pt1);
                    writer.writeString(1, BinaryString.fromString(pt2));
                    writer.complete();
                    return b;
                };

        int numRounds = 20;
        int numRecords = 500;
        ThreadLocalRandom random = ThreadLocalRandom.current();
        boolean samePartitionEachRound = random.nextBoolean();

        List<List<TestRecord>> testRecords = new ArrayList<>();
        List<List<String>> expected = new ArrayList<>();
        Map<String, String> expectedMap = new LinkedHashMap<>();
        for (int r = 0; r < numRounds; r++) {
            List<TestRecord> round = new ArrayList<>();
            for (int i = 0; i < numRecords; i++) {
                int pt1 = (random.nextInt(0, samePartitionEachRound ? 1 : 2) + r) % 3;
                String pt2 = String.valueOf(random.nextInt(10, 12));
                String k = String.valueOf(random.nextInt(0, 100));
                int v1 = random.nextInt();
                long v2 = random.nextLong();
                round.add(
                        new TestRecord(
                                binaryRow.apply(pt1, pt2),
                                GenericRow.of(
                                        BinaryString.fromString(k),
                                        pt1,
                                        BinaryString.fromString(pt2),
                                        v1,
                                        v2)));
                expectedMap.put(
                        String.format("%s, %d, %s", k, pt1, pt2), String.format("%d, %d", v1, v2));
            }
            testRecords.add(round);
            expected.add(
                    expectedMap.entrySet().stream()
                            .map(e -> String.format("Record(%s, %s)", e.getKey(), e.getValue()))
                            .sorted()
                            .collect(Collectors.toList()));
        }

        runCompatibilityTest(
                rowType,
                Arrays.asList("pt1", "pt2"),
                Arrays.asList("k", "pt1", "pt2"),
                testRecords,
                expected,
                Record::toString);
    }

    private void runCompatibilityTest(
            RowType rowType,
            List<String> partitionKeys,
            List<String> primaryKeys,
            List<List<TestRecord>> testRecords,
            List<List<String>> expected,
            Function<Record, String> icebergRecordToString)
            throws Exception {
        FileStoreTable table =
                createPaimonTable(
                        rowType,
                        partitionKeys,
                        primaryKeys,
                        primaryKeys.isEmpty() ? -1 : 2,
                        randomFormat(),
                        Collections.emptyMap());

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);

        for (int r = 0; r < testRecords.size(); r++) {
            List<TestRecord> round = testRecords.get(r);
            for (TestRecord testRecord : round) {
                write.write(testRecord.record);
            }

            if (!primaryKeys.isEmpty()) {
                for (BinaryRow partition :
                        round.stream().map(t -> t.partition).collect(Collectors.toSet())) {
                    for (int b = 0; b < 2; b++) {
                        write.compact(partition, b, true);
                    }
                }
            }
            commit.commit(r, write.prepareCommit(true, r));

            assertThat(
                            getIcebergResult(
                                    icebergTable -> IcebergGenerics.read(icebergTable).build(),
                                    icebergRecordToString))
                    .hasSameElementsAs(expected.get(r));
        }

        write.close();
        commit.close();
    }

    @Test
    public void testSchemaAndPropertiesChange() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        FileStoreTable table =
                createPaimonTable(
                        rowType,
                        Collections.emptyList(),
                        Collections.singletonList("k"),
                        1,
                        randomFormat(),
                        Collections.emptyMap());

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);

        write.write(GenericRow.of(1, 10));
        write.write(GenericRow.of(2, 20));
        commit.commit(1, write.prepareCommit(false, 1));
        assertThat(getIcebergResult()).containsExactlyInAnyOrder("Record(1, 10)", "Record(2, 20)");

        SchemaManager schemaManager = new SchemaManager(table.fileIO(), table.location());
        // change1: add a column
        // change2: change 'metadata.iceberg.delete-after-commit.enabled' to false
        // change3: change 'metadata.iceberg.previous-versions-max' to 10
        schemaManager.commitChanges(
                SchemaChange.addColumn("v2", DataTypes.STRING()),
                SchemaChange.setOption(IcebergOptions.METADATA_DELETE_AFTER_COMMIT.key(), "false"),
                SchemaChange.setOption(IcebergOptions.METADATA_PREVIOUS_VERSIONS_MAX.key(), "10"));
        table = table.copy(table.schemaManager().latest().get());
        write.close();
        write = table.newWrite(commitUser);
        commit.close();
        commit = table.newCommit(commitUser);

        write.write(GenericRow.of(1, 11, BinaryString.fromString("one")));
        write.write(GenericRow.of(3, 30, BinaryString.fromString("three")));
        write.compact(BinaryRow.EMPTY_ROW, 0, true);
        commit.commit(2, write.prepareCommit(true, 2));
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrder(
                        "Record(1, 11, one)", "Record(2, 20, null)", "Record(3, 30, three)");

        write.write(GenericRow.of(2, 21, BinaryString.fromString("two")));
        write.compact(BinaryRow.EMPTY_ROW, 0, true);
        commit.commit(3, write.prepareCommit(true, 3));
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrder(
                        "Record(1, 11, one)", "Record(2, 21, two)", "Record(3, 30, three)");

        Table icebergTable = restCatalog.loadTable(TableIdentifier.of("mydb", "t"));
        assertThat(icebergTable.currentSnapshot().snapshotId()).isEqualTo(5);
        // 1 metadata for createTable + 4 history metadata
        assertThat(((BaseTable) icebergTable).operations().current().previousFiles().size())
                .isEqualTo(5);

        write.close();
        commit.close();
    }

    @Test
    public void testSchemaChangeBeforeSync() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        Map<String, String> options = new HashMap<>();
        options.put(IcebergOptions.METADATA_ICEBERG_STORAGE.key(), "disabled");
        // disable iceberg compatibility
        FileStoreTable table =
                createPaimonTable(
                                rowType,
                                Collections.emptyList(),
                                Collections.singletonList("k"),
                                1,
                                randomFormat(),
                                Collections.emptyMap())
                        .copy(options);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);

        write.write(GenericRow.of(1, 10));
        write.write(GenericRow.of(2, 20));
        commit.commit(1, write.prepareCommit(false, 1));

        // schema change
        SchemaManager schemaManager = new SchemaManager(table.fileIO(), table.location());
        schemaManager.commitChanges(SchemaChange.addColumn("v2", DataTypes.STRING()));
        table = table.copyWithLatestSchema();
        write.close();
        write = table.newWrite(commitUser);
        commit.close();
        commit = table.newCommit(commitUser);

        write.write(GenericRow.of(1, 11, BinaryString.fromString("one")));
        write.write(GenericRow.of(3, 30, BinaryString.fromString("three")));
        write.compact(BinaryRow.EMPTY_ROW, 0, true);
        commit.commit(2, write.prepareCommit(true, 2));

        // enable iceberg compatibility
        options.put(IcebergOptions.METADATA_ICEBERG_STORAGE.key(), "rest-catalog");
        table = table.copy(options);
        write.close();
        write = table.newWrite(commitUser);
        commit.close();
        commit = table.newCommit(commitUser);

        write.write(GenericRow.of(4, 40, BinaryString.fromString("four")));
        write.compact(BinaryRow.EMPTY_ROW, 0, true);
        commit.commit(3, write.prepareCommit(true, 3));
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrder(
                        "Record(1, 11, one)",
                        "Record(2, 20, null)",
                        "Record(3, 30, three)",
                        "Record(4, 40, four)");

        write.close();
        commit.close();
    }

    @Test
    public void testIcebergSnapshotExpire() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(), "3");
        options.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(), "3");
        FileStoreTable table =
                createPaimonTable(
                        rowType,
                        Collections.emptyList(),
                        Collections.singletonList("k"),
                        1,
                        randomFormat(),
                        options);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);

        write.write(GenericRow.of(1, 10));
        write.write(GenericRow.of(2, 20));
        commit.commit(1, write.prepareCommit(false, 1));
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(1L);
        FileIO fileIO = table.fileIO();
        IcebergMetadata metadata =
                IcebergMetadata.fromPath(
                        fileIO, new Path(catalogTableMetadataPath(table), "v1.metadata.json"));
        Table icebergTable = restCatalog.loadTable(TableIdentifier.of("mydb", "t"));
        assertThat(metadata.snapshots()).hasSize(1);
        assertThat(metadata.currentSnapshotId()).isEqualTo(1);
        // check table in rest-catalog
        assertThat(icebergTable.currentSnapshot().snapshotId()).isEqualTo(1);
        assertThat(ImmutableList.copyOf(icebergTable.snapshots()).size()).isEqualTo(1);

        write.write(GenericRow.of(1, 11));
        write.write(GenericRow.of(3, 30));
        write.compact(BinaryRow.EMPTY_ROW, 0, true);
        commit.commit(2, write.prepareCommit(true, 2));
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(3L);
        icebergTable = restCatalog.loadTable(TableIdentifier.of("mydb", "t"));
        metadata =
                IcebergMetadata.fromPath(
                        fileIO, new Path(catalogTableMetadataPath(table), "v3.metadata.json"));
        assertThat(metadata.snapshots()).hasSize(3);
        assertThat(metadata.currentSnapshotId()).isEqualTo(3);
        // check table in rest-catalog
        assertThat(icebergTable.currentSnapshot().snapshotId()).isEqualTo(3);
        assertThat(ImmutableList.copyOf(icebergTable.snapshots()).size()).isEqualTo(3);

        // Number of snapshots will become 5 with the next commit, however only 3 Iceberg snapshots
        // are kept. So the first 2 Iceberg snapshots will be expired.

        write.write(GenericRow.of(2, 21));
        write.write(GenericRow.of(3, 31));
        write.compact(BinaryRow.EMPTY_ROW, 0, true);
        commit.commit(3, write.prepareCommit(true, 3));
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(5L);
        metadata =
                IcebergMetadata.fromPath(
                        fileIO, new Path(catalogTableMetadataPath(table), "v5.metadata.json"));
        icebergTable = restCatalog.loadTable(TableIdentifier.of("mydb", "t"));
        assertThat(metadata.snapshots()).hasSize(3);
        assertThat(metadata.currentSnapshotId()).isEqualTo(5);
        // check table in rest-catalog
        assertThat(icebergTable.currentSnapshot().snapshotId()).isEqualTo(5);
        assertThat(ImmutableList.copyOf(icebergTable.snapshots()).size()).isEqualTo(3);

        write.close();
        commit.close();

        // The old metadata.json is removed when the new metadata.json is created
        // depending on the old metadata retention configuration.
        assertThat(((BaseTable) icebergTable).operations().current().previousFiles().size())
                .isEqualTo(1);

        assertThat(getIcebergResult())
                .containsExactlyInAnyOrder("Record(1, 11)", "Record(2, 21)", "Record(3, 31)");
        assertThat(
                        getIcebergResult(
                                t -> IcebergGenerics.read(t).useSnapshot(3).build(),
                                Record::toString))
                .containsExactlyInAnyOrder("Record(1, 11)", "Record(2, 20)", "Record(3, 30)");
        assertThat(
                        getIcebergResult(
                                t -> IcebergGenerics.read(t).useSnapshot(4).build(),
                                Record::toString))
                .containsExactlyInAnyOrder("Record(1, 11)", "Record(2, 20)", "Record(3, 30)");
    }

    @Test
    public void testWithIncorrectBase() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        FileStoreTable table =
                createPaimonTable(
                        rowType,
                        Collections.emptyList(),
                        Collections.singletonList("k"),
                        1,
                        randomFormat(),
                        Collections.emptyMap());

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);

        write.write(GenericRow.of(1, 10));
        write.write(GenericRow.of(2, 20));
        commit.commit(1, write.prepareCommit(false, 1));

        write.write(GenericRow.of(1, 11));
        write.write(GenericRow.of(3, 30));
        write.compact(BinaryRow.EMPTY_ROW, 0, true);
        commit.commit(2, write.prepareCommit(true, 2));
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrder("Record(1, 11)", "Record(2, 20)", "Record(3, 30)");
        Table icebergTable = restCatalog.loadTable(TableIdentifier.of("mydb", "t"));
        // generate 3 metadata files in iceberg table, and current snapshot id is 3
        assertThat(icebergTable.currentSnapshot().snapshotId()).isEqualTo(3);

        // disable iceberg compatibility
        Map<String, String> options = new HashMap<>();
        options.put(IcebergOptions.METADATA_ICEBERG_STORAGE.key(), "disabled");
        table = table.copy(options);
        write.close();
        write = table.newWrite(commitUser);
        commit.close();
        commit = table.newCommit(commitUser);

        write.write(GenericRow.of(4, 40));
        write.write(GenericRow.of(5, 50));
        write.compact(BinaryRow.EMPTY_ROW, 0, true);
        commit.commit(3, write.prepareCommit(true, 3));
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(5L);

        // enable iceberg compatibility
        options.put(IcebergOptions.METADATA_ICEBERG_STORAGE.key(), "rest-catalog");
        table = table.copy(options);
        write.close();
        write = table.newWrite(commitUser);
        commit.close();
        commit = table.newCommit(commitUser);

        write.write(GenericRow.of(6, 60));
        write.compact(BinaryRow.EMPTY_ROW, 0, true);
        commit.commit(4, write.prepareCommit(true, 4));
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(7L);
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrder(
                        "Record(1, 11)",
                        "Record(2, 20)",
                        "Record(3, 30)",
                        "Record(4, 40)",
                        "Record(5, 50)",
                        "Record(6, 60)");
        icebergTable = restCatalog.loadTable(TableIdentifier.of("mydb", "t"));
        assertThat(icebergTable.currentSnapshot().snapshotId()).isEqualTo(7);
        assertThat(ImmutableList.copyOf(icebergTable.snapshots()).size()).isEqualTo(2);

        write.write(GenericRow.of(4, 41));
        write.compact(BinaryRow.EMPTY_ROW, 0, true);
        commit.commit(5, write.prepareCommit(true, 5));
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrder(
                        "Record(1, 11)",
                        "Record(2, 20)",
                        "Record(3, 30)",
                        "Record(4, 41)",
                        "Record(5, 50)",
                        "Record(6, 60)");

        write.close();
        commit.close();
    }

    @Test
    public void testWithExistedTableLocation() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put(
                IcebergOptions.METADATA_ICEBERG_STORAGE_LOCATION.key(),
                IcebergOptions.StorageLocation.TABLE_LOCATION.toString());
        dynamicOptions.put(IcebergOptions.METADATA_ICEBERG_STORAGE.key(), "table-location");

        FileStoreTable table =
                createPaimonTable(
                                rowType,
                                Collections.emptyList(),
                                Collections.singletonList("k"),
                                1,
                                "avro",
                                Collections.emptyMap())
                        .copy(dynamicOptions);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);

        write.write(GenericRow.of(1, 10));
        write.write(GenericRow.of(2, 20));
        commit.commit(1, write.prepareCommit(false, 1));
        assertThat(table.fileIO().exists(new Path(table.location(), "metadata/v1.metadata.json")))
                .isTrue();

        dynamicOptions.put(IcebergOptions.METADATA_ICEBERG_STORAGE.key(), "rest-catalog");
        table = table.copy(dynamicOptions);
        write.close();
        write = table.newWrite(commitUser);
        commit.close();
        commit = table.newCommit(commitUser);

        write.write(GenericRow.of(1, 11));
        write.write(GenericRow.of(3, 30));
        write.compact(BinaryRow.EMPTY_ROW, 0, true);
        commit.commit(2, write.prepareCommit(true, 2));
        assertThat(table.fileIO().exists(new Path(table.location(), "metadata/v3.metadata.json")))
                .isTrue();
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrder("Record(1, 11)", "Record(2, 20)", "Record(3, 30)");

        write.close();
        commit.close();
    }

    @Test
    public void testParentSnapshotIdTracking() throws Exception {
        // create and write with paimon client
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        FileStoreTable table =
                createPaimonTable(
                        rowType,
                        Collections.emptyList(),
                        Collections.singletonList("k"),
                        1,
                        randomFormat(),
                        Collections.emptyMap());

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);

        // First commit - should have null parent snapshot ID
        write.write(GenericRow.of(1, 10));
        write.write(GenericRow.of(2, 20));
        commit.commit(1, write.prepareCommit(false, 1));

        FileIO fileIO = table.fileIO();
        IcebergMetadata metadata1 =
                IcebergMetadata.fromPath(
                        fileIO, new Path(catalogTableMetadataPath(table), "v1.metadata.json"));
        assertThat(metadata1.snapshots()).hasSize(1);
        assertThat(metadata1.snapshots().get(0).parentSnapshotId()).isNull();
        assertThat(metadata1.snapshots().get(0).snapshotId()).isEqualTo(1);

        // Second commit - should have parent snapshot ID pointing to first snapshot
        write.write(GenericRow.of(1, 11));
        write.write(GenericRow.of(3, 30));
        commit.commit(2, write.prepareCommit(true, 2));

        IcebergMetadata metadata2 =
                IcebergMetadata.fromPath(
                        fileIO, new Path(catalogTableMetadataPath(table), "v2.metadata.json"));
        assertThat(metadata2.snapshots()).hasSize(2);
        // The last snapshot should have parent pointing to the previous snapshot
        IcebergSnapshot lastSnapshot = metadata2.snapshots().get(metadata2.snapshots().size() - 1);
        assertThat(lastSnapshot.parentSnapshotId()).isEqualTo(1L);
        assertThat(lastSnapshot.snapshotId()).isEqualTo(2);

        // Third commit - should have parent snapshot ID pointing to second snapshot
        write.write(GenericRow.of(2, 21));
        write.write(GenericRow.of(4, 40));
        commit.commit(3, write.prepareCommit(true, 3));

        IcebergMetadata metadata3 =
                IcebergMetadata.fromPath(
                        fileIO, new Path(catalogTableMetadataPath(table), "v3.metadata.json"));
        assertThat(metadata3.snapshots()).hasSize(3);
        // The last snapshot should have parent pointing to the previous snapshot
        IcebergSnapshot lastSnapshot3 = metadata3.snapshots().get(metadata3.snapshots().size() - 1);
        assertThat(lastSnapshot3.parentSnapshotId()).isEqualTo(2L);
        assertThat(lastSnapshot3.snapshotId()).isEqualTo(3);

        write.close();
        commit.close();
    }

    private static class TestRecord {
        private final BinaryRow partition;
        private final GenericRow record;

        private TestRecord(BinaryRow partition, GenericRow record) {
            this.partition = partition;
            this.record = record;
        }
    }

    private FileStoreTable createPaimonTable(
            RowType rowType,
            List<String> partitionKeys,
            List<String> primaryKeys,
            int numBuckets,
            String fileFormat,
            Map<String, String> customOptions)
            throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        org.apache.paimon.fs.Path path = new org.apache.paimon.fs.Path(tempDir.toString());

        Options options = new Options(customOptions);
        options.set(CoreOptions.BUCKET, numBuckets);
        options.set(
                IcebergOptions.METADATA_ICEBERG_STORAGE, IcebergOptions.StorageType.REST_CATALOG);
        options.set(CoreOptions.FILE_FORMAT, fileFormat);
        options.set(CoreOptions.TARGET_FILE_SIZE, MemorySize.ofKibiBytes(32));
        options.set(IcebergOptions.COMPACT_MIN_FILE_NUM, 4);
        options.set(IcebergOptions.COMPACT_MIN_FILE_NUM, 8);
        options.set(IcebergOptions.METADATA_DELETE_AFTER_COMMIT, true);
        options.set(IcebergOptions.METADATA_PREVIOUS_VERSIONS_MAX, 1);
        options.set(CoreOptions.MANIFEST_TARGET_FILE_SIZE, MemorySize.ofKibiBytes(8));

        // rest-catalog options
        options.set(
                IcebergOptions.REST_CONFIG_PREFIX + CatalogProperties.URI,
                restCatalog.properties().get(CatalogProperties.URI));
        options.set(
                IcebergOptions.REST_CONFIG_PREFIX + CatalogProperties.WAREHOUSE_LOCATION,
                restCatalog.properties().get(CatalogProperties.WAREHOUSE_LOCATION));
        options.set(
                IcebergOptions.REST_CONFIG_PREFIX + CatalogProperties.CLIENT_POOL_SIZE,
                restCatalog.properties().get(CatalogProperties.CLIENT_POOL_SIZE));

        org.apache.paimon.schema.Schema schema =
                new org.apache.paimon.schema.Schema(
                        rowType.getFields(), partitionKeys, primaryKeys, options.toMap(), "");

        try (FileSystemCatalog paimonCatalog = new FileSystemCatalog(fileIO, path)) {
            paimonCatalog.createDatabase("mydb", false);
            Identifier paimonIdentifier = Identifier.create("mydb", "t");
            paimonCatalog.createTable(paimonIdentifier, schema, false);
            return (FileStoreTable) paimonCatalog.getTable(paimonIdentifier);
        }
    }

    private List<String> getIcebergResult() throws Exception {
        return getIcebergResult(
                icebergTable -> IcebergGenerics.read(icebergTable).build(), Record::toString);
    }

    private List<String> getIcebergResult(
            Function<Table, CloseableIterable<Record>> query,
            Function<Record, String> icebergRecordToString)
            throws Exception {
        Table icebergTable = restCatalog.loadTable(TableIdentifier.of("mydb", "t"));
        CloseableIterable<Record> result = query.apply(icebergTable);
        List<String> actual = new ArrayList<>();
        for (Record record : result) {
            actual.add(icebergRecordToString.apply(record));
        }
        result.close();
        return actual;
    }

    private String randomFormat() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int i = random.nextInt(3);
        String[] formats = new String[] {"orc", "parquet", "avro"};
        return formats[i];
    }
}
