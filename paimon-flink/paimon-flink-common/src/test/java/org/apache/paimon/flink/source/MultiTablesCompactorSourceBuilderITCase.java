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

package org.apache.paimon.flink.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.util.AbstractTestBase;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;

import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link CombinedTableCompactorSourceBuilder}. */
public class MultiTablesCompactorSourceBuilderITCase extends AbstractTestBase
        implements Serializable {
    private String warehouse;
    private Options catalogOptions;

    private String commitUser;

    private static final String[] DATABASE_NAMES = new String[] {"db1", "db2"};
    private static final String[] TABLE_NAMES = new String[] {"t1", "t2"};
    private static final String[] New_DATABASE_NAMES = new String[] {"db3"};
    private static final String[] New_TABLE_NAMES = new String[] {"t1", "t2"};
    private static final Map<String, RowType> ROW_TYPE_MAP = new HashMap<>(TABLE_NAMES.length);

    @BeforeAll
    public static void beforeAll() {
        // set different datatype and RowType
        DataType[] dataTypes1 =
                new DataType[] {
                    DataTypes.INT(), DataTypes.INT(), DataTypes.INT(), DataTypes.STRING()
                };
        DataType[] dataTypes2 =
                new DataType[] {
                    DataTypes.INT(), DataTypes.INT(), DataTypes.INT(), DataTypes.STRING()
                };

        ROW_TYPE_MAP.put("t1", RowType.of(dataTypes1, new String[] {"k", "v", "hh", "dt"}));
        ROW_TYPE_MAP.put("t2", RowType.of(dataTypes2, new String[] {"k", "v1", "hh", "dt"}));
    }

    @BeforeEach
    public void before() throws IOException {
        warehouse = getTempDirPath();
        catalogOptions = new Options();
        commitUser = UUID.randomUUID().toString();
    }

    @ParameterizedTest(name = "defaultOptions = {0}")
    @ValueSource(booleans = {true, false})
    public void testBatchRead(boolean defaultOptions) throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.WRITE_ONLY.key(), "true");
        if (!defaultOptions) {
            // change options to test whether CompactorSourceBuilder work normally
            options.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), "2");
        }
        options.put("bucket", "1");
        long monitorInterval = 1000;

        for (String dbName : DATABASE_NAMES) {
            for (String tableName : TABLE_NAMES) {
                FileStoreTable table =
                        createTable(
                                dbName,
                                tableName,
                                ROW_TYPE_MAP.get(tableName),
                                Arrays.asList("dt", "hh"),
                                Arrays.asList("dt", "hh", "k"),
                                options);
                SnapshotManager snapshotManager = table.snapshotManager();
                StreamWriteBuilder streamWriteBuilder =
                        table.newStreamWriteBuilder().withCommitUser(commitUser);
                StreamTableWrite write = streamWriteBuilder.newWrite();
                StreamTableCommit commit = streamWriteBuilder.newCommit();

                writeData(
                        write,
                        commit,
                        0,
                        rowData(1, 100, 15, BinaryString.fromString("20221208")),
                        rowData(1, 100, 16, BinaryString.fromString("20221208")),
                        rowData(1, 100, 15, BinaryString.fromString("20221209")));

                writeData(
                        write,
                        commit,
                        1,
                        rowData(2, 100, 15, BinaryString.fromString("20221208")),
                        rowData(2, 100, 16, BinaryString.fromString("20221208")),
                        rowData(2, 100, 15, BinaryString.fromString("20221209")));

                Snapshot snapshot = snapshotManager.snapshot(snapshotManager.latestSnapshotId());
                assertThat(snapshot.id()).isEqualTo(2);
                assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);

                write.close();
                commit.close();
            }
        }

        StreamExecutionEnvironment env =
                streamExecutionEnvironmentBuilder()
                        .batchMode()
                        .parallelism(ThreadLocalRandom.current().nextInt(2) + 1)
                        .build();
        DataStream<RowData> source =
                new CombinedTableCompactorSourceBuilder(
                                catalogLoader(),
                                Pattern.compile("db1|db2"),
                                Pattern.compile(".*"),
                                null,
                                monitorInterval)
                        .withContinuousMode(false)
                        .withEnv(env)
                        .buildAwareBucketTableSource();
        CloseableIterator<RowData> it = source.executeAndCollect();
        List<String> actual = new ArrayList<>();
        while (it.hasNext()) {
            actual.add(toString(it.next()));
        }
        assertThat(actual)
                .hasSameElementsAs(
                        Arrays.asList(
                                "+I 2|20221208|15|0|0|db1|t1",
                                "+I 2|20221209|15|0|0|db1|t1",
                                "+I 2|20221208|16|0|0|db1|t1",
                                "+I 2|20221208|15|0|0|db1|t2",
                                "+I 2|20221209|15|0|0|db1|t2",
                                "+I 2|20221208|16|0|0|db1|t2",
                                "+I 2|20221208|15|0|0|db2|t1",
                                "+I 2|20221209|15|0|0|db2|t1",
                                "+I 2|20221208|16|0|0|db2|t1",
                                "+I 2|20221208|15|0|0|db2|t2",
                                "+I 2|20221209|15|0|0|db2|t2",
                                "+I 2|20221208|16|0|0|db2|t2"));
        it.close();
    }

    @ParameterizedTest(name = "defaultOptions = {0}")
    @ValueSource(booleans = {true, false})
    public void testStreamingRead(boolean defaultOptions) throws Exception {
        Map<String, String> options = new HashMap<>();
        if (!defaultOptions) {
            // change options to test whether CompactorSourceBuilder work normally
            options.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), "2");
            options.put(
                    CoreOptions.CHANGELOG_PRODUCER.key(),
                    CoreOptions.ChangelogProducer.NONE.toString());
            options.put(CoreOptions.SCAN_BOUNDED_WATERMARK.key(), "0");
        }
        options.put("bucket", "1");
        long monitorInterval = 1000;
        List<FileStoreTable> tables = new ArrayList<>();
        for (String dbName : DATABASE_NAMES) {
            for (String tableName : TABLE_NAMES) {
                FileStoreTable table =
                        createTable(
                                dbName,
                                tableName,
                                ROW_TYPE_MAP.get(tableName),
                                Arrays.asList("dt", "hh"),
                                Arrays.asList("dt", "hh", "k"),
                                options);
                tables.add(table);
                StreamWriteBuilder streamWriteBuilder =
                        table.newStreamWriteBuilder().withCommitUser(commitUser);
                StreamTableWrite write = streamWriteBuilder.newWrite();
                StreamTableCommit commit = streamWriteBuilder.newCommit();

                writeData(
                        write,
                        commit,
                        0,
                        rowData(1, 1510, 15, BinaryString.fromString("20221208")),
                        rowData(2, 1620, 16, BinaryString.fromString("20221208")));

                write.write(rowData(1, 1511, 15, BinaryString.fromString("20221208")));
                write.write(rowData(1, 1510, 15, BinaryString.fromString("20221209")));
                write.compact(binaryRow("20221208", 15), 0, true);
                write.compact(binaryRow("20221209", 15), 0, true);
                commit.commit(1, write.prepareCommit(true, 1));

                writeData(
                        write,
                        commit,
                        2,
                        rowData(2, 1520, 15, BinaryString.fromString("20221208")),
                        rowData(2, 1621, 16, BinaryString.fromString("20221208")));
                writeData(
                        write,
                        commit,
                        3,
                        rowData(1, 1512, 15, BinaryString.fromString("20221208")),
                        rowData(2, 1620, 16, BinaryString.fromString("20221209")));

                write.close();
                commit.close();
            }
        }

        StreamExecutionEnvironment env =
                streamExecutionEnvironmentBuilder().streamingMode().build();
        DataStream<RowData> compactorSource =
                new CombinedTableCompactorSourceBuilder(
                                catalogLoader(),
                                Pattern.compile(".*"),
                                Pattern.compile(".*"),
                                null,
                                monitorInterval)
                        .withContinuousMode(true)
                        .withEnv(env)
                        .buildAwareBucketTableSource();
        CloseableIterator<RowData> it = compactorSource.executeAndCollect();

        List<String> actual = new ArrayList<>();
        for (int i = 0; i < 16; i++) {
            actual.add(toString(it.next()));
        }
        assertThat(actual)
                .hasSameElementsAs(
                        Arrays.asList(
                                "+I 4|20221208|15|0|1|db1|t1",
                                "+I 4|20221208|16|0|1|db1|t1",
                                "+I 4|20221208|15|0|1|db1|t2",
                                "+I 4|20221208|16|0|1|db1|t2",
                                "+I 4|20221208|15|0|1|db2|t1",
                                "+I 4|20221208|16|0|1|db2|t1",
                                "+I 4|20221208|15|0|1|db2|t2",
                                "+I 4|20221208|16|0|1|db2|t2",
                                "+I 5|20221209|16|0|1|db1|t1",
                                "+I 5|20221208|15|0|1|db1|t1",
                                "+I 5|20221209|16|0|1|db1|t2",
                                "+I 5|20221208|15|0|1|db1|t2",
                                "+I 5|20221209|16|0|1|db2|t1",
                                "+I 5|20221208|15|0|1|db2|t1",
                                "+I 5|20221209|16|0|1|db2|t2",
                                "+I 5|20221208|15|0|1|db2|t2"));

        for (FileStoreTable table : tables) {
            StreamWriteBuilder streamWriteBuilder =
                    table.newStreamWriteBuilder().withCommitUser(commitUser);
            StreamTableWrite write = streamWriteBuilder.newWrite();
            StreamTableCommit commit = streamWriteBuilder.newCommit();
            writeData(
                    write,
                    commit,
                    4,
                    rowData(2, 1520, 15, BinaryString.fromString("20221209")),
                    rowData(1, 1510, 16, BinaryString.fromString("20221208")),
                    rowData(1, 1511, 15, BinaryString.fromString("20221209")));
            write.close();
            commit.close();
        }
        actual.clear();
        for (int i = 0; i < 8; i++) {
            actual.add(toString(it.next()));
        }
        assertThat(actual)
                .hasSameElementsAs(
                        Arrays.asList(
                                "+I 6|20221209|15|0|1|db1|t1",
                                "+I 6|20221208|16|0|1|db1|t1",
                                "+I 6|20221209|15|0|1|db1|t2",
                                "+I 6|20221208|16|0|1|db1|t2",
                                "+I 6|20221209|15|0|1|db2|t1",
                                "+I 6|20221208|16|0|1|db2|t1",
                                "+I 6|20221209|15|0|1|db2|t2",
                                "+I 6|20221208|16|0|1|db2|t2"));

        // check if newly created tables can be detected
        for (String dbName : New_DATABASE_NAMES) {
            for (String tableName : New_TABLE_NAMES) {
                FileStoreTable table =
                        createTable(
                                dbName,
                                tableName,
                                ROW_TYPE_MAP.get(tableName),
                                Arrays.asList("dt", "hh"),
                                Arrays.asList("dt", "hh", "k"),
                                options);
                tables.add(table);
                StreamWriteBuilder streamWriteBuilder =
                        table.newStreamWriteBuilder().withCommitUser(commitUser);
                StreamTableWrite write = streamWriteBuilder.newWrite();
                StreamTableCommit commit = streamWriteBuilder.newCommit();
                writeData(
                        write,
                        commit,
                        0,
                        rowData(2, 1520, 15, BinaryString.fromString("20221209")),
                        rowData(1, 1510, 16, BinaryString.fromString("20221208")));
                write.close();
                commit.close();
            }
        }
        actual.clear();
        for (int i = 0; i < 4; i++) {
            actual.add(toString(it.next()));
        }
        assertThat(actual)
                .hasSameElementsAs(
                        Arrays.asList(
                                "+I 1|20221209|15|0|1|db3|t2",
                                "+I 1|20221208|16|0|1|db3|t2",
                                "+I 1|20221209|15|0|1|db3|t1",
                                "+I 1|20221208|16|0|1|db3|t1"));
        it.close();
    }

    @ParameterizedTest(name = "defaultOptions = {0}")
    @ValueSource(booleans = {true, false})
    public void testIncludeAndExcludeTableRead(boolean defaultOptions) throws Exception {
        Map<String, String> options = new HashMap<>();
        if (!defaultOptions) {
            // change options to test whether CompactorSourceBuilder work normally
            options.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), "2");
            options.put(
                    CoreOptions.CHANGELOG_PRODUCER.key(),
                    CoreOptions.ChangelogProducer.NONE.toString());
            options.put(CoreOptions.SCAN_BOUNDED_WATERMARK.key(), "0");
        }
        options.put("bucket", "1");
        long monitorInterval = 1000;

        for (String dbName : DATABASE_NAMES) {
            for (String tableName : TABLE_NAMES) {
                FileStoreTable table =
                        createTable(
                                dbName,
                                tableName,
                                ROW_TYPE_MAP.get(tableName),
                                Arrays.asList("dt", "hh"),
                                Arrays.asList("dt", "hh", "k"),
                                options);
                StreamWriteBuilder streamWriteBuilder =
                        table.newStreamWriteBuilder().withCommitUser(commitUser);
                StreamTableWrite write = streamWriteBuilder.newWrite();
                StreamTableCommit commit = streamWriteBuilder.newCommit();

                writeData(
                        write,
                        commit,
                        0,
                        rowData(1, 1510, 15, BinaryString.fromString("20221208")),
                        rowData(2, 1620, 16, BinaryString.fromString("20221208")));

                write.write(rowData(1, 1511, 15, BinaryString.fromString("20221208")));
                write.write(rowData(1, 1510, 15, BinaryString.fromString("20221209")));
                write.compact(binaryRow("20221208", 15), 0, true);
                write.compact(binaryRow("20221209", 15), 0, true);
                commit.commit(1, write.prepareCommit(true, 1));

                writeData(
                        write,
                        commit,
                        2,
                        rowData(2, 1520, 15, BinaryString.fromString("20221208")),
                        rowData(2, 1621, 16, BinaryString.fromString("20221208")));
                writeData(
                        write,
                        commit,
                        3,
                        rowData(1, 1512, 15, BinaryString.fromString("20221208")),
                        rowData(2, 1620, 16, BinaryString.fromString("20221209")));

                write.close();
                commit.close();
            }
        }

        StreamExecutionEnvironment env =
                streamExecutionEnvironmentBuilder().streamingMode().build();
        DataStream<RowData> compactorSource =
                new CombinedTableCompactorSourceBuilder(
                                catalogLoader(),
                                Pattern.compile(".*"),
                                Pattern.compile("db1.+|db2.t1|db3.t1"),
                                Pattern.compile("db1.t2"),
                                monitorInterval)
                        .withContinuousMode(true)
                        .withEnv(env)
                        .buildAwareBucketTableSource();
        CloseableIterator<RowData> it = compactorSource.executeAndCollect();

        List<String> actual = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            actual.add(toString(it.next()));
        }
        assertThat(actual)
                .hasSameElementsAs(
                        Arrays.asList(
                                "+I 4|20221208|15|0|1|db1|t1",
                                "+I 4|20221208|16|0|1|db1|t1",
                                "+I 4|20221208|15|0|1|db2|t1",
                                "+I 4|20221208|16|0|1|db2|t1",
                                "+I 5|20221209|16|0|1|db1|t1",
                                "+I 5|20221208|15|0|1|db1|t1",
                                "+I 5|20221209|16|0|1|db2|t1",
                                "+I 5|20221208|15|0|1|db2|t1"));

        // check if newly created tables conform to the pattern rule
        for (String dbName : New_DATABASE_NAMES) {
            for (String tableName : New_TABLE_NAMES) {
                FileStoreTable table =
                        createTable(
                                dbName,
                                tableName,
                                ROW_TYPE_MAP.get(tableName),
                                Arrays.asList("dt", "hh"),
                                Arrays.asList("dt", "hh", "k"),
                                options);
                StreamWriteBuilder streamWriteBuilder =
                        table.newStreamWriteBuilder().withCommitUser(commitUser);
                StreamTableWrite write = streamWriteBuilder.newWrite();
                StreamTableCommit commit = streamWriteBuilder.newCommit();
                writeData(
                        write,
                        commit,
                        0,
                        rowData(2, 1520, 15, BinaryString.fromString("20221209")),
                        rowData(1, 1510, 16, BinaryString.fromString("20221208")));
                write.close();
                commit.close();
            }
        }
        actual.clear();
        for (int i = 0; i < 2; i++) {
            actual.add(toString(it.next()));
        }
        assertThat(actual)
                .hasSameElementsAs(
                        Arrays.asList(
                                "+I 1|20221209|15|0|1|db3|t1", "+I 1|20221208|16|0|1|db3|t1"));
        it.close();
    }

    @ParameterizedTest(name = "defaultOptions = {0}")
    @ValueSource(booleans = {true, false})
    public void testHistoryPatitionRead(boolean defaultOptions) throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.WRITE_ONLY.key(), "true");
        if (!defaultOptions) {
            // change options to test whether CompactorSourceBuilder work normally
            options.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), "2");
        }
        options.put("bucket", "1");
        long monitorInterval = 1000;
        Duration partitionIdleTime = Duration.ofMillis(3000);
        List<FileStoreTable> tables = new ArrayList<>();

        for (String dbName : DATABASE_NAMES) {
            for (String tableName : TABLE_NAMES) {
                FileStoreTable table =
                        createTable(
                                dbName,
                                tableName,
                                ROW_TYPE_MAP.get(tableName),
                                Arrays.asList("dt", "hh"),
                                Arrays.asList("dt", "hh", "k"),
                                options);
                tables.add(table);
                SnapshotManager snapshotManager = table.snapshotManager();
                StreamWriteBuilder streamWriteBuilder =
                        table.newStreamWriteBuilder().withCommitUser(commitUser);
                StreamTableWrite write = streamWriteBuilder.newWrite();
                StreamTableCommit commit = streamWriteBuilder.newCommit();

                writeData(
                        write,
                        commit,
                        0,
                        rowData(1, 100, 15, BinaryString.fromString("20221208")),
                        rowData(1, 100, 16, BinaryString.fromString("20221208")),
                        rowData(1, 100, 15, BinaryString.fromString("20221209")));

                writeData(
                        write,
                        commit,
                        1,
                        rowData(2, 100, 15, BinaryString.fromString("20221208")),
                        rowData(2, 100, 16, BinaryString.fromString("20221208")),
                        rowData(2, 100, 15, BinaryString.fromString("20221209")));

                Snapshot snapshot = snapshotManager.snapshot(snapshotManager.latestSnapshotId());
                assertThat(snapshot.id()).isEqualTo(2);
                assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);

                write.close();
                commit.close();
            }
        }

        // sleep 3 seconds, and update partition 20221208-16
        Thread.sleep(3000);

        for (FileStoreTable table : tables) {
            StreamWriteBuilder streamWriteBuilder =
                    table.newStreamWriteBuilder().withCommitUser(commitUser);
            StreamTableWrite write = streamWriteBuilder.newWrite();
            StreamTableCommit commit = streamWriteBuilder.newCommit();
            writeData(write, commit, 2, rowData(3, 100, 16, BinaryString.fromString("20221208")));
        }

        StreamExecutionEnvironment env =
                streamExecutionEnvironmentBuilder()
                        .batchMode()
                        .parallelism(ThreadLocalRandom.current().nextInt(2) + 1)
                        .build();

        DataStream<RowData> source =
                new CombinedTableCompactorSourceBuilder(
                                catalogLoader(),
                                Pattern.compile("db1|db2"),
                                Pattern.compile(".*"),
                                null,
                                monitorInterval)
                        .withPartitionIdleTime(partitionIdleTime)
                        .withContinuousMode(false)
                        .withEnv(env)
                        .buildAwareBucketTableSource();
        CloseableIterator<RowData> it = source.executeAndCollect();
        List<String> actual = new ArrayList<>();
        while (it.hasNext()) {
            actual.add(toString(it.next()));
        }
        assertThat(actual)
                .hasSameElementsAs(
                        Arrays.asList(
                                "+I 3|20221208|15|0|0|db1|t1",
                                "+I 3|20221209|15|0|0|db1|t1",
                                "+I 3|20221208|15|0|0|db1|t2",
                                "+I 3|20221209|15|0|0|db1|t2",
                                "+I 3|20221208|15|0|0|db2|t1",
                                "+I 3|20221209|15|0|0|db2|t1",
                                "+I 3|20221208|15|0|0|db2|t2",
                                "+I 3|20221209|15|0|0|db2|t2"));
        it.close();
    }

    private FileStoreTable createTable(
            String databaseName,
            String tableName,
            RowType rowType,
            List<String> partitionKeys,
            List<String> primaryKeys,
            Map<String, String> options)
            throws Exception {
        try (Catalog catalog = catalogLoader().load()) {
            Identifier identifier = Identifier.create(databaseName, tableName);
            catalog.createDatabase(databaseName, true);
            catalog.createTable(
                    identifier,
                    new Schema(rowType.getFields(), partitionKeys, primaryKeys, options, ""),
                    false);
            return (FileStoreTable) catalog.getTable(identifier);
        }
    }

    private GenericRow rowData(Object... values) {
        return GenericRow.of(values);
    }

    private void writeData(
            StreamTableWrite write,
            StreamTableCommit commit,
            long incrementalIdentifier,
            GenericRow... data)
            throws Exception {
        for (GenericRow d : data) {
            write.write(d);
        }
        commit.commit(incrementalIdentifier, write.prepareCommit(true, incrementalIdentifier));
    }

    private String toString(RowData rowData) {
        int numFiles;
        DataFileMetaSerializer dataFileMetaSerializer = new DataFileMetaSerializer();
        try {
            numFiles = dataFileMetaSerializer.deserializeList(rowData.getBinary(3)).size();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        BinaryRow partition = deserializeBinaryRow(rowData.getBinary(1));

        return String.format(
                "%s %d|%s|%d|%d|%d|%s|%s",
                rowData.getRowKind().shortString(),
                rowData.getLong(0),
                partition.getString(0),
                partition.getInt(1),
                rowData.getInt(2),
                numFiles,
                rowData.getString(4),
                rowData.getString(5));
    }

    private BinaryRow binaryRow(String dt, int hh) {
        BinaryRow b = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(b);
        writer.writeString(0, BinaryString.fromString(dt));
        writer.writeInt(1, hh);
        writer.complete();
        return b;
    }

    private Catalog.Loader catalogLoader() {
        // to make the action workflow serializable
        catalogOptions.set(CatalogOptions.WAREHOUSE, warehouse);
        return () -> FlinkCatalogFactory.createPaimonCatalog(catalogOptions);
    }
}
