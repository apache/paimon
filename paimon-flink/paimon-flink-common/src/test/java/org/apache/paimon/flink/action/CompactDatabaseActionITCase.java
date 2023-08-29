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

package org.apache.paimon.flink.action;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CommonTestUtils;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link CompactDatabaseAction}. */
public class CompactDatabaseActionITCase extends CompactActionITCaseBase {

    private static final String[] DATABASE_NAMES = new String[] {"db1", "db2"};
    private static final String[] TABLE_NAMES = new String[] {"t1", "t2"};
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
                    DataTypes.INT(), DataTypes.BIGINT(), DataTypes.INT(), DataTypes.STRING()
                };

        ROW_TYPE_MAP.put("t1", RowType.of(dataTypes1, new String[] {"k", "v", "hh", "dt"}));
        ROW_TYPE_MAP.put("t2", RowType.of(dataTypes2, new String[] {"k", "v1", "hh", "dt"}));
    }

    private FileStoreTable createTable(
            String databaseName,
            String tableName,
            RowType rowType,
            List<String> partitionKeys,
            List<String> primaryKeys,
            Map<String, String> options)
            throws Exception {

        Identifier identifier = Identifier.create(databaseName, tableName);
        catalog.createDatabase(databaseName, true);
        catalog.createTable(
                identifier,
                new Schema(rowType.getFields(), partitionKeys, primaryKeys, options, ""),
                false);
        return (FileStoreTable) catalog.getTable(identifier);
    }

    @Test
    @Timeout(60)
    public void testBatchCompact() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.WRITE_ONLY.key(), "true");

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
                snapshotManager = table.snapshotManager();
                StreamWriteBuilder streamWriteBuilder =
                        table.newStreamWriteBuilder().withCommitUser(commitUser);
                write = streamWriteBuilder.newWrite();
                commit = streamWriteBuilder.newCommit();

                Object value = null;
                if (tableName.equals("t1")) {
                    value = 100;
                } else if (tableName.equals("t2")) {
                    value = 100L;
                }

                writeData(
                        rowData(1, value, 15, BinaryString.fromString("20221208")),
                        rowData(1, value, 16, BinaryString.fromString("20221208")),
                        rowData(1, value, 15, BinaryString.fromString("20221209")));

                writeData(
                        rowData(2, value, 15, BinaryString.fromString("20221208")),
                        rowData(2, value, 16, BinaryString.fromString("20221208")),
                        rowData(2, value, 15, BinaryString.fromString("20221209")));

                Snapshot snapshot = snapshotManager.snapshot(snapshotManager.latestSnapshotId());
                assertThat(snapshot.id()).isEqualTo(2);
                assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);
            }
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(ThreadLocalRandom.current().nextInt(2) + 1);
        new CompactDatabaseAction(warehouse, "db1|db2", null, null, new HashMap<>()).build(env);
        env.execute();

        for (FileStoreTable table : tables) {
            Snapshot snapshot =
                    table.snapshotManager().snapshot(snapshotManager.latestSnapshotId());
            assertThat(snapshot.id()).isEqualTo(3);
            assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);

            List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
            assertThat(splits.size()).isEqualTo(3);
            for (DataSplit split : splits) {
                assertThat(split.dataFiles().size()).isEqualTo(1);
            }
        }
    }

    @Test
    @Timeout(60)
    public void includeTableCompaction() throws Exception {
        includingAndExcludingTablesImpl(
                "db1.t1",
                null,
                Collections.singletonList(Identifier.fromString("db1.t1")),
                Arrays.asList(
                        Identifier.fromString("db1.t2"),
                        Identifier.fromString("db2.t1"),
                        Identifier.fromString("db2.t2")));
    }

    @Test
    @Timeout(60)
    public void excludeTableCompaction() throws Exception {
        includingAndExcludingTablesImpl(
                null,
                "db2.t2",
                Arrays.asList(
                        Identifier.fromString("db1.t1"),
                        Identifier.fromString("db1.t2"),
                        Identifier.fromString("db2.t1")),
                Collections.singletonList(Identifier.fromString("db2.t2")));
    }

    @Test
    @Timeout(60)
    public void includeAndExcludeTableCompaction() throws Exception {
        includingAndExcludingTablesImpl(
                "db1.+|db2.t1",
                "db1.t2",
                Arrays.asList(Identifier.fromString("db1.t1"), Identifier.fromString("db2.t1")),
                Arrays.asList(Identifier.fromString("db1.t2"), Identifier.fromString("db2.t2")));
    }

    private void includingAndExcludingTablesImpl(
            String includingPattern,
            String excludesPattern,
            List<Identifier> includeTables,
            List<Identifier> excludeTables)
            throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.WRITE_ONLY.key(), "true");

        List<FileStoreTable> compactionTables = new ArrayList<>();
        List<FileStoreTable> noCompactionTables = new ArrayList<>();

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
                if (includeTables.contains(Identifier.create(dbName, tableName))) {
                    compactionTables.add(table);
                } else if (excludeTables.contains(Identifier.create(dbName, tableName))) {
                    noCompactionTables.add(table);
                }

                snapshotManager = table.snapshotManager();
                StreamWriteBuilder streamWriteBuilder =
                        table.newStreamWriteBuilder().withCommitUser(commitUser);
                write = streamWriteBuilder.newWrite();
                commit = streamWriteBuilder.newCommit();

                Object value = null;
                if (tableName.equals("t1")) {
                    value = 100;
                } else if (tableName.equals("t2")) {
                    value = 100L;
                }

                writeData(
                        rowData(1, value, 15, BinaryString.fromString("20221208")),
                        rowData(1, value, 16, BinaryString.fromString("20221208")),
                        rowData(1, value, 15, BinaryString.fromString("20221209")));

                writeData(
                        rowData(2, value, 15, BinaryString.fromString("20221208")),
                        rowData(2, value, 16, BinaryString.fromString("20221208")),
                        rowData(2, value, 15, BinaryString.fromString("20221209")));

                Snapshot snapshot = snapshotManager.snapshot(snapshotManager.latestSnapshotId());
                assertThat(snapshot.id()).isEqualTo(2);
                assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);
            }
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(ThreadLocalRandom.current().nextInt(2) + 1);
        new CompactDatabaseAction(
                        warehouse, "db1|db2", includingPattern, excludesPattern, new HashMap<>())
                .build(env);
        env.execute();

        for (FileStoreTable table : compactionTables) {
            snapshotManager = table.snapshotManager();
            Snapshot snapshot =
                    table.snapshotManager().snapshot(snapshotManager.latestSnapshotId());

            assertThat(snapshot.id()).isEqualTo(3);
            assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);

            List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
            assertThat(splits.size()).isEqualTo(3);
            for (DataSplit split : splits) {
                assertThat(split.dataFiles().size()).isEqualTo(1);
            }
        }

        for (FileStoreTable table : noCompactionTables) {
            snapshotManager = table.snapshotManager();
            Snapshot snapshot =
                    table.snapshotManager().snapshot(snapshotManager.latestSnapshotId());

            assertThat(snapshot.id()).isEqualTo(2);
            assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);

            List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
            assertThat(splits.size()).isEqualTo(3);
            for (DataSplit split : splits) {
                assertThat(split.dataFiles().size()).isEqualTo(2);
            }
        }
    }

    @Test
    public void testStreamingCompact() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.CHANGELOG_PRODUCER.key(), "full-compaction");
        options.put(
                FlinkConnectorOptions.CHANGELOG_PRODUCER_FULL_COMPACTION_TRIGGER_INTERVAL.key(),
                "1s");
        options.put(CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL.key(), "1s");
        options.put(CoreOptions.WRITE_ONLY.key(), "true");
        // test that dedicated compact job will expire snapshots
        options.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(), "3");
        options.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(), "3");

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
                snapshotManager = table.snapshotManager();
                StreamWriteBuilder streamWriteBuilder =
                        table.newStreamWriteBuilder().withCommitUser(commitUser);
                write = streamWriteBuilder.newWrite();
                commit = streamWriteBuilder.newCommit();

                Object value = null;
                if (tableName.equals("t1")) {
                    value = 100;
                } else if (tableName.equals("t2")) {
                    value = 100L;
                }

                // base records
                writeData(
                        rowData(1, value, 15, BinaryString.fromString("20221208")),
                        rowData(1, value, 16, BinaryString.fromString("20221208")),
                        rowData(1, value, 15, BinaryString.fromString("20221209")));

                Snapshot snapshot = snapshotManager.snapshot(snapshotManager.latestSnapshotId());
                assertThat(snapshot.id()).isEqualTo(1);
                assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);

                // no full compaction has happened, so plan should be empty
                StreamTableScan scan = table.newReadBuilder().newStreamScan();
                TableScan.Plan plan = scan.plan();
                assertThat(plan.splits()).isEmpty();
            }
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointInterval(500);
        env.setParallelism(ThreadLocalRandom.current().nextInt(2) + 1);
        new CompactDatabaseAction(warehouse, "db1|db2", null, null, new HashMap<>()).build(env);
        JobClient client = env.executeAsync();

        for (FileStoreTable table : tables) {
            StreamTableScan scan = table.newReadBuilder().newStreamScan();
            // first full compaction
            validateResult(
                    table,
                    ROW_TYPE_MAP.get(table.name()),
                    scan,
                    Arrays.asList(
                            "+I[1, 100, 15, 20221208]",
                            "+I[1, 100, 15, 20221209]",
                            "+I[1, 100, 16, 20221208]"),
                    60_000);

            Object value = null;
            String tName = table.name();
            if (tName.equals("t1")) {
                value = 101;
            } else if (tName.equals("t2")) {
                value = 101L;
            }

            snapshotManager = table.snapshotManager();
            StreamWriteBuilder streamWriteBuilder =
                    table.newStreamWriteBuilder().withCommitUser(commitUser);
            write = streamWriteBuilder.newWrite();
            commit = streamWriteBuilder.newCommit();

            // incremental records
            writeData(
                    rowData(1, value, 15, BinaryString.fromString("20221208")),
                    rowData(1, value, 16, BinaryString.fromString("20221208")),
                    rowData(1, value, 15, BinaryString.fromString("20221209")));

            // second full compaction
            validateResult(
                    table,
                    ROW_TYPE_MAP.get(table.name()),
                    scan,
                    Arrays.asList(
                            "+U[1, 101, 15, 20221208]",
                            "+U[1, 101, 15, 20221209]",
                            "+U[1, 101, 16, 20221208]",
                            "-U[1, 100, 15, 20221208]",
                            "-U[1, 100, 15, 20221209]",
                            "-U[1, 100, 16, 20221208]"),
                    60_000);

            // assert dedicated compact job will expire snapshots
            CommonTestUtils.waitUtil(
                    () ->
                            snapshotManager.latestSnapshotId() - 2
                                    == snapshotManager.earliestSnapshotId(),
                    Duration.ofSeconds(60_000),
                    Duration.ofSeconds(100),
                    String.format(
                            "Cannot validate snapshot expiration in %s milliseconds.", 60_000));
        }

        client.cancel();
    }

    @Test
    public void testUnawareBucketStreamingCompact() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL.key(), "1s");
        // test that dedicated compact job will expire snapshots
        options.put(CoreOptions.BUCKET.key(), "-1");
        options.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2");
        options.put(CoreOptions.COMPACTION_MAX_FILE_NUM.key(), "2");

        List<FileStoreTable> tables = new ArrayList<>();
        for (String tableName : TABLE_NAMES) {
            FileStoreTable table =
                    createTable(
                            database,
                            tableName,
                            ROW_TYPE_MAP.get(tableName),
                            Arrays.asList("k"),
                            Collections.emptyList(),
                            options);
            tables.add(table);
            snapshotManager = table.snapshotManager();
            StreamWriteBuilder streamWriteBuilder =
                    table.newStreamWriteBuilder().withCommitUser(commitUser);
            write = streamWriteBuilder.newWrite();
            commit = streamWriteBuilder.newCommit();

            Object value = null;
            if (tableName.equals("t1")) {
                value = 100;
            } else if (tableName.equals("t2")) {
                value = 100L;
            }

            // base records
            writeData(
                    rowData(1, value, 15, BinaryString.fromString("20221208")),
                    rowData(1, value, 16, BinaryString.fromString("20221208")),
                    rowData(1, value, 15, BinaryString.fromString("20221209")));

            writeData(
                    rowData(1, value, 15, BinaryString.fromString("20221208")),
                    rowData(1, value, 16, BinaryString.fromString("20221208")),
                    rowData(1, value, 15, BinaryString.fromString("20221209")));

            Snapshot snapshot = snapshotManager.snapshot(snapshotManager.latestSnapshotId());
            assertThat(snapshot.id()).isEqualTo(2);
            assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointInterval(500);
        env.setParallelism(ThreadLocalRandom.current().nextInt(2) + 1);
        new CompactDatabaseAction(warehouse, database, null, null, new HashMap<>()).build(env);
        JobClient client = env.executeAsync();

        for (FileStoreTable table : tables) {
            FileStoreScan storeScan = table.store().newScan();

            snapshotManager = table.snapshotManager();
            StreamWriteBuilder streamWriteBuilder =
                    table.newStreamWriteBuilder().withCommitUser(commitUser);
            write = streamWriteBuilder.newWrite();
            commit = streamWriteBuilder.newCommit();

            // first compaction, snapshot will be 3
            checkFileAndRowSize(storeScan, 3L, 30_000L, 1, 6);

            Object value = null;
            String tName = table.name();
            if (tName.equals("t1")) {
                value = 101;
            } else if (tName.equals("t2")) {
                value = 101L;
            }

            writeData(
                    rowData(1, value, 15, BinaryString.fromString("20221208")),
                    rowData(1, value, 16, BinaryString.fromString("20221208")),
                    rowData(1, value, 15, BinaryString.fromString("20221209")));

            // second compaction, snapshot will be 5
            checkFileAndRowSize(storeScan, 5L, 30_000L, 1, 9);
        }

        client.cancel().get();
    }

    @Test
    public void testUnawareBucketBatchCompact() throws Exception {
        Map<String, String> options = new HashMap<>();
        // test that dedicated compact job will expire snapshots
        options.put(CoreOptions.BUCKET.key(), "-1");
        options.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2");
        options.put(CoreOptions.COMPACTION_MAX_FILE_NUM.key(), "2");

        List<FileStoreTable> tables = new ArrayList<>();
        for (String tableName : TABLE_NAMES) {
            FileStoreTable table =
                    createTable(
                            database,
                            tableName,
                            ROW_TYPE_MAP.get(tableName),
                            Collections.singletonList("k"),
                            Collections.emptyList(),
                            options);
            tables.add(table);
            snapshotManager = table.snapshotManager();
            StreamWriteBuilder streamWriteBuilder =
                    table.newStreamWriteBuilder().withCommitUser(commitUser);
            write = streamWriteBuilder.newWrite();
            commit = streamWriteBuilder.newCommit();

            Object value = null;
            if (tableName.equals("t1")) {
                value = 100;
            } else if (tableName.equals("t2")) {
                value = 100L;
            }

            // base records
            writeData(
                    rowData(1, value, 15, BinaryString.fromString("20221208")),
                    rowData(1, value, 16, BinaryString.fromString("20221208")),
                    rowData(1, value, 15, BinaryString.fromString("20221209")));

            writeData(
                    rowData(1, value, 15, BinaryString.fromString("20221208")),
                    rowData(1, value, 16, BinaryString.fromString("20221208")),
                    rowData(1, value, 15, BinaryString.fromString("20221209")));

            Snapshot snapshot = snapshotManager.snapshot(snapshotManager.latestSnapshotId());
            assertThat(snapshot.id()).isEqualTo(2);
            assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(ThreadLocalRandom.current().nextInt(2) + 1);
        new CompactDatabaseAction(warehouse, database, null, null, new HashMap<>()).build(env);
        env.execute();

        for (FileStoreTable table : tables) {
            FileStoreScan storeScan = table.store().newScan();
            snapshotManager = table.snapshotManager();
            // first compaction, snapshot will be 3.
            checkFileAndRowSize(storeScan, 3L, 0L, 1, 6);
        }
    }
}
