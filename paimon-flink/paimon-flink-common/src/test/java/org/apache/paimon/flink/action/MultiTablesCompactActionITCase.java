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
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CommonTestUtils;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** this is a doc. */
public class MultiTablesCompactActionITCase extends CompactActionITCaseBase {
    private static final String[] DATABASE_NAMES = new String[] {"db1", "db2"};
    private static final String[] TABLE_NAMES = new String[] {"t1", "t2"};
    private static final String[] New_DATABASE_NAMES = new String[] {"db3"};
    private static final String[] New_TABLE_NAMES = new String[] {"t3"};
    private static final RowType ROW_TYPE =
            RowType.of(
                    new DataType[] {
                        DataTypes.INT(), DataTypes.INT(), DataTypes.INT(), DataTypes.STRING()
                    },
                    new String[] {"k", "v", "hh", "dt"});

    @Test
    public void testBatchCompact() throws Exception {
        Map<String, String> compactOptions = new HashMap<>();
        compactOptions.put(CoreOptions.WRITE_ONLY.key(), "true");

        List<FileStoreTable> tables = new ArrayList<>();

        for (String dbName : DATABASE_NAMES) {
            for (String tableName : TABLE_NAMES) {
                FileStoreTable table =
                        createTable(
                                dbName,
                                tableName,
                                ROW_TYPE,
                                Arrays.asList("dt", "hh"),
                                Arrays.asList("dt", "hh", "k"),
                                compactOptions);
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

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(ThreadLocalRandom.current().nextInt(2) + 1);

        new MultiTablesCompactAction(warehouse, null, null, null, new HashMap<>(), compactOptions)
                .build(env);

        env.execute();

        for (FileStoreTable table : tables) {
            SnapshotManager snapshotManager = table.snapshotManager();
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
    public void testStreamingCompact() throws Exception {
        Map<String, String> compactOptions = new HashMap<>();
        compactOptions.put(CoreOptions.CHANGELOG_PRODUCER.key(), "full-compaction");
        compactOptions.put(
                FlinkConnectorOptions.CHANGELOG_PRODUCER_FULL_COMPACTION_TRIGGER_INTERVAL.key(),
                "1s");
        compactOptions.put(CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL.key(), "1s");
        compactOptions.put(CoreOptions.WRITE_ONLY.key(), "true");
        // test that dedicated compact job will expire snapshots
        compactOptions.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(), "3");
        compactOptions.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(), "3");

        List<FileStoreTable> tables = new ArrayList<>();
        for (String dbName : DATABASE_NAMES) {
            for (String tableName : TABLE_NAMES) {
                FileStoreTable table =
                        createTable(
                                dbName,
                                tableName,
                                ROW_TYPE,
                                Arrays.asList("dt", "hh"),
                                Arrays.asList("dt", "hh", "k"),
                                compactOptions);
                tables.add(table);
                SnapshotManager snapshotManager = table.snapshotManager();
                StreamWriteBuilder streamWriteBuilder =
                        table.newStreamWriteBuilder().withCommitUser(commitUser);
                StreamTableWrite write = streamWriteBuilder.newWrite();
                StreamTableCommit commit = streamWriteBuilder.newCommit();

                // base records
                writeData(
                        write,
                        commit,
                        0,
                        rowData(1, 100, 15, BinaryString.fromString("20221208")),
                        rowData(1, 100, 16, BinaryString.fromString("20221208")),
                        rowData(1, 100, 15, BinaryString.fromString("20221209")));

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
        new MultiTablesCompactAction(warehouse, null, null, null, new HashMap<>(), compactOptions)
                .build(env);
        JobClient client = env.executeAsync();

        for (FileStoreTable table : tables) {
            StreamTableScan scan = table.newReadBuilder().newStreamScan();
            // first full compaction
            validateResult(
                    table,
                    ROW_TYPE,
                    scan,
                    Arrays.asList(
                            "+I[1, 100, 15, 20221208]",
                            "+I[1, 100, 15, 20221209]",
                            "+I[1, 100, 16, 20221208]"),
                    60_000);

            SnapshotManager snapshotManager = table.snapshotManager();
            StreamWriteBuilder streamWriteBuilder =
                    table.newStreamWriteBuilder().withCommitUser(commitUser);
            StreamTableWrite write = streamWriteBuilder.newWrite();
            StreamTableCommit commit = streamWriteBuilder.newCommit();

            // incremental records
            writeData(
                    write,
                    commit,
                    1,
                    rowData(1, 101, 15, BinaryString.fromString("20221208")),
                    rowData(1, 101, 16, BinaryString.fromString("20221208")),
                    rowData(1, 101, 15, BinaryString.fromString("20221209")));

            // second full compaction
            validateResult(
                    table,
                    ROW_TYPE,
                    scan,
                    Arrays.asList(
                            "+U[1, 101, 15, 20221208]",
                            "+U[1, 101, 15, 20221209]",
                            "+U[1, 101, 16, 20221208]",
                            "-U[1, 100, 15, 20221208]",
                            "-U[1, 100, 15, 20221209]",
                            "-U[1, 100, 16, 20221208]"),
                    60_000);

            //             assert dedicated compact job will expire snapshots
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

    // In streaming mode, test if newly created tables can be detected
    @Test
    public void testNewlyCreatedTablesCompact() throws Exception {
        Map<String, String> compactOptions = new HashMap<>();
        compactOptions.put(CoreOptions.CHANGELOG_PRODUCER.key(), "full-compaction");
        compactOptions.put(
                FlinkConnectorOptions.CHANGELOG_PRODUCER_FULL_COMPACTION_TRIGGER_INTERVAL.key(),
                "1s");
        compactOptions.put(CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL.key(), "1s");
        compactOptions.put(CoreOptions.WRITE_ONLY.key(), "true");
        // test that dedicated compact job will expire snapshots
        compactOptions.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(), "3");
        compactOptions.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(), "3");

        List<FileStoreTable> tables = new ArrayList<>();

        // first create tables and write data to tables
        for (String dbName : DATABASE_NAMES) {
            for (String tableName : TABLE_NAMES) {
                FileStoreTable table =
                        createTable(
                                dbName,
                                tableName,
                                ROW_TYPE,
                                Arrays.asList("dt", "hh"),
                                Arrays.asList("dt", "hh", "k"),
                                compactOptions);
                tables.add(table);
                SnapshotManager snapshotManager = table.snapshotManager();
                StreamWriteBuilder streamWriteBuilder =
                        table.newStreamWriteBuilder().withCommitUser(commitUser);
                StreamTableWrite write = streamWriteBuilder.newWrite();
                StreamTableCommit commit = streamWriteBuilder.newCommit();

                // base records
                writeData(
                        write,
                        commit,
                        0,
                        rowData(1, 100, 15, BinaryString.fromString("20221208")),
                        rowData(1, 100, 16, BinaryString.fromString("20221208")),
                        rowData(1, 100, 15, BinaryString.fromString("20221209")));

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
        new MultiTablesCompactAction(warehouse, null, null, null, new HashMap<>(), compactOptions)
                .build(env);
        JobClient client = env.executeAsync();

        // second create tables and write data to tables
        for (String dbName : New_DATABASE_NAMES) {
            for (String tableName : New_TABLE_NAMES) {
                FileStoreTable table =
                        createTable(
                                dbName,
                                tableName,
                                ROW_TYPE,
                                Arrays.asList("dt", "hh"),
                                Arrays.asList("dt", "hh", "k"),
                                compactOptions);
                tables.add(table);
                SnapshotManager snapshotManager = table.snapshotManager();
                StreamWriteBuilder streamWriteBuilder =
                        table.newStreamWriteBuilder().withCommitUser(commitUser);
                StreamTableWrite write = streamWriteBuilder.newWrite();
                StreamTableCommit commit = streamWriteBuilder.newCommit();

                // base records
                writeData(
                        write,
                        commit,
                        0,
                        rowData(1, 100, 15, BinaryString.fromString("20221208")),
                        rowData(1, 100, 16, BinaryString.fromString("20221208")),
                        rowData(1, 100, 15, BinaryString.fromString("20221209")));

                Snapshot snapshot = snapshotManager.snapshot(snapshotManager.latestSnapshotId());
                assertThat(snapshot.id()).isEqualTo(1);
                assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);

                // no full compaction has happened, so plan should be empty
                StreamTableScan scan = table.newReadBuilder().newStreamScan();
                TableScan.Plan plan = scan.plan();
                assertThat(plan.splits()).isEmpty();
            }
        }

        // newly created tables should be detected
        for (FileStoreTable table : tables) {
            StreamTableScan scan = table.newReadBuilder().newStreamScan();
            // first full compaction
            validateResult(
                    table,
                    ROW_TYPE,
                    scan,
                    Arrays.asList(
                            "+I[1, 100, 15, 20221208]",
                            "+I[1, 100, 15, 20221209]",
                            "+I[1, 100, 16, 20221208]"),
                    60_000);

            SnapshotManager snapshotManager = table.snapshotManager();
            StreamWriteBuilder streamWriteBuilder =
                    table.newStreamWriteBuilder().withCommitUser(commitUser);
            StreamTableWrite write = streamWriteBuilder.newWrite();
            StreamTableCommit commit = streamWriteBuilder.newCommit();

            // incremental records
            writeData(
                    write,
                    commit,
                    1,
                    rowData(1, 101, 15, BinaryString.fromString("20221208")),
                    rowData(1, 101, 16, BinaryString.fromString("20221208")),
                    rowData(1, 101, 15, BinaryString.fromString("20221209")));

            // second full compaction
            validateResult(
                    table,
                    ROW_TYPE,
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

    private FileStoreTable createTable(
            String databaseName,
            String tableName,
            RowType rowType,
            List<String> partitionKeys,
            List<String> primaryKeys,
            Map<String, String> options)
            throws Exception {

        Catalog catalog = catalog();
        Identifier identifier = Identifier.create(databaseName, tableName);
        catalog.createDatabase(databaseName, true);
        catalog.createTable(
                identifier,
                new Schema(rowType.getFields(), partitionKeys, primaryKeys, options, ""),
                false);
        return (FileStoreTable) catalog.getTable(identifier);
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
}
