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
import org.apache.paimon.utils.SnapshotManager;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

import static org.apache.paimon.utils.CommonTestUtils.waitUtil;
import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link CompactDatabaseAction}. */
public class CompactDatabaseActionITCase extends CompactActionITCaseBase {

    private static final String[] DATABASE_NAMES = new String[] {"db1", "db2"};
    private static final String[] TABLE_NAMES = new String[] {"t1", "t2", "t3_unaware_bucket"};
    private static final String[] New_DATABASE_NAMES = new String[] {"db3", "db4"};
    private static final String[] New_TABLE_NAMES = new String[] {"t3", "t4"};
    private static final RowType ROW_TYPE =
            RowType.of(
                    new DataType[] {
                        DataTypes.INT(), DataTypes.INT(), DataTypes.INT(), DataTypes.STRING()
                    },
                    new String[] {"k", "v", "hh", "dt"});

    private static Stream<Arguments> testData() {
        return Stream.of(
                Arguments.of("combined", "action"),
                Arguments.of("divided", "action"),
                Arguments.of("combined", "procedure_indexed"),
                Arguments.of("divided", "procedure_indexed"),
                Arguments.of("combined", "procedure_named"),
                Arguments.of("divided", "procedure_named"));
    }

    private FileStoreTable createTable(
            String databaseName,
            String tableName,
            List<String> partitionKeys,
            List<String> primaryKeys,
            Map<String, String> options)
            throws Exception {
        Identifier identifier = Identifier.create(databaseName, tableName);
        catalog.createDatabase(databaseName, true);
        catalog.createTable(
                identifier,
                new Schema(ROW_TYPE.getFields(), partitionKeys, primaryKeys, options, ""),
                false);
        return (FileStoreTable) catalog.getTable(identifier);
    }

    @ParameterizedTest(name = "mode = {0}, invoker = {1}")
    @MethodSource("testData")
    @Timeout(6000)
    public void testStreamCompactForUnawareTable(String mode, String invoker) throws Exception {

        // step0. create tables
        Map<Identifier, FileStoreTable> tableToCompaction = new HashMap<>();
        for (String dbName : DATABASE_NAMES) {
            for (String tableName : TABLE_NAMES) {
                Map<String, String> option = new HashMap<>();
                option.put(CoreOptions.WRITE_ONLY.key(), "true");
                List<String> keys;
                if (tableName.endsWith("unaware_bucket")) {
                    option.put("bucket", "-1");
                    option.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2");
                    option.put(CoreOptions.COMPACTION_MAX_FILE_NUM.key(), "2");
                    keys = Lists.newArrayList();
                    FileStoreTable table =
                            createTable(dbName, tableName, Arrays.asList("dt", "hh"), keys, option);
                    tableToCompaction.put(Identifier.create(dbName, tableName), table);
                }
            }
        }

        // step1. run streaming compaction task for tables
        switch (invoker) {
            case "action":
                StreamExecutionEnvironment env =
                        streamExecutionEnvironmentBuilder().streamingMode().build();
                createAction(
                                CompactDatabaseAction.class,
                                "compact_database",
                                "--warehouse",
                                warehouse,
                                "--mode",
                                mode)
                        .withStreamExecutionEnvironment(env)
                        .build();
                env.executeAsync();
                break;
            case "procedure_indexed":
                executeSQL(String.format("CALL sys.compact_database('', '%s')", mode), true, false);
                break;
            case "procedure_named":
                executeSQL(
                        String.format("CALL sys.compact_database(mode => '%s')", mode),
                        true,
                        false);
                break;
            default:
                throw new UnsupportedOperationException(invoker);
        }

        // step3. write datas to table wait for compaction
        for (Map.Entry<Identifier, FileStoreTable> identifierFileStoreTableEntry :
                tableToCompaction.entrySet()) {
            FileStoreTable table = identifierFileStoreTableEntry.getValue();
            SnapshotManager snapshotManager = table.snapshotManager();
            StreamWriteBuilder streamWriteBuilder =
                    table.newStreamWriteBuilder().withCommitUser(commitUser);
            write = streamWriteBuilder.newWrite();
            commit = streamWriteBuilder.newCommit();

            writeData(
                    rowData(1, 100, 15, BinaryString.fromString("20221208")),
                    rowData(1, 100, 16, BinaryString.fromString("20221208")),
                    rowData(1, 100, 15, BinaryString.fromString("20221209")));

            writeData(
                    rowData(2, 100, 15, BinaryString.fromString("20221208")),
                    rowData(2, 100, 16, BinaryString.fromString("20221208")),
                    rowData(2, 100, 15, BinaryString.fromString("20221209")));

            Snapshot snapshot = snapshotManager.snapshot(snapshotManager.latestSnapshotId());
            assertThat(snapshot.id()).isEqualTo(2);
            assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);
            write.close();
            commit.close();
        }

        for (Map.Entry<Identifier, FileStoreTable> identifierFileStoreTableEntry :
                tableToCompaction.entrySet()) {
            FileStoreTable table = identifierFileStoreTableEntry.getValue();
            SnapshotManager snapshotManager = table.snapshotManager();
            while (true) {
                if (snapshotManager.latestSnapshotId() == 2) {
                    Thread.sleep(1000);
                } else {
                    validateResult(
                            table,
                            ROW_TYPE,
                            table.newReadBuilder().newStreamScan(),
                            Arrays.asList(
                                    "+I[1, 100, 15, 20221208]",
                                    "+I[1, 100, 15, 20221209]",
                                    "+I[1, 100, 16, 20221208]",
                                    "+I[2, 100, 15, 20221208]",
                                    "+I[2, 100, 15, 20221209]",
                                    "+I[2, 100, 16, 20221208]"),
                            60_000);
                    break;
                }
            }
        }
    }

    @ParameterizedTest(name = "mode = {0}, invoker = {1}")
    @MethodSource("testData")
    @Timeout(60)
    public void testBatchCompact(String mode, String invoker) throws Exception {
        List<FileStoreTable> tables = new ArrayList<>();

        for (String dbName : DATABASE_NAMES) {
            for (String tableName : TABLE_NAMES) {
                Map<String, String> option = new HashMap<>();
                option.put(CoreOptions.WRITE_ONLY.key(), "true");
                List<String> keys;
                if (tableName.endsWith("unaware_bucket")) {
                    option.put("bucket", "-1");
                    option.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2");
                    option.put(CoreOptions.COMPACTION_MAX_FILE_NUM.key(), "2");
                    keys = Lists.newArrayList();
                } else {
                    option.put("bucket", "1");
                    keys = Arrays.asList("dt", "hh", "k");
                }
                FileStoreTable table =
                        createTable(dbName, tableName, Arrays.asList("dt", "hh"), keys, option);
                tables.add(table);
                SnapshotManager snapshotManager = table.snapshotManager();
                StreamWriteBuilder streamWriteBuilder =
                        table.newStreamWriteBuilder().withCommitUser(commitUser);
                write = streamWriteBuilder.newWrite();
                commit = streamWriteBuilder.newCommit();

                writeData(
                        rowData(1, 100, 15, BinaryString.fromString("20221208")),
                        rowData(1, 100, 16, BinaryString.fromString("20221208")),
                        rowData(1, 100, 15, BinaryString.fromString("20221209")));

                writeData(
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

        switch (invoker) {
            case "action":
                StreamExecutionEnvironment env =
                        streamExecutionEnvironmentBuilder().batchMode().build();
                createAction(
                                CompactDatabaseAction.class,
                                "compact_database",
                                "--warehouse",
                                warehouse,
                                "--mode",
                                mode)
                        .withStreamExecutionEnvironment(env)
                        .build();
                env.execute();
                break;
            case "procedure_indexed":
                executeSQL(String.format("CALL sys.compact_database('', '%s')", mode), false, true);
                break;
            case "procedure_named":
                executeSQL(
                        String.format("CALL sys.compact_database(mode => '%s')", mode),
                        false,
                        true);
                break;
            default:
                throw new UnsupportedOperationException(invoker);
        }

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

    @ParameterizedTest(name = "mode = {0}, invoker = {1}")
    @MethodSource("testData")
    public void testStreamingCompact(String mode, String invoker) throws Exception {
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
        options.put("bucket", "1");

        List<FileStoreTable> tables = new ArrayList<>();
        for (String dbName : DATABASE_NAMES) {
            for (String tableName : TABLE_NAMES) {
                FileStoreTable table =
                        createTable(
                                dbName,
                                tableName,
                                Arrays.asList("dt", "hh"),
                                Arrays.asList("dt", "hh", "k"),
                                options);
                tables.add(table);
                SnapshotManager snapshotManager = table.snapshotManager();
                StreamWriteBuilder streamWriteBuilder =
                        table.newStreamWriteBuilder().withCommitUser(commitUser);
                write = streamWriteBuilder.newWrite();
                commit = streamWriteBuilder.newCommit();

                // base records
                writeData(
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
                write.close();
                commit.close();
            }
        }

        switch (invoker) {
            case "action":
                CompactDatabaseAction action;
                if (mode.equals("divided")) {
                    action =
                            createAction(
                                    CompactDatabaseAction.class,
                                    "compact_database",
                                    "--warehouse",
                                    warehouse);
                } else {
                    // if CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL.key() use default value, the
                    // cost
                    // time in combined mode will be over 1 min
                    action =
                            createAction(
                                    CompactDatabaseAction.class,
                                    "compact_database",
                                    "--warehouse",
                                    warehouse,
                                    "--mode",
                                    "combined",
                                    "--table_conf",
                                    CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL.key() + "=1s");
                }
                StreamExecutionEnvironment env =
                        streamExecutionEnvironmentBuilder().streamingMode().build();
                action.withStreamExecutionEnvironment(env).build();
                env.executeAsync();
                break;
            case "procedure_indexed":
                if (mode.equals("divided")) {
                    executeSQL("CALL sys.compact_database()", true, false);
                } else {
                    executeSQL(
                            "CALL sys.compact_database('', 'combined', '', '', 'continuous.discovery-interval=1s')",
                            true,
                            false);
                }
                break;
            case "procedure_named":
                if (mode.equals("divided")) {
                    executeSQL("CALL sys.compact_database()", true, false);
                } else {
                    executeSQL(
                            "CALL sys.compact_database(mode => 'combined', table_options => 'continuous.discovery-interval=1s')",
                            true,
                            false);
                }
                break;
            default:
                throw new UnsupportedOperationException(invoker);
        }

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
            write = streamWriteBuilder.newWrite();
            commit = streamWriteBuilder.newCommit();

            // incremental records
            writeData(
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
            waitUtil(
                    () ->
                            snapshotManager.latestSnapshotId() - 2
                                    == snapshotManager.earliestSnapshotId(),
                    Duration.ofSeconds(60),
                    Duration.ofMillis(100),
                    String.format(
                            "Cannot validate snapshot expiration in %s milliseconds.", 60_000));
            write.close();
            commit.close();
        }

        // In combined mode, check whether newly created table can be detected
        if (mode.equals("combined")) {
            // second create tables and write data to tables
            List<FileStoreTable> newtables = new ArrayList<>();
            for (String dbName : New_DATABASE_NAMES) {
                for (String tableName : New_TABLE_NAMES) {
                    FileStoreTable table =
                            createTable(
                                    dbName,
                                    tableName,
                                    Arrays.asList("dt", "hh"),
                                    Arrays.asList("dt", "hh", "k"),
                                    options);
                    newtables.add(table);
                    SnapshotManager snapshotManager = table.snapshotManager();
                    StreamWriteBuilder streamWriteBuilder =
                            table.newStreamWriteBuilder().withCommitUser(commitUser);
                    write = streamWriteBuilder.newWrite();
                    commit = streamWriteBuilder.newCommit();

                    // base records
                    writeData(
                            write,
                            commit,
                            0,
                            rowData(1, 100, 15, BinaryString.fromString("20221208")),
                            rowData(1, 100, 16, BinaryString.fromString("20221208")),
                            rowData(1, 100, 15, BinaryString.fromString("20221209")));

                    Snapshot snapshot =
                            snapshotManager.snapshot(snapshotManager.latestSnapshotId());
                    assertThat(snapshot.id()).isEqualTo(1);
                    assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);
                    write.close();
                    commit.close();
                }
            }

            for (FileStoreTable table : newtables) {
                StreamTableScan scan = table.newReadBuilder().newStreamScan();
                // first full compaction for new tables
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
                write = streamWriteBuilder.newWrite();
                commit = streamWriteBuilder.newCommit();

                // incremental records
                writeData(
                        write,
                        commit,
                        1,
                        rowData(1, 101, 15, BinaryString.fromString("20221208")),
                        rowData(1, 101, 16, BinaryString.fromString("20221208")),
                        rowData(1, 101, 15, BinaryString.fromString("20221209")));

                // second full compaction for new tables
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
                waitUtil(
                        () ->
                                snapshotManager.latestSnapshotId() - 2
                                        == snapshotManager.earliestSnapshotId(),
                        Duration.ofSeconds(60),
                        Duration.ofMillis(100),
                        String.format(
                                "Cannot validate snapshot expiration in %s milliseconds.", 60_000));
                write.close();
                commit.close();
            }
        }
    }

    @ParameterizedTest(name = "mode = {0}, invoker = {1}")
    @MethodSource("testData")
    @Timeout(60)
    public void testHistoryPartitionCompact(String mode, String invoker) throws Exception {
        List<FileStoreTable> tables = new ArrayList<>();
        String partitionIdleTime = "10s";

        for (String dbName : DATABASE_NAMES) {
            for (String tableName : TABLE_NAMES) {
                Map<String, String> option = new HashMap<>();
                option.put(CoreOptions.WRITE_ONLY.key(), "true");
                List<String> keys;
                if (tableName.endsWith("unaware_bucket")) {
                    option.put("bucket", "-1");
                    option.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2");
                    option.put(CoreOptions.COMPACTION_MAX_FILE_NUM.key(), "2");
                    keys = Lists.newArrayList();
                } else {
                    option.put("bucket", "1");
                    keys = Arrays.asList("dt", "hh", "k");
                }
                FileStoreTable table =
                        createTable(dbName, tableName, Arrays.asList("dt", "hh"), keys, option);
                tables.add(table);
                SnapshotManager snapshotManager = table.snapshotManager();
                StreamWriteBuilder streamWriteBuilder =
                        table.newStreamWriteBuilder().withCommitUser(commitUser);
                write = streamWriteBuilder.newWrite();
                commit = streamWriteBuilder.newCommit();

                writeData(
                        rowData(1, 100, 15, BinaryString.fromString("20221208")),
                        rowData(1, 100, 16, BinaryString.fromString("20221208")),
                        rowData(1, 100, 15, BinaryString.fromString("20221209")));

                writeData(
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

        // sleep 3s, update partition 20221208-16
        Thread.sleep(10000);
        for (FileStoreTable table : tables) {
            StreamWriteBuilder streamWriteBuilder =
                    table.newStreamWriteBuilder().withCommitUser(commitUser);
            write = streamWriteBuilder.newWrite();
            commit = streamWriteBuilder.newCommit();
            writeData(rowData(3, 100, 16, BinaryString.fromString("20221208")));
        }

        switch (invoker) {
            case "action":
                StreamExecutionEnvironment env =
                        streamExecutionEnvironmentBuilder().batchMode().build();
                createAction(
                                CompactDatabaseAction.class,
                                "compact_database",
                                "--warehouse",
                                warehouse,
                                "--mode",
                                mode,
                                "--partition_idle_time",
                                partitionIdleTime)
                        .withStreamExecutionEnvironment(env)
                        .build();
                env.execute();
                break;
            case "procedure_indexed":
                executeSQL(
                        String.format(
                                "CALL sys.compact_database('', '%s','','','','%s')",
                                mode, partitionIdleTime),
                        false,
                        true);
                break;
            case "procedure_named":
                executeSQL(
                        String.format(
                                "CALL sys.compact_database(mode => '%s', partition_idle_time => '%s')",
                                mode, partitionIdleTime),
                        false,
                        true);
                break;
            default:
                throw new UnsupportedOperationException(invoker);
        }

        for (FileStoreTable table : tables) {
            SnapshotManager snapshotManager = table.snapshotManager();
            Snapshot snapshot =
                    table.snapshotManager().snapshot(snapshotManager.latestSnapshotId());
            assertThat(snapshot.id()).isEqualTo(4);
            assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);

            List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
            assertThat(splits.size()).isEqualTo(3);
            for (DataSplit split : splits) {
                if (split.partition().getInt(1) == 16) {
                    assertThat(split.dataFiles().size()).isEqualTo(3);
                } else {
                    assertThat(split.dataFiles().size()).isEqualTo(1);
                }
            }
        }
    }

    @ParameterizedTest(name = "mode = {0}")
    @MethodSource("testData")
    @Timeout(60)
    public void includeTableCompaction(String mode, String invoker) throws Exception {
        includingAndExcludingTablesImpl(
                mode,
                invoker,
                "db1.t1",
                null,
                Collections.singletonList(Identifier.fromString("db1.t1")),
                Arrays.asList(
                        Identifier.fromString("db1.t2"),
                        Identifier.fromString("db2.t1"),
                        Identifier.fromString("db2.t2")));
    }

    @ParameterizedTest(name = "mode = {0}, invoker = {1}")
    @MethodSource("testData")
    @Timeout(60)
    public void excludeTableCompaction(String mode, String invoker) throws Exception {
        includingAndExcludingTablesImpl(
                mode,
                invoker,
                null,
                "db2.t2",
                Arrays.asList(
                        Identifier.fromString("db1.t1"),
                        Identifier.fromString("db1.t2"),
                        Identifier.fromString("db2.t1")),
                Collections.singletonList(Identifier.fromString("db2.t2")));
    }

    @ParameterizedTest(name = "mode = {0}, invoker = {1}")
    @MethodSource("testData")
    @Timeout(60)
    public void includeAndExcludeTableCompaction(String mode, String invoker) throws Exception {
        includingAndExcludingTablesImpl(
                mode,
                invoker,
                "db1.+|db2.t1",
                "db1.t2",
                Arrays.asList(Identifier.fromString("db1.t1"), Identifier.fromString("db2.t1")),
                Arrays.asList(Identifier.fromString("db1.t2"), Identifier.fromString("db2.t2")));
    }

    private void includingAndExcludingTablesImpl(
            String mode,
            String invoker,
            @Nullable String includingPattern,
            @Nullable String excludesPattern,
            List<Identifier> includeTables,
            List<Identifier> excludeTables)
            throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.WRITE_ONLY.key(), "true");
        options.put("bucket", "1");

        List<FileStoreTable> compactionTables = new ArrayList<>();
        List<FileStoreTable> noCompactionTables = new ArrayList<>();

        for (String dbName : DATABASE_NAMES) {
            for (String tableName : TABLE_NAMES) {
                FileStoreTable table =
                        createTable(
                                dbName,
                                tableName,
                                Arrays.asList("dt", "hh"),
                                Arrays.asList("dt", "hh", "k"),
                                options);
                if (includeTables.contains(Identifier.create(dbName, tableName))) {
                    compactionTables.add(table);
                } else if (excludeTables.contains(Identifier.create(dbName, tableName))) {
                    noCompactionTables.add(table);
                }

                SnapshotManager snapshotManager = table.snapshotManager();
                StreamWriteBuilder streamWriteBuilder =
                        table.newStreamWriteBuilder().withCommitUser(commitUser);
                write = streamWriteBuilder.newWrite();
                commit = streamWriteBuilder.newCommit();

                writeData(
                        rowData(1, 100, 15, BinaryString.fromString("20221208")),
                        rowData(1, 100, 16, BinaryString.fromString("20221208")),
                        rowData(1, 100, 15, BinaryString.fromString("20221209")));

                writeData(
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

        switch (invoker) {
            case "action":
                List<String> args = new ArrayList<>();
                args.add("compact_database");
                args.add("--warehouse");
                args.add(warehouse);
                if (includingPattern != null) {
                    args.add("--including_tables");
                    args.add(includingPattern);
                }
                if (excludesPattern != null) {
                    args.add("--excluding_tables");
                    args.add(excludesPattern);
                }
                args.add("--mode");
                args.add(mode);
                if (mode.equals("combined")) {
                    args.add("--table_conf");
                    args.add(CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL.key() + "=1s");
                }

                StreamExecutionEnvironment env =
                        streamExecutionEnvironmentBuilder().batchMode().build();
                createAction(CompactDatabaseAction.class, args)
                        .withStreamExecutionEnvironment(env)
                        .build();
                env.execute();
                break;
            case "procedure_indexed":
                if (mode.equals("divided")) {
                    executeSQL(
                            String.format(
                                    "CALL sys.compact_database('', 'divided', '%s', '%s')",
                                    nonNull(includingPattern), nonNull(excludesPattern)),
                            false,
                            true);
                } else {
                    executeSQL(
                            String.format(
                                    "CALL sys.compact_database('', 'combined', '%s', '%s', 'continuous.discovery-interval=1s')",
                                    nonNull(includingPattern), nonNull(excludesPattern)),
                            false,
                            true);
                }
                break;
            case "procedure_named":
                if (mode.equals("divided")) {
                    executeSQL(
                            String.format(
                                    "CALL sys.compact_database(mode => 'divided', including_tables => '%s', excluding_tables => '%s')",
                                    nonNull(includingPattern), nonNull(excludesPattern)),
                            false,
                            true);
                } else {
                    executeSQL(
                            String.format(
                                    "CALL sys.compact_database(mode => 'combined', including_tables => '%s', excluding_tables => '%s', table_options => 'continuous.discovery-interval=1s')",
                                    nonNull(includingPattern), nonNull(excludesPattern)),
                            false,
                            true);
                }
                break;
            default:
                throw new UnsupportedOperationException(invoker);
        }

        for (FileStoreTable table : compactionTables) {
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

        for (FileStoreTable table : noCompactionTables) {
            SnapshotManager snapshotManager = table.snapshotManager();
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

    private String nonNull(@Nullable String s) {
        return s == null ? "" : s;
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
                            Collections.singletonList("k"),
                            Collections.emptyList(),
                            options);
            tables.add(table);
            SnapshotManager snapshotManager = table.snapshotManager();
            StreamWriteBuilder streamWriteBuilder =
                    table.newStreamWriteBuilder().withCommitUser(commitUser);
            write = streamWriteBuilder.newWrite();
            commit = streamWriteBuilder.newCommit();

            // base records
            writeData(
                    rowData(1, 100, 15, BinaryString.fromString("20221208")),
                    rowData(1, 100, 16, BinaryString.fromString("20221208")),
                    rowData(1, 100, 15, BinaryString.fromString("20221209")));

            writeData(
                    rowData(1, 100, 15, BinaryString.fromString("20221208")),
                    rowData(1, 100, 16, BinaryString.fromString("20221208")),
                    rowData(1, 100, 15, BinaryString.fromString("20221209")));

            Snapshot snapshot = snapshotManager.snapshot(snapshotManager.latestSnapshotId());
            assertThat(snapshot.id()).isEqualTo(2);
            assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);
            write.close();
            commit.close();
        }

        if (ThreadLocalRandom.current().nextBoolean()) {
            StreamExecutionEnvironment env =
                    streamExecutionEnvironmentBuilder().streamingMode().build();
            createAction(CompactDatabaseAction.class, "compact_database", "--warehouse", warehouse)
                    .withStreamExecutionEnvironment(env)
                    .build();
            env.executeAsync();
        } else {
            executeSQL("CALL sys.compact_database()");
        }

        for (FileStoreTable table : tables) {
            StreamWriteBuilder streamWriteBuilder =
                    table.newStreamWriteBuilder().withCommitUser(commitUser);
            write = streamWriteBuilder.newWrite();
            commit = streamWriteBuilder.newCommit();

            // first compaction, snapshot will be 3
            checkFileAndRowSize(table, 3L, 30_000L, 1, 6);

            writeData(
                    rowData(1, 101, 15, BinaryString.fromString("20221208")),
                    rowData(1, 101, 16, BinaryString.fromString("20221208")),
                    rowData(1, 101, 15, BinaryString.fromString("20221209")));

            // second compaction, snapshot will be 5
            checkFileAndRowSize(table, 5L, 30_000L, 1, 9);
            write.close();
            commit.close();
        }
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
                            Collections.singletonList("k"),
                            Collections.emptyList(),
                            options);
            tables.add(table);
            SnapshotManager snapshotManager = table.snapshotManager();
            StreamWriteBuilder streamWriteBuilder =
                    table.newStreamWriteBuilder().withCommitUser(commitUser);
            write = streamWriteBuilder.newWrite();
            commit = streamWriteBuilder.newCommit();

            // base records
            writeData(
                    rowData(1, 100, 15, BinaryString.fromString("20221208")),
                    rowData(1, 100, 16, BinaryString.fromString("20221208")),
                    rowData(1, 100, 15, BinaryString.fromString("20221209")));

            writeData(
                    rowData(1, 100, 15, BinaryString.fromString("20221208")),
                    rowData(1, 100, 16, BinaryString.fromString("20221208")),
                    rowData(1, 100, 15, BinaryString.fromString("20221209")));

            Snapshot snapshot = snapshotManager.snapshot(snapshotManager.latestSnapshotId());
            assertThat(snapshot.id()).isEqualTo(2);
            assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);
            write.close();
            commit.close();
        }

        if (ThreadLocalRandom.current().nextBoolean()) {
            StreamExecutionEnvironment env =
                    streamExecutionEnvironmentBuilder().batchMode().build();
            createAction(CompactDatabaseAction.class, "compact_database", "--warehouse", warehouse)
                    .withStreamExecutionEnvironment(env)
                    .build();
            env.execute();
        } else {
            executeSQL("CALL sys.compact_database()", false, true);
        }

        for (FileStoreTable table : tables) {
            // first compaction, snapshot will be 3.
            checkFileAndRowSize(table, 3L, 0L, 1, 6);
        }
    }

    @Test
    public void testCombinedModeWithDynamicOptions() throws Exception {
        // create table and commit data
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.WRITE_ONLY.key(), "true");
        options.put(CoreOptions.BUCKET.key(), "1");
        options.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(), "1000");
        FileStoreTable table =
                createTable(
                        "test_db",
                        "t",
                        Arrays.asList("dt", "hh"),
                        Arrays.asList("dt", "hh", "k"),
                        options);

        StreamWriteBuilder streamWriteBuilder =
                table.newStreamWriteBuilder().withCommitUser(commitUser);
        write = streamWriteBuilder.newWrite();
        commit = streamWriteBuilder.newCommit();

        for (int i = 0; i < 10; i++) {
            writeData(rowData(1, i, 15, BinaryString.fromString("20221208")));
        }
        SnapshotManager snapshotManager = table.snapshotManager();
        assertThat(snapshotManager.latestSnapshotId()).isEqualTo(10);

        // if CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL.key() use default value, the cost
        // time in combined mode will be over 1 min
        CompactDatabaseAction action =
                createAction(
                        CompactDatabaseAction.class,
                        "compact_database",
                        "--warehouse",
                        warehouse,
                        "--mode",
                        "combined",
                        "--table_conf",
                        CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL.key() + "=1s",
                        // test dynamic options will be copied in commit
                        "--table_conf",
                        CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key() + "=3",
                        "--table_conf",
                        CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key() + "=3");

        StreamExecutionEnvironment env =
                streamExecutionEnvironmentBuilder().streamingMode().build();
        action.withStreamExecutionEnvironment(env).build();
        JobClient jobClient = env.executeAsync();

        waitUtil(
                () -> snapshotManager.latestSnapshotId() == 11L,
                Duration.ofSeconds(60),
                Duration.ofMillis(500));
        jobClient.cancel();

        assertThat(snapshotManager.latestSnapshot().commitKind())
                .isEqualTo(Snapshot.CommitKind.COMPACT);

        waitUtil(
                () -> snapshotManager.earliestSnapshotId() == 9L,
                Duration.ofSeconds(60),
                Duration.ofMillis(200),
                "Failed to wait snapshot expiration success");
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
