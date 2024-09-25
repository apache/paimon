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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CommonTestUtils;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link CompactAction}. */
public class CompactActionITCase extends CompactActionITCaseBase {

    private static final DataType[] FIELD_TYPES =
            new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.INT(), DataTypes.STRING()};

    private static final RowType ROW_TYPE =
            RowType.of(FIELD_TYPES, new String[] {"k", "v", "hh", "dt"});

    @Test
    @Timeout(60)
    public void testBatchCompact() throws Exception {
        FileStoreTable table =
                prepareTable(
                        Arrays.asList("dt", "hh"),
                        Arrays.asList("dt", "hh", "k"),
                        Collections.emptyList(),
                        Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "true"));

        writeData(
                rowData(1, 100, 15, BinaryString.fromString("20221208")),
                rowData(1, 100, 16, BinaryString.fromString("20221208")),
                rowData(1, 100, 15, BinaryString.fromString("20221209")));

        writeData(
                rowData(2, 100, 15, BinaryString.fromString("20221208")),
                rowData(2, 100, 16, BinaryString.fromString("20221208")),
                rowData(2, 100, 15, BinaryString.fromString("20221209")));

        checkLatestSnapshot(table, 2, Snapshot.CommitKind.APPEND);

        runAction(false);

        checkLatestSnapshot(table, 3, Snapshot.CommitKind.COMPACT);

        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        assertThat(splits.size()).isEqualTo(3);
        for (DataSplit split : splits) {
            if (split.partition().getInt(1) == 15) {
                // compacted
                assertThat(split.dataFiles().size()).isEqualTo(1);
            } else {
                // not compacted
                assertThat(split.dataFiles().size()).isEqualTo(2);
            }
        }
    }

    @Test
    public void testStreamingCompact() throws Exception {
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(CoreOptions.CHANGELOG_PRODUCER.key(), "full-compaction");
        tableOptions.put(CoreOptions.FULL_COMPACTION_DELTA_COMMITS.key(), "1");
        tableOptions.put(CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL.key(), "1s");
        tableOptions.put(CoreOptions.WRITE_ONLY.key(), "true");
        // test that dedicated compact job will expire snapshots
        tableOptions.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(), "3");
        tableOptions.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(), "3");

        FileStoreTable table =
                prepareTable(
                        Arrays.asList("dt", "hh"),
                        Arrays.asList("dt", "hh", "k"),
                        Collections.emptyList(),
                        tableOptions);

        // base records
        writeData(
                rowData(1, 100, 15, BinaryString.fromString("20221208")),
                rowData(1, 100, 16, BinaryString.fromString("20221208")),
                rowData(1, 100, 15, BinaryString.fromString("20221209")));

        checkLatestSnapshot(table, 1, Snapshot.CommitKind.APPEND);

        // no full compaction has happened, so plan should be empty
        StreamTableScan scan = table.newReadBuilder().newStreamScan();
        TableScan.Plan plan = scan.plan();
        assertThat(plan.splits()).isEmpty();

        runAction(true);

        // first full compaction
        validateResult(
                table,
                ROW_TYPE,
                scan,
                Arrays.asList("+I[1, 100, 15, 20221208]", "+I[1, 100, 15, 20221209]"),
                60_000);

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
                        "-U[1, 100, 15, 20221208]",
                        "-U[1, 100, 15, 20221209]"),
                60_000);

        // assert dedicated compact job will expire snapshots
        SnapshotManager snapshotManager = table.snapshotManager();
        CommonTestUtils.waitUtil(
                () ->
                        snapshotManager.latestSnapshotId() - 2
                                == snapshotManager.earliestSnapshotId(),
                Duration.ofSeconds(60_000),
                Duration.ofSeconds(100),
                String.format("Cannot validate snapshot expiration in %s milliseconds.", 60_000));
    }

    @ParameterizedTest(name = "mode = {0}")
    @ValueSource(booleans = {true, false})
    @Timeout(60)
    public void testHistoryPartitionCompact(boolean mode) throws Exception {
        String partitionIdleTime = "5s";
        FileStoreTable table;
        if (mode) {
            table =
                    prepareTable(
                            Arrays.asList("dt", "hh"),
                            Arrays.asList("dt", "hh", "k"),
                            Collections.emptyList(),
                            Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "true"));
        } else {
            // for unaware bucket table
            Map<String, String> tableOptions = new HashMap<>();
            tableOptions.put(CoreOptions.BUCKET.key(), "-1");
            tableOptions.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2");
            tableOptions.put(CoreOptions.COMPACTION_MAX_FILE_NUM.key(), "2");

            table =
                    prepareTable(
                            Arrays.asList("dt", "hh"),
                            Collections.emptyList(),
                            Collections.emptyList(),
                            tableOptions);
        }

        writeData(
                rowData(1, 100, 15, BinaryString.fromString("20221208")),
                rowData(1, 100, 16, BinaryString.fromString("20221208")),
                rowData(1, 100, 15, BinaryString.fromString("20221209")));

        writeData(
                rowData(2, 100, 15, BinaryString.fromString("20221208")),
                rowData(2, 100, 16, BinaryString.fromString("20221208")),
                rowData(2, 100, 15, BinaryString.fromString("20221209")));

        Thread.sleep(5000);
        writeData(rowData(3, 100, 16, BinaryString.fromString("20221208")));
        checkLatestSnapshot(table, 3, Snapshot.CommitKind.APPEND);

        CompactAction action =
                createAction(
                        CompactAction.class,
                        "compact",
                        "--warehouse",
                        warehouse,
                        "--database",
                        database,
                        "--table",
                        tableName,
                        "--partition_idle_time",
                        partitionIdleTime);
        StreamExecutionEnvironment env = streamExecutionEnvironmentBuilder().batchMode().build();
        action.withStreamExecutionEnvironment(env).build();
        env.execute();

        checkLatestSnapshot(table, 4, Snapshot.CommitKind.COMPACT);

        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        assertThat(splits.size()).isEqualTo(3);
        for (DataSplit split : splits) {
            if (split.partition().getInt(1) == 15) {
                // compacted
                assertThat(split.dataFiles().size()).isEqualTo(1);
            } else {
                // not compacted
                assertThat(split.dataFiles().size()).isEqualTo(3);
            }
        }
    }

    @Test
    public void testUnawareBucketStreamingCompact() throws Exception {
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL.key(), "1s");
        tableOptions.put(CoreOptions.BUCKET.key(), "-1");
        tableOptions.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2");
        tableOptions.put(CoreOptions.COMPACTION_MAX_FILE_NUM.key(), "2");

        FileStoreTable table =
                prepareTable(
                        Collections.singletonList("k"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        tableOptions);

        // base records
        writeData(
                rowData(1, 100, 15, BinaryString.fromString("20221208")),
                rowData(1, 100, 16, BinaryString.fromString("20221208")),
                rowData(1, 100, 15, BinaryString.fromString("20221209")));

        writeData(
                rowData(1, 100, 15, BinaryString.fromString("20221208")),
                rowData(1, 100, 16, BinaryString.fromString("20221208")),
                rowData(1, 100, 15, BinaryString.fromString("20221209")));

        checkLatestSnapshot(table, 2, Snapshot.CommitKind.APPEND);

        // repairing that the ut don't specify the real partition of table
        runActionForUnawareTable(true);

        // first compaction, snapshot will be 3
        checkFileAndRowSize(table, 3L, 30_000L, 1, 6);

        writeData(
                rowData(1, 101, 15, BinaryString.fromString("20221208")),
                rowData(1, 101, 16, BinaryString.fromString("20221208")),
                rowData(1, 101, 15, BinaryString.fromString("20221209")));

        // second compaction, snapshot will be 5
        checkFileAndRowSize(table, 5L, 30_000L, 1, 9);
    }

    @Test
    public void testUnawareBucketBatchCompact() throws Exception {
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(CoreOptions.BUCKET.key(), "-1");
        tableOptions.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2");
        tableOptions.put(CoreOptions.COMPACTION_MAX_FILE_NUM.key(), "2");

        FileStoreTable table =
                prepareTable(
                        Collections.singletonList("k"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        tableOptions);

        // base records
        writeData(
                rowData(1, 100, 15, BinaryString.fromString("20221208")),
                rowData(1, 100, 16, BinaryString.fromString("20221208")),
                rowData(1, 100, 15, BinaryString.fromString("20221209")));

        writeData(
                rowData(1, 100, 15, BinaryString.fromString("20221208")),
                rowData(1, 100, 16, BinaryString.fromString("20221208")),
                rowData(1, 100, 15, BinaryString.fromString("20221209")));

        checkLatestSnapshot(table, 2, Snapshot.CommitKind.APPEND);

        // repairing that the ut don't specify the real partition of table
        runActionForUnawareTable(false);

        // first compaction, snapshot will be 3.
        checkFileAndRowSize(table, 3L, 0L, 1, 6);
    }

    @Test
    public void testTableConf() throws Exception {
        prepareTable(
                Arrays.asList("dt", "hh"),
                Arrays.asList("dt", "hh", "k"),
                Collections.emptyList(),
                Collections.emptyMap());

        CompactAction compactAction =
                createAction(
                        CompactAction.class,
                        "compact",
                        "--warehouse",
                        warehouse,
                        "--database",
                        database,
                        "--table",
                        tableName,
                        "--table_conf",
                        FlinkConnectorOptions.SCAN_PARALLELISM.key() + "=6");

        assertThat(compactAction.table.options().get(FlinkConnectorOptions.SCAN_PARALLELISM.key()))
                .isEqualTo("6");
    }

    @Test
    public void testSpecifyNonPartitionField() throws Exception {
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(CoreOptions.WRITE_ONLY.key(), "true");
        tableOptions.put(CoreOptions.BUCKET.key(), "-1");

        //  compaction specify a non-partion field
        prepareTable(
                Collections.singletonList("v"),
                Arrays.asList(),
                Collections.emptyList(),
                tableOptions);

        // base records
        writeData(
                rowData(1, 100, 15, BinaryString.fromString("20221208")),
                rowData(1, 100, 16, BinaryString.fromString("20221208")),
                rowData(1, 100, 15, BinaryString.fromString("20221209")));

        Assertions.assertThatThrownBy(() -> runAction(false))
                .hasMessage("Only partition key can be specialized in compaction action.");
    }

    @Test
    public void testWrongUsage() throws Exception {
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(CoreOptions.WRITE_ONLY.key(), "true");
        tableOptions.put(CoreOptions.BUCKET.key(), "-1");

        prepareTable(
                Collections.singletonList("v"),
                Arrays.asList(),
                Collections.emptyList(),
                tableOptions);

        // partition_idle_time can not be used with order-strategy
        Assertions.assertThatThrownBy(
                        () ->
                                createAction(
                                        CompactAction.class,
                                        "compact",
                                        "--warehouse",
                                        warehouse,
                                        "--database",
                                        database,
                                        "--table",
                                        tableName,
                                        "--partition_idle_time",
                                        "5s",
                                        "--order_strategy",
                                        "zorder",
                                        "--order_by",
                                        "dt,hh"))
                .hasMessage("sort compact do not support 'partition_idle_time'.");
    }

    private FileStoreTable prepareTable(
            List<String> partitionKeys,
            List<String> primaryKeys,
            List<String> bucketKey,
            Map<String, String> tableOptions)
            throws Exception {
        FileStoreTable table =
                createFileStoreTable(ROW_TYPE, partitionKeys, primaryKeys, bucketKey, tableOptions);

        StreamWriteBuilder streamWriteBuilder =
                table.newStreamWriteBuilder().withCommitUser(commitUser);
        write = streamWriteBuilder.newWrite();
        commit = streamWriteBuilder.newCommit();

        return table;
    }

    private void checkLatestSnapshot(
            FileStoreTable table, long snapshotId, Snapshot.CommitKind commitKind) {
        SnapshotManager snapshotManager = table.snapshotManager();
        Snapshot snapshot = snapshotManager.snapshot(snapshotManager.latestSnapshotId());
        assertThat(snapshot.id()).isEqualTo(snapshotId);
        assertThat(snapshot.commitKind()).isEqualTo(commitKind);
    }

    private void runAction(boolean isStreaming) throws Exception {
        runAction(isStreaming, false);
    }

    private void runActionForUnawareTable(boolean isStreaming) throws Exception {
        runAction(isStreaming, true);
    }

    private void runAction(boolean isStreaming, boolean unawareBucket) throws Exception {
        StreamExecutionEnvironment env;
        if (isStreaming) {
            env = streamExecutionEnvironmentBuilder().streamingMode().build();
        } else {
            env = streamExecutionEnvironmentBuilder().batchMode().build();
        }

        ArrayList<String> baseArgs =
                Lists.newArrayList(
                        "compact",
                        "--warehouse",
                        warehouse,
                        "--database",
                        database,
                        "--table",
                        tableName);
        ThreadLocalRandom random = ThreadLocalRandom.current();
        if (unawareBucket) {
            if (true) {
                baseArgs.addAll(Lists.newArrayList("--where", "k=1"));
            } else {
                baseArgs.addAll(Lists.newArrayList("--partition", "k=1"));
            }
        } else {
            if (random.nextBoolean()) {
                baseArgs.addAll(
                        Lists.newArrayList(
                                "--where", "(dt=20221208 and hh=15) or (dt=20221209 and hh=15)"));
            } else {
                baseArgs.addAll(
                        Lists.newArrayList(
                                "--partition",
                                "dt=20221208,hh=15",
                                "--partition",
                                "dt=20221209,hh=15"));
            }
        }

        CompactAction action = createAction(CompactAction.class, baseArgs.toArray(new String[0]));

        action.withStreamExecutionEnvironment(env).build();
        if (isStreaming) {
            env.executeAsync();
        } else {
            env.execute();
        }
    }
}
