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
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TraceableFileIO;

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
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.paimon.utils.CommonTestUtils.waitUtil;
import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link CompactAction}. */
public class CompactActionITCase extends CompactActionITCaseBase {

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
    @Timeout(60)
    public void testCompactWhenSkipLevel0() throws Exception {
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(CoreOptions.WRITE_ONLY.key(), "true");
        // in dv mode or merge-engine = first-row, batch read will skip level-0
        if (ThreadLocalRandom.current().nextBoolean()) {
            tableOptions.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        } else {
            tableOptions.put(CoreOptions.MERGE_ENGINE.key(), "first-row");
        }
        tableOptions.put(CoreOptions.CHANGELOG_PRODUCER.key(), "lookup");

        FileStoreTable table =
                prepareTable(
                        Arrays.asList("dt", "hh"),
                        Arrays.asList("dt", "hh", "k"),
                        Collections.emptyList(),
                        tableOptions);

        writeData(
                rowData(1, 100, 15, BinaryString.fromString("20221208")),
                rowData(1, 100, 16, BinaryString.fromString("20221208")),
                rowData(1, 100, 15, BinaryString.fromString("20221209")));

        writeData(
                rowData(2, 100, 15, BinaryString.fromString("20221208")),
                rowData(2, 100, 16, BinaryString.fromString("20221208")),
                rowData(2, 100, 15, BinaryString.fromString("20221209")));

        checkLatestSnapshot(table, 2, Snapshot.CommitKind.APPEND);
        assertThat(table.newScan().plan().splits().size()).isEqualTo(0);

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
        waitUtil(
                () ->
                        snapshotManager.latestSnapshotId() - 2
                                == snapshotManager.earliestSnapshotId(),
                Duration.ofSeconds(60_000),
                Duration.ofSeconds(10),
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
    public void testStreamingCompactWithChangedExternalPath() throws Exception {
        String externalPath1 = getTempDirPath();
        String externalPath2 = getTempDirPath();

        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(CoreOptions.CHANGELOG_PRODUCER.key(), "full-compaction");
        tableOptions.put(CoreOptions.FULL_COMPACTION_DELTA_COMMITS.key(), "1");
        tableOptions.put(CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL.key(), "1s");
        tableOptions.put(
                CoreOptions.DATA_FILE_EXTERNAL_PATHS.key(),
                TraceableFileIO.SCHEME + "://" + externalPath1);
        tableOptions.put(CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY.key(), "round-robin");
        tableOptions.put(
                FlinkConnectorOptions.SINK_WRITER_REFRESH_DETECTORS.key(), "external-paths");
        tableOptions.put(CoreOptions.WRITE_ONLY.key(), "true");

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
        LocalFileIO fileIO = LocalFileIO.create();
        assertThat(fileIO.exists(new Path(externalPath2))).isFalse();
        assertThat(fileIO.listStatus(new Path(externalPath1)).length).isGreaterThanOrEqualTo(1);

        SchemaChange schemaChange =
                SchemaChange.setOption(
                        CoreOptions.DATA_FILE_EXTERNAL_PATHS.key(),
                        TraceableFileIO.SCHEME + "://" + externalPath2);
        table.schemaManager().commitChanges(schemaChange);
        // wait a checkpoint at least to wait the writer refresh
        Thread.sleep(1000);

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

        assertThat(fileIO.exists(new Path(externalPath2))).isTrue();
        assertThat(fileIO.listStatus(new Path(externalPath2)).length).isGreaterThanOrEqualTo(1);
    }

    @Test
    public void testStreamingCompactWithAddingColumns() throws Exception {
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(CoreOptions.CHANGELOG_PRODUCER.key(), "full-compaction");
        tableOptions.put(CoreOptions.FULL_COMPACTION_DELTA_COMMITS.key(), "1");
        tableOptions.put(CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL.key(), "1s");
        tableOptions.put(CoreOptions.WRITE_ONLY.key(), "true");

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
        runAction(true);
        checkLatestSnapshot(table, 2, Snapshot.CommitKind.COMPACT, 60_000);

        SchemaChange schemaChange = SchemaChange.addColumn("new_col", DataTypes.INT());
        table.schemaManager().commitChanges(schemaChange);
        // wait a checkpoint at least to wait the writer refresh
        Thread.sleep(1000);

        // incremental records
        table = getFileStoreTable(tableName);
        StreamWriteBuilder streamWriteBuilder =
                table.newStreamWriteBuilder().withCommitUser(commitUser);
        write = streamWriteBuilder.newWrite();
        commit = streamWriteBuilder.newCommit();
        writeData(
                rowData(1, 101, 15, BinaryString.fromString("20221208"), 1),
                rowData(1, 101, 16, BinaryString.fromString("20221208"), 1),
                rowData(2, 101, 15, BinaryString.fromString("20221209"), 2));
        checkLatestSnapshot(table, 3, Snapshot.CommitKind.APPEND);
        checkLatestSnapshot(table, 4, Snapshot.CommitKind.COMPACT, 60_000);

        List<String> res =
                getResult(
                        table.newRead(),
                        table.newSnapshotReader().read().splits(),
                        table.rowType());
        assertThat(res)
                .containsExactlyInAnyOrder(
                        "+I[1, 101, 15, 20221208, 1]",
                        "+I[1, 101, 16, 20221208, 1]",
                        "+I[1, 100, 15, 20221209, NULL]",
                        "+I[2, 101, 15, 20221209, 2]");
    }

    @Test
    public void testUnawareBucketStreamingCompact() throws Exception {
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL.key(), "1s");
        tableOptions.put(CoreOptions.BUCKET.key(), "-1");
        tableOptions.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2");

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
    public void testUnawareBucketStreamingCompactWithChangedExternalPath() throws Exception {
        String externalPath1 = getTempDirPath();
        String externalPath2 = getTempDirPath();

        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL.key(), "1s");
        tableOptions.put(CoreOptions.BUCKET.key(), "-1");
        tableOptions.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2");
        tableOptions.put(CoreOptions.WRITE_ONLY.key(), "true");
        tableOptions.put(
                CoreOptions.DATA_FILE_EXTERNAL_PATHS.key(),
                TraceableFileIO.SCHEME + "://" + externalPath1);
        tableOptions.put(CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY.key(), "round-robin");
        tableOptions.put(
                FlinkConnectorOptions.SINK_WRITER_REFRESH_DETECTORS.key(), "external-paths");

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
        LocalFileIO fileIO = LocalFileIO.create();
        assertThat(fileIO.exists(new Path(externalPath2))).isFalse();
        assertThat(fileIO.listStatus(new Path(externalPath1)).length).isGreaterThanOrEqualTo(1);

        SchemaChange schemaChange =
                SchemaChange.setOption(
                        CoreOptions.DATA_FILE_EXTERNAL_PATHS.key(),
                        TraceableFileIO.SCHEME + "://" + externalPath2);
        table.schemaManager().commitChanges(schemaChange);
        Thread.sleep(1000);

        writeData(
                rowData(1, 101, 15, BinaryString.fromString("20221208")),
                rowData(1, 101, 16, BinaryString.fromString("20221208")),
                rowData(1, 101, 15, BinaryString.fromString("20221209")));

        // second compaction, snapshot will be 5
        checkFileAndRowSize(table, 5L, 30_000L, 1, 9);

        assertThat(fileIO.exists(new Path(externalPath2))).isTrue();
        assertThat(fileIO.listStatus(new Path(externalPath2)).length).isGreaterThanOrEqualTo(1);
    }

    @Test
    public void testUnawareBucketStreamingCompactWithWithAddingColumns() throws Exception {
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL.key(), "1s");
        tableOptions.put(CoreOptions.BUCKET.key(), "-1");
        tableOptions.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2");
        tableOptions.put(CoreOptions.WRITE_ONLY.key(), "true");

        FileStoreTable table =
                prepareTable(
                        Collections.singletonList("k"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        tableOptions);

        // base records
        writeData(rowData(1, 100, 15, BinaryString.fromString("20221208")));
        writeData(rowData(1, 100, 16, BinaryString.fromString("20221208")));

        checkLatestSnapshot(table, 2, Snapshot.CommitKind.APPEND);
        // repairing that the ut don't specify the real partition of table
        runActionForUnawareTable(true);

        checkLatestSnapshot(table, 3, Snapshot.CommitKind.COMPACT, 60_000);

        SchemaChange schemaChange = SchemaChange.addColumn("new_col", DataTypes.INT());
        table.schemaManager().commitChanges(schemaChange);
        Thread.sleep(1000);

        table = getFileStoreTable(tableName);
        StreamWriteBuilder streamWriteBuilder =
                table.newStreamWriteBuilder().withCommitUser(commitUser);
        write = streamWriteBuilder.newWrite();
        commit = streamWriteBuilder.newCommit();
        writeData(rowData(1, 101, 15, BinaryString.fromString("20221208"), 1));
        checkLatestSnapshot(table, 4, Snapshot.CommitKind.APPEND);
        checkLatestSnapshot(table, 5, Snapshot.CommitKind.COMPACT, 60_000);

        List<String> res =
                getResult(
                        table.newRead(),
                        table.newSnapshotReader().read().splits(),
                        table.rowType());
        assertThat(res)
                .containsExactlyInAnyOrder(
                        "+I[1, 100, 15, 20221208, NULL]",
                        "+I[1, 100, 16, 20221208, NULL]",
                        "+I[1, 101, 15, 20221208, 1]");
    }

    @Test
    public void testUnawareBucketBatchCompact() throws Exception {
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(CoreOptions.BUCKET.key(), "-1");
        tableOptions.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2");

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
    public void testLotsOfPartitionsCompact() throws Exception {
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(CoreOptions.BUCKET.key(), "-1");
        tableOptions.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2");

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

        List<String> partitions = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            partitions.add("--partition");
            partitions.add("k=" + i);
        }

        // repairing that the ut don't specify the real partition of table
        runActionForUnawareTable(false, partitions);

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

    @Test
    public void testStreamingCompactWithEmptyOverwriteUpgrade() throws Exception {
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL.key(), "1s");
        tableOptions.put(CoreOptions.COMPACTION_FORCE_UP_LEVEL_0.key(), "true");
        tableOptions.put(CoreOptions.WRITE_ONLY.key(), "true");

        DataType[] fieldTypes = new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.INT()};
        RowType rowType = RowType.of(fieldTypes, new String[] {"k", "v", "pt"});
        FileStoreTable table =
                createFileStoreTable(
                        rowType,
                        Collections.singletonList("pt"),
                        Arrays.asList("k", "pt"),
                        Collections.emptyList(),
                        tableOptions);
        SnapshotManager snapshotManager = table.snapshotManager();

        StreamExecutionEnvironment env =
                streamExecutionEnvironmentBuilder().checkpointIntervalMs(500).build();
        createAction(
                        CompactAction.class,
                        "compact",
                        "--database",
                        database,
                        "--table",
                        tableName,
                        "--catalog_conf",
                        "warehouse=" + warehouse)
                .withStreamExecutionEnvironment(env)
                .build();
        env.executeAsync();

        StreamWriteBuilder streamWriteBuilder =
                table.newStreamWriteBuilder().withCommitUser(commitUser);
        write = streamWriteBuilder.newWrite();
        commit = streamWriteBuilder.newCommit();

        writeData(rowData(1, 100, 1), rowData(2, 200, 1), rowData(1, 100, 2));

        waitUtil(
                () -> {
                    Snapshot latest = snapshotManager.latestSnapshot();
                    return latest != null && latest.commitKind() == Snapshot.CommitKind.COMPACT;
                },
                Duration.ofSeconds(10),
                Duration.ofMillis(100));

        long snapshotId1 = snapshotManager.latestSnapshotId();

        // overwrite empty partition and let it upgrade
        String newCommitUser = UUID.randomUUID().toString();
        try (TableWriteImpl<?> newWrite = table.newWrite(newCommitUser);
                TableCommitImpl newCommit =
                        table.newCommit(newCommitUser)
                                .withOverwrite(Collections.singletonMap("pt", "3"))) {
            newWrite.write(rowData(1, 100, 3));
            newWrite.write(rowData(2, 200, 3));
            newCommit.commit(newWrite.prepareCommit(false, 1));
        }
        // write level 0 file to trigger compaction
        writeData(rowData(1, 101, 3));

        waitUtil(
                () -> {
                    Snapshot latest = snapshotManager.latestSnapshot();
                    return latest.id() > snapshotId1
                            && latest.commitKind() == Snapshot.CommitKind.COMPACT;
                },
                Duration.ofSeconds(10),
                Duration.ofMillis(100));

        validateResult(
                table,
                rowType,
                table.newStreamScan(),
                Arrays.asList(
                        "+I[1, 100, 1]",
                        "+I[1, 100, 2]",
                        "+I[1, 101, 3]",
                        "+I[2, 200, 1]",
                        "+I[2, 200, 3]"),
                60_000);
    }

    private void runAction(boolean isStreaming) throws Exception {
        runAction(isStreaming, false, Collections.emptyList());
    }

    private void runActionForUnawareTable(boolean isStreaming, List<String> extra)
            throws Exception {
        runAction(isStreaming, true, extra);
    }

    private void runActionForUnawareTable(boolean isStreaming) throws Exception {
        runAction(isStreaming, true, Collections.emptyList());
    }

    private void runAction(boolean isStreaming, boolean unawareBucket, List<String> extra)
            throws Exception {
        StreamExecutionEnvironment env;
        if (isStreaming) {
            env = streamExecutionEnvironmentBuilder().streamingMode().build();
        } else {
            env = streamExecutionEnvironmentBuilder().batchMode().build();
        }

        ArrayList<String> baseArgs =
                Lists.newArrayList("compact", "--database", database, "--table", tableName);

        ThreadLocalRandom random = ThreadLocalRandom.current();
        if (random.nextBoolean()) {
            baseArgs.addAll(Lists.newArrayList("--warehouse", warehouse));
        } else {
            baseArgs.addAll(Lists.newArrayList("--catalog_conf", "warehouse=" + warehouse));
        }

        if (unawareBucket) {
            if (random.nextBoolean()) {
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

        baseArgs.addAll(extra);

        CompactAction action = createAction(CompactAction.class, baseArgs.toArray(new String[0]));

        action.withStreamExecutionEnvironment(env).build();
        if (isStreaming) {
            env.executeAsync();
        } else {
            env.execute();
        }
    }
}
