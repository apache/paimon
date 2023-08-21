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
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.options.Options;
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
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

/** this is a doc. */
public class CompactActionForDbITCase extends ActionITCaseBase {
    private static final DataType[] FIELD_TYPES =
            new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.INT(), DataTypes.STRING()};

    private static final RowType ROW_TYPE =
            RowType.of(FIELD_TYPES, new String[] {"k", "v", "hh", "dt"});

    private Catalog catalog;

    @Test
    public void testBatchCompact() throws Exception {
        Map<String, String> options = new HashMap<>();
        //        options.put(CoreOptions.WRITE_ONLY.key(), "true");

        createDatabase();

        FileStoreTable table1 =
                createFileStoreTable(
                        "table1",
                        ROW_TYPE,
                        Arrays.asList("dt", "hh"),
                        Arrays.asList("dt", "hh", "k"),
                        options);
        FileStoreTable table2 =
                createFileStoreTable(
                        "table2",
                        ROW_TYPE,
                        Arrays.asList("dt", "hh"),
                        Arrays.asList("dt", "hh", "k"),
                        options);
        SnapshotManager snapshotManager1 = table1.snapshotManager();
        String commitUser1 = UUID.randomUUID().toString();
        StreamWriteBuilder streamWriteBuilder1 =
                table1.newStreamWriteBuilder().withCommitUser(commitUser1);
        StreamTableWrite write1 = streamWriteBuilder1.newWrite();
        StreamTableCommit commit1 = streamWriteBuilder1.newCommit();

        write1.write(rowData(1, 100, 15, BinaryString.fromString("20221208")));
        write1.write(rowData(1, 100, 16, BinaryString.fromString("20221208")));
        write1.write(rowData(1, 100, 15, BinaryString.fromString("20221209")));
        commit1.commit(0, write1.prepareCommit(true, 0));

        write1.write(rowData(2, 100, 15, BinaryString.fromString("20221208")));
        write1.write(rowData(2, 100, 16, BinaryString.fromString("20221208")));
        write1.write(rowData(2, 100, 15, BinaryString.fromString("20221209")));
        commit1.commit(1, write1.prepareCommit(true, 1));

        Snapshot snapshot = snapshotManager1.snapshot(snapshotManager1.latestSnapshotId());
        assertThat(snapshot.id()).isEqualTo(2);
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);

        // ******************************** table2

        SnapshotManager snapshotManager2 = table2.snapshotManager();
        String commitUser2 = UUID.randomUUID().toString();
        StreamWriteBuilder streamWriteBuilder2 =
                table2.newStreamWriteBuilder().withCommitUser(commitUser2);
        StreamTableWrite write2 = streamWriteBuilder2.newWrite();
        StreamTableCommit commit2 = streamWriteBuilder2.newCommit();

        write2.write(rowData(1, 100, 15, BinaryString.fromString("20221208")));
        write2.write(rowData(1, 100, 16, BinaryString.fromString("20221208")));
        write2.write(rowData(1, 100, 15, BinaryString.fromString("20221209")));
        commit2.commit(0, write2.prepareCommit(true, 0));

        write2.write(rowData(2, 100, 15, BinaryString.fromString("20221208")));
        write2.write(rowData(2, 100, 16, BinaryString.fromString("20221208")));
        write2.write(rowData(2, 100, 15, BinaryString.fromString("20221209")));
        commit2.commit(1, write2.prepareCommit(true, 1));

        Snapshot snapshot2 = snapshotManager2.snapshot(snapshotManager2.latestSnapshotId());
        assertThat(snapshot2.id()).isEqualTo(2);
        assertThat(snapshot2.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(ThreadLocalRandom.current().nextInt(2) + 1);
        new CompactActionForDb(
                        warehouse,
                        database,
                        table1.bucketMode(),
                        Options.fromMap(table1.options()),
                        table1.coreOptions())
                .build(env);
        env.execute();

        snapshot = snapshotManager1.snapshot(snapshotManager1.latestSnapshotId());
        assertThat(snapshot.id()).isEqualTo(3);
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);

        List<DataSplit> splits = table1.newSnapshotReader().read().dataSplits();
        assertThat(splits.size()).isEqualTo(3);
        for (DataSplit split : splits) {
            assertThat(split.dataFiles().size()).isEqualTo(1);
        }

        snapshot2 = snapshotManager2.snapshot(snapshotManager2.latestSnapshotId());
        assertThat(snapshot2.id()).isEqualTo(3);
        assertThat(snapshot2.commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);

        splits = table2.newSnapshotReader().read().dataSplits();
        assertThat(splits.size()).isEqualTo(3);
        for (DataSplit split : splits) {
            assertThat(split.dataFiles().size()).isEqualTo(1);
        }
    }

    @Test
    public void testStreamingCompact() throws Exception {
        Map<String, String> options = new HashMap<>();
        createDatabase();

        options.put(CoreOptions.CHANGELOG_PRODUCER.key(), "full-compaction");
        options.put(
                FlinkConnectorOptions.CHANGELOG_PRODUCER_FULL_COMPACTION_TRIGGER_INTERVAL.key(),
                "1s");
        options.put(CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL.key(), "1s");
        //        options.put(CoreOptions.WRITE_ONLY.key(), "true");
        // test that dedicated compact job will expire snapshots
        options.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(), "3");
        options.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(), "3");

        FileStoreTable table =
                createFileStoreTable(
                        "table1",
                        ROW_TYPE,
                        Arrays.asList("dt", "hh"),
                        Arrays.asList("dt", "hh", "k"),
                        options);
        snapshotManager = table.snapshotManager();
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

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointInterval(500);
        env.setParallelism(ThreadLocalRandom.current().nextInt(2) + 1);

        new CompactActionForDb(
                        warehouse,
                        database,
                        table.bucketMode(),
                        Options.fromMap(table.options()),
                        table.coreOptions())
                .build(env);
        JobClient client = env.executeAsync();

        // first full compaction
        validateResult(
                table,
                scan,
                Arrays.asList(
                        "+I[1, 100, 15, 20221208]",
                        "+I[1, 100, 15, 20221209]",
                        "+I[1, 100, 16, 20221208]"),
                60_000);

        // incremental records
        writeData(
                rowData(1, 101, 15, BinaryString.fromString("20221208")),
                rowData(1, 101, 16, BinaryString.fromString("20221208")),
                rowData(1, 101, 15, BinaryString.fromString("20221209")));

        // second full compaction
        validateResult(
                table,
                scan,
                Arrays.asList(
                        "+U[1, 101, 15, 20221208]",
                        "+U[1, 101, 15, 20221209]",
                        "+U[1, 101, 16, 20221208]",
                        "-U[1, 100, 15, 20221208]",
                        "-U[1, 100, 15, 20221209]",
                        "-U[1, 100, 16, 20221208]"),
                60_000);
        System.out.println("ok");

        // assert dedicated compact job will expire snapshots
        CommonTestUtils.waitUtil(
                () ->
                        snapshotManager.latestSnapshotId() - 2
                                == snapshotManager.earliestSnapshotId(),
                Duration.ofSeconds(60_000),
                Duration.ofSeconds(100),
                String.format("Cannot validate snapshot expiration in %s milliseconds.", 60_000));

        client.cancel();
    }

    protected void createDatabase() throws Catalog.DatabaseAlreadyExistException {
        catalog = catalog();
        catalog.createDatabase(database, true);
    }

    protected FileStoreTable createFileStoreTable(
            String tableName,
            RowType rowType,
            List<String> partitionKeys,
            List<String> primaryKeys,
            Map<String, String> options)
            throws Exception {
        Identifier identifier = Identifier.create(database, tableName);
        catalog.createTable(
                identifier,
                new Schema(rowType.getFields(), partitionKeys, primaryKeys, options, ""),
                false);
        return (FileStoreTable) catalog.getTable(identifier);
    }

    private void validateResult(
            FileStoreTable table, StreamTableScan scan, List<String> expected, long timeout)
            throws Exception {
        List<String> actual = new ArrayList<>();
        long start = System.currentTimeMillis();
        while (actual.size() != expected.size()) {
            TableScan.Plan plan = scan.plan();
            actual.addAll(getResult(table.newReadBuilder().newRead(), plan.splits(), ROW_TYPE));
            //            for (String line : actual) {
            //                System.out.println(line);
            //            }

            if (System.currentTimeMillis() - start > timeout) {
                break;
            }
        }
        if (actual.size() != expected.size()) {
            throw new TimeoutException(
                    String.format(
                            "Cannot collect %s records in %s milliseconds.",
                            expected.size(), timeout));
        }
        actual.sort(String::compareTo);
        assertThat(actual).isEqualTo(expected);
    }
}
