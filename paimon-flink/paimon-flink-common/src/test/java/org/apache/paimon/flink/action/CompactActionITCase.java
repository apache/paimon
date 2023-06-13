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
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.table.AbstractFileStoreTable;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;

/** IT cases for {@link CompactAction}. */
public class CompactActionITCase extends ActionITCaseBase {

    private static final DataType[] FIELD_TYPES =
            new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.INT(), DataTypes.STRING()};

    private static final RowType ROW_TYPE =
            RowType.of(FIELD_TYPES, new String[] {"k", "v", "hh", "dt"});

    @Test
    @Timeout(60)
    public void testBatchCompact() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.WRITE_ONLY.key(), "true");

        FileStoreTable table =
                createFileStoreTable(
                        ROW_TYPE,
                        Arrays.asList("dt", "hh"),
                        Arrays.asList("dt", "hh", "k"),
                        options);
        snapshotManager = table.snapshotManager();
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
        Assertions.assertEquals(2, snapshot.id());
        Assertions.assertEquals(Snapshot.CommitKind.APPEND, snapshot.commitKind());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(ThreadLocalRandom.current().nextInt(2) + 1);
        new CompactAction(warehouse, database, tableName)
                .withPartitions(getSpecifiedPartitions())
                .build(env);
        env.execute();

        snapshot = snapshotManager.snapshot(snapshotManager.latestSnapshotId());
        Assertions.assertEquals(3, snapshot.id());
        Assertions.assertEquals(Snapshot.CommitKind.COMPACT, snapshot.commitKind());

        List<DataSplit> splits = table.newSnapshotSplitReader().splits();
        Assertions.assertEquals(3, splits.size());
        for (DataSplit split : splits) {
            if (split.partition().getInt(1) == 15) {
                // compacted
                Assertions.assertEquals(1, split.files().size());
            } else {
                // not compacted
                Assertions.assertEquals(2, split.files().size());
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

        FileStoreTable table =
                createFileStoreTable(
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
        Assertions.assertEquals(1, snapshot.id());
        Assertions.assertEquals(Snapshot.CommitKind.APPEND, snapshot.commitKind());

        // no full compaction has happened, so plan should be empty
        StreamTableScan scan = table.newReadBuilder().newStreamScan();
        TableScan.Plan plan = scan.plan();
        Assertions.assertTrue(plan.splits().isEmpty());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointInterval(500);
        env.setParallelism(ThreadLocalRandom.current().nextInt(2) + 1);
        new CompactAction(warehouse, database, tableName)
                .withPartitions(getSpecifiedPartitions())
                .build(env);
        JobClient client = env.executeAsync();

        // first full compaction
        validateResult(
                table,
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
                scan,
                Arrays.asList(
                        "+U[1, 101, 15, 20221208]",
                        "+U[1, 101, 15, 20221209]",
                        "-U[1, 100, 15, 20221208]",
                        "-U[1, 100, 15, 20221209]"),
                60_000);

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

    @Test
    public void testUnawareBucketStreamingCompact() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL.key(), "1s");
        // test that dedicated compact job will expire snapshots
        options.put(CoreOptions.BUCKET.key(), "-1");
        options.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2");
        options.put(CoreOptions.COMPACTION_MAX_FILE_NUM.key(), "2");

        FileStoreTable table =
                createFileStoreTable(
                        ROW_TYPE, Arrays.asList("k"), Collections.emptyList(), options);
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

        writeData(
                rowData(1, 100, 15, BinaryString.fromString("20221208")),
                rowData(1, 100, 16, BinaryString.fromString("20221208")),
                rowData(1, 100, 15, BinaryString.fromString("20221209")));

        Snapshot snapshot = snapshotManager.snapshot(snapshotManager.latestSnapshotId());
        Assertions.assertEquals(2, snapshot.id());
        Assertions.assertEquals(Snapshot.CommitKind.APPEND, snapshot.commitKind());

        FileStoreScan storeScan = ((AbstractFileStoreTable) table).store().newScan();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointInterval(500);
        env.setParallelism(ThreadLocalRandom.current().nextInt(2) + 1);
        new CompactAction(warehouse, database, tableName).build(env);
        JobClient client = env.executeAsync();

        // first compaction, snapshot will be 3
        checkFileAndRowSize(storeScan, 3L, 30_000L, 1, 6);

        writeData(
                rowData(1, 101, 15, BinaryString.fromString("20221208")),
                rowData(1, 101, 16, BinaryString.fromString("20221208")),
                rowData(1, 101, 15, BinaryString.fromString("20221209")));

        // second compaction, snapshot will be 5
        checkFileAndRowSize(storeScan, 5L, 30_000L, 1, 9);

        client.cancel().get();
    }

    @Test
    public void testUnawareBucketBatchCompact() throws Exception {
        Map<String, String> options = new HashMap<>();
        // test that dedicated compact job will expire snapshots
        options.put(CoreOptions.BUCKET.key(), "-1");
        options.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2");
        options.put(CoreOptions.COMPACTION_MAX_FILE_NUM.key(), "2");

        FileStoreTable table =
                createFileStoreTable(
                        ROW_TYPE, Arrays.asList("k"), Collections.emptyList(), options);
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

        writeData(
                rowData(1, 100, 15, BinaryString.fromString("20221208")),
                rowData(1, 100, 16, BinaryString.fromString("20221208")),
                rowData(1, 100, 15, BinaryString.fromString("20221209")));

        Snapshot snapshot = snapshotManager.snapshot(snapshotManager.latestSnapshotId());
        Assertions.assertEquals(2, snapshot.id());
        Assertions.assertEquals(Snapshot.CommitKind.APPEND, snapshot.commitKind());

        FileStoreScan storeScan = ((AbstractFileStoreTable) table).store().newScan();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(ThreadLocalRandom.current().nextInt(2) + 1);
        new CompactAction(warehouse, database, tableName).build(env);
        env.execute();

        // first compaction, snapshot will be 3.
        checkFileAndRowSize(storeScan, 3L, 0L, 1, 6);

        writeData(
                rowData(1, 101, 15, BinaryString.fromString("20221208")),
                rowData(1, 101, 16, BinaryString.fromString("20221208")),
                rowData(1, 101, 15, BinaryString.fromString("20221209")));
    }

    private List<Map<String, String>> getSpecifiedPartitions() {
        Map<String, String> partition1 = new HashMap<>();
        partition1.put("dt", "20221208");
        partition1.put("hh", "15");

        Map<String, String> partition2 = new HashMap<>();
        partition2.put("dt", "20221209");
        partition2.put("hh", "15");

        return Arrays.asList(partition1, partition2);
    }

    private void validateResult(
            FileStoreTable table, StreamTableScan scan, List<String> expected, long timeout)
            throws Exception {
        List<String> actual = new ArrayList<>();
        long start = System.currentTimeMillis();
        while (actual.size() != expected.size()) {
            TableScan.Plan plan = scan.plan();
            actual.addAll(getResult(table.newReadBuilder().newRead(), plan.splits(), ROW_TYPE));

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
        Assertions.assertEquals(expected, actual);
    }

    private void checkFileAndRowSize(
            FileStoreScan scan, Long expectedSnapshotId, Long timeout, int fileNum, long rowCount)
            throws Exception {

        long start = System.currentTimeMillis();
        while (!Objects.equals(snapshotManager.latestSnapshotId(), expectedSnapshotId)) {
            Thread.sleep(500);
            if (System.currentTimeMillis() - start > timeout) {
                throw new RuntimeException("can't wait for a compaction.");
            }
        }

        List<ManifestEntry> files =
                scan.withSnapshot(snapshotManager.latestSnapshotId()).plan().files(FileKind.ADD);
        long count = 0;
        for (ManifestEntry file : files) {
            count += file.file().rowCount();
        }
        Assertions.assertEquals(fileNum, files.size());
        Assertions.assertEquals(rowCount, count);
    }
}
