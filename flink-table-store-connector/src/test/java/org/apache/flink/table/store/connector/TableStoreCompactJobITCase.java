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

package org.apache.flink.table.store.connector;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.mergetree.compact.ConcatRecordReader;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.file.schema.UpdateSchema;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.file.utils.RecordReaderIterator;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.FileStoreTableFactory;
import org.apache.flink.table.store.table.sink.TableCommit;
import org.apache.flink.table.store.table.sink.TableWrite;
import org.apache.flink.table.store.table.source.DataSplit;
import org.apache.flink.table.store.table.source.DataTableScan;
import org.apache.flink.table.store.table.source.Split;
import org.apache.flink.table.store.table.source.TableRead;
import org.apache.flink.table.store.table.source.snapshot.ContinuousDataFileSnapshotEnumerator;
import org.apache.flink.table.store.table.source.snapshot.SnapshotEnumerator;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** IT cases for {@link TableStoreCompactJob}. */
public class TableStoreCompactJobITCase extends AbstractTestBase {

    private static final RowType ROW_TYPE =
            RowType.of(
                    new LogicalType[] {
                        DataTypes.INT().getLogicalType(),
                        DataTypes.INT().getLogicalType(),
                        DataTypes.INT().getLogicalType(),
                        DataTypes.INT().getLogicalType()
                    },
                    new String[] {"dt", "hh", "k", "v"});

    private Path tablePath;
    private String commitUser;

    @Before
    public void before() throws IOException {
        tablePath = new Path(TEMPORARY_FOLDER.newFolder().toString());
        commitUser = UUID.randomUUID().toString();
    }

    @Test(timeout = 60000)
    public void testBatchCompact() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.WRITE_COMPACTION_SKIP.key(), "true");

        FileStoreTable table = createFileStoreTable(options);
        SnapshotManager snapshotManager = table.snapshotManager();
        TableWrite write = table.newWrite(commitUser);
        TableCommit commit = table.newCommit(commitUser);

        write.write(rowData(20221208, 15, 1, 100));
        write.write(rowData(20221208, 16, 1, 100));
        write.write(rowData(20221209, 15, 1, 100));
        commit.commit(0, write.prepareCommit(true, 0));

        write.write(rowData(20221208, 15, 2, 200));
        write.write(rowData(20221208, 16, 2, 200));
        write.write(rowData(20221209, 15, 2, 200));
        commit.commit(1, write.prepareCommit(true, 1));

        Snapshot snapshot = snapshotManager.snapshot(snapshotManager.latestSnapshotId());
        Assert.assertEquals(2, snapshot.id());
        Assert.assertEquals(Snapshot.CommitKind.APPEND, snapshot.commitKind());

        write.close();
        commit.close();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        JobClient jobClient =
                TableStoreCompactJob.runCompactJob(
                        tablePath, "dt=20221208,hh=15/dt=20221209,hh=15", env);
        while (true) {
            if (jobClient.getJobStatus().get().isGloballyTerminalState()) {
                break;
            }
            Thread.sleep(1000);
        }

        snapshot = snapshotManager.snapshot(snapshotManager.latestSnapshotId());
        Assert.assertEquals(3, snapshot.id());
        Assert.assertEquals(Snapshot.CommitKind.COMPACT, snapshot.commitKind());

        DataTableScan.DataFilePlan plan = table.newScan().plan();
        Assert.assertEquals(3, plan.splits().size());
        for (DataSplit split : plan.splits) {
            if (split.partition().getInt(1) == 15) {
                // compacted
                Assert.assertEquals(1, split.files().size());
            } else {
                // not compacted
                Assert.assertEquals(2, split.files().size());
            }
        }
    }

    @Test(timeout = 60000)
    public void testStreamingCompact() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.CHANGELOG_PRODUCER.key(), "full-compaction");
        options.put(CoreOptions.CHANGELOG_PRODUCER_FULL_COMPACTION_TRIGGER_INTERVAL.key(), "1s");
        options.put(CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL.key(), "1s");
        options.put(CoreOptions.WRITE_COMPACTION_SKIP.key(), "true");

        FileStoreTable table = createFileStoreTable(options);
        SnapshotManager snapshotManager = table.snapshotManager();
        TableWrite write = table.newWrite(commitUser);
        TableCommit commit = table.newCommit(commitUser);

        // base records
        write.write(rowData(20221208, 15, 1, 100));
        write.write(rowData(20221208, 16, 1, 100));
        write.write(rowData(20221209, 15, 1, 100));
        commit.commit(0, write.prepareCommit(true, 0));

        Snapshot snapshot = snapshotManager.snapshot(snapshotManager.latestSnapshotId());
        Assert.assertEquals(1, snapshot.id());
        Assert.assertEquals(Snapshot.CommitKind.APPEND, snapshot.commitKind());

        // no full compaction has happened, so plan should be empty
        SnapshotEnumerator snapshotEnumerator =
                ContinuousDataFileSnapshotEnumerator.create(table, table.newScan(), null);
        DataTableScan.DataFilePlan plan = snapshotEnumerator.enumerate();
        Assert.assertEquals(1, (long) plan.snapshotId);
        Assert.assertTrue(plan.splits().isEmpty());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointInterval(500);
        TableStoreCompactJob.runCompactJob(tablePath, "dt=20221208,hh=15/dt=20221209,hh=15", env);

        while (true) {
            plan = snapshotEnumerator.enumerate();
            if (plan != null) {
                break;
            }
            Thread.sleep(1000);
        }

        // first full compaction
        Assert.assertEquals(2, (long) plan.snapshotId);
        List<String> actual = getResult(table.newRead(), plan.splits());
        actual.sort(String::compareTo);
        Assert.assertEquals(Arrays.asList("+I 20221208|15|1|100", "+I 20221209|15|1|100"), actual);

        // incremental records
        write.write(rowData(20221208, 15, 1, 101));
        write.write(rowData(20221208, 16, 1, 101));
        write.write(rowData(20221209, 15, 1, 101));
        commit.commit(1, write.prepareCommit(true, 1));

        write.close();
        commit.close();

        while (true) {
            plan = snapshotEnumerator.enumerate();
            if (plan != null) {
                break;
            }
            Thread.sleep(1000);
        }

        // second full compaction
        Assert.assertEquals(4, (long) plan.snapshotId);
        actual = getResult(table.newRead(), plan.splits());
        actual.sort(String::compareTo);
        Assert.assertEquals(
                Arrays.asList(
                        "+U 20221208|15|1|101",
                        "+U 20221209|15|1|101",
                        "-U 20221208|15|1|100",
                        "-U 20221209|15|1|100"),
                actual);
    }

    private GenericRowData rowData(Object... values) {
        return GenericRowData.of(values);
    }

    private List<String> getResult(TableRead read, List<Split> splits) throws Exception {
        List<ConcatRecordReader.ReaderSupplier<RowData>> readers = new ArrayList<>();
        for (Split split : splits) {
            readers.add(() -> read.createReader(split));
        }
        RecordReader<RowData> recordReader = ConcatRecordReader.create(readers);
        RecordReaderIterator<RowData> iterator = new RecordReaderIterator<>(recordReader);
        List<String> result = new ArrayList<>();
        while (iterator.hasNext()) {
            RowData rowData = iterator.next();
            result.add(rowDataToString(rowData));
        }
        iterator.close();
        return result;
    }

    private String rowDataToString(RowData rowData) {
        return String.format(
                "%s %d|%d|%d|%d",
                rowData.getRowKind().shortString(),
                rowData.getInt(0),
                rowData.getInt(1),
                rowData.getInt(2),
                rowData.getInt(3));
    }

    private FileStoreTable createFileStoreTable(Map<String, String> options) throws Exception {
        SchemaManager schemaManager = new SchemaManager(tablePath);
        TableSchema tableSchema =
                schemaManager.commitNewVersion(
                        new UpdateSchema(
                                ROW_TYPE,
                                Arrays.asList("dt", "hh"),
                                Arrays.asList("dt", "hh", "k"),
                                options,
                                ""));
        return FileStoreTableFactory.create(tablePath, tableSchema);
    }
}
