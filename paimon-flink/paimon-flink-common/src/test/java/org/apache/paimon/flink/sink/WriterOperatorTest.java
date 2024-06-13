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

package org.apache.paimon.flink.sink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.utils.InternalRowTypeSerializer;
import org.apache.paimon.flink.utils.InternalTypeInfo;
import org.apache.paimon.flink.utils.TestingMetricUtils;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TableWriteOperator}. */
public class WriterOperatorTest {

    @TempDir public java.nio.file.Path tempDir;
    private Path tablePath;

    @BeforeEach
    public void before() {
        tablePath = new Path(tempDir.toString());
    }

    @Test
    public void testPrimaryKeyTableMetrics() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"a", "b"});

        Options options = new Options();
        options.set("bucket", "1");
        options.set("write-buffer-size", "256 b");
        options.set("page-size", "32 b");

        FileStoreTable table =
                createFileStoreTable(
                        rowType, Collections.singletonList("a"), Collections.emptyList(), options);
        testMetricsImpl(table);
    }

    @Test
    public void testAppendOnlyTableMetrics() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"a", "b"});

        Options options = new Options();
        options.set("write-buffer-for-append", "true");
        options.set("write-buffer-size", "256 b");
        options.set("page-size", "32 b");
        options.set("write-buffer-spillable", "false");

        FileStoreTable table =
                createFileStoreTable(
                        rowType, Collections.emptyList(), Collections.emptyList(), options);
        testMetricsImpl(table);
    }

    private void testMetricsImpl(FileStoreTable fileStoreTable) throws Exception {
        String tableName = tablePath.getName();
        RowDataStoreWriteOperator operator =
                new RowDataStoreWriteOperator(
                        fileStoreTable,
                        null,
                        (table, commitUser, state, ioManager, memoryPool, metricGroup) ->
                                new StoreSinkWriteImpl(
                                        table,
                                        commitUser,
                                        state,
                                        ioManager,
                                        false,
                                        false,
                                        true,
                                        memoryPool,
                                        metricGroup),
                        "test");
        OneInputStreamOperatorTestHarness<InternalRow, Committable> harness =
                createHarness(operator);

        TypeSerializer<Committable> serializer =
                new CommittableTypeInfo().createSerializer(new ExecutionConfig());
        harness.setup(serializer);
        harness.open();

        int size = 10;
        for (int i = 0; i < size; i++) {
            GenericRow row = GenericRow.of(1, 1);
            harness.processElement(row, 1);
        }
        harness.prepareSnapshotPreBarrier(1);
        harness.snapshot(1, 2);
        harness.notifyOfCompletedCheckpoint(1);

        OperatorMetricGroup metricGroup = operator.getMetricGroup();
        MetricGroup writerBufferMetricGroup =
                metricGroup
                        .addGroup("paimon")
                        .addGroup("table", tableName)
                        .addGroup("writerBuffer");

        Gauge<Long> bufferPreemptCount =
                TestingMetricUtils.getGauge(writerBufferMetricGroup, "bufferPreemptCount");
        assertThat(bufferPreemptCount.getValue()).isEqualTo(0);

        Gauge<Long> totalWriteBufferSizeByte =
                TestingMetricUtils.getGauge(writerBufferMetricGroup, "totalWriteBufferSizeByte");
        assertThat(totalWriteBufferSizeByte.getValue()).isEqualTo(256);

        GenericRow row = GenericRow.of(1, 1);
        harness.processElement(row, 1);
        Gauge<Long> usedWriteBufferSizeByte =
                TestingMetricUtils.getGauge(writerBufferMetricGroup, "usedWriteBufferSizeByte");
        assertThat(usedWriteBufferSizeByte.getValue()).isGreaterThan(0);

        harness.close();
    }

    @Test
    public void testAsyncLookupWithFailure() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.INT()},
                        new String[] {"pt", "k", "v"});

        Options options = new Options();
        options.set("bucket", "1");
        options.set("changelog-producer", "lookup");

        FileStoreTable fileStoreTable =
                createFileStoreTable(
                        rowType, Arrays.asList("pt", "k"), Collections.singletonList("k"), options);

        // we don't wait for compaction because this is async lookup test
        RowDataStoreWriteOperator operator = getAsyncLookupWriteOperator(fileStoreTable, false);
        OneInputStreamOperatorTestHarness<InternalRow, Committable> harness =
                createHarness(operator);

        TableCommitImpl commit = fileStoreTable.newCommit("test");

        TypeSerializer<Committable> serializer =
                new CommittableTypeInfo().createSerializer(new ExecutionConfig());
        harness.setup(serializer);
        harness.open();

        // write basic records
        harness.processElement(GenericRow.of(1, 10, 100), 1);
        harness.processElement(GenericRow.of(2, 20, 200), 2);
        harness.processElement(GenericRow.of(3, 30, 300), 3);
        harness.prepareSnapshotPreBarrier(1);
        harness.snapshot(1, 10);
        harness.notifyOfCompletedCheckpoint(1);
        commitAll(harness, commit, 1);

        // apply changes but does not wait for compaction
        harness.processElement(GenericRow.of(1, 10, 101), 11);
        harness.processElement(GenericRow.of(3, 30, 301), 13);
        harness.prepareSnapshotPreBarrier(2);
        OperatorSubtaskState state = harness.snapshot(2, 20);
        harness.notifyOfCompletedCheckpoint(2);
        commitAll(harness, commit, 2);

        // operator is closed due to failure
        harness.close();

        // re-create operator from state, this time wait for compaction to check result
        operator = getAsyncLookupWriteOperator(fileStoreTable, true);
        harness = createHarness(operator);
        harness.setup(serializer);
        harness.initializeState(state);
        harness.open();

        // write nothing, just wait for compaction
        harness.prepareSnapshotPreBarrier(3);
        harness.snapshot(3, 30);
        harness.notifyOfCompletedCheckpoint(3);
        commitAll(harness, commit, 3);

        harness.close();
        commit.close();

        // check result
        ReadBuilder readBuilder = fileStoreTable.newReadBuilder();
        StreamTableScan scan = readBuilder.newStreamScan();
        List<Split> splits = scan.plan().splits();
        TableRead read = readBuilder.newRead();
        RecordReader<InternalRow> reader = read.createReader(splits);
        List<String> actual = new ArrayList<>();
        reader.forEachRemaining(
                row ->
                        actual.add(
                                String.format(
                                        "%s[%d, %d, %d]",
                                        row.getRowKind().shortString(),
                                        row.getInt(0),
                                        row.getInt(1),
                                        row.getInt(2))));
        assertThat(actual)
                .containsExactlyInAnyOrder("+I[1, 10, 101]", "+I[2, 20, 200]", "+I[3, 30, 301]");
    }

    private RowDataStoreWriteOperator getAsyncLookupWriteOperator(
            FileStoreTable fileStoreTable, boolean waitCompaction) {
        return new RowDataStoreWriteOperator(
                fileStoreTable,
                null,
                (table, commitUser, state, ioManager, memoryPool, metricGroup) ->
                        new AsyncLookupSinkWrite(
                                table,
                                commitUser,
                                state,
                                ioManager,
                                false,
                                waitCompaction,
                                true,
                                memoryPool,
                                metricGroup),
                "test");
    }

    @SuppressWarnings("unchecked")
    private void commitAll(
            OneInputStreamOperatorTestHarness<InternalRow, Committable> harness,
            TableCommitImpl commit,
            long commitIdentifier) {
        List<CommitMessage> commitMessages = new ArrayList<>();
        while (!harness.getOutput().isEmpty()) {
            Committable committable =
                    ((StreamRecord<Committable>) harness.getOutput().poll()).getValue();
            assertThat(committable.kind()).isEqualTo(Committable.Kind.FILE);
            commitMessages.add((CommitMessage) committable.wrappedCommittable());
        }
        commit.commit(commitIdentifier, commitMessages);
    }

    private FileStoreTable createFileStoreTable(
            RowType rowType, List<String> primaryKeys, List<String> partitionKeys, Options conf)
            throws Exception {
        conf.set(CoreOptions.PATH, tablePath.toString());
        SchemaManager schemaManager = new SchemaManager(LocalFileIO.create(), tablePath);
        schemaManager.createTable(
                new Schema(rowType.getFields(), partitionKeys, primaryKeys, conf.toMap(), ""));
        return FileStoreTableFactory.create(LocalFileIO.create(), conf);
    }

    private OneInputStreamOperatorTestHarness<InternalRow, Committable> createHarness(
            RowDataStoreWriteOperator operator) throws Exception {
        InternalTypeInfo<InternalRow> internalRowInternalTypeInfo =
                new InternalTypeInfo<>(new InternalRowTypeSerializer(RowType.builder().build()));
        return new OneInputStreamOperatorTestHarness<>(
                operator, internalRowInternalTypeInfo.createSerializer(new ExecutionConfig()));
    }
}
