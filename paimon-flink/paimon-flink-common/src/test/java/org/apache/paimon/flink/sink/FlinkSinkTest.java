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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.utils.MetricUtils;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.operation.KeyValueFileStoreWrite;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.state.StateInitializationContextImpl;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.collect.utils.MockOperatorStateStore;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test class for {@link FlinkSink}. */
public class FlinkSinkTest {

    @TempDir Path tempPath;

    @Test
    public void testOptimizeKeyValueWriterForBatch() throws Exception {
        // test for batch mode auto enable spillable
        FileStoreTable fileStoreTable = createFileStoreTable();
        StreamExecutionEnvironment streamExecutionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // set this when batch executing
        streamExecutionEnvironment.setRuntimeMode(RuntimeExecutionMode.BATCH);
        assertThat(testSpillable(streamExecutionEnvironment, fileStoreTable)).isTrue();

        // set this to streaming, we should get a false then
        streamExecutionEnvironment.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        assertThat(testSpillable(streamExecutionEnvironment, fileStoreTable)).isFalse();
    }

    @Test
    public void testCompactionMetrics() throws Exception {
        FileStoreTable table = createFileStoreTable();
        RowDataStoreWriteOperator operator = createWriteOperator(table);
        OneInputStreamOperatorTestHarness<InternalRow, Committable> testHarness =
                createTestHarness(operator);
        MetricGroup compactionMetricGroup =
                operator.getMetricGroup()
                        .addGroup("paimon")
                        .addGroup("table", table.name())
                        .addGroup("partition", "_")
                        .addGroup("bucket", "0")
                        .addGroup("compaction");
        testHarness.open();

        final GenericRow row1 = GenericRow.of(1, 2);
        final GenericRow row2 = GenericRow.of(2, 3);
        final GenericRow row3 = GenericRow.of(3, 4);
        final GenericRow row4 = GenericRow.of(4, 5);

        List<StreamRecord<InternalRow>> streamRecords = new ArrayList<>();
        streamRecords.add(new StreamRecord<>(row1));
        streamRecords.add(new StreamRecord<>(row2));
        streamRecords.add(new StreamRecord<>(row3));

        long cpId = 1L;
        testHarness.processElements(streamRecords);
        operator.write.compact(BinaryRow.EMPTY_ROW, 0, true);
        operator.write.prepareCommit(true, cpId++);
        assertThat(
                        MetricUtils.getGauge(compactionMetricGroup, "lastTableFilesCompactedBefore")
                                .getValue())
                .isEqualTo(1L);
        assertThat(
                        MetricUtils.getGauge(compactionMetricGroup, "lastTableFilesCompactedAfter")
                                .getValue())
                .isEqualTo(1L);
        assertThat(
                        MetricUtils.getGauge(compactionMetricGroup, "lastChangelogFilesCompacted")
                                .getValue())
                .isEqualTo(0L);

        testHarness.processElement(row4, 0);
        operator.write.compact(BinaryRow.EMPTY_ROW, 0, true);
        operator.write.prepareCommit(true, cpId);
        assertThat(
                        MetricUtils.getGauge(compactionMetricGroup, "lastTableFilesCompactedBefore")
                                .getValue())
                .isEqualTo(2L);
        assertThat(
                        MetricUtils.getGauge(compactionMetricGroup, "lastTableFilesCompactedAfter")
                                .getValue())
                .isEqualTo(1L);
        assertThat(
                        MetricUtils.getGauge(compactionMetricGroup, "lastChangelogFilesCompacted")
                                .getValue())
                .isEqualTo(0L);

        // operator closed, metric groups should be unregistered
        testHarness.close();
        assertThat(MetricUtils.getGauge(compactionMetricGroup, "lastTableFilesCompactedBefore"))
                .isNull();
        assertThat(MetricUtils.getGauge(compactionMetricGroup, "lastTableFilesCompactedAfter"))
                .isNull();
        assertThat(MetricUtils.getGauge(compactionMetricGroup, "lastChangelogFilesCompacted"))
                .isNull();
    }

    @Test
    public void testDynamicBucketCompactionMetrics() throws Exception {
        FileStoreTable table = createFileStoreTable();
        DynamicBucketRowWriteOperator operator = createDynamicBucketWriteOperator(table);
        OneInputStreamOperatorTestHarness<Tuple2<InternalRow, Integer>, Committable> testHarness =
                createDynamicBucketTestHarness(operator);
        MetricGroup compactionMetricGroup =
                operator.getMetricGroup()
                        .addGroup("paimon")
                        .addGroup("table", table.name())
                        .addGroup("partition", "_")
                        .addGroup("bucket", "0")
                        .addGroup("compaction");
        testHarness.open();

        final GenericRow row1 = GenericRow.of(1, 2);
        final GenericRow row2 = GenericRow.of(2, 3);
        final GenericRow row3 = GenericRow.of(3, 4);
        final GenericRow row4 = GenericRow.of(4, 5);

        List<StreamRecord<Tuple2<InternalRow, Integer>>> streamRecords = new ArrayList<>();
        streamRecords.add(new StreamRecord<>(Tuple2.of(row1, 0)));
        streamRecords.add(new StreamRecord<>(Tuple2.of(row2, 1)));
        streamRecords.add(new StreamRecord<>(Tuple2.of(row3, 2)));

        long cpId = 1L;
        testHarness.processElements(streamRecords);
        operator.write.compact(BinaryRow.EMPTY_ROW, 0, true);
        operator.write.prepareCommit(true, cpId++);
        assertThat(
                        MetricUtils.getGauge(compactionMetricGroup, "lastTableFilesCompactedBefore")
                                .getValue())
                .isEqualTo(1L);
        assertThat(
                        MetricUtils.getGauge(compactionMetricGroup, "lastTableFilesCompactedAfter")
                                .getValue())
                .isEqualTo(1L);
        assertThat(
                        MetricUtils.getGauge(compactionMetricGroup, "lastChangelogFilesCompacted")
                                .getValue())
                .isEqualTo(0L);

        testHarness.processElement(Tuple2.of(row4, 0), 0);
        operator.write.compact(BinaryRow.EMPTY_ROW, 0, true);
        operator.write.prepareCommit(true, cpId);
        assertThat(
                        MetricUtils.getGauge(compactionMetricGroup, "lastTableFilesCompactedBefore")
                                .getValue())
                .isEqualTo(2L);
        assertThat(
                        MetricUtils.getGauge(compactionMetricGroup, "lastTableFilesCompactedAfter")
                                .getValue())
                .isEqualTo(1L);
        assertThat(
                        MetricUtils.getGauge(compactionMetricGroup, "lastChangelogFilesCompacted")
                                .getValue())
                .isEqualTo(0L);

        // operator closed, metric groups should be unregistered
        testHarness.close();
        assertThat(MetricUtils.getGauge(compactionMetricGroup, "lastTableFilesCompactedBefore"))
                .isNull();
        assertThat(MetricUtils.getGauge(compactionMetricGroup, "lastTableFilesCompactedAfter"))
                .isNull();
        assertThat(MetricUtils.getGauge(compactionMetricGroup, "lastChangelogFilesCompacted"))
                .isNull();
    }

    private boolean testSpillable(
            StreamExecutionEnvironment streamExecutionEnvironment, FileStoreTable fileStoreTable)
            throws Exception {
        DataStreamSource<InternalRow> source =
                streamExecutionEnvironment.fromCollection(
                        Collections.singletonList(GenericRow.of(1, 1)));
        FlinkSink<InternalRow> flinkSink = new FixedBucketSink(fileStoreTable, null, null);
        DataStream<Committable> written = flinkSink.doWrite(source, "123", 1);
        RowDataStoreWriteOperator operator =
                ((RowDataStoreWriteOperator)
                        ((SimpleOperatorFactory)
                                        ((OneInputTransformation) written.getTransformation())
                                                .getOperatorFactory())
                                .getOperator());
        StateInitializationContextImpl context =
                new StateInitializationContextImpl(
                        null,
                        new MockOperatorStateStore() {
                            @Override
                            public <S> ListState<S> getUnionListState(
                                    ListStateDescriptor<S> stateDescriptor) throws Exception {
                                return getListState(stateDescriptor);
                            }
                        },
                        null,
                        null,
                        null);
        operator.initStateAndWriter(context, (a, b, c) -> true, new IOManagerAsync(), "123");
        return ((KeyValueFileStoreWrite) ((StoreSinkWriteImpl) operator.write).write.getWrite())
                .bufferSpillable();
    }

    protected static final RowType ROW_TYPE =
            RowType.of(
                    new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"pk", "pt0"});

    private FileStoreTable createFileStoreTable() throws Exception {
        org.apache.paimon.fs.Path tablePath = new org.apache.paimon.fs.Path(tempPath.toString());
        Options options = new Options();
        options.set(CoreOptions.PATH, tablePath.toString());
        options.set(CoreOptions.BUCKET, 1);
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(LocalFileIO.create(), tablePath),
                        new Schema(
                                ROW_TYPE.getFields(),
                                Collections.emptyList(),
                                Collections.singletonList("pk"),
                                options.toMap(),
                                ""));
        return FileStoreTableFactory.create(
                FileIOFinder.find(tablePath),
                tablePath,
                tableSchema,
                options,
                new CatalogEnvironment(Lock.emptyFactory(), null, null));
    }

    private OneInputStreamOperatorTestHarness<InternalRow, Committable> createTestHarness(
            OneInputStreamOperator<InternalRow, Committable> operator) throws Exception {
        TypeSerializer<Committable> serializer =
                new CommittableTypeInfo().createSerializer(new ExecutionConfig());
        OneInputStreamOperatorTestHarness<InternalRow, Committable> harness =
                new OneInputStreamOperatorTestHarness<>(operator);
        harness.setup(serializer);
        return harness;
    }

    private OneInputStreamOperatorTestHarness<Tuple2<InternalRow, Integer>, Committable>
            createDynamicBucketTestHarness(
                    OneInputStreamOperator<Tuple2<InternalRow, Integer>, Committable> operator)
                    throws Exception {
        TypeSerializer<Committable> serializer =
                new CommittableTypeInfo().createSerializer(new ExecutionConfig());
        OneInputStreamOperatorTestHarness<Tuple2<InternalRow, Integer>, Committable> harness =
                new OneInputStreamOperatorTestHarness<>(operator);
        harness.setup(serializer);
        return harness;
    }

    protected RowDataStoreWriteOperator createWriteOperator(FileStoreTable table) {
        return new RowDataStoreWriteOperator(
                table,
                null,
                (t, commitUser, state, ioManager, memoryPool, metricGroup) ->
                        new StoreSinkWriteImpl(
                                t,
                                commitUser,
                                state,
                                ioManager,
                                false,
                                false,
                                true,
                                memoryPool,
                                metricGroup),
                "test");
    }

    protected DynamicBucketRowWriteOperator createDynamicBucketWriteOperator(FileStoreTable table) {
        return new DynamicBucketRowWriteOperator(
                table,
                (t, commitUser, state, ioManager, memoryPool, metricGroup) ->
                        new StoreSinkWriteImpl(
                                t,
                                commitUser,
                                state,
                                ioManager,
                                false,
                                false,
                                true,
                                memoryPool,
                                metricGroup),
                "test");
    }
}
