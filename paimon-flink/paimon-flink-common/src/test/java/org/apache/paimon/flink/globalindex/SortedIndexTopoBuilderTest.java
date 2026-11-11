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

package org.apache.paimon.flink.globalindex;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.flink.globalindex.SortedIndexTopoBuilder.SortedBuildTask;
import org.apache.paimon.flink.utils.InternalTypeInfo;
import org.apache.paimon.globalindex.GlobalIndexKeyExtractor;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.ScanResult;
import org.apache.paimon.globalindex.sorted.NullBitmapBuffer;
import org.apache.paimon.globalindex.sorted.SortedGlobalIndexScanner;
import org.apache.paimon.globalindex.sorted.SortedGlobalIndexWriter;
import org.apache.paimon.globalindex.sorted.SortedSingleColumnIndexWriter;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RowRangeIndex;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.paimon.globalindex.sorted.SortedIndexOptions.SORTED_INDEX_NULL_BITMAP_SHUFFLE_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/** Tests for {@link SortedIndexTopoBuilder}. */
public class SortedIndexTopoBuilderTest {

    @Test
    public void testNormalizedLogicalTypeHasNoBitmapPayloadField() throws Exception {
        String column = "_SORTED_INDEX_NULL_BITMAP";
        RowType source =
                SpecialFields.rowTypeWithRowId(
                        RowType.of(
                                new org.apache.paimon.types.DataField(0, column, DataTypes.INT())));
        Method normalize =
                SortedIndexTopoBuilder.class.getDeclaredMethod(
                        "normalizedReadType",
                        RowType.class,
                        String.class,
                        String.class,
                        org.apache.paimon.types.DataType.class);
        normalize.setAccessible(true);
        RowType type = (RowType) normalize.invoke(null, source, "task", column, DataTypes.INT());
        assertThat(type.getFieldNames())
                .containsExactly("task", column, SpecialFields.ROW_ID.name());
        BinaryRow row =
                new org.apache.paimon.data.serializer.InternalRowSerializer(type)
                        .toBinaryRow(GenericRow.of(2, null, 5L));
        assertThat(row.getFieldCount()).isEqualTo(3);
        assertThat(row.getInt(0)).isEqualTo(2);
        assertThat(row.getLong(2)).isEqualTo(5L);
    }

    @Test
    public void testScalarReaderPreservesEverySamplingRow() throws Exception {
        checkUncompressedReaderRows(false);
    }

    @Test
    public void testAllNullReaderPreservesEverySamplingRow() throws Exception {
        checkUncompressedReaderRows(true);
    }

    @SuppressWarnings("unchecked")
    private void checkUncompressedReaderRows(boolean allNull) throws Exception {
        ReadBuilder readBuilder = mock(ReadBuilder.class);
        TableRead read = mock(TableRead.class);
        Split split = mock(Split.class);
        RecordReader<InternalRow> reader = mock(RecordReader.class);
        RecordReader.RecordIterator<InternalRow> batch = mock(RecordReader.RecordIterator.class);
        when(readBuilder.newRead()).thenReturn(read);
        when(read.createReader(split)).thenReturn(reader);
        when(reader.readBatch()).thenReturn(batch).thenReturn(null);
        final int[] position = {0};
        when(batch.next())
                .thenAnswer(
                        invocation -> {
                            int i = position[0]++;
                            return i <= NullBitmapBuffer.MAX_ROWS * 2 + 1
                                    ? GenericRow.of(!allNull && i == 1 ? 7 : null, (1L << 33) + i)
                                    : null;
                        });
        OneInputStreamOperator<SortedIndexTopoBuilder.SortedSplitTask, InternalRow> operator =
                (OneInputStreamOperator<SortedIndexTopoBuilder.SortedSplitTask, InternalRow>)
                        newOperator(
                                "ReadDataOperator",
                                readBuilder,
                                GlobalIndexKeyExtractor.identity(DataTypes.INT()),
                                RowType.of(DataTypes.INT()));
        try (OneInputStreamOperatorTestHarness<SortedIndexTopoBuilder.SortedSplitTask, InternalRow>
                harness = new OneInputStreamOperatorTestHarness<>(operator)) {
            harness.open();
            harness.processElement(
                    new StreamRecord<>(new SortedIndexTopoBuilder.SortedSplitTask(3, split)));
            List<InternalRow> rows = harness.extractOutputValues();
            assertThat(rows).hasSize(NullBitmapBuffer.MAX_ROWS * 2 + 2);
            for (int i = 0; i < rows.size(); i++) {
                InternalRow row = rows.get(i);
                assertThat(row.getFieldCount()).isEqualTo(3);
                assertThat(row.getInt(0)).isEqualTo(3);
                assertThat(row.getLong(2)).isEqualTo((1L << 33) + i);
                assertThat(row.isNullAt(1)).isEqualTo(allNull || i != 1);
            }
        }
    }

    @Test
    public void testDefaultTopologyUsesOriginalThreeFieldRowsAndSerializer() throws Exception {
        checkDisabledTopology(new Options());
    }

    @Test
    public void testDisabledTopologyUsesOriginalThreeFieldRowsAndSerializer() throws Exception {
        Options options = new Options();
        options.set(SORTED_INDEX_NULL_BITMAP_SHUFFLE_ENABLED, false);
        checkDisabledTopology(options);
    }

    private void checkDisabledTopology(Options options) throws Exception {
        Transformation<?> root = buildScalarTopology(options);
        assertThat(findTransformation(root, "TRANSFORM ASSIGNED RANGES")).isNull();
        assertThat(findTransformation(root, "TRANSFORM EXCHANGED ROWS")).isNull();
        assertThat(
                        findTransformation(root, "Read Data")
                                .getOutputType()
                                .createSerializer(new ExecutionConfig().getSerializerConfig())
                                .createInstance())
                .isInstanceOfSatisfying(
                        InternalRow.class, row -> assertThat(row.getFieldCount()).isEqualTo(3));
        Transformation<?> assignment = findTransformation(root, "ASSIGN RANGE INDEX");
        assertThat(assignment).isNotNull();
        TupleTypeInfo<?> preAssignmentType =
                (TupleTypeInfo<?>) assignment.getInputs().get(1).getOutputType();
        TypeInformation<?> valueType = preAssignmentType.getTypeAt(1);
        assertThat(valueType).isNotInstanceOf(NullBitmapTransportTypeInfo.class);
        TypeSerializer<?> serializer =
                valueType.createSerializer(new ExecutionConfig().getSerializerConfig());
        assertThat(serializer).isInstanceOf(RowDataSerializer.class);
        assertThat(((RowData) serializer.createInstance()).getArity()).isEqualTo(3);
    }

    @Test
    public void testEnabledTopologyKeepsThreeFieldSourceAndSixFieldLocalSort() throws Exception {
        Options options = new Options();
        options.set(SORTED_INDEX_NULL_BITMAP_SHUFFLE_ENABLED, true);
        Transformation<?> root = buildScalarTopology(options);
        assertThat(findTransformation(root, "TRANSFORM ASSIGNED RANGES")).isNotNull();
        assertThat(findTransformation(root, "TRANSFORM EXCHANGED ROWS")).isNotNull();
        InternalRow sourceRow =
                (InternalRow)
                        findTransformation(root, "Read Data")
                                .getOutputType()
                                .createSerializer(new ExecutionConfig().getSerializerConfig())
                                .createInstance();
        assertThat(sourceRow.getFieldCount()).isEqualTo(3);
        BinaryRow sortRow =
                (BinaryRow)
                        findTransformation(root, "LOCAL SORT")
                                .getOutputType()
                                .createSerializer(new ExecutionConfig().getSerializerConfig())
                                .createInstance();
        assertThat(sortRow.getFieldCount()).isEqualTo(6);
        assertThat(sortRow.getFixedLengthPartSize()).isEqualTo(56);
    }

    private Transformation<?> buildScalarTopology(Options options) throws Exception {
        RowType rowType =
                RowType.of(new org.apache.paimon.types.DataField(0, "id", DataTypes.INT()));
        FileStoreTable table = mock(FileStoreTable.class, withSettings().serializable());
        ReadBuilder readBuilder = mock(ReadBuilder.class, withSettings().serializable());
        when(table.rowType()).thenReturn(rowType);
        when(table.coreOptions()).thenReturn(new CoreOptions(new Options()));
        when(table.partitionKeys()).thenReturn(Collections.emptyList());
        when(table.newReadBuilder()).thenReturn(readBuilder);
        when(readBuilder.withReadType(SpecialFields.rowTypeWithRowId(rowType)))
                .thenReturn(readBuilder);
        DataFileMeta file =
                DataFileMeta.forAppend(
                        "data.parquet",
                        1024,
                        10,
                        SimpleStats.EMPTY_STATS,
                        0,
                        9,
                        0,
                        Collections.emptyList(),
                        null,
                        null,
                        null,
                        null,
                        0L,
                        null);
        DataSplit split =
                DataSplit.builder()
                        .withSnapshot(1L)
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .withDataFiles(Collections.singletonList(file))
                        .rawConvertible(false)
                        .build();
        SortedGlobalIndexScanner scanner = mock(SortedGlobalIndexScanner.class);
        when(scanner.withIndexFields(Collections.singletonList("id"))).thenReturn(scanner);
        when(scanner.incrementalScan())
                .thenReturn(
                        Optional.of(
                                new ScanResult<>(
                                        1L,
                                        RowRangeIndex.create(
                                                Collections.singletonList(new Range(0, 9))),
                                        Collections.singletonList(split),
                                        Collections.emptyList())));
        return SortedIndexTopoBuilder.buildIndexStream(
                        StreamExecutionEnvironment.getExecutionEnvironment(),
                        () -> scanner,
                        table,
                        Collections.singletonList(Collections.singletonList("id")),
                        "btree",
                        null,
                        options)
                .get()
                .getTransformation();
    }

    private static Transformation<?> findTransformation(Transformation<?> root, String name) {
        if (root.getName().equals(name)) {
            return root;
        }
        for (Transformation<?> input : root.getInputs()) {
            Transformation<?> match = findTransformation(input, name);
            if (match != null) {
                return match;
            }
        }
        return null;
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testBitmapBatchesReuseCurrentWriterAndLocalRowIds() throws Exception {
        long start = 1L << 33;
        Range range = new Range(start, start + 100);
        SortedGlobalIndexWriter writer = mock(SortedGlobalIndexWriter.class);
        GlobalIndexSingleColumnWriter underlying = mock(GlobalIndexSingleColumnWriter.class);
        SortedSingleColumnIndexWriter taskWriter =
                SortedSingleColumnIndexWriter.forSourceRowCount(101, underlying);
        when(writer.createTaskWriter(range)).thenReturn(taskWriter);
        OneInputStreamOperator<InternalRow, org.apache.paimon.flink.sink.Committable> operator =
                (OneInputStreamOperator<InternalRow, org.apache.paimon.flink.sink.Committable>)
                        newOperator(
                                "WriteIndexOperator",
                                Collections.singletonList(
                                        new SortedBuildTask(
                                                0,
                                                range,
                                                new BinaryRowSerializer(0)
                                                        .serializeToBytes(BinaryRow.EMPTY_ROW))),
                                0,
                                writer,
                                1L,
                                0,
                                1,
                                2,
                                DataTypes.INT());
        try (OneInputStreamOperatorTestHarness<
                        InternalRow, org.apache.paimon.flink.sink.Committable>
                harness = new OneInputStreamOperatorTestHarness<>(operator)) {
            harness.open();
            for (int i = 0; i < 2; i++) {
                NullBitmapBuffer buffer = new NullBitmapBuffer();
                buffer.add(start + i * 2);
                buffer.add(start + i * 2 + 1);
                harness.processElement(
                        new StreamRecord<>(
                                GenericRow.of(0, null, buffer.firstRowId(), buffer.drain())));
            }
            harness.processElement(new StreamRecord<>(GenericRow.of(0, 7, start + 4, null)));
            verify(writer).createTaskWriter(range);
            for (long i = 0; i < 4; i++) {
                verify(underlying).write(null, i);
            }
            verify(underlying).write(7, 4L);
        }
    }

    private static Object newOperator(String name, Object... args) throws Exception {
        for (Class<?> candidate : SortedIndexTopoBuilder.class.getDeclaredClasses()) {
            if (candidate.getSimpleName().equals(name)) {
                Constructor<?> constructor = candidate.getDeclaredConstructors()[0];
                constructor.setAccessible(true);
                return constructor.newInstance(args);
            }
        }
        throw new AssertionError("Missing operator " + name);
    }

    @Test
    public void testSupportsBitmapAndBTree() {
        assertThat(SortedIndexTopoBuilder.supports("bitmap")).isTrue();
        assertThat(SortedIndexTopoBuilder.supports("btree")).isTrue();
        assertThat(SortedIndexTopoBuilder.supports("multivalue")).isTrue();
        assertThat(SortedIndexTopoBuilder.supports("fm")).isFalse();
        assertThat(SortedIndexTopoBuilder.supports("inverted")).isFalse();
    }

    @Test
    public void testWriteIndexOperatorClosesActiveWriter() throws Exception {
        Class<?> operatorClass = null;
        for (Class<?> candidate : SortedIndexTopoBuilder.class.getDeclaredClasses()) {
            if (candidate.getSimpleName().equals("WriteIndexOperator")) {
                operatorClass = candidate;
                break;
            }
        }
        assertThat(operatorClass).isNotNull();
        Constructor<?> constructor =
                operatorClass.getDeclaredConstructor(
                        List.class,
                        int.class,
                        SortedGlobalIndexWriter.class,
                        long.class,
                        int.class,
                        int.class,
                        int.class,
                        org.apache.paimon.types.DataType.class);
        constructor.setAccessible(true);
        Object operator =
                constructor.newInstance(
                        Collections.emptyList(),
                        0,
                        mock(SortedGlobalIndexWriter.class),
                        1L,
                        0,
                        0,
                        0,
                        DataTypes.INT());
        GlobalIndexSingleColumnWriter activeWriter =
                mock(
                        GlobalIndexSingleColumnWriter.class,
                        org.mockito.Mockito.withSettings().extraInterfaces(Closeable.class));
        SortedSingleColumnIndexWriter taskWriter =
                SortedSingleColumnIndexWriter.forSourceRowCount(1, activeWriter);
        Field currentWriter = operatorClass.getDeclaredField("currentWriter");
        currentWriter.setAccessible(true);
        currentWriter.set(operator, taskWriter);

        Method close = operatorClass.getMethod("close");
        close.invoke(operator);

        verify((Closeable) activeWriter).close();
    }

    @Test
    public void testBuildIndexReturnsFalseWhenNoBuildTask() throws Exception {
        SortedGlobalIndexScanner indexScanner = mock(SortedGlobalIndexScanner.class);
        when(indexScanner.withIndexFields(Collections.singletonList("id")))
                .thenReturn(indexScanner);
        when(indexScanner.incrementalScan()).thenReturn(Optional.empty());
        StreamExecutionEnvironment env = mock(StreamExecutionEnvironment.class);

        assertThat(
                        SortedIndexTopoBuilder.buildIndex(
                                env,
                                () -> indexScanner,
                                mock(FileStoreTable.class),
                                Collections.singletonList(Collections.singletonList("id")),
                                "btree",
                                null,
                                new Options()))
                .isFalse();
        verify(indexScanner).incrementalScan();
        verifyNoInteractions(env);
    }

    @Test
    public void testBuildIndexStreamReturnsEmptyWhenNoBuildTask() throws Exception {
        SortedGlobalIndexScanner indexScanner = mock(SortedGlobalIndexScanner.class);
        when(indexScanner.withIndexFields(Collections.singletonList("id")))
                .thenReturn(indexScanner);
        when(indexScanner.incrementalScan()).thenReturn(Optional.empty());
        StreamExecutionEnvironment env = mock(StreamExecutionEnvironment.class);

        assertThat(
                        SortedIndexTopoBuilder.buildIndexStream(
                                env,
                                () -> indexScanner,
                                mock(FileStoreTable.class),
                                Collections.singletonList(Collections.singletonList("id")),
                                "btree",
                                null,
                                new Options()))
                .isEmpty();
        verify(indexScanner).incrementalScan();
        verifyNoInteractions(env);
    }

    @Test
    public void testCalculateParallelismByTotalRowsInsteadOfRangeCount() {
        List<SortedBuildTask> tasks = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            tasks.add(new SortedBuildTask(i, new Range(i * 10L, i * 10L + 9), new byte[0]));
        }

        assertThat(SortedIndexTopoBuilder.calculateParallelism(tasks, 1000L, 4096)).isEqualTo(1);
    }

    @Test
    public void testCalculateParallelismHonorsMaxParallelism() {
        List<SortedBuildTask> tasks = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            tasks.add(new SortedBuildTask(i, new Range(i * 1000L, i * 1000L + 999), new byte[0]));
        }

        assertThat(SortedIndexTopoBuilder.calculateParallelism(tasks, 1000L, 16)).isEqualTo(16);
    }

    @Test
    public void testCalculateParallelismKeepsSingleRangeBehavior() {
        List<SortedBuildTask> tasks = new ArrayList<>();
        tasks.add(new SortedBuildTask(0, new Range(0, 1499), new byte[0]));

        assertThat(SortedIndexTopoBuilder.calculateParallelism(tasks, 1000L, 16)).isEqualTo(1);
    }

    @Test
    public void testNormalizedSortColumnsUseRowIdAsTieBreaker() {
        assertThat(SortedIndexTopoBuilder.createSortColumns("task-id", "index-key"))
                .containsExactly("task-id", "index-key", SpecialFields.ROW_ID.name());
    }

    @Test
    public void testBuildTaskPartitioner() {
        assertThat(SortedIndexTopoBuilder.BUILD_TASK_PARTITIONER.partition(0, 4)).isEqualTo(0);
        assertThat(SortedIndexTopoBuilder.BUILD_TASK_PARTITIONER.partition(5, 4)).isEqualTo(1);
        assertThat(SortedIndexTopoBuilder.BUILD_TASK_PARTITIONER.partition(9, 4)).isEqualTo(1);
    }

    @Test
    public void testBuildTaskPartitionUsesBatchExchange() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, 0);
        writer.complete();
        DataStream<InternalRow> input =
                env.fromData(
                        Collections.<InternalRow>singletonList(row),
                        InternalTypeInfo.fromRowType(RowType.of(DataTypes.INT())));

        DataStream<InternalRow> partitioned = SortedIndexTopoBuilder.partitionByBuildTask(input, 0);

        assertThat(partitioned.getTransformation()).isInstanceOf(PartitionTransformation.class);
        assertThat(((PartitionTransformation<?>) partitioned.getTransformation()).getExchangeMode())
                .isEqualTo(StreamExchangeMode.BATCH);
    }
}
