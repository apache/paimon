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
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.flink.FlinkRowWrapper;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.flink.shuffle.RangeShuffle;
import org.apache.paimon.flink.sorter.OrderSorter;
import org.apache.paimon.flink.sorter.SortOperator;
import org.apache.paimon.flink.sorter.TableSortInfo;
import org.apache.paimon.flink.utils.InternalTypeInfo;
import org.apache.paimon.globalindex.sorted.NullBitmapBuffer;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.KeyComparatorSupplier;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class NullBitmapShuffleOperatorTest {

    private static final RowType KEY_TYPE =
            RowType.of(DataTypes.INT(), DataTypes.INT(), DataTypes.BIGINT());

    @Test
    void testExpansionRestoresOriginalRowsBeforeLocalSort() throws Exception {
        NullBitmapExpandOperator decoder = new NullBitmapExpandOperator();
        try (OneInputStreamOperatorTestHarness<
                        Tuple2<InternalRow, RowData>, Tuple2<InternalRow, RowData>>
                harness = new OneInputStreamOperatorTestHarness<>(decoder)) {
            harness.open();
            // Two reader batches whose row-ID ranges interleave on the same destination.
            harness.processElement(new StreamRecord<>(compressed(9, 1)));
            harness.processElement(new StreamRecord<>(assigned(0, 0, 7, 2).f1));
            harness.processElement(new StreamRecord<>(compressed(5, 3)));
            List<Tuple2<InternalRow, RowData>> expanded = harness.extractOutputValues();
            assertThat(expanded).hasSize(5);
            for (Tuple2<InternalRow, RowData> row : expanded) {
                assertThat(row.f1.getArity()).isEqualTo(3);
                assertThat(row.f0.getLong(2)).isEqualTo(row.f1.getLong(2));
            }
            List<Tuple2<InternalRow, RowData>> baseline = new ArrayList<>();
            for (long id : new long[] {9, 1, 2, 5, 3}) {
                baseline.add(assigned(0, 0, id == 2 ? 7 : null, id).f1);
            }
            List<InternalRow> actual = localSort(expanded);
            assertThat(actual).isEqualTo(localSort(baseline));
            assertThat(actual)
                    .extracting(row -> row.getLong(5))
                    .containsExactly(1L, 3L, 5L, 9L, 2L);
        }
    }

    @Test
    void testBitmapExpansionEmitsThreeFieldRows() throws Exception {
        try (OneInputStreamOperatorTestHarness<
                        Tuple2<InternalRow, RowData>, Tuple2<InternalRow, RowData>>
                harness = new OneInputStreamOperatorTestHarness<>(new NullBitmapExpandOperator())) {
            harness.open();
            harness.processElement(new StreamRecord<>(compressed(9, 1, 1L << 33)));
            List<Tuple2<InternalRow, RowData>> expanded = harness.extractOutputValues();
            assertThat(expanded).hasSize(3);
            assertThat(expanded)
                    .extracting(row -> row.f1.getLong(2))
                    .containsExactly(1L, 9L, 1L << 33);
            for (Tuple2<InternalRow, RowData> row : expanded) {
                assertThat(row.f1.getArity()).isEqualTo(3);
                assertThat(row.f1.getInt(0)).isZero();
                assertThat(row.f1.isNullAt(1)).isTrue();
                assertThat(row.f0.getLong(2)).isEqualTo(row.f1.getLong(2));
            }
        }
    }

    @Test
    void testExpansionAcceptsRawThreeFieldRows() throws Exception {
        try (OneInputStreamOperatorTestHarness<
                        Tuple2<InternalRow, RowData>, Tuple2<InternalRow, RowData>>
                harness = new OneInputStreamOperatorTestHarness<>(new NullBitmapExpandOperator())) {
            harness.open();
            harness.processElement(new StreamRecord<>(assigned(0, 2, null, 5).f1));
            harness.processElement(new StreamRecord<>(assigned(0, 2, 7, 6).f1));
            List<Tuple2<InternalRow, RowData>> expanded = harness.extractOutputValues();
            assertThat(expanded).hasSize(2);
            assertThat(expanded).extracting(row -> row.f1.getArity()).containsOnly(3);
            assertThat(expanded).extracting(row -> row.f1.getLong(2)).containsExactly(5L, 6L);
            assertThat(expanded).extracting(row -> row.f1.getInt(0)).containsOnly(2);
            assertThat(expanded.get(0).f1.isNullAt(1)).isTrue();
            assertThat(expanded.get(1).f1.getInt(1)).isEqualTo(7);
        }
    }

    private static Tuple2<InternalRow, RowData> compressed(long... ids) throws Exception {
        NullBitmapBuffer buffer = new NullBitmapBuffer();
        for (long id : ids) {
            buffer.add(id);
        }
        return Tuple2.of(
                GenericRow.of(0, null, buffer.firstRowId()),
                GenericRowData.of(0, null, buffer.firstRowId(), buffer.drain()));
    }

    private static List<InternalRow> localSort(List<Tuple2<InternalRow, RowData>> rows)
            throws Exception {
        RowType longType =
                RowType.of(
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.BIGINT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.BIGINT());
        SortOperator sorter =
                new SortOperator(
                        KEY_TYPE,
                        longType,
                        1024 * 1024,
                        16 * 1024,
                        128,
                        CompressOptions.defaultOptions(),
                        1,
                        MemorySize.MAX_VALUE);
        try (OneInputStreamOperatorTestHarness<InternalRow, InternalRow> harness =
                new OneInputStreamOperatorTestHarness<>(sorter)) {
            harness.setup(
                    InternalTypeInfo.fromRowType(longType)
                            .createSerializer(new org.apache.flink.api.common.ExecutionConfig()));
            harness.open();
            for (Tuple2<InternalRow, RowData> row : rows) {
                harness.processElement(
                        new StreamRecord<>(new JoinedRow(row.f0, new FlinkRowWrapper(row.f1))));
            }
            sorter.endInput();
            List<InternalRow> sorted = harness.extractOutputValues();
            InternalRowSerializer serializer = new InternalRowSerializer(longType);
            for (InternalRow row : sorted) {
                assertThat(row.getFieldCount()).isEqualTo(6);
                assertThat(serializer.toBinaryRow(row).getSizeInBytes()).isEqualTo(56);
            }
            return sorted;
        }
    }

    @Test
    void testHookRunsAfterAssignmentAndBeforeExchange() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        RowType valueType = KEY_TYPE;
        DataStream<Tuple2<InternalRow, RowData>> input =
                env.fromData(
                        java.util.Collections.singletonList(assigned(0, 0, null, 0).f1),
                        new TupleTypeInfo<>(
                                InternalTypeInfo.fromRowType(KEY_TYPE),
                                org.apache.flink.table.runtime.typeutils.InternalTypeInfo.of(
                                        LogicalTypeConversion.toLogicalType(valueType))));
        DataStream<Tuple2<InternalRow, RowData>> shuffled =
                RangeShuffle.rangeShuffleByKey(
                        input,
                        new KeyComparatorSupplier(KEY_TYPE),
                        InternalTypeInfo.fromRowType(KEY_TYPE),
                        100,
                        100,
                        20,
                        2,
                        valueType,
                        true,
                        new NullBitmapShuffleOperator(KEY_TYPE),
                        new NullBitmapExpandOperator());
        assertThat(shuffled.getTransformation().getName()).isEqualTo("TRANSFORM EXCHANGED ROWS");
        Transformation<?> removeRange = shuffled.getTransformation().getInputs().get(0);
        assertThat(removeRange.getName()).isEqualTo("REMOVE RANGE INDEX");
        Transformation<?> exchange = removeRange.getInputs().get(0);
        Transformation<?> hook = exchange.getInputs().get(0);
        assertThat(hook.getName()).isEqualTo("TRANSFORM ASSIGNED RANGES");
        Transformation<?> assignment = hook.getInputs().get(0);
        assertThat(assignment.getName()).isEqualTo("ASSIGN RANGE INDEX");
        Transformation<?> global = assignment.getInputs().get(0).getInputs().get(0);
        assertThat(global.getName()).isEqualTo("GLOBAL SAMPLE");
        Transformation<?> local = global.getInputs().get(0);
        assertThat(local.getName()).isEqualTo("LOCAL SAMPLE");
        Transformation<?> keyAndSize = local.getInputs().get(0);
        assertThat(keyAndSize.getName()).isEqualTo("ABSTRACT KEY AND SIZE");
        assertThat(keyAndSize.getInputs().get(0)).isSameAs(input.getTransformation());
        assertThat(assignment.getInputs().get(1).getInputs().get(0))
                .isSameAs(input.getTransformation());
        DataStream<Tuple2<InternalRow, RowData>> baseline =
                RangeShuffle.rangeShuffleByKey(
                        input,
                        new KeyComparatorSupplier(KEY_TYPE),
                        InternalTypeInfo.fromRowType(KEY_TYPE),
                        100,
                        100,
                        20,
                        2,
                        valueType,
                        true);
        assertThat(baseline.getTransformation().getInputs().get(0).getInputs().get(0).getName())
                .isEqualTo("ASSIGN RANGE INDEX");
    }

    @Test
    void testNullPayloadPreservesOriginalSamplingWeights() throws Exception {
        RowType originalType = KEY_TYPE;
        RowType transportType =
                RowType.of(DataTypes.INT(), DataTypes.INT(), DataTypes.BIGINT(), DataTypes.BYTES());
        for (boolean sortBySize : new boolean[] {false, true}) {
            RangeShuffle.KeyAndSizeExtractor<InternalRow> original =
                    new RangeShuffle.KeyAndSizeExtractor<>(originalType, sortBySize);
            RangeShuffle.KeyAndSizeExtractor<InternalRow> transport =
                    new RangeShuffle.KeyAndSizeExtractor<>(transportType, sortBySize);
            original.open(new Configuration());
            transport.open(new Configuration());
            for (Integer value : new Integer[] {null, 7}) {
                InternalRow key = GenericRow.of(0, value, 5L);
                int oldWeight = original.map(Tuple2.of(key, GenericRowData.of(0, value, 5L))).f1;
                int newWeight =
                        transport.map(Tuple2.of(key, GenericRowData.of(0, value, 5L, null))).f1;
                assertThat(newWeight).isEqualTo(oldWeight);
                assertThat(newWeight).isEqualTo(sortBySize ? (value == null ? 12 : 16) : 1);
            }
        }
    }

    @Test
    void testAllNullBufferThresholds() throws Exception {
        for (int count : new int[] {4095, 4096, 4097, 8193}) {
            NullBitmapShuffleOperator operator = new NullBitmapShuffleOperator(KEY_TYPE, false);
            try (OneInputStreamOperatorTestHarness<
                            Tuple2<Integer, Tuple2<InternalRow, RowData>>,
                            Tuple2<Integer, Tuple2<InternalRow, RowData>>>
                    harness = new OneInputStreamOperatorTestHarness<>(operator)) {
                harness.open();
                for (int i = 0; i < count; i++) {
                    harness.processElement(
                            new StreamRecord<>(assigned(6, 2, null, (1L << 33) + i)));
                }
                assertThat(harness.extractOutputValues())
                        .hasSize(count / NullBitmapBuffer.MAX_ROWS);
                operator.endInput();
                List<Tuple2<Integer, Tuple2<InternalRow, RowData>>> output =
                        harness.extractOutputValues();
                assertThat(output).hasSize((count + 4095) / 4096);
                Map<Long, Integer> actual = decodeDestinations(output);
                assertThat(actual).hasSize(count);
                for (int i = 0; i < count; i++) {
                    assertThat(actual.get((1L << 33) + i)).isEqualTo(6);
                }
            }
        }
    }

    @Test
    void testAssignedDestinationsAndTasksNeverMix() throws Exception {
        NullBitmapShuffleOperator operator = new NullBitmapShuffleOperator(KEY_TYPE, false);
        try (OneInputStreamOperatorTestHarness<
                        Tuple2<Integer, Tuple2<InternalRow, RowData>>,
                        Tuple2<Integer, Tuple2<InternalRow, RowData>>>
                harness = new OneInputStreamOperatorTestHarness<>(operator)) {
            harness.open();
            List<Tuple2<Integer, Tuple2<InternalRow, RowData>>> input = new ArrayList<>();
            input.add(assigned(0, 3, null, 10));
            input.add(assigned(0, 3, null, 9));
            input.add(assigned(1, 3, null, 11));
            input.add(assigned(1, 4, null, 12));
            input.add(assigned(1, 4, null, 13));
            // Repeated hot non-null keys must keep their original assigned destination too.
            for (int i = 14; i < 114; i++) {
                input.add(assigned(i % 3, 4, 7, i));
            }
            input.add(assigned(0, 4, null, 114));
            Map<Long, Integer> expected = new HashMap<>();
            for (Tuple2<Integer, Tuple2<InternalRow, RowData>> row : input) {
                expected.put(row.f1.f1.getLong(2), row.f0);
                harness.processElement(new StreamRecord<>(row));
            }
            operator.endInput();
            List<Tuple2<Integer, Tuple2<InternalRow, RowData>>> output =
                    harness.extractOutputValues();
            assertThat(decodeDestinations(output)).isEqualTo(expected);
            assertThat(output.get(0).f1.f1.getLong(2)).isEqualTo(9);
            assertThat(output.get(0).f1.f0.getLong(2)).isEqualTo(9);
            assertThat(output.get(0).f1.f1.getInt(0)).isEqualTo(3);
            assertThat(output.get(2).f1.f1.getInt(0)).isEqualTo(4);
        }
    }

    @Test
    void testNonNullDestinationsDoNotFlushPendingNulls() throws Exception {
        NullBitmapShuffleOperator operator = new NullBitmapShuffleOperator(KEY_TYPE, false);
        try (OneInputStreamOperatorTestHarness<
                        Tuple2<Integer, Tuple2<InternalRow, RowData>>,
                        Tuple2<Integer, Tuple2<InternalRow, RowData>>>
                harness = new OneInputStreamOperatorTestHarness<>(operator)) {
            harness.open();
            for (int i = 0; i < 100; i++) {
                harness.processElement(new StreamRecord<>(assigned(0, 1, null, i * 2L)));
                harness.processElement(new StreamRecord<>(assigned(2, 3, 7, i * 2L + 1)));
            }
            assertThat(harness.extractOutputValues()).hasSize(100);
            operator.endInput();
            List<Tuple2<Integer, Tuple2<InternalRow, RowData>>> output =
                    harness.extractOutputValues();
            assertThat(output).hasSize(101);
            Tuple2<Integer, Tuple2<InternalRow, RowData>> compressed = output.get(100);
            assertThat(compressed.f0).isZero();
            assertThat(compressed.f1.f1.getInt(0)).isEqualTo(1);
            RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
            bitmap.deserialize(compressed.f1.f1.getBinary(3));
            List<Long> ids = new ArrayList<>();
            bitmap.forEach(ids::add);
            assertThat(ids).hasSize(100);
            assertThat(ids).contains(0L, 198L).doesNotContain(1L, 199L);
        }
    }

    @Test
    void testRetainedKeyIsCopiedBeforeInputReuse() throws Exception {
        NullBitmapShuffleOperator operator = new NullBitmapShuffleOperator(KEY_TYPE, false);
        try (OneInputStreamOperatorTestHarness<
                        Tuple2<Integer, Tuple2<InternalRow, RowData>>,
                        Tuple2<Integer, Tuple2<InternalRow, RowData>>>
                harness = new OneInputStreamOperatorTestHarness<>(operator)) {
            harness.open();
            Tuple2<Integer, Tuple2<InternalRow, RowData>> input = assigned(2, 1, null, 5);
            harness.processElement(new StreamRecord<>(input));
            ((GenericRow) input.f1.f0).setField(2, 999L);
            ((GenericRowData) input.f1.f1).setField(2, 999L);
            operator.endInput();
            assertThat(harness.extractOutputValues().get(0).f1.f0.getLong(2)).isEqualTo(5);
            assertThat(harness.extractOutputValues().get(0).f1.f1.getLong(2)).isEqualTo(5);
        }
    }

    @Test
    void testOriginalRowsUseCompactSerializerOnPreAssignmentBatchEdge() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        RowType valueType = KEY_TYPE;
        DataStream<RowData> input =
                env.fromData(
                        java.util.Collections.singletonList(assigned(0, 1, 7, 1).f1.f1),
                        org.apache.flink.table.runtime.typeutils.InternalTypeInfo.of(
                                LogicalTypeConversion.toLogicalType(valueType)));
        TableSortInfo sortInfo =
                new TableSortInfo.Builder()
                        .setSortColumns(valueType.getFieldNames().subList(0, 3))
                        .setSortStrategy(CoreOptions.OrderType.ORDER)
                        .setSinkParallelism(2)
                        .setLocalSampleSize(100)
                        .setGlobalSampleSize(100)
                        .setRangeNumber(20)
                        .build();
        DataStream<RowData> result =
                new OrderSorter(env, input, new CoreOptions(new Options()), valueType, sortInfo)
                        .sort(
                                new NullBitmapShuffleOperator(KEY_TYPE),
                                new NullBitmapExpandOperator());
        Transformation<?> assignment =
                findTransformation(result.getTransformation(), "ASSIGN RANGE INDEX");
        TupleTypeInfo<?> preAssignmentType =
                (TupleTypeInfo<?>) assignment.getInputs().get(1).getOutputType();
        assertThat(preAssignmentType.getTypeAt(1)).isInstanceOf(NullBitmapTransportTypeInfo.class);
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
    void testAdaptiveDensityChangesFlushBeforeDisableAndCanReenable() throws Exception {
        NullBitmapShuffleOperator operator = new NullBitmapShuffleOperator(KEY_TYPE);
        try (OneInputStreamOperatorTestHarness<
                        Tuple2<Integer, Tuple2<InternalRow, RowData>>,
                        Tuple2<Integer, Tuple2<InternalRow, RowData>>>
                harness = new OneInputStreamOperatorTestHarness<>(operator)) {
            harness.open();
            long id = 0;
            for (; id < 256; id++) {
                harness.processElement(new StreamRecord<>(assigned(0, 1, null, id)));
                assertThat(harness.extractOutputValues()).hasSize((int) id + 1);
            }
            // The next window is mostly non-null, but starts with pending compressed nulls.
            for (; id < 256 + 4096; id++) {
                harness.processElement(new StreamRecord<>(assigned(0, 1, id < 356 ? null : 7, id)));
            }
            List<Tuple2<Integer, Tuple2<InternalRow, RowData>>> output =
                    harness.extractOutputValues();
            assertThat(output.stream().filter(row -> row.f1.f1.getArity() == 4).count()).isOne();
            assertThat(decodeDestinations(output)).hasSize((int) id);
            // Disabled nulls pass through immediately while the new dense window is observed.
            int previousSize = output.size();
            for (int i = 0; i < 4096; i++, id++) {
                harness.processElement(new StreamRecord<>(assigned(0, 1, null, id)));
                assertThat(harness.extractOutputValues()).hasSize(previousSize + i + 1);
            }
            harness.processElement(new StreamRecord<>(assigned(0, 1, null, id++)));
            assertThat(harness.extractOutputValues()).hasSize(previousSize + 4096);
            operator.endInput();
            assertThat(decodeDestinations(harness.extractOutputValues())).hasSize((int) id);
        }
    }

    @Test
    void testAdaptiveActualExchangeBytesAgainstOriginalThreeFieldSchema() throws Exception {
        final int count = 20000;
        SerializerConfig config = new ExecutionConfig().getSerializerConfig();
        TypeSerializer<Tuple2<Integer, Tuple2<InternalRow, RowData>>> baselineSerializer =
                new TupleTypeInfo<Tuple2<Integer, Tuple2<InternalRow, RowData>>>(
                                BasicTypeInfo.INT_TYPE_INFO,
                                new TupleTypeInfo<>(
                                        InternalTypeInfo.fromRowType(KEY_TYPE),
                                        org.apache.flink.table.runtime.typeutils.InternalTypeInfo
                                                .of(LogicalTypeConversion.toLogicalType(KEY_TYPE))))
                        .createSerializer(config);
        TypeSerializer<Tuple2<Integer, Tuple2<InternalRow, RowData>>> transportSerializer =
                new TupleTypeInfo<Tuple2<Integer, Tuple2<InternalRow, RowData>>>(
                                BasicTypeInfo.INT_TYPE_INFO,
                                new TupleTypeInfo<>(
                                        InternalTypeInfo.fromRowType(KEY_TYPE),
                                        new NullBitmapTransportTypeInfo(KEY_TYPE)))
                        .createSerializer(config);
        TypeSerializer<RowData> schema4Serializer =
                org.apache.flink.table.runtime.typeutils.InternalTypeInfo.of(
                                LogicalTypeConversion.toLogicalType(
                                        RowType.of(
                                                DataTypes.INT(),
                                                DataTypes.INT(),
                                                DataTypes.BIGINT(),
                                                DataTypes.BYTES())))
                        .createSerializer(config);
        DataOutputSerializer bytes = new DataOutputSerializer(128);
        baselineSerializer.serialize(
                Tuple2.of(0, Tuple2.of(GenericRow.of(1, 7, 1L), GenericRowData.of(1, 7, 1L))),
                bytes);
        byte[] baselineRecord = bytes.getCopyOfBuffer();
        bytes.clear();
        transportSerializer.serialize(assigned(0, 1, 7, 1), bytes);
        assertThat(bytes.getCopyOfBuffer()).isEqualTo(baselineRecord);
        bytes.clear();
        schema4Serializer.serialize(GenericRowData.of(1, 7, 1L, null), bytes);
        assertThat(bytes.length()).isEqualTo(44);
        bytes.clear();
        for (int percent : new int[] {0, 2, 50, 90, 100}) {
            NullBitmapShuffleOperator operator = new NullBitmapShuffleOperator(KEY_TYPE);
            try (OneInputStreamOperatorTestHarness<
                            Tuple2<Integer, Tuple2<InternalRow, RowData>>,
                            Tuple2<Integer, Tuple2<InternalRow, RowData>>>
                    harness = new OneInputStreamOperatorTestHarness<>(operator)) {
                harness.open();
                long baselineBytes = 0;
                for (int i = 0; i < count; i++) {
                    Integer value = i % 100 < percent ? null : 7;
                    Tuple2<Integer, Tuple2<InternalRow, RowData>> row = assigned(0, 1, value, i);
                    harness.processElement(new StreamRecord<>(row));
                    baselineSerializer.serialize(
                            Tuple2.of(
                                    0, Tuple2.of(row.f1.f0, GenericRowData.of(1, value, (long) i))),
                            bytes);
                    baselineBytes += bytes.length();
                    bytes.clear();
                }
                operator.endInput();
                long transportBytes = 0;
                List<Tuple2<Integer, Tuple2<InternalRow, RowData>>> roundTripped =
                        new ArrayList<>();
                for (Tuple2<Integer, Tuple2<InternalRow, RowData>> row :
                        harness.extractOutputValues()) {
                    transportSerializer.serialize(row, bytes);
                    transportBytes += bytes.length();
                    roundTripped.add(
                            transportSerializer.deserialize(
                                    new DataInputDeserializer(bytes.getCopyOfBuffer())));
                    bytes.clear();
                }
                assertThat(decodeDestinations(roundTripped)).hasSize(count);
                if (percent <= 50) {
                    assertThat(transportBytes).isEqualTo(baselineBytes);
                    assertThat(roundTripped).hasSize(count);
                } else {
                    assertThat(transportBytes).isLessThan(baselineBytes / 2);
                }
                System.out.printf(
                        "Flink null density %d%%: baseline3=%d adaptive=%d records=%d%n",
                        percent, baselineBytes, transportBytes, roundTripped.size());
            }
        }
    }

    @Test
    void testTransportSerializerCopiesRestoresAndPreservesVariableLengthValues() throws Exception {
        RowType type = RowType.of(DataTypes.INT(), DataTypes.STRING(), DataTypes.BIGINT());
        TypeSerializer<RowData> serializer =
                new NullBitmapTransportTypeInfo(type).createSerializer(new ExecutionConfig());
        DataOutputSerializer snapshotBytes = new DataOutputSerializer(128);
        TypeSerializerSnapshot.writeVersionedSnapshot(
                snapshotBytes, serializer.snapshotConfiguration());
        TypeSerializerSnapshot<RowData> snapshot =
                TypeSerializerSnapshot.readVersionedSnapshot(
                        new DataInputDeserializer(snapshotBytes.getCopyOfBuffer()),
                        getClass().getClassLoader());
        assertThat(snapshot.restoreSerializer()).isEqualTo(serializer);
        TypeSerializer<RowData> restored =
                org.apache.flink.util.InstantiationUtil.clone(
                        snapshot.restoreSerializer().duplicate());
        for (RowData row :
                new RowData[] {
                    GenericRowData.of(1, null, 99L),
                    GenericRowData.of(
                            1,
                            org.apache.flink.table.data.StringData.fromString(
                                    "a variable length value longer than the inline binary slot"),
                            100L),
                    GenericRowData.of(1, null, 101L, new byte[] {1, 2, 3})
                }) {
            DataOutputSerializer encoded = new DataOutputSerializer(128);
            serializer.serialize(row, encoded);
            DataOutputSerializer copied = new DataOutputSerializer(128);
            restored.copy(new DataInputDeserializer(encoded.getCopyOfBuffer()), copied);
            assertThat(copied.getCopyOfBuffer()).isEqualTo(encoded.getCopyOfBuffer());
            RowData decoded =
                    restored.deserialize(new DataInputDeserializer(copied.getCopyOfBuffer()));
            assertThat(decoded.getArity()).isEqualTo(row.getArity());
            assertThat(decoded.getLong(2)).isEqualTo(row.getLong(2));
            assertThat(decoded.isNullAt(1)).isEqualTo(row.isNullAt(1));
            if (!row.isNullAt(1)) {
                assertThat(decoded.getString(1)).isEqualTo(row.getString(1));
            }
            if (row.getArity() == 4) {
                assertThat(decoded.getBinary(3)).isEqualTo(row.getBinary(3));
            }
            RowData copiedRow = restored.copy(decoded);
            assertThat(copiedRow.getArity()).isEqualTo(row.getArity());
            assertThat(copiedRow.getLong(2)).isEqualTo(row.getLong(2));
            if (row.getArity() == 4) {
                assertThat(copiedRow.getBinary(3)).isEqualTo(row.getBinary(3));
            }
        }
    }

    private static Tuple2<Integer, Tuple2<InternalRow, RowData>> assigned(
            int range, int task, Integer key, long rowId) {
        return Tuple2.of(
                range,
                Tuple2.of(GenericRow.of(task, key, rowId), GenericRowData.of(task, key, rowId)));
    }

    private static Map<Long, Integer> decodeDestinations(
            List<Tuple2<Integer, Tuple2<InternalRow, RowData>>> output) throws Exception {
        Map<Long, Integer> destinations = new HashMap<>();
        for (Tuple2<Integer, Tuple2<InternalRow, RowData>> assigned : output) {
            RowData row = assigned.f1.f1;
            if (row.getArity() == 3) {
                assertThat(destinations.put(row.getLong(2), assigned.f0)).isNull();
            } else {
                RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
                bitmap.deserialize(row.getBinary(3));
                int count = 0;
                for (long id : bitmap) {
                    assertThat(destinations.put(id, assigned.f0)).isNull();
                    count++;
                }
                assertThat(count).isBetween(1, NullBitmapBuffer.MAX_ROWS);
            }
        }
        return destinations;
    }
}
