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

import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.flink.FlinkRowWrapper;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.flink.sorter.SortOperator;
import org.apache.paimon.flink.utils.InternalTypeInfo;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.sort.BinaryExternalSortBuffer;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Measures actual local-sort spills after the enabled transport path at low null density. */
class NullBitmapSortSpillBenchmarkTest {

    private static final int ROW_COUNT = 20000;
    private static final long FIRST_ROW_ID = 1L << 33;
    private static final RowType KEY_TYPE =
            RowType.of(DataTypes.INT(), DataTypes.INT(), DataTypes.BIGINT());
    private static final RowType SORT_TYPE =
            RowType.of(
                    DataTypes.INT(),
                    DataTypes.INT(),
                    DataTypes.BIGINT(),
                    DataTypes.INT(),
                    DataTypes.INT(),
                    DataTypes.BIGINT());
    private static final RowType LEGACY_SORT_TYPE =
            RowType.of(
                    DataTypes.INT(),
                    DataTypes.INT(),
                    DataTypes.BIGINT(),
                    DataTypes.INT(),
                    DataTypes.INT(),
                    DataTypes.BIGINT(),
                    DataTypes.BYTES());

    @ParameterizedTest
    @ValueSource(ints = {0, 2})
    void testLowNullSortSpillsWithOriginalEnabledAndLegacyRows(int nullPercent) throws Exception {
        List<Tuple2<InternalRow, RowData>> source = new ArrayList<>();
        for (long id = FIRST_ROW_ID + ROW_COUNT - 1; id >= FIRST_ROW_ID; id--) {
            Integer key = key(id, nullPercent);
            source.add(Tuple2.of(GenericRow.of(0, key, id), GenericRowData.of(0, key, id)));
        }
        List<Long> expectedIds =
                LongStream.range(FIRST_ROW_ID, FIRST_ROW_ID + ROW_COUNT)
                        .boxed()
                        .sorted(
                                Comparator.comparing(
                                                (Long id) -> key(id, nullPercent),
                                                Comparator.nullsFirst(Integer::compareTo))
                                        .thenComparingLong(Long::longValue))
                        .collect(Collectors.toList());

        // Native exchanges also decode to binary rows; compare the same representation.
        List<Tuple2<InternalRow, RowData>> original = roundTripNativeTransport(source, false);
        List<Tuple2<InternalRow, RowData>> legacyRows = roundTripNativeTransport(source, true);
        long transportStart = System.nanoTime();
        List<Tuple2<InternalRow, RowData>> transported = roundTripAdaptiveTransport(source);
        long transportMillis = (System.nanoTime() - transportStart) / 1000000;
        assertThat(transported).hasSize(ROW_COUNT);
        assertThat(transported).allSatisfy(row -> assertThat(row.f1.getArity()).isEqualTo(3));

        // Warm all layouts, then rotate their order; measure sorting separately from transport.
        sort(original, false);
        sort(transported, false);
        sort(legacyRows, true);
        SortMetrics[] measured = new SortMetrics[3];
        long[][] elapsed = new long[3][3];
        for (int run = 0; run < 3; run++) {
            for (int offset = 0; offset < 3; offset++) {
                int variant = (run + offset) % 3;
                List<Tuple2<InternalRow, RowData>> rows =
                        variant == 0 ? original : variant == 1 ? transported : legacyRows;
                SortMetrics metrics = sort(rows, variant == 2);
                measured[variant] = metrics;
                elapsed[variant][run] = metrics.elapsedMillis;
            }
        }
        for (int variant = 0; variant < 3; variant++) {
            Arrays.sort(elapsed[variant]);
            measured[variant].elapsedMillis = elapsed[variant][1];
        }
        SortMetrics off = measured[0];
        SortMetrics enabled = measured[1];
        SortMetrics legacy = measured[2];

        for (SortMetrics metrics : new SortMetrics[] {off, enabled, legacy}) {
            assertThat(metrics.sortedIds).containsExactlyElementsOf(expectedIds);
            assertThat(metrics.firstSpill).isBetween(1, ROW_COUNT - 1);
            assertThat(metrics.finalSpillBytes).isPositive();
        }
        assertThat(off.rowBytes).isEqualTo(56);
        assertThat(enabled.rowBytes).isEqualTo(56);
        assertThat(legacy.rowBytes).isEqualTo(64);
        assertThat(enabled.firstSpill).isEqualTo(off.firstSpill);
        assertThat(enabled.peakSpillBytes).isEqualTo(off.peakSpillBytes);
        assertThat(enabled.finalSpillBytes).isEqualTo(off.finalSpillBytes);
        assertThat(legacy.firstSpill).isLessThan(off.firstSpill);
        report(nullPercent, "original/off6", off);
        report(nullPercent, "enabled6", enabled);
        report(nullPercent, "legacy7", legacy);
        System.out.printf(
                "FLINK_NULL_TRANSPORT nulls=%d%% harnessElapsedMs=%d (includes test harness setup)%n",
                nullPercent, transportMillis);
    }

    private static Integer key(long rowId, int nullPercent) {
        long ordinal = rowId - FIRST_ROW_ID;
        return ordinal * 31 % 100 < nullPercent ? null : (int) (ordinal * 17 % 1000);
    }

    private static List<Tuple2<InternalRow, RowData>> roundTripAdaptiveTransport(
            List<Tuple2<InternalRow, RowData>> input) throws Exception {
        NullBitmapShuffleOperator compressor = new NullBitmapShuffleOperator(KEY_TYPE);
        TypeSerializer<RowData> codec =
                new NullBitmapTransportTypeInfo(KEY_TYPE).createSerializer(new ExecutionConfig());
        try (OneInputStreamOperatorTestHarness<
                                Tuple2<Integer, Tuple2<InternalRow, RowData>>,
                                Tuple2<Integer, Tuple2<InternalRow, RowData>>>
                        compressHarness = new OneInputStreamOperatorTestHarness<>(compressor);
                OneInputStreamOperatorTestHarness<
                                Tuple2<InternalRow, RowData>, Tuple2<InternalRow, RowData>>
                        expandHarness =
                                new OneInputStreamOperatorTestHarness<>(
                                        new NullBitmapExpandOperator())) {
            compressHarness.open();
            expandHarness.open();
            for (Tuple2<InternalRow, RowData> row : input) {
                compressHarness.processElement(new StreamRecord<>(Tuple2.of(0, row)));
            }
            compressor.endInput();
            DataOutputSerializer encoded = new DataOutputSerializer(128);
            for (Tuple2<Integer, Tuple2<InternalRow, RowData>> row :
                    compressHarness.extractOutputValues()) {
                encoded.clear();
                codec.serialize(row.f1.f1, encoded);
                RowData decoded =
                        codec.deserialize(new DataInputDeserializer(encoded.getCopyOfBuffer()));
                expandHarness.processElement(new StreamRecord<>(Tuple2.of(row.f1.f0, decoded)));
            }
            return expandHarness.extractOutputValues();
        }
    }

    private static List<Tuple2<InternalRow, RowData>> roundTripNativeTransport(
            List<Tuple2<InternalRow, RowData>> input, boolean legacy) throws IOException {
        RowType valueType =
                legacy
                        ? RowType.of(
                                DataTypes.INT(),
                                DataTypes.INT(),
                                DataTypes.BIGINT(),
                                DataTypes.BYTES())
                        : KEY_TYPE;
        RowDataSerializer serializer =
                new RowDataSerializer(LogicalTypeConversion.toLogicalType(valueType));
        List<Tuple2<InternalRow, RowData>> rows = new ArrayList<>();
        DataOutputSerializer encoded = new DataOutputSerializer(128);
        for (Tuple2<InternalRow, RowData> row : input) {
            RowData value = row.f1;
            if (legacy) {
                value =
                        GenericRowData.of(
                                value.getInt(0),
                                value.isNullAt(1) ? null : value.getInt(1),
                                value.getLong(2),
                                null);
            }
            encoded.clear();
            serializer.serialize(value, encoded);
            rows.add(
                    Tuple2.of(
                            row.f0,
                            serializer.deserialize(
                                    new DataInputDeserializer(encoded.getCopyOfBuffer()))));
        }
        return rows;
    }

    private static SortMetrics sort(List<Tuple2<InternalRow, RowData>> rows, boolean legacy)
            throws Exception {
        RowType sortType = legacy ? LEGACY_SORT_TYPE : SORT_TYPE;
        SortOperator sorter =
                new SortOperator(
                        KEY_TYPE,
                        sortType,
                        64 * 1024,
                        4 * 1024,
                        128,
                        CompressOptions.defaultOptions(),
                        1,
                        MemorySize.MAX_VALUE);
        try (OneInputStreamOperatorTestHarness<InternalRow, InternalRow> harness =
                new OneInputStreamOperatorTestHarness<>(sorter)) {
            harness.setup(
                    InternalTypeInfo.fromRowType(sortType).createSerializer(new ExecutionConfig()));
            harness.open();
            File[] spillDirs = harness.getEnvironment().getIOManager().getSpillingDirectories();
            Field bufferField = SortOperator.class.getDeclaredField("buffer");
            bufferField.setAccessible(true);
            BinaryExternalSortBuffer buffer = (BinaryExternalSortBuffer) bufferField.get(sorter);
            InternalRowSerializer serializer = new InternalRowSerializer(sortType);
            SortMetrics metrics = new SortMetrics();
            long sortNanos = 0;
            long previousOccupancy = 0;
            int count = 0;
            for (Tuple2<InternalRow, RowData> row : rows) {
                InternalRow sortRow = new JoinedRow(row.f0, new FlinkRowWrapper(row.f1));
                if (count == 0) {
                    metrics.rowBytes = serializer.toBinaryRow(sortRow).getSizeInBytes();
                }
                long writeStart = System.nanoTime();
                harness.processElement(new StreamRecord<>(sortRow));
                sortNanos += System.nanoTime() - writeStart;
                count++;
                long occupancy = buffer.getOccupancy();
                // A drop marks a completed spill; measure the files written by that run.
                if (occupancy < previousOccupancy) {
                    long bytes = spillBytes(spillDirs);
                    assertThat(bytes).isPositive();
                    if (metrics.firstSpill == 0) {
                        metrics.firstSpill = count;
                    }
                    metrics.peakSpillBytes = Math.max(metrics.peakSpillBytes, bytes);
                }
                previousOccupancy = occupancy;
            }
            long endStart = System.nanoTime();
            sorter.endInput();
            sortNanos += System.nanoTime() - endStart;
            metrics.elapsedMillis = sortNanos / 1000000;
            metrics.finalSpillBytes = spillBytes(spillDirs);
            metrics.peakSpillBytes = Math.max(metrics.peakSpillBytes, metrics.finalSpillBytes);
            for (InternalRow row : harness.extractOutputValues()) {
                metrics.sortedIds.add(row.getLong(5));
            }
            return metrics;
        }
    }

    private static long spillBytes(File[] directories) throws IOException {
        long bytes = 0;
        for (File directory : directories) {
            try (Stream<Path> files = Files.walk(directory.toPath())) {
                Iterator<Path> iterator = files.filter(Files::isRegularFile).iterator();
                while (iterator.hasNext()) {
                    bytes += Files.size(iterator.next());
                }
            }
        }
        return bytes;
    }

    private static void report(int nullPercent, String variant, SortMetrics metrics) {
        System.out.printf(
                "FLINK_NULL_SORT nulls=%d%% variant=%s rows=%d rowBytes=%d firstSpill=%d peakSpillBytes=%d finalSpillBytes=%d medianSortMs=%d%n",
                nullPercent,
                variant,
                metrics.sortedIds.size(),
                metrics.rowBytes,
                metrics.firstSpill,
                metrics.peakSpillBytes,
                metrics.finalSpillBytes,
                metrics.elapsedMillis);
    }

    private static class SortMetrics {
        private int rowBytes;
        private int firstSpill;
        private long peakSpillBytes;
        private long finalSpillBytes;
        private long elapsedMillis;
        private final List<Long> sortedIds = new ArrayList<>();
    }
}
