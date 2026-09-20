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

package org.apache.paimon.flink.source.operator;

import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.flink.utils.TestingMetricUtils;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FallbackReadFileStoreTable.FallbackSplitImpl;
import org.apache.paimon.table.source.ChainSplit;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.QueryAuthSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IteratorRecordReader;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.flink.LogicalTypeConversion.toLogicalType;
import static org.apache.paimon.io.DataFileTestUtils.newFile;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests {@link ReadOperator} over every shape of {@link Split} it can be handed. */
public class ReadOperatorSplitShapeTest {

    private static final RowType READ_TYPE = RowType.of(DataTypes.INT());

    private static final int VALUE = 42;

    @Test
    public void testWrappedDataSplitReadsTheRowsOfTheSplitItWraps() throws Exception {
        Split split = dataSplit();

        ReadResult plain = read(split);
        ReadResult authorized = read(withQueryAuth(split));

        assertThat(plain.values).containsExactly(VALUE);
        assertThat(authorized.values).isEqualTo(plain.values);
    }

    @Test
    public void testWrappedDataSplitIsHandedToTheReadStillWrapped() throws Exception {
        Split wrapped = withQueryAuth(dataSplit());

        ReadResult authorized = read(wrapped);

        assertThat(authorized.readSplit).isSameAs(wrapped);
        assertThat(authorized.readSplit).isInstanceOf(QueryAuthSplit.class);
    }

    @Test
    public void testSplitThatIsNotADataSplitIsReadWrappedOrNot() throws Exception {
        ReadResult plain = read(chainSplit());
        ReadResult authorized = read(withQueryAuth(chainSplit()));

        assertThat(plain.values).containsExactly(VALUE);
        assertThat(authorized.values).isEqualTo(plain.values);
    }

    @Test
    public void testFallbackWrappedQueryAuthSplitReadsTheRowsOfTheSplitItWraps() throws Exception {
        Split split = dataSplit();

        ReadResult plain = read(split);
        ReadResult nested = read(withFallback(withQueryAuth(split)));

        assertThat(plain.values).containsExactly(VALUE);
        assertThat(nested.values).isEqualTo(plain.values);
    }

    @Test
    public void testFallbackWrappedQueryAuthSplitIsHandedToTheReadStillWrapped() throws Exception {
        Split nested = withFallback(withQueryAuth(dataSplit()));

        ReadResult result = read(nested);

        assertThat(result.readSplit).isSameAs(nested);
        assertThat(result.readSplit).isInstanceOf(FallbackSplitImpl.class);
    }

    @Test
    public void testFallbackWrappedQueryAuthSplitKeepsTheEventTimeOfTheSplitItWraps()
            throws Exception {
        Split split = dataSplit();

        ReadResult plain = read(split);
        ReadResult authorized = read(withQueryAuth(split));
        ReadResult nested = read(withFallback(withQueryAuth(split)));
        ReadResult fallback = read(withFallback(split));

        assertThat(plain.fetchEventTimeLag).isGreaterThan(0L);
        assertThat(authorized.fetchEventTimeLag).isGreaterThan(0L);
        assertThat(nested.fetchEventTimeLag).isGreaterThan(0L);
        assertThat(fallback.fetchEventTimeLag).isGreaterThan(0L);
    }

    @Test
    public void testSplitWithoutAnEventTimeLeavesTheMetricUndefined() throws Exception {
        assertThat(read(chainSplit()).fetchEventTimeLag).isEqualTo(-1L);
        assertThat(read(withFallback(withQueryAuth(chainSplit()))).fetchEventTimeLag)
                .isEqualTo(-1L);
    }

    private static ReadResult read(Split split) throws Exception {
        RecordingTableRead tableRead = new RecordingTableRead();
        ReadOperator operator = new ReadOperator(() -> tableRead, null, null, READ_TYPE, false);

        OneInputStreamOperatorTestHarness<Split, RowData> harness =
                new OneInputStreamOperatorTestHarness<>(operator);
        harness.setup(InternalSerializers.create(toLogicalType(READ_TYPE)));
        harness.open();
        try {
            harness.processElement(new StreamRecord<>(split));

            List<Integer> values = new ArrayList<>();
            for (Object record : harness.getOutput()) {
                @SuppressWarnings("unchecked")
                StreamRecord<RowData> streamRecord = (StreamRecord<RowData>) record;
                values.add(streamRecord.getValue().getInt(0));
            }
            long fetchEventTimeLag =
                    (Long)
                            TestingMetricUtils.getGauge(
                                            operator.getMetricGroup(), "currentFetchEventTimeLag")
                                    .getValue();
            return new ReadResult(values, tableRead.split, fetchEventTimeLag);
        } finally {
            harness.close();
        }
    }

    private static Split dataSplit() {
        return DataSplit.builder()
                .withSnapshot(1)
                .withPartition(BinaryRow.EMPTY_ROW)
                .withBucket(0)
                .withBucketPath("bucket-0")
                .withDataFiles(Collections.singletonList(newFile(0L, 1L)))
                .build();
    }

    private static Split chainSplit() {
        return new ChainSplit(
                BinaryRow.EMPTY_ROW,
                Collections.singletonList(newFile(0L, 1L)),
                Collections.emptyMap(),
                Collections.emptyMap(),
                null);
    }

    private static Split withQueryAuth(Split split) {
        return new QueryAuthSplit(split, new TableQueryAuthResult(null, null));
    }

    private static Split withFallback(Split split) {
        return FallbackReadFileStoreTable.toFallbackSplit(split, true);
    }

    private static class ReadResult {

        private final List<Integer> values;
        private final Split readSplit;
        private final long fetchEventTimeLag;

        private ReadResult(List<Integer> values, Split readSplit, long fetchEventTimeLag) {
            this.values = values;
            this.readSplit = readSplit;
            this.fetchEventTimeLag = fetchEventTimeLag;
        }
    }

    private static class RecordingTableRead implements TableRead {

        private Split split;

        @Override
        public TableRead withMetricRegistry(MetricRegistry registry) {
            return this;
        }

        @Override
        public TableRead executeFilter() {
            return this;
        }

        @Override
        public TableRead withIOManager(IOManager ioManager) {
            return this;
        }

        @Override
        public RecordReader<InternalRow> createReader(Split split) throws IOException {
            this.split = split;
            InternalRow row = GenericRow.of(VALUE);
            return new IteratorRecordReader<>(Collections.singleton(row).iterator());
        }
    }
}
