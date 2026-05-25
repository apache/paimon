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

package org.apache.paimon.flink.source;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link StaticRowDataSource}. */
class StaticRowDataSourceTest {

    private static final RowType ROW_TYPE = RowType.of(DataTypes.INT());

    @Test
    void testReaderCheckpoints() throws Exception {
        List<InternalRow> rows = rows(0, 100);
        TestingReaderOutput<RowData> out = new TestingReaderOutput<>();

        SourceReader<RowData, StaticRowDataSource.StaticRowsSplit> reader = createReader();
        reader.addSplits(
                Arrays.asList(
                        new StaticRowDataSource.StaticRowsSplit("split-1", rows.subList(0, 35), 0),
                        new StaticRowDataSource.StaticRowsSplit(
                                "split-2", rows.subList(35, rows.size()), 0)));

        int remainingInCycle = 17;
        while (reader.pollNext(out) != InputStatus.END_OF_INPUT) {
            if (--remainingInCycle <= 0) {
                remainingInCycle = 17;

                List<StaticRowDataSource.StaticRowsSplit> splits = reader.snapshotState(1L);

                reader = createReader();
                if (splits.isEmpty()) {
                    reader.notifyNoMoreSplits();
                } else {
                    reader.addSplits(splits);
                }
            }
        }

        assertThat(values(out.getEmittedRecords())).containsExactlyElementsOf(values(0, 100));
    }

    @Test
    void testSplitSerializer() throws Exception {
        StaticRowDataSource source = new StaticRowDataSource(Collections.emptyList(), ROW_TYPE);
        SimpleVersionedSerializer<StaticRowDataSource.StaticRowsSplit> serializer =
                source.getSplitSerializer();

        StaticRowDataSource.StaticRowsSplit split =
                new StaticRowDataSource.StaticRowsSplit("split-1", rows(0, 10), 3);
        StaticRowDataSource.StaticRowsSplit restored =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(split));

        assertThat(read(restored)).containsExactlyElementsOf(values(3, 10));
    }

    @Test
    void testEnumeratorCheckpointSerializer() throws Exception {
        StaticRowDataSource source = new StaticRowDataSource(Collections.emptyList(), ROW_TYPE);
        SimpleVersionedSerializer<Collection<StaticRowDataSource.StaticRowsSplit>> serializer =
                source.getEnumeratorCheckpointSerializer();

        Collection<StaticRowDataSource.StaticRowsSplit> checkpoint =
                Arrays.asList(
                        new StaticRowDataSource.StaticRowsSplit("split-1", rows(0, 5), 2),
                        new StaticRowDataSource.StaticRowsSplit("split-2", rows(5, 9), 1));
        Collection<StaticRowDataSource.StaticRowsSplit> restored =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(checkpoint));

        TestingReaderOutput<RowData> out = new TestingReaderOutput<>();
        SourceReader<RowData, StaticRowDataSource.StaticRowsSplit> reader = createReader();
        reader.addSplits(new ArrayList<>(restored));
        read(reader, out, 6);

        assertThat(values(out.getEmittedRecords())).containsExactly(2, 3, 4, 6, 7, 8);
    }

    private static List<Integer> read(StaticRowDataSource.StaticRowsSplit split) throws Exception {
        TestingReaderOutput<RowData> out = new TestingReaderOutput<>();
        SourceReader<RowData, StaticRowDataSource.StaticRowsSplit> reader = createReader();
        reader.addSplits(Collections.singletonList(split));
        read(reader, out, 7);
        return values(out.getEmittedRecords());
    }

    private static void read(
            SourceReader<RowData, StaticRowDataSource.StaticRowsSplit> reader,
            TestingReaderOutput<RowData> out,
            int expectedSize)
            throws Exception {
        while (out.getEmittedRecords().size() < expectedSize) {
            assertThat(reader.pollNext(out)).isNotEqualTo(InputStatus.END_OF_INPUT);
        }
    }

    private static List<InternalRow> rows(int from, int to) {
        List<InternalRow> rows = new ArrayList<>(to - from);
        for (int i = from; i < to; i++) {
            rows.add(GenericRow.of(i));
        }
        return rows;
    }

    private static List<Integer> values(int from, int to) {
        List<Integer> values = new ArrayList<>(to - from);
        for (int i = from; i < to; i++) {
            values.add(i);
        }
        return values;
    }

    private static List<Integer> values(List<RowData> rows) {
        List<Integer> values = new ArrayList<>(rows.size());
        for (RowData row : rows) {
            values.add(row.getInt(0));
        }
        return values;
    }

    private static SourceReader<RowData, StaticRowDataSource.StaticRowsSplit> createReader() {
        return new StaticRowDataSource(Collections.emptyList(), ROW_TYPE)
                .createReader(new DummyReaderContext());
    }

    private static final class DummyReaderContext implements SourceReaderContext {

        @Override
        public SourceReaderMetricGroup metricGroup() {
            return UnregisteredMetricsGroup.createSourceReaderMetricGroup();
        }

        @Override
        public Configuration getConfiguration() {
            return new Configuration();
        }

        @Override
        public String getLocalHostName() {
            return "localhost";
        }

        @Override
        public int getIndexOfSubtask() {
            return 0;
        }

        @Override
        public void sendSplitRequest() {}

        @Override
        public void sendSourceEventToCoordinator(SourceEvent sourceEvent) {}

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            return SimpleUserCodeClassLoader.create(getClass().getClassLoader());
        }

        @Override
        public int currentParallelism() {
            return 1;
        }
    }

    private static final class TestingReaderOutput<E> implements ReaderOutput<E> {

        private final ArrayList<E> emittedRecords = new ArrayList<>();

        @Override
        public void collect(E record) {
            emittedRecords.add(record);
        }

        @Override
        public void collect(E record, long timestamp) {
            collect(record);
        }

        @Override
        public void emitWatermark(Watermark watermark) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void markIdle() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void markActive() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SourceOutput<E> createOutputForSplit(String splitId) {
            return this;
        }

        @Override
        public void releaseOutputForSplit(String splitId) {}

        private ArrayList<E> getEmittedRecords() {
            return emittedRecords;
        }
    }
}
