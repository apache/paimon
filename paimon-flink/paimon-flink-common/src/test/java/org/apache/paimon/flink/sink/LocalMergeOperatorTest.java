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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.mergetree.localmerge.HashMapLocalMerger;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Consumer;

import static org.apache.paimon.CoreOptions.LOCAL_MERGE_BUFFER_SIZE;
import static org.apache.paimon.CoreOptions.SEQUENCE_FIELD;
import static org.apache.paimon.data.BinaryString.fromString;
import static org.apache.paimon.types.RowKind.DELETE;
import static org.assertj.core.api.Assertions.assertThat;

class LocalMergeOperatorTest {

    private LocalMergeOperator operator;

    @Test
    public void testHashNormal() throws Exception {
        prepareHashOperator();
        List<String> result = new ArrayList<>();
        setOutput(result);

        // first test
        processElement("a", 1);
        processElement("b", 1);
        processElement("a", 2);
        processElement(DELETE, "b", 2);
        operator.prepareSnapshotPreBarrier(0);
        assertThat(result).containsExactlyInAnyOrder("+I:a->2", "-D:b->2");
        result.clear();

        // second test
        processElement("c", 1);
        processElement("d", 1);
        operator.prepareSnapshotPreBarrier(0);
        assertThat(result).containsExactlyInAnyOrder("+I:c->1", "+I:d->1");
        result.clear();

        // large records
        Map<String, String> expected = new HashMap<>();
        Random rnd = new Random();
        int records = 10_000;
        for (int i = 0; i < records; i++) {
            String key = rnd.nextInt(records) + "";
            expected.put(key, "+I:" + key + "->" + i);
            processElement(key, i);
        }

        operator.prepareSnapshotPreBarrier(0);
        assertThat(result).containsExactlyInAnyOrderElementsOf(expected.values());
        result.clear();
    }

    @Test
    public void testUserDefineSequence() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(SEQUENCE_FIELD.key(), "f1");
        prepareHashOperator(options);

        List<String> result = new ArrayList<>();
        setOutput(result);

        processElement("a", 2);
        processElement("b", 1);
        processElement("a", 1);
        operator.prepareSnapshotPreBarrier(0);
        assertThat(result).containsExactlyInAnyOrder("+I:a->2", "+I:b->1");
        result.clear();
    }

    @Test
    public void testHashSpill() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(LOCAL_MERGE_BUFFER_SIZE.key(), "2 m");
        prepareHashOperator(options);
        List<String> result = new ArrayList<>();
        setOutput(result);

        Map<String, String> expected = new HashMap<>();
        for (int i = 0; i < 30_000; i++) {
            String key = i + "";
            expected.put(key, "+I:" + key + "->" + i);
            processElement(key, i);
        }

        operator.prepareSnapshotPreBarrier(0);
        assertThat(result).containsExactlyInAnyOrderElementsOf(expected.values());
        result.clear();
    }

    private void prepareHashOperator() throws Exception {
        prepareHashOperator(new HashMap<>());
    }

    private void prepareHashOperator(Map<String, String> options) throws Exception {
        if (!options.containsKey(LOCAL_MERGE_BUFFER_SIZE.key())) {
            options.put(LOCAL_MERGE_BUFFER_SIZE.key(), "10 m");
        }
        RowType rowType =
                RowType.of(
                        DataTypes.STRING(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT());
        TableSchema schema =
                new TableSchema(
                        0L,
                        rowType.getFields(),
                        rowType.getFieldCount(),
                        Collections.emptyList(),
                        Collections.singletonList("f0"),
                        options,
                        null);
        operator = new LocalMergeOperator(schema);
        operator.open();
        assertThat(operator.merger()).isInstanceOf(HashMapLocalMerger.class);
    }

    private void setOutput(List<String> result) {
        operator.setOutput(
                new TestOutput(
                        row ->
                                result.add(
                                        row.getRowKind().shortString()
                                                + ":"
                                                + row.getString(0)
                                                + "->"
                                                + row.getInt(1))));
    }

    private void processElement(String key, int value) throws Exception {
        processElement(RowKind.INSERT, key, value);
    }

    private void processElement(RowKind rowKind, String key, int value) throws Exception {
        operator.processElement(
                new StreamRecord<>(
                        GenericRow.ofKind(rowKind, fromString(key), value, value, value, value)));
    }

    private static class TestOutput implements Output<StreamRecord<InternalRow>> {

        private final Consumer<InternalRow> consumer;

        private TestOutput(Consumer<InternalRow> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void emitWatermark(Watermark mark) {}

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {}

        @Override
        public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {}

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) {}

        @Override
        public void emitRecordAttributes(RecordAttributes recordAttributes) {}

        @Override
        public void collect(StreamRecord<InternalRow> record) {
            consumer.accept(record.getValue());
        }

        @Override
        public void close() {}
    }
}
