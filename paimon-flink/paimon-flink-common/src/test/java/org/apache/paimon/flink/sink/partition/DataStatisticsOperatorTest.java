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

package org.apache.paimon.flink.sink.partition;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DataStatisticsOperator}. */
class DataStatisticsOperatorTest {

    private static final RowType ROW_TYPE =
            RowType.builder()
                    .field("id", DataTypes.INT())
                    .field("pt", DataTypes.STRING())
                    .field("data", DataTypes.STRING())
                    .build();

    private static final TableSchema SCHEMA =
            new TableSchema(
                    0L,
                    ROW_TYPE.getFields(),
                    ROW_TYPE.getFieldCount(),
                    Collections.singletonList("pt"),
                    Collections.emptyList(),
                    new HashMap<>(),
                    null);

    @SuppressWarnings("unchecked")
    private List<StatisticsOrRecord> extractRecordOutput(
            OneInputStreamOperatorTestHarness<InternalRow, StatisticsOrRecord> testHarness) {
        List<StatisticsOrRecord> result = new ArrayList<>();
        for (Object o : testHarness.getOutput()) {
            if (o instanceof StreamRecord) {
                result.add(((StreamRecord<StatisticsOrRecord>) o).getValue());
            }
        }
        return result;
    }

    @Test
    void testProcessElement() throws Exception {
        DataStatisticsOperatorFactory factory = new DataStatisticsOperatorFactory(SCHEMA);

        try (OneInputStreamOperatorTestHarness<InternalRow, StatisticsOrRecord> testHarness =
                new OneInputStreamOperatorTestHarness<>(factory, 1, 1, 0)) {
            testHarness.open();

            InternalRow row1 =
                    GenericRow.of(1, BinaryString.fromString("pt1"), BinaryString.fromString("a"));
            InternalRow row2 =
                    GenericRow.of(2, BinaryString.fromString("pt1"), BinaryString.fromString("b"));
            InternalRow row3 =
                    GenericRow.of(3, BinaryString.fromString("pt2"), BinaryString.fromString("c"));

            testHarness.processElement(new StreamRecord<>(row1));
            testHarness.processElement(new StreamRecord<>(row2));
            testHarness.processElement(new StreamRecord<>(row3));

            List<StatisticsOrRecord> output = extractRecordOutput(testHarness);
            assertThat(output).hasSize(3);
            assertThat(output.get(0).isRecord()).isTrue();
            assertThat(output.get(1).isRecord()).isTrue();
            assertThat(output.get(2).isRecord()).isTrue();
        }
    }

    @Test
    void testHandleOperatorEvent() throws Exception {
        DataStatisticsOperatorFactory factory = new DataStatisticsOperatorFactory(SCHEMA);

        try (OneInputStreamOperatorTestHarness<InternalRow, StatisticsOrRecord> testHarness =
                new OneInputStreamOperatorTestHarness<>(factory, 1, 1, 0)) {
            testHarness.open();

            // Simulate receiving global statistics from coordinator
            Map<BinaryRow, Long> globalStats = new HashMap<>();
            globalStats.put(BinaryRow.singleColumn("pt1"), 1000L);
            globalStats.put(BinaryRow.singleColumn("pt2"), 2000L);
            DataStatistics globalStatistics = new DataStatistics(globalStats);
            StatisticsEvent event =
                    StatisticsEvent.createStatisticsEvent(
                            1L, globalStatistics, new DataStatisticsSerializer());

            ((DataStatisticsOperator) testHarness.getOperator()).handleOperatorEvent(event);

            List<StatisticsOrRecord> output = extractRecordOutput(testHarness);
            assertThat(output).hasSize(1);
            assertThat(output.get(0).isStatistics()).isTrue();
            assertThat(output.get(0).statistics().result()).isEqualTo(globalStats);
        }
    }

    @Test
    void testProcessAndHandleEvent() throws Exception {
        DataStatisticsOperatorFactory factory = new DataStatisticsOperatorFactory(SCHEMA);

        try (OneInputStreamOperatorTestHarness<InternalRow, StatisticsOrRecord> testHarness =
                new OneInputStreamOperatorTestHarness<>(factory, 1, 1, 0)) {
            testHarness.open();

            // Process some records
            InternalRow row1 =
                    GenericRow.of(1, BinaryString.fromString("pt1"), BinaryString.fromString("a"));
            InternalRow row2 =
                    GenericRow.of(2, BinaryString.fromString("pt2"), BinaryString.fromString("b"));
            testHarness.processElement(new StreamRecord<>(row1));
            testHarness.processElement(new StreamRecord<>(row2));

            // Receive global statistics
            Map<BinaryRow, Long> globalStats = new HashMap<>();
            globalStats.put(BinaryRow.singleColumn("pt1"), 500L);
            StatisticsEvent event =
                    StatisticsEvent.createStatisticsEvent(
                            1L, new DataStatistics(globalStats), new DataStatisticsSerializer());
            ((DataStatisticsOperator) testHarness.getOperator()).handleOperatorEvent(event);

            // Process more records
            InternalRow row3 =
                    GenericRow.of(3, BinaryString.fromString("pt1"), BinaryString.fromString("c"));
            testHarness.processElement(new StreamRecord<>(row3));

            List<StatisticsOrRecord> output = extractRecordOutput(testHarness);
            // 2 records + 1 statistics + 1 record = 4
            assertThat(output).hasSize(4);
            assertThat(output.get(0).isRecord()).isTrue();
            assertThat(output.get(1).isRecord()).isTrue();
            assertThat(output.get(2).isStatistics()).isTrue();
            assertThat(output.get(3).isRecord()).isTrue();
        }
    }
}
