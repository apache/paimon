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
import org.apache.paimon.table.sink.RowPartitionKeyExtractor;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link StatisticsOrRecordChannelComputer}. */
class StatisticsOrRecordChannelComputerTest {

    private static TableSchema schema;

    @BeforeAll
    static void init() {
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("pt", DataTypes.STRING())
                        .field("data", DataTypes.STRING())
                        .build();
        schema =
                new TableSchema(
                        0L,
                        rowType.getFields(),
                        rowType.getFieldCount(),
                        Collections.singletonList("pt"),
                        Collections.emptyList(),
                        new HashMap<>(),
                        null);
    }

    @Test
    void testShuffleWithoutStatistics() {
        int downstreamParallelism = 8;
        StatisticsOrRecordChannelComputer channelComputer =
                new StatisticsOrRecordChannelComputer(schema);
        channelComputer.setup(downstreamParallelism);

        Map<Integer, Double> subtaskAssignedCounts = new HashMap<>();
        int totalRowNum = 50000;
        for (int i = 0; i < totalRowNum; i++) {
            InternalRow row =
                    GenericRow.of(i, BinaryString.fromString("pt1"), BinaryString.fromString("d"));
            int channel = channelComputer.channel(StatisticsOrRecord.fromRecord(row));
            subtaskAssignedCounts.merge(channel, 1.0 / totalRowNum, Double::sum);
        }

        // Without statistics, fallback assigns min(numChannels, 4) subtasks
        int targetParallelism = Math.min(downstreamParallelism, 4);
        assertThat(subtaskAssignedCounts.size()).isEqualTo(targetParallelism);
        for (Double percentage : subtaskAssignedCounts.values()) {
            assertThat(percentage).isCloseTo(1.0 / targetParallelism, Percentage.withPercentage(5));
        }
    }

    @Test
    void testShuffleWithSinglePartitionStatistics() {
        int downstreamParallelism = 8;
        StatisticsOrRecordChannelComputer channelComputer =
                new StatisticsOrRecordChannelComputer(schema);
        channelComputer.setup(downstreamParallelism);

        // Feed statistics: single partition gets all weight -> spread across all subtasks
        Map<BinaryRow, Long> partitionFrequency = new HashMap<>();
        InternalRow sampleRow =
                GenericRow.of(0, BinaryString.fromString("pt1"), BinaryString.fromString("d"));
        BinaryRow partitionKey = getPartitionKey(sampleRow);
        partitionFrequency.put(partitionKey, 10000L);
        channelComputer.channel(
                StatisticsOrRecord.fromStatistics(new DataStatistics(partitionFrequency)));

        Map<Integer, Double> subtaskAssignedCounts = new HashMap<>();
        int totalRowNum = 50000;
        for (int i = 0; i < totalRowNum; i++) {
            InternalRow row =
                    GenericRow.of(i, BinaryString.fromString("pt1"), BinaryString.fromString("d"));
            int channel = channelComputer.channel(StatisticsOrRecord.fromRecord(row));
            subtaskAssignedCounts.merge(channel, 1.0 / totalRowNum, Double::sum);
        }

        assertThat(subtaskAssignedCounts.size()).isEqualTo(downstreamParallelism);
        for (Double percentage : subtaskAssignedCounts.values()) {
            assertThat(percentage)
                    .isCloseTo(1.0 / downstreamParallelism, Percentage.withPercentage(5));
        }
    }

    @Test
    void testShuffleWithMultiplePartitionStatistics() {
        int downstreamParallelism = 8;
        StatisticsOrRecordChannelComputer channelComputer =
                new StatisticsOrRecordChannelComputer(schema);
        channelComputer.setup(downstreamParallelism);

        InternalRow sampleRow1 =
                GenericRow.of(0, BinaryString.fromString("pt1"), BinaryString.fromString("d"));
        InternalRow sampleRow2 =
                GenericRow.of(0, BinaryString.fromString("pt2"), BinaryString.fromString("d"));
        BinaryRow partitionKey1 = getPartitionKey(sampleRow1);
        BinaryRow partitionKey2 = getPartitionKey(sampleRow2);

        // partition 1 has 1/4 of the weight, partition 2 has 3/4
        Map<BinaryRow, Long> partitionFrequency = new HashMap<>();
        partitionFrequency.put(partitionKey1, 10000L);
        partitionFrequency.put(partitionKey2, 30000L);
        channelComputer.channel(
                StatisticsOrRecord.fromStatistics(new DataStatistics(partitionFrequency)));

        Map<Integer, Double> subtaskAssignedCounts = new HashMap<>();
        int totalRowNum = 50000;
        for (int i = 0; i < totalRowNum; i++) {
            InternalRow row =
                    GenericRow.of(i, BinaryString.fromString("pt1"), BinaryString.fromString("d"));
            int channel = channelComputer.channel(StatisticsOrRecord.fromRecord(row));
            subtaskAssignedCounts.merge(channel, 1.0 / totalRowNum, Double::sum);
        }

        // partition 1 is 1/4 of total, so it should be assigned to ~2 subtasks (8/4)
        assertThat(subtaskAssignedCounts.size()).isEqualTo(downstreamParallelism / 4);
    }

    @Test
    void testMultipleUnknownPartitionsWithoutStatistics() {
        int downstreamParallelism = 8;
        StatisticsOrRecordChannelComputer channelComputer =
                new StatisticsOrRecordChannelComputer(schema);
        channelComputer.setup(downstreamParallelism);

        // Send records from multiple unknown partitions in interleaved order.
        // This exercises the fallback cache in MapPartitioner.select(): each partition
        // key must be stored as a copy, otherwise the reused BinaryRow from
        // RowPartitionKeyExtractor corrupts earlier cache entries.
        Map<String, Map<Integer, Integer>> partitionChannelCounts = new HashMap<>();
        String[] partitions = {"pt_a", "pt_b", "pt_c"};
        int recordsPerPartition = 10000;
        for (int i = 0; i < recordsPerPartition; i++) {
            for (String pt : partitions) {
                InternalRow row =
                        GenericRow.of(i, BinaryString.fromString(pt), BinaryString.fromString("d"));
                int channel = channelComputer.channel(StatisticsOrRecord.fromRecord(row));
                partitionChannelCounts
                        .computeIfAbsent(pt, k -> new HashMap<>())
                        .merge(channel, 1, Integer::sum);
            }
        }

        // Each unknown partition should be assigned to its own set of subtasks
        // (deterministic based on partition key hash). Verify they don't all collapse
        // into the same assignment, which would happen if the mutable key were shared.
        for (String pt : partitions) {
            Map<Integer, Integer> counts = partitionChannelCounts.get(pt);
            assertThat(counts).isNotEmpty();
            int total = counts.values().stream().mapToInt(Integer::intValue).sum();
            assertThat(total).isEqualTo(recordsPerPartition);
        }

        // Verify that not all partitions share the exact same channel set, which would
        // indicate mutable key corruption (all keys pointing to the last partition's hash)
        Map<Integer, Integer> countsA = partitionChannelCounts.get("pt_a");
        Map<Integer, Integer> countsB = partitionChannelCounts.get("pt_b");
        Map<Integer, Integer> countsC = partitionChannelCounts.get("pt_c");
        boolean allSame =
                countsA.keySet().equals(countsB.keySet())
                        && countsB.keySet().equals(countsC.keySet());
        assertThat(allSame)
                .as(
                        "Different partitions should generally get different channel assignments, "
                                + "but all three got the same channels: %s",
                        countsA.keySet())
                .isFalse();
    }

    @Test
    void testBuildAssignment() {
        StatisticsOrRecordChannelComputer channelComputer =
                new StatisticsOrRecordChannelComputer(schema);
        channelComputer.setup(4);

        InternalRow sampleRow1 =
                GenericRow.of(0, BinaryString.fromString("p1"), BinaryString.fromString("d"));
        InternalRow sampleRow2 =
                GenericRow.of(0, BinaryString.fromString("p2"), BinaryString.fromString("d"));
        BinaryRow p1 = getPartitionKey(sampleRow1);
        BinaryRow p2 = getPartitionKey(sampleRow2);

        Map<BinaryRow, Long> statistics = new HashMap<>();
        statistics.put(p1, 100L);
        statistics.put(p2, 300L);

        Map<BinaryRow, WeightedRandomAssignment> assignment =
                channelComputer.buildAssignment(4, statistics);

        assertThat(assignment).containsKey(p1);
        assertThat(assignment).containsKey(p2);
    }

    private BinaryRow getPartitionKey(InternalRow row) {
        RowPartitionKeyExtractor extractor = new RowPartitionKeyExtractor(schema);
        return extractor.partition(row).copy();
    }
}
