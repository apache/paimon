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
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.ChannelComputer;
import org.apache.paimon.table.sink.RowPartitionKeyExtractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * {@link ChannelComputer} for {@link StatisticsOrRecord} which dynamically adjusts shuffle based on
 * partition statistics.
 */
public class StatisticsOrRecordChannelComputer implements ChannelComputer<StatisticsOrRecord> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG =
            LoggerFactory.getLogger(StatisticsOrRecordChannelComputer.class);

    private static final int DEFAULT_SUBTASK_COUNT_FOR_UNKNOWN_PARTITION = 4;

    private final TableSchema schema;

    private transient int numChannels;
    private transient RowPartitionKeyExtractor extractor;
    private transient MapPartitioner delegatePartitioner;
    private transient Random random;

    public StatisticsOrRecordChannelComputer(TableSchema schema) {
        this.schema = schema;
    }

    @Override
    public void setup(int numChannels) {
        this.numChannels = numChannels;
        this.extractor = new RowPartitionKeyExtractor(schema);
        this.random = ThreadLocalRandom.current();
    }

    @Override
    public int channel(StatisticsOrRecord wrapper) {
        if (wrapper.isStatistics()) {
            this.delegatePartitioner = buildPartitioner(wrapper.statistics());
            return ThreadLocalRandom.current().nextInt(numChannels);
        } else {
            if (delegatePartitioner == null) {
                delegatePartitioner = buildPartitioner(null);
            }
            BinaryRow partition = extractor.partition(wrapper.record());
            return delegatePartitioner.select(partition, numChannels);
        }
    }

    private MapPartitioner buildPartitioner(@Nullable DataStatistics statistics) {
        if (statistics == null) {
            return new MapPartitioner(new HashMap<>());
        }
        return new MapPartitioner(buildAssignment(numChannels, statistics.result()));
    }

    Map<BinaryRow, WeightedRandomAssignment> buildAssignment(
            int downstreamParallelism, Map<BinaryRow, Long> statistics) {
        if (statistics.isEmpty()) {
            return new HashMap<>();
        }

        long totalWeight = statistics.values().stream().mapToLong(l -> l).sum();
        if (totalWeight <= 0) {
            return new HashMap<>();
        }
        long targetWeightPerSubtask =
                (long) Math.ceil(((double) totalWeight) / downstreamParallelism);

        // Sort keys for deterministic assignment across JVMs
        List<Map.Entry<BinaryRow, Long>> sortedEntries = new ArrayList<>(statistics.entrySet());
        sortedEntries.sort(Comparator.comparingInt(e -> e.getKey().hashCode()));

        Map<BinaryRow, WeightedRandomAssignment> assignmentMap = new HashMap<>(statistics.size());
        Iterator<Map.Entry<BinaryRow, Long>> entryIterator = sortedEntries.iterator();
        int subtaskId = 0;
        BinaryRow currentKey = null;
        long keyRemainingWeight = 0L;
        long subtaskRemainingWeight = targetWeightPerSubtask;
        List<Integer> assignedSubtasks = new ArrayList<>();
        List<Long> subtaskWeights = new ArrayList<>();

        while (entryIterator.hasNext() || currentKey != null) {
            if (subtaskId >= downstreamParallelism) {
                LOG.error(
                        "Internal algorithm error: exhausted subtasks. parallelism: {}, "
                                + "target weight per subtask: {}, statistics: {}",
                        downstreamParallelism,
                        targetWeightPerSubtask,
                        statistics);
                throw new IllegalStateException(
                        "Internal algorithm error: exhausted subtasks with unassigned keys left");
            }

            if (currentKey == null) {
                Map.Entry<BinaryRow, Long> entry = entryIterator.next();
                currentKey = entry.getKey();
                keyRemainingWeight = entry.getValue();
            }

            assignedSubtasks.add(subtaskId);
            if (keyRemainingWeight < subtaskRemainingWeight) {
                subtaskWeights.add(keyRemainingWeight);
                subtaskRemainingWeight -= keyRemainingWeight;
                keyRemainingWeight = 0L;
            } else {
                long assignedWeight = subtaskRemainingWeight;
                keyRemainingWeight -= subtaskRemainingWeight;
                subtaskWeights.add(assignedWeight);
                subtaskId += 1;
                subtaskRemainingWeight = targetWeightPerSubtask;
            }

            checkState(
                    assignedSubtasks.size() == subtaskWeights.size(),
                    "List size mismatch: assigned subtasks = %s, subtask weights = %s",
                    assignedSubtasks,
                    subtaskWeights);

            if (keyRemainingWeight == 0) {
                WeightedRandomAssignment assignment =
                        new WeightedRandomAssignment(assignedSubtasks, subtaskWeights, random);
                assignmentMap.put(currentKey, assignment);
                assignedSubtasks = new ArrayList<>();
                subtaskWeights = new ArrayList<>();
                currentKey = null;
            }
        }

        LOG.debug("Assignment map: {}", assignmentMap);
        return assignmentMap;
    }

    @Override
    public String toString() {
        return "PARTITION_DYNAMIC";
    }

    private class MapPartitioner {

        private final Map<BinaryRow, WeightedRandomAssignment> assignments;

        MapPartitioner(Map<BinaryRow, WeightedRandomAssignment> assignments) {
            this.assignments = assignments;
        }

        int select(BinaryRow partitionKey, int numChannels) {
            WeightedRandomAssignment assignment = assignments.get(partitionKey);
            if (assignment == null) {
                int defaultSubtaskCount =
                        Math.min(numChannels, DEFAULT_SUBTASK_COUNT_FOR_UNKNOWN_PARTITION);
                int startChannel = Math.abs(partitionKey.hashCode()) % numChannels;
                List<Integer> subtasks = new ArrayList<>(defaultSubtaskCount);
                List<Long> weights = new ArrayList<>(defaultSubtaskCount);
                for (int i = 0; i < defaultSubtaskCount; i++) {
                    subtasks.add((startChannel + i) % numChannels);
                    weights.add(1L);
                }
                assignment = new WeightedRandomAssignment(subtasks, weights, random);
                assignments.put(partitionKey.copy(), assignment);
            }
            return assignment.select();
        }
    }
}
