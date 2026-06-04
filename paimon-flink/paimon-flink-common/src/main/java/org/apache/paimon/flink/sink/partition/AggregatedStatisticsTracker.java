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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * Tracks statistics aggregation from {@link DataStatisticsOperator} subtasks for every checkpoint.
 */
class AggregatedStatisticsTracker {

    private static final Logger LOG = LoggerFactory.getLogger(AggregatedStatisticsTracker.class);

    private final String operatorName;
    private final int parallelism;
    private final TypeSerializer<DataStatistics> statisticsSerializer;
    private final NavigableMap<Long, Aggregation> aggregationsPerCheckpoint;

    private long completedCheckpointId;
    private DataStatistics completedStatistics;

    AggregatedStatisticsTracker(String operatorName, int parallelism) {
        this.operatorName = operatorName;
        this.parallelism = parallelism;
        this.statisticsSerializer = new DataStatisticsSerializer();
        this.aggregationsPerCheckpoint = new TreeMap<>();
        this.completedCheckpointId = -1;
    }

    DataStatistics updateAndCheckCompletion(int subtask, StatisticsEvent event) {
        long checkpointId = event.getCheckpointId();
        LOG.debug(
                "Handling statistics event from subtask {} of operator {} for checkpoint {}",
                subtask,
                operatorName,
                checkpointId);

        if (completedStatistics != null && completedCheckpointId > checkpointId) {
            LOG.debug(
                    "Ignore stale statistics event from operator {} subtask {} for older checkpoint {}. "
                            + "Was expecting checkpoint higher than {}",
                    operatorName,
                    subtask,
                    checkpointId,
                    completedCheckpointId);
            return null;
        }

        Aggregation aggregation =
                aggregationsPerCheckpoint.computeIfAbsent(
                        checkpointId, ignored -> new Aggregation(parallelism));
        DataStatistics dataStatistics =
                StatisticsUtil.deserializeDataStatistics(
                        event.getStatisticsBytes(), statisticsSerializer);
        if (!aggregation.merge(subtask, dataStatistics)) {
            LOG.debug(
                    "Ignore duplicate data statistics from operator {} subtask {} for checkpoint {}.",
                    operatorName,
                    subtask,
                    checkpointId);
        }

        if (aggregation.isComplete()) {
            this.completedStatistics = aggregation.completedStatistics();
            this.completedCheckpointId = checkpointId;
            aggregationsPerCheckpoint.headMap(checkpointId, true).clear();
            return completedStatistics;
        }

        return null;
    }

    static class Aggregation {
        private final int parallelism;
        private final Set<Integer> subtaskSet;
        private final Map<BinaryRow, Long> partitionStatistics;

        Aggregation(int parallelism) {
            this.parallelism = parallelism;
            this.subtaskSet = new HashSet<>();
            this.partitionStatistics = new HashMap<>();
        }

        boolean isComplete() {
            return subtaskSet.size() == parallelism;
        }

        boolean merge(int subtask, DataStatistics taskStatistics) {
            if (subtaskSet.contains(subtask)) {
                return false;
            }
            subtaskSet.add(subtask);
            Map<BinaryRow, Long> result = taskStatistics.result();
            result.forEach(
                    (partition, count) -> partitionStatistics.merge(partition, count, Long::sum));
            return true;
        }

        DataStatistics completedStatistics() {
            return new DataStatistics(partitionStatistics);
        }
    }
}
