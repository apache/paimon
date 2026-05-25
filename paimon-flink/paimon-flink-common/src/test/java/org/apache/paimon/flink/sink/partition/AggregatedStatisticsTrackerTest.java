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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link AggregatedStatisticsTracker}. */
class AggregatedStatisticsTrackerTest {

    private static final DataStatisticsSerializer SERIALIZER = new DataStatisticsSerializer();

    @Test
    void testAggregationCompletesWhenAllSubtasksReport() {
        AggregatedStatisticsTracker tracker = new AggregatedStatisticsTracker("test-op", 2);

        BinaryRow p1 = BinaryRow.singleColumn("p1");
        BinaryRow p2 = BinaryRow.singleColumn("p2");
        BinaryRow p3 = BinaryRow.singleColumn("p3");

        DataStatistics stats0 = new DataStatistics();
        stats0.add(p1, 100L);
        stats0.add(p2, 50L);

        DataStatistics stats1 = new DataStatistics();
        stats1.add(p1, 200L);
        stats1.add(p3, 75L);

        StatisticsEvent event0 = StatisticsEvent.createStatisticsEvent(1L, stats0, SERIALIZER);
        StatisticsEvent event1 = StatisticsEvent.createStatisticsEvent(1L, stats1, SERIALIZER);

        // First subtask reports - not complete yet
        DataStatistics result = tracker.updateAndCheckCompletion(0, event0);
        assertThat(result).isNull();

        // Second subtask reports - now complete
        result = tracker.updateAndCheckCompletion(1, event1);
        assertThat(result).isNotNull();
        assertThat(result.result().get(p1)).isEqualTo(300L);
        assertThat(result.result().get(p2)).isEqualTo(50L);
        assertThat(result.result().get(p3)).isEqualTo(75L);
    }

    @Test
    void testIgnoresDuplicateSubtaskReport() {
        AggregatedStatisticsTracker tracker = new AggregatedStatisticsTracker("test-op", 2);

        BinaryRow p1 = BinaryRow.singleColumn("p1");
        Map<BinaryRow, Long> map = new HashMap<>();
        map.put(p1, 100L);
        DataStatistics stats = new DataStatistics(map);
        StatisticsEvent event = StatisticsEvent.createStatisticsEvent(1L, stats, SERIALIZER);

        // First report from subtask 0
        DataStatistics result = tracker.updateAndCheckCompletion(0, event);
        assertThat(result).isNull();

        // Duplicate report from subtask 0 - should be ignored, still not complete
        result = tracker.updateAndCheckCompletion(0, event);
        assertThat(result).isNull();
    }

    @Test
    void testIgnoresStaleCheckpoint() {
        AggregatedStatisticsTracker tracker = new AggregatedStatisticsTracker("test-op", 1);

        BinaryRow p1 = BinaryRow.singleColumn("p1");
        Map<BinaryRow, Long> map = new HashMap<>();
        map.put(p1, 100L);
        DataStatistics stats = new DataStatistics(map);

        // Complete checkpoint 2
        StatisticsEvent event2 = StatisticsEvent.createStatisticsEvent(2L, stats, SERIALIZER);
        DataStatistics result = tracker.updateAndCheckCompletion(0, event2);
        assertThat(result).isNotNull();

        // Now send event for older checkpoint 1 - should be ignored
        StatisticsEvent event1 = StatisticsEvent.createStatisticsEvent(1L, stats, SERIALIZER);
        result = tracker.updateAndCheckCompletion(0, event1);
        assertThat(result).isNull();
    }

    @Test
    void testMultipleCheckpoints() {
        AggregatedStatisticsTracker tracker = new AggregatedStatisticsTracker("test-op", 2);

        BinaryRow p1 = BinaryRow.singleColumn("p1");

        // Checkpoint 1: subtask 0 reports
        Map<BinaryRow, Long> map1 = new HashMap<>();
        map1.put(p1, 10L);
        DataStatistics statsChk1Sub0 = new DataStatistics(map1);
        StatisticsEvent eventChk1Sub0 =
                StatisticsEvent.createStatisticsEvent(1L, statsChk1Sub0, SERIALIZER);
        assertThat(tracker.updateAndCheckCompletion(0, eventChk1Sub0)).isNull();

        // Checkpoint 2: subtask 0 reports (before checkpoint 1 completes)
        Map<BinaryRow, Long> map2 = new HashMap<>();
        map2.put(p1, 20L);
        DataStatistics statsChk2Sub0 = new DataStatistics(map2);
        StatisticsEvent eventChk2Sub0 =
                StatisticsEvent.createStatisticsEvent(2L, statsChk2Sub0, SERIALIZER);
        assertThat(tracker.updateAndCheckCompletion(0, eventChk2Sub0)).isNull();

        // Checkpoint 1: subtask 1 reports - completes checkpoint 1
        Map<BinaryRow, Long> map3 = new HashMap<>();
        map3.put(p1, 15L);
        DataStatistics statsChk1Sub1 = new DataStatistics(map3);
        StatisticsEvent eventChk1Sub1 =
                StatisticsEvent.createStatisticsEvent(1L, statsChk1Sub1, SERIALIZER);
        DataStatistics result = tracker.updateAndCheckCompletion(1, eventChk1Sub1);
        assertThat(result).isNotNull();
        assertThat(result.result().get(p1)).isEqualTo(25L);
    }

    @Test
    void testEmptyStatisticsSkipped() {
        AggregatedStatisticsTracker tracker = new AggregatedStatisticsTracker("test-op", 1);

        DataStatistics emptyStats = new DataStatistics();
        StatisticsEvent event = StatisticsEvent.createStatisticsEvent(1L, emptyStats, SERIALIZER);
        DataStatistics result = tracker.updateAndCheckCompletion(0, event);
        // Returns the completed (empty) statistics - caller decides to skip
        assertThat(result).isNotNull();
        assertThat(result.isEmpty()).isTrue();
    }

    @Test
    void testThreeSubtasks() {
        AggregatedStatisticsTracker tracker = new AggregatedStatisticsTracker("test-op", 3);

        BinaryRow p1 = BinaryRow.singleColumn("p1");
        BinaryRow p2 = BinaryRow.singleColumn("p2");

        Map<BinaryRow, Long> freq0 = new HashMap<>();
        freq0.put(p1, 100L);
        Map<BinaryRow, Long> freq1 = new HashMap<>();
        freq1.put(p1, 200L);
        freq1.put(p2, 50L);
        Map<BinaryRow, Long> freq2 = new HashMap<>();
        freq2.put(p2, 150L);

        assertThat(
                        tracker.updateAndCheckCompletion(
                                0,
                                StatisticsEvent.createStatisticsEvent(
                                        1L, new DataStatistics(freq0), SERIALIZER)))
                .isNull();
        assertThat(
                        tracker.updateAndCheckCompletion(
                                1,
                                StatisticsEvent.createStatisticsEvent(
                                        1L, new DataStatistics(freq1), SERIALIZER)))
                .isNull();

        DataStatistics result =
                tracker.updateAndCheckCompletion(
                        2,
                        StatisticsEvent.createStatisticsEvent(
                                1L, new DataStatistics(freq2), SERIALIZER));
        assertThat(result).isNotNull();
        assertThat(result.result().get(p1)).isEqualTo(300L);
        assertThat(result.result().get(p2)).isEqualTo(200L);
    }
}
