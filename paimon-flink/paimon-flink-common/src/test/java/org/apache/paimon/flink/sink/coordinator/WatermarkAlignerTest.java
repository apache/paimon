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

package org.apache.paimon.flink.sink.coordinator;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link WatermarkAligner}. */
public class WatermarkAlignerTest {

    private static final int PARALLELISM = 3;

    @Test
    public void testAllActiveTakesMinAcrossSubtasks() {
        WatermarkAligner aligner = new WatermarkAligner(PARALLELISM);
        assertThat(aligner.align(new SubtaskWatermark[] {active(100L), active(200L), active(300L)}))
                .isEqualTo(100L);
    }

    @Test
    public void testMonotonicMinAcrossCheckpoints() {
        WatermarkAligner aligner = new WatermarkAligner(PARALLELISM);
        aligner.align(new SubtaskWatermark[] {active(100L), active(200L), active(300L)});
        // Min across subtasks for the next reading is 250 — greater than the last emit of 100.
        assertThat(aligner.align(new SubtaskWatermark[] {active(400L), active(250L), active(500L)}))
                .isEqualTo(250L);
    }

    @Test
    public void testIdleSubtaskIsSkippedAndActiveMinAdvances() {
        WatermarkAligner aligner = new WatermarkAligner(PARALLELISM);
        aligner.align(new SubtaskWatermark[] {active(100L), active(200L), active(300L)});
        // subtask-0 goes IDLE; remaining actives push min to 400.
        assertThat(aligner.align(new SubtaskWatermark[] {idle(100L), active(400L), active(500L)}))
                .isEqualTo(400L);
    }

    @Test
    public void testAllIdleFlushesMaxThenLatches() {
        WatermarkAligner aligner = new WatermarkAligner(PARALLELISM);
        aligner.align(new SubtaskWatermark[] {active(100L), active(200L), active(300L)});

        // Everyone goes IDLE with their last known watermarks — flush max=300.
        assertThat(aligner.align(new SubtaskWatermark[] {idle(100L), idle(200L), idle(300L)}))
                .isEqualTo(300L);

        // Still all idle — flush must not re-fire, emission stays at 300.
        assertThat(aligner.align(new SubtaskWatermark[] {idle(100L), idle(200L), idle(350L)}))
                .isEqualTo(300L);
    }

    @Test
    public void testIdleThenActiveWithLagStaysHeldUntilCatchUp() {
        WatermarkAligner aligner = new WatermarkAligner(PARALLELISM);
        aligner.align(new SubtaskWatermark[] {active(100L), active(200L), active(300L)});

        // subtask-0 IDLE; actives advance min to 250.
        assertThat(aligner.align(new SubtaskWatermark[] {idle(100L), active(250L), active(400L)}))
                .isEqualTo(250L);

        // subtask-0 comes back ACTIVE at 150 — still lagging (< last=250) so it stays unaligned;
        // min over aligned actives is min(300, 500) = 300.
        assertThat(aligner.align(new SubtaskWatermark[] {active(150L), active(300L), active(500L)}))
                .isEqualTo(300L);

        // subtask-0 climbs to 260 (< last=300) — still lagging, aligned min = 320.
        assertThat(aligner.align(new SubtaskWatermark[] {active(260L), active(320L), active(600L)}))
                .isEqualTo(320L);

        // subtask-0 finally reaches 350 >= last=320, rejoins the aligned set; new min=350.
        assertThat(aligner.align(new SubtaskWatermark[] {active(350L), active(400L), active(700L)}))
                .isEqualTo(350L);
    }

    @Test
    public void testAbsentSubtaskBlocksMinLikeNeverReported() {
        WatermarkAligner aligner = new WatermarkAligner(PARALLELISM);
        // Absent subtasks are represented by ACTIVE + Long.MIN_VALUE — the same shape Flink's
        // valve exposes for a channel that has never emitted a watermark. Under that reading the
        // min stays at Long.MIN_VALUE (never-reported subtasks block the min, unlike idle ones).
        assertThat(
                        aligner.align(
                                new SubtaskWatermark[] {
                                    active(500L), active(Long.MIN_VALUE), active(Long.MIN_VALUE)
                                }))
                .isEqualTo(Long.MIN_VALUE);
    }

    private static SubtaskWatermark active(long watermark) {
        return new SubtaskWatermark(watermark, false);
    }

    private static SubtaskWatermark idle(long watermark) {
        return new SubtaskWatermark(watermark, true);
    }
}
