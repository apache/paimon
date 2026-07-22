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

/**
 * Coordinator-side counterpart of Flink's {@code StatusWatermarkValve}. Aggregates a per-subtask
 * ({@link SubtaskWatermark} array indexed by subtask id) into a single monotonically non-decreasing
 * watermark.
 *
 * <p>The algorithm mirrors {@code StatusWatermarkValve} channel-by-channel:
 *
 * <ul>
 *   <li>A subtask that reports {@code idle=true} becomes unaligned and is excluded from the
 *       aligned-set min.
 *   <li>A subtask that reports {@code idle=false} rejoins the aligned set once its watermark has
 *       caught up to the last emitted watermark; while lagging it stays unaligned so it does not
 *       drag the global watermark backwards.
 *   <li>If every subtask is unaligned (all idle, or all lagging), the aligner emits a one-shot
 *       "flush max" — the maximum over every subtask's last known watermark — matching what {@code
 *       StatusWatermarkValve} does when the last active channel transitions to IDLE.
 * </ul>
 *
 * <p>State is purely in-memory: the aligner mirrors {@code StatusWatermarkValve}'s "no checkpoint,
 * rebuild on restart" contract. A fresh instance starts every subtask ACTIVE with {@code
 * Long.MIN_VALUE}, so upstream must re-send {@code WatermarkStatus.IDLE} for it to take effect
 * again — same as Flink's built-in valve on task restart.
 */
public class WatermarkAligner {

    private final boolean[] aligned;

    private long lastEmittedWatermark;
    private boolean idleStatus;

    public WatermarkAligner(int parallelism) {
        this.aligned = new boolean[parallelism];
        for (int i = 0; i < parallelism; i++) {
            aligned[i] = true;
        }
        this.lastEmittedWatermark = Long.MIN_VALUE;
        this.idleStatus = false;
    }

    /**
     * Aggregate the given per-subtask readings into a single watermark. Updates the aligner's
     * internal per-subtask alignment as a side effect, so successive calls see valve-faithful
     * catch-up semantics.
     *
     * <p>Contract: successive calls must correspond to strictly increasing checkpoint ids. Calling
     * out of order would apply a later checkpoint's alignment side effects to an earlier one,
     * silently corrupting the emitted watermark sequence.
     *
     * @return the aligned watermark for this call, guaranteed to be monotonically non-decreasing
     *     across successive calls.
     */
    public long align(SubtaskWatermark[] subtaskWatermarks) {
        int parallelism = subtaskWatermarks.length;
        if (parallelism != aligned.length) {
            throw new IllegalStateException(
                    "Aligner parallelism "
                            + aligned.length
                            + " does not match input "
                            + parallelism);
        }

        long alignedMin = Long.MAX_VALUE;
        boolean anyAligned = false;
        for (int i = 0; i < parallelism; i++) {
            long watermark = subtaskWatermarks[i].watermark();
            boolean idle = subtaskWatermarks[i].idle();
            if (idle) {
                aligned[i] = false;
                continue;
            }
            if (!aligned[i] && watermark >= lastEmittedWatermark) {
                aligned[i] = true;
            }
            if (aligned[i]) {
                anyAligned = true;
                if (watermark < alignedMin) {
                    alignedMin = watermark;
                }
            }
        }

        if (anyAligned) {
            if (alignedMin > lastEmittedWatermark) {
                lastEmittedWatermark = alignedMin;
            }
            idleStatus = false;
        } else if (!idleStatus) {
            // All-unaligned transition: flush the max across every subtask's last known watermark
            // (equivalent to StatusWatermarkValve.findAndOutputMaxWatermarkAcrossAllSubpartitions),
            // then latch to IDLE so subsequent all-unaligned checkpoints do not re-flush.
            long flushMax = Long.MIN_VALUE;
            for (int i = 0; i < parallelism; i++) {
                long watermark = subtaskWatermarks[i].watermark();
                if (watermark > flushMax) {
                    flushMax = watermark;
                }
            }
            if (flushMax > lastEmittedWatermark) {
                lastEmittedWatermark = flushMax;
            }
            idleStatus = true;
        }

        return lastEmittedWatermark;
    }
}
