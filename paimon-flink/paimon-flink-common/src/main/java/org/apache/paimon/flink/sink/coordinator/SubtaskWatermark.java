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
 * Point-in-time watermark reading from a single subtask, paired with whether that subtask was idle
 * at the moment the reading was taken. Consumed by {@link WatermarkAligner} to reproduce {@code
 * StatusWatermarkValve} semantics over inputs that arrive via the {@code OperatorEvent} channel
 * rather than the operator's own edge.
 */
public final class SubtaskWatermark {

    private final long watermark;
    private final boolean idle;

    public SubtaskWatermark(long watermark, boolean idle) {
        this.watermark = watermark;
        this.idle = idle;
    }

    public long watermark() {
        return watermark;
    }

    public boolean idle() {
        return idle;
    }
}
