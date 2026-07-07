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

/** Unit test case of {@link SimpleWatermarkValve}. */
public class SimpleWatermarkValveTest {
    @Test
    public void test() {
        SimpleWatermarkValve watermarkValve = new SimpleWatermarkValve(3);
        assertThat(watermarkValve.getCurrentWatermark()).isEqualTo(Long.MIN_VALUE);
        // update task-0
        watermarkValve.updateSubtaskWatermark(0, 1L);
        assertThat(watermarkValve.getCurrentWatermark()).isEqualTo(Long.MIN_VALUE);
        // update task-1
        watermarkValve.updateSubtaskWatermark(1, 2L);
        assertThat(watermarkValve.getCurrentWatermark()).isEqualTo(Long.MIN_VALUE);
        // re-update task-0
        watermarkValve.updateSubtaskWatermark(0, 3L);
        assertThat(watermarkValve.getCurrentWatermark()).isEqualTo(Long.MIN_VALUE);
        // update task-2, it's aligned
        watermarkValve.updateSubtaskWatermark(2, 4L);
        assertThat(watermarkValve.getCurrentWatermark()).isEqualTo(2L);
        // reset
        watermarkValve.reset(0L);
        assertThat(watermarkValve.getCurrentWatermark()).isEqualTo(0L);
        // re-update all watermarks
        watermarkValve.updateSubtaskWatermark(0, 101L);
        watermarkValve.updateSubtaskWatermark(1, 100L);
        watermarkValve.updateSubtaskWatermark(2, 102L);
        assertThat(watermarkValve.getCurrentWatermark()).isEqualTo(100L);
        // older watermark would be ignored
        watermarkValve.updateSubtaskWatermark(1, 1L);
        assertThat(watermarkValve.getCurrentWatermark()).isEqualTo(100L);
        // Long.MAX_VALUE would be ignored
        watermarkValve.updateSubtaskWatermark(0, Long.MAX_VALUE);
        watermarkValve.updateSubtaskWatermark(1, Long.MAX_VALUE);
        watermarkValve.updateSubtaskWatermark(2, Long.MAX_VALUE);
        assertThat(watermarkValve.getCurrentWatermark()).isEqualTo(100L);
    }
}
