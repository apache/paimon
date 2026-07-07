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

import org.apache.flink.runtime.state.heap.HeapPriorityQueue;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * A simple implementation of coordinator watermark alignment. It cannot fully match the operator
 * watermark semantics as it lacks idle information.
 *
 * <p>And currently, it does not support revert watermark. The watermark always grows.
 */
public class SimpleWatermarkValve {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleWatermarkValve.class);

    private final SimpleWatermark[] subtaskWatermarks;

    // reuse HeapPriorityQueue of flink runtime
    private final HeapPriorityQueue<SimpleWatermark> watermarkPriorityQueue;

    private long currentWatermark;

    public SimpleWatermarkValve(int parallelism) {
        this.subtaskWatermarks = new SimpleWatermark[parallelism];
        this.watermarkPriorityQueue =
                new HeapPriorityQueue<>(
                        (left, right) ->
                                Long.compare(left.getWatermarkValue(), right.getWatermarkValue()),
                        parallelism);
        for (int i = 0; i < parallelism; i++) {
            subtaskWatermarks[i] = new SimpleWatermark();
            watermarkPriorityQueue.add(subtaskWatermarks[i]);
        }
        this.currentWatermark = Long.MIN_VALUE;
    }

    public void reset(long watermark) {
        LOG.info("Reset watermark valve to {}", watermark);
        for (SimpleWatermark simpleWatermark : subtaskWatermarks) {
            simpleWatermark.setWatermarkValue(watermark);
        }
        this.currentWatermark = watermark;
    }

    public void updateSubtaskWatermark(int subtask, long watermark) {
        if (watermark == Long.MAX_VALUE) {
            // Do not consume Long.MAX_VALUE watermark in case of batch or bounded stream
            return;
        }
        if (watermark > subtaskWatermarks[subtask].getWatermarkValue()) {
            subtaskWatermarks[subtask].setWatermarkValue(watermark);
            watermarkPriorityQueue.adjustModifiedElement(subtaskWatermarks[subtask]);
            long minWatermark =
                    Objects.requireNonNull(watermarkPriorityQueue.peek()).getWatermarkValue();
            if (currentWatermark != minWatermark) {
                LOG.debug("Update watermark to {}", minWatermark);
                currentWatermark = minWatermark;
            }
        }
    }

    public long getCurrentWatermark() {
        return currentWatermark;
    }

    private static class SimpleWatermark implements HeapPriorityQueueElement {
        private int heapIndex = HeapPriorityQueueElement.NOT_CONTAINED;

        private long watermarkValue = Long.MIN_VALUE;

        public void setWatermarkValue(long watermarkValue) {
            this.watermarkValue = watermarkValue;
        }

        public long getWatermarkValue() {
            return watermarkValue;
        }

        @Override
        public int getInternalIndex() {
            return heapIndex;
        }

        @Override
        public void setInternalIndex(int newIndex) {
            heapIndex = newIndex;
        }
    }
}
