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

package org.apache.paimon.flink.sink.cdc;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Testing {@link RichParallelSourceFunction} to produce {@link TestCdcEvent}. {@link TestCdcEvent}s
 * with the same key will be produced by the same parallelism.
 */
public class TestCdcSourceFunction extends RichParallelSourceFunction<TestCdcEvent>
        implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;

    private final LinkedList<TestCdcEvent> events;

    private volatile boolean isRunning = true;
    private transient int numRecordsPerCheckpoint;
    private transient AtomicInteger recordsThisCheckpoint;
    private transient ListState<Integer> remainingEventsCount;

    public TestCdcSourceFunction(Collection<TestCdcEvent> events) {
        this.events = new LinkedList<>(events);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        numRecordsPerCheckpoint = events.size() / ThreadLocalRandom.current().nextInt(10, 20) + 1;
        recordsThisCheckpoint = new AtomicInteger(0);

        remainingEventsCount =
                context.getOperatorStateStore()
                        .getListState(new ListStateDescriptor<>("count", Integer.class));

        if (context.isRestored()) {
            int count = 0;
            for (int c : remainingEventsCount.get()) {
                count += c;
            }
            while (events.size() > count) {
                events.poll();
            }
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        recordsThisCheckpoint.set(0);
        remainingEventsCount.clear();
        remainingEventsCount.add(events.size());
    }

    @Override
    public void run(SourceContext<TestCdcEvent> ctx) throws Exception {
        while (isRunning && !events.isEmpty()) {
            if (recordsThisCheckpoint.get() >= numRecordsPerCheckpoint) {
                Thread.sleep(10);
                continue;
            }

            synchronized (ctx.getCheckpointLock()) {
                TestCdcEvent event = events.poll();
                if (event.records() != null) {
                    int subtaskId = getRuntimeContext().getIndexOfThisSubtask();
                    int totalSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
                    if (Math.abs(event.hashCode()) % totalSubtasks != subtaskId) {
                        continue;
                    }
                }
                ctx.collect(event);
                recordsThisCheckpoint.incrementAndGet();
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
