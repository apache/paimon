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

package org.apache.paimon.flink;

import org.apache.paimon.utils.Preconditions;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * A stream source that: 1) emits a list of elements without allowing checkpoints, 2) then waits for
 * two more checkpoints to complete, 3) then re-emits the same elements before 4) waiting for
 * another two checkpoints and 5) exiting.
 *
 * <p>The reason this class is rewritten is to support {@link CheckpointedFunction}.
 */
public class FiniteTestSource<T>
        implements SourceFunction<T>, CheckpointedFunction, CheckpointListener {

    private static final long serialVersionUID = 1L;

    private final List<T> elements;

    private final boolean emitOnce;

    private volatile boolean running = true;

    private transient int numCheckpointsComplete;

    private transient ListState<Integer> checkpointedState;

    private volatile int numTimesEmitted;

    public FiniteTestSource(List<T> elements, boolean emitOnce) {
        this.elements = elements;
        this.emitOnce = emitOnce;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        this.checkpointedState =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>("emit-times", IntSerializer.INSTANCE));

        if (context.isRestored()) {
            List<Integer> retrievedStates = new ArrayList<>();
            for (Integer entry : this.checkpointedState.get()) {
                retrievedStates.add(entry);
            }

            // given that the parallelism of the function is 1, we can only have 1 state
            Preconditions.checkArgument(
                    retrievedStates.size() == 1,
                    getClass().getSimpleName() + " retrieved invalid state.");

            this.numTimesEmitted = retrievedStates.get(0);
            Preconditions.checkArgument(
                    numTimesEmitted <= 2,
                    getClass().getSimpleName()
                            + " retrieved invalid numTimesEmitted: "
                            + numTimesEmitted);
        } else {
            this.numTimesEmitted = 0;
        }
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        switch (numTimesEmitted) {
            case 0:
                emitElementsAndWaitForCheckpoints(ctx, false);
                emitElementsAndWaitForCheckpoints(ctx, true);
                break;
            case 1:
                emitElementsAndWaitForCheckpoints(ctx, true);
                break;
            case 2:
                // Maybe missed notifyCheckpointComplete, wait next notifyCheckpointComplete
                final Object lock = ctx.getCheckpointLock();
                synchronized (lock) {
                    int checkpointToAwait = numCheckpointsComplete + 2;
                    while (running && numCheckpointsComplete < checkpointToAwait) {
                        lock.wait(1);
                    }
                }
                break;
        }
    }

    private void emitElementsAndWaitForCheckpoints(SourceContext<T> ctx, boolean isSecond)
            throws InterruptedException {
        final Object lock = ctx.getCheckpointLock();

        final int checkpointToAwait;
        synchronized (lock) {
            checkpointToAwait = numCheckpointsComplete + 2;
            if (!isSecond || !emitOnce) {
                for (T t : elements) {
                    ctx.collect(t);
                }
            }
            numTimesEmitted++;
        }

        synchronized (lock) {
            while (running && numCheckpointsComplete < checkpointToAwait) {
                lock.wait(1);
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        numCheckpointsComplete++;
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {}

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        Preconditions.checkState(
                this.checkpointedState != null,
                "The " + getClass().getSimpleName() + " has not been properly initialized.");

        this.checkpointedState.clear();
        this.checkpointedState.add(this.numTimesEmitted);
    }
}
