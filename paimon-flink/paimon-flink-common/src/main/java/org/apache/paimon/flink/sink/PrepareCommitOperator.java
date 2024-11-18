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

package org.apache.paimon.flink.sink;

import org.apache.paimon.flink.memory.FlinkMemorySegmentPool;
import org.apache.paimon.flink.memory.MemorySegmentAllocator;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.options.Options;

import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_USE_MANAGED_MEMORY;
import static org.apache.paimon.flink.utils.ManagedMemoryUtils.computeManagedMemory;

/** Prepare commit operator to emit {@link Committable}s. */
public abstract class PrepareCommitOperator<IN, OUT> extends AbstractStreamOperator<OUT>
        implements OneInputStreamOperator<IN, OUT>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    @Nullable protected transient MemorySegmentPool memoryPool;
    @Nullable private transient MemorySegmentAllocator memoryAllocator;
    private final Options options;
    private boolean endOfInput = false;

    public PrepareCommitOperator(Options options) {
        this.options = options;
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<OUT>> output) {
        super.setup(containingTask, config, output);
        if (options.get(SINK_USE_MANAGED_MEMORY)) {
            MemoryManager memoryManager = containingTask.getEnvironment().getMemoryManager();
            memoryAllocator = new MemorySegmentAllocator(containingTask, memoryManager);
            memoryPool =
                    new FlinkMemorySegmentPool(
                            computeManagedMemory(this),
                            memoryManager.getPageSize(),
                            memoryAllocator);
        }
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        if (!endOfInput) {
            emitCommittables(false, checkpointId);
        }
        // no records are expected to emit after endOfInput
    }

    @Override
    public void endInput() throws Exception {
        endOfInput = true;
        emitCommittables(true, Long.MAX_VALUE);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (memoryAllocator != null) {
            memoryAllocator.release();
        }
    }

    private void emitCommittables(boolean waitCompaction, long checkpointId) throws IOException {
        prepareCommit(waitCompaction, checkpointId)
                .forEach(committable -> output.collect(new StreamRecord<>(committable)));
    }

    protected abstract List<OUT> prepareCommit(boolean waitCompaction, long checkpointId)
            throws IOException;
}
