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

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;
import java.util.List;

/** Prepare commit operator to emit {@link Committable}s. */
public abstract class PrepareCommitOperator<T> extends AbstractStreamOperator<Committable>
        implements OneInputStreamOperator<T, Committable>, BoundedOneInput {

    private boolean endOfInput = false;

    public PrepareCommitOperator() {
        setChainingStrategy(ChainingStrategy.ALWAYS);
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

    private void emitCommittables(boolean doCompaction, long checkpointId) throws IOException {
        prepareCommit(doCompaction, checkpointId)
                .forEach(committable -> output.collect(new StreamRecord<>(committable)));
    }

    protected abstract List<Committable> prepareCommit(boolean doCompaction, long checkpointId)
            throws IOException;
}
