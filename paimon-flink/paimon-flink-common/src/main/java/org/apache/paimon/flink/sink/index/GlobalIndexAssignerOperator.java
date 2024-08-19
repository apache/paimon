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

package org.apache.paimon.flink.sink.index;

import org.apache.paimon.crosspartition.GlobalIndexAssigner;
import org.apache.paimon.crosspartition.KeyPartOrRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.table.Table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import static org.apache.paimon.flink.utils.ManagedMemoryUtils.computeManagedMemory;

/** A {@link OneInputStreamOperator} for {@link GlobalIndexAssigner}. */
public class GlobalIndexAssignerOperator
        extends AbstractStreamOperator<Tuple2<InternalRow, Integer>>
        implements OneInputStreamOperator<
                        Tuple2<KeyPartOrRow, InternalRow>, Tuple2<InternalRow, Integer>>,
                BoundedOneInput {

    private static final long serialVersionUID = 1L;

    private final GlobalIndexAssigner assigner;

    private transient IOManager ioManager;

    public GlobalIndexAssignerOperator(GlobalIndexAssigner assigner) {
        this.assigner = assigner;
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        org.apache.flink.runtime.io.disk.iomanager.IOManager flinkIoManager =
                getContainingTask().getEnvironment().getIOManager();
        ioManager = IOManager.create(flinkIoManager.getSpillingDirectoriesPaths());
        assigner.open(
                computeManagedMemory(this),
                ioManager,
                getRuntimeContext().getNumberOfParallelSubtasks(),
                getRuntimeContext().getIndexOfThisSubtask(),
                this::collect);
    }

    @Override
    public void processElement(StreamRecord<Tuple2<KeyPartOrRow, InternalRow>> streamRecord)
            throws Exception {
        Tuple2<KeyPartOrRow, InternalRow> tuple2 = streamRecord.getValue();
        InternalRow value = tuple2.f1;
        switch (tuple2.f0) {
            case KEY_PART:
                assigner.bootstrapKey(value);
                break;
            case ROW:
                assigner.processInput(value);
                break;
        }
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        endBootstrap(false);
    }

    @Override
    public void endInput() throws Exception {
        endBootstrap(true);
    }

    private void endBootstrap(boolean isEndInput) throws Exception {
        if (assigner.inBoostrap()) {
            assigner.endBoostrap(isEndInput);
        }
    }

    private void collect(InternalRow value, int bucket) {
        output.collect(new StreamRecord<>(new Tuple2<>(value, bucket)));
    }

    @Override
    public void close() throws Exception {
        this.assigner.close();
        if (ioManager != null) {
            ioManager.close();
        }
    }

    public static GlobalIndexAssignerOperator forRowData(Table table) {
        return new GlobalIndexAssignerOperator(new GlobalIndexAssigner(table));
    }
}
