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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.flink.ProcessRecordAttributesUtil;
import org.apache.paimon.flink.sink.StoreSinkWriteState.StateValueFilter;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.ChannelComputer;

import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;

import java.io.IOException;
import java.util.List;

/** An abstract class for table write operator. */
public abstract class TableWriteOperator<IN> extends PrepareCommitOperator<IN, Committable> {

    protected FileStoreTable table;

    private final StoreSinkWrite.Provider storeSinkWriteProvider;
    private final String initialCommitUser;

    private transient StoreSinkWriteState state;
    protected transient StoreSinkWrite write;

    public TableWriteOperator(
            FileStoreTable table,
            StoreSinkWrite.Provider storeSinkWriteProvider,
            String initialCommitUser) {
        super(Options.fromMap(table.options()));
        this.table = table;
        this.storeSinkWriteProvider = storeSinkWriteProvider;
        this.initialCommitUser = initialCommitUser;
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        // Each job can only have one user name and this name must be consistent across restarts.
        // We cannot use job id as commit user name here because user may change job id by creating
        // a savepoint, stop the job and then resume from savepoint.
        String commitUser =
                StateUtils.getSingleValueFromState(
                        context, "commit_user_state", String.class, initialCommitUser);

        boolean containLogSystem = containLogSystem();
        int numTasks = getRuntimeContext().getNumberOfParallelSubtasks();
        StateValueFilter stateFilter =
                (tableName, partition, bucket) -> {
                    int task =
                            containLogSystem
                                    ? ChannelComputer.select(bucket, numTasks)
                                    : ChannelComputer.select(partition, bucket, numTasks);
                    return task == getRuntimeContext().getIndexOfThisSubtask();
                };

        initStateAndWriter(
                context,
                stateFilter,
                getContainingTask().getEnvironment().getIOManager(),
                commitUser);
    }

    @VisibleForTesting
    void initStateAndWriter(
            StateInitializationContext context,
            StateValueFilter stateFilter,
            IOManager ioManager,
            String commitUser)
            throws Exception {
        // We put state and write init in this method for convenient testing. Without construct a
        // runtime context, we can test to construct a writer here
        state = new StoreSinkWriteState(context, stateFilter);

        write =
                storeSinkWriteProvider.provide(
                        table, commitUser, state, ioManager, memoryPool, getMetricGroup());
    }

    @Override
    public void processRecordAttributes(RecordAttributes recordAttributes) throws Exception {
        ProcessRecordAttributesUtil.processWithWrite(recordAttributes, write);
        super.processRecordAttributes(recordAttributes);
    }

    protected abstract boolean containLogSystem();

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);

        write.snapshotState();
        state.snapshotState();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (write != null) {
            write.close();
        }
    }

    @Override
    protected List<Committable> prepareCommit(boolean waitCompaction, long checkpointId)
            throws IOException {
        return write.prepareCommit(waitCompaction, checkpointId);
    }

    @VisibleForTesting
    public StoreSinkWrite getWrite() {
        return write;
    }
}
