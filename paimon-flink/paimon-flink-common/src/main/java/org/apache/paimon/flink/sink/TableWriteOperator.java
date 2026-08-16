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
import org.apache.paimon.flink.sink.StoreSinkWriteState.StateValueFilter;
import org.apache.paimon.flink.sink.coordinator.CoordinatedWriteRestore;
import org.apache.paimon.flink.sink.coordinator.WriteOperatorCoordinator;
import org.apache.paimon.flink.utils.RuntimeContextUtils;
import org.apache.paimon.operation.WriteRestore;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.ChannelComputer;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

/** An abstract class for table write operator. */
public abstract class TableWriteOperator<IN> extends PrepareCommitOperator<IN, Committable> {

    private static final long serialVersionUID = 1L;

    protected FileStoreTable table;

    protected final StoreSinkWrite.Provider storeSinkWriteProvider;
    protected final String initialCommitUser;

    protected transient @Nullable WriteRestore writeRestore;
    protected transient String commitUser;
    protected transient StoreSinkWriteState state;
    protected transient StoreSinkWrite write;

    protected transient @Nullable ConfigRefresher configRefresher;

    public TableWriteOperator(
            StreamOperatorParameters<Committable> parameters,
            FileStoreTable table,
            StoreSinkWrite.Provider storeSinkWriteProvider,
            String initialCommitUser) {
        super(parameters, Options.fromMap(table.options()));
        this.table = table;
        this.storeSinkWriteProvider = storeSinkWriteProvider;
        this.initialCommitUser = initialCommitUser;
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        int numTasks = RuntimeContextUtils.getNumberOfParallelSubtasks(getRuntimeContext());
        int subtaskId = RuntimeContextUtils.getIndexOfThisSubtask(getRuntimeContext());
        StateValueFilter stateFilter =
                (tableName, partition, bucket) ->
                        subtaskId == ChannelComputer.select(partition, bucket, numTasks);

        state = createState(subtaskId, context, stateFilter);
        write =
                storeSinkWriteProvider.provide(
                        table,
                        getCommitUser(context),
                        state,
                        getContainingTask().getEnvironment().getIOManager(),
                        memoryPoolFactory,
                        getMetricGroup());
        if (writeRestore != null) {
            write.setWriteRestore(writeRestore);
        }
        this.configRefresher = ConfigRefresher.create(write.streamingMode(), table, write::replace);
    }

    public void setWriteRestore(@Nullable WriteRestore writeRestore) {
        this.writeRestore = writeRestore;
    }

    protected StoreSinkWriteState createState(
            int subtaskId,
            StateInitializationContext context,
            StoreSinkWriteState.StateValueFilter stateFilter)
            throws Exception {
        return new StoreSinkWriteStateImpl(subtaskId, context, stateFilter);
    }

    protected String getCommitUser(StateInitializationContext context) throws Exception {
        if (commitUser == null) {
            // Each job can only have one username and this name must be consistent across restarts.
            // We cannot use job id as commit username here because user may change job id by
            // creating a savepoint, stop the job and then resume from savepoint.
            commitUser =
                    StateUtils.getSingleValueFromState(
                            context, "commit_user_state", String.class, initialCommitUser);
        }

        return commitUser;
    }

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

    protected void tryRefreshWrite() {
        if (configRefresher != null) {
            configRefresher.tryRefresh();
        }
    }

    /** {@link StreamOperatorFactory} of {@link TableWriteOperator}. */
    protected abstract static class Factory<IN>
            extends PrepareCommitOperator.Factory<IN, Committable> {

        private static final long serialVersionUID = 1L;

        protected final FileStoreTable table;
        protected final StoreSinkWrite.Provider storeSinkWriteProvider;
        protected final String initialCommitUser;

        protected Factory(
                FileStoreTable table,
                StoreSinkWrite.Provider storeSinkWriteProvider,
                String initialCommitUser) {
            super(Options.fromMap(table.options()));
            this.table = table;
            this.storeSinkWriteProvider = storeSinkWriteProvider;
            this.initialCommitUser = initialCommitUser;
        }
    }

    /** {@link StreamOperatorFactory} of {@link TableWriteOperator}. */
    protected abstract static class CoordinatedFactory<IN>
            extends PrepareCommitOperator.Factory<IN, Committable>
            implements CoordinatedOperatorFactory<Committable> {

        private static final long serialVersionUID = 1L;

        protected final FileStoreTable table;
        protected final StoreSinkWrite.Provider storeSinkWriteProvider;
        protected final String initialCommitUser;

        protected CoordinatedFactory(
                FileStoreTable table,
                StoreSinkWrite.Provider storeSinkWriteProvider,
                String initialCommitUser) {
            super(Options.fromMap(table.options()));
            this.table = table;
            this.storeSinkWriteProvider = storeSinkWriteProvider;
            this.initialCommitUser = initialCommitUser;
        }

        @Override
        public OperatorCoordinator.Provider getCoordinatorProvider(
                String operatorName, OperatorID operatorID) {
            return new WriteOperatorCoordinator.Provider(operatorID, table);
        }

        @Override
        @SuppressWarnings("unchecked")
        public final <T extends StreamOperator<Committable>> T createStreamOperator(
                StreamOperatorParameters<Committable> parameters) {
            OperatorID operatorID = parameters.getStreamConfig().getOperatorID();
            TaskOperatorEventGateway gateway =
                    parameters
                            .getContainingTask()
                            .getEnvironment()
                            .getOperatorCoordinatorEventGateway();
            TableWriteOperator<IN> operator = createStreamOperatorImpl(parameters);
            operator.setWriteRestore(new CoordinatedWriteRestore(gateway, operatorID));
            return (T) operator;
        }

        public abstract <T extends TableWriteOperator<IN>> T createStreamOperatorImpl(
                StreamOperatorParameters<Committable> parameters);
    }
}
