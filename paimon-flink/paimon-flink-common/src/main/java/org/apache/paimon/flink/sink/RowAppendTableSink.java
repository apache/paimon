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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.sink.coordinator.CommittingWriteOperatorCoordinator;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

import java.util.Map;

/** An {@link AppendTableSink} which handles {@link InternalRow}. */
public class RowAppendTableSink extends AppendTableSink<InternalRow> {

    private static final long serialVersionUID = 1L;

    public RowAppendTableSink(
            FileStoreTable table, Map<String, String> overwritePartitions, Integer parallelism) {
        super(table, overwritePartitions, parallelism);
    }

    @Override
    protected OneInputStreamOperatorFactory<InternalRow, Committable> createWriteOperatorFactory(
            StoreSinkWrite.Provider writeProvider, String commitUser) {
        return createWriteOperatorFactory(writeProvider, commitUser, true);
    }

    @Override
    protected OneInputStreamOperatorFactory<InternalRow, Committable> createWriteOperatorFactory(
            StoreSinkWrite.Provider writeProvider,
            String commitUser,
            boolean streamingCheckpointEnabled) {
        if (coordinatorCommitEnabled()) {
            return createCoordinatorCommittingRowWriteOperatorFactory(
                    table,
                    writeProvider,
                    commitUser,
                    streamingCheckpointEnabled,
                    true,
                    createCommitterFactory());
        }
        return createNoStateRowWriteOperatorFactory(table, writeProvider, commitUser);
    }

    @Override
    protected boolean coordinatorCommitEnabled() {
        return new Options(table.options())
                .get(FlinkConnectorOptions.SINK_COORDINATOR_COMMIT_ENABLED);
    }

    @Override
    protected CommittableStateManager<ManifestCommittable> createCommittableStateManager() {
        return createRestoreOnlyCommittableStateManager(table);
    }

    /**
     * Creates a writer factory whose committer runs in a JM-side {@link
     * CommittingWriteOperatorCoordinator} instead of a downstream global committer.
     */
    private static OneInputStreamOperatorFactory<InternalRow, Committable>
            createCoordinatorCommittingRowWriteOperatorFactory(
                    FileStoreTable table,
                    StoreSinkWrite.Provider writeProvider,
                    String commitUser,
                    boolean streamingCheckpointEnabled,
                    boolean failoverAfterRecovery,
                    Committer.Factory<Committable, ManifestCommittable> committerFactory) {
        return new CoordinatorCommittingFactory(
                table,
                writeProvider,
                commitUser,
                streamingCheckpointEnabled,
                failoverAfterRecovery,
                committerFactory);
    }

    private static class CoordinatorCommittingFactory extends RowDataStoreWriteOperator.Factory
            implements CoordinatedOperatorFactory<Committable> {

        private static final long serialVersionUID = 1L;

        private final boolean streamingCheckpointEnabled;
        private final boolean failoverAfterRecovery;
        private final Committer.Factory<Committable, ManifestCommittable> committerFactory;

        CoordinatorCommittingFactory(
                FileStoreTable table,
                StoreSinkWrite.Provider storeSinkWriteProvider,
                String initialCommitUser,
                boolean streamingCheckpointEnabled,
                boolean failoverAfterRecovery,
                Committer.Factory<Committable, ManifestCommittable> committerFactory) {
            super(table, storeSinkWriteProvider, initialCommitUser);
            this.streamingCheckpointEnabled = streamingCheckpointEnabled;
            this.failoverAfterRecovery = failoverAfterRecovery;
            this.committerFactory = committerFactory;
        }

        @Override
        public OperatorCoordinator.Provider getCoordinatorProvider(
                String operatorName, OperatorID operatorID) {
            return new CommittingWriteOperatorCoordinator.Provider(
                    operatorID,
                    committerFactory,
                    streamingCheckpointEnabled,
                    initialCommitUser,
                    failoverAfterRecovery);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T extends StreamOperator<Committable>> T createStreamOperator(
                StreamOperatorParameters<Committable> parameters) {
            OperatorID operatorId = parameters.getStreamConfig().getOperatorID();
            OperatorEventGateway gateway =
                    parameters.getOperatorEventDispatcher().getOperatorEventGateway(operatorId);
            return (T)
                    new CoordinatorCommittingRowDataStoreWriteOperator(
                            parameters, table, storeSinkWriteProvider, initialCommitUser, gateway);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return CoordinatorCommittingRowDataStoreWriteOperator.class;
        }
    }
}
