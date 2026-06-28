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

import org.apache.paimon.flink.sink.coordinator.PaimonWriterCoordinator;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

/** Factory that installs a JM-side committer coordinator for writer operators. */
public class CommitterCoordinatedFactory<CommitT, GlobalCommitT>
        extends PrepareCommitOperator.Factory<CommitT, Committable>
        implements CoordinatedOperatorFactory<Committable> {

    private static final long serialVersionUID = 1L;

    private final boolean streamingCheckpointEnabled;
    private final TableWriteOperator.Factory<CommitT> writeFactory;
    private final Committer.Factory<Committable, GlobalCommitT> committerFactory;
    private final String initialCommitUser;
    private final Long endInputWatermark;

    public CommitterCoordinatedFactory(
            boolean streamingCheckpointEnabled,
            TableWriteOperator.Factory<CommitT> writeFactory,
            Committer.Factory<Committable, GlobalCommitT> committerFactory,
            String initialCommitUser,
            Long endInputWatermark) {
        super(writeFactory.options);
        this.streamingCheckpointEnabled = streamingCheckpointEnabled;
        this.writeFactory = writeFactory;
        this.committerFactory = committerFactory;
        this.initialCommitUser = initialCommitUser;
        this.endInputWatermark = endInputWatermark;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<Committable>> T createStreamOperator(
            StreamOperatorParameters<Committable> parameters) {
        OperatorID operatorId = parameters.getStreamConfig().getOperatorID();
        TaskOperatorEventGateway gateway =
                parameters
                        .getContainingTask()
                        .getEnvironment()
                        .getOperatorCoordinatorEventGateway();
        TableWriteOperator<CommitT> operator = writeFactory.createStreamOperator(parameters);
        operator.setCommitHandler(new CoordinatedCommitHandler(gateway, operatorId));
        parameters.getOperatorEventDispatcher().registerEventHandler(operatorId, operator);
        return (T) operator;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return writeFactory.getStreamOperatorClass(classLoader);
    }

    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(
            String operatorName, OperatorID operatorID) {
        return new PaimonWriterCoordinator.WriterCoordinatorProvider(
                streamingCheckpointEnabled,
                operatorName,
                operatorID,
                initialCommitUser,
                committerFactory,
                endInputWatermark);
    }
}
