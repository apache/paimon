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

package org.apache.paimon.flink.sink.state;

import javax.annotation.Nonnull;

import java.util.Map;

/**
 * The state persisted by {@link org.apache.paimon.flink.sink.coordinator.WriteOperatorCoordinator}
 * during {@code checkpointCoordinator}.
 *
 * <p>Note that uncommitted committables are <b>not</b> persisted here. Writers keep their own
 * uncommitted committables in operator state and re-emit them to the coordinator on {@code
 * initializeState}; this avoids races caused by the fact that {@code
 * OperatorCoordinator.checkpointCoordinator} runs before any operator subtask has captured its own
 * snapshot.
 */
public class CoordinatorState {

    @Nonnull private final String commitUser;
    @Nonnull private final Map<String, byte[]> committerStates;

    public CoordinatorState(
            @Nonnull String commitUser, @Nonnull Map<String, byte[]> committerStates) {
        this.commitUser = commitUser;
        this.committerStates = committerStates;
    }

    @Nonnull
    public String getCommitUser() {
        return commitUser;
    }

    @Nonnull
    public Map<String, byte[]> getCommitterStates() {
        return committerStates;
    }
}
