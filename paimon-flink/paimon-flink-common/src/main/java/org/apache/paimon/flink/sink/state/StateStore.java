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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;

/**
 * Abstraction for accessing list state from a {@link
 * org.apache.paimon.flink.sink.Committer.Context}.
 *
 * <p>Decouples committer / commit-listener state access from Flink's {@code OperatorStateStore} so
 * that the same committer code can run in both a stream operator and a job-manager-side {@code
 * OperatorCoordinator}.
 */
public interface StateStore {

    /**
     * Returns a {@link ListState} for the given descriptor. Implementations should follow the same
     * "operator state" semantics: the returned state is local to the current execution component
     * (subtask or coordinator) and is checkpointed together with that component.
     */
    <T> ListState<T> getListState(ListStateDescriptor<T> descriptor) throws Exception;
}
