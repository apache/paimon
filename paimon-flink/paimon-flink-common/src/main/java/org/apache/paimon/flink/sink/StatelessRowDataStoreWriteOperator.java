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

import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

import javax.annotation.Nullable;

/** Stateless writer used for unaware append and postpone bucket table. */
public class StatelessRowDataStoreWriteOperator extends RowDataStoreWriteOperator {

    private static final long serialVersionUID = 1L;

    public StatelessRowDataStoreWriteOperator(
            StreamOperatorParameters<Committable> parameters,
            FileStoreTable table,
            @Nullable LogSinkFunction logSinkFunction,
            StoreSinkWrite.Provider storeSinkWriteProvider,
            String initialCommitUser) {
        super(parameters, table, logSinkFunction, storeSinkWriteProvider, initialCommitUser);
    }

    @Override
    protected StoreSinkWriteState createState(
            int subtaskId,
            StateInitializationContext context,
            StoreSinkWriteState.StateValueFilter stateFilter) {
        // No conflicts will occur in append only unaware bucket writer, so no state
        // is needed.
        return new NoopStoreSinkWriteState(subtaskId);
    }

    @Override
    protected String getCommitUser(StateInitializationContext context) {
        // No conflicts will occur in append only unaware bucket writer, so
        // commitUser does not matter.
        return commitUser == null ? initialCommitUser : commitUser;
    }
}
