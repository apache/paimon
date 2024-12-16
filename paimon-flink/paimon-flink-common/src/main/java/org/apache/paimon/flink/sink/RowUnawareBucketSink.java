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
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

import java.util.Map;

/** An {@link UnawareBucketSink} which handles {@link InternalRow}. */
public class RowUnawareBucketSink extends UnawareBucketSink<InternalRow> {

    public RowUnawareBucketSink(
            FileStoreTable table,
            Map<String, String> overwritePartitions,
            LogSinkFunction logSinkFunction,
            Integer parallelism) {
        super(table, overwritePartitions, logSinkFunction, parallelism);
    }

    @Override
    protected OneInputStreamOperatorFactory<InternalRow, Committable> createWriteOperatorFactory(
            StoreSinkWrite.Provider writeProvider, String commitUser) {
        return new RowDataStoreWriteOperator.Factory(
                table, logSinkFunction, writeProvider, commitUser) {
            @Override
            public StreamOperator createStreamOperator(StreamOperatorParameters parameters) {
                return new RowDataStoreWriteOperator(
                        parameters, table, logSinkFunction, writeProvider, commitUser) {

                    @Override
                    protected StoreSinkWriteState createState(
                            StateInitializationContext context,
                            StoreSinkWriteState.StateValueFilter stateFilter)
                            throws Exception {
                        // No conflicts will occur in append only unaware bucket writer, so no state
                        // is
                        // needed.
                        return new NoopStoreSinkWriteState(stateFilter);
                    }

                    @Override
                    protected String getCommitUser(StateInitializationContext context)
                            throws Exception {
                        // No conflicts will occur in append only unaware bucket writer, so
                        // commitUser does
                        // not matter.
                        return commitUser;
                    }
                };
            }
        };
    }
}
