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

import org.apache.paimon.append.AppendCompactTask;
import org.apache.paimon.flink.source.AppendTableCompactSource;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Operator to execute {@link AppendCompactTask} passed from {@link AppendTableCompactSource} for
 * compacting single unaware bucket tables in divided mode.
 */
public class AppendOnlySingleTableCompactionWorkerOperator
        extends AppendCompactWorkerOperator<AppendCompactTask> {

    private AppendOnlySingleTableCompactionWorkerOperator(
            StreamOperatorParameters<Committable> parameters,
            FileStoreTable table,
            String commitUser,
            boolean isStreaming) {
        super(parameters, table, commitUser, isStreaming);
    }

    @Override
    public void processElement(StreamRecord<AppendCompactTask> element) throws Exception {
        AppendCompactTask task = element.getValue();
        this.unawareBucketCompactor.tryRefreshWrite(task.compactBefore());
        this.unawareBucketCompactor.processElement(task);
    }

    /** {@link StreamOperatorFactory} of {@link AppendOnlySingleTableCompactionWorkerOperator}. */
    public static class Factory extends AppendCompactWorkerOperator.Factory<AppendCompactTask> {

        public Factory(FileStoreTable table, String initialCommitUser, boolean isStreaming) {
            super(table, initialCommitUser, isStreaming);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T extends StreamOperator<Committable>> T createStreamOperator(
                StreamOperatorParameters<Committable> parameters) {
            return (T)
                    new AppendOnlySingleTableCompactionWorkerOperator(
                            parameters, table, commitUser, isStreaming);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return AppendOnlySingleTableCompactionWorkerOperator.class;
        }
    }
}
