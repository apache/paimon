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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.utils.RuntimeContextUtils;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.HashMap;
import java.util.Map;

/** {@link TableWriteOperator} for writing records in postpone bucket table. */
public class PostponeBucketTableWriteOperator extends TableWriteOperator<InternalRow> {

    private static final long serialVersionUID = 1L;

    public PostponeBucketTableWriteOperator(
            StreamOperatorParameters<Committable> parameters,
            FileStoreTable table,
            StoreSinkWrite.Provider storeSinkWriteProvider,
            String initialCommitUser) {
        super(parameters, table, storeSinkWriteProvider, initialCommitUser);
    }

    @Override
    protected boolean containLogSystem() {
        return false;
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put(
                CoreOptions.DATA_FILE_PREFIX.key(),
                String.format(
                        "%s-u-%s-s-%d-w-",
                        table.coreOptions().dataFilePrefix(),
                        getCommitUser(context),
                        RuntimeContextUtils.getIndexOfThisSubtask(getRuntimeContext())));
        table = table.copy(dynamicOptions);

        super.initializeState(context);
    }

    @Override
    public void processElement(StreamRecord<InternalRow> element) throws Exception {
        write.write(element.getValue(), BucketMode.POSTPONE_BUCKET);
    }

    /** Factory to create {@link PostponeBucketTableWriteOperator}. */
    public static class Factory extends TableWriteOperator.Factory<InternalRow> {

        protected Factory(
                FileStoreTable table,
                StoreSinkWrite.Provider storeSinkWriteProvider,
                String initialCommitUser) {
            super(table, storeSinkWriteProvider, initialCommitUser);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T extends StreamOperator<Committable>> T createStreamOperator(
                StreamOperatorParameters<Committable> parameters) {
            return (T)
                    new PostponeBucketTableWriteOperator(
                            parameters, table, storeSinkWriteProvider, initialCommitUser);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return PostponeBucketTableWriteOperator.class;
        }
    }
}
