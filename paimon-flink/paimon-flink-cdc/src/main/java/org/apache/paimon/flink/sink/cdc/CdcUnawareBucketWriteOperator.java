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

package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.PrepareCommitOperator;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowKind;

import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/** A {@link PrepareCommitOperator} to write {@link CdcRecord} to unaware-bucket mode table. */
public class CdcUnawareBucketWriteOperator extends CdcRecordStoreWriteOperator {

    private CdcUnawareBucketWriteOperator(
            StreamOperatorParameters<Committable> parameters,
            FileStoreTable table,
            StoreSinkWrite.Provider storeSinkWriteProvider,
            String initialCommitUser) {
        super(parameters, table, storeSinkWriteProvider, initialCommitUser);
    }

    @Override
    public void processElement(StreamRecord<CdcRecord> element) throws Exception {
        // only accepts INSERT record
        if (element.getValue().kind() == RowKind.INSERT) {
            super.processElement(element);
        }
    }

    /** {@link StreamOperatorFactory} of {@link CdcUnawareBucketWriteOperator}. */
    public static class Factory extends CdcRecordStoreWriteOperator.Factory {

        public Factory(
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
                    new CdcUnawareBucketWriteOperator(
                            parameters, table, storeSinkWriteProvider, initialCommitUser);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return CdcUnawareBucketWriteOperator.class;
        }
    }
}
