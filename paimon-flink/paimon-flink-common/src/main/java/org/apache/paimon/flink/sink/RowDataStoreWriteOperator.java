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
import org.apache.paimon.table.sink.SinkRecord;

import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

/** A {@link PrepareCommitOperator} to write {@link InternalRow}. Record schema is fixed. */
public class RowDataStoreWriteOperator extends TableWriteOperator<InternalRow> {

    private static final long serialVersionUID = 3L;

    protected RowDataStoreWriteOperator(
            StreamOperatorParameters<Committable> parameters,
            FileStoreTable table,
            StoreSinkWrite.Provider storeSinkWriteProvider,
            String initialCommitUser) {
        super(parameters, table, storeSinkWriteProvider, initialCommitUser);
    }

    @Override
    public void open() throws Exception {
        super.open();
    }

    @Override
    public void processElement(StreamRecord<InternalRow> element) throws Exception {
        write(element.getValue());
    }

    @Nullable
    protected SinkRecord write(InternalRow row) throws Exception {
        try {
            return write.write(row);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    protected List<Committable> prepareCommit(boolean waitCompaction, long checkpointId)
            throws IOException {
        List<Committable> committables = super.prepareCommit(waitCompaction, checkpointId);
        tryRefreshWrite();
        return committables;
    }

    /** {@link StreamOperatorFactory} of {@link RowDataStoreWriteOperator}. */
    public static class Factory extends TableWriteOperator.Factory<InternalRow> {

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
                    new RowDataStoreWriteOperator(
                            parameters, table, storeSinkWriteProvider, initialCommitUser);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return RowDataStoreWriteOperator.class;
        }
    }

    /** {@link CoordinatedFactory} of {@link RowDataStoreWriteOperator}. */
    public static class CoordinatedFactory
            extends TableWriteOperator.CoordinatedFactory<InternalRow> {

        public CoordinatedFactory(
                FileStoreTable table,
                StoreSinkWrite.Provider storeSinkWriteProvider,
                String initialCommitUser) {
            super(table, storeSinkWriteProvider, initialCommitUser);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T extends TableWriteOperator<InternalRow>> T createStreamOperatorImpl(
                StreamOperatorParameters<Committable> parameters) {
            return (T)
                    new RowDataStoreWriteOperator(
                            parameters, table, storeSinkWriteProvider, initialCommitUser);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return RowDataStoreWriteOperator.class;
        }
    }
}
