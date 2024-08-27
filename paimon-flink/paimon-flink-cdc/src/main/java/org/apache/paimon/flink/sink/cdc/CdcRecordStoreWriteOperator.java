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

import org.apache.paimon.flink.sink.PrepareCommitOperator;
import org.apache.paimon.flink.sink.StateValueFilter;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.flink.sink.StoreSinkWriteWithUnionListState;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;

/**
 * A {@link PrepareCommitOperator} to write {@link CdcRecord} with StoreSinkWriteState. Record
 * schema may change. If current known schema does not fit record schema, this operator will wait
 * for schema changes.
 */
public class CdcRecordStoreWriteOperator extends AbstractCdcRecordStoreWriteOperator {

    private static final long serialVersionUID = 1L;
    private transient StoreSinkWriteWithUnionListState state;

    public CdcRecordStoreWriteOperator(
            FileStoreTable table,
            StoreSinkWrite.Provider storeSinkWriteProvider,
            String initialCommitUser) {
        super(table, storeSinkWriteProvider, initialCommitUser);
    }

    @Override
    protected void initStateAndWriter(
            StateInitializationContext context,
            StateValueFilter stateFilter,
            IOManager ioManager,
            String commitUser)
            throws Exception {
        state = new StoreSinkWriteWithUnionListState(context, stateFilter);
        write =
                storeSinkWriteProvider.provide(
                        table, commitUser, state, ioManager, memoryPool, getMetricGroup());
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        state.snapshotState();
    }
}
