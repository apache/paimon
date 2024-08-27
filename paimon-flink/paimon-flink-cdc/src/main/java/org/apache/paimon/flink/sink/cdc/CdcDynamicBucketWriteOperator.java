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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.sink.PrepareCommitOperator;
import org.apache.paimon.flink.sink.StateValueFilter;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.flink.sink.StoreSinkWriteWithUnionListState;
import org.apache.paimon.flink.sink.TableWriteOperator;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;
import java.util.Optional;

import static org.apache.paimon.flink.sink.cdc.CdcRecordStoreWriteOperator.RETRY_SLEEP_TIME;
import static org.apache.paimon.flink.sink.cdc.CdcRecordUtils.toGenericRow;

/**
 * A {@link PrepareCommitOperator} to write {@link CdcRecord} with bucket. Record schema is fixed.
 */
public class CdcDynamicBucketWriteOperator extends TableWriteOperator<Tuple2<CdcRecord, Integer>> {

    private static final long serialVersionUID = 1L;
    private transient StoreSinkWriteWithUnionListState state;
    private final long retrySleepMillis;

    public CdcDynamicBucketWriteOperator(
            FileStoreTable table,
            StoreSinkWrite.Provider storeSinkWriteProvider,
            String initialCommitUser) {
        super(table, storeSinkWriteProvider, initialCommitUser);
        this.retrySleepMillis =
                table.coreOptions().toConfiguration().get(RETRY_SLEEP_TIME).toMillis();
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        table = table.copyWithLatestSchema();
        super.initializeState(context);
        initStateAndWriter(
                context,
                stateFilter,
                getContainingTask().getEnvironment().getIOManager(),
                commitUser);
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

    @Override
    protected boolean containLogSystem() {
        return false;
    }

    @Override
    public void processElement(StreamRecord<Tuple2<CdcRecord, Integer>> element) throws Exception {
        Tuple2<CdcRecord, Integer> record = element.getValue();
        Optional<GenericRow> optionalConverted = toGenericRow(record.f0, table.schema().fields());
        if (!optionalConverted.isPresent()) {
            while (true) {
                table = table.copyWithLatestSchema();
                optionalConverted = toGenericRow(record.f0, table.schema().fields());
                if (optionalConverted.isPresent()) {
                    break;
                }
                Thread.sleep(retrySleepMillis);
            }
            write.replace(table);
        }

        try {
            write.write(optionalConverted.get(), record.f1);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
