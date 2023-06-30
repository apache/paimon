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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.sink.MultiTableCommittable;
import org.apache.paimon.flink.sink.PrepareCommitOperator;
import org.apache.paimon.flink.sink.StateUtils;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.flink.sink.StoreSinkWriteState;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.sink.cdc.CdcRecordStoreWriteOperator.RETRY_SLEEP_TIME;
import static org.apache.paimon.flink.sink.cdc.CdcRecordUtils.toGenericRow;

/**
 * A {@link PrepareCommitOperator} to write {@link CdcRecord}. Record schema may change. If current
 * known schema does not fit record schema, this operator will wait for schema changes.
 */
public class CdcRecordStoreMultiWriteOperator
        extends PrepareCommitOperator<CdcMultiplexRecord, MultiTableCommittable> {

    private static final long serialVersionUID = 1L;

    private final StoreSinkWrite.Provider storeSinkWriteProvider;
    private final String initialCommitUser;
    private final Catalog.Loader catalogLoader;

    protected Catalog catalog;
    protected Map<Identifier, FileStoreTable> tables;
    protected StoreSinkWriteState state;
    protected Map<Identifier, StoreSinkWrite> writes;
    protected String commitUser;

    public CdcRecordStoreMultiWriteOperator(
            Catalog.Loader catalogLoader,
            StoreSinkWrite.Provider storeSinkWriteProvider,
            String initialCommitUser,
            Options options) {
        super(options);
        this.catalogLoader = catalogLoader;
        this.storeSinkWriteProvider = storeSinkWriteProvider;
        this.initialCommitUser = initialCommitUser;
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        catalog = catalogLoader.load();

        // Each job can only have one user name and this name must be consistent across restarts.
        // We cannot use job id as commit user name here because user may change job id by creating
        // a savepoint, stop the job and then resume from savepoint.
        commitUser =
                StateUtils.getSingleValueFromState(
                        context, "commit_user_state", String.class, initialCommitUser);

        // TODO: should use CdcRecordMultiChannelComputer to filter
        state = new StoreSinkWriteState(context, (tableName, partition, bucket) -> true);
        tables = new HashMap<>();
        writes = new HashMap<>();
    }

    @Override
    public void processElement(StreamRecord<CdcMultiplexRecord> element) throws Exception {
        CdcMultiplexRecord record = element.getValue();

        String databaseName = record.databaseName();
        String tableName = record.tableName();
        Identifier tableId = Identifier.create(databaseName, tableName);

        FileStoreTable table = getTable(tableId);

        // TODO memoryPool should not be null
        // TODO set executor service to write
        StoreSinkWrite write =
                writes.computeIfAbsent(
                        tableId,
                        id ->
                                storeSinkWriteProvider.provide(
                                        table,
                                        commitUser,
                                        state,
                                        getContainingTask().getEnvironment().getIOManager(),
                                        memoryPool));

        Optional<GenericRow> optionalConverted =
                toGenericRow(record.record(), table.schema().fields());
        if (!optionalConverted.isPresent()) {
            FileStoreTable latestTable = table;
            while (true) {
                latestTable = latestTable.copyWithLatestSchema();
                tables.put(tableId, latestTable);
                optionalConverted = toGenericRow(record.record(), latestTable.schema().fields());
                if (optionalConverted.isPresent()) {
                    break;
                }
                Thread.sleep(
                        latestTable
                                .coreOptions()
                                .toConfiguration()
                                .get(RETRY_SLEEP_TIME)
                                .toMillis());
            }
            write.replace(latestTable);
        }

        try {
            write.write(optionalConverted.get());
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private FileStoreTable getTable(Identifier tableId) throws InterruptedException {
        FileStoreTable table = tables.get(tableId);
        if (table == null) {
            while (true) {
                try {
                    table = (FileStoreTable) catalog.getTable(tableId);
                    tables.put(tableId, table);
                    break;
                } catch (Catalog.TableNotExistException e) {
                    // table not found, waiting until table is created by
                    //     upstream operators
                }
                Thread.sleep(RETRY_SLEEP_TIME.defaultValue().toMillis());
            }
        }
        return table;
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);

        for (StoreSinkWrite write : writes.values()) {
            write.snapshotState();
        }
        state.snapshotState();
    }

    @Override
    public void close() throws Exception {
        super.close();
        for (StoreSinkWrite write : writes.values()) {
            write.close();
        }
    }

    @Override
    protected List<MultiTableCommittable> prepareCommit(boolean doCompaction, long checkpointId)
            throws IOException {
        List<MultiTableCommittable> committables = new LinkedList<>();
        for (Map.Entry<Identifier, StoreSinkWrite> entry : writes.entrySet()) {
            Identifier key = entry.getKey();
            StoreSinkWrite write = entry.getValue();
            committables.addAll(
                    write.prepareCommit(doCompaction, checkpointId).stream()
                            .map(
                                    committable ->
                                            MultiTableCommittable.fromCommittable(key, committable))
                            .collect(Collectors.toList()));
        }
        return committables;
    }
}
