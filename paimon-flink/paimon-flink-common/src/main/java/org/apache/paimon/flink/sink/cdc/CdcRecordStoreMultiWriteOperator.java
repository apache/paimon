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
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.MultiTableCommittable;
import org.apache.paimon.flink.sink.PrepareCommitOperator;
import org.apache.paimon.flink.sink.StateUtils;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.flink.sink.StoreSinkWriteState;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A {@link PrepareCommitOperator} to write {@link CdcRecord}. Record schema may change. If current
 * known schema does not fit record schema, this operator will wait for schema changes.
 */
public class CdcRecordStoreMultiWriteOperator extends PrepareCommitOperator<MultiplexCdcRecord> {

    static final ConfigOption<Duration> RETRY_SLEEP_TIME =
            ConfigOptions.key("cdc.retry-sleep-time")
                    .durationType()
                    .defaultValue(Duration.ofMillis(500));
    private static final long serialVersionUID = 1L;
    private final Catalog catalog;
    private final StoreSinkWrite.Provider storeSinkWriteProvider;
    private final String initialCommitUser;
    private final long retrySleepMillis;
    private FileStoreTable table;
    private Map<Identifier, FileStoreTable> tables;
    private StoreSinkWriteState state;
    private StoreSinkWrite write;
    private Map<Identifier, StoreSinkWrite> writes;
    private String commitUser;

    public CdcRecordStoreMultiWriteOperator(
            Catalog catalog,
            StoreSinkWrite.Provider storeSinkWriteProvider,
            String initialCommitUser) {
        this.catalog = catalog;
        this.storeSinkWriteProvider = storeSinkWriteProvider;
        this.initialCommitUser = initialCommitUser;
        this.retrySleepMillis = RETRY_SLEEP_TIME.defaultValue().toMillis();
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        // Each job can only have one user name and this name must be consistent across restarts.
        // We cannot use job id as commit user name here because user may change job id by creating
        // a savepoint, stop the job and then resume from savepoint.
        commitUser =
                StateUtils.getSingleValueFromState(
                        context, "commit_user_state", String.class, initialCommitUser);

        state = new StoreSinkWriteState(context, (tableName, partition, bucket) -> false);
        this.tables = new HashMap<>();
        this.writes = new HashMap<>();
    }

    @Override
    public void processElement(StreamRecord<MultiplexCdcRecord> element) throws Exception {
        MultiplexCdcRecord record = element.getValue();

        String databaseName = record.getDatabaseName();
        String tableName = record.getTableName();
        Identifier key = Identifier.create(databaseName, tableName);
        table =
                tables.computeIfAbsent(
                        key,
                        id -> {
                            try {
                                return (FileStoreTable) catalog.getTable(id);
                            } catch (Catalog.TableNotExistException e) {
                                return null;
                            }
                        });

        if (table == null) {
            throw new IOException("Failed to get table " + key);
        }

        write =
                writes.computeIfAbsent(
                        key,
                        id ->
                                storeSinkWriteProvider.provide(
                                        table,
                                        commitUser,
                                        state,
                                        getContainingTask().getEnvironment().getIOManager()));

        Optional<GenericRow> optionalConverted = record.toGenericRow(table.schema().fields());
        if (!optionalConverted.isPresent()) {
            while (true) {
                table = table.copyWithLatestSchema();
                optionalConverted = record.toGenericRow(table.schema().fields());
                if (optionalConverted.isPresent()) {
                    break;
                }
                Thread.sleep(
                        table.coreOptions().toConfiguration().get(RETRY_SLEEP_TIME).toMillis());
            }
            write.replace(table);
        }

        try {
            write.write(optionalConverted.get());
        } catch (Exception e) {
            throw new IOException(e);
        }
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
    protected List<Committable> prepareCommit(boolean doCompaction, long checkpointId)
            throws IOException {
        List<Committable> committables = new LinkedList<>();
        for (Map.Entry<Identifier, StoreSinkWrite> entry : writes.entrySet()) {
            Identifier key = entry.getKey();
            StoreSinkWrite write = entry.getValue();
            committables.addAll(
                    write.prepareCommit(doCompaction, checkpointId).stream()
                            .map(
                                    committable ->
                                            MultiTableCommittable.fromCommittable(
                                                    key, commitUser, committable))
                            .collect(Collectors.toList()));
        }
        return committables;
    }
}
