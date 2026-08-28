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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.sink.MultiTableCommittable;
import org.apache.paimon.flink.sink.PrepareCommitOperator;
import org.apache.paimon.flink.sink.StateUtils;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.flink.sink.StoreSinkWriteImpl;
import org.apache.paimon.flink.sink.StoreSinkWriteState;
import org.apache.paimon.flink.sink.StoreSinkWriteStateImpl;
import org.apache.paimon.flink.utils.RuntimeContextUtils;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.ExecutorThreadFactory;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.sink.cdc.CdcRecordStoreWriteOperator.LOG_CORRUPT_RECORD;
import static org.apache.paimon.flink.sink.cdc.CdcRecordStoreWriteOperator.MAX_RETRY_NUM_TIMES;
import static org.apache.paimon.flink.sink.cdc.CdcRecordStoreWriteOperator.RETRY_SLEEP_TIME;
import static org.apache.paimon.flink.sink.cdc.CdcRecordStoreWriteOperator.SKIP_CORRUPT_RECORD;
import static org.apache.paimon.flink.sink.cdc.CdcRecordUtils.toGenericRow;

/**
 * A {@link PrepareCommitOperator} to write {@link CdcRecord}. Record schema may change. If current
 * known schema does not fit record schema, this operator will wait for schema changes.
 *
 * <p>When {@code stateDatabaseName} is given, this operator assumes its input is partitioned by
 * {@link CdcMultiplexRecordChannelComputer} and that every incoming record belongs to that
 * database. {@link FlinkCdcMultiTableSink} guarantees both by applying the partitioner itself. The
 * {@link StoreSinkWriteState} filter below then distributes state values among subtasks with
 * exactly the same formula as the channel computer, so a record and the state of the bucket it
 * belongs to always end up in the same subtask.
 *
 * <p>The compatibility constructor does not have a database name and therefore keeps the legacy
 * behavior of restoring all union state values into every subtask. It does not guarantee unique
 * bucket-state ownership after a restore.
 */
public class CdcRecordStoreMultiWriteOperator
        extends PrepareCommitOperator<CdcMultiplexRecord, MultiTableCommittable> {

    private static final long serialVersionUID = 1L;

    private final StoreSinkWrite.Provider storeSinkWriteProvider;
    private final String initialCommitUser;
    private final CatalogLoader catalogLoader;
    @Nullable private final String stateDatabaseName;

    private Catalog catalog;
    private Map<Identifier, FileStoreTable> tables;
    private StoreSinkWriteState state;
    private Map<Identifier, StoreSinkWrite> writes;
    private String commitUser;
    private ExecutorService compactExecutor;

    private CdcRecordStoreMultiWriteOperator(
            StreamOperatorParameters<MultiTableCommittable> parameters,
            CatalogLoader catalogLoader,
            StoreSinkWrite.Provider storeSinkWriteProvider,
            String initialCommitUser,
            @Nullable String stateDatabaseName,
            Options options) {
        super(parameters, options);
        this.catalogLoader = catalogLoader;
        this.storeSinkWriteProvider = storeSinkWriteProvider;
        this.initialCommitUser = initialCommitUser;
        this.stateDatabaseName = stateDatabaseName;
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

        int numTasks = RuntimeContextUtils.getNumberOfParallelSubtasks(getRuntimeContext());
        int subtaskId = RuntimeContextUtils.getIndexOfThisSubtask(getRuntimeContext());
        StoreSinkWriteState.StateValueFilter stateFilter;
        if (stateDatabaseName == null) {
            // Preserve the behavior of the old constructor for compatibility. Without a database
            // name, every subtask restores all union state and bucket ownership is not guaranteed.
            stateFilter = (tableName, partition, bucket) -> true;
        } else {
            // Keep this filter in sync with CdcMultiplexRecordChannelComputer, which partitions the
            // input of this operator. Otherwise state values would be restored into a subtask which
            // never writes the corresponding bucket.
            stateFilter =
                    (tableName, partition, bucket) ->
                            subtaskId
                                    == CdcMultiplexRecordChannelComputer.computeChannel(
                                            stateDatabaseName,
                                            tableName,
                                            partition,
                                            bucket,
                                            numTasks);
        }
        state = new StoreSinkWriteStateImpl(subtaskId, context, stateFilter);
        tables = new HashMap<>();
        writes = new HashMap<>();
        compactExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory(
                                Thread.currentThread().getName() + "-CdcMultiWrite-Compaction"));
    }

    @Override
    public void processElement(StreamRecord<CdcMultiplexRecord> element) throws Exception {
        CdcMultiplexRecord record = element.getValue();

        String databaseName = record.databaseName();
        Preconditions.checkArgument(
                stateDatabaseName == null || stateDatabaseName.equals(databaseName),
                "This writer only accepts records from database %s, but received a record from %s.",
                stateDatabaseName,
                databaseName);
        String tableName = record.tableName();
        Identifier tableId = Identifier.create(databaseName, tableName);

        FileStoreTable table = getTable(tableId);

        int retryCnt = table.coreOptions().toConfiguration().get(MAX_RETRY_NUM_TIMES);
        boolean skipCorruptRecord = table.coreOptions().toConfiguration().get(SKIP_CORRUPT_RECORD);

        StoreSinkWrite write =
                writes.computeIfAbsent(
                        tableId,
                        id ->
                                storeSinkWriteProvider.provide(
                                        table,
                                        commitUser,
                                        state,
                                        getContainingTask().getEnvironment().getIOManager(),
                                        memoryPoolFactory,
                                        getMetricGroup()));

        ((StoreSinkWriteImpl) write).withCompactExecutor(compactExecutor);

        boolean logCorruptRecord = table.coreOptions().toConfiguration().get(LOG_CORRUPT_RECORD);
        Optional<GenericRow> optionalConverted =
                toGenericRow(record.record(), table.schema().fields(), logCorruptRecord);
        if (!optionalConverted.isPresent()) {
            FileStoreTable latestTable = table;
            for (int retry = 0; retry < retryCnt; ++retry) {
                latestTable = latestTable.copyWithLatestSchema();
                tables.put(tableId, latestTable);
                optionalConverted =
                        toGenericRow(
                                record.record(), latestTable.schema().fields(), logCorruptRecord);
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

        if (!optionalConverted.isPresent()) {
            if (skipCorruptRecord) {
                LOG.warn(
                        "Skipping corrupt or unparsable record {}",
                        (logCorruptRecord ? record : "<redacted>"));
            } else {
                throw new RuntimeException(
                        "Unable to process element. Possibly a corrupt record: "
                                + (logCorruptRecord ? record : "<redacted>"));
            }
        } else {
            try {
                write.write(optionalConverted.get());
            } catch (Exception e) {
                throw new IOException(e);
            }
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

        if (table.bucketMode() != BucketMode.HASH_FIXED) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Combine mode Sink only supports FIXED bucket mode, but %s is %s",
                            table.name(), table.bucketMode()));
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
        // initializeState may have failed before these were assigned, and Flink still closes the
        // operator. Do not mask the original failure with a NullPointerException.
        if (writes != null) {
            for (StoreSinkWrite write : writes.values()) {
                write.close();
            }
        }
        if (compactExecutor != null) {
            compactExecutor.shutdownNow();
        }
        if (catalog != null) {
            catalog.close();
            catalog = null;
        }
    }

    @Override
    protected List<MultiTableCommittable> prepareCommit(boolean waitCompaction, long checkpointId)
            throws IOException {
        List<MultiTableCommittable> committables = new LinkedList<>();
        for (Map.Entry<Identifier, StoreSinkWrite> entry : writes.entrySet()) {
            Identifier key = entry.getKey();
            StoreSinkWrite write = entry.getValue();
            try {
                committables.addAll(
                        write.prepareCommit(waitCompaction, checkpointId).stream()
                                .map(
                                        committable ->
                                                MultiTableCommittable.fromCommittable(
                                                        key, committable))
                                .collect(Collectors.toList()));
            } catch (Exception e) {
                throw new IOException("Failed to prepare commit for table: " + key.toString(), e);
            }
        }
        return committables;
    }

    @VisibleForTesting
    public Map<Identifier, FileStoreTable> tables() {
        return tables;
    }

    @VisibleForTesting
    public Map<Identifier, StoreSinkWrite> writes() {
        return writes;
    }

    @VisibleForTesting
    public String commitUser() {
        return commitUser;
    }

    @VisibleForTesting
    public StoreSinkWriteState state() {
        return state;
    }

    /** {@link StreamOperatorFactory} of {@link CdcRecordStoreMultiWriteOperator}. */
    public static class Factory
            extends PrepareCommitOperator.Factory<CdcMultiplexRecord, MultiTableCommittable> {
        private final StoreSinkWrite.Provider storeSinkWriteProvider;
        private final String initialCommitUser;
        private final CatalogLoader catalogLoader;
        @Nullable private final String stateDatabaseName;

        /**
         * @deprecated Use {@link #Factory(CatalogLoader, StoreSinkWrite.Provider, String, String,
         *     Options)} instead. Without a database name, every subtask restores all union writer
         *     state and unique bucket-state ownership is not guaranteed.
         */
        @Deprecated
        public Factory(
                CatalogLoader catalogLoader,
                StoreSinkWrite.Provider storeSinkWriteProvider,
                String initialCommitUser,
                Options options) {
            super(options);
            this.catalogLoader = catalogLoader;
            this.storeSinkWriteProvider = storeSinkWriteProvider;
            this.initialCommitUser = initialCommitUser;
            this.stateDatabaseName = null;
        }

        public Factory(
                CatalogLoader catalogLoader,
                StoreSinkWrite.Provider storeSinkWriteProvider,
                String initialCommitUser,
                String stateDatabaseName,
                Options options) {
            super(options);
            this.catalogLoader = catalogLoader;
            this.storeSinkWriteProvider = storeSinkWriteProvider;
            this.initialCommitUser = initialCommitUser;
            this.stateDatabaseName = Preconditions.checkNotNull(stateDatabaseName);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T extends StreamOperator<MultiTableCommittable>> T createStreamOperator(
                StreamOperatorParameters<MultiTableCommittable> parameters) {
            return (T)
                    new CdcRecordStoreMultiWriteOperator(
                            parameters,
                            catalogLoader,
                            storeSinkWriteProvider,
                            initialCommitUser,
                            stateDatabaseName,
                            options);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return CdcRecordStoreMultiWriteOperator.class;
        }
    }
}
