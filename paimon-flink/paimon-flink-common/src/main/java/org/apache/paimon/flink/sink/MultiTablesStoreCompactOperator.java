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

import static org.apache.paimon.CoreOptions.FULL_COMPACTION_DELTA_COMMITS;
import static org.apache.paimon.flink.FlinkConnectorOptions.CHANGELOG_PRODUCER_FULL_COMPACTION_TRIGGER_INTERVAL;
import static org.apache.paimon.flink.FlinkConnectorOptions.CHANGELOG_PRODUCER_LOOKUP_WAIT;
import static org.apache.paimon.flink.sink.cdc.CdcRecordStoreWriteOperator.RETRY_SLEEP_TIME;
import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;

import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** this is a doc. */
public class MultiTablesStoreCompactOperator
        extends PrepareCommitOperator<RowData, MultiTableCommittable> {

    private static final long serialVersionUID = 1L;

    private StoreSinkWrite.Provider storeSinkWriteProvider;
    private CheckpointConfig checkpointConfig;
    private boolean isStreaming;
    private boolean ignorePreviousFiles;
    private final String initialCommitUser;

    private transient StoreSinkWriteState state;
    private transient DataFileMetaSerializer dataFileMetaSerializer;

    private final Catalog.Loader catalogLoader;

    protected Catalog catalog;
    protected Map<Identifier, FileStoreTable> tables;
    protected Map<Identifier, StoreSinkWrite> writes;
    protected String commitUser;

    public MultiTablesStoreCompactOperator(
            Catalog.Loader catalogLoader,
            String initialCommitUser,
            CheckpointConfig checkpointConfig,
            boolean isStreaming,
            boolean ignorePreviousFiles,
            Options options) {
        super(options);
        // should guarantee table.coreOptions().writeOnly() = false!!
        //                Preconditions.checkArgument(
        //                        !table.coreOptions().writeOnly(),
        //                        CoreOptions.WRITE_ONLY.key() + " should not be true for
        //         StoreCompactOperator.");
        this.catalogLoader = catalogLoader;
        this.initialCommitUser = initialCommitUser;
        this.checkpointConfig = checkpointConfig;
        this.isStreaming = isStreaming;
        this.ignorePreviousFiles = ignorePreviousFiles;
    }

    public MultiTablesStoreCompactOperator(
            Catalog.Loader catalogLoader,
            String initialCommitUser,
            CheckpointConfig checkpointConfig,
            boolean isStreaming,
            boolean ignorePreviousFiles) {
        super(new Options());
        // should guarantee table.coreOptions().writeOnly() = false!!
        //                Preconditions.checkArgument(
        //                        !table.coreOptions().writeOnly(),
        //                        CoreOptions.WRITE_ONLY.key() + " should not be true for
        //         StoreCompactOperator.");
        this.catalogLoader = catalogLoader;
        this.initialCommitUser = initialCommitUser;
        this.checkpointConfig = checkpointConfig;
        this.isStreaming = isStreaming;
        this.ignorePreviousFiles = ignorePreviousFiles;
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

        state =
                new StoreSinkWriteState(
                        context,
                        (tableName, partition, bucket) ->
                                ChannelComputer.select(
                                                partition,
                                                bucket,
                                                getRuntimeContext().getNumberOfParallelSubtasks())
                                        == getRuntimeContext().getIndexOfThisSubtask());

        tables = new HashMap<>();
        writes = new HashMap<>();
    }

    @Override
    public void open() throws Exception {
        super.open();
        dataFileMetaSerializer = new DataFileMetaSerializer();
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        RowData record = element.getValue();

        long snapshotId = record.getLong(0);
        BinaryRow partition = deserializeBinaryRow(record.getBinary(1));
        int bucket = record.getInt(2);
        byte[] serializedFiles = record.getBinary(3);
        List<DataFileMeta> files = dataFileMetaSerializer.deserializeList(serializedFiles);
        String databaseName = record.getString(4).toString();
        String tableName = record.getString(5).toString();

        Identifier tableId = Identifier.create(databaseName, tableName);
        FileStoreTable table = getTable(tableId);
        storeSinkWriteProvider =
                createWriteProvider(table, checkpointConfig, isStreaming, ignorePreviousFiles);

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

        if (write.streamingMode()) {
            write.notifyNewFiles(snapshotId, partition, bucket, files);
            write.compact(partition, bucket, false);
        } else {
            Preconditions.checkArgument(
                    files.isEmpty(),
                    "Batch compact job does not concern what files are compacted. "
                            + "They only need to know what buckets are compacted.");
            write.compact(partition, bucket, true);
        }
    }

    @Override
    protected List<MultiTableCommittable> prepareCommit(boolean waitCompaction, long checkpointId)
            throws IOException {

        List<MultiTableCommittable> committables = new LinkedList<>();
        for (Map.Entry<Identifier, StoreSinkWrite> entry : writes.entrySet()) {
            Identifier key = entry.getKey();
            StoreSinkWrite write = entry.getValue();
            committables.addAll(
                    write.prepareCommit(waitCompaction, checkpointId).stream()
                            .map(
                                    committable ->
                                            MultiTableCommittable.fromCommittable(key, committable))
                            .collect(Collectors.toList()));
        }
        return committables;
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

    private FileStoreTable getTable(Identifier tableId) throws InterruptedException {
        FileStoreTable table = tables.get(tableId);
        if (table == null) {
            while (true) {
                try {
                    table = (FileStoreTable) catalog.getTable(tableId);
                    table =
                            table.copy(
                                    Collections.singletonMap(
                                            CoreOptions.WRITE_ONLY.key(), "false"));
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

    private StoreSinkWrite.Provider createWriteProvider(
            FileStoreTable fileStoreTable,
            CheckpointConfig checkpointConfig,
            boolean isStreaming,
            boolean ignorePreviousFiles) {
        boolean waitCompaction;
        if (fileStoreTable.coreOptions().writeOnly()) {
            waitCompaction = false;
        } else {
            Options options = fileStoreTable.coreOptions().toConfiguration();
            CoreOptions.ChangelogProducer changelogProducer =
                    fileStoreTable.coreOptions().changelogProducer();
            waitCompaction =
                    changelogProducer == CoreOptions.ChangelogProducer.LOOKUP
                            && options.get(CHANGELOG_PRODUCER_LOOKUP_WAIT);

            int deltaCommits = -1;
            if (options.contains(FULL_COMPACTION_DELTA_COMMITS)) {
                deltaCommits = options.get(FULL_COMPACTION_DELTA_COMMITS);
            } else if (options.contains(CHANGELOG_PRODUCER_FULL_COMPACTION_TRIGGER_INTERVAL)) {
                long fullCompactionThresholdMs =
                        options.get(CHANGELOG_PRODUCER_FULL_COMPACTION_TRIGGER_INTERVAL).toMillis();
                deltaCommits =
                        (int)
                                (fullCompactionThresholdMs
                                        / checkpointConfig.getCheckpointInterval());
            }

            if (changelogProducer == CoreOptions.ChangelogProducer.FULL_COMPACTION
                    || deltaCommits >= 0) {
                int finalDeltaCommits = Math.max(deltaCommits, 1);
                // 这个 lamda 表达式代表接口 Provider 的具体实现
                return (table, commitUser, state, ioManager, memoryPool) ->
                        new GlobalFullCompactionSinkWrite(
                                table,
                                commitUser,
                                state,
                                ioManager,
                                ignorePreviousFiles,
                                waitCompaction,
                                finalDeltaCommits,
                                isStreaming,
                                memoryPool);
            }
        }

        return (table, commitUser, state, ioManager, memoryPool) ->
                new StoreSinkWriteImpl(
                        table,
                        commitUser,
                        state,
                        ioManager,
                        ignorePreviousFiles,
                        waitCompaction,
                        isStreaming,
                        memoryPool);
    }
}
