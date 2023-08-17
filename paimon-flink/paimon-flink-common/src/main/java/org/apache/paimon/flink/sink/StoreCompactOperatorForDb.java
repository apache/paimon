package org.apache.paimon.flink.sink;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.sink.cdc.CdcRecordStoreWriteOperator.RETRY_SLEEP_TIME;
import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;

/** this is a doc. */
public class StoreCompactOperatorForDb
        extends PrepareCommitOperator<RowData, MultiTableCommittable> {

    private static final long serialVersionUID = 1L;

    private final StoreSinkWrite.Provider storeSinkWriteProvider;
    private final String initialCommitUser;

    private transient StoreSinkWriteState state;
    private transient DataFileMetaSerializer dataFileMetaSerializer;

    private final Catalog.Loader catalogLoader;

    protected Catalog catalog;
    protected Map<Identifier, FileStoreTable> tables;
    protected Map<Identifier, StoreSinkWrite> writes;
    protected String commitUser;

    public StoreCompactOperatorForDb(
            Catalog.Loader catalogLoader,
            StoreSinkWrite.Provider storeSinkWriteProvider,
            String initialCommitUser,
            Options options) {
        super(options);
        //        Preconditions.checkArgument(
        //                !table.coreOptions().writeOnly(),
        //                CoreOptions.WRITE_ONLY.key() + " should not be true for
        // StoreCompactOperator.");
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
                    tables.put(tableId, table);
                    break;
                } catch (Catalog.TableNotExistException e) {
                    // table not found, waiting until table is created by
                    //     upstream operators
                }
                Thread.sleep(RETRY_SLEEP_TIME.defaultValue().toMillis());
            }
        }

        //        if (table.bucketMode() != BucketMode.FIXED) {
        //            throw new UnsupportedOperationException(
        //                    "Unified Sink only supports FIXED bucket mode, but is " +
        // table.bucketMode());
        //        }
        return table;
    }
}
