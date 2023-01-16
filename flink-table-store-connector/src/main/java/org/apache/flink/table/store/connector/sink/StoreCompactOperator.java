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

package org.apache.flink.table.store.connector.sink;

import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.connector.FlinkRowWrapper;
import org.apache.flink.table.store.data.BinaryRow;
import org.apache.flink.table.store.data.RowDataSerializer;
import org.apache.flink.table.store.file.io.DataFileMeta;
import org.apache.flink.table.store.file.io.DataFileMetaSerializer;
import org.apache.flink.table.store.file.utils.OffsetRow;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.List;

/**
 * A dedicated operator for manual triggered compaction.
 *
 * <p>In-coming records are generated by sources built from {@link
 * org.apache.flink.table.store.connector.source.CompactorSourceBuilder}. The records will contain
 * partition keys in the first few columns, and bucket number in the last column.
 */
public class StoreCompactOperator extends PrepareCommitOperator {

    private final FileStoreTable table;
    private final StoreSinkWrite.Provider storeSinkWriteProvider;
    private final boolean isStreaming;

    private transient StoreSinkWrite write;
    private transient RowDataSerializer partitionSerializer;
    private transient OffsetRow reusedPartition;
    private transient DataFileMetaSerializer dataFileMetaSerializer;

    public StoreCompactOperator(
            FileStoreTable table,
            StoreSinkWrite.Provider storeSinkWriteProvider,
            boolean isStreaming) {
        Preconditions.checkArgument(
                !table.options().writeOnly(),
                CoreOptions.WRITE_ONLY.key() + " should not be true for StoreCompactOperator.");
        this.table = table;
        this.storeSinkWriteProvider = storeSinkWriteProvider;
        this.isStreaming = isStreaming;
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        write =
                storeSinkWriteProvider.provide(
                        table, context, getContainingTask().getEnvironment().getIOManager());
    }

    @Override
    public void open() throws Exception {
        super.open();
        partitionSerializer = new RowDataSerializer(table.schema().logicalPartitionType());
        reusedPartition = new OffsetRow(partitionSerializer.getArity(), 1);
        dataFileMetaSerializer = new DataFileMetaSerializer();
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        RowData record = element.getValue();

        long snapshotId = record.getLong(0);

        reusedPartition.replace(new FlinkRowWrapper(record));
        BinaryRow partition = partitionSerializer.toBinaryRow(reusedPartition).copy();

        int bucket = record.getInt(partitionSerializer.getArity() + 1);

        byte[] serializedFiles = record.getBinary(partitionSerializer.getArity() + 2);
        List<DataFileMeta> files = dataFileMetaSerializer.deserializeList(serializedFiles);

        if (isStreaming) {
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
    protected List<Committable> prepareCommit(boolean doCompaction, long checkpointId)
            throws IOException {
        return write.prepareCommit(doCompaction, checkpointId);
    }
}
