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

package org.apache.paimon.table.sink;

import org.apache.paimon.file.disk.IOManager;
import org.apache.paimon.file.io.DataFileMeta;
import org.apache.paimon.file.operation.AbstractFileStoreWrite;
import org.apache.paimon.file.operation.FileStoreWrite;
import org.apache.paimon.file.utils.Restorable;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;

import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * {@link TableWrite} implementation.
 *
 * @param <T> type of record to write into {@link org.apache.paimon.file.FileStore}.
 */
public class TableWriteImpl<T>
        implements InnerTableWrite, Restorable<List<AbstractFileStoreWrite.State>> {

    private final AbstractFileStoreWrite<T> write;
    private final SinkRecordConverter recordConverter;
    private final RecordExtractor<T> recordExtractor;

    private boolean batchCommitted = false;

    public TableWriteImpl(
            FileStoreWrite<T> write,
            SinkRecordConverter recordConverter,
            RecordExtractor<T> recordExtractor) {
        this.write = (AbstractFileStoreWrite<T>) write;
        this.recordConverter = recordConverter;
        this.recordExtractor = recordExtractor;
    }

    @Override
    public TableWriteImpl<T> withOverwrite(boolean overwrite) {
        write.withOverwrite(overwrite);
        return this;
    }

    @Override
    public TableWriteImpl<T> withIOManager(IOManager ioManager) {
        write.withIOManager(ioManager);
        return this;
    }

    @Override
    public BinaryRow getPartition(InternalRow row) {
        return recordConverter.partition(row);
    }

    @Override
    public int getBucket(InternalRow row) {
        return recordConverter.bucket(row);
    }

    @Override
    public void write(InternalRow row) throws Exception {
        writeAndReturn(row);
    }

    public SinkRecord writeAndReturn(InternalRow row) throws Exception {
        SinkRecord record = recordConverter.convert(row);
        write.write(record.partition(), record.bucket(), recordExtractor.extract(record));
        return record;
    }

    public SinkRecord toLogRecord(SinkRecord record) {
        return recordConverter.convertToLogSinkRecord(record);
    }

    @Override
    public void compact(BinaryRow partition, int bucket, boolean fullCompaction) throws Exception {
        write.compact(partition, bucket, fullCompaction);
    }

    /**
     * Notify that some new files are created at given snapshot in given bucket.
     *
     * <p>Most probably, these files are created by another job. Currently this method is only used
     * by the dedicated compact job to see files created by writer jobs.
     */
    public void notifyNewFiles(
            long snapshotId, BinaryRow partition, int bucket, List<DataFileMeta> files) {
        write.notifyNewFiles(snapshotId, partition, bucket, files);
    }

    @Override
    public List<CommitMessage> prepareCommit(boolean waitCompaction, long commitIdentifier)
            throws Exception {
        return write.prepareCommit(waitCompaction, commitIdentifier);
    }

    @Override
    public List<CommitMessage> prepareCommit() throws Exception {
        checkState(!batchCommitted, "BatchTableWrite only support one-time committing.");
        batchCommitted = true;
        return prepareCommit(true, BatchWriteBuilder.COMMIT_IDENTIFIER);
    }

    @Override
    public void close() throws Exception {
        write.close();
    }

    @Override
    public List<AbstractFileStoreWrite.State> checkpoint() {
        return write.checkpoint();
    }

    @Override
    public void restore(List<AbstractFileStoreWrite.State> state) {
        write.restore(state);
    }

    /** Extractor to extract {@link T} from the {@link SinkRecord}. */
    public interface RecordExtractor<T> {

        T extract(SinkRecord record);
    }
}
