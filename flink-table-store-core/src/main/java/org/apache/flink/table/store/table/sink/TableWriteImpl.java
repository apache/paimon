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

package org.apache.flink.table.store.table.sink;

import org.apache.flink.table.store.data.BinaryRow;
import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.file.disk.IOManager;
import org.apache.flink.table.store.file.io.DataFileMeta;
import org.apache.flink.table.store.file.operation.FileStoreWrite;

import java.util.List;

/**
 * {@link TableWrite} implementation.
 *
 * @param <T> type of record to write into {@link org.apache.flink.table.store.file.FileStore}.
 */
public class TableWriteImpl<T> implements TableWrite {

    private final FileStoreWrite<T> write;
    private final SinkRecordConverter recordConverter;
    private final RecordExtractor<T> recordExtractor;

    public TableWriteImpl(
            FileStoreWrite<T> write,
            SinkRecordConverter recordConverter,
            RecordExtractor<T> recordExtractor) {
        this.write = write;
        this.recordConverter = recordConverter;
        this.recordExtractor = recordExtractor;
    }

    @Override
    public TableWrite withOverwrite(boolean overwrite) {
        write.withOverwrite(overwrite);
        return this;
    }

    @Override
    public TableWrite withIOManager(IOManager ioManager) {
        write.withIOManager(ioManager);
        return this;
    }

    @Override
    public SinkRecord write(InternalRow rowData) throws Exception {
        SinkRecord record = recordConverter.convert(rowData);
        write.write(record.partition(), record.bucket(), recordExtractor.extract(record));
        return record;
    }

    @Override
    public SinkRecord toLogRecord(SinkRecord record) {
        return recordConverter.convertToLogSinkRecord(record);
    }

    @Override
    public void compact(BinaryRow partition, int bucket, boolean fullCompaction) throws Exception {
        write.compact(partition, bucket, fullCompaction);
    }

    @Override
    public void notifyNewFiles(
            long snapshotId, BinaryRow partition, int bucket, List<DataFileMeta> files) {
        write.notifyNewFiles(snapshotId, partition, bucket, files);
    }

    @Override
    public List<FileCommittable> prepareCommit(boolean blocking, long commitIdentifier)
            throws Exception {
        return write.prepareCommit(blocking, commitIdentifier);
    }

    @Override
    public void close() throws Exception {
        write.close();
    }

    /** Extractor to extract {@link T} from the {@link SinkRecord}. */
    public interface RecordExtractor<T> {

        T extract(SinkRecord record);
    }
}
