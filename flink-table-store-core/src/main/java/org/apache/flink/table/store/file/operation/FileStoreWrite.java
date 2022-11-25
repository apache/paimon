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

package org.apache.flink.table.store.file.operation;

import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.FileStore;
import org.apache.flink.table.store.file.utils.RecordWriter;
import org.apache.flink.table.store.table.sink.FileCommittable;
import org.apache.flink.table.store.table.sink.SinkRecord;

import java.util.List;

/**
 * Write operation which provides {@link RecordWriter} creation and writes {@link SinkRecord} to
 * {@link FileStore}.
 *
 * @param <T> type of record to write.
 */
public interface FileStoreWrite<T> {

    FileStoreWrite<T> withIOManager(IOManager ioManager);

    /**
     * If overwrite is true, the writer will overwrite the store, otherwise it won't.
     *
     * @param overwrite the overwrite flag
     */
    void withOverwrite(boolean overwrite);

    /**
     * Write the data to the store according to the partition and bucket.
     *
     * @param partition the partition of the data
     * @param bucket the bucket id of the data
     * @param data the given data
     * @throws Exception the thrown exception when writing the record
     */
    void write(BinaryRowData partition, int bucket, T data) throws Exception;

    /**
     * Compact data stored in given partition and bucket. Note that compaction process is only
     * submitted and may not be completed when the method returns.
     *
     * @param partition the partition to compact
     * @param bucket the bucket to compact
     * @param fullCompaction whether to trigger full compaction or just normal compaction
     * @throws Exception the thrown exception when compacting the records
     */
    void compact(BinaryRowData partition, int bucket, boolean fullCompaction) throws Exception;

    /**
     * Prepare commit in the write.
     *
     * @param blocking if this method need to wait for current compaction to complete
     * @param commitIdentifier identifier of the commit being prepared
     * @return the file committable list
     * @throws Exception the thrown exception
     */
    List<FileCommittable> prepareCommit(boolean blocking, long commitIdentifier) throws Exception;

    /**
     * Close the writer.
     *
     * @throws Exception the thrown exception
     */
    void close() throws Exception;
}
