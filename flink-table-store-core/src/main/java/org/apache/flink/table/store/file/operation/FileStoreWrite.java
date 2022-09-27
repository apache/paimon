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
import org.apache.flink.table.store.file.compact.CompactResult;
import org.apache.flink.table.store.file.io.DataFileMeta;
import org.apache.flink.table.store.file.utils.RecordWriter;
import org.apache.flink.table.store.table.sink.FileCommittable;
import org.apache.flink.table.store.table.sink.SinkRecord;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

/**
 * Write operation which provides {@link RecordWriter} creation and writes {@link SinkRecord} to
 * {@link FileStore}.
 *
 * @param <T> type of record to write.
 */
public interface FileStoreWrite<T> {

    FileStoreWrite<T> withIOManager(IOManager ioManager);

    /** Create a {@link RecordWriter} from partition and bucket. */
    RecordWriter<T> createWriter(
            BinaryRowData partition, int bucket, ExecutorService compactExecutor);

    /** Create an empty {@link RecordWriter} from partition and bucket. */
    RecordWriter<T> createEmptyWriter(
            BinaryRowData partition, int bucket, ExecutorService compactExecutor);

    /**
     * Create a {@link Callable} compactor from partition, bucket.
     *
     * @param compactFiles input files of compaction. When it is null, will automatically read all
     *     files of the current bucket.
     */
    Callable<CompactResult> createCompactWriter(
            BinaryRowData partition, int bucket, @Nullable List<DataFileMeta> compactFiles);

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
     * Prepare commit in the write.
     *
     * @param endOfInput if true, the data writing is ended
     * @return the file committable list
     * @throws Exception the thrown exception
     */
    List<FileCommittable> prepareCommit(boolean endOfInput) throws Exception;

    /**
     * Close the writer.
     *
     * @throws Exception the thrown exception
     */
    void close() throws Exception;
}
