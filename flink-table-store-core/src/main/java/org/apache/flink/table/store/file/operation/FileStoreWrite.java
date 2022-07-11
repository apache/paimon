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

import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.compact.CompactResult;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.writer.RecordWriter;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

/**
 * Write operation which provides {@link RecordWriter} creation.
 *
 * @param <T> type of record to write.
 */
public interface FileStoreWrite<T> {

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
}
