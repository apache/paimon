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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.operation.FileStoreWrite;
import org.apache.paimon.operation.FileStoreWrite.State;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.utils.Restorable;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.ExecutorService;

/** replace for {@link TableWriteImpl} . */
public interface TableWriteApi<T> extends InnerTableWrite, Restorable<List<State<T>>> {
    TableWriteApi<T> withCompactExecutor(ExecutorService compactExecutor);

    TableWriteApi<T> withBucketMode(BucketMode bucketMode);

    TableWriteApi<T> withMemoryPoolFactory(MemoryPoolFactory memoryPoolFactory);

    @Nullable
    SinkRecord writeAndReturn(InternalRow row) throws Exception;

    @Nullable
    SinkRecord writeAndReturn(InternalRow row, int bucket) throws Exception;

    SinkRecord toLogRecord(SinkRecord record);

    /**
     * Notify that some new files are created at given snapshot in given bucket.
     *
     * <p>Most probably, these files are created by another job. Currently this method is only used
     * by the dedicated compact job to see files created by writer jobs.
     */
    void notifyNewFiles(long snapshotId, BinaryRow partition, int bucket, List<DataFileMeta> files);

    @VisibleForTesting
    FileStoreWrite<T> getWrite();

    default boolean isTableWriteApi() {
        return true;
    }

    default TableWriteApi<?> asTableWriteApi() {
        return this;
    }
}
