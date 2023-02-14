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

import java.util.List;

/**
 * An abstraction layer above {@link org.apache.flink.table.store.file.operation.FileStoreWrite} to
 * provide {@link InternalRow} writing.
 */
public interface TableWrite extends AutoCloseable {

    TableWrite withOverwrite(boolean overwrite);

    TableWrite withIOManager(IOManager ioManager);

    SinkRecord write(InternalRow rowData) throws Exception;

    /** Log record need to preserve original pk (which includes partition fields). */
    SinkRecord toLogRecord(SinkRecord record);

    void compact(BinaryRow partition, int bucket, boolean fullCompaction) throws Exception;

    /**
     * Notify that some new files are created at given snapshot in given bucket.
     *
     * <p>Most probably, these files are created by another job. Currently this method is only used
     * by the dedicated compact job to see files created by writer jobs.
     */
    void notifyNewFiles(long snapshotId, BinaryRow partition, int bucket, List<DataFileMeta> files);

    List<FileCommittable> prepareCommit(boolean blocking, long commitIdentifier) throws Exception;

    void close() throws Exception;
}
