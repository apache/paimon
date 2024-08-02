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

package org.apache.paimon.utils;

import org.apache.paimon.io.DataFileMeta;

import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.Collection;
import java.util.List;

/**
 * The {@code RecordWriter} is responsible for writing data and handling in-progress files used to
 * write yet un-staged data. The incremental files ready to commit is returned to the system by the
 * {@link #prepareCommit(boolean)}.
 *
 * @param <T> type of record to write.
 */
public interface RecordWriter<T> {

    /** Add a key-value element to the writer. */
    void write(T record) throws Exception;

    void writeDirect(VectorSchemaRoot vectorSchemaRoot) throws Exception;

    /**
     * Compact files related to the writer. Note that compaction process is only submitted and may
     * not be completed when the method returns.
     *
     * @param fullCompaction whether to trigger full compaction or just normal compaction
     */
    void compact(boolean fullCompaction) throws Exception;

    /**
     * Add files to the internal {@link org.apache.paimon.compact.CompactManager}.
     *
     * @param files files to add
     */
    void addNewFiles(List<DataFileMeta> files);

    /** Get all data files maintained by this writer. */
    Collection<DataFileMeta> dataFiles();

    /** Get max sequence number of records written by this writer. */
    long maxSequenceNumber();

    /**
     * Prepare for a commit.
     *
     * @param waitCompaction if this method need to wait for current compaction to complete
     * @return Incremental files in this snapshot cycle
     */
    CommitIncrement prepareCommit(boolean waitCompaction) throws Exception;

    /** Check if a compaction is in progress, or if a compaction result remains to be fetched. */
    boolean isCompacting();

    /**
     * Sync the writer. The structure related to file reading and writing is thread unsafe, there
     * are asynchronous threads inside the writer, which should be synced before reading data.
     */
    void sync() throws Exception;

    /**
     * This method is called when the insert only status of the records changes.
     *
     * @param insertOnly If true, all the following records would be of {@link
     *     org.apache.paimon.types.RowKind#INSERT}, and no two records would have the same primary
     *     key.
     */
    void withInsertOnly(boolean insertOnly);

    /** Close this writer, the call will delete newly generated but not committed files. */
    void close() throws Exception;
}
