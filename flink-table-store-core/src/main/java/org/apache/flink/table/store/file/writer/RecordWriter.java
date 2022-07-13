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

package org.apache.flink.table.store.file.writer;

import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.mergetree.Increment;

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

    /**
     * Prepare for a commit.
     *
     * @param endOfInput Signal that there is no committable anymore.
     * @return Incremental files in this snapshot cycle
     */
    Increment prepareCommit(boolean endOfInput) throws Exception;

    /**
     * Sync the writer. The structure related to file reading and writing is thread unsafe, there
     * are asynchronous threads inside the writer, which should be synced before reading data.
     */
    void sync() throws Exception;

    /**
     * Close this writer, the call will delete newly generated but not committed files.
     *
     * @return Deleted files.
     */
    List<DataFileMeta> close() throws Exception;
}
