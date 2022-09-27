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

import org.apache.flink.table.store.file.utils.RecordWriter;
import org.apache.flink.table.store.table.AppendOnlyFileStoreTable;
import org.apache.flink.table.store.table.ChangelogValueCountFileStoreTable;
import org.apache.flink.table.store.table.ChangelogWithKeyFileStoreTable;

/**
 * Write operation of the record with the given writer.
 *
 * @param <T> type of record to write.
 */
public interface WriteFunction<T> {
    /**
     * Write the record with given writer, operations in {@link AppendOnlyFileStoreTable}, {@link
     * ChangelogValueCountFileStoreTable} and {@link ChangelogWithKeyFileStoreTable} are different.
     *
     * @param record the record to write
     * @param writer the writer
     * @throws Exception the thrown exception
     */
    void write(SinkRecord record, RecordWriter<T> writer) throws Exception;
}
