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

package org.apache.flink.table.store.table;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.store.file.FileStore;
import org.apache.flink.table.store.file.FileStoreOptions;
import org.apache.flink.table.store.file.WriteMode;
import org.apache.flink.table.store.file.schema.Schema;

/**
 * An abstraction layer above {@link org.apache.flink.table.store.file.FileStore} to provide reading
 * and writing of {@link org.apache.flink.table.data.RowData}.
 */
public interface FileStoreTable {

    TableScan newScan();

    TableRead newRead();

    // TODO remove this once TableWrite is introduced
    @VisibleForTesting
    FileStore fileStore();

    static FileStoreTable create(
            Schema schema, boolean isStreaming, Configuration conf, String user) {
        if (conf.get(FileStoreOptions.WRITE_MODE) == WriteMode.APPEND_ONLY) {
            return new AppendOnlyFileStoreTable(schema, isStreaming, conf, user);
        } else {
            if (schema.primaryKeys().isEmpty()) {
                return new ChangelogValueCountFileStoreTable(schema, isStreaming, conf, user);
            } else {
                return new ChangelogWithKeyFileStoreTable(schema, isStreaming, conf, user);
            }
        }
    }
}
