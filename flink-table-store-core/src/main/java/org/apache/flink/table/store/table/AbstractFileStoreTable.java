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

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.file.FileStore;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.table.sink.TableCommit;

/** Abstract {@link FileStoreTable}. */
public abstract class AbstractFileStoreTable implements FileStoreTable {

    private static final long serialVersionUID = 1L;

    private final Path path;
    protected final TableSchema tableSchema;

    public AbstractFileStoreTable(Path path, TableSchema tableSchema) {
        this.path = path;
        this.tableSchema = tableSchema;
    }

    protected abstract FileStore<?> store();

    @Override
    public CoreOptions options() {
        return store().options();
    }

    @Override
    public Path location() {
        return path;
    }

    @Override
    public TableSchema schema() {
        return tableSchema;
    }

    @Override
    public SnapshotManager snapshotManager() {
        return store().snapshotManager();
    }

    @Override
    public TableCommit newCommit(String commitUser) {
        return new TableCommit(store().newCommit(commitUser), store().newExpire());
    }
}
