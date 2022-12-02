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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.file.FileStore;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.table.sink.TableCommit;

import java.util.Map;
import java.util.Objects;

import static org.apache.flink.table.store.CoreOptions.PATH;

/** Abstract {@link FileStoreTable}. */
public abstract class AbstractFileStoreTable implements FileStoreTable {

    private static final long serialVersionUID = 1L;

    protected final Path path;
    protected final TableSchema tableSchema;

    public AbstractFileStoreTable(Path path, TableSchema tableSchema) {
        this.path = path;
        this.tableSchema = tableSchema;
    }

    protected abstract FileStore<?> store();

    protected abstract FileStoreTable copy(TableSchema newTableSchema);

    @Override
    public FileStoreTable copy(Map<String, String> dynamicOptions) {
        // check option is not immutable
        Map<String, String> options = tableSchema.options();
        dynamicOptions.forEach(
                (k, v) -> {
                    if (!Objects.equals(v, options.get(k))) {
                        SchemaManager.checkAlterTableOption(k);
                    }
                });

        Configuration newOptions = Configuration.fromMap(options);

        // merge dynamic options into schema.options
        dynamicOptions.forEach(newOptions::setString);

        // set path always
        newOptions.set(PATH, path.toString());

        // set dynamic options with default values
        CoreOptions.setDefaultValues(newOptions);

        // copy a new table store to contain dynamic options
        TableSchema newTableSchema = tableSchema.copy(newOptions.toMap());

        // validate schema wit new options
        CoreOptions.validateTableSchema(newTableSchema);

        return copy(newTableSchema);
    }

    protected SchemaManager schemaManager() {
        return new SchemaManager(path);
    }

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
