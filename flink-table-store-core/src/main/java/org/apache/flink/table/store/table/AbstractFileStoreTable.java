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

import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.annotation.VisibleForTesting;
import org.apache.flink.table.store.file.FileStore;
import org.apache.flink.table.store.file.operation.FileStoreScan;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.SchemaValidation;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.fs.FileIO;
import org.apache.flink.table.store.fs.Path;
import org.apache.flink.table.store.options.Options;
import org.apache.flink.table.store.table.sink.TableCommitImpl;
import org.apache.flink.table.store.table.source.BatchDataTableScan;
import org.apache.flink.table.store.table.source.BatchDataTableScanImpl;
import org.apache.flink.table.store.table.source.SplitGenerator;
import org.apache.flink.table.store.table.source.StreamDataTableScan;
import org.apache.flink.table.store.table.source.StreamDataTableScanImpl;
import org.apache.flink.table.store.table.source.snapshot.SnapshotSplitReader;
import org.apache.flink.table.store.table.source.snapshot.SnapshotSplitReaderImpl;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;

import static org.apache.flink.table.store.CoreOptions.PATH;

/** Abstract {@link FileStoreTable}. */
public abstract class AbstractFileStoreTable implements FileStoreTable {

    private static final long serialVersionUID = 1L;

    protected final FileIO fileIO;
    protected final Path path;
    protected final TableSchema tableSchema;

    public AbstractFileStoreTable(FileIO fileIO, Path path, TableSchema tableSchema) {
        this.fileIO = fileIO;
        this.path = path;
        this.tableSchema = tableSchema;
    }

    @VisibleForTesting
    public abstract FileStore<?> store();

    @Override
    public SnapshotSplitReader newSnapshotSplitReader() {
        return new SnapshotSplitReaderImpl(
                store().newScan(),
                tableSchema,
                options(),
                snapshotManager(),
                splitGenerator(),
                nonPartitionFilterConsumer());
    }

    @Override
    public BatchDataTableScan newScan() {
        return new BatchDataTableScanImpl(options(), newSnapshotSplitReader(), snapshotManager());
    }

    @Override
    public StreamDataTableScan newStreamScan() {
        return new StreamDataTableScanImpl(
                options(),
                newSnapshotSplitReader(),
                snapshotManager(),
                supportStreamingReadOverwrite());
    }

    public abstract SplitGenerator splitGenerator();

    protected abstract boolean supportStreamingReadOverwrite();

    public abstract BiConsumer<FileStoreScan, Predicate> nonPartitionFilterConsumer();

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

        Options newOptions = Options.fromMap(options);

        // merge dynamic options into schema.options
        dynamicOptions.forEach(newOptions::setString);

        // set path always
        newOptions.set(PATH, path.toString());

        // set dynamic options with default values
        CoreOptions.setDefaultValues(newOptions);

        // copy a new table store to contain dynamic options
        TableSchema newTableSchema = tableSchema.copy(newOptions.toMap());

        // validate schema wit new options
        SchemaValidation.validateTableSchema(newTableSchema);

        return copy(newTableSchema);
    }

    @Override
    public FileStoreTable copyWithLatestSchema() {
        Map<String, String> options = tableSchema.options();
        SchemaManager schemaManager = new SchemaManager(fileIO(), location());
        Optional<TableSchema> optionalLatestSchema = schemaManager.latest();
        if (optionalLatestSchema.isPresent()) {
            TableSchema newTableSchema = optionalLatestSchema.get();
            newTableSchema = newTableSchema.copy(options);
            SchemaValidation.validateTableSchema(newTableSchema);
            return copy(newTableSchema);
        } else {
            return this;
        }
    }

    protected SchemaManager schemaManager() {
        return new SchemaManager(fileIO(), path);
    }

    @Override
    public CoreOptions options() {
        return store().options();
    }

    @Override
    public FileIO fileIO() {
        return fileIO;
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
    public TableCommitImpl newCommit(String commitUser) {
        return new TableCommitImpl(
                store().newCommit(commitUser),
                options().writeOnly() ? null : store().newExpire(),
                options().writeOnly() ? null : store().newPartitionExpire(commitUser));
    }
}
