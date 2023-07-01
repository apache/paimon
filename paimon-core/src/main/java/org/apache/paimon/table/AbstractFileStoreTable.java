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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.operation.TagDeletion;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaValidation;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.DynamicBucketRowKeyExtractor;
import org.apache.paimon.table.sink.FixedBucketRowKeyExtractor;
import org.apache.paimon.table.sink.RowKeyExtractor;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.UnawareBucketRowKeyExtractor;
import org.apache.paimon.table.source.InnerStreamTableScan;
import org.apache.paimon.table.source.InnerStreamTableScanImpl;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.InnerTableScanImpl;
import org.apache.paimon.table.source.SplitGenerator;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.table.source.snapshot.SnapshotReaderImpl;
import org.apache.paimon.table.source.snapshot.StaticFromTimestampStartingScanner;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.StringUtils;
import org.apache.paimon.utils.TagManager;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;

import static org.apache.paimon.CoreOptions.PATH;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Abstract {@link FileStoreTable}. */
public abstract class AbstractFileStoreTable implements FileStoreTable {

    private static final long serialVersionUID = 1L;

    protected final FileIO fileIO;
    protected final Path path;
    protected final TableSchema tableSchema;
    protected final Lock.Factory lockFactory;

    public AbstractFileStoreTable(
            FileIO fileIO, Path path, TableSchema tableSchema, Lock.Factory lockFactory) {
        this.fileIO = fileIO;
        this.path = path;
        if (!tableSchema.options().containsKey(PATH.key())) {
            // make sure table is always available
            Map<String, String> newOptions = new HashMap<>(tableSchema.options());
            newOptions.put(PATH.key(), path.toString());
            tableSchema = tableSchema.copy(newOptions);
        }
        this.tableSchema = tableSchema;
        this.lockFactory = lockFactory;
    }

    @Override
    public BucketMode bucketMode() {
        return store().bucketMode();
    }

    public RowKeyExtractor createRowKeyExtractor() {
        switch (bucketMode()) {
            case FIXED:
                return new FixedBucketRowKeyExtractor(schema());
            case DYNAMIC:
                return new DynamicBucketRowKeyExtractor(schema());
            case UNAWARE:
                return new UnawareBucketRowKeyExtractor(schema());
            default:
                throw new UnsupportedOperationException("Unsupported mode: " + bucketMode());
        }
    }

    @Override
    public SnapshotReader newSnapshotReader() {
        return new SnapshotReaderImpl(
                store().newScan(),
                tableSchema,
                coreOptions(),
                snapshotManager(),
                splitGenerator(),
                nonPartitionFilterConsumer());
    }

    @Override
    public InnerTableScan newScan() {
        return new InnerTableScanImpl(coreOptions(), newSnapshotReader(), snapshotManager());
    }

    @Override
    public InnerStreamTableScan newStreamScan() {
        return new InnerStreamTableScanImpl(
                coreOptions(),
                newSnapshotReader(),
                snapshotManager(),
                supportStreamingReadOverwrite());
    }

    public abstract SplitGenerator splitGenerator();

    protected abstract boolean supportStreamingReadOverwrite();

    public abstract BiConsumer<FileStoreScan, Predicate> nonPartitionFilterConsumer();

    protected abstract FileStoreTable copy(TableSchema newTableSchema);

    @Override
    public FileStoreTable copy(Map<String, String> dynamicOptions) {
        Map<String, String> options = tableSchema.options();
        // check option is not immutable
        dynamicOptions.forEach(
                (k, v) -> {
                    if (!Objects.equals(v, options.get(k))) {
                        SchemaManager.checkAlterTableOption(k);
                    }
                });

        return internalCopyWithoutCheck(dynamicOptions);
    }

    @Override
    public FileStoreTable internalCopyWithoutCheck(Map<String, String> dynamicOptions) {
        Map<String, String> options = new HashMap<>(tableSchema.options());

        // merge non-null dynamic options into schema.options
        dynamicOptions.forEach(
                (k, v) -> {
                    if (v == null) {
                        options.remove(k);
                    } else {
                        options.put(k, v);
                    }
                });

        Options newOptions = Options.fromMap(options);

        // set path always
        newOptions.set(PATH, path.toString());

        // set dynamic options with default values
        CoreOptions.setDefaultValues(newOptions);

        // copy a new table schema to contain dynamic options
        TableSchema newTableSchema = tableSchema.copy(newOptions.toMap());

        // validate schema with new options
        SchemaValidation.validateTableSchema(newTableSchema);

        // see if merged options contain time travel option
        newTableSchema = tryTimeTravel(newOptions).orElse(newTableSchema);

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
    public CoreOptions coreOptions() {
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
                coreOptions().writeOnly() ? null : store().newExpire(),
                coreOptions().writeOnly() ? null : store().newPartitionExpire(commitUser),
                lockFactory.create(),
                CoreOptions.fromMap(options()).consumerExpireTime(),
                new ConsumerManager(fileIO, path));
    }

    private Optional<TableSchema> tryTimeTravel(Options options) {
        CoreOptions coreOptions = new CoreOptions(options);

        switch (coreOptions.startupMode()) {
            case FROM_SNAPSHOT:
            case FROM_SNAPSHOT_FULL:
                if (coreOptions.scanSnapshotId() != null) {
                    long snapshotId = coreOptions.scanSnapshotId();
                    if (snapshotManager().snapshotExists(snapshotId)) {
                        long schemaId = snapshotManager().snapshot(snapshotId).schemaId();
                        return Optional.of(schemaManager().schema(schemaId).copy(options.toMap()));
                    }
                } else {
                    String tagName = coreOptions.scanTagName();
                    TagManager tagManager = new TagManager(fileIO, path);
                    if (tagManager.tagExists(tagName)) {
                        long schemaId = tagManager.taggedSnapshot(tagName).schemaId();
                        return Optional.of(schemaManager().schema(schemaId).copy(options.toMap()));
                    }
                }
                return Optional.empty();
            case FROM_TIMESTAMP:
                Snapshot snapshot =
                        StaticFromTimestampStartingScanner.timeTravelToTimestamp(
                                snapshotManager(), coreOptions.scanTimestampMills());
                if (snapshot != null) {
                    long schemaId = snapshot.schemaId();
                    return Optional.of(schemaManager().schema(schemaId).copy(options.toMap()));
                }
                return Optional.empty();
            default:
                return Optional.empty();
        }
    }

    @Override
    public void rollbackTo(long snapshotId) {
        try {
            snapshotManager().rollbackTo(store().newSnapshotDeletion(), snapshotId);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void createTag(String tagName, long fromSnapshotId) {
        SnapshotManager snapshotManager = snapshotManager();
        checkArgument(
                snapshotManager.snapshotExists(fromSnapshotId),
                "Cannot create tag because given snapshot #%s doesn't exist.",
                fromSnapshotId);

        Snapshot snapshot = snapshotManager.snapshot(fromSnapshotId);
        TagManager tagManager = new TagManager(fileIO, path);
        tagManager.createTag(snapshot, tagName);
    }

    @Override
    public void deleteTag(String tagName) {
        checkArgument(!StringUtils.isBlank(tagName), "Tag name '%s' is blank.", tagName);

        TagManager tagManager = new TagManager(fileIO, path);
        Snapshot taggedSnapshot = tagManager.taggedSnapshot(tagName);

        TagDeletion tagDeletion = store().newTagDeletion();
        tagDeletion.delete(taggedSnapshot);
        fileIO.deleteQuietly(tagManager.tagPath(tagName));
    }
}
