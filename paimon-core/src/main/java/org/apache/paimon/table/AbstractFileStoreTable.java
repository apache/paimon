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
import org.apache.paimon.metastore.AddPartitionCommitCallback;
import org.apache.paimon.metastore.AddPartitionTagCallback;
import org.apache.paimon.metastore.MetastoreClient;
import org.apache.paimon.metastore.TagPreviewCommitCallback;
import org.apache.paimon.operation.DefaultValueAssigner;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaValidation;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.sink.CallbackUtils;
import org.apache.paimon.table.sink.CommitCallback;
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
import org.apache.paimon.table.source.snapshot.StaticFromWatermarkStartingScanner;
import org.apache.paimon.tag.TagPreview;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.function.BiConsumer;

import static org.apache.paimon.CoreOptions.PATH;
import static org.apache.paimon.utils.BranchManager.DEFAULT_MAIN_BRANCH;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Abstract {@link FileStoreTable}. */
abstract class AbstractFileStoreTable implements FileStoreTable {

    private static final long serialVersionUID = 1L;
    private static final String WATERMARK_PREFIX = "watermark-";

    protected final FileIO fileIO;
    protected final Path path;
    protected final TableSchema tableSchema;
    protected final CatalogEnvironment catalogEnvironment;

    protected AbstractFileStoreTable(
            FileIO fileIO,
            Path path,
            TableSchema tableSchema,
            CatalogEnvironment catalogEnvironment) {
        this.fileIO = fileIO;
        this.path = path;
        if (!tableSchema.options().containsKey(PATH.key())) {
            // make sure table is always available
            Map<String, String> newOptions = new HashMap<>(tableSchema.options());
            newOptions.put(PATH.key(), path.toString());
            tableSchema = tableSchema.copy(newOptions);
        }
        this.tableSchema = tableSchema;
        this.catalogEnvironment = catalogEnvironment;
    }

    @Override
    public Optional<Statistics> statistics() {
        // todo: support time travel
        Snapshot latestSnapshot = snapshotManager().latestSnapshot();
        if (latestSnapshot != null) {
            return store().newStatsFileHandler().readStats(latestSnapshot);
        }
        return Optional.empty();
    }

    @Override
    public BucketMode bucketMode() {
        return store().bucketMode();
    }

    @Override
    public CatalogEnvironment catalogEnvironment() {
        return catalogEnvironment;
    }

    public RowKeyExtractor createRowKeyExtractor() {
        switch (bucketMode()) {
            case HASH_FIXED:
                return new FixedBucketRowKeyExtractor(schema());
            case HASH_DYNAMIC:
            case CROSS_PARTITION:
                return new DynamicBucketRowKeyExtractor(schema());
            case BUCKET_UNAWARE:
                return new UnawareBucketRowKeyExtractor(schema());
            default:
                throw new UnsupportedOperationException("Unsupported mode: " + bucketMode());
        }
    }

    @Override
    public SnapshotReader newSnapshotReader() {
        return newSnapshotReader(DEFAULT_MAIN_BRANCH);
    }

    @Override
    public SnapshotReader newSnapshotReader(String branchName) {
        return new SnapshotReaderImpl(
                store().newScan(branchName),
                tableSchema,
                coreOptions(),
                snapshotManager(),
                splitGenerator(),
                nonPartitionFilterConsumer(),
                DefaultValueAssigner.create(tableSchema),
                store().pathFactory(),
                name(),
                store().newIndexFileHandler());
    }

    @Override
    public InnerTableScan newScan() {
        return new InnerTableScanImpl(
                tableSchema.primaryKeys().size() > 0,
                coreOptions(),
                newSnapshotReader(),
                DefaultValueAssigner.create(tableSchema));
    }

    @Override
    public InnerStreamTableScan newStreamScan() {
        return new InnerStreamTableScanImpl(
                coreOptions(),
                newSnapshotReader(),
                snapshotManager(),
                supportStreamingReadOverwrite(),
                DefaultValueAssigner.create(tableSchema));
    }

    protected abstract SplitGenerator splitGenerator();

    protected abstract BiConsumer<FileStoreScan, Predicate> nonPartitionFilterConsumer();

    @Override
    public FileStoreTable copy(Map<String, String> dynamicOptions) {
        checkImmutability(dynamicOptions);
        return copyInternal(dynamicOptions, true);
    }

    @Override
    public FileStoreTable copyWithoutTimeTravel(Map<String, String> dynamicOptions) {
        checkImmutability(dynamicOptions);
        return copyInternal(dynamicOptions, false);
    }

    private void checkImmutability(Map<String, String> dynamicOptions) {
        Map<String, String> options = tableSchema.options();
        // check option is not immutable
        dynamicOptions.forEach(
                (k, v) -> {
                    if (!Objects.equals(v, options.get(k))) {
                        SchemaManager.checkAlterTableOption(k);

                        if (CoreOptions.BUCKET.key().equals(k)) {
                            throw new UnsupportedOperationException(
                                    "Cannot change bucket number through dynamic options. You might need to rescale bucket.");
                        }
                    }
                });
    }

    private FileStoreTable copyInternal(Map<String, String> dynamicOptions, boolean tryTimeTravel) {
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

        if (tryTimeTravel) {
            // see if merged options contain time travel option
            newTableSchema = tryTimeTravel(newOptions).orElse(newTableSchema);
        }

        // validate schema with new options
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
    public ExpireSnapshots newExpireSnapshots() {
        return new ExpireSnapshotsImpl(
                snapshotManager(),
                store().newSnapshotDeletion(),
                store().newTagManager(),
                coreOptions().snapshotExpireCleanEmptyDirectories(),
                coreOptions().changelogLifecycleDecoupled());
    }

    @Override
    public ExpireSnapshots newExpireChangelog() {
        return new ExpireChangelogImpl(
                snapshotManager(),
                tagManager(),
                store().newSnapshotDeletion(),
                coreOptions().snapshotExpireCleanEmptyDirectories());
    }

    @Override
    public TableCommitImpl newCommit(String commitUser) {
        // Compatibility with previous design, the main branch is written by default
        return newCommit(commitUser, DEFAULT_MAIN_BRANCH);
    }

    public TableCommitImpl newCommit(String commitUser, String branchName) {
        CoreOptions options = coreOptions();
        Runnable snapshotExpire = null;
        if (!options.writeOnly()) {
            boolean changelogDecoupled = options.changelogLifecycleDecoupled();
            ExpireSnapshots expireChangelog =
                    newExpireChangelog()
                            .maxDeletes(options.snapshotExpireLimit())
                            .retainMin(options.changelogNumRetainMin())
                            .retainMax(options.changelogNumRetainMax());
            ExpireSnapshots expireSnapshots =
                    newExpireSnapshots()
                            .retainMax(options.snapshotNumRetainMax())
                            .retainMin(options.snapshotNumRetainMin())
                            .maxDeletes(options.snapshotExpireLimit());
            long snapshotTimeRetain = options.snapshotTimeRetain().toMillis();
            long changelogTimeRetain = options.changelogTimeRetain().toMillis();
            snapshotExpire =
                    () -> {
                        long current = System.currentTimeMillis();
                        expireSnapshots.olderThanMills(current - snapshotTimeRetain).expire();
                        if (changelogDecoupled) {
                            expireChangelog.olderThanMills(current - changelogTimeRetain).expire();
                        }
                    };
        }

        return new TableCommitImpl(
                store().newCommit(commitUser, branchName),
                createCommitCallbacks(),
                snapshotExpire,
                options.writeOnly() ? null : store().newPartitionExpire(commitUser),
                options.writeOnly() ? null : store().newTagCreationManager(),
                catalogEnvironment.lockFactory().create(),
                CoreOptions.fromMap(options()).consumerExpireTime(),
                new ConsumerManager(fileIO, path),
                coreOptions().snapshotExpireExecutionMode(),
                name(),
                coreOptions().forceCreatingSnapshot());
    }

    private List<CommitCallback> createCommitCallbacks() {
        List<CommitCallback> callbacks =
                new ArrayList<>(CallbackUtils.loadCommitCallbacks(coreOptions()));
        CoreOptions options = coreOptions();
        MetastoreClient.Factory metastoreClientFactory =
                catalogEnvironment.metastoreClientFactory();
        if (options.partitionedTableInMetastore()
                && metastoreClientFactory != null
                && tableSchema.partitionKeys().size() > 0) {
            callbacks.add(new AddPartitionCommitCallback(metastoreClientFactory.create()));
        }
        TagPreview tagPreview = TagPreview.create(options);
        if (options.tagToPartitionField() != null
                && tagPreview != null
                && metastoreClientFactory != null
                && tableSchema.partitionKeys().isEmpty()) {
            TagPreviewCommitCallback callback =
                    new TagPreviewCommitCallback(
                            new AddPartitionTagCallback(
                                    metastoreClientFactory.create(), options.tagToPartitionField()),
                            tagPreview);
            callbacks.add(callback);
        }
        return callbacks;
    }

    private Optional<TableSchema> tryTimeTravel(Options options) {
        CoreOptions coreOptions = new CoreOptions(options);

        switch (coreOptions.startupMode()) {
            case FROM_SNAPSHOT:
            case FROM_SNAPSHOT_FULL:
                if (coreOptions.scanVersion() != null) {
                    return travelToVersion(coreOptions.scanVersion(), options);
                } else if (coreOptions.scanSnapshotId() != null) {
                    return travelToSnapshot(coreOptions.scanSnapshotId(), options);
                } else if (coreOptions.scanWatermark() != null) {
                    return travelToWatermark(coreOptions.scanWatermark(), options);
                } else {
                    return travelToTag(coreOptions.scanTagName(), options);
                }
            case FROM_TIMESTAMP:
                Snapshot snapshot =
                        StaticFromTimestampStartingScanner.timeTravelToTimestamp(
                                snapshotManager(), coreOptions.scanTimestampMills());
                return travelToSnapshot(snapshot, options);
            default:
                return Optional.empty();
        }
    }

    /** Tag first when travelling to a version. */
    private Optional<TableSchema> travelToVersion(String version, Options options) {
        options.remove(CoreOptions.SCAN_VERSION.key());
        if (tagManager().tagExists(version)) {
            options.set(CoreOptions.SCAN_TAG_NAME, version);
            return travelToTag(version, options);
        } else if (version.startsWith(WATERMARK_PREFIX)) {
            long watermark = Long.parseLong(version.substring(WATERMARK_PREFIX.length()));
            options.set(CoreOptions.SCAN_WATERMARK, watermark);
            return travelToWatermark(watermark, options);
        } else if (version.chars().allMatch(Character::isDigit)) {
            options.set(CoreOptions.SCAN_SNAPSHOT_ID.key(), version);
            return travelToSnapshot(Long.parseLong(version), options);
        } else {
            throw new RuntimeException("Cannot find a time travel version for " + version);
        }
    }

    private Optional<TableSchema> travelToTag(String tagName, Options options) {
        return travelToSnapshot(tagManager().taggedSnapshot(tagName), options);
    }

    private Optional<TableSchema> travelToSnapshot(long snapshotId, Options options) {
        SnapshotManager snapshotManager = snapshotManager();
        if (snapshotManager.snapshotExists(snapshotId)) {
            return travelToSnapshot(snapshotManager.snapshot(snapshotId), options);
        }
        return Optional.empty();
    }

    private Optional<TableSchema> travelToWatermark(long watermark, Options options) {
        Snapshot snapshot =
                StaticFromWatermarkStartingScanner.timeTravelToWatermark(
                        snapshotManager(), watermark);
        if (snapshot != null) {
            return Optional.of(schemaManager().schema(snapshot.schemaId()).copy(options.toMap()));
        }
        return Optional.empty();
    }

    private Optional<TableSchema> travelToSnapshot(@Nullable Snapshot snapshot, Options options) {
        if (snapshot != null) {
            return Optional.of(schemaManager().schema(snapshot.schemaId()).copy(options.toMap()));
        }
        return Optional.empty();
    }

    @Override
    public void rollbackTo(long snapshotId) {
        SnapshotManager snapshotManager = snapshotManager();
        checkArgument(
                snapshotManager.snapshotExists(snapshotId),
                "Rollback snapshot '%s' doesn't exist.",
                snapshotId);

        rollbackHelper().cleanLargerThan(snapshotManager.snapshot(snapshotId));
    }

    public Snapshot createTagInternal(long fromSnapshotId) {
        SnapshotManager snapshotManager = snapshotManager();
        Snapshot snapshot = null;
        if (snapshotManager.snapshotExists(fromSnapshotId)) {
            snapshot = snapshotManager.snapshot(fromSnapshotId);
        } else {
            SortedMap<Snapshot, List<String>> tags = tagManager().tags();
            for (Snapshot snap : tags.keySet()) {
                if (snap.id() == fromSnapshotId) {
                    snapshot = snap;
                    break;
                } else if (snap.id() > fromSnapshotId) {
                    break;
                }
            }
        }
        checkArgument(
                snapshot != null,
                "Cannot create tag because given snapshot #%s doesn't exist.",
                fromSnapshotId);
        return snapshot;
    }

    @Override
    public void createTag(String tagName, long fromSnapshotId) {
        createTag(
                tagName, createTagInternal(fromSnapshotId), coreOptions().tagDefaultTimeRetained());
    }

    @Override
    public void createTag(String tagName, long fromSnapshotId, Duration timeRetained) {
        createTag(tagName, createTagInternal(fromSnapshotId), timeRetained);
    }

    @Override
    public void createTag(String tagName) {
        Snapshot latestSnapshot = snapshotManager().latestSnapshot();
        checkNotNull(latestSnapshot, "Cannot create tag because latest snapshot doesn't exist.");
        createTag(tagName, latestSnapshot, coreOptions().tagDefaultTimeRetained());
    }

    @Override
    public void createTag(String tagName, Duration timeRetained) {
        Snapshot latestSnapshot = snapshotManager().latestSnapshot();
        checkNotNull(latestSnapshot, "Cannot create tag because latest snapshot doesn't exist.");
        createTag(tagName, latestSnapshot, timeRetained);
    }

    private void createTag(String tagName, Snapshot fromSnapshot, @Nullable Duration timeRetained) {
        tagManager().createTag(fromSnapshot, tagName, timeRetained, store().createTagCallbacks());
    }

    @Override
    public void deleteTag(String tagName) {
        tagManager()
                .deleteTag(
                        tagName,
                        store().newTagDeletion(),
                        snapshotManager(),
                        store().createTagCallbacks());
    }

    @Override
    public void createBranch(String branchName) {
        branchManager().createBranch(branchName);
    }

    @Override
    public void createBranch(String branchName, long snapshotId) {
        branchManager().createBranch(branchName, snapshotId);
    }

    @Override
    public void createBranch(String branchName, String tagName) {
        branchManager().createBranch(branchName, tagName);
    }

    @Override
    public void deleteBranch(String branchName) {
        branchManager().deleteBranch(branchName);
    }

    @Override
    public void rollbackTo(String tagName) {
        TagManager tagManager = tagManager();
        checkArgument(tagManager.tagExists(tagName), "Rollback tag '%s' doesn't exist.", tagName);

        Snapshot taggedSnapshot = tagManager.taggedSnapshot(tagName);
        rollbackHelper().cleanLargerThan(taggedSnapshot);

        try {
            // it is possible that the earliest snapshot is later than the rollback tag because of
            // snapshot expiration, in this case the `cleanLargerThan` method will delete all
            // snapshots, so we should write the tag file to snapshot directory and modify the
            // earliest hint
            SnapshotManager snapshotManager = snapshotManager();
            if (!snapshotManager.snapshotExists(taggedSnapshot.id())) {
                fileIO.writeFileUtf8(
                        snapshotManager().snapshotPath(taggedSnapshot.id()),
                        fileIO.readFileUtf8(tagManager.tagPath(tagName)));
                snapshotManager.commitEarliestHint(taggedSnapshot.id());
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public TagManager tagManager() {
        return new TagManager(fileIO, path);
    }

    @Override
    public BranchManager branchManager() {
        return new BranchManager(fileIO, path, snapshotManager(), tagManager(), schemaManager());
    }

    private RollbackHelper rollbackHelper() {
        return new RollbackHelper(
                snapshotManager(),
                tagManager(),
                fileIO,
                store().newSnapshotDeletion(),
                store().newTagDeletion());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AbstractFileStoreTable that = (AbstractFileStoreTable) o;
        return Objects.equals(path, that.path) && Objects.equals(tableSchema, that.tableSchema);
    }
}
