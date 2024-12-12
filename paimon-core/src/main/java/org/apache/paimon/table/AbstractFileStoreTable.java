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
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.TablePathProvider;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.metastore.AddPartitionCommitCallback;
import org.apache.paimon.metastore.AddPartitionTagCallback;
import org.apache.paimon.metastore.MetastoreClient;
import org.apache.paimon.metastore.TagPreviewCommitCallback;
import org.apache.paimon.operation.DefaultValueAssigner;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.options.ExpireConfig;
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
import org.apache.paimon.table.sink.FixedBucketWriteSelector;
import org.apache.paimon.table.sink.RowKeyExtractor;
import org.apache.paimon.table.sink.RowKindGenerator;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.UnawareBucketRowKeyExtractor;
import org.apache.paimon.table.sink.WriteSelector;
import org.apache.paimon.table.source.DataTableBatchScan;
import org.apache.paimon.table.source.DataTableStreamScan;
import org.apache.paimon.table.source.SplitGenerator;
import org.apache.paimon.table.source.StreamDataTableScan;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.table.source.snapshot.SnapshotReaderImpl;
import org.apache.paimon.table.source.snapshot.StaticFromTimestampStartingScanner;
import org.apache.paimon.table.source.snapshot.StaticFromWatermarkStartingScanner;
import org.apache.paimon.table.source.snapshot.TimeTravelUtil;
import org.apache.paimon.tag.TagPreview;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SegmentsCache;
import org.apache.paimon.utils.SimpleFileReader;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.SnapshotNotExistException;
import org.apache.paimon.utils.TagManager;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;

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
import java.util.OptionalLong;
import java.util.SortedMap;
import java.util.function.BiConsumer;

import static org.apache.paimon.CoreOptions.PATH;
import static org.apache.paimon.CoreOptions.WAREHOUSE_ROOT_PATH;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Abstract {@link FileStoreTable}. */
abstract class AbstractFileStoreTable implements FileStoreTable {

    private static final long serialVersionUID = 1L;

    private static final String WATERMARK_PREFIX = "watermark-";

    protected final FileIO fileIO;
    protected final TablePathProvider tablePathProvider;
    protected final TableSchema tableSchema;
    protected final CatalogEnvironment catalogEnvironment;

    @Nullable protected transient SegmentsCache<Path> manifestCache;
    @Nullable protected transient Cache<Path, Snapshot> snapshotCache;
    @Nullable protected transient Cache<String, Statistics> statsCache;

    protected AbstractFileStoreTable(
            FileIO fileIO,
            TablePathProvider tablePathProvider,
            TableSchema tableSchema,
            CatalogEnvironment catalogEnvironment) {
        this.fileIO = fileIO;
        this.tablePathProvider = tablePathProvider;
        if (!tableSchema.options().containsKey(PATH.key())) {
            // make sure table is always available
            Map<String, String> newOptions = new HashMap<>(tableSchema.options());
            newOptions.put(PATH.key(), tablePathProvider.getTableWritePathString());
            tableSchema = tableSchema.copy(newOptions);
        }
        this.tableSchema = tableSchema;
        this.catalogEnvironment = catalogEnvironment;
    }

    public String currentBranch() {
        return CoreOptions.branch(options());
    }

    @Override
    public void setManifestCache(SegmentsCache<Path> manifestCache) {
        this.manifestCache = manifestCache;
        store().setManifestCache(manifestCache);
    }

    @Override
    public void setSnapshotCache(Cache<Path, Snapshot> cache) {
        this.snapshotCache = cache;
        store().setSnapshotCache(cache);
    }

    @Override
    public void setStatsCache(Cache<String, Statistics> cache) {
        this.statsCache = cache;
    }

    @Override
    public OptionalLong latestSnapshotId() {
        Long snapshot = store().snapshotManager().latestSnapshotId();
        return snapshot == null ? OptionalLong.empty() : OptionalLong.of(snapshot);
    }

    @Override
    public Snapshot snapshot(long snapshotId) {
        return store().snapshotManager().snapshot(snapshotId);
    }

    @Override
    public SimpleFileReader<ManifestFileMeta> manifestListReader() {
        return store().manifestListFactory().create();
    }

    @Override
    public SimpleFileReader<ManifestEntry> manifestFileReader() {
        return store().manifestFileFactory().create();
    }

    @Override
    public SimpleFileReader<IndexManifestEntry> indexManifestFileReader() {
        return store().indexManifestFileFactory().create();
    }

    @Override
    public String name() {
        return identifier().getObjectName();
    }

    @Override
    public String fullName() {
        return identifier().getFullName();
    }

    public Identifier identifier() {
        Identifier identifier = catalogEnvironment.identifier();
        return identifier == null
                ? SchemaManager.identifierFromPath(
                        location().toUri().toString(), true, currentBranch())
                : identifier;
    }

    @Override
    public String uuid() {
        if (catalogEnvironment.uuid() != null) {
            return catalogEnvironment.uuid();
        }
        long earliestCreationTime = schemaManager().earliestCreationTime();
        return fullName() + "." + earliestCreationTime;
    }

    @Override
    public Optional<Statistics> statistics() {
        Snapshot snapshot = TimeTravelUtil.resolveSnapshot(this);
        if (snapshot != null) {
            String file = snapshot.statistics();
            if (file == null) {
                return Optional.empty();
            }
            if (statsCache != null) {
                Statistics stats = statsCache.getIfPresent(file);
                if (stats != null) {
                    return Optional.of(stats);
                }
            }
            Statistics stats = store().newStatsFileHandler().readStats(file);
            if (statsCache != null) {
                statsCache.put(file, stats);
            }
            return Optional.of(stats);
        }
        return Optional.empty();
    }

    @Override
    public Optional<WriteSelector> newWriteSelector() {
        switch (bucketMode()) {
            case HASH_FIXED:
                return Optional.of(new FixedBucketWriteSelector(schema()));
            case BUCKET_UNAWARE:
                return Optional.empty();
            default:
                throw new UnsupportedOperationException(
                        "Currently, write selector does not support table mode: " + bucketMode());
        }
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
        return new SnapshotReaderImpl(
                store().newScan(),
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
    public DataTableBatchScan newScan() {
        return new DataTableBatchScan(
                tableSchema.primaryKeys().size() > 0,
                coreOptions(),
                newSnapshotReader(),
                DefaultValueAssigner.create(tableSchema));
    }

    @Override
    public StreamDataTableScan newStreamScan() {
        return new DataTableStreamScan(
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
        Map<String, String> oldOptions = tableSchema.options();
        // check option is not immutable
        dynamicOptions.forEach(
                (k, newValue) -> {
                    String oldValue = oldOptions.get(k);
                    if (!Objects.equals(oldValue, newValue)) {
                        SchemaManager.checkAlterTableOption(k, oldValue, newValue, true);

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

        // set warehouse table path always
        newOptions.set(PATH, tablePathProvider.getTableWritePathString());

        // set warehouse root path always
        newOptions.set(WAREHOUSE_ROOT_PATH, tablePathProvider.getWarehouseRootPathString());

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
        Optional<TableSchema> optionalLatestSchema = schemaManager().latest();
        if (optionalLatestSchema.isPresent()) {
            Map<String, String> options = tableSchema.options();
            TableSchema newTableSchema = optionalLatestSchema.get();
            newTableSchema = newTableSchema.copy(options);
            SchemaValidation.validateTableSchema(newTableSchema);
            return copy(newTableSchema);
        } else {
            return this;
        }
    }

    @Override
    public FileStoreTable copy(TableSchema newTableSchema) {
        AbstractFileStoreTable copied =
                newTableSchema.primaryKeys().isEmpty()
                        ? new AppendOnlyFileStoreTable(
                                fileIO, tablePathProvider, newTableSchema, catalogEnvironment)
                        : new PrimaryKeyFileStoreTable(
                                fileIO, tablePathProvider, newTableSchema, catalogEnvironment);
        if (snapshotCache != null) {
            copied.setSnapshotCache(snapshotCache);
        }
        if (manifestCache != null) {
            copied.setManifestCache(manifestCache);
        }
        if (statsCache != null) {
            copied.setStatsCache(statsCache);
        }
        return copied;
    }

    @Override
    public SchemaManager schemaManager() {
        return new SchemaManager(fileIO(), tablePathProvider.getTableWritePath(), currentBranch());
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
        return tablePathProvider.getTableWritePath();
    }

    @Override
    public TablePathProvider pathProvider() {
        return tablePathProvider;
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
                snapshotManager(), store().newSnapshotDeletion(), store().newTagManager());
    }

    @Override
    public ExpireSnapshots newExpireChangelog() {
        return new ExpireChangelogImpl(
                snapshotManager(), tagManager(), store().newChangelogDeletion());
    }

    @Override
    public TableCommitImpl newCommit(String commitUser) {
        CoreOptions options = coreOptions();
        Runnable snapshotExpire = null;
        if (!options.writeOnly()) {
            boolean changelogDecoupled = options.changelogLifecycleDecoupled();
            ExpireConfig expireConfig = options.expireConfig();
            ExpireSnapshots expireChangelog = newExpireChangelog().config(expireConfig);
            ExpireSnapshots expireSnapshots = newExpireSnapshots().config(expireConfig);
            snapshotExpire =
                    () -> {
                        expireSnapshots.expire();
                        if (changelogDecoupled) {
                            expireChangelog.expire();
                        }
                    };
        }

        return new TableCommitImpl(
                store().newCommit(commitUser, createCommitCallbacks(commitUser)),
                snapshotExpire,
                options.writeOnly() ? null : store().newPartitionExpire(commitUser),
                options.writeOnly() ? null : store().newTagCreationManager(),
                catalogEnvironment.lockFactory().create(),
                CoreOptions.fromMap(options()).consumerExpireTime(),
                new ConsumerManager(
                        fileIO, tablePathProvider.getTableWritePath(), snapshotManager().branch()),
                options.snapshotExpireExecutionMode(),
                name(),
                options.forceCreatingSnapshot());
    }

    protected List<CommitCallback> createCommitCallbacks(String commitUser) {
        List<CommitCallback> callbacks =
                new ArrayList<>(CallbackUtils.loadCommitCallbacks(coreOptions()));
        CoreOptions options = coreOptions();
        MetastoreClient.Factory metastoreClientFactory =
                catalogEnvironment.metastoreClientFactory();

        if (options.partitionedTableInMetastore()
                && metastoreClientFactory != null
                && !tableSchema.partitionKeys().isEmpty()) {
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

    public Snapshot findSnapshot(long fromSnapshotId) throws SnapshotNotExistException {
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

        SnapshotNotExistException.checkNotNull(
                snapshot,
                String.format(
                        "Cannot create tag because given snapshot #%s doesn't exist.",
                        fromSnapshotId));

        return snapshot;
    }

    @Override
    public void createTag(String tagName, long fromSnapshotId) {
        createTag(tagName, findSnapshot(fromSnapshotId), coreOptions().tagDefaultTimeRetained());
    }

    @Override
    public void createTag(String tagName, long fromSnapshotId, Duration timeRetained) {
        createTag(tagName, findSnapshot(fromSnapshotId), timeRetained);
    }

    @Override
    public void createTag(String tagName) {
        Snapshot latestSnapshot = snapshotManager().latestSnapshot();
        SnapshotNotExistException.checkNotNull(
                latestSnapshot, "Cannot create tag because latest snapshot doesn't exist.");
        createTag(tagName, latestSnapshot, coreOptions().tagDefaultTimeRetained());
    }

    @Override
    public void createTag(String tagName, Duration timeRetained) {
        Snapshot latestSnapshot = snapshotManager().latestSnapshot();
        SnapshotNotExistException.checkNotNull(
                latestSnapshot, "Cannot create tag because latest snapshot doesn't exist.");
        createTag(tagName, latestSnapshot, timeRetained);
    }

    private void createTag(String tagName, Snapshot fromSnapshot, @Nullable Duration timeRetained) {
        tagManager().createTag(fromSnapshot, tagName, timeRetained, store().createTagCallbacks());
    }

    @Override
    public void renameTag(String tagName, String targetTagName) {
        tagManager().renameTag(tagName, targetTagName);
    }

    @Override
    public void replaceTag(
            String tagName, @Nullable Long fromSnapshotId, @Nullable Duration timeRetained) {
        if (fromSnapshotId == null) {
            Snapshot latestSnapshot = snapshotManager().latestSnapshot();
            SnapshotNotExistException.checkNotNull(
                    latestSnapshot, "Cannot replace tag because latest snapshot doesn't exist.");
            tagManager().replaceTag(latestSnapshot, tagName, timeRetained);
        } else {
            tagManager().replaceTag(findSnapshot(fromSnapshotId), tagName, timeRetained);
        }
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
    public void createBranch(String branchName, String tagName) {
        branchManager().createBranch(branchName, tagName);
    }

    @Override
    public void deleteBranch(String branchName) {
        branchManager().deleteBranch(branchName);
    }

    @Override
    public void fastForward(String branchName) {
        branchManager().fastForward(branchName);
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
                fileIO.writeFile(
                        snapshotManager().snapshotPath(taggedSnapshot.id()),
                        fileIO.readFileUtf8(tagManager.tagPath(tagName)),
                        false);
                snapshotManager.commitEarliestHint(taggedSnapshot.id());
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public TagManager tagManager() {
        return new TagManager(fileIO, tablePathProvider.getTableWritePath(), currentBranch());
    }

    @Override
    public BranchManager branchManager() {
        return new BranchManager(
                fileIO,
                tablePathProvider.getTableWritePath(),
                snapshotManager(),
                tagManager(),
                schemaManager());
    }

    @Override
    public FileStoreTable switchToBranch(String branchName) {
        String currentBranch = BranchManager.normalizeBranch(currentBranch());
        String targetBranch = BranchManager.normalizeBranch(branchName);
        if (currentBranch.equals(targetBranch)) {
            return this;
        }

        Optional<TableSchema> optionalSchema =
                new SchemaManager(fileIO(), location(), targetBranch).latest();
        Preconditions.checkArgument(
                optionalSchema.isPresent(), "Branch " + targetBranch + " does not exist");

        TableSchema branchSchema = optionalSchema.get();
        Options branchOptions = new Options(branchSchema.options());
        branchOptions.set(CoreOptions.BRANCH, targetBranch);
        branchSchema = branchSchema.copy(branchOptions.toMap());
        return FileStoreTableFactory.create(
                fileIO(), location(), branchSchema, new Options(), catalogEnvironment());
    }

    private RollbackHelper rollbackHelper() {
        return new RollbackHelper(
                snapshotManager(),
                tagManager(),
                fileIO,
                store().newSnapshotDeletion(),
                store().newChangelogDeletion(),
                store().newTagDeletion());
    }

    protected RowKindGenerator rowKindGenerator() {
        return RowKindGenerator.create(schema(), store().options());
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
        return Objects.equals(tablePathProvider, that.tablePathProvider)
                && Objects.equals(tableSchema, that.tableSchema);
    }
}
