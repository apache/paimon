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
import org.apache.paimon.globalindex.DataEvolutionBatchScan;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaValidation;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.sink.AppendTableRowKeyExtractor;
import org.apache.paimon.table.sink.DynamicBucketRowKeyExtractor;
import org.apache.paimon.table.sink.FixedBucketRowKeyExtractor;
import org.apache.paimon.table.sink.FixedBucketWriteSelector;
import org.apache.paimon.table.sink.PostponeBucketRowKeyExtractor;
import org.apache.paimon.table.sink.RowKeyExtractor;
import org.apache.paimon.table.sink.RowKindGenerator;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.WriteSelector;
import org.apache.paimon.table.source.DataTableBatchScan;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.DataTableStreamScan;
import org.apache.paimon.table.source.SplitGenerator;
import org.apache.paimon.table.source.StreamDataTableScan;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.table.source.snapshot.SnapshotReaderImpl;
import org.apache.paimon.table.source.snapshot.TimeTravelUtil;
import org.apache.paimon.tag.TagAutoManager;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.CatalogBranchManager;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.DVMetaCache;
import org.apache.paimon.utils.FileSystemBranchManager;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SegmentsCache;
import org.apache.paimon.utils.SimpleFileReader;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.SnapshotNotExistException;
import org.apache.paimon.utils.StringUtils;
import org.apache.paimon.utils.TagManager;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.function.BiConsumer;

import static org.apache.paimon.CoreOptions.PATH;

/** Abstract {@link FileStoreTable}. */
abstract class AbstractFileStoreTable implements FileStoreTable {

    private static final long serialVersionUID = 1L;

    protected final FileIO fileIO;
    protected final Path path;
    protected final TableSchema tableSchema;
    protected final CatalogEnvironment catalogEnvironment;

    @Nullable protected transient SegmentsCache<Path> manifestCache;
    @Nullable protected transient Cache<Path, Snapshot> snapshotCache;
    @Nullable protected transient Cache<String, Statistics> statsCache;
    @Nullable protected transient DVMetaCache dvmetaCache;

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

    public String currentBranch() {
        return CoreOptions.branch(options());
    }

    @Override
    public void setManifestCache(SegmentsCache<Path> manifestCache) {
        this.manifestCache = manifestCache;
        store().setManifestCache(manifestCache);
    }

    @Nullable
    @Override
    public SegmentsCache<Path> getManifestCache() {
        return manifestCache;
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
    public void setDVMetaCache(DVMetaCache cache) {
        this.dvmetaCache = cache;
    }

    @Override
    public Optional<Snapshot> latestSnapshot() {
        Snapshot snapshot = store().snapshotManager().latestSnapshot();
        return Optional.ofNullable(snapshot);
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
        Snapshot snapshot = TimeTravelUtil.tryTravelOrLatest(this);
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
            case POSTPONE_MODE:
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

    protected CatalogEnvironment newCatalogEnvironment(String branch) {
        Identifier identifier = identifier();
        return catalogEnvironment.copy(
                new Identifier(
                        identifier.getDatabaseName(),
                        identifier.getTableName(),
                        branch,
                        identifier.getSystemTableName()));
    }

    public RowKeyExtractor createRowKeyExtractor() {
        switch (bucketMode()) {
            case HASH_FIXED:
                return new FixedBucketRowKeyExtractor(schema());
            case HASH_DYNAMIC:
            case KEY_DYNAMIC:
                return new DynamicBucketRowKeyExtractor(schema());
            case BUCKET_UNAWARE:
                return new AppendTableRowKeyExtractor(schema());
            case POSTPONE_MODE:
                return new PostponeBucketRowKeyExtractor(schema());
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
                changelogManager(),
                splitGenerator(),
                nonPartitionFilterConsumer(),
                store().pathFactory(),
                name(),
                store().newIndexFileHandler(),
                dvmetaCache);
    }

    @Override
    public DataTableScan newScan() {
        DataTableBatchScan scan =
                new DataTableBatchScan(
                        tableSchema,
                        schemaManager(),
                        coreOptions(),
                        newSnapshotReader(),
                        catalogEnvironment.tableQueryAuth(coreOptions()));
        if (coreOptions().dataEvolutionEnabled()) {
            return new DataEvolutionBatchScan(this, scan);
        }
        return scan;
    }

    @Override
    public StreamDataTableScan newStreamScan() {
        return new DataTableStreamScan(
                tableSchema,
                coreOptions(),
                newSnapshotReader(),
                snapshotManager(),
                changelogManager(),
                supportStreamingReadOverwrite(),
                catalogEnvironment.tableQueryAuth(coreOptions()),
                !tableSchema.primaryKeys().isEmpty());
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
                        SchemaManager.checkAlterTableOption(oldOptions, k, oldValue, newValue);
                    }
                });
    }

    protected FileStoreTable copyInternal(
            Map<String, String> dynamicOptions, boolean tryTimeTravel) {
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
                                fileIO, path, newTableSchema, catalogEnvironment)
                        : new PrimaryKeyFileStoreTable(
                                fileIO, path, newTableSchema, catalogEnvironment);
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
        return new SchemaManager(fileIO(), path, currentBranch());
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
    public ChangelogManager changelogManager() {
        return store().changelogManager();
    }

    @Override
    public ExpireSnapshots newExpireSnapshots() {
        return new ExpireSnapshotsImpl(
                snapshotManager(),
                changelogManager(),
                store().newSnapshotDeletion(),
                store().newTagManager());
    }

    @Override
    public ExpireSnapshots newExpireChangelog() {
        return new ExpireChangelogImpl(
                snapshotManager(),
                changelogManager(),
                tagManager(),
                store().newChangelogDeletion());
    }

    @Override
    public TableCommitImpl newCommit(String commitUser) {
        CoreOptions options = coreOptions();
        return new TableCommitImpl(
                store().newCommit(commitUser, this),
                newExpireRunnable(),
                options.writeOnly() ? null : store().newPartitionExpire(commitUser, this),
                options.writeOnly() ? null : store().newTagAutoManager(this),
                options.writeOnly() ? null : CoreOptions.fromMap(options()).consumerExpireTime(),
                new ConsumerManager(fileIO, path, snapshotManager().branch()),
                options.snapshotExpireExecutionMode(),
                name(),
                options.forceCreatingSnapshot(),
                options.fileOperationThreadNum());
    }

    @Override
    public ConsumerManager consumerManager() {
        return new ConsumerManager(fileIO, path, snapshotManager().branch());
    }

    @Nullable
    protected Runnable newExpireRunnable() {
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

        return snapshotExpire;
    }

    private Optional<TableSchema> tryTimeTravel(Options options) {
        Snapshot snapshot;
        try {
            snapshot =
                    TimeTravelUtil.tryTravelToSnapshot(options, snapshotManager(), tagManager())
                            .orElse(null);
        } catch (Exception e) {
            return Optional.empty();
        }
        if (snapshot == null) {
            return Optional.empty();
        }
        return Optional.of(schemaManager().schema(snapshot.schemaId()).copy(options.toMap()));
    }

    @Override
    public void rollbackTo(long snapshotId) {
        SnapshotManager snapshotManager = snapshotManager();
        try {
            snapshotManager.rollback(Instant.snapshot(snapshotId));
        } catch (UnsupportedOperationException e) {
            try {
                Snapshot snapshot = snapshotManager.tryGetSnapshot(snapshotId);
                rollbackHelper().cleanLargerThan(snapshot);
            } catch (FileNotFoundException ex) {
                // try to get snapshot from tag
                TagManager tagManager = tagManager();
                SortedMap<Snapshot, List<String>> tags = tagManager.tags();
                for (Map.Entry<Snapshot, List<String>> entry : tags.entrySet()) {
                    if (entry.getKey().id() == snapshotId) {
                        rollbackTo(entry.getValue().get(0));
                        return;
                    }
                }
                throw new IllegalArgumentException(
                        String.format("Rollback snapshot '%s' doesn't exist.", snapshotId), ex);
            }
        }
    }

    @Override
    public void rollbackTo(String tagName) {
        SnapshotManager snapshotManager = snapshotManager();
        try {
            snapshotManager.rollback(Instant.tag(tagName));
        } catch (UnsupportedOperationException e) {
            Snapshot taggedSnapshot = tagManager().getOrThrow(tagName).trimToSnapshot();
            RollbackHelper rollbackHelper = rollbackHelper();
            rollbackHelper.cleanLargerThan(taggedSnapshot);
            rollbackHelper.createSnapshotFileIfNeeded(taggedSnapshot);
        }
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
    public TagAutoManager newTagAutoManager() {
        return store().newTagAutoManager(this);
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
        tagManager()
                .createTag(
                        fromSnapshot,
                        tagName,
                        timeRetained,
                        store().createTagCallbacks(this),
                        false);
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
            tagManager()
                    .replaceTag(
                            latestSnapshot,
                            tagName,
                            timeRetained,
                            store().createTagCallbacks(this));
        } else {
            tagManager()
                    .replaceTag(
                            findSnapshot(fromSnapshotId),
                            tagName,
                            timeRetained,
                            store().createTagCallbacks(this));
        }
    }

    @Override
    public void deleteTag(String tagName) {
        tagManager()
                .deleteTag(
                        tagName,
                        store().newTagDeletion(),
                        snapshotManager(),
                        store().createTagCallbacks(this));
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
        String fallbackBranch =
                coreOptions().toConfiguration().get(CoreOptions.SCAN_FALLBACK_BRANCH);
        if (!StringUtils.isNullOrWhitespaceOnly(fallbackBranch)
                && branchName.equals(fallbackBranch)) {
            throw new IllegalArgumentException(
                    String.format(
                            "can not delete the fallback branch. "
                                    + "branchName to be deleted is %s. you have set 'scan.fallback-branch' = '%s'. "
                                    + "you should reset 'scan.fallback-branch' before deleting this branch.",
                            branchName, fallbackBranch));
        }

        branchManager().dropBranch(branchName);
    }

    @Override
    public void fastForward(String branchName) {
        branchManager().fastForward(branchName);
    }

    @Override
    public TagManager tagManager() {
        return new TagManager(fileIO, path, currentBranch(), coreOptions());
    }

    @Override
    public BranchManager branchManager() {
        if (catalogEnvironment.catalogLoader() != null
                && catalogEnvironment.supportsVersionManagement()) {
            return new CatalogBranchManager(catalogEnvironment.catalogLoader(), identifier());
        }
        return new FileSystemBranchManager(
                fileIO, path, snapshotManager(), tagManager(), schemaManager());
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
                fileIO(),
                location(),
                branchSchema,
                new Options(),
                newCatalogEnvironment(targetBranch));
    }

    private RollbackHelper rollbackHelper() {
        return new RollbackHelper(snapshotManager(), changelogManager(), tagManager(), fileIO);
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
        return Objects.equals(path, that.path) && Objects.equals(tableSchema, that.tableSchema);
    }
}
