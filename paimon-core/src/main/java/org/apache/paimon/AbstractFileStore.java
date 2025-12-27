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

package org.apache.paimon;

import org.apache.paimon.CoreOptions.ExternalPathStrategy;
import org.apache.paimon.catalog.RenamingSnapshotCommit;
import org.apache.paimon.catalog.SnapshotCommit;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.globalindex.GlobalIndexScanBuilder;
import org.apache.paimon.globalindex.GlobalIndexScanBuilderImpl;
import org.apache.paimon.iceberg.IcebergCommitCallback;
import org.apache.paimon.iceberg.IcebergOptions;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.manifest.IndexManifestFile;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.metastore.AddPartitionCommitCallback;
import org.apache.paimon.metastore.AddPartitionTagCallback;
import org.apache.paimon.metastore.TagPreviewCommitCallback;
import org.apache.paimon.operation.ChangelogDeletion;
import org.apache.paimon.operation.FileStoreCommitImpl;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.operation.ManifestsReader;
import org.apache.paimon.operation.PartitionExpire;
import org.apache.paimon.operation.SnapshotDeletion;
import org.apache.paimon.operation.TagDeletion;
import org.apache.paimon.operation.commit.ConflictDetection;
import org.apache.paimon.partition.PartitionExpireStrategy;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.service.ServiceManager;
import org.apache.paimon.stats.StatsFile;
import org.apache.paimon.stats.StatsFileHandler;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PartitionHandler;
import org.apache.paimon.table.sink.CallbackUtils;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.table.sink.TagCallback;
import org.apache.paimon.tag.SuccessFileTagCallback;
import org.apache.paimon.tag.TagAutoManager;
import org.apache.paimon.tag.TagPreview;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.IndexFilePathFactories;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.SegmentsCache;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.apache.paimon.catalog.Identifier.DEFAULT_MAIN_BRANCH;
import static org.apache.paimon.partition.PartitionExpireStrategy.createPartitionExpireStrategy;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Base {@link FileStore} implementation.
 *
 * @param <T> type of record to read and write.
 */
abstract class AbstractFileStore<T> implements FileStore<T> {

    protected final FileIO fileIO;
    protected final SchemaManager schemaManager;
    protected final TableSchema schema;
    protected final String tableName;
    protected final CoreOptions options;
    protected final RowType partitionType;
    protected final CatalogEnvironment catalogEnvironment;

    @Nullable private SegmentsCache<Path> readManifestCache;
    @Nullable private Cache<Path, Snapshot> snapshotCache;

    protected AbstractFileStore(
            FileIO fileIO,
            SchemaManager schemaManager,
            TableSchema schema,
            String tableName,
            CoreOptions options,
            RowType partitionType,
            CatalogEnvironment catalogEnvironment) {
        this.fileIO = fileIO;
        this.schemaManager = schemaManager;
        this.schema = schema;
        this.tableName = tableName;
        this.options = options;
        this.partitionType = partitionType;
        this.catalogEnvironment = catalogEnvironment;
    }

    @Override
    public FileStorePathFactory pathFactory() {
        return pathFactory(options, options.fileFormatString());
    }

    protected FileStorePathFactory pathFactory(CoreOptions options, String format) {
        return new FileStorePathFactory(
                options.path(),
                partitionType,
                options.partitionDefaultName(),
                format,
                options.dataFilePrefix(),
                options.changelogFilePrefix(),
                options.legacyPartitionName(),
                options.fileSuffixIncludeCompression(),
                options.fileCompression(),
                options.dataFilePathDirectory(),
                createExternalPaths(),
                options.externalPathStrategy(),
                options.indexFileInDataFileDir());
    }

    private List<Path> createExternalPaths() {
        String externalPaths = options.dataFileExternalPaths();
        ExternalPathStrategy strategy = options.externalPathStrategy();
        if (externalPaths == null
                || externalPaths.isEmpty()
                || strategy == ExternalPathStrategy.NONE) {
            return Collections.emptyList();
        }

        String specificFS = options.externalSpecificFS();

        List<Path> paths = new ArrayList<>();
        for (String pathString : externalPaths.split(",")) {
            Path path = new Path(pathString.trim());
            String scheme = path.toUri().getScheme();
            if (scheme == null) {
                throw new IllegalArgumentException("scheme should not be null: " + path);
            }

            if (strategy == ExternalPathStrategy.SPECIFIC_FS) {
                checkArgument(
                        specificFS != null,
                        "External path specificFS should not be null when strategy is specificFS.");
                if (scheme.equalsIgnoreCase(specificFS)) {
                    paths.add(path);
                }
            } else {
                paths.add(path);
            }
        }
        checkArgument(!paths.isEmpty(), "External paths should not be empty");
        return paths;
    }

    @Override
    public SnapshotManager snapshotManager() {
        return new SnapshotManager(
                fileIO,
                options.path(),
                options.branch(),
                catalogEnvironment.snapshotLoader(),
                snapshotCache);
    }

    @Override
    public ChangelogManager changelogManager() {
        return new ChangelogManager(fileIO, options.path(), options.branch());
    }

    @Override
    public ManifestFile.Factory manifestFileFactory() {
        return new ManifestFile.Factory(
                fileIO,
                schemaManager,
                partitionType,
                FileFormat.manifestFormat(options),
                options.manifestCompression(),
                pathFactory(),
                options.manifestTargetSize().getBytes(),
                readManifestCache);
    }

    @Override
    public ManifestList.Factory manifestListFactory() {
        return new ManifestList.Factory(
                fileIO,
                FileFormat.manifestFormat(options),
                options.manifestCompression(),
                pathFactory(),
                readManifestCache);
    }

    @Override
    public IndexManifestFile.Factory indexManifestFileFactory() {
        return new IndexManifestFile.Factory(
                fileIO,
                FileFormat.manifestFormat(options),
                options.manifestCompression(),
                pathFactory(),
                readManifestCache);
    }

    @Override
    public IndexFileHandler newIndexFileHandler() {
        return new IndexFileHandler(
                fileIO,
                snapshotManager(),
                indexManifestFileFactory().create(),
                new IndexFilePathFactories(pathFactory()),
                options.dvIndexFileTargetSize(),
                options.deletionVectorBitmap64());
    }

    @Override
    public StatsFileHandler newStatsFileHandler() {
        return new StatsFileHandler(
                snapshotManager(),
                schemaManager,
                new StatsFile(fileIO, pathFactory().statsFileFactory()));
    }

    protected ManifestsReader newManifestsReader() {
        return new ManifestsReader(
                partitionType,
                options.partitionDefaultName(),
                snapshotManager(),
                manifestListFactory());
    }

    @Override
    public RowType partitionType() {
        return partitionType;
    }

    @Override
    public CoreOptions options() {
        return options;
    }

    @Override
    public boolean mergeSchema(RowType rowType, boolean allowExplicitCast) {
        return schemaManager.mergeSchema(rowType, allowExplicitCast);
    }

    @Override
    public FileStoreCommitImpl newCommit(String commitUser, FileStoreTable table) {
        SnapshotManager snapshotManager = snapshotManager();
        SnapshotCommit snapshotCommit = catalogEnvironment.snapshotCommit(snapshotManager);
        if (snapshotCommit == null) {
            snapshotCommit = new RenamingSnapshotCommit(snapshotManager, Lock.empty());
        }
        ConflictDetection conflictDetection =
                new ConflictDetection(
                        tableName,
                        commitUser,
                        partitionType,
                        pathFactory(),
                        newKeyComparator(),
                        bucketMode(),
                        options.deletionVectorsEnabled(),
                        newIndexFileHandler(),
                        newScan());
        return new FileStoreCommitImpl(
                snapshotCommit,
                fileIO,
                schemaManager,
                tableName,
                commitUser,
                partitionType,
                options,
                options.partitionDefaultName(),
                pathFactory(),
                snapshotManager,
                manifestFileFactory(),
                manifestListFactory(),
                indexManifestFileFactory(),
                newScan(),
                options.bucket(),
                options.manifestTargetSize(),
                options.manifestFullCompactionThresholdSize(),
                options.manifestMergeMinCount(),
                partitionType.getFieldCount() > 0 && options.dynamicPartitionOverwrite(),
                options.branch(),
                newStatsFileHandler(),
                bucketMode(),
                options.scanManifestParallelism(),
                createCommitCallbacks(commitUser, table),
                options.commitMaxRetries(),
                options.commitTimeout(),
                options.commitMinRetryWait(),
                options.commitMaxRetryWait(),
                options.commitStrictModeLastSafeSnapshot().orElse(null),
                options.rowTrackingEnabled(),
                options.commitDiscardDuplicateFiles(),
                conflictDetection);
    }

    @Override
    public SnapshotDeletion newSnapshotDeletion() {
        return new SnapshotDeletion(
                fileIO,
                pathFactory(),
                manifestFileFactory().create(),
                manifestListFactory().create(),
                newIndexFileHandler(),
                newStatsFileHandler(),
                options.changelogProducer() != CoreOptions.ChangelogProducer.NONE,
                options.cleanEmptyDirectories(),
                options.fileOperationThreadNum());
    }

    @Override
    public ChangelogDeletion newChangelogDeletion() {
        return new ChangelogDeletion(
                fileIO,
                pathFactory(),
                manifestFileFactory().create(),
                manifestListFactory().create(),
                newIndexFileHandler(),
                newStatsFileHandler(),
                options.cleanEmptyDirectories(),
                options.fileOperationThreadNum());
    }

    @Override
    public TagManager newTagManager() {
        return new TagManager(fileIO, options.path(), DEFAULT_MAIN_BRANCH, options);
    }

    @Override
    public TagDeletion newTagDeletion() {
        return new TagDeletion(
                fileIO,
                pathFactory(),
                manifestFileFactory().create(),
                manifestListFactory().create(),
                newIndexFileHandler(),
                newStatsFileHandler(),
                options.cleanEmptyDirectories(),
                options.fileOperationThreadNum());
    }

    public abstract Comparator<InternalRow> newKeyComparator();

    @Override
    public InternalRowPartitionComputer partitionComputer() {
        return new InternalRowPartitionComputer(
                options.partitionDefaultName(),
                schema.logicalPartitionType(),
                schema.partitionKeys().toArray(new String[0]),
                options.legacyPartitionName());
    }

    private List<CommitCallback> createCommitCallbacks(String commitUser, FileStoreTable table) {
        List<CommitCallback> callbacks = new ArrayList<>();

        if (options.partitionedTableInMetastore() && !schema.partitionKeys().isEmpty()) {
            PartitionHandler partitionHandler = catalogEnvironment.partitionHandler();
            if (partitionHandler != null) {
                callbacks.add(
                        new AddPartitionCommitCallback(partitionHandler, partitionComputer()));
            }
        }

        TagPreview tagPreview = TagPreview.create(options);
        if (options.tagToPartitionField() != null
                && tagPreview != null
                && schema.partitionKeys().isEmpty()) {
            PartitionHandler partitionHandler = catalogEnvironment.partitionHandler();
            if (partitionHandler != null) {
                TagPreviewCommitCallback callback =
                        new TagPreviewCommitCallback(
                                new AddPartitionTagCallback(
                                        partitionHandler, options.tagToPartitionField()),
                                tagPreview);
                callbacks.add(callback);
            }
        }

        if (options.toConfiguration().get(IcebergOptions.METADATA_ICEBERG_STORAGE)
                != IcebergOptions.StorageType.DISABLED) {
            callbacks.add(new IcebergCommitCallback(table, commitUser));
        }

        callbacks.addAll(CallbackUtils.loadCommitCallbacks(options, table));
        return callbacks;
    }

    @Override
    @Nullable
    public PartitionExpire newPartitionExpire(String commitUser, FileStoreTable table) {
        Duration partitionExpireTime = options.partitionExpireTime();
        if (partitionExpireTime == null || partitionType().getFieldCount() == 0) {
            return null;
        }

        return newPartitionExpire(
                commitUser,
                table,
                partitionExpireTime,
                options.partitionExpireCheckInterval(),
                createPartitionExpireStrategy(
                        options,
                        partitionType(),
                        catalogEnvironment.catalogLoader(),
                        catalogEnvironment.identifier()));
    }

    @Override
    public PartitionExpire newPartitionExpire(
            String commitUser,
            FileStoreTable table,
            Duration expirationTime,
            Duration checkInterval,
            PartitionExpireStrategy expireStrategy) {
        PartitionHandler partitionHandler = null;
        if (options.partitionedTableInMetastore()) {
            partitionHandler = catalogEnvironment.partitionHandler();
        }

        return new PartitionExpire(
                expirationTime,
                checkInterval,
                expireStrategy,
                newScan(),
                newCommit(commitUser, table),
                partitionHandler,
                options.endInputCheckPartitionExpire(),
                options.partitionExpireMaxNum(),
                options.partitionExpireBatchSize());
    }

    @Override
    public TagAutoManager newTagAutoManager(FileStoreTable table) {
        return TagAutoManager.create(
                options,
                snapshotManager(),
                newTagManager(),
                newTagDeletion(),
                createTagCallbacks(table));
    }

    @Override
    public List<TagCallback> createTagCallbacks(FileStoreTable table) {
        List<TagCallback> callbacks = new ArrayList<>(CallbackUtils.loadTagCallbacks(options));
        String partitionField = options.tagToPartitionField();

        if (partitionField != null) {
            PartitionHandler partitionHandler = catalogEnvironment.partitionHandler();
            if (partitionHandler != null) {
                callbacks.add(new AddPartitionTagCallback(partitionHandler, partitionField));
            }
        }
        if (options.tagCreateSuccessFile()) {
            callbacks.add(new SuccessFileTagCallback(fileIO, newTagManager().tagDirectory()));
        }
        if (options.toConfiguration().get(IcebergOptions.METADATA_ICEBERG_STORAGE)
                != IcebergOptions.StorageType.DISABLED) {
            callbacks.add(new IcebergCommitCallback(table, ""));
        }
        return callbacks;
    }

    @Override
    public ServiceManager newServiceManager() {
        return new ServiceManager(fileIO, options.path());
    }

    @Override
    public void setManifestCache(SegmentsCache<Path> manifestCache) {
        this.readManifestCache = manifestCache;
    }

    @Override
    public void setSnapshotCache(Cache<Path, Snapshot> cache) {
        this.snapshotCache = cache;
    }

    @Override
    public GlobalIndexScanBuilder newGlobalIndexScanBuilder() {
        return new GlobalIndexScanBuilderImpl(
                options.toConfiguration(),
                schema.logicalRowType(),
                fileIO,
                pathFactory().globalIndexFileFactory(),
                snapshotManager(),
                newIndexFileHandler());
    }
}
