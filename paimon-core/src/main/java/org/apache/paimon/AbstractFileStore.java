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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.DeletionVectorsIndexFile;
import org.apache.paimon.fs.ExternalPathProvider;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.HashIndexFile;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.manifest.IndexManifestFile;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.metastore.AddPartitionTagCallback;
import org.apache.paimon.metastore.MetastoreClient;
import org.apache.paimon.operation.ChangelogDeletion;
import org.apache.paimon.operation.FileStoreCommitImpl;
import org.apache.paimon.operation.ManifestsReader;
import org.apache.paimon.operation.PartitionExpire;
import org.apache.paimon.operation.SnapshotDeletion;
import org.apache.paimon.operation.TagDeletion;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.partition.PartitionExpireStrategy;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.service.ServiceManager;
import org.apache.paimon.stats.StatsFile;
import org.apache.paimon.stats.StatsFileHandler;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.sink.CallbackUtils;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.table.sink.TagCallback;
import org.apache.paimon.tag.TagAutoManager;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
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
    private final CatalogEnvironment catalogEnvironment;

    @Nullable private final SegmentsCache<Path> writeManifestCache;
    @Nullable private SegmentsCache<Path> readManifestCache;
    @Nullable private Cache<Path, Snapshot> snapshotCache;
    private final ExternalPathProvider externalPathProvider;

    protected AbstractFileStore(
            FileIO fileIO,
            SchemaManager schemaManager,
            TableSchema schema,
            String tableName,
            CoreOptions options,
            RowType partitionType,
            CatalogEnvironment catalogEnvironment,
            ExternalPathProvider externalPathProvider) {
        this.fileIO = fileIO;
        this.schemaManager = schemaManager;
        this.schema = schema;
        this.tableName = tableName;
        this.options = options;
        this.partitionType = partitionType;
        this.catalogEnvironment = catalogEnvironment;
        this.writeManifestCache =
                SegmentsCache.create(
                        options.pageSize(), options.writeManifestCache(), Long.MAX_VALUE);
        this.externalPathProvider = externalPathProvider;
    }

    @Override
    public FileStorePathFactory pathFactory() {
        return pathFactory(options.fileFormatString());
    }

    protected FileStorePathFactory pathFactory(String format) {
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
                externalPathProvider);
    }

    @Override
    public SnapshotManager snapshotManager() {
        return new SnapshotManager(fileIO, options.path(), options.branch(), snapshotCache);
    }

    @Override
    public ManifestFile.Factory manifestFileFactory() {
        return manifestFileFactory(false);
    }

    protected ManifestFile.Factory manifestFileFactory(boolean forWrite) {
        return new ManifestFile.Factory(
                fileIO,
                schemaManager,
                partitionType,
                options.manifestFormat(),
                options.manifestCompression(),
                pathFactory(),
                options.manifestTargetSize().getBytes(),
                forWrite ? writeManifestCache : readManifestCache);
    }

    @Override
    public ManifestList.Factory manifestListFactory() {
        return manifestListFactory(false);
    }

    protected ManifestList.Factory manifestListFactory(boolean forWrite) {
        return new ManifestList.Factory(
                fileIO,
                options.manifestFormat(),
                options.manifestCompression(),
                pathFactory(),
                forWrite ? writeManifestCache : readManifestCache);
    }

    @Override
    public IndexManifestFile.Factory indexManifestFileFactory() {
        return new IndexManifestFile.Factory(
                fileIO,
                options.manifestFormat(),
                options.manifestCompression(),
                pathFactory(),
                readManifestCache);
    }

    @Override
    public IndexFileHandler newIndexFileHandler() {
        return new IndexFileHandler(
                snapshotManager(),
                pathFactory().indexFileFactory(),
                indexManifestFileFactory().create(),
                new HashIndexFile(fileIO, pathFactory().indexFileFactory()),
                new DeletionVectorsIndexFile(
                        fileIO,
                        pathFactory().indexFileFactory(),
                        bucketMode() == BucketMode.BUCKET_UNAWARE
                                ? options.deletionVectorIndexFileTargetSize()
                                : MemorySize.ofBytes(Long.MAX_VALUE)));
    }

    @Override
    public StatsFileHandler newStatsFileHandler() {
        return new StatsFileHandler(
                snapshotManager(),
                schemaManager,
                new StatsFile(fileIO, pathFactory().statsFileFactory()));
    }

    protected ManifestsReader newManifestsReader(boolean forWrite) {
        return new ManifestsReader(partitionType, snapshotManager(), manifestListFactory(forWrite));
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
    public FileStoreCommitImpl newCommit(String commitUser) {
        return newCommit(commitUser, Collections.emptyList());
    }

    @Override
    public FileStoreCommitImpl newCommit(String commitUser, List<CommitCallback> callbacks) {
        return new FileStoreCommitImpl(
                fileIO,
                schemaManager,
                tableName,
                commitUser,
                partitionType,
                options,
                options.partitionDefaultName(),
                pathFactory(),
                snapshotManager(),
                manifestFileFactory(),
                manifestListFactory(),
                indexManifestFileFactory(),
                newScan(),
                options.bucket(),
                options.manifestTargetSize(),
                options.manifestFullCompactionThresholdSize(),
                options.manifestMergeMinCount(),
                partitionType.getFieldCount() > 0 && options.dynamicPartitionOverwrite(),
                newKeyComparator(),
                options.branch(),
                newStatsFileHandler(),
                bucketMode(),
                options.scanManifestParallelism(),
                callbacks,
                options.commitMaxRetries(),
                options.commitTimeout());
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
                options.deleteFileThreadNum());
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
                options.deleteFileThreadNum());
    }

    @Override
    public TagManager newTagManager() {
        return new TagManager(fileIO, options.path());
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
                options.deleteFileThreadNum());
    }

    public abstract Comparator<InternalRow> newKeyComparator();

    @Override
    @Nullable
    public PartitionExpire newPartitionExpire(String commitUser) {
        Duration partitionExpireTime = options.partitionExpireTime();
        if (partitionExpireTime == null || partitionType().getFieldCount() == 0) {
            return null;
        }

        MetastoreClient.Factory metastoreClientFactory =
                catalogEnvironment.metastoreClientFactory();
        MetastoreClient metastoreClient = null;
        if (options.partitionedTableInMetastore() && metastoreClientFactory != null) {
            metastoreClient = metastoreClientFactory.create();
        }

        return new PartitionExpire(
                partitionExpireTime,
                options.partitionExpireCheckInterval(),
                PartitionExpireStrategy.createPartitionExpireStrategy(options, partitionType()),
                newScan(),
                newCommit(commitUser),
                metastoreClient,
                options.endInputCheckPartitionExpire(),
                options.partitionExpireMaxNum());
    }

    @Override
    public TagAutoManager newTagCreationManager() {
        return TagAutoManager.create(
                options,
                snapshotManager(),
                newTagManager(),
                newTagDeletion(),
                createTagCallbacks());
    }

    @Override
    public List<TagCallback> createTagCallbacks() {
        List<TagCallback> callbacks = new ArrayList<>(CallbackUtils.loadTagCallbacks(options));
        String partitionField = options.tagToPartitionField();
        MetastoreClient.Factory metastoreClientFactory =
                catalogEnvironment.metastoreClientFactory();
        if (partitionField != null && metastoreClientFactory != null) {
            callbacks.add(
                    new AddPartitionTagCallback(metastoreClientFactory.create(), partitionField));
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
}
