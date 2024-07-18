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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.index.HashIndexFile;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.manifest.IndexManifestFile;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.metastore.AddPartitionTagCallback;
import org.apache.paimon.metastore.MetastoreClient;
import org.apache.paimon.operation.ChangelogDeletion;
import org.apache.paimon.operation.FileStoreCommitImpl;
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
import org.apache.paimon.table.sink.TagCallback;
import org.apache.paimon.tag.TagAutoManager;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.SegmentsCache;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
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
    protected final CoreOptions options;
    protected final RowType partitionType;
    private final CatalogEnvironment catalogEnvironment;

    @Nullable private final SegmentsCache<String> writeManifestCache;

    protected AbstractFileStore(
            FileIO fileIO,
            SchemaManager schemaManager,
            TableSchema schema,
            CoreOptions options,
            RowType partitionType,
            CatalogEnvironment catalogEnvironment) {
        this.fileIO = fileIO;
        this.schemaManager = schemaManager;
        this.schema = schema;
        this.options = options;
        this.partitionType = partitionType;
        this.catalogEnvironment = catalogEnvironment;
        MemorySize writeManifestCache = options.writeManifestCache();
        this.writeManifestCache =
                writeManifestCache.getBytes() == 0
                        ? null
                        : new SegmentsCache<>(options.pageSize(), writeManifestCache);
    }

    @Override
    public FileStorePathFactory pathFactory() {
        return new FileStorePathFactory(
                options.path(),
                partitionType,
                options.partitionDefaultName(),
                options.fileFormat().getFormatIdentifier());
    }

    @Override
    public SnapshotManager snapshotManager() {
        return new SnapshotManager(fileIO, options.path(), options.branch());
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
                forWrite ? writeManifestCache : null);
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
                forWrite ? writeManifestCache : null);
    }

    protected IndexManifestFile.Factory indexManifestFileFactory() {
        return new IndexManifestFile.Factory(
                fileIO, options.manifestFormat(), options.manifestCompression(), pathFactory());
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
        return new FileStoreCommitImpl(
                fileIO,
                schemaManager,
                commitUser,
                partitionType,
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
                options.scanManifestParallelism());
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
                metastoreClient);
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
}
