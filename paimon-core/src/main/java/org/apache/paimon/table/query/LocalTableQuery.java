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

package org.apache.paimon.table.query;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.FileStore;
import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.deletionvectors.DeletionVectorsIndexFile;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.LookupStoreFactory;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.mergetree.Levels;
import org.apache.paimon.mergetree.LookupFile;
import org.apache.paimon.mergetree.LookupLevels;
import org.apache.paimon.mergetree.lookup.LookupSerializerFactory;
import org.apache.paimon.mergetree.lookup.PersistProcessor;
import org.apache.paimon.mergetree.lookup.PersistValueAndPosProcessor;
import org.apache.paimon.mergetree.lookup.PersistValueProcessor;
import org.apache.paimon.mergetree.lookup.PositionedKeyValue;
import org.apache.paimon.mergetree.lookup.RemoteLookupFileManager;
import org.apache.paimon.operation.metrics.PartialLookupMetrics;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.OutOfRangeException;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.KeyComparatorSupplier;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import static org.apache.paimon.lookup.LookupStoreFactory.bloomFilterBuilderFactory;
import static org.apache.paimon.mergetree.LookupFile.localFilePrefix;

/** Implementation for {@link TableQuery} for caching data and file in local. */
public class LocalTableQuery implements TableQuery {

    private final Map<BinaryRow, Map<Integer, BucketLookupState>> tableView;

    private final CoreOptions options;

    private final Supplier<Comparator<InternalRow>> keyComparatorSupplier;

    private final KeyValueFileReaderFactory.Builder readerFactoryBuilder;

    private final LookupStoreFactory lookupStoreFactory;

    private final int startLevel;

    private final boolean deletionVectorsEnabled;

    @Nullable private final IndexFileHandler indexFileHandler;

    @Nullable private final SnapshotManager snapshotManager;

    private final Object deletionVectorCacheLock = new Object();

    private long cachedDeletionVectorSnapshotId = Long.MIN_VALUE;

    @Nullable private List<IndexManifestEntry> cachedDeletionVectorEntries;

    private IOManager ioManager;

    @Nullable private volatile Cache<String, LookupFile> lookupFileCache;

    private final RowType rowType;
    private final RowType partitionType;
    private final FileIO fileIO;

    @Nullable private Filter<InternalRow> cacheRowFilter;

    @Nullable private PartialLookupMetrics partialLookupMetrics;

    public LocalTableQuery(FileStoreTable table) {
        this.options = table.coreOptions();
        if (options.queryAuthEnabled()) {
            // the lookup cache serves rows straight from the store, past the auth reader
            throw new UnsupportedOperationException(
                    "Local table query is not supported on a query-auth table: it returns raw "
                            + "values, outside row filters and column masks.");
        }
        this.tableView = new ConcurrentHashMap<>();
        FileStore<?> tableStore = table.store();
        if (!(tableStore instanceof KeyValueFileStore)) {
            throw new UnsupportedOperationException(
                    "Table Query only supports table with primary key.");
        }
        KeyValueFileStore store = (KeyValueFileStore) tableStore;

        this.readerFactoryBuilder = store.newReaderFactoryBuilder();
        this.rowType = table.schema().logicalRowType();
        this.partitionType = table.schema().logicalPartitionType();
        this.fileIO = table.fileIO();
        RowType keyType = readerFactoryBuilder.keyType();
        this.keyComparatorSupplier = new KeyComparatorSupplier(readerFactoryBuilder.keyType());
        this.lookupStoreFactory =
                LookupStoreFactory.create(
                        options,
                        new CacheManager(
                                options.lookupCacheMaxMemory(),
                                options.lookupCacheHighPrioPoolRatio()),
                        new RowCompactedSerializer(keyType).createSliceComparator());
        // needLookup() is also true for deletion vectors, but that flag is the compaction lookup
        // which starts above the level being merged. A point query has to see level 0, where an
        // uncompacted delete still lives, only when that level-0 row is already the merged value.
        // Partial-update, aggregation and sequence fields keep the historical level: a level-0
        // record is unmerged, and returning it skips the merge function.
        this.deletionVectorsEnabled = options.deletionVectorsEnabled();
        if (deletionVectorsEnabled) {
            this.indexFileHandler = table.store().newIndexFileHandler();
            this.snapshotManager = table.snapshotManager();
        } else {
            this.indexFileHandler = null;
            this.snapshotManager = null;
        }
        boolean level0RowIsComplete =
                options.mergeEngine() == CoreOptions.MergeEngine.DEDUPLICATE
                        && options.sequenceField().isEmpty();
        if (deletionVectorsEnabled && level0RowIsComplete) {
            this.startLevel = 0;
        } else {
            this.startLevel = options.needLookup() ? 1 : 0;
        }
    }

    /**
     * Installs {@code dataFiles} and drops {@code beforeFiles} for one bucket.
     *
     * @param snapshotId snapshot that produced these files. Deletion vectors are loaded from this
     *     snapshot so a newer commit cannot hide or reveal rows in the files being installed. A
     *     negative id means the files are not committed yet and no deletion vector is applied.
     */
    public void refreshFiles(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> beforeFiles,
            List<DataFileMeta> dataFiles,
            long snapshotId) {
        // Both tableView and its nested bucket maps are ConcurrentHashMaps; this nested
        // computeIfAbsent pattern relies on each map providing atomic insertion.
        BucketLookupState state =
                tableView
                        .computeIfAbsent(partition, k -> new ConcurrentHashMap<>())
                        .computeIfAbsent(bucket, k -> new BucketLookupState());
        state.lock.writeLock().lock();
        try {
            // Load before touching the levels so a failure leaves the previous view intact.
            // Compaction can mark a row deleted without rewriting its data file. Streaming
            // plans leave deletion files empty (SnapshotReaderImpl fills them for batch plans
            // only), and the updated file may not appear in this split at all, so the bucket
            // index is read from snapshotId rather than the latest snapshot. Unchanged index
            // files are not read again.
            Map<String, DeletionVector> deletionVectors =
                    loadDeletionVectors(partition, bucket, snapshotId, state);
            if (state.lookupLevels == null) {
                // Initial phase: ignore beforeFiles as they represent deletions from previous state
                state.lookupLevels = createLookupLevels(partition, bucket, dataFiles);
            } else {
                state.lookupLevels.getLevels().update(beforeFiles, dataFiles);
            }
            state.deletionVectors = deletionVectors;
        } finally {
            state.lock.writeLock().unlock();
        }
    }

    private Map<String, DeletionVector> loadDeletionVectors(
            BinaryRow partition, int bucket, long snapshotId, BucketLookupState state) {
        if (!deletionVectorsEnabled) {
            return Collections.emptyMap();
        }
        List<IndexFileMeta> fileMetas = new ArrayList<>();
        for (IndexManifestEntry entry : deletionVectorEntries(snapshotId)) {
            if (entry.bucket() == bucket && entry.partition().equals(partition)) {
                fileMetas.add(entry.indexFile());
            }
        }
        if (fileMetas.equals(state.deletionVectorFiles)) {
            return state.deletionVectors;
        }
        if (fileMetas.isEmpty()) {
            state.deletionVectorFiles = fileMetas;
            return Collections.emptyMap();
        }
        // Record the file list only after the read succeeds. A failure leaves the previous
        // list and vectors in place, so the next refresh retries instead of trusting them.
        Map<String, DeletionVector> loaded =
                indexFileHandler.readAllDeletionVectors(partition, bucket, fileMetas);
        state.deletionVectorFiles = fileMetas;
        return loaded;
    }

    private List<IndexManifestEntry> deletionVectorEntries(long snapshotId) {
        synchronized (deletionVectorCacheLock) {
            if (snapshotId != cachedDeletionVectorSnapshotId) {
                cachedDeletionVectorEntries =
                        snapshotId < 0
                                ? Collections.emptyList()
                                : indexFileHandler.scan(
                                        snapshotOf(snapshotId),
                                        DeletionVectorsIndexFile.DELETION_VECTORS_INDEX);
                cachedDeletionVectorSnapshotId = snapshotId;
            }
            return cachedDeletionVectorEntries;
        }
    }

    private Snapshot snapshotOf(long snapshotId) {
        try {
            return snapshotManager.tryGetSnapshot(snapshotId);
        } catch (FileNotFoundException e) {
            // The files were planned from a snapshot that has since expired. Serving them without
            // their deletion vectors would resurrect deleted rows, so fail with the exception the
            // lookup function already handles by reopening against the current snapshot.
            throw new OutOfRangeException(
                    String.format(
                            "The snapshot %d that produced these files has expired, so its "
                                    + "deletion vectors can no longer be read. Lookup has to "
                                    + "restart from a live snapshot.",
                            snapshotId));
        }
    }

    private LookupLevels<?> createLookupLevels(
            BinaryRow partition, int bucket, List<DataFileMeta> dataFiles) {
        Levels levels = new Levels(keyComparatorSupplier.get(), dataFiles, options.numLevels());
        // Do not apply the deletion vector while building the lookup file. Dropping the key here
        // would make lookup continue into an older level and return a stale value. The vector is
        // checked against the stored row position at lookup time, including after a later refresh.
        KeyValueFileReaderFactory factory =
                readerFactoryBuilder.build(partition, bucket, DeletionVector.emptyFactory());
        if (deletionVectorsEnabled) {
            return newLookupLevels(
                    partition,
                    bucket,
                    levels,
                    factory,
                    PersistValueAndPosProcessor.factory(readerFactoryBuilder.readValueType()));
        }

        LookupLevels<KeyValue> lookupLevels =
                newLookupLevels(
                        partition,
                        bucket,
                        levels,
                        factory,
                        PersistValueProcessor.factory(readerFactoryBuilder.readValueType()));

        // Optimization - download lookup files if already persisted to object store.
        // This branch builds value-only files, so remote files are used when:
        // 1. lookup.remote-file.enabled is true, so compaction persisted them
        // 2. deletion vectors are disabled. Their SSTables store a row position beside the
        //    value, which these remote files do not contain
        // 3. the client reads the full row. Remote files are written at compaction time with
        //    the whole value; projecting after a full download reads more data than it saves
        boolean fullValueRead = readerFactoryBuilder.readValueType().equals(rowType);
        if (this.options.lookupRemoteFileEnabled() && fullValueRead) {
            // Calling the constructor tells `lookupLevels` to load remote files
            new RemoteLookupFileManager<>(
                    fileIO,
                    factory.pathFactory(),
                    lookupLevels,
                    this.options.lookupRemoteLevelThreshold());
        }

        return lookupLevels;
    }

    private <T> LookupLevels<T> newLookupLevels(
            BinaryRow partition,
            int bucket,
            Levels levels,
            KeyValueFileReaderFactory factory,
            PersistProcessor.Factory<T> processorFactory) {
        Options lookupOptions = this.options.toConfiguration();
        RowType readValueType = readerFactoryBuilder.readValueType();
        return new LookupLevels<>(
                schemaId -> readValueType,
                0L,
                levels,
                keyComparatorSupplier.get(),
                readerFactoryBuilder.keyType(),
                processorFactory,
                LookupSerializerFactory.INSTANCE.get(),
                file -> {
                    RecordReader<KeyValue> reader = factory.createRecordReader(file);
                    if (cacheRowFilter != null) {
                        reader = reader.filter(keyValue -> cacheRowFilter.test(keyValue.value()));
                    }
                    return reader;
                },
                file ->
                        Preconditions.checkNotNull(ioManager, "IOManager is required.")
                                .createChannel(
                                        localFilePrefix(partitionType, partition, bucket, file))
                                .getPathFile(),
                lookupStoreFactory,
                bloomFilterBuilderFactory(lookupOptions),
                lookupFileCache(lookupOptions));
    }

    private Cache<String, LookupFile> lookupFileCache(Options options) {
        Cache<String, LookupFile> cache = lookupFileCache;
        if (cache == null) {
            synchronized (this) {
                cache = lookupFileCache;
                if (cache == null) {
                    cache =
                            LookupFile.createCache(
                                    options.get(CoreOptions.LOOKUP_CACHE_FILE_RETENTION),
                                    options.get(CoreOptions.LOOKUP_CACHE_MAX_DISK_SIZE));
                    lookupFileCache = cache;
                }
            }
        }
        return cache;
    }

    @Nullable
    @Override
    public InternalRow lookup(BinaryRow partition, int bucket, InternalRow key) throws IOException {
        PartialLookupMetrics currentMetrics = partialLookupMetrics;
        LookupLevels.LookupContext context =
                currentMetrics == null ? null : new LookupLevels.LookupContext();
        try {
            return lookup(partition, bucket, key, context);
        } finally {
            if (currentMetrics != null) {
                currentMetrics.reportLookup(context != null && context.remoteAccessed());
            }
        }
    }

    @Nullable
    private InternalRow lookup(
            BinaryRow partition,
            int bucket,
            InternalRow key,
            @Nullable LookupLevels.LookupContext context)
            throws IOException {
        Map<Integer, BucketLookupState> buckets = tableView.get(partition);
        if (buckets == null || buckets.isEmpty()) {
            return null;
        }
        BucketLookupState state = buckets.get(bucket);
        if (state == null) {
            return null;
        }

        state.lock.readLock().lock();
        try {
            LookupLevels<?> lookupLevels = state.lookupLevels;
            if (lookupLevels == null) {
                return null;
            }

            Object lookedUp = lookup(lookupLevels, key, context);
            return visibleValue(lookedUp, state.deletionVectors);
        } finally {
            state.lock.readLock().unlock();
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Object lookup(
            LookupLevels<?> lookupLevels,
            InternalRow key,
            @Nullable LookupLevels.LookupContext context)
            throws IOException {
        return ((LookupLevels) lookupLevels).lookup(key, startLevel, context);
    }

    @Nullable
    private InternalRow visibleValue(
            @Nullable Object lookedUp, Map<String, DeletionVector> deletionVectors) {
        if (lookedUp == null) {
            return null;
        }
        KeyValue kv;
        if (lookedUp instanceof PositionedKeyValue) {
            PositionedKeyValue positioned = (PositionedKeyValue) lookedUp;
            DeletionVector deletionVector = deletionVectors.get(positioned.fileName());
            // A hit inside the deletion vector is the newest record and is gone. Older levels are
            // older versions of the same key, so do not continue the search.
            if (deletionVector != null && deletionVector.isDeleted(positioned.rowPosition())) {
                return null;
            }
            kv = positioned.keyValue();
        } else {
            kv = (KeyValue) lookedUp;
        }
        if (kv.valueKind().isRetract()) {
            return null;
        }
        return kv.value();
    }

    @Override
    public LocalTableQuery withValueProjection(int[] projection) {
        this.readerFactoryBuilder.withReadValueType(rowType.project(projection));
        return this;
    }

    public LocalTableQuery withIOManager(IOManager ioManager) {
        this.ioManager = ioManager;
        return this;
    }

    public LocalTableQuery withCacheRowFilter(Filter<InternalRow> cacheRowFilter) {
        this.cacheRowFilter = cacheRowFilter;
        return this;
    }

    public LocalTableQuery withMetrics(@Nullable PartialLookupMetrics metrics) {
        this.partialLookupMetrics = metrics;
        return this;
    }

    @Override
    public InternalRowSerializer createValueSerializer() {
        return InternalSerializers.create(readerFactoryBuilder.readValueType());
    }

    @Override
    public void close() throws IOException {
        // ConcurrentHashMap iteration is weakly consistent. close is expected not to race with
        // refreshFiles for the same query instance; callers may rebuild this query after close.
        for (Map.Entry<BinaryRow, Map<Integer, BucketLookupState>> buckets : tableView.entrySet()) {
            for (Map.Entry<Integer, BucketLookupState> bucket : buckets.getValue().entrySet()) {
                BucketLookupState state = bucket.getValue();
                state.lock.writeLock().lock();
                try {
                    if (state.lookupLevels != null) {
                        state.lookupLevels.close();
                    }
                } finally {
                    state.lock.writeLock().unlock();
                }
            }
        }
        if (lookupFileCache != null) {
            lookupFileCache.invalidateAll();
        }
        tableView.clear();
    }

    private static class BucketLookupState {

        private final ReadWriteLock lock = new ReentrantReadWriteLock();

        @Nullable private LookupLevels<?> lookupLevels;

        private Map<String, DeletionVector> deletionVectors = Collections.emptyMap();

        /** Index files that produced {@link #deletionVectors}. Null until the first load. */
        @Nullable private List<IndexFileMeta> deletionVectorFiles;
    }
}
