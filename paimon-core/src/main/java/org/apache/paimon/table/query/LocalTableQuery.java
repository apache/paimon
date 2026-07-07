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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.LookupStoreFactory;
import org.apache.paimon.mergetree.Levels;
import org.apache.paimon.mergetree.LookupFile;
import org.apache.paimon.mergetree.LookupLevels;
import org.apache.paimon.mergetree.lookup.LookupSerializerFactory;
import org.apache.paimon.mergetree.lookup.PersistValueProcessor;
import org.apache.paimon.mergetree.lookup.RemoteLookupFileManager;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.KeyComparatorSupplier;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import static org.apache.paimon.lookup.LookupStoreFactory.bfGenerator;
import static org.apache.paimon.mergetree.LookupFile.localFilePrefix;

/** Implementation for {@link TableQuery} for caching data and file in local. */
public class LocalTableQuery implements TableQuery {

    private final Map<BinaryRow, Map<Integer, BucketLookupState>> tableView;

    private final CoreOptions options;

    private final Supplier<Comparator<InternalRow>> keyComparatorSupplier;

    private final KeyValueFileReaderFactory.Builder readerFactoryBuilder;

    private final LookupStoreFactory lookupStoreFactory;

    private final int startLevel;

    private IOManager ioManager;

    @Nullable private volatile Cache<String, LookupFile> lookupFileCache;

    private final RowType rowType;
    private final RowType partitionType;
    private final FileIO fileIO;

    @Nullable private Filter<InternalRow> cacheRowFilter;

    public LocalTableQuery(FileStoreTable table) {
        this.options = table.coreOptions();
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
        startLevel = options.needLookup() ? 1 : 0;
    }

    public void refreshFiles(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> beforeFiles,
            List<DataFileMeta> dataFiles) {
        // Both tableView and its nested bucket maps are ConcurrentHashMaps; this nested
        // computeIfAbsent pattern relies on each map providing atomic insertion.
        BucketLookupState state =
                tableView
                        .computeIfAbsent(partition, k -> new ConcurrentHashMap<>())
                        .computeIfAbsent(bucket, k -> new BucketLookupState());
        state.lock.writeLock().lock();
        try {
            if (state.lookupLevels == null) {
                // Initial phase: ignore beforeFiles as they represent deletions from previous state
                state.lookupLevels = createLookupLevels(partition, bucket, dataFiles);
            } else {
                state.lookupLevels.getLevels().update(beforeFiles, dataFiles);
            }
        } finally {
            state.lock.writeLock().unlock();
        }
    }

    private LookupLevels<KeyValue> createLookupLevels(
            BinaryRow partition, int bucket, List<DataFileMeta> dataFiles) {
        Levels levels = new Levels(keyComparatorSupplier.get(), dataFiles, options.numLevels());
        // TODO pass DeletionVector factory
        KeyValueFileReaderFactory factory =
                readerFactoryBuilder.build(partition, bucket, DeletionVector.emptyFactory());
        Options options = this.options.toConfiguration();

        RowType readValueType = readerFactoryBuilder.readValueType();
        LookupLevels<KeyValue> lookupLevels =
                new LookupLevels<>(
                        schemaId -> readValueType,
                        0L,
                        levels,
                        keyComparatorSupplier.get(),
                        readerFactoryBuilder.keyType(),
                        PersistValueProcessor.factory(readValueType),
                        LookupSerializerFactory.INSTANCE.get(),
                        file -> {
                            RecordReader<KeyValue> reader = factory.createRecordReader(file);
                            if (cacheRowFilter != null) {
                                reader =
                                        reader.filter(
                                                keyValue -> cacheRowFilter.test(keyValue.value()));
                            }
                            return reader;
                        },
                        file ->
                                Preconditions.checkNotNull(ioManager, "IOManager is required.")
                                        .createChannel(
                                                localFilePrefix(
                                                        partitionType, partition, bucket, file))
                                        .getPathFile(),
                        lookupStoreFactory,
                        bfGenerator(options),
                        lookupFileCache(options));

        // Optimization - download lookup files if already persisted to object store
        // We download these files if three conditions are met
        // 1) lookup.remote-file.enabled is true - files are persisted in the first place
        // 2) deletion-vectors.enabled is false - SSTables only contain row positions, not values,
        // when DVs are enabled
        // 3) The client is accessing the full data row, as opposed to a projection
        //    - The persisted remote SSTable files are created during compaction and hold the entire
        // data row value
        //    - We could deserialize and project in memory, but we'll have to read much more data,
        // not as clear of a win
        boolean fullValueRead = readerFactoryBuilder.readValueType().equals(rowType);
        if (this.options.lookupRemoteFileEnabled()
                && !this.options.deletionVectorsEnabled()
                && fullValueRead) {
            // Calling the constructor tells `lookupLevels` to load remote files
            new RemoteLookupFileManager<>(
                    fileIO,
                    factory.pathFactory(),
                    lookupLevels,
                    this.options.lookupRemoteLevelThreshold());
        }

        return lookupLevels;
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
            LookupLevels<KeyValue> lookupLevels = state.lookupLevels;
            if (lookupLevels == null) {
                return null;
            }

            KeyValue kv = lookupLevels.lookup(key, startLevel);
            if (kv == null || kv.valueKind().isRetract()) {
                return null;
            } else {
                return kv.value();
            }
        } finally {
            state.lock.readLock().unlock();
        }
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

        @Nullable private LookupLevels<KeyValue> lookupLevels;
    }
}
