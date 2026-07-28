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

package org.apache.paimon.lookup.local;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.ListState;
import org.apache.paimon.lookup.SetState;
import org.apache.paimon.lookup.StateFactory;
import org.apache.paimon.lookup.ValueState;
import org.apache.paimon.lookup.sort.db.LocalKvDb;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.LongSupplier;

import static org.apache.paimon.CoreOptions.LOOKUP_CACHE_BLOOM_FILTER_ENABLED;
import static org.apache.paimon.CoreOptions.LOOKUP_CACHE_BLOOM_FILTER_FPP;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Factory for lookup states backed by {@link LocalKvDb}.
 *
 * <p>Each state uses a separate database so that its serialized keys keep their natural byte
 * ordering and can be bulk-loaded independently. All databases share one block cache and,
 * optionally, one caller-owned compaction executor.
 */
public class LocalKvStateFactory implements StateFactory {

    private final File rootDirectory;
    private final CoreOptions coreOptions;
    private final Options options;
    private final CacheManager cacheManager;
    private final LocalKvValueCodec valueCodec;
    @Nullable private final ExecutorService compactionExecutor;
    private final Map<String, LocalKvDb> databases;

    private boolean closed;

    public LocalKvStateFactory(
            String path,
            Options options,
            @Nullable Duration ttl,
            @Nullable ExecutorService compactionExecutor,
            boolean offHeapCache) {
        this(path, options, ttl, compactionExecutor, offHeapCache, System::currentTimeMillis);
    }

    LocalKvStateFactory(
            String path,
            Options options,
            @Nullable Duration ttl,
            @Nullable ExecutorService compactionExecutor,
            boolean offHeapCache,
            LongSupplier currentTimeMillis) {
        this.rootDirectory = new File(path);
        if ((!rootDirectory.exists() && !rootDirectory.mkdirs()) || !rootDirectory.isDirectory()) {
            throw new IllegalStateException(
                    "Failed to create LocalKvStateFactory directory: " + rootDirectory);
        }
        this.coreOptions = new CoreOptions(options);
        this.options = options;
        MemorySize cacheMemory = coreOptions.lookupCacheMaxMemory();
        this.cacheManager =
                offHeapCache
                        ? CacheManager.createOffHeap(
                                cacheMemory, coreOptions.lookupCacheHighPrioPoolRatio())
                        : new CacheManager(cacheMemory, coreOptions.lookupCacheHighPrioPoolRatio());
        this.valueCodec = new LocalKvValueCodec(ttl, currentTimeMillis);
        this.compactionExecutor = compactionExecutor;
        this.databases = new LinkedHashMap<>();
        this.closed = false;
    }

    @Override
    public <K, V> ValueState<K, V> valueState(
            String name,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            long lruCacheSize)
            throws IOException {
        return new LocalKvValueState<>(
                createDatabase(name, null),
                keySerializer,
                valueSerializer,
                lruCacheSize,
                valueCodec);
    }

    @Override
    public <K, V> SetState<K, V> setState(
            String name,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            long lruCacheSize)
            throws IOException {
        return new LocalKvSetState<>(
                createDatabase(name, null),
                keySerializer,
                valueSerializer,
                lruCacheSize,
                valueCodec);
    }

    @Override
    public <K, V> ListState<K, V> listState(
            String name,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            long lruCacheSize)
            throws IOException {
        return new LocalKvListState<>(
                createDatabase(name, new LocalKvListMergeOperator(valueCodec)),
                keySerializer,
                valueSerializer,
                lruCacheSize,
                valueCodec);
    }

    @Override
    public boolean preferBulkLoad() {
        return true;
    }

    private LocalKvDb createDatabase(String name, @Nullable LocalKvDb.MergeOperator mergeOperator) {
        checkArgument(!closed, "LocalKvStateFactory is already closed.");
        checkArgument(!databases.containsKey(name), "State '%s' already exists.", name);

        File stateDirectory =
                new File(rootDirectory, String.format("state-%06d", databases.size()));
        LocalKvDb db =
                LocalKvDb.builder(stateDirectory)
                        .cacheManager(cacheManager)
                        .blockSize(coreOptions.localKvDbBlockSize())
                        .compressOptions(coreOptions.lookupCompressOptions())
                        .bloomFilterEnabled(options.get(LOOKUP_CACHE_BLOOM_FILTER_ENABLED))
                        .bloomFilterFpp(options.get(LOOKUP_CACHE_BLOOM_FILTER_FPP))
                        .expiredValuePredicate(
                                valueCodec.ttlEnabled() ? valueCodec::isExpired : null)
                        .mergeOperator(mergeOperator)
                        .compactionExecutor(compactionExecutor)
                        .build();
        databases.put(name, db);
        return db;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;

        IOException failure = null;
        for (LocalKvDb db : databases.values()) {
            try {
                db.close();
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        databases.clear();
        cacheManager.close();
        if (failure != null) {
            throw failure;
        }
    }
}
