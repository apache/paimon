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

package org.apache.paimon.catalog;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.system.SystemTableLoader;
import org.apache.paimon.utils.SegmentsCache;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Ticker;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Weigher;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.options.CatalogOptions.CACHE_ENABLED;
import static org.apache.paimon.options.CatalogOptions.CACHE_EXPIRATION_INTERVAL_MS;
import static org.apache.paimon.options.CatalogOptions.CACHE_MANIFEST_MAX_MEMORY;
import static org.apache.paimon.options.CatalogOptions.CACHE_MANIFEST_SMALL_FILE_MEMORY;
import static org.apache.paimon.options.CatalogOptions.CACHE_MANIFEST_SMALL_FILE_THRESHOLD;
import static org.apache.paimon.options.CatalogOptions.CACHE_PARTITION_MAX_NUM;
import static org.apache.paimon.options.CatalogOptions.CACHE_SNAPSHOT_MAX_NUM_PER_TABLE;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** A {@link Catalog} to cache databases and tables and manifests. */
public class CachingCatalog extends DelegateCatalog {

    private final Options options;

    private final Duration expirationInterval;
    private final int snapshotMaxNumPerTable;
    private final long cachedPartitionMaxNum;

    protected Cache<String, Database> databaseCache;
    protected Cache<Identifier, Table> tableCache;
    @Nullable protected final SegmentsCache<Path> manifestCache;

    // partition cache will affect data latency
    @Nullable protected Cache<Identifier, List<Partition>> partitionCache;

    public CachingCatalog(Catalog wrapped, Options options) {
        super(wrapped);
        this.options = options;
        MemorySize manifestMaxMemory = options.get(CACHE_MANIFEST_SMALL_FILE_MEMORY);
        long manifestCacheThreshold = options.get(CACHE_MANIFEST_SMALL_FILE_THRESHOLD).getBytes();
        Optional<MemorySize> maxMemory = options.getOptional(CACHE_MANIFEST_MAX_MEMORY);
        if (maxMemory.isPresent() && maxMemory.get().compareTo(manifestMaxMemory) > 0) {
            // cache all manifest files
            manifestMaxMemory = maxMemory.get();
            manifestCacheThreshold = Long.MAX_VALUE;
        }

        this.expirationInterval = options.get(CACHE_EXPIRATION_INTERVAL_MS);
        if (expirationInterval.isZero() || expirationInterval.isNegative()) {
            throw new IllegalArgumentException(
                    "When cache.expiration-interval is set to negative or 0, the catalog cache should be disabled.");
        }
        this.snapshotMaxNumPerTable = options.get(CACHE_SNAPSHOT_MAX_NUM_PER_TABLE);
        this.manifestCache = SegmentsCache.create(manifestMaxMemory, manifestCacheThreshold);

        this.cachedPartitionMaxNum = options.get(CACHE_PARTITION_MAX_NUM);
        init(Ticker.systemTicker());
    }

    @VisibleForTesting
    void init(Ticker ticker) {
        this.databaseCache =
                Caffeine.newBuilder()
                        .softValues()
                        .executor(Runnable::run)
                        .expireAfterAccess(expirationInterval)
                        .ticker(ticker)
                        .build();
        this.tableCache =
                Caffeine.newBuilder()
                        .softValues()
                        .executor(Runnable::run)
                        .expireAfterAccess(expirationInterval)
                        .ticker(ticker)
                        .build();
        this.partitionCache =
                cachedPartitionMaxNum == 0
                        ? null
                        : Caffeine.newBuilder()
                                .softValues()
                                .executor(Runnable::run)
                                .expireAfterAccess(expirationInterval)
                                .weigher(
                                        (Weigher<Identifier, List<Partition>>)
                                                (identifier, v) -> v.size())
                                .maximumWeight(cachedPartitionMaxNum)
                                .ticker(ticker)
                                .build();
    }

    public static Catalog tryToCreate(Catalog catalog, Options options) {
        if (!options.get(CACHE_ENABLED)) {
            return catalog;
        }

        return new CachingCatalog(catalog, options);
    }

    @Override
    public CatalogLoader catalogLoader() {
        return new CachingCatalogLoader(wrapped.catalogLoader(), options);
    }

    @Override
    public Database getDatabase(String databaseName) throws DatabaseNotExistException {
        Database database = databaseCache.getIfPresent(databaseName);
        if (database != null) {
            return database;
        }

        database = super.getDatabase(databaseName);
        databaseCache.put(databaseName, database);
        return database;
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException {
        super.dropDatabase(name, ignoreIfNotExists, cascade);
        databaseCache.invalidate(name);
        if (cascade) {
            List<Identifier> tables = new ArrayList<>();
            for (Identifier identifier : tableCache.asMap().keySet()) {
                if (identifier.getDatabaseName().equals(name)) {
                    tables.add(identifier);
                }
            }
            tables.forEach(tableCache::invalidate);
        }
    }

    @Override
    public void alterDatabase(String name, List<PropertyChange> changes, boolean ignoreIfNotExists)
            throws DatabaseNotExistException {
        super.alterDatabase(name, changes, ignoreIfNotExists);
        databaseCache.invalidate(name);
    }

    @Override
    public void dropTable(Identifier identifier, boolean ignoreIfNotExists)
            throws TableNotExistException {
        super.dropTable(identifier, ignoreIfNotExists);
        invalidateTable(identifier);

        // clear all branch tables of this table
        for (Identifier i : tableCache.asMap().keySet()) {
            if (identifier.getTableName().equals(i.getTableName())) {
                tableCache.invalidate(i);
            }
        }
    }

    @Override
    public void renameTable(Identifier fromTable, Identifier toTable, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException {
        super.renameTable(fromTable, toTable, ignoreIfNotExists);
        invalidateTable(fromTable);
    }

    @Override
    public void alterTable(
            Identifier identifier, List<SchemaChange> changes, boolean ignoreIfNotExists)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {
        super.alterTable(identifier, changes, ignoreIfNotExists);
        invalidateTable(identifier);
    }

    @Override
    public Table getTable(Identifier identifier) throws TableNotExistException {
        Table table = tableCache.getIfPresent(identifier);
        if (table != null) {
            return table;
        }

        // For system table, do not cache it directly. Instead, cache the origin table and then wrap
        // it to generate the system table.
        if (identifier.isSystemTable()) {
            Identifier originIdentifier =
                    new Identifier(
                            identifier.getDatabaseName(),
                            identifier.getTableName(),
                            identifier.getBranchName(),
                            null);
            Table originTable = getTable(originIdentifier);
            table =
                    SystemTableLoader.load(
                            checkNotNull(identifier.getSystemTableName()),
                            (FileStoreTable) originTable);
            if (table == null) {
                throw new TableNotExistException(identifier);
            }
            return table;
        }

        table = wrapped.getTable(identifier);
        putTableCache(identifier, table);
        return table;
    }

    private void putTableCache(Identifier identifier, Table table) {
        if (table instanceof FileStoreTable) {
            FileStoreTable storeTable = (FileStoreTable) table;
            storeTable.setSnapshotCache(
                    Caffeine.newBuilder()
                            .softValues()
                            .expireAfterAccess(expirationInterval)
                            .maximumSize(snapshotMaxNumPerTable)
                            .executor(Runnable::run)
                            .build());
            storeTable.setStatsCache(
                    Caffeine.newBuilder()
                            .softValues()
                            .expireAfterAccess(expirationInterval)
                            .maximumSize(5)
                            .executor(Runnable::run)
                            .build());
            if (manifestCache != null) {
                storeTable.setManifestCache(manifestCache);
            }
        }

        tableCache.put(identifier, table);
    }

    @Override
    public List<Partition> listPartitions(Identifier identifier) throws TableNotExistException {
        if (partitionCache == null) {
            return wrapped.listPartitions(identifier);
        }

        List<Partition> result = partitionCache.getIfPresent(identifier);
        if (result == null) {
            result = wrapped.listPartitions(identifier);
            partitionCache.put(identifier, result);
        }
        return result;
    }

    @Override
    public void dropPartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException {
        wrapped.dropPartitions(identifier, partitions);
        if (partitionCache != null) {
            partitionCache.invalidate(identifier);
        }
    }

    @Override
    public void alterPartitions(Identifier identifier, List<Partition> partitions)
            throws TableNotExistException {
        wrapped.alterPartitions(identifier, partitions);
        if (partitionCache != null) {
            partitionCache.invalidate(identifier);
        }
    }

    @Override
    public void invalidateTable(Identifier identifier) {
        tableCache.invalidate(identifier);
        if (partitionCache != null) {
            partitionCache.invalidate(identifier);
        }
    }

    // ================================== Cache Public API
    // ================================================

    /**
     * Partition cache will affect the latency of table, so refresh method is provided for compute
     * engine.
     */
    public void refreshPartitions(Identifier identifier) throws TableNotExistException {
        if (partitionCache != null) {
            List<Partition> result = wrapped.listPartitions(identifier);
            partitionCache.put(identifier, result);
        }
    }

    /**
     * Cache sizes for compute engine. This method can let the outside know the specific usage of
     * cache.
     */
    public CacheSizes estimatedCacheSizes() {
        long databaseCacheSize = databaseCache.estimatedSize();
        long tableCacheSize = tableCache.estimatedSize();
        long manifestCacheSize = 0L;
        long manifestCacheBytes = 0L;
        if (manifestCache != null) {
            manifestCacheSize = manifestCache.estimatedSize();
            manifestCacheBytes = manifestCache.totalCacheBytes();
        }
        long partitionCacheSize = 0L;
        if (partitionCache != null) {
            for (Map.Entry<Identifier, List<Partition>> entry : partitionCache.asMap().entrySet()) {
                partitionCacheSize += entry.getValue().size();
            }
        }
        return new CacheSizes(
                databaseCacheSize,
                tableCacheSize,
                manifestCacheSize,
                manifestCacheBytes,
                partitionCacheSize);
    }

    /** Cache sizes of a caching catalog. */
    public static class CacheSizes {

        private final long databaseCacheSize;
        private final long tableCacheSize;
        private final long manifestCacheSize;
        private final long manifestCacheBytes;
        private final long partitionCacheSize;

        public CacheSizes(
                long databaseCacheSize,
                long tableCacheSize,
                long manifestCacheSize,
                long manifestCacheBytes,
                long partitionCacheSize) {
            this.databaseCacheSize = databaseCacheSize;
            this.tableCacheSize = tableCacheSize;
            this.manifestCacheSize = manifestCacheSize;
            this.manifestCacheBytes = manifestCacheBytes;
            this.partitionCacheSize = partitionCacheSize;
        }

        public long databaseCacheSize() {
            return databaseCacheSize;
        }

        public long tableCacheSize() {
            return tableCacheSize;
        }

        public long manifestCacheSize() {
            return manifestCacheSize;
        }

        public long manifestCacheBytes() {
            return manifestCacheBytes;
        }

        public long partitionCacheSize() {
            return partitionCacheSize;
        }
    }
}
