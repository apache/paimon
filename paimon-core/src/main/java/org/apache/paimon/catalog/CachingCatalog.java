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

import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.system.SystemTableLoader;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SegmentsCache;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.RemovalCause;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.RemovalListener;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Ticker;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Weigher;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.catalog.AbstractCatalog.isSpecifiedSystemTable;
import static org.apache.paimon.options.CatalogOptions.CACHE_ENABLED;
import static org.apache.paimon.options.CatalogOptions.CACHE_EXPIRATION_INTERVAL_MS;
import static org.apache.paimon.options.CatalogOptions.CACHE_MANIFEST_MAX_MEMORY;
import static org.apache.paimon.options.CatalogOptions.CACHE_MANIFEST_SMALL_FILE_MEMORY;
import static org.apache.paimon.options.CatalogOptions.CACHE_MANIFEST_SMALL_FILE_THRESHOLD;
import static org.apache.paimon.options.CatalogOptions.CACHE_PARTITION_MAX_NUM;
import static org.apache.paimon.table.system.SystemTableLoader.SYSTEM_TABLES;

/** A {@link Catalog} to cache databases and tables and manifests. */
public class CachingCatalog extends DelegateCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(CachingCatalog.class);

    protected final Cache<String, Database> databaseCache;
    protected final Cache<Identifier, Table> tableCache;
    @Nullable protected final SegmentsCache<Path> manifestCache;

    // partition cache will affect data latency
    @Nullable protected final Cache<Identifier, List<PartitionEntry>> partitionCache;

    public CachingCatalog(Catalog wrapped) {
        this(
                wrapped,
                CACHE_EXPIRATION_INTERVAL_MS.defaultValue(),
                CACHE_MANIFEST_SMALL_FILE_MEMORY.defaultValue(),
                CACHE_MANIFEST_SMALL_FILE_THRESHOLD.defaultValue().getBytes(),
                CACHE_PARTITION_MAX_NUM.defaultValue());
    }

    public CachingCatalog(
            Catalog wrapped,
            Duration expirationInterval,
            MemorySize manifestMaxMemory,
            long manifestCacheThreshold,
            long cachedPartitionMaxNum) {
        this(
                wrapped,
                expirationInterval,
                manifestMaxMemory,
                manifestCacheThreshold,
                cachedPartitionMaxNum,
                Ticker.systemTicker());
    }

    public CachingCatalog(
            Catalog wrapped,
            Duration expirationInterval,
            MemorySize manifestMaxMemory,
            long manifestCacheThreshold,
            long cachedPartitionMaxNum,
            Ticker ticker) {
        super(wrapped);
        if (expirationInterval.isZero() || expirationInterval.isNegative()) {
            throw new IllegalArgumentException(
                    "When cache.expiration-interval is set to negative or 0, the catalog cache should be disabled.");
        }

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
                        .removalListener(new TableInvalidatingRemovalListener())
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
                                        (Weigher<Identifier, List<PartitionEntry>>)
                                                (identifier, v) -> v.size())
                                .maximumWeight(cachedPartitionMaxNum)
                                .ticker(ticker)
                                .build();
        this.manifestCache = SegmentsCache.create(manifestMaxMemory, manifestCacheThreshold);
    }

    public static Catalog tryToCreate(Catalog catalog, Options options) {
        if (!options.get(CACHE_ENABLED)) {
            return catalog;
        }

        MemorySize manifestMaxMemory = options.get(CACHE_MANIFEST_SMALL_FILE_MEMORY);
        long manifestThreshold = options.get(CACHE_MANIFEST_SMALL_FILE_THRESHOLD).getBytes();
        Optional<MemorySize> maxMemory = options.getOptional(CACHE_MANIFEST_MAX_MEMORY);
        if (maxMemory.isPresent() && maxMemory.get().compareTo(manifestMaxMemory) > 0) {
            // cache all manifest files
            manifestMaxMemory = maxMemory.get();
            manifestThreshold = Long.MAX_VALUE;
        }
        return new CachingCatalog(
                catalog,
                options.get(CACHE_EXPIRATION_INTERVAL_MS),
                manifestMaxMemory,
                manifestThreshold,
                options.get(CACHE_PARTITION_MAX_NUM));
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
    public void dropTable(Identifier identifier, boolean ignoreIfNotExists)
            throws TableNotExistException {
        super.dropTable(identifier, ignoreIfNotExists);
        invalidateTable(identifier);
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

        if (isSpecifiedSystemTable(identifier)) {
            Identifier originIdentifier =
                    new Identifier(
                            identifier.getDatabaseName(),
                            identifier.getTableName(),
                            identifier.getBranchName(),
                            null);
            Table originTable = tableCache.getIfPresent(originIdentifier);
            if (originTable == null) {
                originTable = wrapped.getTable(originIdentifier);
                putTableCache(originIdentifier, originTable);
            }
            table =
                    SystemTableLoader.load(
                            Preconditions.checkNotNull(identifier.getSystemTableName()),
                            (FileStoreTable) originTable);
            if (table == null) {
                throw new TableNotExistException(identifier);
            }
            putTableCache(identifier, table);
            return table;
        }

        table = wrapped.getTable(identifier);
        putTableCache(identifier, table);
        return table;
    }

    private void putTableCache(Identifier identifier, Table table) {
        if (manifestCache != null && table instanceof FileStoreTable) {
            ((FileStoreTable) table).setManifestCache(manifestCache);
        }
        tableCache.put(identifier, table);
    }

    @Override
    public List<PartitionEntry> listPartitions(Identifier identifier)
            throws TableNotExistException {
        if (partitionCache == null) {
            return wrapped.listPartitions(identifier);
        }

        List<PartitionEntry> result = partitionCache.getIfPresent(identifier);
        if (result == null) {
            result = wrapped.listPartitions(identifier);
            partitionCache.put(identifier, result);
        }
        return result;
    }

    @Override
    public void dropPartition(Identifier identifier, Map<String, String> partitions)
            throws TableNotExistException, PartitionNotExistException {
        wrapped.dropPartition(identifier, partitions);
        if (partitionCache != null) {
            partitionCache.invalidate(identifier);
        }
    }

    private class TableInvalidatingRemovalListener implements RemovalListener<Identifier, Table> {
        @Override
        public void onRemoval(Identifier identifier, Table table, @NonNull RemovalCause cause) {
            LOG.debug("Evicted {} from the table cache ({})", identifier, cause);
            if (RemovalCause.EXPIRED.equals(cause)) {
                tryInvalidateSysTables(identifier);
            }
        }
    }

    @Override
    public void invalidateTable(Identifier identifier) {
        tableCache.invalidate(identifier);
        tryInvalidateSysTables(identifier);
        if (partitionCache != null) {
            partitionCache.invalidate(identifier);
        }
    }

    private void tryInvalidateSysTables(Identifier identifier) {
        if (!isSpecifiedSystemTable(identifier)) {
            tableCache.invalidateAll(allSystemTables(identifier));
        }
    }

    private static Iterable<Identifier> allSystemTables(Identifier ident) {
        List<Identifier> tables = new ArrayList<>();
        for (String type : SYSTEM_TABLES) {
            tables.add(Identifier.fromString(ident.getFullName() + SYSTEM_TABLE_SPLITTER + type));
        }
        return tables;
    }

    // ================================== refresh ================================================
    // following caches will affect the latency of table, so refresh method is provided for engine

    public void refreshPartitions(Identifier identifier) throws TableNotExistException {
        if (partitionCache != null) {
            List<PartitionEntry> result = wrapped.listPartitions(identifier);
            partitionCache.put(identifier, result);
        }
    }
}
