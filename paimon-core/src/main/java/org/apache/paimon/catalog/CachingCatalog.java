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

import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.system.SystemTableLoader;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.RemovalCause;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.RemovalListener;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Ticker;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.catalog.AbstractCatalog.isSpecifiedSystemTable;
import static org.apache.paimon.catalog.AbstractCatalog.tableAndSystemName;
import static org.apache.paimon.options.CatalogOptions.CACHE_EXPIRATION_INTERVAL_MS;
import static org.apache.paimon.table.system.SystemTableLoader.SYSTEM_TABLES;

/** A {@link Catalog} to cache databases and tables and manifests. */
public class CachingCatalog extends DelegateCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(CachingCatalog.class);

    protected final Cache<String, Map<String, String>> databaseCache;
    protected final Cache<Identifier, Table> tableCache;

    public CachingCatalog(Catalog wrapped) {
        this(wrapped, CACHE_EXPIRATION_INTERVAL_MS.defaultValue());
    }

    public CachingCatalog(Catalog wrapped, Duration expirationInterval) {
        this(wrapped, expirationInterval, Ticker.systemTicker());
    }

    public CachingCatalog(Catalog wrapped, Duration expirationInterval, Ticker ticker) {
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
    }

    @Override
    public Map<String, String> loadDatabaseProperties(String databaseName)
            throws DatabaseNotExistException {
        Map<String, String> properties = databaseCache.getIfPresent(databaseName);
        if (properties != null) {
            return properties;
        }

        properties = super.loadDatabaseProperties(databaseName);
        databaseCache.put(databaseName, properties);
        return properties;
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
            String[] splits = tableAndSystemName(identifier);
            String tableName = splits[0];
            String type = splits[1];

            Identifier originIdentifier =
                    Identifier.create(identifier.getDatabaseName(), tableName);
            Table originTable = tableCache.getIfPresent(originIdentifier);
            if (originTable == null) {
                originTable = wrapped.getTable(originIdentifier);
                tableCache.put(originIdentifier, originTable);
            }
            table = SystemTableLoader.load(type, (FileStoreTable) originTable);
            if (table == null) {
                throw new TableNotExistException(identifier);
            }
            tableCache.put(identifier, table);
            return table;
        }

        table = wrapped.getTable(identifier);
        tableCache.put(identifier, table);
        return table;
    }

    private class TableInvalidatingRemovalListener implements RemovalListener<Identifier, Table> {
        @Override
        public void onRemoval(
                Identifier tableIdentifier, Table table, @NonNull RemovalCause cause) {
            LOG.debug("Evicted {} from the table cache ({})", tableIdentifier, cause);
            if (RemovalCause.EXPIRED.equals(cause)) {
                if (!isSpecifiedSystemTable(tableIdentifier)) {
                    tableCache.invalidateAll(allSystemTables(tableIdentifier));
                }
            }
        }
    }

    private void invalidateTable(Identifier identifier) {
        tableCache.invalidate(identifier);
        tableCache.invalidateAll(allSystemTables(identifier));
    }

    private static Iterable<Identifier> allSystemTables(Identifier ident) {
        List<Identifier> tables = new ArrayList<>();
        for (String type : SYSTEM_TABLES) {
            tables.add(Identifier.fromString(ident.getFullName() + SYSTEM_TABLE_SPLITTER + type));
        }
        return tables;
    }
}
