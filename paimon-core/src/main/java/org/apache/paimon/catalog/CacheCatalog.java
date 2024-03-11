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

import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.Table;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.RemovalCause;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.RemovalListener;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Ticker;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Class that wraps a paimon catalog to cache tables. */
public class CacheCatalog extends AbstractCatalog {

    private final AbstractCatalog catalog;
    private final long expirationIntervalMillis;
    private final Cache<Identifier, Table> tableCache;

    private CacheCatalog(Catalog catalog, long expirationIntervalMillis) {
        super(((AbstractCatalog) catalog).fileIO(), ((AbstractCatalog) catalog).catalogOptions);
        this.catalog = (AbstractCatalog) catalog;
        this.expirationIntervalMillis = expirationIntervalMillis;
        this.tableCache = createTableCache();
    }

    public static Catalog wrap(Catalog catalog, long expirationIntervalMillis) {
        AbstractCatalog abstractCatalog = (AbstractCatalog) catalog;
        return new CacheCatalog(abstractCatalog, expirationIntervalMillis);
    }

    @Override
    public Optional<CatalogLock.LockFactory> lockFactory() {
        return catalog.lockFactory();
    }

    @Override
    public List<String> listDatabases() {
        return catalog.listDatabases();
    }

    @Override
    public Optional<CatalogLock.LockContext> lockContext() {
        return catalog.lockContext();
    }

    @Override
    public boolean databaseExists(String databaseName) {
        return catalog.databaseExists(databaseName);
    }

    @Override
    public void createDatabase(String name, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException {
        catalog.createDatabase(name, ignoreIfExists);
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException {
        catalog.dropDatabase(name, ignoreIfNotExists, cascade);
    }

    @Override
    public Table getTable(Identifier identifier) throws TableNotExistException {
        Table cached = tableCache.getIfPresent(identifier);
        if (cached != null) {
            return cached;
        }

        Table table = catalog.getTable(identifier);
        tableCache.put(identifier, table);
        return table;
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException {
        return catalog.listTables(databaseName);
    }

    @Override
    public void dropTable(Identifier identifier, boolean ignoreIfNotExists)
            throws TableNotExistException {
        catalog.dropTable(identifier, ignoreIfNotExists);
        invalidateTable(identifier);
    }

    @Override
    public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException {
        catalog.createTable(identifier, schema, ignoreIfExists);
    }

    @Override
    public void renameTable(Identifier fromTable, Identifier toTable, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException {
        catalog.renameTable(fromTable, toTable, ignoreIfNotExists);
        invalidateTable(fromTable);
    }

    @Override
    public void alterTable(
            Identifier identifier, List<SchemaChange> changes, boolean ignoreIfNotExists)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {
        catalog.alterTable(identifier, changes, ignoreIfNotExists);
        invalidateTable(identifier);
    }

    @Override
    public void dropPartition(Identifier identifier, Map<String, String> partitions)
            throws TableNotExistException {
        catalog.dropPartition(identifier, partitions);
        invalidateTable(identifier);
    }

    @Override
    protected boolean databaseExistsImpl(String databaseName) {
        return catalog.databaseExistsImpl(databaseName);
    }

    @Override
    protected Map<String, String> loadDatabasePropertiesImpl(String name) {
        return catalog.loadDatabasePropertiesImpl(name);
    }

    @Override
    protected void createDatabaseImpl(String name, Map<String, String> properties) {
        catalog.createDatabaseImpl(name, properties);
    }

    @Override
    protected void dropDatabaseImpl(String name) {
        catalog.dropDatabaseImpl(name);
    }

    @Override
    protected List<String> listTablesImpl(String databaseName) {
        return catalog.listTablesImpl(databaseName);
    }

    @Override
    protected void dropTableImpl(Identifier identifier) {
        catalog.dropTableImpl(identifier);
    }

    @Override
    protected void createTableImpl(Identifier identifier, Schema schema) {
        catalog.createTableImpl(identifier, schema);
    }

    @Override
    protected void renameTableImpl(Identifier fromTable, Identifier toTable) {
        catalog.renameTableImpl(fromTable, toTable);
    }

    @Override
    protected void alterTableImpl(Identifier identifier, List<SchemaChange> changes)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {
        catalog.alterTableImpl(identifier, changes);
    }

    @Override
    public String warehouse() {
        return catalog.warehouse();
    }

    @Override
    protected TableSchema getDataTableSchema(Identifier identifier) throws TableNotExistException {
        return catalog.getDataTableSchema(identifier);
    }

    @Override
    public boolean caseSensitive() {
        return catalog.caseSensitive();
    }

    @Override
    public void close() throws Exception {
        catalog.close();
    }

    private Cache<Identifier, Table> createTableCache() {
        Caffeine<Object, Object> cacheBuilder = Caffeine.newBuilder().softValues();

        if (expirationIntervalMillis > 0) {
            return cacheBuilder
                    .removalListener(new MetadataTableInvalidatingRemovalListener())
                    .executor(Runnable::run)
                    .expireAfterAccess(Duration.ofMillis(expirationIntervalMillis))
                    .ticker(Ticker.systemTicker())
                    .build();
        }

        return cacheBuilder.build();
    }

    public Catalog catalog() {
        return catalog;
    }

    class MetadataTableInvalidatingRemovalListener implements RemovalListener<Identifier, Table> {
        @Override
        public void onRemoval(Identifier tableIdentifier, Table table, RemovalCause cause) {
            if (RemovalCause.EXPIRED.equals(cause)) {
                tableCache.invalidateAll();
            }
        }
    }

    private void invalidateTable(Identifier identifier) {
        tableCache.invalidate(identifier);
        tableCache.invalidate(
                Identifier.create(
                        identifier.getDatabaseName(), identifier.getObjectName() + "$files"));
        tableCache.invalidate(
                Identifier.create(
                        identifier.getDatabaseName(), identifier.getObjectName() + "$consumers"));
        tableCache.invalidate(
                Identifier.create(
                        identifier.getDatabaseName(), identifier.getObjectName() + "$tags"));
        tableCache.invalidate(
                Identifier.create(
                        identifier.getDatabaseName(), identifier.getObjectName() + "$manifests"));
        tableCache.invalidate(
                Identifier.create(
                        identifier.getDatabaseName(), identifier.getObjectName() + "$partitions"));
        tableCache.invalidate(
                Identifier.create(
                        identifier.getDatabaseName(), identifier.getObjectName() + "$audit_log"));
        tableCache.invalidate(
                Identifier.create(
                        identifier.getDatabaseName(), identifier.getObjectName() + "$options"));
        tableCache.invalidate(
                Identifier.create(
                        identifier.getDatabaseName(), identifier.getObjectName() + "$schemas"));
        tableCache.invalidate(
                Identifier.create(
                        identifier.getDatabaseName(), identifier.getObjectName() + "$snapshots"));
    }
}
