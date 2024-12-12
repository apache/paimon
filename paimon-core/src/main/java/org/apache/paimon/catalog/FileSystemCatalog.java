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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static org.apache.paimon.catalog.FileSystemCatalogOptions.CASE_SENSITIVE;

/** A catalog implementation for {@link FileIO}. */
public class FileSystemCatalog extends AbstractCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemCatalog.class);

    private final Path warehouse;

    public FileSystemCatalog(FileIO fileIO, Path warehouse) {
        super(fileIO);
        this.warehouse = warehouse;
    }

    public FileSystemCatalog(FileIO fileIO, Path warehouse, Options options) {
        super(fileIO, options);
        this.warehouse = warehouse;
    }

    @Override
    public List<String> listDatabases() {
        return uncheck(() -> listDatabasesInFileSystem(warehouse));
    }

    @Override
    protected void createDatabaseImpl(String name, Map<String, String> properties) {
        if (properties.containsKey(Catalog.DB_LOCATION_PROP)) {
            throw new IllegalArgumentException(
                    "Cannot specify location for a database when using fileSystem catalog.");
        }
        if (!properties.isEmpty()) {
            LOG.warn(
                    "Currently filesystem catalog can't store database properties, discard properties: {}",
                    properties);
        }

        Path databasePath = newDatabasePath(name);
        if (!uncheck(() -> fileIO.mkdirs(databasePath))) {
            throw new RuntimeException(
                    String.format(
                            "Create database location failed, " + "database: %s, location: %s",
                            name, databasePath));
        }
    }

    @Override
    public Database getDatabaseImpl(String name) throws DatabaseNotExistException {
        if (!uncheck(() -> fileIO.exists(newDatabasePath(name)))) {
            throw new DatabaseNotExistException(name);
        }
        return Database.of(name);
    }

    @Override
    protected void dropDatabaseImpl(String name) {
        uncheck(() -> fileIO.delete(newDatabasePath(name), true));
    }

    @Override
    protected void alertDatabaseImpl(String name, List<DatabaseChange> changes) {
        throw new UnsupportedOperationException("Alert database is not supported.");
    }

    @Override
    protected List<String> listTablesImpl(String databaseName) {
        return uncheck(() -> listTablesInFileSystem(newDatabasePath(databaseName)));
    }

    @Override
    public TableSchema getDataTableSchema(Identifier identifier) throws TableNotExistException {
        return tableSchemaInFileSystem(
                        getTableLocation(identifier), identifier.getBranchNameOrDefault())
                .orElseThrow(() -> new TableNotExistException(identifier));
    }

    @Override
    protected void dropTableImpl(Identifier identifier) {
        Path path = getTableLocation(identifier);
        uncheck(() -> fileIO.delete(path, true));
    }

    @Override
    public void createTableImpl(Identifier identifier, Schema schema) {
        uncheck(() -> schemaManager(identifier).createTable(schema));
    }

    private SchemaManager schemaManager(Identifier identifier) {
        Path path = getTableLocation(identifier);
        CatalogLock catalogLock =
                lockFactory().map(fac -> fac.createLock(assertGetLockContext())).orElse(null);
        return new SchemaManager(fileIO, path, identifier.getBranchNameOrDefault())
                .withLock(catalogLock == null ? null : Lock.fromCatalog(catalogLock, identifier));
    }

    private CatalogLockContext assertGetLockContext() {
        return lockContext()
                .orElseThrow(() -> new RuntimeException("No lock context when lock is enabled."));
    }

    @Override
    public void renameTableImpl(Identifier fromTable, Identifier toTable) {
        Path fromPath = getTableLocation(fromTable);
        Path toPath = getTableLocation(toTable);
        uncheck(() -> fileIO.rename(fromPath, toPath));
    }

    @Override
    protected void alterTableImpl(Identifier identifier, List<SchemaChange> changes)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {
        schemaManager(identifier).commitChanges(changes);
    }

    protected static <T> T uncheck(Callable<T> callable) {
        try {
            return callable.call();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {}

    @Override
    public String warehouse() {
        return warehouse.toString();
    }

    @Override
    public boolean allowUpperCase() {
        return catalogOptions.get(CASE_SENSITIVE);
    }
}
