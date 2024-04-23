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
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
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
        List<String> databases = new ArrayList<>();
        for (FileStatus status : uncheck(() -> fileIO.listDirectories(warehouse))) {
            Path path = status.getPath();
            if (status.isDir() && isDatabase(path)) {
                databases.add(database(path));
            }
        }
        return databases;
    }

    @Override
    protected boolean databaseExistsImpl(String databaseName) {
        return uncheck(() -> fileIO.exists(newDatabasePath(databaseName)));
    }

    @Override
    protected void createDatabaseImpl(String name, Map<String, String> properties) {
        if (properties.containsKey(AbstractCatalog.DB_LOCATION_PROP)) {
            throw new IllegalArgumentException(
                    "Cannot specify location for a database when using fileSystem catalog.");
        }
        if (!properties.isEmpty()) {
            LOG.warn(
                    "Currently filesystem catalog can't store database properties, discard properties: {}",
                    properties);
        }
        uncheck(() -> fileIO.mkdirs(newDatabasePath(name)));
    }

    @Override
    public Map<String, String> loadDatabasePropertiesImpl(String name) {
        return Collections.emptyMap();
    }

    @Override
    protected void dropDatabaseImpl(String name) {
        uncheck(() -> fileIO.delete(newDatabasePath(name), true));
    }

    @Override
    protected List<String> listTablesImpl(String databaseName) {
        List<String> tables = new ArrayList<>();
        for (FileStatus status :
                uncheck(() -> fileIO.listDirectories(newDatabasePath(databaseName)))) {
            if (status.isDir() && tableExists(status.getPath())) {
                tables.add(status.getPath().getName());
            }
        }
        return tables;
    }

    @Override
    public boolean tableExists(Identifier identifier) {
        if (isSystemTable(identifier)) {
            return super.tableExists(identifier);
        }

        return tableExists(getDataTableLocation(identifier));
    }

    private boolean tableExists(Path tablePath) {
        return new SchemaManager(fileIO, tablePath).listAllIds().size() > 0;
    }

    @Override
    public TableSchema getDataTableSchema(Identifier identifier) throws TableNotExistException {
        return schemaManager(identifier)
                .latest()
                .orElseThrow(() -> new TableNotExistException(identifier));
    }

    @Override
    protected void dropTableImpl(Identifier identifier) {
        Path path = getDataTableLocation(identifier);
        uncheck(() -> fileIO.delete(path, true));
    }

    @Override
    public void createTableImpl(Identifier identifier, Schema schema) {
        uncheck(() -> schemaManager(identifier).createTable(schema));
    }

    private SchemaManager schemaManager(Identifier identifier) {
        Path path = getDataTableLocation(identifier);
        CatalogLock catalogLock =
                lockFactory()
                        .map(
                                fac ->
                                        fac.createLock(
                                                lockContext()
                                                        .orElseThrow(
                                                                () ->
                                                                        new RuntimeException(
                                                                                "No lock context when lock is enabled."))))
                        .orElse(null);
        return new SchemaManager(fileIO, path)
                .withLock(catalogLock == null ? null : Lock.fromCatalog(catalogLock, identifier));
    }

    @Override
    public void renameTableImpl(Identifier fromTable, Identifier toTable) {
        Path fromPath = getDataTableLocation(fromTable);
        Path toPath = getDataTableLocation(toTable);
        uncheck(() -> fileIO.rename(fromPath, toPath));
    }

    @Override
    protected void alterTableImpl(Identifier identifier, List<SchemaChange> changes)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {
        schemaManager(identifier).commitChanges(changes);
    }

    private static <T> T uncheck(Callable<T> callable) {
        try {
            return callable.call();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean isDatabase(Path path) {
        return path.getName().endsWith(DB_SUFFIX);
    }

    private static String database(Path path) {
        String name = path.getName();
        return name.substring(0, name.length() - DB_SUFFIX.length());
    }

    @Override
    public void close() throws Exception {}

    @Override
    public String warehouse() {
        return warehouse.toString();
    }

    @Override
    public boolean caseSensitive() {
        return catalogOptions.get(CASE_SENSITIVE);
    }
}
