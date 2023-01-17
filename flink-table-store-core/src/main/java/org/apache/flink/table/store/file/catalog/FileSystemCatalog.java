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

package org.apache.flink.table.store.file.catalog;

import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.store.file.schema.SchemaChange;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.file.schema.UpdateSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;

import static org.apache.flink.table.store.file.utils.FileUtils.safelyListFileStatus;

/** A catalog implementation for {@link FileSystem}. */
public class FileSystemCatalog extends AbstractCatalog {

    private final FileSystem fs;
    private final Path warehouse;

    public FileSystemCatalog(Path warehouse) {
        this.warehouse = warehouse;
        this.fs = uncheck(warehouse::getFileSystem);
    }

    @Override
    public Optional<CatalogLock.Factory> lockFactory() {
        return Optional.empty();
    }

    @Override
    public List<String> listDatabases() {
        List<String> databases = new ArrayList<>();
        for (FileStatus status : uncheck(() -> safelyListFileStatus(warehouse))) {
            Path path = status.getPath();
            if (status.isDir() && isDatabase(path)) {
                databases.add(database(path));
            }
        }
        return databases;
    }

    @Override
    public boolean databaseExists(String databaseName) {
        return uncheck(() -> fs.exists(databasePath(databaseName)));
    }

    @Override
    public void createDatabase(String name, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException {
        if (databaseExists(name)) {
            if (ignoreIfExists) {
                return;
            }
            throw new DatabaseAlreadyExistException(name);
        }
        uncheck(() -> fs.mkdirs(databasePath(name)));
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException {
        if (!databaseExists(name)) {
            if (ignoreIfNotExists) {
                return;
            }

            throw new DatabaseNotExistException(name);
        }

        if (!cascade && listTables(name).size() > 0) {
            throw new DatabaseNotEmptyException(name);
        }

        uncheck(() -> fs.delete(databasePath(name), true));
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(databaseName);
        }

        List<String> tables = new ArrayList<>();
        for (FileStatus status : uncheck(() -> safelyListFileStatus(databasePath(databaseName)))) {
            if (status.isDir() && tableExists(status.getPath())) {
                tables.add(status.getPath().getName());
            }
        }
        return tables;
    }

    @Override
    public TableSchema getTableSchema(Identifier identifier) throws TableNotExistException {
        Path path = getTableLocation(identifier);
        return new SchemaManager(path)
                .latest()
                .orElseThrow(() -> new TableNotExistException(identifier));
    }

    @Override
    public boolean tableExists(Identifier identifier) {
        return tableExists(getTableLocation(identifier));
    }

    private boolean tableExists(Path identifier) {
        return new SchemaManager(identifier).listAllIds().size() > 0;
    }

    @Override
    public void dropTable(Identifier identifier, boolean ignoreIfNotExists)
            throws TableNotExistException {
        Path path = getTableLocation(identifier);
        if (!tableExists(path)) {
            if (ignoreIfNotExists) {
                return;
            }

            throw new TableNotExistException(identifier);
        }

        uncheck(() -> fs.delete(path, true));
    }

    @Override
    public void createTable(Identifier identifier, UpdateSchema table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException {
        if (!databaseExists(identifier.getDatabaseName())) {
            throw new DatabaseNotExistException(identifier.getDatabaseName());
        }

        Path path = getTableLocation(identifier);
        if (tableExists(path)) {
            if (ignoreIfExists) {
                return;
            }

            throw new TableAlreadyExistException(identifier);
        }

        uncheck(() -> new SchemaManager(path).commitNewVersion(table));
    }

    @Override
    public void alterTable(
            Identifier identifier, List<SchemaChange> changes, boolean ignoreIfNotExists)
            throws TableNotExistException {
        if (!tableExists(identifier)) {
            throw new TableNotExistException(identifier);
        }
        uncheck(() -> new SchemaManager(getTableLocation(identifier)).commitChanges(changes));
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
    protected String warehouse() {
        return warehouse.toString();
    }
}
