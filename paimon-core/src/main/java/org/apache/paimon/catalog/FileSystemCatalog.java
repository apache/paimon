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
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;

/** A catalog implementation for {@link FileIO}. */
public class FileSystemCatalog extends AbstractCatalog {

    private final Path warehouse;

    public FileSystemCatalog(FileIO fileIO, Path warehouse) {
        super(fileIO);
        this.warehouse = warehouse;
    }

    public FileSystemCatalog(FileIO fileIO, Path warehouse, Map<String, String> options) {
        super(fileIO, options);
        this.warehouse = warehouse;
    }

    @Override
    public Optional<CatalogLock.Factory> lockFactory() {
        return Optional.empty();
    }

    @Override
    public List<String> listDatabases() {
        List<String> databases = new ArrayList<>();
        for (FileStatus status : uncheck(() -> fileIO.listStatus(warehouse))) {
            Path path = status.getPath();
            if (status.isDir() && isDatabase(path)) {
                databases.add(database(path));
            }
        }
        return databases;
    }

    @Override
    public boolean databaseExists(String databaseName) {
        if (isSystemDatabase(databaseName)) {
            return true;
        }
        return uncheck(() -> fileIO.exists(databasePath(databaseName)));
    }

    @Override
    public void createDatabase(String name, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException {
        if (isSystemDatabase(name)) {
            throw new ProcessSystemDatabaseException();
        }
        if (databaseExists(name)) {
            if (ignoreIfExists) {
                return;
            }
            throw new DatabaseAlreadyExistException(name);
        }
        uncheck(() -> fileIO.mkdirs(databasePath(name)));
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException {
        if (isSystemDatabase(name)) {
            throw new ProcessSystemDatabaseException();
        }
        if (!databaseExists(name)) {
            if (ignoreIfNotExists) {
                return;
            }

            throw new DatabaseNotExistException(name);
        }

        if (!cascade && listTables(name).size() > 0) {
            throw new DatabaseNotEmptyException(name);
        }

        uncheck(() -> fileIO.delete(databasePath(name), true));
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException {
        if (isSystemDatabase(databaseName)) {
            return GLOBAL_TABLES;
        }
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(databaseName);
        }

        List<String> tables = new ArrayList<>();
        for (FileStatus status : uncheck(() -> fileIO.listStatus(databasePath(databaseName)))) {
            if (status.isDir() && tableExists(status.getPath())) {
                tables.add(status.getPath().getName());
            }
        }
        return tables;
    }

    @Override
    public TableSchema getDataTableSchema(Identifier identifier) throws TableNotExistException {
        Path path = getDataTableLocation(identifier);
        return new SchemaManager(fileIO, path)
                .latest()
                .orElseThrow(() -> new TableNotExistException(identifier));
    }

    private boolean tableExists(Path tablePath) {
        return new SchemaManager(fileIO, tablePath).listAllIds().size() > 0;
    }

    @Override
    public void dropTable(Identifier identifier, boolean ignoreIfNotExists)
            throws TableNotExistException {
        checkNotSystemTable(identifier, "dropTable");
        Path path = getDataTableLocation(identifier);
        if (!tableExists(path)) {
            if (ignoreIfNotExists) {
                return;
            }

            throw new TableNotExistException(identifier);
        }

        uncheck(() -> fileIO.delete(path, true));
    }

    @Override
    public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException {
        checkNotSystemTable(identifier, "createTable");
        if (!databaseExists(identifier.getDatabaseName())) {
            throw new DatabaseNotExistException(identifier.getDatabaseName());
        }

        Path path = getDataTableLocation(identifier);
        if (tableExists(path)) {
            if (ignoreIfExists) {
                return;
            }

            throw new TableAlreadyExistException(identifier);
        }

        copyTableDefaultOptions(schema.options());

        uncheck(() -> new SchemaManager(fileIO, path).createTable(schema));
    }

    @Override
    public void renameTable(Identifier fromTable, Identifier toTable, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException {
        checkNotSystemTable(fromTable, "renameTable");
        checkNotSystemTable(toTable, "renameTable");
        Path fromPath = getDataTableLocation(fromTable);
        if (!tableExists(fromPath)) {
            if (ignoreIfNotExists) {
                return;
            }

            throw new TableNotExistException(fromTable);
        }

        Path toPath = getDataTableLocation(toTable);
        if (tableExists(toPath)) {
            throw new TableAlreadyExistException(toTable);
        }

        uncheck(() -> fileIO.rename(fromPath, toPath));
    }

    @Override
    public void alterTable(
            Identifier identifier, List<SchemaChange> changes, boolean ignoreIfNotExists)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {
        checkNotSystemTable(identifier, "alterTable");
        if (!tableExists(getDataTableLocation(identifier))) {
            throw new TableNotExistException(identifier);
        }

        new SchemaManager(fileIO, getDataTableLocation(identifier)).commitChanges(changes);
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
