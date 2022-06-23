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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.file.schema.UpdateSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

import static org.apache.flink.table.store.file.utils.FileUtils.safelyListFileStatus;

/** A catalog implementation for {@link FileSystem}. */
public class FileSystemCatalog implements Catalog {

    public static final String DB_SUFFIX = ".db";

    private final FileSystem fs;
    private final Path warehouse;

    public FileSystemCatalog(Path warehouse) {
        this.warehouse = warehouse;
        this.fs = uncheck(warehouse::getFileSystem);
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
    public void dropDatabase(String name, boolean cascade) throws DatabaseNotEmptyException {
        if (!cascade && listTables(name).size() > 0) {
            throw new DatabaseNotEmptyException(name);
        }

        uncheck(() -> fs.delete(databasePath(name), true));
    }

    @Override
    public List<String> listTables(String databaseName) {
        if (!uncheck(() -> fs.exists(databasePath(databaseName)))) {
            return Collections.emptyList();
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
    public Path getTableLocation(ObjectPath tablePath) {
        return tablePath(tablePath);
    }

    @Override
    public TableSchema getTable(ObjectPath tablePath) throws TableNotExistException {
        Path path = tablePath(tablePath);
        return new SchemaManager(path)
                .latest()
                .orElseThrow(() -> new TableNotExistException(tablePath));
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) {
        return tableExists(tablePath(tablePath));
    }

    private boolean tableExists(Path tablePath) {
        return new SchemaManager(tablePath).listAllIds().size() > 0;
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException {
        Path path = tablePath(tablePath);
        if (!tableExists(path)) {
            if (ignoreIfNotExists) {
                return;
            }

            throw new TableNotExistException(tablePath);
        }

        uncheck(() -> fs.delete(path, true));
    }

    @Override
    public void createTable(ObjectPath tablePath, UpdateSchema table, boolean ignoreIfExists)
            throws TableAlreadyExistException {
        Path path = tablePath(tablePath);
        if (tableExists(path)) {
            if (ignoreIfExists) {
                return;
            }

            throw new TableAlreadyExistException(tablePath);
        }

        commitTableChange(path, table);
    }

    @Override
    public void alterTable(ObjectPath tablePath, UpdateSchema newTable, boolean ignoreIfNotExists)
            throws TableNotExistException {
        Path path = tablePath(tablePath);
        if (!tableExists(path)) {
            if (ignoreIfNotExists) {
                return;
            }

            throw new TableNotExistException(tablePath);
        }

        commitTableChange(path, newTable);
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

    private Path databasePath(String database) {
        return new Path(warehouse, database + DB_SUFFIX);
    }

    @VisibleForTesting
    Path tablePath(ObjectPath objectPath) {
        return new Path(databasePath(objectPath.getDatabaseName()), objectPath.getObjectName());
    }

    private void commitTableChange(Path tablePath, UpdateSchema table) {
        uncheck(() -> new SchemaManager(tablePath).commitNewVersion(table));
    }

    @Override
    public void close() throws Exception {}
}
