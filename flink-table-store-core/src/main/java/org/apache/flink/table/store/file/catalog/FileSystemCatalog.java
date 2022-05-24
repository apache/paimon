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
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.store.file.schema.Schema;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.UpdateSchema;
import org.apache.flink.util.function.RunnableWithException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;

import static org.apache.flink.table.catalog.GenericInMemoryCatalogFactoryOptions.DEFAULT_DATABASE;
import static org.apache.flink.table.store.file.FileStoreOptions.PATH;

/** A catalog implementation for {@link FileSystem}. */
public class FileSystemCatalog extends TableStoreCatalog {

    public static final String DB_SUFFIX = ".db";

    public static final CatalogDatabaseImpl DUMMY_DATABASE =
            new CatalogDatabaseImpl(Collections.emptyMap(), null);

    private final FileSystem fs;
    private final Path root;

    public FileSystemCatalog(String name, Path root) {
        this(name, root, DEFAULT_DATABASE.defaultValue());
    }

    public FileSystemCatalog(String name, Path root, String defaultDatabase) {
        super(name, defaultDatabase);
        this.root = root;
        this.fs = uncheck(root::getFileSystem);
        uncheck(() -> createDatabase(defaultDatabase, DUMMY_DATABASE, true));
    }

    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(
                FactoryUtil.discoverFactory(
                        classLoader(), DynamicTableFactory.class, "table-store"));
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        List<String> databases = new ArrayList<>();
        for (FileStatus status : uncheck(() -> fs.listStatus(root))) {
            Path path = status.getPath();
            if (status.isDir() && isDatabase(path)) {
                databases.add(database(path));
            }
        }
        return databases;
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
        return new CatalogDatabaseImpl(Collections.emptyMap(), "");
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return uncheck(() -> fs.exists(databasePath(databaseName)));
    }

    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        if (database.getProperties().size() > 0) {
            throw new UnsupportedOperationException(
                    "Create database with properties is unsupported.");
        }

        if (database.getDescription().isPresent() && !database.getDescription().get().equals("")) {
            throw new UnsupportedOperationException(
                    "Create database with description is unsupported.");
        }

        if (!ignoreIfExists && databaseExists(name)) {
            throw new DatabaseAlreadyExistException(getName(), name);
        }

        uncheck(() -> fs.mkdirs(databasePath(name)));
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        if (!databaseExists(name)) {
            if (ignoreIfNotExists) {
                return;
            }

            throw new DatabaseNotExistException(getName(), name);
        }

        if (listTables(name).size() > 0) {
            throw new DatabaseNotEmptyException(getName(), name);
        }

        uncheck(() -> fs.delete(databasePath(name), true));
    }

    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        List<String> tables = new ArrayList<>();
        for (FileStatus status : uncheck(() -> fs.listStatus(databasePath(databaseName)))) {
            if (status.isDir() && isTable(status.getPath())) {
                tables.add(status.getPath().getName());
            }
        }
        return tables;
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        Path path = tablePath(tablePath);
        Schema schema =
                new SchemaManager(path)
                        .latest()
                        .orElseThrow(() -> new TableNotExistException(getName(), tablePath));

        Map<String, String> options = new HashMap<>(schema.options());
        options.put(PATH.key(), path.toString());

        //noinspection deprecation
        return new CatalogTableImpl(
                schema.getTableSchema(), schema.partitionKeys(), options, schema.comment());
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        return isTable(tablePath(tablePath));
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        Path path = tablePath(tablePath);
        if (!uncheck(() -> fs.exists(path))) {
            if (ignoreIfNotExists) {
                return;
            }

            throw new TableNotExistException(getName(), tablePath);
        }

        uncheck(() -> fs.delete(path, true));
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        if (!databaseExists(tablePath.getDatabaseName())) {
            throw new DatabaseNotExistException(getName(), tablePath.getDatabaseName());
        }

        Path path = tablePath(tablePath);
        if (uncheck(() -> fs.exists(path))) {
            if (ignoreIfExists) {
                return;
            }

            throw new TableAlreadyExistException(getName(), tablePath);
        }

        commitTableChange(path, table);
    }

    @Override
    public void alterTable(
            ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        Path path = tablePath(tablePath);
        if (uncheck(() -> !fs.exists(path))) {
            if (ignoreIfNotExists) {
                return;
            }

            throw new TableNotExistException(getName(), tablePath);
        }

        commitTableChange(path, newTable);
    }

    private static <T> T uncheck(Callable<T> callable) {
        try {
            return callable.call();
        } catch (Exception e) {
            throw new CatalogException(e);
        }
    }

    private static void uncheck(RunnableWithException runnable) {
        try {
            runnable.run();
        } catch (Exception e) {
            throw new CatalogException(e);
        }
    }

    private static ClassLoader classLoader() {
        return FileSystemCatalog.class.getClassLoader();
    }

    private static boolean isDatabase(Path path) {
        return path.getName().endsWith(DB_SUFFIX);
    }

    private static String database(Path path) {
        String name = path.getName();
        return name.substring(0, name.length() - DB_SUFFIX.length());
    }

    private Path databasePath(String database) {
        return new Path(root, database + DB_SUFFIX);
    }

    private static boolean isTable(Path path) {
        return new SchemaManager(path).listAllIds().size() > 0;
    }

    private Path tablePath(ObjectPath objectPath) {
        return new Path(databasePath(objectPath.getDatabaseName()), objectPath.getObjectName());
    }

    private void commitTableChange(Path tablePath, CatalogBaseTable table) {
        SchemaManager schemaManager = new SchemaManager(tablePath);
        UpdateSchema updateSchema = createUpdateSchema(table, tablePath);
        uncheck(() -> schemaManager.commitNewVersion(updateSchema));
    }

    private ResolvedCatalogTable castToResolved(CatalogBaseTable table) {
        if (!(table instanceof ResolvedCatalogTable)) {
            throw new UnsupportedOperationException(
                    "Only support ResolvedCatalogTable, but is: " + table.getClass());
        }

        return (ResolvedCatalogTable) table;
    }

    private UpdateSchema createUpdateSchema(CatalogBaseTable table, Path tablePath) {
        ResolvedCatalogTable resolvedTable = castToResolved(table);
        ResolvedSchema resolvedSchema = resolvedTable.getResolvedSchema();
        if (resolvedSchema.getColumns().stream().anyMatch(column -> !column.isPhysical())) {
            throw new UnsupportedOperationException("TODO: Non physical column is unsupported.");
        }

        if (resolvedSchema.getWatermarkSpecs().size() > 0) {
            throw new UnsupportedOperationException("TODO: Watermark is unsupported.");
        }

        // remove table path
        Map<String, String> options = resolvedTable.getOptions();
        String specific = options.remove(PATH.key());
        if (specific != null) {
            if (!tablePath.equals(new Path(specific))) {
                throw new IllegalArgumentException(
                        "Illegal table path in table options: " + specific);
            }
            resolvedTable = resolvedTable.copy(options);
        }

        return UpdateSchema.fromCatalogTable(resolvedTable);
    }
}
