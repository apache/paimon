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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.factories.FactoryUtil;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.FileStoreCommit;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.object.ObjectTable;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.system.SystemTableLoader;
import org.apache.paimon.types.RowType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.OBJECT_LOCATION;
import static org.apache.paimon.CoreOptions.PATH;
import static org.apache.paimon.CoreOptions.TYPE;
import static org.apache.paimon.CoreOptions.createCommitUser;
import static org.apache.paimon.catalog.CatalogUtils.checkNotBranch;
import static org.apache.paimon.catalog.CatalogUtils.checkNotSystemDatabase;
import static org.apache.paimon.catalog.CatalogUtils.checkNotSystemTable;
import static org.apache.paimon.catalog.CatalogUtils.isSystemDatabase;
import static org.apache.paimon.catalog.CatalogUtils.listPartitionsFromFileSystem;
import static org.apache.paimon.catalog.CatalogUtils.validateAutoCreateClose;
import static org.apache.paimon.options.CatalogOptions.LOCK_ENABLED;
import static org.apache.paimon.options.CatalogOptions.LOCK_TYPE;
import static org.apache.paimon.utils.BranchManager.DEFAULT_MAIN_BRANCH;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Common implementation of {@link Catalog}. */
public abstract class AbstractCatalog implements Catalog {

    protected final FileIO fileIO;
    protected final Map<String, String> tableDefaultOptions;
    protected final Options catalogOptions;

    protected AbstractCatalog(FileIO fileIO) {
        this.fileIO = fileIO;
        this.tableDefaultOptions = new HashMap<>();
        this.catalogOptions = new Options();
    }

    protected AbstractCatalog(FileIO fileIO, Options options) {
        this.fileIO = fileIO;
        this.tableDefaultOptions = CatalogUtils.tableDefaultOptions(options.toMap());
        this.catalogOptions = options;
    }

    @Override
    public Map<String, String> options() {
        return catalogOptions.toMap();
    }

    @Override
    public FileIO fileIO() {
        return fileIO;
    }

    @Override
    public FileIO fileIO(Path path) {
        return fileIO;
    }

    public Optional<CatalogLockFactory> lockFactory() {
        if (!lockEnabled()) {
            return Optional.empty();
        }

        String lock = catalogOptions.get(LOCK_TYPE);
        if (lock == null) {
            return defaultLockFactory();
        }

        return Optional.of(
                FactoryUtil.discoverFactory(
                        AbstractCatalog.class.getClassLoader(), CatalogLockFactory.class, lock));
    }

    public Optional<CatalogLockFactory> defaultLockFactory() {
        return Optional.empty();
    }

    public Optional<CatalogLockContext> lockContext() {
        return Optional.of(CatalogLockContext.fromOptions(catalogOptions));
    }

    protected boolean lockEnabled() {
        return catalogOptions.getOptional(LOCK_ENABLED).orElse(fileIO.isObjectStore());
    }

    protected boolean allowCustomTablePath() {
        return false;
    }

    @Override
    public void createDatabase(String name, boolean ignoreIfExists, Map<String, String> properties)
            throws DatabaseAlreadyExistException {
        checkNotSystemDatabase(name);
        try {
            getDatabase(name);
            if (ignoreIfExists) {
                return;
            }
            throw new DatabaseAlreadyExistException(name);
        } catch (DatabaseNotExistException ignored) {
        }
        createDatabaseImpl(name, properties);
    }

    @Override
    public Database getDatabase(String name) throws DatabaseNotExistException {
        if (isSystemDatabase(name)) {
            return Database.of(name);
        }
        return getDatabaseImpl(name);
    }

    protected abstract Database getDatabaseImpl(String name) throws DatabaseNotExistException;

    @Override
    public void createPartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException {}

    @Override
    public void dropPartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException {
        checkNotSystemTable(identifier, "dropPartition");
        Table table = getTable(identifier);
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        try (FileStoreCommit commit =
                fileStoreTable
                        .store()
                        .newCommit(
                                createCommitUser(fileStoreTable.coreOptions().toConfiguration()))) {
            commit.dropPartitions(partitions, BatchWriteBuilder.COMMIT_IDENTIFIER);
        }
    }

    @Override
    public void alterPartitions(Identifier identifier, List<Partition> partitions)
            throws TableNotExistException {}

    @Override
    public void markDonePartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException {}

    @Override
    public List<Partition> listPartitions(Identifier identifier) throws TableNotExistException {
        return listPartitionsFromFileSystem(getTable(identifier));
    }

    protected abstract void createDatabaseImpl(String name, Map<String, String> properties);

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException {
        checkNotSystemDatabase(name);
        try {
            getDatabase(name);
        } catch (DatabaseNotExistException e) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new DatabaseNotExistException(name);
        }

        if (!cascade && !listTables(name).isEmpty()) {
            throw new DatabaseNotEmptyException(name);
        }

        dropDatabaseImpl(name);
    }

    protected abstract void dropDatabaseImpl(String name);

    @Override
    public void alterDatabase(String name, List<PropertyChange> changes, boolean ignoreIfNotExists)
            throws DatabaseNotExistException {
        checkNotSystemDatabase(name);
        try {
            if (changes == null || changes.isEmpty()) {
                return;
            }
            alterDatabaseImpl(name, changes);
        } catch (DatabaseNotExistException e) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new DatabaseNotExistException(name);
        }
    }

    protected abstract void alterDatabaseImpl(String name, List<PropertyChange> changes)
            throws DatabaseNotExistException;

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException {
        if (isSystemDatabase(databaseName)) {
            return SystemTableLoader.loadGlobalTableNames();
        }

        // check db exists
        getDatabase(databaseName);

        return listTablesImpl(databaseName).stream().sorted().collect(Collectors.toList());
    }

    protected abstract List<String> listTablesImpl(String databaseName);

    @Override
    public void dropTable(Identifier identifier, boolean ignoreIfNotExists)
            throws TableNotExistException {
        checkNotBranch(identifier, "dropTable");
        checkNotSystemTable(identifier, "dropTable");

        try {
            getTable(identifier);
        } catch (TableNotExistException e) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new TableNotExistException(identifier);
        }

        dropTableImpl(identifier);
    }

    protected abstract void dropTableImpl(Identifier identifier);

    @Override
    public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException {
        checkNotBranch(identifier, "createTable");
        checkNotSystemTable(identifier, "createTable");
        validateAutoCreateClose(schema.options());
        validateCustomTablePath(schema.options());

        // check db exists
        getDatabase(identifier.getDatabaseName());

        try {
            getTable(identifier);
            if (ignoreIfExists) {
                return;
            }
            throw new TableAlreadyExistException(identifier);
        } catch (TableNotExistException ignored) {
        }

        copyTableDefaultOptions(schema.options());

        switch (Options.fromMap(schema.options()).get(TYPE)) {
            case TABLE:
            case MATERIALIZED_TABLE:
                createTableImpl(identifier, schema);
                break;
            case OBJECT_TABLE:
                createObjectTable(identifier, schema);
                break;
            case FORMAT_TABLE:
                createFormatTable(identifier, schema);
                break;
        }
    }

    private void createObjectTable(Identifier identifier, Schema schema) {
        RowType rowType = schema.rowType();
        checkArgument(
                rowType.getFields().isEmpty()
                        || new HashSet<>(ObjectTable.SCHEMA.getFields())
                                .containsAll(rowType.getFields()),
                "Schema of Object Table can be empty or %s, but is %s.",
                ObjectTable.SCHEMA,
                rowType);
        checkArgument(
                schema.options().containsKey(OBJECT_LOCATION.key()),
                "Object table should have object-location option.");
        createTableImpl(identifier, schema.copy(ObjectTable.SCHEMA));
    }

    protected abstract void createTableImpl(Identifier identifier, Schema schema);

    @Override
    public void renameTable(Identifier fromTable, Identifier toTable, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException {
        checkNotBranch(fromTable, "renameTable");
        checkNotBranch(toTable, "renameTable");
        checkNotSystemTable(fromTable, "renameTable");
        checkNotSystemTable(toTable, "renameTable");

        try {
            getTable(fromTable);
        } catch (TableNotExistException e) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new TableNotExistException(fromTable);
        }

        try {
            getTable(toTable);
            throw new TableAlreadyExistException(toTable);
        } catch (TableNotExistException ignored) {
        }

        renameTableImpl(fromTable, toTable);
    }

    protected abstract void renameTableImpl(Identifier fromTable, Identifier toTable);

    @Override
    public void alterTable(
            Identifier identifier, List<SchemaChange> changes, boolean ignoreIfNotExists)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {
        checkNotSystemTable(identifier, "alterTable");

        try {
            getTable(identifier);
        } catch (TableNotExistException e) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new TableNotExistException(identifier);
        }

        alterTableImpl(identifier, changes);
    }

    protected abstract void alterTableImpl(Identifier identifier, List<SchemaChange> changes)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException;

    @Override
    public Table getTable(Identifier identifier) throws TableNotExistException {
        Lock.Factory lockFactory =
                Lock.factory(lockFactory().orElse(null), lockContext().orElse(null), identifier);
        return CatalogUtils.loadTable(this, identifier, this::loadTableMetadata, lockFactory);
    }

    /**
     * Create a {@link FormatTable} identified by the given {@link Identifier}.
     *
     * @param identifier Path of the table
     * @param schema Schema of the table
     */
    public void createFormatTable(Identifier identifier, Schema schema) {
        throw new UnsupportedOperationException(
                this.getClass().getName() + " currently does not support format table");
    }

    /**
     * Get warehouse path for specified database. If a catalog would like to provide individual path
     * for each database, this method can be `Override` in that catalog.
     *
     * @param database The given database name
     * @return The warehouse path for the database
     */
    public Path newDatabasePath(String database) {
        return newDatabasePath(warehouse(), database);
    }

    protected TableMetadata loadTableMetadata(Identifier identifier) throws TableNotExistException {
        return new TableMetadata(loadTableSchema(identifier), null);
    }

    protected abstract TableSchema loadTableSchema(Identifier identifier)
            throws TableNotExistException;

    public Path getTableLocation(Identifier identifier) {
        return new Path(newDatabasePath(identifier.getDatabaseName()), identifier.getTableName());
    }

    protected void assertMainBranch(Identifier identifier) {
        if (identifier.getBranchName() != null
                && !DEFAULT_MAIN_BRANCH.equals(identifier.getBranchName())) {
            throw new UnsupportedOperationException(
                    this.getClass().getName() + " currently does not support table branches");
        }
    }

    public static Path newTableLocation(String warehouse, Identifier identifier) {
        checkNotBranch(identifier, "newTableLocation");
        checkNotSystemTable(identifier, "newTableLocation");
        return new Path(
                newDatabasePath(warehouse, identifier.getDatabaseName()),
                identifier.getTableName());
    }

    public static Path newDatabasePath(String warehouse, String database) {
        return new Path(warehouse, database + DB_SUFFIX);
    }

    private void copyTableDefaultOptions(Map<String, String> options) {
        tableDefaultOptions.forEach(options::putIfAbsent);
    }

    private void validateCustomTablePath(Map<String, String> options) {
        if (!allowCustomTablePath() && options.containsKey(CoreOptions.PATH.key())) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The current catalog %s does not support specifying the table path when creating a table.",
                            this.getClass().getSimpleName()));
        }
    }

    // =============================== Meta in File System =====================================

    protected List<String> listDatabasesInFileSystem(Path warehouse) throws IOException {
        List<String> databases = new ArrayList<>();
        for (FileStatus status : fileIO.listDirectories(warehouse)) {
            Path path = status.getPath();
            if (status.isDir() && path.getName().endsWith(DB_SUFFIX)) {
                String fileName = path.getName();
                databases.add(fileName.substring(0, fileName.length() - DB_SUFFIX.length()));
            }
        }
        return databases;
    }

    protected List<String> listTablesInFileSystem(Path databasePath) throws IOException {
        List<String> tables = new ArrayList<>();
        for (FileStatus status : fileIO.listDirectories(databasePath)) {
            if (status.isDir() && tableExistsInFileSystem(status.getPath(), DEFAULT_MAIN_BRANCH)) {
                tables.add(status.getPath().getName());
            }
        }
        return tables;
    }

    protected boolean tableExistsInFileSystem(Path tablePath, String branchName) {
        SchemaManager schemaManager = new SchemaManager(fileIO, tablePath, branchName);

        // in order to improve the performance, check the schema-0 firstly.
        boolean schemaZeroExists = schemaManager.schemaExists(0);
        if (schemaZeroExists) {
            return true;
        } else {
            // if schema-0 not exists, fallback to check other schemas
            return !schemaManager.listAllIds().isEmpty();
        }
    }

    public Optional<TableSchema> tableSchemaInFileSystem(Path tablePath, String branchName) {
        Optional<TableSchema> schema = new SchemaManager(fileIO, tablePath, branchName).latest();
        if (!DEFAULT_MAIN_BRANCH.equals(branchName)) {
            schema =
                    schema.map(
                            s -> {
                                Options branchOptions = new Options(s.options());
                                branchOptions.set(CoreOptions.BRANCH, branchName);
                                return s.copy(branchOptions.toMap());
                            });
        }
        schema.ifPresent(s -> s.options().put(PATH.key(), tablePath.toString()));
        return schema;
    }
}
