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
import org.apache.paimon.TableType;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.metastore.MetastoreClient;
import org.apache.paimon.operation.FileStoreCommit;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.object.ObjectTable;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.system.SystemTableLoader;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.TYPE;
import static org.apache.paimon.CoreOptions.createCommitUser;
import static org.apache.paimon.catalog.CatalogUtils.checkNotBranch;
import static org.apache.paimon.catalog.CatalogUtils.checkNotSystemDatabase;
import static org.apache.paimon.catalog.CatalogUtils.checkNotSystemTable;
import static org.apache.paimon.catalog.CatalogUtils.isSystemDatabase;
import static org.apache.paimon.catalog.CatalogUtils.lockFactory;
import static org.apache.paimon.utils.BranchManager.DEFAULT_MAIN_BRANCH;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

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

    public Optional<CatalogLockFactory> defaultLockFactory() {
        return Optional.empty();
    }

    public Optional<CatalogLockContext> lockContext() {
        return CatalogUtils.lockContext(catalogOptions);
    }

    protected boolean lockEnabled() {
        return CatalogUtils.lockEnabled(catalogOptions, fileIO);
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
    public void createPartition(Identifier identifier, Map<String, String> partitionSpec)
            throws TableNotExistException {
        Identifier tableIdentifier =
                Identifier.create(identifier.getDatabaseName(), identifier.getTableName());
        FileStoreTable table = (FileStoreTable) getTable(tableIdentifier);

        if (table.partitionKeys().isEmpty() || !table.coreOptions().partitionedTableInMetastore()) {
            throw new UnsupportedOperationException(
                    "The table is not partitioned table in metastore.");
        }

        MetastoreClient.Factory metastoreFactory =
                table.catalogEnvironment().metastoreClientFactory();
        if (metastoreFactory == null) {
            throw new UnsupportedOperationException(
                    "The catalog must have metastore to create partition.");
        }

        try (MetastoreClient client = metastoreFactory.create()) {
            client.addPartition(new LinkedHashMap<>(partitionSpec));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void dropPartition(Identifier identifier, Map<String, String> partitionSpec)
            throws TableNotExistException {
        checkNotSystemTable(identifier, "dropPartition");
        Table table = getTable(identifier);
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        try (FileStoreCommit commit =
                fileStoreTable
                        .store()
                        .newCommit(
                                createCommitUser(fileStoreTable.coreOptions().toConfiguration()))) {
            commit.dropPartitions(
                    Collections.singletonList(partitionSpec), BatchWriteBuilder.COMMIT_IDENTIFIER);
        }
    }

    @Override
    public List<PartitionEntry> listPartitions(Identifier identifier)
            throws TableNotExistException {
        return getTable(identifier).newReadBuilder().newScan().listPartitionEntries();
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
                schema.options().containsKey(CoreOptions.OBJECT_LOCATION.key()),
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
        if (isSystemDatabase(identifier.getDatabaseName())) {
            String tableName = identifier.getTableName();
            Table table =
                    SystemTableLoader.loadGlobal(
                            tableName, fileIO, this::allTablePaths, catalogOptions);
            if (table == null) {
                throw new TableNotExistException(identifier);
            }
            return table;
        } else if (identifier.isSystemTable()) {
            Table originTable =
                    getDataOrFormatTable(
                            new Identifier(
                                    identifier.getDatabaseName(),
                                    identifier.getTableName(),
                                    identifier.getBranchName(),
                                    null));
            return CatalogUtils.getSystemTable(identifier, originTable);
        } else {
            return getDataOrFormatTable(identifier);
        }
    }

    protected Table getDataOrFormatTable(Identifier identifier) throws TableNotExistException {
        Preconditions.checkArgument(identifier.getSystemTableName() == null);
        TableMeta tableMeta = getDataTableMeta(identifier);
        FileStoreTable table =
                FileStoreTableFactory.create(
                        fileIO,
                        getTableLocation(identifier),
                        tableMeta.schema,
                        new CatalogEnvironment(
                                identifier,
                                tableMeta.uuid,
                                Lock.factory(
                                        lockFactory(catalogOptions, fileIO(), defaultLockFactory())
                                                .orElse(null),
                                        lockContext().orElse(null),
                                        identifier),
                                metastoreClientFactory(identifier).orElse(null)));
        CoreOptions options = table.coreOptions();
        if (options.type() == TableType.OBJECT_TABLE) {
            String objectLocation = options.objectLocation();
            checkNotNull(objectLocation, "Object location should not be null for object table.");
            table =
                    ObjectTable.builder()
                            .underlyingTable(table)
                            .objectLocation(objectLocation)
                            .objectFileIO(objectFileIO(objectLocation))
                            .build();
        }
        return table;
    }

    /**
     * Catalog implementation may override this method to provide {@link FileIO} to object table.
     */
    protected FileIO objectFileIO(String objectLocation) {
        return fileIO;
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
        return CatalogUtils.newDatabasePath(warehouse(), database);
    }

    public Map<String, Map<String, Path>> allTablePaths() {
        try {
            Map<String, Map<String, Path>> allPaths = new HashMap<>();
            for (String database : listDatabases()) {
                Map<String, Path> tableMap =
                        allPaths.computeIfAbsent(database, d -> new HashMap<>());
                for (String table : listTables(database)) {
                    tableMap.put(table, getTableLocation(Identifier.create(database, table)));
                }
            }
            return allPaths;
        } catch (DatabaseNotExistException e) {
            throw new RuntimeException("Database is deleted while listing", e);
        }
    }

    protected TableMeta getDataTableMeta(Identifier identifier) throws TableNotExistException {
        return new TableMeta(getDataTableSchema(identifier), null);
    }

    protected abstract TableSchema getDataTableSchema(Identifier identifier)
            throws TableNotExistException;

    /** Get metastore client factory for the table specified by {@code identifier}. */
    public Optional<MetastoreClient.Factory> metastoreClientFactory(Identifier identifier) {
        return Optional.empty();
    }

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

    private void copyTableDefaultOptions(Map<String, String> options) {
        tableDefaultOptions.forEach(options::putIfAbsent);
    }

    private void validateAutoCreateClose(Map<String, String> options) {
        checkArgument(
                !Boolean.parseBoolean(
                        options.getOrDefault(
                                CoreOptions.AUTO_CREATE.key(),
                                CoreOptions.AUTO_CREATE.defaultValue().toString())),
                String.format(
                        "The value of %s property should be %s.",
                        CoreOptions.AUTO_CREATE.key(), Boolean.FALSE));
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
        return new SchemaManager(fileIO, tablePath, branchName)
                .latest()
                .map(
                        s -> {
                            if (!DEFAULT_MAIN_BRANCH.equals(branchName)) {
                                Options branchOptions = new Options(s.options());
                                branchOptions.set(CoreOptions.BRANCH, branchName);
                                return s.copy(branchOptions.toMap());
                            } else {
                                return s;
                            }
                        });
    }

    /** Table metadata. */
    protected static class TableMeta {

        private final TableSchema schema;
        @Nullable private final String uuid;

        public TableMeta(TableSchema schema, @Nullable String uuid) {
            this.schema = schema;
            this.uuid = uuid;
        }

        public TableSchema schema() {
            return schema;
        }

        @Nullable
        public String uuid() {
            return uuid;
        }
    }
}
