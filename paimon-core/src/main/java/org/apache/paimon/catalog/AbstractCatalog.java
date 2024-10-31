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
import org.apache.paimon.lineage.LineageMetaFactory;
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
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.system.SystemTableLoader;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.TYPE;
import static org.apache.paimon.CoreOptions.createCommitUser;
import static org.apache.paimon.TableType.FORMAT_TABLE;
import static org.apache.paimon.options.CatalogOptions.ALLOW_UPPER_CASE;
import static org.apache.paimon.options.CatalogOptions.LINEAGE_META;
import static org.apache.paimon.options.CatalogOptions.LOCK_ENABLED;
import static org.apache.paimon.options.CatalogOptions.LOCK_TYPE;
import static org.apache.paimon.utils.BranchManager.DEFAULT_MAIN_BRANCH;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Common implementation of {@link Catalog}. */
public abstract class AbstractCatalog implements Catalog {

    protected final FileIO fileIO;
    protected final Map<String, String> tableDefaultOptions;
    protected final Options catalogOptions;

    @Nullable protected final LineageMetaFactory lineageMetaFactory;

    protected AbstractCatalog(FileIO fileIO) {
        this.fileIO = fileIO;
        this.lineageMetaFactory = null;
        this.tableDefaultOptions = new HashMap<>();
        this.catalogOptions = new Options();
    }

    protected AbstractCatalog(FileIO fileIO, Options options) {
        this.fileIO = fileIO;
        this.lineageMetaFactory =
                findAndCreateLineageMeta(options, AbstractCatalog.class.getClassLoader());
        this.tableDefaultOptions = Catalog.tableDefaultOptions(options.toMap());
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

    @Override
    public Optional<CatalogLockContext> lockContext() {
        return Optional.of(CatalogLockContext.fromOptions(catalogOptions));
    }

    protected boolean lockEnabled() {
        return catalogOptions.getOptional(LOCK_ENABLED).orElse(fileIO.isObjectStore());
    }

    @Override
    public boolean allowUpperCase() {
        return catalogOptions.getOptional(ALLOW_UPPER_CASE).orElse(true);
    }

    @Override
    public void createDatabase(String name, boolean ignoreIfExists, Map<String, String> properties)
            throws DatabaseAlreadyExistException {
        checkNotSystemDatabase(name);
        if (databaseExists(name)) {
            if (ignoreIfExists) {
                return;
            }
            throw new DatabaseAlreadyExistException(name);
        }
        createDatabaseImpl(name, properties);
    }

    @Override
    public Map<String, String> loadDatabaseProperties(String name)
            throws DatabaseNotExistException {
        if (isSystemDatabase(name)) {
            return Collections.emptyMap();
        }
        return loadDatabasePropertiesImpl(name);
    }

    protected abstract Map<String, String> loadDatabasePropertiesImpl(String name)
            throws DatabaseNotExistException;

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

    protected abstract void createDatabaseImpl(String name, Map<String, String> properties);

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException {
        checkNotSystemDatabase(name);
        if (!databaseExists(name)) {
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
    public List<String> listTables(String databaseName) throws DatabaseNotExistException {
        if (isSystemDatabase(databaseName)) {
            return SystemTableLoader.loadGlobalTableNames();
        }
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(databaseName);
        }

        return listTablesImpl(databaseName).stream().sorted().collect(Collectors.toList());
    }

    protected abstract List<String> listTablesImpl(String databaseName);

    @Override
    public void dropTable(Identifier identifier, boolean ignoreIfNotExists)
            throws TableNotExistException {
        checkNotBranch(identifier, "dropTable");
        checkNotSystemTable(identifier, "dropTable");

        if (!tableExists(identifier)) {
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
        validateIdentifierNameCaseInsensitive(identifier);
        validateFieldNameCaseInsensitive(schema.rowType().getFieldNames());
        validateAutoCreateClose(schema.options());

        if (!databaseExists(identifier.getDatabaseName())) {
            throw new DatabaseNotExistException(identifier.getDatabaseName());
        }

        if (tableExists(identifier)) {
            if (ignoreIfExists) {
                return;
            }
            throw new TableAlreadyExistException(identifier);
        }

        copyTableDefaultOptions(schema.options());

        if (Options.fromMap(schema.options()).get(TYPE) == FORMAT_TABLE) {
            createFormatTable(identifier, schema);
        } else {
            createTableImpl(identifier, schema);
        }
    }

    protected abstract void createTableImpl(Identifier identifier, Schema schema);

    @Override
    public void renameTable(Identifier fromTable, Identifier toTable, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException {
        checkNotBranch(fromTable, "renameTable");
        checkNotBranch(toTable, "renameTable");
        checkNotSystemTable(fromTable, "renameTable");
        checkNotSystemTable(toTable, "renameTable");
        validateIdentifierNameCaseInsensitive(toTable);

        if (!tableExists(fromTable)) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new TableNotExistException(fromTable);
        }

        if (tableExists(toTable)) {
            throw new TableAlreadyExistException(toTable);
        }

        renameTableImpl(fromTable, toTable);
    }

    protected abstract void renameTableImpl(Identifier fromTable, Identifier toTable);

    @Override
    public void alterTable(
            Identifier identifier, List<SchemaChange> changes, boolean ignoreIfNotExists)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {
        checkNotSystemTable(identifier, "alterTable");
        validateIdentifierNameCaseInsensitive(identifier);
        validateFieldNameCaseInsensitiveInSchemaChange(changes);

        if (!tableExists(identifier)) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new TableNotExistException(identifier);
        }

        alterTableImpl(identifier, changes);
    }

    protected abstract void alterTableImpl(Identifier identifier, List<SchemaChange> changes)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException;

    @Nullable
    private LineageMetaFactory findAndCreateLineageMeta(Options options, ClassLoader classLoader) {
        return options.getOptional(LINEAGE_META)
                .map(
                        meta ->
                                FactoryUtil.discoverFactory(
                                        classLoader, LineageMetaFactory.class, meta))
                .orElse(null);
    }

    @Override
    public Table getTable(Identifier identifier) throws TableNotExistException {
        if (isSystemDatabase(identifier.getDatabaseName())) {
            String tableName = identifier.getTableName();
            Table table =
                    SystemTableLoader.loadGlobal(
                            tableName,
                            fileIO,
                            this::allTablePaths,
                            catalogOptions,
                            lineageMetaFactory);
            if (table == null) {
                throw new TableNotExistException(identifier);
            }
            return table;
        } else if (isSpecifiedSystemTable(identifier)) {
            Table originTable =
                    getDataOrFormatTable(
                            new Identifier(
                                    identifier.getDatabaseName(),
                                    identifier.getTableName(),
                                    identifier.getBranchName(),
                                    null));
            if (!(originTable instanceof FileStoreTable)) {
                throw new UnsupportedOperationException(
                        String.format(
                                "Only data table support system tables, but this table %s is %s.",
                                identifier, originTable.getClass()));
            }
            Table table =
                    SystemTableLoader.load(
                            Preconditions.checkNotNull(identifier.getSystemTableName()),
                            (FileStoreTable) originTable);
            if (table == null) {
                throw new TableNotExistException(identifier);
            }
            return table;
        } else {
            return getDataOrFormatTable(identifier);
        }
    }

    protected Table getDataOrFormatTable(Identifier identifier) throws TableNotExistException {
        Preconditions.checkArgument(identifier.getSystemTableName() == null);
        return FileStoreTableFactory.create(
                fileIO,
                getTableLocation(identifier),
                getDataTableSchema(identifier),
                new CatalogEnvironment(
                        identifier,
                        Lock.factory(
                                lockFactory().orElse(null), lockContext().orElse(null), identifier),
                        metastoreClientFactory(identifier).orElse(null),
                        lineageMetaFactory));
    }

    protected CatalogEnvironment catalogEnvironment(Identifier identifier)
            throws TableNotExistException {
        return new CatalogEnvironment(
                identifier,
                Lock.factory(lockFactory().orElse(null), lockContext().orElse(null), identifier),
                metastoreClientFactory(identifier).orElse(null),
                lineageMetaFactory);
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

    protected abstract TableSchema getDataTableSchema(Identifier identifier)
            throws TableNotExistException;

    @Override
    public Path getTableLocation(Identifier identifier) {
        return new Path(newDatabasePath(identifier.getDatabaseName()), identifier.getTableName());
    }

    protected static void checkNotBranch(Identifier identifier, String method) {
        if (identifier.getBranchName() != null) {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot '%s' for branch table '%s', "
                                    + "please modify the table with the default branch.",
                            method, identifier));
        }
    }

    protected void assertMainBranch(Identifier identifier) {
        if (identifier.getBranchName() != null
                && !DEFAULT_MAIN_BRANCH.equals(identifier.getBranchName())) {
            throw new UnsupportedOperationException(
                    this.getClass().getName() + " currently does not support table branches");
        }
    }

    public static boolean isSpecifiedSystemTable(Identifier identifier) {
        return identifier.getSystemTableName() != null;
    }

    protected static boolean isTableInSystemDatabase(Identifier identifier) {
        return isSystemDatabase(identifier.getDatabaseName()) || isSpecifiedSystemTable(identifier);
    }

    protected static void checkNotSystemTable(Identifier identifier, String method) {
        if (isTableInSystemDatabase(identifier)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot '%s' for system table '%s', please use data table.",
                            method, identifier));
        }
    }

    private void copyTableDefaultOptions(Map<String, String> options) {
        tableDefaultOptions.forEach(options::putIfAbsent);
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

    public static boolean isSystemDatabase(String database) {
        return SYSTEM_DATABASE_NAME.equals(database);
    }

    /** Validate database cannot be a system database. */
    protected void checkNotSystemDatabase(String database) {
        if (isSystemDatabase(database)) {
            throw new ProcessSystemDatabaseException();
        }
    }

    protected void validateIdentifierNameCaseInsensitive(Identifier identifier) {
        Catalog.validateCaseInsensitive(allowUpperCase(), "Database", identifier.getDatabaseName());
        Catalog.validateCaseInsensitive(allowUpperCase(), "Table", identifier.getObjectName());
    }

    private void validateFieldNameCaseInsensitiveInSchemaChange(List<SchemaChange> changes) {
        List<String> fieldNames = new ArrayList<>();
        for (SchemaChange change : changes) {
            if (change instanceof SchemaChange.AddColumn) {
                SchemaChange.AddColumn addColumn = (SchemaChange.AddColumn) change;
                fieldNames.add(addColumn.fieldName());
            } else if (change instanceof SchemaChange.RenameColumn) {
                SchemaChange.RenameColumn rename = (SchemaChange.RenameColumn) change;
                fieldNames.add(rename.newName());
            }
        }
        validateFieldNameCaseInsensitive(fieldNames);
    }

    protected void validateFieldNameCaseInsensitive(List<String> fieldNames) {
        Catalog.validateCaseInsensitive(allowUpperCase(), "Field", fieldNames);
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
        return !new SchemaManager(fileIO, tablePath, branchName).listAllIds().isEmpty();
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
}
