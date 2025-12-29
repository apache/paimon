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
import org.apache.paimon.PagedList;
import org.apache.paimon.Snapshot;
import org.apache.paimon.factories.FactoryUtil;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.function.Function;
import org.apache.paimon.function.FunctionChange;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.rest.responses.GetTagResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.Instant;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.TableSnapshot;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.system.SystemTableLoader;
import org.apache.paimon.utils.SnapshotNotExistException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.DATA_FILE_EXTERNAL_PATHS;
import static org.apache.paimon.CoreOptions.PATH;
import static org.apache.paimon.CoreOptions.TYPE;
import static org.apache.paimon.catalog.CatalogUtils.checkNotBranch;
import static org.apache.paimon.catalog.CatalogUtils.checkNotSystemDatabase;
import static org.apache.paimon.catalog.CatalogUtils.checkNotSystemTable;
import static org.apache.paimon.catalog.CatalogUtils.isSystemDatabase;
import static org.apache.paimon.catalog.CatalogUtils.listPartitionsFromFileSystem;
import static org.apache.paimon.catalog.CatalogUtils.validateCreateTable;
import static org.apache.paimon.catalog.Identifier.DEFAULT_MAIN_BRANCH;
import static org.apache.paimon.options.CatalogOptions.LOCK_ENABLED;
import static org.apache.paimon.options.CatalogOptions.LOCK_TYPE;

/** Common implementation of {@link Catalog}. */
public abstract class AbstractCatalog implements Catalog {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractCatalog.class);

    protected final FileIO fileIO;
    protected final Map<String, String> tableDefaultOptions;
    protected final CatalogContext context;

    protected AbstractCatalog(FileIO fileIO) {
        this.fileIO = fileIO;
        this.tableDefaultOptions = new HashMap<>();
        this.context = CatalogContext.create(new Options());
    }

    protected AbstractCatalog(FileIO fileIO, CatalogContext context) {
        this.fileIO = fileIO;
        this.tableDefaultOptions = CatalogUtils.tableDefaultOptions(context.options().toMap());
        this.context = context;
    }

    @Override
    public Map<String, String> options() {
        return context.options().toMap();
    }

    public abstract String warehouse();

    public FileIO fileIO() {
        return fileIO;
    }

    protected FileIO fileIO(Path path) {
        return fileIO;
    }

    public Optional<CatalogLockFactory> lockFactory() {
        if (!lockEnabled()) {
            return Optional.empty();
        }

        String lock = context.options().get(LOCK_TYPE);
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
        return Optional.of(CatalogLockContext.fromOptions(context.options()));
    }

    protected boolean lockEnabled() {
        return context.options().getOptional(LOCK_ENABLED).orElse(fileIO.isObjectStore());
    }

    protected boolean allowCustomTablePath() {
        return false;
    }

    @Override
    public PagedList<String> listDatabasesPaged(
            Integer maxResults, String pageToken, String databaseNamePattern) {
        CatalogUtils.validateNamePattern(this, databaseNamePattern);
        return new PagedList<>(listDatabases(), null);
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
    public void markDonePartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException {}

    @Override
    public List<Partition> listPartitions(Identifier identifier) throws TableNotExistException {
        return listPartitionsFromFileSystem(getTable(identifier));
    }

    @Override
    public PagedList<Partition> listPartitionsPaged(
            Identifier identifier,
            Integer maxResults,
            String pageToken,
            String partitionNamePattern)
            throws TableNotExistException {
        CatalogUtils.validateNamePattern(this, partitionNamePattern);
        return new PagedList<>(listPartitions(identifier), null);
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
            this.alterDatabaseImpl(name, changes);
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

    @Override
    public PagedList<String> listTablesPaged(
            String databaseName,
            Integer maxResults,
            String pageToken,
            String tableNamePattern,
            String tableType)
            throws DatabaseNotExistException {
        CatalogUtils.validateNamePattern(this, tableNamePattern);
        CatalogUtils.validateTableType(this, tableType);
        return new PagedList<>(listTables(databaseName), null);
    }

    @Override
    public PagedList<Table> listTableDetailsPaged(
            String databaseName,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String tableNamePattern,
            @Nullable String tableType)
            throws DatabaseNotExistException {
        CatalogUtils.validateNamePattern(this, tableNamePattern);
        CatalogUtils.validateTableType(this, tableType);
        if (isSystemDatabase(databaseName)) {
            List<Table> systemTables =
                    SystemTableLoader.loadGlobalTableNames().stream()
                            .map(
                                    tableName -> {
                                        try {
                                            return getTable(
                                                    Identifier.create(databaseName, tableName));
                                        } catch (TableNotExistException ignored) {
                                            LOG.warn(
                                                    "system table {}.{} does not exist",
                                                    databaseName,
                                                    tableName);
                                            return null;
                                        }
                                    })
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList());
            return new PagedList<>(systemTables, null);
        }

        // check db exists
        getDatabase(databaseName);

        return listTableDetailsPagedImpl(
                databaseName, maxResults, pageToken, tableNamePattern, tableType);
    }

    protected abstract List<String> listTablesImpl(String databaseName);

    protected PagedList<Table> listTableDetailsPagedImpl(
            String databaseName,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String tableNamePattern,
            @Nullable String tableType)
            throws DatabaseNotExistException {
        PagedList<String> pagedTableNames =
                listTablesPaged(databaseName, maxResults, pageToken, tableNamePattern, tableType);
        return new PagedList<>(
                pagedTableNames.getElements().stream()
                        .map(
                                tableName -> {
                                    try {
                                        return getTable(Identifier.create(databaseName, tableName));
                                    } catch (TableNotExistException ignored) {
                                        LOG.warn(
                                                "table {}.{} does not exist",
                                                databaseName,
                                                tableName);
                                        return null;
                                    }
                                })
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList()),
                pagedTableNames.getNextPageToken());
    }

    @Override
    public void dropTable(Identifier identifier, boolean ignoreIfNotExists)
            throws TableNotExistException {
        checkNotBranch(identifier, "dropTable");
        checkNotSystemTable(identifier, "dropTable");

        Set<Path> externalPaths = new HashSet<>();
        try {
            Table table = getTable(identifier);
            if (table instanceof FileStoreTable) {
                FileStoreTable fileStoreTable = (FileStoreTable) table;
                List<Path> schemaExternalPaths =
                        getSchemaExternalPaths(fileStoreTable.schemaManager().listAll());
                externalPaths.addAll(schemaExternalPaths);
                // get table branch external path
                List<String> branches = fileStoreTable.branchManager().branches();
                for (String branch : branches) {
                    SchemaManager schemaManager =
                            fileStoreTable.schemaManager().copyWithBranch(branch);
                    externalPaths.addAll(getSchemaExternalPaths(schemaManager.listAll()));
                }
            }
        } catch (TableNotExistException e) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new TableNotExistException(identifier);
        }

        dropTableImpl(identifier, new ArrayList<>(externalPaths));
    }

    private List<Path> getSchemaExternalPaths(List<TableSchema> schemas) {
        if (schemas == null) {
            return Collections.emptyList();
        }
        return schemas.stream()
                .map(schema -> schema.toSchema().options().get(DATA_FILE_EXTERNAL_PATHS.key()))
                .filter(Objects::nonNull)
                .flatMap(externalPath -> Arrays.stream(externalPath.split(",")))
                .map(Path::new)
                .distinct()
                .collect(Collectors.toList());
    }

    protected abstract void dropTableImpl(Identifier identifier, List<Path> externalPaths);

    @Override
    public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException {
        checkNotBranch(identifier, "createTable");
        checkNotSystemTable(identifier, "createTable");
        validateCreateTable(schema, false);
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
            case FORMAT_TABLE:
                createFormatTable(identifier, schema);
                break;
            case OBJECT_TABLE:
                throw new UnsupportedOperationException(
                        String.format(
                                "Catalog %s cannot support object tables.",
                                this.getClass().getName()));
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
        return CatalogUtils.loadTable(
                this,
                identifier,
                p -> fileIO(),
                this::fileIO,
                this::loadTableMetadata,
                lockFactory().orElse(null),
                lockContext().orElse(null),
                context,
                false);
    }

    @Override
    public void createBranch(Identifier identifier, String branch, @Nullable String fromTag)
            throws TableNotExistException, BranchAlreadyExistException, TagNotExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropBranch(Identifier identifier, String branch) throws BranchNotExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void fastForward(Identifier identifier, String branch) throws BranchNotExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> listBranches(Identifier identifier) throws TableNotExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public GetTagResponse getTag(Identifier identifier, String tagName)
            throws TableNotExistException, TagNotExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createTag(
            Identifier identifier,
            String tagName,
            @Nullable Long snapshotId,
            @Nullable String timeRetained,
            boolean ignoreIfExists)
            throws TableNotExistException, SnapshotNotExistException, TagAlreadyExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PagedList<String> listTagsPaged(
            Identifier identifier, @Nullable Integer maxResults, @Nullable String pageToken)
            throws TableNotExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteTag(Identifier identifier, String tagName)
            throws TableNotExistException, TagNotExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean commitSnapshot(
            Identifier identifier,
            @Nullable String tableUuid,
            Snapshot snapshot,
            List<PartitionStatistics> statistics) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<TableSnapshot> loadSnapshot(Identifier identifier) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Snapshot> loadSnapshot(Identifier identifier, String version) {
        throw new UnsupportedOperationException();
    }

    @Override
    public PagedList<Snapshot> listSnapshotsPaged(
            Identifier identifier, @Nullable Integer maxResults, @Nullable String pageToken) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rollbackTo(Identifier identifier, Instant instant, @Nullable Long fromSnapshot)
            throws Catalog.TableNotExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsListObjectsPaged() {
        return false;
    }

    @Override
    public boolean supportsVersionManagement() {
        return false;
    }

    @Override
    public List<String> authTableQuery(Identifier identifier, List<String> select) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createPartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException {}

    @Override
    public void dropPartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException {
        Table table = getTable(identifier);
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.truncatePartitions(partitions);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void alterPartitions(Identifier identifier, List<PartitionStatistics> partitions)
            throws TableNotExistException {}

    @Override
    public List<String> listFunctions(String databaseName) {
        return Collections.emptyList();
    }

    @Override
    public Function getFunction(Identifier identifier) throws FunctionNotExistException {
        throw new FunctionNotExistException(identifier);
    }

    @Override
    public void createFunction(Identifier identifier, Function function, boolean ignoreIfExists)
            throws FunctionAlreadyExistException, DatabaseNotExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropFunction(Identifier identifier, boolean ignoreIfNotExists)
            throws FunctionNotExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterFunction(
            Identifier identifier, List<FunctionChange> changes, boolean ignoreIfNotExists)
            throws FunctionNotExistException, DefinitionAlreadyExistException,
                    DefinitionNotExistException {
        throw new UnsupportedOperationException();
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
        return new TableMetadata(loadTableSchema(identifier), false, null);
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
