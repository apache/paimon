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
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.factories.FactoryUtil;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.lineage.LineageMetaFactory;
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
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.system.SystemTableLoader;
import org.apache.paimon.utils.StringUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static org.apache.paimon.options.CatalogOptions.LINEAGE_META;
import static org.apache.paimon.options.CatalogOptions.LOCK_ENABLED;
import static org.apache.paimon.options.CatalogOptions.LOCK_TYPE;
import static org.apache.paimon.options.OptionsUtils.convertToPropertiesPrefixKey;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Common implementation of {@link Catalog}. */
public abstract class AbstractCatalog implements Catalog {

    public static final String DB_SUFFIX = ".db";
    protected static final String TABLE_DEFAULT_OPTION_PREFIX = "table-default.";
    protected static final String DB_LOCATION_PROP = "location";

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
        this.tableDefaultOptions =
                convertToPropertiesPrefixKey(options.toMap(), TABLE_DEFAULT_OPTION_PREFIX);
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
        return catalogOptions.get(LOCK_ENABLED);
    }

    protected List<String> listDatabases(Path warehouse) {
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
    public boolean databaseExists(String databaseName) {
        if (isSystemDatabase(databaseName)) {
            return true;
        }

        return databaseExistsImpl(databaseName);
    }

    protected abstract boolean databaseExistsImpl(String databaseName);

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
        if (!databaseExists(name)) {
            throw new DatabaseNotExistException(name);
        }
        return loadDatabasePropertiesImpl(name);
    }

    protected abstract Map<String, String> loadDatabasePropertiesImpl(String name);

    @Override
    public void dropPartition(Identifier identifier, Map<String, String> partitionSpec)
            throws TableNotExistException {
        Table table = getTable(identifier);
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        FileStoreCommit commit = fileStoreTable.store().newCommit(UUID.randomUUID().toString());
        commit.dropPartitions(
                Collections.singletonList(partitionSpec), BatchWriteBuilder.COMMIT_IDENTIFIER);
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

        if (!cascade && listTables(name).size() > 0) {
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

    protected List<String> listTablesImpl(Path databasePath) {
        List<String> tables = new ArrayList<>();
        for (FileStatus status : uncheck(() -> fileIO.listDirectories(databasePath))) {
            if (status.isDir() && tableExists(status.getPath())) {
                tables.add(status.getPath().getName());
            }
        }
        return tables;
    }

    protected abstract List<String> listTablesImpl(String databaseName);

    protected boolean tableExists(Path tablePath) {
        return !new SchemaManager(fileIO, tablePath).listAllIds().isEmpty();
    }

    @Override
    public void dropTable(Identifier identifier, boolean ignoreIfNotExists)
            throws TableNotExistException {
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

        createTableImpl(identifier, schema);
    }

    protected abstract void createTableImpl(Identifier identifier, Schema schema);

    @Override
    public void renameTable(Identifier fromTable, Identifier toTable, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException {
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
            String tableName = identifier.getObjectName();
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
            String[] splits = tableAndSystemName(identifier);
            String tableName = splits[0];
            String type = splits[1];
            FileStoreTable originTable =
                    getDataTable(new Identifier(identifier.getDatabaseName(), tableName));
            Table table = SystemTableLoader.load(type, fileIO, originTable);
            if (table == null) {
                throw new TableNotExistException(identifier);
            }
            return table;
        } else {
            return getDataTable(identifier);
        }
    }

    private FileStoreTable getDataTable(Identifier identifier) throws TableNotExistException {
        TableSchema tableSchema = getDataTableSchema(identifier);
        return FileStoreTableFactory.create(
                fileIO,
                getDataTableLocation(identifier),
                tableSchema,
                new CatalogEnvironment(
                        Lock.factory(
                                lockFactory().orElse(null), lockContext().orElse(null), identifier),
                        metastoreClientFactory(identifier).orElse(null),
                        lineageMetaFactory));
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
                    tableMap.put(table, getDataTableLocation(Identifier.create(database, table)));
                }
            }
            return allPaths;
        } catch (DatabaseNotExistException e) {
            throw new RuntimeException("Database is deleted while listing", e);
        }
    }

    protected abstract TableSchema getDataTableSchema(Identifier identifier)
            throws TableNotExistException;

    @VisibleForTesting
    public Path getDataTableLocation(Identifier identifier) {
        return new Path(newDatabasePath(identifier.getDatabaseName()), identifier.getObjectName());
    }

    private static boolean isSpecifiedSystemTable(Identifier identifier) {
        return identifier.getObjectName().contains(SYSTEM_TABLE_SPLITTER);
    }

    protected boolean isSystemTable(Identifier identifier) {
        return isSystemDatabase(identifier.getDatabaseName()) || isSpecifiedSystemTable(identifier);
    }

    protected void checkNotSystemTable(Identifier identifier, String method) {
        if (isSystemTable(identifier)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot '%s' for system table '%s', please use data table.",
                            method, identifier));
        }
    }

    public void copyTableDefaultOptions(Map<String, String> options) {
        tableDefaultOptions.forEach(options::putIfAbsent);
    }

    private String[] tableAndSystemName(Identifier identifier) {
        String[] splits = StringUtils.split(identifier.getObjectName(), SYSTEM_TABLE_SPLITTER);
        if (splits.length != 2) {
            throw new IllegalArgumentException(
                    "System table can only contain one '$' separator, but this is: "
                            + identifier.getObjectName());
        }
        return splits;
    }

    public static Path newTableLocation(String warehouse, Identifier identifier) {
        if (isSpecifiedSystemTable(identifier)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Table name[%s] cannot contain '%s' separator",
                            identifier.getObjectName(), SYSTEM_TABLE_SPLITTER));
        }
        return new Path(
                newDatabasePath(warehouse, identifier.getDatabaseName()),
                identifier.getObjectName());
    }

    public static Path newDatabasePath(String warehouse, String database) {
        return new Path(warehouse, database + DB_SUFFIX);
    }

    private boolean isSystemDatabase(String database) {
        return SYSTEM_DATABASE_NAME.equals(database);
    }

    /** Validate database cannot be a system database. */
    protected void checkNotSystemDatabase(String database) {
        if (isSystemDatabase(database)) {
            throw new ProcessSystemDatabaseException();
        }
    }

    /** Validate database, table and field names must be lowercase when not case-sensitive. */
    public static void validateCaseInsensitive(
            boolean caseSensitive, String type, String... names) {
        validateCaseInsensitive(caseSensitive, type, Arrays.asList(names));
    }

    /** Validate database, table and field names must be lowercase when not case-sensitive. */
    public static void validateCaseInsensitive(
            boolean caseSensitive, String type, List<String> names) {
        if (caseSensitive) {
            return;
        }
        List<String> illegalNames =
                names.stream().filter(f -> !f.equals(f.toLowerCase())).collect(Collectors.toList());
        checkArgument(
                illegalNames.isEmpty(),
                String.format(
                        "%s name %s cannot contain upper case in the catalog.",
                        type, illegalNames));
    }

    protected void validateIdentifierNameCaseInsensitive(Identifier identifier) {
        validateCaseInsensitive(caseSensitive(), "Database", identifier.getDatabaseName());
        validateCaseInsensitive(caseSensitive(), "Table", identifier.getObjectName());
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
        validateCaseInsensitive(caseSensitive(), "Field", fieldNames);
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

    private static boolean isDatabase(Path path) {
        return path.getName().endsWith(DB_SUFFIX);
    }

    private static String database(Path path) {
        String name = path.getName();
        return name.substring(0, name.length() - DB_SUFFIX.length());
    }

    protected static <T> T uncheck(Callable<T> callable) {
        try {
            return callable.call();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
