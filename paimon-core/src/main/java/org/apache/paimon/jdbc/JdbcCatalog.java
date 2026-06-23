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

package org.apache.paimon.jdbc;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.PagedList;
import org.apache.paimon.TableType;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.AbstractCatalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.CatalogLockContext;
import org.apache.paimon.catalog.CatalogLockFactory;
import org.apache.paimon.catalog.CatalogUtils;
import org.apache.paimon.catalog.Database;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.PropertyChange;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.view.View;
import org.apache.paimon.view.ViewChange;
import org.apache.paimon.view.ViewImpl;
import org.apache.paimon.view.ViewSchema;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;
import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;
import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;
import org.apache.paimon.shade.guava30.com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.PATH;
import static org.apache.paimon.CoreOptions.TYPE;
import static org.apache.paimon.catalog.CatalogUtils.checkNotBranch;
import static org.apache.paimon.catalog.CatalogUtils.checkNotSystemDatabase;
import static org.apache.paimon.catalog.CatalogUtils.checkNotSystemTable;
import static org.apache.paimon.catalog.CatalogUtils.validateCreateTable;
import static org.apache.paimon.jdbc.JdbcCatalogLock.acquireTimeout;
import static org.apache.paimon.jdbc.JdbcCatalogLock.checkMaxSleep;
import static org.apache.paimon.jdbc.JdbcUtils.deleteProperties;
import static org.apache.paimon.jdbc.JdbcUtils.execute;
import static org.apache.paimon.jdbc.JdbcUtils.insertProperties;
import static org.apache.paimon.jdbc.JdbcUtils.updateProperties;
import static org.apache.paimon.jdbc.JdbcUtils.updateTable;

/* This file is based on source code from the Iceberg Project (http://iceberg.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Support jdbc catalog. */
public class JdbcCatalog extends AbstractCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcCatalog.class);

    public static final String PROPERTY_PREFIX = "jdbc.";
    private static final String DATABASE_EXISTS_PROPERTY = "exists";

    private final JdbcClientPool connections;
    private final String catalogKey;
    private final Options options;
    private final String warehouse;

    protected JdbcCatalog(
            FileIO fileIO, String catalogKey, CatalogContext context, String warehouse) {
        super(fileIO, context);
        this.catalogKey = catalogKey;
        this.options = context.options();
        this.warehouse = warehouse;
        Preconditions.checkNotNull(options, "Invalid catalog properties: null");
        this.connections =
                new JdbcClientPool(
                        options.get(CatalogOptions.CLIENT_POOL_SIZE),
                        options.get(CatalogOptions.URI.key()),
                        options.toMap());
        try {
            initializeCatalogTablesIfNeed();
        } catch (SQLException e) {
            throw new RuntimeException("Cannot initialize JDBC catalog", e);
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted in call to initialize", e);
        }
    }

    @VisibleForTesting
    public JdbcClientPool getConnections() {
        return connections;
    }

    public String getCatalogKey() {
        return catalogKey;
    }

    /** Initialize catalog tables. */
    private void initializeCatalogTablesIfNeed() throws SQLException, InterruptedException {
        String uri = options.get(CatalogOptions.URI.key());
        Preconditions.checkNotNull(uri, "JDBC connection URI is required");
        // Check and create catalog table.
        connections.run(
                conn -> {
                    DatabaseMetaData dbMeta = conn.getMetaData();
                    try (ResultSet tableExists =
                            dbMeta.getTables(null, null, JdbcUtils.CATALOG_TABLE_NAME, null)) {
                        if (tableExists.next()) {
                            return true;
                        }
                    }
                    try (PreparedStatement statement =
                            conn.prepareStatement(JdbcUtils.CREATE_CATALOG_TABLE)) {
                        return statement.execute();
                    }
                });

        // Check and create database properties table.
        connections.run(
                conn -> {
                    DatabaseMetaData dbMeta = conn.getMetaData();
                    try (ResultSet tableExists =
                            dbMeta.getTables(
                                    null, null, JdbcUtils.DATABASE_PROPERTIES_TABLE_NAME, null)) {
                        if (tableExists.next()) {
                            return true;
                        }
                    }
                    try (PreparedStatement statement =
                            conn.prepareStatement(JdbcUtils.CREATE_DATABASE_PROPERTIES_TABLE)) {
                        return statement.execute();
                    }
                });

        // Check and create table properties table.
        connections.run(
                conn -> {
                    DatabaseMetaData dbMeta = conn.getMetaData();
                    try (ResultSet tableExists =
                            dbMeta.getTables(
                                    null, null, JdbcUtils.TABLE_PROPERTIES_TABLE_NAME, null)) {
                        if (tableExists.next()) {
                            return true;
                        }
                    }
                    try (PreparedStatement statement =
                            conn.prepareStatement(JdbcUtils.CREATE_TABLE_PROPERTIES_TABLE)) {
                        return statement.execute();
                    }
                });

        // Check and create view table.
        connections.run(
                conn -> {
                    DatabaseMetaData dbMeta = conn.getMetaData();
                    try (ResultSet tableExists =
                            dbMeta.getTables(null, null, JdbcUtils.VIEW_TABLE_NAME, null)) {
                        if (tableExists.next()) {
                            return true;
                        }
                    }
                    try (PreparedStatement statement =
                            conn.prepareStatement(JdbcUtils.CREATE_VIEW_TABLE)) {
                        return statement.execute();
                    }
                });

        // if lock enabled, Check and create distributed lock table.
        if (lockEnabled()) {
            JdbcUtils.createDistributedLockTable(connections, options);
        }
    }

    @Override
    public String warehouse() {
        return warehouse;
    }

    @Override
    public CatalogLoader catalogLoader() {
        return new JdbcCatalogLoader(fileIO, catalogKey, context, warehouse);
    }

    @Override
    public List<String> listDatabases() {
        List<String> databases = Lists.newArrayList();
        databases.addAll(
                fetch(
                        row -> row.getString(JdbcUtils.TABLE_DATABASE),
                        JdbcUtils.LIST_ALL_TABLE_DATABASES_SQL,
                        catalogKey));

        databases.addAll(
                fetch(
                        row -> row.getString(JdbcUtils.DATABASE_NAME),
                        JdbcUtils.LIST_ALL_PROPERTY_DATABASES_SQL,
                        catalogKey));
        return databases.stream().distinct().collect(Collectors.toList());
    }

    @Override
    protected Database getDatabaseImpl(String databaseName) throws DatabaseNotExistException {
        if (!JdbcUtils.databaseExists(connections, catalogKey, databaseName)) {
            throw new DatabaseNotExistException(databaseName);
        }
        Map<String, String> options = Maps.newHashMap();
        options.putAll(fetchProperties(databaseName));
        if (!options.containsKey(DB_LOCATION_PROP)) {
            options.put(DB_LOCATION_PROP, newDatabasePath(databaseName).getName());
        }
        options.remove(DATABASE_EXISTS_PROPERTY);
        return Database.of(databaseName, options, null);
    }

    @Override
    protected void createDatabaseImpl(String name, Map<String, String> properties) {
        Map<String, String> createProps = new HashMap<>();
        createProps.put(DATABASE_EXISTS_PROPERTY, "true");
        if (properties != null && !properties.isEmpty()) {
            createProps.putAll(properties);
        }

        if (!createProps.containsKey(DB_LOCATION_PROP)) {
            Path databasePath = newDatabasePath(name);
            createProps.put(DB_LOCATION_PROP, databasePath.toString());
        }
        insertProperties(connections, catalogKey, name, createProps);
    }

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

        if (!cascade && (!listTables(name).isEmpty() || !listViews(name).isEmpty())) {
            throw new DatabaseNotEmptyException(name);
        }

        dropDatabaseImpl(name);
    }

    @Override
    protected void dropDatabaseImpl(String name) {
        // Delete table from paimon_tables
        execute(connections, JdbcUtils.DELETE_TABLES_SQL, catalogKey, name);
        // Delete properties from paimon_database_properties
        execute(connections, JdbcUtils.DELETE_ALL_DATABASE_PROPERTIES_SQL, catalogKey, name);
        // Delete table properties from paimon_table_properties
        if (syncTableProperties()) {
            execute(
                    connections,
                    JdbcUtils.DELETE_ALL_TABLE_PROPERTIES_FOR_DB_SQL,
                    catalogKey,
                    name);
        }
        // Delete views from paimon_views.
        execute(connections, JdbcUtils.DELETE_VIEWS_SQL, catalogKey, name);
    }

    @Override
    protected void alterDatabaseImpl(String name, List<PropertyChange> changes) {
        Pair<Map<String, String>, Set<String>> setPropertiesToRemoveKeys =
                PropertyChange.getSetPropertiesToRemoveKeys(changes);
        Map<String, String> setProperties = setPropertiesToRemoveKeys.getLeft();
        Set<String> removeKeys = setPropertiesToRemoveKeys.getRight();
        Map<String, String> startingProperties = fetchProperties(name);
        Map<String, String> inserts = Maps.newHashMap();
        Map<String, String> updates = Maps.newHashMap();
        Set<String> removes = Sets.newHashSet();
        if (!setProperties.isEmpty()) {
            setProperties.forEach(
                    (k, v) -> {
                        if (!startingProperties.containsKey(k)) {
                            inserts.put(k, v);
                        } else {
                            updates.put(k, v);
                        }
                    });
        }
        if (!removeKeys.isEmpty()) {
            removeKeys.forEach(
                    k -> {
                        if (startingProperties.containsKey(k)) {
                            removes.add(k);
                        }
                    });
        }
        if (!inserts.isEmpty()) {
            insertProperties(connections, catalogKey, name, inserts);
        }
        if (!updates.isEmpty()) {
            updateProperties(connections, catalogKey, name, updates);
        }
        if (!removes.isEmpty()) {
            deleteProperties(connections, catalogKey, name, removes);
        }
    }

    @Override
    protected List<String> listTablesImpl(String databaseName) {
        return fetch(
                row -> row.getString(JdbcUtils.TABLE_NAME),
                JdbcUtils.LIST_TABLES_SQL,
                catalogKey,
                databaseName);
    }

    @Override
    public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException {
        checkNotBranch(identifier, "createTable");
        checkNotSystemTable(identifier, "createTable");
        validateCreateTable(schema, false);
        validateCustomTablePath(schema.options());

        getDatabase(identifier.getDatabaseName());

        copyTableDefaultOptions(schema.options());

        TableType tableType = Options.fromMap(schema.options()).get(TYPE);
        switch (tableType) {
            case TABLE:
            case MATERIALIZED_TABLE:
                try {
                    runWithLock(
                            identifier,
                            () -> {
                                if (!validateTableNotExists(identifier, ignoreIfExists)) {
                                    return null;
                                }
                                createTableImplWithLock(identifier, schema);
                                return null;
                            });
                } catch (TableAlreadyExistException e) {
                    throw e;
                } catch (Exception e) {
                    throw new RuntimeException(
                            "Failed to create table " + identifier.getFullName(), e);
                }
                break;
            case FORMAT_TABLE:
                if (!validateTableNotExists(identifier, ignoreIfExists)) {
                    return;
                }
                createFormatTable(identifier, schema);
                break;
            case OBJECT_TABLE:
                throw new UnsupportedOperationException(
                        String.format(
                                "Catalog %s cannot support object tables.",
                                this.getClass().getName()));
        }
    }

    @Override
    protected void dropTableImpl(Identifier identifier, List<Path> externalPaths) {
        try {
            int deletedRecords =
                    execute(
                            connections,
                            JdbcUtils.DROP_TABLE_SQL,
                            catalogKey,
                            identifier.getDatabaseName(),
                            identifier.getTableName());

            if (deletedRecords == 0) {
                LOG.info("Skipping drop, table does not exist: {}", identifier);
                return;
            }
            if (syncTableProperties()) {
                execute(
                        connections,
                        JdbcUtils.DELETE_ALL_TABLE_PROPERTIES_SQL,
                        catalogKey,
                        identifier.getDatabaseName(),
                        identifier.getTableName());
            }
            Path path = getTableLocation(identifier);
            try {
                if (fileIO.exists(path)) {
                    fileIO.deleteDirectoryQuietly(path);
                }
                for (Path externalPath : externalPaths) {
                    if (fileIO.exists(externalPath)) {
                        fileIO.deleteDirectoryQuietly(externalPath);
                    }
                }
            } catch (Exception ex) {
                LOG.error("Delete directory[{}] fail for table {}", path, identifier, ex);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to drop table " + identifier.getFullName(), e);
        }
    }

    @Override
    protected void createTableImpl(Identifier identifier, Schema schema) {
        try {
            runWithLock(
                    identifier,
                    () -> {
                        validateTableNotExists(identifier, false);
                        createTableImplWithLock(identifier, schema);
                        return null;
                    });
        } catch (Exception e) {
            throw new RuntimeException("Failed to create table " + identifier.getFullName(), e);
        }
    }

    private void createTableImplWithLock(Identifier identifier, Schema schema) {
        try {
            // create table file
            SchemaManager schemaManager = getSchemaManager(identifier);
            TableSchema tableSchema = schemaManager.createTable(schema);
            // Update schema metadata
            Path path = getTableLocation(identifier);
            if (JdbcUtils.insertTable(
                    connections,
                    catalogKey,
                    identifier.getDatabaseName(),
                    identifier.getTableName())) {
                LOG.debug("Successfully committed to new table: {}", identifier);
            } else {
                try {
                    fileIO.deleteDirectoryQuietly(path);
                } catch (Exception ee) {
                    LOG.error("Delete directory[{}] fail for table {}", path, identifier, ee);
                }
                throw new RuntimeException(
                        String.format(
                                "Failed to create table %s in catalog %s",
                                identifier.getFullName(), catalogKey));
            }
            if (syncTableProperties()) {
                JdbcUtils.insertTableProperties(
                        connections,
                        catalogKey,
                        identifier.getDatabaseName(),
                        identifier.getTableName(),
                        collectTableProperties(tableSchema));
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to create table " + identifier.getFullName(), e);
        }
    }

    private boolean validateTableNotExists(Identifier identifier, boolean ignoreIfExists)
            throws TableAlreadyExistException {
        if (JdbcUtils.tableExists(
                        connections,
                        catalogKey,
                        identifier.getDatabaseName(),
                        identifier.getObjectName())
                || JdbcUtils.viewExists(
                        connections,
                        catalogKey,
                        identifier.getDatabaseName(),
                        identifier.getObjectName())) {
            if (ignoreIfExists) {
                return false;
            }
            throw new TableAlreadyExistException(identifier);
        }
        return true;
    }

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

        if (JdbcUtils.viewExists(
                connections, catalogKey, toTable.getDatabaseName(), toTable.getObjectName())) {
            throw new TableAlreadyExistException(toTable);
        }

        renameTableImpl(fromTable, toTable);
    }

    @Override
    protected void renameTableImpl(Identifier fromTable, Identifier toTable) {
        try {
            // update table metadata info
            updateTable(connections, catalogKey, fromTable, toTable);
            if (syncTableProperties()) {
                execute(
                        connections,
                        JdbcUtils.RENAME_TABLE_PROPERTIES_SQL,
                        toTable.getDatabaseName(),
                        toTable.getObjectName(),
                        catalogKey,
                        fromTable.getDatabaseName(),
                        fromTable.getObjectName());
            }

            Path fromPath = getTableLocation(fromTable);
            if (!new SchemaManager(fileIO, fromPath).listAllIds().isEmpty()) {
                // Rename the file system's table directory. Maintain consistency between tables in
                // the file system and tables in the Hive Metastore.
                Path toPath = getTableLocation(toTable);
                try {
                    fileIO.rename(fromPath, toPath);
                } catch (IOException e) {
                    throw new RuntimeException(
                            "Failed to rename changes of table "
                                    + toTable.getFullName()
                                    + " to underlying files.",
                            e);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to rename table " + fromTable.getFullName(), e);
        }
    }

    @Override
    protected void alterTableImpl(Identifier identifier, List<SchemaChange> changes)
            throws ColumnAlreadyExistException, TableNotExistException, ColumnNotExistException {
        assertMainBranch(identifier);
        SchemaManager schemaManager = getSchemaManager(identifier);
        try {
            runWithLock(identifier, () -> schemaManager.commitChanges(changes));
            if (syncTableProperties()) {
                TableSchema updatedSchema = schemaManager.latest().get();
                execute(
                        connections,
                        JdbcUtils.DELETE_ALL_TABLE_PROPERTIES_SQL,
                        catalogKey,
                        identifier.getDatabaseName(),
                        identifier.getTableName());
                JdbcUtils.insertTableProperties(
                        connections,
                        catalogKey,
                        identifier.getDatabaseName(),
                        identifier.getTableName(),
                        collectTableProperties(updatedSchema));
            }
        } catch (TableNotExistException
                | ColumnAlreadyExistException
                | ColumnNotExistException
                | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to alter table " + identifier.getFullName(), e);
        }
    }

    @Override
    protected TableSchema loadTableSchema(Identifier identifier) throws TableNotExistException {
        assertMainBranch(identifier);
        if (!JdbcUtils.tableExists(
                connections, catalogKey, identifier.getDatabaseName(), identifier.getTableName())) {
            throw new TableNotExistException(identifier);
        }
        Path tableLocation = getTableLocation(identifier);
        return tableSchemaInFileSystem(tableLocation, identifier.getBranchNameOrDefault())
                .orElseThrow(
                        () -> new RuntimeException("There is no paimon table in " + tableLocation));
    }

    @Override
    public boolean caseSensitive() {
        return false;
    }

    @Override
    public Optional<CatalogLockFactory> defaultLockFactory() {
        return Optional.of(new JdbcCatalogLockFactory());
    }

    @Override
    public Optional<CatalogLockContext> lockContext() {
        return Optional.of(new JdbcCatalogLockContext(catalogKey, options));
    }

    public <T> T runWithLock(Identifier identifier, Callable<T> callable) throws Exception {
        if (!lockEnabled()) {
            return callable.call();
        }
        JdbcCatalogLock lock =
                new JdbcCatalogLock(
                        connections,
                        catalogKey,
                        checkMaxSleep(options.toMap()),
                        acquireTimeout(options.toMap()));
        return Lock.fromCatalog(lock, identifier).runWithLock(callable);
    }

    @Override
    public void repairCatalog() {
        List<String> databases;
        try {
            databases = listDatabasesInFileSystem(new Path(warehouse));
        } catch (IOException e) {
            throw new RuntimeException("Failed to list databases in file system", e);
        }
        for (String database : databases) {
            repairDatabase(database);
        }
    }

    @Override
    public void repairDatabase(String databaseName) {
        checkNotSystemDatabase(databaseName);

        // First check if database exists in file system
        Path databasePath = newDatabasePath(databaseName);
        List<String> tables;
        try {
            if (!fileIO.exists(databasePath)) {
                throw new RuntimeException("Database directory does not exist: " + databasePath);
            }
            tables = listTablesInFileSystem(databasePath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (!JdbcUtils.databaseExists(connections, catalogKey, databaseName)) {
            createDatabaseImpl(databaseName, Collections.emptyMap());
        }

        // Repair tables
        for (String table : tables) {
            try {
                repairTable(Identifier.create(databaseName, table));
            } catch (TableNotExistException ignore) {
                // Table might not exist due to concurrent operations
            }
        }
    }

    @Override
    public void repairTable(Identifier identifier) throws TableNotExistException {
        checkNotBranch(identifier, "repairTable");
        checkNotSystemTable(identifier, "repairTable");

        // First check if table exists in file system
        Path tableLocation = getTableLocation(identifier);
        TableSchema tableSchema =
                tableSchemaInFileSystem(tableLocation, identifier.getBranchNameOrDefault())
                        .orElseThrow(() -> new TableNotExistException(identifier));

        if (!JdbcUtils.databaseExists(connections, catalogKey, identifier.getDatabaseName())) {
            createDatabaseImpl(identifier.getDatabaseName(), Collections.emptyMap());
        }
        // Table exists in file system, now check if it exists in JDBC catalog
        if (!JdbcUtils.tableExists(
                connections, catalogKey, identifier.getDatabaseName(), identifier.getTableName())) {
            // Table missing from JDBC catalog, repair it
            if (JdbcUtils.insertTable(
                    connections,
                    catalogKey,
                    identifier.getDatabaseName(),
                    identifier.getTableName())) {
                LOG.debug("Successfully repaired table: {}", identifier);
            } else {
                LOG.error("Failed to repair table: {}", identifier);
            }
        }
        if (syncTableProperties()) {
            // Delete existing properties and reinsert from filesystem schema
            execute(
                    connections,
                    JdbcUtils.DELETE_ALL_TABLE_PROPERTIES_SQL,
                    catalogKey,
                    identifier.getDatabaseName(),
                    identifier.getTableName());
            JdbcUtils.insertTableProperties(
                    connections,
                    catalogKey,
                    identifier.getDatabaseName(),
                    identifier.getTableName(),
                    collectTableProperties(tableSchema));
        }
    }

    @Override
    public void close() throws Exception {
        connections.close();
    }

    private boolean syncTableProperties() {
        return options.get(CatalogOptions.SYNC_ALL_PROPERTIES);
    }

    private void copyTableDefaultOptions(Map<String, String> tableOptions) {
        tableDefaultOptions.forEach(tableOptions::putIfAbsent);
    }

    private void validateCustomTablePath(Map<String, String> tableOptions) {
        if (!allowCustomTablePath() && tableOptions.containsKey(PATH.key())) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The current catalog %s does not support specifying the table path when creating a table.",
                            this.getClass().getSimpleName()));
        }
    }

    private Map<String, String> convertToPropertiesTableKey(TableSchema tableSchema) {
        Map<String, String> properties = new HashMap<>();
        if (!tableSchema.primaryKeys().isEmpty()) {
            properties.put(
                    CoreOptions.PRIMARY_KEY.key(), String.join(",", tableSchema.primaryKeys()));
        }
        if (!tableSchema.partitionKeys().isEmpty()) {
            properties.put(
                    CoreOptions.PARTITION.key(), String.join(",", tableSchema.partitionKeys()));
        }
        if (!tableSchema.bucketKeys().isEmpty()) {
            properties.put(
                    CoreOptions.BUCKET_KEY.key(), String.join(",", tableSchema.bucketKeys()));
        }
        return properties;
    }

    private Map<String, String> collectTableProperties(TableSchema tableSchema) {
        Map<String, String> properties = new HashMap<>(tableSchema.options());
        properties.putAll(convertToPropertiesTableKey(tableSchema));
        return properties;
    }

    private SchemaManager getSchemaManager(Identifier identifier) {
        return new SchemaManager(fileIO, getTableLocation(identifier));
    }

    private Map<String, String> fetchProperties(String databaseName) {
        List<Map.Entry<String, String>> entries =
                fetch(
                        row ->
                                new AbstractMap.SimpleImmutableEntry<>(
                                        row.getString(JdbcUtils.DATABASE_PROPERTY_KEY),
                                        row.getString(JdbcUtils.DATABASE_PROPERTY_VALUE)),
                        JdbcUtils.GET_ALL_DATABASE_PROPERTIES_SQL,
                        catalogKey,
                        databaseName);
        return ImmutableMap.<String, String>builder().putAll(entries).build();
    }

    @FunctionalInterface
    interface RowProducer<R> {
        R apply(ResultSet result) throws SQLException;
    }

    @SuppressWarnings("checkstyle:NestedTryDepth")
    private <R> List<R> fetch(RowProducer<R> toRow, String sql, String... args) {
        try {
            return connections.run(
                    conn -> {
                        List<R> result = Lists.newArrayList();
                        try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
                            for (int pos = 0; pos < args.length; pos += 1) {
                                preparedStatement.setString(pos + 1, args[pos]);
                            }
                            try (ResultSet rs = preparedStatement.executeQuery()) {
                                while (rs.next()) {
                                    result.add(toRow.apply(rs));
                                }
                            }
                        }
                        return result;
                    });
        } catch (SQLException e) {
            throw new RuntimeException(String.format("Failed to execute query: %s", sql), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted in SQL query", e);
        }
    }

    // ======================= view methods ===============================

    @Override
    public View getView(Identifier identifier) throws ViewNotExistException {
        try {
            String viewSchemaJson =
                    JdbcUtils.getViewSchema(
                            connections,
                            catalogKey,
                            identifier.getDatabaseName(),
                            identifier.getObjectName());
            if (viewSchemaJson == null) {
                throw new ViewNotExistException(identifier);
            }

            ViewSchema viewSchema = JsonSerdeUtil.fromJson(viewSchemaJson, ViewSchema.class);
            return new ViewImpl(
                    identifier,
                    viewSchema.fields(),
                    viewSchema.query(),
                    viewSchema.dialects(),
                    viewSchema.comment(),
                    viewSchema.options());
        } catch (SQLException | InterruptedException e) {
            throw new RuntimeException("Failed to get view " + identifier.getFullName(), e);
        }
    }

    @Override
    public void createView(Identifier identifier, View view, boolean ignoreIfExists)
            throws ViewAlreadyExistException, DatabaseNotExistException {
        // Check if database exists
        try {
            getDatabase(identifier.getDatabaseName());
        } catch (DatabaseNotExistException e) {
            throw e;
        }

        // Serialize view schema to JSON
        ViewSchema viewSchema =
                new ViewSchema(
                        view.rowType().getFields(),
                        view.query(),
                        view.dialects(),
                        view.comment().orElse(null),
                        view.options());
        String viewSchemaJson = JsonSerdeUtil.toJson(viewSchema);

        // Insert view
        try {
            runWithLock(
                    identifier,
                    () -> {
                        if (!validateViewNotExists(identifier, ignoreIfExists)) {
                            return null;
                        }
                        JdbcUtils.insertView(
                                connections,
                                catalogKey,
                                identifier.getDatabaseName(),
                                identifier.getObjectName(),
                                viewSchemaJson);
                        return null;
                    });
        } catch (RuntimeException e) {
            if (e.getMessage() != null && e.getMessage().contains("View already exists")) {
                throw new ViewAlreadyExistException(identifier, e);
            }
            throw new RuntimeException("Failed to create view " + identifier.getFullName(), e);
        } catch (ViewAlreadyExistException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create view " + identifier.getFullName(), e);
        }
    }

    private boolean validateViewNotExists(Identifier identifier, boolean ignoreIfExists)
            throws ViewAlreadyExistException {
        if (JdbcUtils.tableExists(
                        connections,
                        catalogKey,
                        identifier.getDatabaseName(),
                        identifier.getObjectName())
                || JdbcUtils.viewExists(
                        connections,
                        catalogKey,
                        identifier.getDatabaseName(),
                        identifier.getObjectName())) {
            if (ignoreIfExists) {
                return false;
            }
            throw new ViewAlreadyExistException(identifier);
        }
        return true;
    }

    @Override
    public void dropView(Identifier identifier, boolean ignoreIfNotExists)
            throws ViewNotExistException {
        // Check if view exists
        if (!JdbcUtils.viewExists(
                connections,
                catalogKey,
                identifier.getDatabaseName(),
                identifier.getObjectName())) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new ViewNotExistException(identifier);
        }

        // Delete view
        int deletedRecords =
                execute(
                        connections,
                        JdbcUtils.DROP_VIEW_SQL,
                        catalogKey,
                        identifier.getDatabaseName(),
                        identifier.getObjectName());

        if (deletedRecords != 1) {
            throw new RuntimeException(
                    String.format(
                            "Failed to drop view %s: affected %d rows",
                            identifier.getFullName(), deletedRecords));
        }
    }

    @Override
    public List<String> listViews(String databaseName) throws DatabaseNotExistException {
        if (CatalogUtils.isSystemDatabase(databaseName)) {
            return Collections.emptyList();
        }

        // Check if database exists
        if (!JdbcUtils.databaseExists(connections, catalogKey, databaseName)) {
            throw new DatabaseNotExistException(databaseName);
        }

        return fetch(
                row -> row.getString(JdbcUtils.VIEW_NAME),
                JdbcUtils.LIST_VIEWS_SQL,
                catalogKey,
                databaseName);
    }

    @Override
    public PagedList<String> listViewsPaged(
            String databaseName, Integer maxResults, String pageToken, String viewNamePattern)
            throws DatabaseNotExistException {
        CatalogUtils.validateNamePattern(this, viewNamePattern);
        return new PagedList<>(listViews(databaseName), null);
    }

    @Override
    public PagedList<View> listViewDetailsPaged(
            String databaseName, Integer maxResults, String pageToken, String viewNamePattern)
            throws DatabaseNotExistException {
        PagedList<String> pagedViews =
                listViewsPaged(databaseName, maxResults, pageToken, viewNamePattern);
        return new PagedList<>(
                pagedViews.getElements().stream()
                        .map(
                                viewName -> {
                                    try {
                                        return getView(Identifier.create(databaseName, viewName));
                                    } catch (ViewNotExistException ignored) {
                                        LOG.warn(
                                                "view {}.{} does not exist",
                                                databaseName,
                                                viewName);
                                        return null;
                                    }
                                })
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList()),
                pagedViews.getNextPageToken());
    }

    @Override
    public void renameView(Identifier fromView, Identifier toView, boolean ignoreIfNotExists)
            throws ViewNotExistException, ViewAlreadyExistException {
        // Check if source view exists
        if (!JdbcUtils.viewExists(
                connections, catalogKey, fromView.getDatabaseName(), fromView.getObjectName())) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new ViewNotExistException(fromView);
        }

        // Check if target view already exists
        if (JdbcUtils.viewExists(
                connections, catalogKey, toView.getDatabaseName(), toView.getObjectName())) {
            throw new ViewAlreadyExistException(toView);
        }
        if (JdbcUtils.tableExists(
                connections, catalogKey, toView.getDatabaseName(), toView.getObjectName())) {
            throw new ViewAlreadyExistException(toView);
        }
        if (!JdbcUtils.databaseExists(connections, catalogKey, toView.getDatabaseName())) {
            throw new IllegalArgumentException(
                    String.format("Database %s does not exist.", toView.getDatabaseName()));
        }

        // Rename view
        try {
            JdbcUtils.renameView(connections, catalogKey, fromView, toView);
        } catch (RuntimeException e) {
            if (e.getMessage() != null && e.getMessage().contains("View already exists")) {
                throw new ViewAlreadyExistException(toView, e);
            } else if (e.getMessage() != null && e.getMessage().contains("View does not exist")) {
                throw new ViewNotExistException(fromView, e);
            }
            throw new RuntimeException(
                    "Failed to rename view from "
                            + fromView.getFullName()
                            + " to "
                            + toView.getFullName(),
                    e);
        }
    }

    @Override
    public void alterView(
            Identifier identifier, List<ViewChange> changes, boolean ignoreIfNotExists)
            throws ViewNotExistException, DialectAlreadyExistException, DialectNotExistException {
        // Get existing view
        View existingView;
        try {
            existingView = getView(identifier);
        } catch (ViewNotExistException e) {
            if (ignoreIfNotExists) {
                return;
            }
            throw e;
        }

        // Apply changes
        Map<String, String> newOptions = new HashMap<>(existingView.options());
        String newComment = existingView.comment().orElse(null);
        Map<String, String> newDialects = new HashMap<>(existingView.dialects());
        for (ViewChange change : changes) {
            if (change instanceof ViewChange.SetViewOption) {
                ViewChange.SetViewOption setOption = (ViewChange.SetViewOption) change;
                newOptions.put(setOption.key(), setOption.value());
            } else if (change instanceof ViewChange.RemoveViewOption) {
                ViewChange.RemoveViewOption removeOption = (ViewChange.RemoveViewOption) change;
                newOptions.remove(removeOption.key());
            } else if (change instanceof ViewChange.UpdateViewComment) {
                ViewChange.UpdateViewComment updateComment = (ViewChange.UpdateViewComment) change;
                newComment = updateComment.comment();
            } else if (change instanceof ViewChange.AddDialect) {
                ViewChange.AddDialect addDialect = (ViewChange.AddDialect) change;
                if (newDialects.containsKey(addDialect.dialect())) {
                    throw new DialectAlreadyExistException(identifier, addDialect.dialect());
                }
                newDialects.put(addDialect.dialect(), addDialect.query());
            } else if (change instanceof ViewChange.UpdateDialect) {
                ViewChange.UpdateDialect updateDialect = (ViewChange.UpdateDialect) change;
                if (!newDialects.containsKey(updateDialect.dialect())) {
                    throw new DialectNotExistException(identifier, updateDialect.dialect());
                }
                newDialects.put(updateDialect.dialect(), updateDialect.query());
            } else if (change instanceof ViewChange.DropDialect) {
                ViewChange.DropDialect dropDialect = (ViewChange.DropDialect) change;
                if (!newDialects.containsKey(dropDialect.dialect())) {
                    throw new DialectNotExistException(identifier, dropDialect.dialect());
                }
                newDialects.remove(dropDialect.dialect());
            }
        }

        // Create updated view schema
        ViewSchema updatedSchema =
                new ViewSchema(
                        existingView.rowType().getFields(),
                        existingView.query(),
                        newDialects,
                        newComment,
                        newOptions);
        String viewSchemaJson = JsonSerdeUtil.toJson(updatedSchema);

        // Update view
        try {
            JdbcUtils.updateView(
                    connections,
                    catalogKey,
                    identifier.getDatabaseName(),
                    identifier.getObjectName(),
                    viewSchemaJson);
        } catch (RuntimeException e) {
            throw new RuntimeException("Failed to alter view " + identifier.getFullName(), e);
        }
    }
}
