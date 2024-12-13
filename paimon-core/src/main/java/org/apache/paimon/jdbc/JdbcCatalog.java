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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.AbstractCatalog;
import org.apache.paimon.catalog.CatalogLockContext;
import org.apache.paimon.catalog.CatalogLockFactory;
import org.apache.paimon.catalog.Database;
import org.apache.paimon.catalog.DatabaseChange;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Preconditions;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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

    protected JdbcCatalog(FileIO fileIO, String catalogKey, Options options, String warehouse) {
        super(fileIO, options);
        this.catalogKey = catalogKey;
        this.options = options;
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

    /** Initialize catalog tables. */
    private void initializeCatalogTablesIfNeed() throws SQLException, InterruptedException {
        String uri = options.get(CatalogOptions.URI.key());
        Preconditions.checkNotNull(uri, "JDBC connection URI is required");
        // Check and create catalog table.
        connections.run(
                conn -> {
                    DatabaseMetaData dbMeta = conn.getMetaData();
                    ResultSet tableExists =
                            dbMeta.getTables(null, null, JdbcUtils.CATALOG_TABLE_NAME, null);
                    if (tableExists.next()) {
                        return true;
                    }
                    return conn.prepareStatement(JdbcUtils.CREATE_CATALOG_TABLE).execute();
                });

        // Check and create database properties table.
        connections.run(
                conn -> {
                    DatabaseMetaData dbMeta = conn.getMetaData();
                    ResultSet tableExists =
                            dbMeta.getTables(
                                    null, null, JdbcUtils.DATABASE_PROPERTIES_TABLE_NAME, null);
                    if (tableExists.next()) {
                        return true;
                    }
                    return conn.prepareStatement(JdbcUtils.CREATE_DATABASE_PROPERTIES_TABLE)
                            .execute();
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
    protected void dropDatabaseImpl(String name) {
        // Delete table from paimon_tables
        execute(connections, JdbcUtils.DELETE_TABLES_SQL, catalogKey, name);
        // Delete properties from paimon_database_properties
        execute(connections, JdbcUtils.DELETE_ALL_DATABASE_PROPERTIES_SQL, catalogKey, name);
    }

    @Override
    protected void alterDatabaseImpl(String name, List<DatabaseChange> changes) {
        Pair<Map<String, String>, Set<String>> insertProperties2removeKeys =
                DatabaseChange.getAddPropertiesAndRemoveKeys(changes);
        Map<String, String> insertProperties = insertProperties2removeKeys.getLeft();
        Set<String> removeKeys = insertProperties2removeKeys.getRight();
        Map<String, String> startingProperties = fetchProperties(name);
        Map<String, String> inserts = Maps.newHashMap();
        Map<String, String> updates = Maps.newHashMap();
        Set<String> removes = Sets.newHashSet();
        if (!insertProperties.isEmpty()) {
            insertProperties.forEach(
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
    protected void dropTableImpl(Identifier identifier) {
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
            Path path = getTableLocation(identifier);
            try {
                if (fileIO.exists(path)) {
                    fileIO.deleteDirectoryQuietly(path);
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
            // create table file
            getSchemaManager(identifier).createTable(schema);
            // Update schema metadata
            Path path = getTableLocation(identifier);
            int insertRecord =
                    connections.run(
                            conn -> {
                                try (PreparedStatement sql =
                                        conn.prepareStatement(
                                                JdbcUtils.DO_COMMIT_CREATE_TABLE_SQL)) {
                                    sql.setString(1, catalogKey);
                                    sql.setString(2, identifier.getDatabaseName());
                                    sql.setString(3, identifier.getTableName());
                                    return sql.executeUpdate();
                                }
                            });
            if (insertRecord == 1) {
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
        } catch (Exception e) {
            throw new RuntimeException("Failed to create table " + identifier.getFullName(), e);
        }
    }

    @Override
    protected void renameTableImpl(Identifier fromTable, Identifier toTable) {
        try {
            // update table metadata info
            updateTable(connections, catalogKey, fromTable, toTable);

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
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {
        assertMainBranch(identifier);
        SchemaManager schemaManager = getSchemaManager(identifier);
        schemaManager.commitChanges(changes);
    }

    @Override
    protected TableSchema getDataTableSchema(Identifier identifier) throws TableNotExistException {
        assertMainBranch(identifier);
        if (!JdbcUtils.tableExists(
                connections, catalogKey, identifier.getDatabaseName(), identifier.getTableName())) {
            throw new TableNotExistException(identifier);
        }
        Path tableLocation = getTableLocation(identifier);
        return new SchemaManager(fileIO, tableLocation)
                .latest()
                .orElseThrow(
                        () -> new RuntimeException("There is no paimon table in " + tableLocation));
    }

    @Override
    public boolean allowUpperCase() {
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

    private Lock lock(Identifier identifier) {
        if (!lockEnabled()) {
            return new Lock.EmptyLock();
        }
        JdbcCatalogLock lock =
                new JdbcCatalogLock(
                        connections,
                        catalogKey,
                        checkMaxSleep(options.toMap()),
                        acquireTimeout(options.toMap()));
        return Lock.fromCatalog(lock, identifier);
    }

    @Override
    public void close() throws Exception {
        connections.close();
    }

    private SchemaManager getSchemaManager(Identifier identifier) {
        return new SchemaManager(fileIO, getTableLocation(identifier)).withLock(lock(identifier));
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
}
