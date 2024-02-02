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

import org.apache.paimon.catalog.Identifier;

import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

/** Util for jdbc catalog. */
public class JdbcUtils {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcUtils.class);
    public static final String METADATA_LOCATION_PROP = "metadata_location";
    public static final String PREVIOUS_METADATA_LOCATION_PROP = "previous_metadata_location";
    public static final String CATALOG_TABLE_NAME = "paimon_tables";
    public static final String CATALOG_NAME = "catalog_name";
    public static final String TABLE_DATABASE = "database_name";
    public static final String TABLE_NAME = "table_name";

    static final String DO_COMMIT_SQL =
            "UPDATE "
                    + CATALOG_TABLE_NAME
                    + " SET "
                    + METADATA_LOCATION_PROP
                    + " = ? , "
                    + PREVIOUS_METADATA_LOCATION_PROP
                    + " = ? "
                    + " WHERE "
                    + CATALOG_NAME
                    + " = ? AND "
                    + TABLE_DATABASE
                    + " = ? AND "
                    + TABLE_NAME
                    + " = ? AND "
                    + METADATA_LOCATION_PROP
                    + " = ?";
    static final String CREATE_CATALOG_TABLE =
            "CREATE TABLE "
                    + CATALOG_TABLE_NAME
                    + "("
                    + CATALOG_NAME
                    + " VARCHAR(255) NOT NULL,"
                    + TABLE_DATABASE
                    + " VARCHAR(255) NOT NULL,"
                    + TABLE_NAME
                    + " VARCHAR(255) NOT NULL,"
                    + METADATA_LOCATION_PROP
                    + " VARCHAR(1000),"
                    + PREVIOUS_METADATA_LOCATION_PROP
                    + " VARCHAR(1000),"
                    + " PRIMARY KEY ("
                    + CATALOG_NAME
                    + ", "
                    + TABLE_DATABASE
                    + ", "
                    + TABLE_NAME
                    + ")"
                    + ")";
    static final String GET_TABLE_SQL =
            "SELECT * FROM "
                    + CATALOG_TABLE_NAME
                    + " WHERE "
                    + CATALOG_NAME
                    + " = ? AND "
                    + TABLE_DATABASE
                    + " = ? AND "
                    + TABLE_NAME
                    + " = ? ";
    static final String LIST_TABLES_SQL =
            "SELECT * FROM "
                    + CATALOG_TABLE_NAME
                    + " WHERE "
                    + CATALOG_NAME
                    + " = ? AND "
                    + TABLE_DATABASE
                    + " = ?";

    static final String DELETE_TABLES_SQL =
            "DELETE FROM  "
                    + CATALOG_TABLE_NAME
                    + " WHERE "
                    + CATALOG_NAME
                    + " = ? AND "
                    + TABLE_DATABASE
                    + " = ?";
    static final String RENAME_TABLE_SQL =
            "UPDATE "
                    + CATALOG_TABLE_NAME
                    + " SET "
                    + TABLE_DATABASE
                    + " = ? , "
                    + TABLE_NAME
                    + " = ? "
                    + " WHERE "
                    + CATALOG_NAME
                    + " = ? AND "
                    + TABLE_DATABASE
                    + " = ? AND "
                    + TABLE_NAME
                    + " = ? ";
    static final String DROP_TABLE_SQL =
            "DELETE FROM "
                    + CATALOG_TABLE_NAME
                    + " WHERE "
                    + CATALOG_NAME
                    + " = ? AND "
                    + TABLE_DATABASE
                    + " = ? AND "
                    + TABLE_NAME
                    + " = ? ";
    static final String GET_DATABASE_SQL =
            "SELECT "
                    + TABLE_DATABASE
                    + " FROM "
                    + CATALOG_TABLE_NAME
                    + " WHERE "
                    + CATALOG_NAME
                    + " = ? AND "
                    + TABLE_DATABASE
                    + " = ? LIMIT 1";

    static final String LIST_ALL_TABLE_DATABASES_SQL =
            "SELECT DISTINCT "
                    + TABLE_DATABASE
                    + " FROM "
                    + CATALOG_TABLE_NAME
                    + " WHERE "
                    + CATALOG_NAME
                    + " = ?";
    static final String DO_COMMIT_CREATE_TABLE_SQL =
            "INSERT INTO "
                    + CATALOG_TABLE_NAME
                    + " ("
                    + CATALOG_NAME
                    + ", "
                    + TABLE_DATABASE
                    + ", "
                    + TABLE_NAME
                    + ", "
                    + METADATA_LOCATION_PROP
                    + ", "
                    + PREVIOUS_METADATA_LOCATION_PROP
                    + ") "
                    + " VALUES (?,?,?,?,null)";

    // Catalog database Properties
    static final String DATABASE_PROPERTIES_TABLE_NAME = "paimon_database_properties";
    static final String DATABASE_NAME = "database_name";
    static final String DATABASE_PROPERTY_KEY = "property_key";
    static final String DATABASE_PROPERTY_VALUE = "property_value";

    static final String CREATE_DATABASE_PROPERTIES_TABLE =
            "CREATE TABLE "
                    + DATABASE_PROPERTIES_TABLE_NAME
                    + "("
                    + CATALOG_NAME
                    + " VARCHAR(255) NOT NULL,"
                    + DATABASE_NAME
                    + " VARCHAR(255) NOT NULL,"
                    + DATABASE_PROPERTY_KEY
                    + " VARCHAR(255),"
                    + DATABASE_PROPERTY_VALUE
                    + " VARCHAR(1000),"
                    + "PRIMARY KEY ("
                    + CATALOG_NAME
                    + ", "
                    + DATABASE_NAME
                    + ", "
                    + DATABASE_PROPERTY_KEY
                    + ")"
                    + ")";
    static final String GET_DATABASE_PROPERTIES_SQL =
            "SELECT "
                    + DATABASE_NAME
                    + " FROM "
                    + DATABASE_PROPERTIES_TABLE_NAME
                    + " WHERE "
                    + CATALOG_NAME
                    + " = ? AND "
                    + DATABASE_NAME
                    + " = ? ";
    static final String INSERT_DATABASE_PROPERTIES_SQL =
            "INSERT INTO "
                    + DATABASE_PROPERTIES_TABLE_NAME
                    + " ("
                    + CATALOG_NAME
                    + ", "
                    + DATABASE_NAME
                    + ", "
                    + DATABASE_PROPERTY_KEY
                    + ", "
                    + DATABASE_PROPERTY_VALUE
                    + ") VALUES ";
    static final String INSERT_PROPERTIES_VALUES_BASE = "(?,?,?,?)";
    static final String GET_ALL_DATABASE_PROPERTIES_SQL =
            "SELECT * "
                    + " FROM "
                    + DATABASE_PROPERTIES_TABLE_NAME
                    + " WHERE "
                    + CATALOG_NAME
                    + " = ? AND "
                    + DATABASE_NAME
                    + " = ? ";
    static final String DELETE_DATABASE_PROPERTIES_SQL =
            "DELETE FROM "
                    + DATABASE_PROPERTIES_TABLE_NAME
                    + " WHERE "
                    + CATALOG_NAME
                    + " = ? AND "
                    + DATABASE_NAME
                    + " = ? AND "
                    + DATABASE_PROPERTY_KEY
                    + " IN ";
    static final String DELETE_ALL_DATABASE_PROPERTIES_SQL =
            "DELETE FROM "
                    + DATABASE_PROPERTIES_TABLE_NAME
                    + " WHERE "
                    + CATALOG_NAME
                    + " = ? AND "
                    + DATABASE_NAME
                    + " = ?";
    static final String LIST_PROPERTY_DATABASES_SQL =
            "SELECT DISTINCT "
                    + DATABASE_NAME
                    + " FROM "
                    + DATABASE_PROPERTIES_TABLE_NAME
                    + " WHERE "
                    + CATALOG_NAME
                    + " = ? AND "
                    + DATABASE_NAME
                    + " LIKE ?";
    static final String LIST_ALL_PROPERTY_DATABASES_SQL =
            "SELECT DISTINCT "
                    + DATABASE_NAME
                    + " FROM "
                    + DATABASE_PROPERTIES_TABLE_NAME
                    + " WHERE "
                    + CATALOG_NAME
                    + " = ?";

    // Distributed locks table
    static final String DISTRIBUTED_LOCKS_TABLE_NAME = "paimon_distributed_locks";
    static final String LOCK_NAME = "lock_name";
    static final String ACQUIRED_AT = "acquired_at";

    static final String CREATE_DISTRIBUTED_LOCK_TABLE_SQL =
            "CREATE TABLE "
                    + DISTRIBUTED_LOCKS_TABLE_NAME
                    + "("
                    + LOCK_NAME
                    + " VARCHAR(1000) NOT NULL,"
                    + ACQUIRED_AT
                    + " TIMESTAMP NULL DEFAULT NULL,"
                    + "PRIMARY KEY ("
                    + LOCK_NAME
                    + ")"
                    + ")";

    static final String DISTRIBUTED_LOCK_ACQUIRE_SQL =
            "INSERT INTO "
                    + DISTRIBUTED_LOCKS_TABLE_NAME
                    + " ("
                    + LOCK_NAME
                    + ", "
                    + ACQUIRED_AT
                    + ") VALUES (?, ?)";

    static final String DISTRIBUTED_LOCK_RELEASE_SQL =
            "DELETE FROM " + DISTRIBUTED_LOCKS_TABLE_NAME + " WHERE " + LOCK_NAME + " = ?";

    static final String DISTRIBUTED_LOCK_EXPIRE_CLEAR_SQL =
            "DELETE FROM "
                    + DISTRIBUTED_LOCKS_TABLE_NAME
                    + " WHERE "
                    + LOCK_NAME
                    + " = ? AND "
                    + ACQUIRED_AT
                    + " < ?";

    public static Properties extractJdbcConfiguration(
            Map<String, String> properties, String prefix) {
        Properties result = new Properties();
        properties.forEach(
                (key, value) -> {
                    if (key.startsWith(prefix)) {
                        result.put(key.substring(prefix.length()), value);
                    }
                });
        return result;
    }

    /** Get paimon table metadata. */
    public static Map<String, String> getTable(
            JdbcClientPool connections, String catalogName, String databaseName, String tableName)
            throws SQLException, InterruptedException {
        return connections.run(
                conn -> {
                    Map<String, String> table = Maps.newHashMap();

                    try (PreparedStatement sql = conn.prepareStatement(JdbcUtils.GET_TABLE_SQL)) {
                        sql.setString(1, catalogName);
                        sql.setString(2, databaseName);
                        sql.setString(3, tableName);
                        ResultSet rs = sql.executeQuery();
                        if (rs.next()) {
                            table.put(CATALOG_NAME, rs.getString(CATALOG_NAME));
                            table.put(TABLE_DATABASE, rs.getString(TABLE_DATABASE));
                            table.put(TABLE_NAME, rs.getString(TABLE_NAME));
                            table.put(METADATA_LOCATION_PROP, rs.getString(METADATA_LOCATION_PROP));
                            table.put(
                                    PREVIOUS_METADATA_LOCATION_PROP,
                                    rs.getString(PREVIOUS_METADATA_LOCATION_PROP));
                        }
                        rs.close();
                    }
                    return table;
                });
    }

    public static void updateTable(
            JdbcClientPool connections,
            String catalogName,
            Identifier fromTable,
            Identifier toTable) {
        int updatedRecords =
                execute(
                        err -> {
                            // SQLite doesn't set SQLState or throw
                            // SQLIntegrityConstraintViolationException
                            if (err instanceof SQLIntegrityConstraintViolationException
                                    || (err.getMessage() != null
                                            && err.getMessage().contains("constraint failed"))) {
                                throw new RuntimeException(
                                        String.format("Table already exists: %s", toTable));
                            }
                        },
                        connections,
                        JdbcUtils.RENAME_TABLE_SQL,
                        toTable.getDatabaseName(),
                        toTable.getObjectName(),
                        catalogName,
                        fromTable.getDatabaseName(),
                        fromTable.getObjectName());

        if (updatedRecords == 1) {
            LOG.info("Renamed table from {}, to {}", fromTable, toTable);
        } else if (updatedRecords == 0) {
            throw new RuntimeException(String.format("Table does not exist: %s", fromTable));
        } else {
            LOG.warn(
                    "Rename operation affected {} rows: the catalog table's primary key assumption has been violated",
                    updatedRecords);
        }
    }

    /** Update table metadata location. */
    public static void updateTableMetadataLocation(
            JdbcClientPool connections,
            String catalogName,
            Identifier identifier,
            String newMetadataLocation,
            String oldMetadataLocation)
            throws SQLException, InterruptedException {
        int updatedRecords =
                connections.run(
                        conn -> {
                            try (PreparedStatement sql =
                                    conn.prepareStatement(JdbcUtils.DO_COMMIT_SQL)) {
                                // UPDATE
                                sql.setString(1, newMetadataLocation);
                                sql.setString(2, oldMetadataLocation);
                                // WHERE
                                sql.setString(3, catalogName);
                                sql.setString(4, identifier.getDatabaseName());
                                sql.setString(5, identifier.getObjectName());
                                sql.setString(6, oldMetadataLocation);
                                return sql.executeUpdate();
                            }
                        });
        if (updatedRecords == 1) {
            LOG.debug("Successfully committed to existing table: {}", identifier.getFullName());
        } else {
            throw new RuntimeException(
                    String.format(
                            "Failed to update table %s from catalog %s",
                            identifier.getFullName(), catalogName));
        }
    }

    public static boolean databaseExists(
            JdbcClientPool connections, String catalogName, String databaseName) {

        if (exists(connections, JdbcUtils.GET_DATABASE_SQL, catalogName, databaseName)) {
            return true;
        }

        if (exists(connections, JdbcUtils.GET_DATABASE_PROPERTIES_SQL, catalogName, databaseName)) {
            return true;
        }
        return false;
    }

    public static boolean tableExists(
            JdbcClientPool connections, String catalogName, String databaseName, String tableName) {
        if (exists(connections, JdbcUtils.GET_TABLE_SQL, catalogName, databaseName, tableName)) {
            return true;
        }
        return false;
    }

    @SuppressWarnings("checkstyle:NestedTryDepth")
    private static boolean exists(JdbcClientPool connections, String sql, String... args) {
        try {
            return connections.run(
                    conn -> {
                        try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
                            for (int index = 0; index < args.length; index++) {
                                preparedStatement.setString(index + 1, args[index]);
                            }
                            try (ResultSet rs = preparedStatement.executeQuery()) {
                                if (rs.next()) {
                                    return true;
                                }
                            }
                        }
                        return false;
                    });
        } catch (SQLException e) {
            throw new RuntimeException(String.format("Failed to execute exists query: %s", sql), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted in SQL query", e);
        }
    }

    public static int execute(JdbcClientPool connections, String sql, String... args) {
        return execute(err -> {}, connections, sql, args);
    }

    public static int execute(
            Consumer<SQLException> sqlErrorHandler,
            JdbcClientPool connections,
            String sql,
            String... args) {
        try {
            return connections.run(
                    conn -> {
                        try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
                            for (int pos = 0; pos < args.length; pos++) {
                                preparedStatement.setString(pos + 1, args[pos]);
                            }
                            return preparedStatement.executeUpdate();
                        }
                    });
        } catch (SQLException e) {
            sqlErrorHandler.accept(e);
            throw new RuntimeException(String.format("Failed to execute: %s", sql), e);
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted in SQL command", e);
        }
    }

    public static boolean insertProperties(
            JdbcClientPool connections,
            String catalogName,
            String databaseName,
            Map<String, String> properties) {
        String[] args =
                properties.entrySet().stream()
                        .flatMap(
                                entry ->
                                        Stream.of(
                                                catalogName,
                                                databaseName,
                                                entry.getKey(),
                                                entry.getValue()))
                        .toArray(String[]::new);

        int insertedRecords =
                execute(connections, JdbcUtils.insertPropertiesStatement(properties.size()), args);
        if (insertedRecords == properties.size()) {
            return true;
        }
        throw new IllegalStateException(
                String.format(
                        "Failed to insert: %d of %d succeeded",
                        insertedRecords, properties.size()));
    }

    public static boolean updateProperties(
            JdbcClientPool connections,
            String catalogName,
            String databaseName,
            Map<String, String> properties) {
        Stream<String> caseArgs =
                properties.entrySet().stream()
                        .flatMap(entry -> Stream.of(entry.getKey(), entry.getValue()));
        Stream<String> whereArgs =
                Stream.concat(Stream.of(catalogName, databaseName), properties.keySet().stream());
        String[] args = Stream.concat(caseArgs, whereArgs).toArray(String[]::new);
        int updatedRecords =
                execute(connections, JdbcUtils.updatePropertiesStatement(properties.size()), args);
        if (updatedRecords == properties.size()) {
            return true;
        }
        throw new IllegalStateException(
                String.format(
                        "Failed to update: %d of %d succeeded", updatedRecords, properties.size()));
    }

    public static boolean deleteProperties(
            JdbcClientPool connections,
            String catalogName,
            String databaseName,
            Set<String> properties) {
        String[] args =
                Stream.concat(Stream.of(catalogName, databaseName), properties.stream())
                        .toArray(String[]::new);

        return execute(connections, JdbcUtils.deletePropertiesStatement(properties), args) > 0;
    }

    private static String insertPropertiesStatement(int size) {
        StringBuilder sqlStatement = new StringBuilder(JdbcUtils.INSERT_DATABASE_PROPERTIES_SQL);
        for (int i = 0; i < size; i++) {
            if (i != 0) {
                sqlStatement.append(", ");
            }
            sqlStatement.append(JdbcUtils.INSERT_PROPERTIES_VALUES_BASE);
        }
        return sqlStatement.toString();
    }

    private static String deletePropertiesStatement(Set<String> properties) {
        StringBuilder sqlStatement = new StringBuilder(JdbcUtils.DELETE_DATABASE_PROPERTIES_SQL);
        String values =
                String.join(",", Collections.nCopies(properties.size(), String.valueOf('?')));
        sqlStatement.append("(").append(values).append(")");

        return sqlStatement.toString();
    }

    private static String updatePropertiesStatement(int size) {
        StringBuilder sqlStatement =
                new StringBuilder(
                        "UPDATE "
                                + DATABASE_PROPERTIES_TABLE_NAME
                                + " SET "
                                + DATABASE_PROPERTY_VALUE
                                + " = CASE");
        for (int i = 0; i < size; i += 1) {
            sqlStatement.append(" WHEN " + DATABASE_PROPERTY_KEY + " = ? THEN ?");
        }
        sqlStatement.append(
                " END WHERE "
                        + CATALOG_NAME
                        + " = ? AND "
                        + DATABASE_NAME
                        + " = ? AND "
                        + DATABASE_PROPERTY_KEY
                        + " IN ");

        String values = String.join(",", Collections.nCopies(size, String.valueOf('?')));
        sqlStatement.append("(").append(values).append(")");
        return sqlStatement.toString();
    }

    public static boolean acquire(JdbcClientPool connections, String lockName, long timeout)
            throws SQLException, InterruptedException {
        // Check and clear expire lock
        int affectedRows = tryClearExpireLock(connections, lockName, timeout);
        if (affectedRows > 0) {
            LOG.debug("Successfully cleared " + affectedRows + " lock records");
        }
        return connections.run(
                connection -> {
                    try (PreparedStatement preparedStatement =
                            connection.prepareStatement(DISTRIBUTED_LOCK_ACQUIRE_SQL)) {
                        preparedStatement.setString(1, lockName);
                        preparedStatement.setTimestamp(
                                2, new Timestamp(System.currentTimeMillis()));
                        return preparedStatement.executeUpdate() > 0;
                    } catch (SQLException e) {
                        LOG.error("Try acquire lock failed.", e);
                        return false;
                    }
                });
    }

    public static void release(JdbcClientPool connections, String lockName)
            throws SQLException, InterruptedException {
        connections.run(
                connection -> {
                    try (PreparedStatement preparedStatement =
                            connection.prepareStatement(DISTRIBUTED_LOCK_RELEASE_SQL)) {
                        preparedStatement.setString(1, lockName);
                        return preparedStatement.executeUpdate() > 0;
                    }
                });
    }

    private static int tryClearExpireLock(JdbcClientPool connections, String lockName, long timeout)
            throws SQLException, InterruptedException {
        long expirationTimeMillis = System.currentTimeMillis() - timeout * 1000;
        Timestamp expirationTimestamp = new Timestamp(expirationTimeMillis);
        return connections.run(
                connection -> {
                    try (PreparedStatement preparedStatement =
                            connection.prepareStatement(DISTRIBUTED_LOCK_EXPIRE_CLEAR_SQL)) {
                        preparedStatement.setString(1, lockName);
                        preparedStatement.setTimestamp(2, expirationTimestamp);
                        return preparedStatement.executeUpdate();
                    }
                });
    }
}
