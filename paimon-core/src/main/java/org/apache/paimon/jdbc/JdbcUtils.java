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
import org.apache.paimon.options.Options;

import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

/** Util for jdbc catalog. */
public class JdbcUtils {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcUtils.class);
    public static final String CATALOG_TABLE_NAME = "paimon_tables";
    public static final String CATALOG_KEY = "catalog_key";
    public static final String TABLE_DATABASE = "database_name";
    public static final String TABLE_NAME = "table_name";

    static final String CREATE_CATALOG_TABLE =
            "CREATE TABLE "
                    + CATALOG_TABLE_NAME
                    + "("
                    + CATALOG_KEY
                    + " VARCHAR(255) NOT NULL,"
                    + TABLE_DATABASE
                    + " VARCHAR(255) NOT NULL,"
                    + TABLE_NAME
                    + " VARCHAR(255) NOT NULL,"
                    + " PRIMARY KEY ("
                    + CATALOG_KEY
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
                    + CATALOG_KEY
                    + " = ? AND "
                    + TABLE_DATABASE
                    + " = ? AND "
                    + TABLE_NAME
                    + " = ? ";
    static final String LIST_TABLES_SQL =
            "SELECT * FROM "
                    + CATALOG_TABLE_NAME
                    + " WHERE "
                    + CATALOG_KEY
                    + " = ? AND "
                    + TABLE_DATABASE
                    + " = ?";

    static final String DELETE_TABLES_SQL =
            "DELETE FROM  "
                    + CATALOG_TABLE_NAME
                    + " WHERE "
                    + CATALOG_KEY
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
                    + CATALOG_KEY
                    + " = ? AND "
                    + TABLE_DATABASE
                    + " = ? AND "
                    + TABLE_NAME
                    + " = ? ";
    static final String DROP_TABLE_SQL =
            "DELETE FROM "
                    + CATALOG_TABLE_NAME
                    + " WHERE "
                    + CATALOG_KEY
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
                    + CATALOG_KEY
                    + " = ? AND "
                    + TABLE_DATABASE
                    + " = ? LIMIT 1";

    static final String LIST_ALL_TABLE_DATABASES_SQL =
            "SELECT DISTINCT "
                    + TABLE_DATABASE
                    + " FROM "
                    + CATALOG_TABLE_NAME
                    + " WHERE "
                    + CATALOG_KEY
                    + " = ?";
    static final String DO_COMMIT_CREATE_TABLE_SQL =
            "INSERT INTO "
                    + CATALOG_TABLE_NAME
                    + " ("
                    + CATALOG_KEY
                    + ", "
                    + TABLE_DATABASE
                    + ", "
                    + TABLE_NAME
                    + ") "
                    + " VALUES (?,?,?)";

    // Catalog database Properties
    static final String DATABASE_PROPERTIES_TABLE_NAME = "paimon_database_properties";
    static final String DATABASE_NAME = "database_name";
    static final String DATABASE_PROPERTY_KEY = "property_key";
    static final String DATABASE_PROPERTY_VALUE = "property_value";

    static final String CREATE_DATABASE_PROPERTIES_TABLE =
            "CREATE TABLE "
                    + DATABASE_PROPERTIES_TABLE_NAME
                    + "("
                    + CATALOG_KEY
                    + " VARCHAR(255) NOT NULL,"
                    + DATABASE_NAME
                    + " VARCHAR(255) NOT NULL,"
                    + DATABASE_PROPERTY_KEY
                    + " VARCHAR(255),"
                    + DATABASE_PROPERTY_VALUE
                    + " VARCHAR(1000),"
                    + "PRIMARY KEY ("
                    + CATALOG_KEY
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
                    + CATALOG_KEY
                    + " = ? AND "
                    + DATABASE_NAME
                    + " = ? ";
    static final String INSERT_DATABASE_PROPERTIES_SQL =
            "INSERT INTO "
                    + DATABASE_PROPERTIES_TABLE_NAME
                    + " ("
                    + CATALOG_KEY
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
                    + CATALOG_KEY
                    + " = ? AND "
                    + DATABASE_NAME
                    + " = ? ";
    static final String DELETE_DATABASE_PROPERTIES_SQL =
            "DELETE FROM "
                    + DATABASE_PROPERTIES_TABLE_NAME
                    + " WHERE "
                    + CATALOG_KEY
                    + " = ? AND "
                    + DATABASE_NAME
                    + " = ? AND "
                    + DATABASE_PROPERTY_KEY
                    + " IN ";
    static final String DELETE_ALL_DATABASE_PROPERTIES_SQL =
            "DELETE FROM "
                    + DATABASE_PROPERTIES_TABLE_NAME
                    + " WHERE "
                    + CATALOG_KEY
                    + " = ? AND "
                    + DATABASE_NAME
                    + " = ?";
    static final String LIST_ALL_PROPERTY_DATABASES_SQL =
            "SELECT DISTINCT "
                    + DATABASE_NAME
                    + " FROM "
                    + DATABASE_PROPERTIES_TABLE_NAME
                    + " WHERE "
                    + CATALOG_KEY
                    + " = ?";

    // Distributed locks table
    static final String DISTRIBUTED_LOCKS_TABLE_NAME = "paimon_distributed_locks";
    static final String LOCK_ID = "lock_id";
    static final String ACQUIRED_AT = "acquired_at";
    static final String EXPIRE_TIME = "expire_time_seconds";

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
            JdbcClientPool connections, String storeKey, String databaseName, String tableName)
            throws SQLException, InterruptedException {
        return connections.run(
                conn -> {
                    Map<String, String> table = Maps.newHashMap();

                    try (PreparedStatement sql = conn.prepareStatement(JdbcUtils.GET_TABLE_SQL)) {
                        sql.setString(1, storeKey);
                        sql.setString(2, databaseName);
                        sql.setString(3, tableName);
                        ResultSet rs = sql.executeQuery();
                        if (rs.next()) {
                            table.put(CATALOG_KEY, rs.getString(CATALOG_KEY));
                            table.put(TABLE_DATABASE, rs.getString(TABLE_DATABASE));
                            table.put(TABLE_NAME, rs.getString(TABLE_NAME));
                        }
                        rs.close();
                    }
                    return table;
                });
    }

    public static void updateTable(
            JdbcClientPool connections, String storeKey, Identifier fromTable, Identifier toTable) {
        int updatedRecords =
                execute(
                        err -> {
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
                        storeKey,
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

    public static boolean databaseExists(
            JdbcClientPool connections, String storeKey, String databaseName) {

        if (exists(connections, JdbcUtils.GET_DATABASE_SQL, storeKey, databaseName)) {
            return true;
        }

        if (exists(connections, JdbcUtils.GET_DATABASE_PROPERTIES_SQL, storeKey, databaseName)) {
            return true;
        }
        return false;
    }

    public static boolean tableExists(
            JdbcClientPool connections, String storeKey, String databaseName, String tableName) {
        if (exists(connections, JdbcUtils.GET_TABLE_SQL, storeKey, databaseName, tableName)) {
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
            String storeKey,
            String databaseName,
            Map<String, String> properties) {
        String[] args =
                properties.entrySet().stream()
                        .flatMap(
                                entry ->
                                        Stream.of(
                                                storeKey,
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

    public static boolean deleteProperties(
            JdbcClientPool connections,
            String storeKey,
            String databaseName,
            Set<String> removeKeys) {
        String[] args =
                Stream.concat(Stream.of(storeKey, databaseName), removeKeys.stream())
                        .toArray(String[]::new);

        int deleteRecords =
                execute(connections, JdbcUtils.deletePropertiesStatement(removeKeys), args);
        if (deleteRecords > 0) {
            return true;
        }
        throw new IllegalStateException(
                String.format(
                        "Failed to delete: %d of %d succeeded", deleteRecords, removeKeys.size()));
    }

    public static void createDistributedLockTable(JdbcClientPool connections, Options options)
            throws SQLException, InterruptedException {
        DistributedLockDialectFactory.create(connections.getProtocol())
                .createTable(connections, options);
    }

    public static boolean acquire(
            JdbcClientPool connections, String lockId, long timeoutMillSeconds)
            throws SQLException, InterruptedException {
        JdbcDistributedLockDialect distributedLockDialect =
                DistributedLockDialectFactory.create(connections.getProtocol());
        // Check and clear expire lock.
        int affectedRows = distributedLockDialect.tryReleaseTimedOutLock(connections, lockId);
        if (affectedRows > 0) {
            LOG.debug("Successfully cleared " + affectedRows + " lock records");
        }
        return distributedLockDialect.lockAcquire(connections, lockId, timeoutMillSeconds);
    }

    public static void release(JdbcClientPool connections, String lockId)
            throws SQLException, InterruptedException {
        DistributedLockDialectFactory.create(connections.getProtocol())
                .releaseLock(connections, lockId);
    }

    private static String deletePropertiesStatement(Set<String> properties) {
        StringBuilder sqlStatement = new StringBuilder(JdbcUtils.DELETE_DATABASE_PROPERTIES_SQL);
        String values =
                String.join(",", Collections.nCopies(properties.size(), String.valueOf('?')));
        sqlStatement.append("(").append(values).append(")");

        return sqlStatement.toString();
    }
}
