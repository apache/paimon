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

import org.apache.paimon.options.Options;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/** Jdbc distributed lock interface. */
public abstract class AbstractDistributedLockDialect implements JdbcDistributedLockDialect {

    @Override
    public void createTable(JdbcClientPool connections, Options options)
            throws SQLException, InterruptedException {
        Integer lockKeyMaxLength = JdbcCatalogOptions.lockKeyMaxLength(options);
        connections.run(
                conn -> {
                    DatabaseMetaData dbMeta = conn.getMetaData();
                    try (ResultSet tableExists =
                            dbMeta.getTables(
                                    null, null, JdbcUtils.DISTRIBUTED_LOCKS_TABLE_NAME, null)) {
                        if (!tableExists.next()) {
                            try (PreparedStatement statement =
                                    conn.prepareStatement(
                                            String.format(getCreateTableSql(), lockKeyMaxLength))) {
                                statement.execute();
                            }
                        }
                    }
                    // Older catalogs created the lock table without an owner column.
                    try (ResultSet columns =
                            dbMeta.getColumns(
                                    null,
                                    null,
                                    JdbcUtils.DISTRIBUTED_LOCKS_TABLE_NAME,
                                    "lock_owner")) {
                        if (!columns.next()) {
                            try (PreparedStatement statement =
                                    conn.prepareStatement(
                                            "ALTER TABLE "
                                                    + JdbcUtils.DISTRIBUTED_LOCKS_TABLE_NAME
                                                    + " ADD COLUMN lock_owner VARCHAR(36)")) {
                                statement.execute();
                            } catch (SQLException e) {
                                // Another catalog may have upgraded the table concurrently.
                                try (ResultSet updated =
                                        dbMeta.getColumns(
                                                null,
                                                null,
                                                JdbcUtils.DISTRIBUTED_LOCKS_TABLE_NAME,
                                                "lock_owner")) {
                                    if (!updated.next()) {
                                        throw e;
                                    }
                                }
                            }
                        }
                    }
                    return true;
                });
    }

    public abstract String getCreateTableSql();

    @Override
    public boolean lockAcquire(JdbcClientPool connections, String lockId, long timeoutMillSeconds)
            throws SQLException, InterruptedException {
        return connections.run(
                connection -> {
                    try (PreparedStatement preparedStatement =
                            connection.prepareStatement(getLockAcquireSql())) {
                        preparedStatement.setString(1, lockId);
                        preparedStatement.setLong(2, timeoutMillSeconds / 1000);
                        return preparedStatement.executeUpdate() > 0;
                    } catch (SQLException ex) {
                        return false;
                    }
                });
    }

    boolean acquireOwned(JdbcClientPool connections, String lockId, String owner, long leaseMillis)
            throws SQLException, InterruptedException {
        tryReleaseTimedOutLock(connections, lockId);
        return connections.run(
                connection -> {
                    try (PreparedStatement statement =
                            connection.prepareStatement(
                                    "INSERT INTO "
                                            + JdbcUtils.DISTRIBUTED_LOCKS_TABLE_NAME
                                            + " (lock_id, expire_time_seconds, lock_owner) VALUES (?, ?, ?)")) {
                        statement.setString(1, lockId);
                        // A second of precision is lost by the timestamp column; keep a
                        // conservative
                        // millisecond deadline in the holder and round the database TTL up.
                        statement.setLong(2, Math.max(1, leaseMillis / 1000 + 2));
                        statement.setString(3, owner);
                        return statement.executeUpdate() > 0;
                    } catch (SQLException e) {
                        if (isDuplicateKey(e)) {
                            return false;
                        }
                        throw e;
                    }
                });
    }

    private boolean isDuplicateKey(SQLException e) {
        return "23505".equals(e.getSQLState())
                || (e.getErrorCode() == 1062 && "23000".equals(e.getSQLState()))
                || e.getErrorCode() == 1555
                || e.getErrorCode() == 2067
                || (e.getErrorCode() == 19
                        && e.getMessage() != null
                        && e.getMessage().contains("UNIQUE constraint failed"));
    }

    protected String getExpirationCondition() {
        throw new UnsupportedOperationException("Owned leases are not supported by this dialect.");
    }

    protected String getRenewalTime() {
        return "CURRENT_TIMESTAMP";
    }

    protected String getOwnedExpirationCondition() {
        return getExpirationCondition();
    }

    boolean renewOwned(JdbcClientPool connections, String lockId, String owner)
            throws SQLException, InterruptedException {
        return updateOwned(
                connections,
                "UPDATE "
                        + JdbcUtils.DISTRIBUTED_LOCKS_TABLE_NAME
                        + " SET acquired_at = CASE WHEN acquired_at > "
                        + getRenewalTime()
                        + " THEN acquired_at ELSE "
                        + getRenewalTime()
                        + " END WHERE lock_id = ? AND lock_owner = ? AND NOT ("
                        + getOwnedExpirationCondition()
                        + ")",
                lockId,
                owner);
    }

    boolean releaseOwned(JdbcClientPool connections, String lockId, String owner)
            throws SQLException, InterruptedException {
        return updateOwned(
                connections,
                "DELETE FROM "
                        + JdbcUtils.DISTRIBUTED_LOCKS_TABLE_NAME
                        + " WHERE lock_id = ? AND lock_owner = ?",
                lockId,
                owner);
    }

    private boolean updateOwned(JdbcClientPool connections, String sql, String lockId, String owner)
            throws SQLException, InterruptedException {
        return connections.run(
                connection -> {
                    try (PreparedStatement statement = connection.prepareStatement(sql)) {
                        statement.setString(1, lockId);
                        statement.setString(2, owner);
                        return statement.executeUpdate() > 0;
                    }
                });
    }

    public abstract String getLockAcquireSql();

    @Override
    public boolean releaseLock(JdbcClientPool connections, String lockId)
            throws SQLException, InterruptedException {
        return connections.run(
                connection -> {
                    try (PreparedStatement preparedStatement =
                            connection.prepareStatement(getReleaseLockSql())) {
                        preparedStatement.setString(1, lockId);
                        return preparedStatement.executeUpdate() > 0;
                    }
                });
    }

    public abstract String getReleaseLockSql();

    @Override
    public int tryReleaseTimedOutLock(JdbcClientPool connections, String lockId)
            throws SQLException, InterruptedException {
        return connections.run(
                connection -> {
                    try (PreparedStatement preparedStatement =
                            connection.prepareStatement(getTryReleaseTimedOutLock())) {
                        preparedStatement.setString(1, lockId);
                        return preparedStatement.executeUpdate();
                    }
                });
    }

    public abstract String getTryReleaseTimedOutLock();
}
