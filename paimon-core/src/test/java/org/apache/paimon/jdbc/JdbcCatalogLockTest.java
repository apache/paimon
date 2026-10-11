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
import org.apache.paimon.client.ClientPool;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests renewable JDBC ownership using a real database and a single-connection pool. */
class JdbcCatalogLockTest {
    @TempDir Path directory;
    private final ScheduledExecutorService renewer = mock(ScheduledExecutorService.class);
    private final ScheduledFuture<?> future = mock(ScheduledFuture.class);
    private final AtomicLong clock = new AtomicLong();
    private final AbstractDistributedLockDialect dialect = new SqlLiteDistributedLockDialect();

    @Test
    void callbackCanUseThePoolAndRenewalExtendsLifetime() throws Exception {
        try (JdbcClientPool pool = pool()) {
            dialect.createTable(pool, new Options());
            JdbcCatalogLock lock = lock(pool);
            Runnable callback =
                    lock.runWithLock(
                            () -> {
                                // The holder must not keep a pooled connection while the callback
                                // borrows one.
                                assertThat(owner(pool)).isNotEmpty();
                                clock.set(TimeUnit.MILLISECONDS.toNanos(2000));
                                Runnable renewal = renewal();
                                renewal.run();
                                clock.set(TimeUnit.MILLISECONDS.toNanos(4000));
                                lock.ensureValid();
                                return renewal;
                            });
            assertThat(owner(pool)).isNull();
            callback.run();
            assertThat(owner(pool)).isNull();
            verify(future).cancel(false);
        }
    }

    @Test
    void formerHolderCannotRenewOrReleaseSuccessor() throws Exception {
        try (JdbcClientPool pool = pool()) {
            dialect.createTable(pool, new Options());
            JdbcCatalogLock lock = lock(pool);
            lock.runWithLock(
                    () -> {
                        dialect.releaseLock(pool, "catalog.db.table");
                        assertThat(
                                        dialect.acquireOwned(
                                                pool, "catalog.db.table", "successor", 3000))
                                .isTrue();
                        renewal().run();
                        assertThatThrownBy(lock::ensureValid)
                                .hasMessageContaining("no longer usable");
                        assertThat(owner(pool)).isEqualTo("successor");
                        return null;
                    });
            assertThat(owner(pool)).isEqualTo("successor");
        }
    }

    @Test
    void localExpiryInvalidatesHolderWithoutScheduledRenewal() throws Exception {
        try (JdbcClientPool pool = pool()) {
            dialect.createTable(pool, new Options());
            JdbcCatalogLock lock = lock(pool);
            lock.runWithLock(
                    () -> {
                        clock.set(TimeUnit.MILLISECONDS.toNanos(3000));
                        assertThatThrownBy(lock::ensureValid)
                                .hasCauseInstanceOf(IllegalStateException.class);
                        renewal().run();
                        assertThatThrownBy(lock::ensureValid)
                                .hasMessageContaining("no longer usable");
                        return null;
                    });
            assertThat(owner(pool)).isNull();
        }
    }

    @Test
    void legacyLockTableIsUpgradedWithoutRemovingRows() throws Exception {
        try (JdbcClientPool pool = pool()) {
            pool.execute(
                    connection -> {
                        try (PreparedStatement statement =
                                connection.prepareStatement(
                                        "CREATE TABLE paimon_distributed_locks (lock_id VARCHAR(255) PRIMARY KEY, "
                                                + "acquired_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL, "
                                                + "expire_time_seconds BIGINT DEFAULT 0 NOT NULL)")) {
                            statement.execute();
                        }
                    });
            assertThat(dialect.lockAcquire(pool, "old-lock", 3000)).isTrue();
            dialect.createTable(pool, new Options());
            dialect.createTable(pool, new Options());
            assertThat(dialect.lockAcquire(pool, "old-lock", 3000)).isFalse();
            assertThat(dialect.acquireOwned(pool, "new-lock", "new-owner", 3000)).isTrue();
            assertThat(dialect.releaseOwned(pool, "new-lock", "other-owner")).isFalse();
            assertThat(dialect.releaseOwned(pool, "new-lock", "new-owner")).isTrue();
        }
    }

    @Test
    void delayedHeartbeatCannotReviveAnExpiredDatabaseRow() throws Exception {
        try (JdbcClientPool pool = pool()) {
            dialect.createTable(pool, new Options());
            assertThat(dialect.acquireOwned(pool, "lock", "owner", 3000)).isTrue();
            assertThat(dialect.acquireOwned(pool, "lock", "contender", 3000)).isFalse();
            pool.execute(
                    connection -> {
                        try (PreparedStatement statement =
                                connection.prepareStatement(
                                        "UPDATE paimon_distributed_locks SET acquired_at = '2000-01-01 00:00:00'")) {
                            statement.executeUpdate();
                        }
                    });
            assertThat(dialect.renewOwned(pool, "lock", "owner")).isFalse();
            assertThat(dialect.acquireOwned(pool, "lock", "contender", 3000)).isTrue();
        }
    }

    @Test
    void zeroAffectedRowsDoesNotInvalidateUnchangedLease() throws Exception {
        try (JdbcClientPool pool = pool()) {
            dialect.createTable(pool, new Options());
            JdbcCatalogLock lock = lock(zeroUpdateCountPool(pool));
            assertThat(
                            lock.runWithLock(
                                    () -> {
                                        lock.ensureValid();
                                        assertThat(owner(pool)).isNotEmpty();
                                        return "published";
                                    }))
                    .isEqualTo("published");
            assertThat(owner(pool)).isNull();
        }
    }

    @Test
    void zeroAffectedRowsStillRejectsMissingExpiredOrReplacedOwner() throws Exception {
        try (JdbcClientPool pool = pool()) {
            dialect.createTable(pool, new Options());
            JdbcClientPool affectedRowsPool = zeroUpdateCountPool(pool);
            assertThat(dialect.acquireOwned(pool, "lock", "owner", 3000)).isTrue();
            assertThat(dialect.renewOwned(affectedRowsPool, "missing", "owner")).isFalse();
            assertThat(dialect.renewOwned(affectedRowsPool, "lock", "other-owner")).isFalse();
            pool.execute(
                    connection -> {
                        try (PreparedStatement statement =
                                connection.prepareStatement(
                                        "UPDATE paimon_distributed_locks SET acquired_at = '2000-01-01 00:00:00'")) {
                            statement.executeUpdate();
                        }
                    });
            assertThat(dialect.renewOwned(affectedRowsPool, "lock", "owner")).isFalse();
            assertThat(dialect.acquireOwned(pool, "lock", "successor", 3000)).isTrue();
            assertThat(dialect.renewOwned(affectedRowsPool, "lock", "owner")).isFalse();
            assertThat(dialect.renewOwned(affectedRowsPool, "lock", "successor")).isTrue();
        }
    }

    @Test
    void acquisitionPropagatesSqlFailuresOtherThanDuplicateKeys() throws Exception {
        JdbcClientPool pool = mock(JdbcClientPool.class);
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        SQLException failure = new SQLException("Permission denied", "42501");
        when(pool.run(any()))
                .thenAnswer(
                        invocation ->
                                ((ClientPool.Action<?, Connection, SQLException>)
                                                invocation.getArgument(0))
                                        .run(connection));
        when(connection.prepareStatement(dialect.getTryReleaseTimedOutLock()))
                .thenReturn(statement);
        when(connection.prepareStatement(
                        "INSERT INTO paimon_distributed_locks (lock_id, expire_time_seconds, lock_owner) VALUES (?, ?, ?)"))
                .thenThrow(failure);
        assertThatThrownBy(() -> dialect.acquireOwned(pool, "lock", "owner", 3000))
                .isSameAs(failure);
    }

    @Test
    void olderHeartbeatResponseCannotShortenANewerDeadline() throws Exception {
        JdbcClientPool pool = mock(JdbcClientPool.class);
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        PreparedStatement heartbeat = mock(PreparedStatement.class);
        when(pool.getProtocol()).thenReturn("sqlite");
        when(pool.run(any()))
                .thenAnswer(
                        invocation ->
                                ((ClientPool.Action<?, Connection, SQLException>)
                                                invocation.getArgument(0))
                                        .run(connection));
        when(connection.prepareStatement(anyString()))
                .thenAnswer(
                        invocation ->
                                ((String) invocation.getArgument(0)).startsWith("UPDATE")
                                        ? heartbeat
                                        : statement);
        when(statement.executeUpdate()).thenReturn(1);
        JdbcCatalogLock lock = lock(pool);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch oldStarted = new CountDownLatch(1);
        CountDownLatch oldResponse = new CountDownLatch(1);
        AtomicInteger requests = new AtomicInteger();
        try {
            lock.runWithLock(
                    () -> {
                        when(heartbeat.executeUpdate())
                                .thenAnswer(
                                        invocation -> {
                                            if (requests.incrementAndGet() == 1) {
                                                oldStarted.countDown();
                                                assertThat(oldResponse.await(10, TimeUnit.SECONDS))
                                                        .isTrue();
                                            }
                                            return 1;
                                        });
                        clock.set(TimeUnit.MILLISECONDS.toNanos(1000));
                        Future<?> old = executor.submit(renewal());
                        assertThat(oldStarted.await(10, TimeUnit.SECONDS)).isTrue();
                        clock.set(TimeUnit.MILLISECONDS.toNanos(2000));
                        lock.ensureValid();
                        oldResponse.countDown();
                        old.get(10, TimeUnit.SECONDS);
                        clock.set(TimeUnit.MILLISECONDS.toNanos(4000));
                        lock.ensureValid();
                        return null;
                    });
        } finally {
            oldResponse.countDown();
            executor.shutdownNow();
        }
    }

    private JdbcClientPool pool() {
        return new JdbcClientPool(
                1, "jdbc:sqlite:" + directory.resolve("locks.db"), Collections.emptyMap());
    }

    private JdbcClientPool zeroUpdateCountPool(JdbcClientPool pool) throws Exception {
        JdbcClientPool affectedRowsPool = mock(JdbcClientPool.class);
        when(affectedRowsPool.getProtocol()).thenReturn(pool.getProtocol());
        when(affectedRowsPool.run(any()))
                .thenAnswer(
                        invocation ->
                                pool.run(
                                        connection ->
                                                ((ClientPool.Action<?, Connection, SQLException>)
                                                                invocation.getArgument(0))
                                                        .run(
                                                                zeroUpdateCountConnection(
                                                                        connection))));
        return affectedRowsPool;
    }

    private Connection zeroUpdateCountConnection(Connection connection) throws SQLException {
        Connection reported = mock(Connection.class, delegatesTo(connection));
        doAnswer(
                        preparation -> {
                            String sql = preparation.getArgument(0);
                            PreparedStatement statement = connection.prepareStatement(sql);
                            if (!sql.startsWith("UPDATE ")) {
                                return statement;
                            }
                            PreparedStatement changedRows =
                                    mock(PreparedStatement.class, delegatesTo(statement));
                            doAnswer(
                                            update -> {
                                                statement.executeUpdate();
                                                return 0;
                                            })
                                    .when(changedRows)
                                    .executeUpdate();
                            return changedRows;
                        })
                .when(reported)
                .prepareStatement(anyString());
        return reported;
    }

    private JdbcCatalogLock lock(JdbcClientPool pool) {
        when(renewer.scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any()))
                .thenAnswer(invocation -> future);
        return new JdbcCatalogLock(
                pool, "catalog", Identifier.create("db", "table"), 10, 3000, renewer, clock::get);
    }

    private String owner(JdbcClientPool pool) throws Exception {
        return pool.run(
                connection -> {
                    try (PreparedStatement statement =
                                    connection.prepareStatement(
                                            "SELECT lock_owner FROM paimon_distributed_locks WHERE lock_id = 'catalog.db.table'");
                            ResultSet result = statement.executeQuery()) {
                        return result.next() ? result.getString(1) : null;
                    }
                });
    }

    private Runnable renewal() {
        ArgumentCaptor<Runnable> callback = ArgumentCaptor.forClass(Runnable.class);
        verify(renewer).scheduleWithFixedDelay(callback.capture(), anyLong(), anyLong(), any());
        return callback.getValue();
    }
}
