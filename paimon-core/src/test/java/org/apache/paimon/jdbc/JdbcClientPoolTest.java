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

import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link JdbcClientPool} connection validation. */
public class JdbcClientPoolTest {

    private JdbcClientPool createPool(int poolSize) {
        String dbUrl =
                "jdbc:sqlite:file::memory:?ic" + UUID.randomUUID().toString().replace("-", "");
        return new JdbcClientPool(poolSize, dbUrl, Collections.emptyMap());
    }

    @Test
    public void testValidConnectionIsReused() throws SQLException, InterruptedException {
        JdbcClientPool pool = createPool(1);
        try {
            AtomicReference<Connection> firstConn = new AtomicReference<>();
            AtomicReference<Connection> secondConn = new AtomicReference<>();

            pool.run(
                    connection -> {
                        firstConn.set(connection);
                        return null;
                    });

            pool.run(
                    connection -> {
                        secondConn.set(connection);
                        return null;
                    });

            assertThat(secondConn.get()).isSameAs(firstConn.get());
        } finally {
            pool.close();
        }
    }

    @Test
    public void testClosedConnectionIsReplaced() throws SQLException, InterruptedException {
        JdbcClientPool pool = createPool(1);
        try {
            AtomicReference<Connection> firstConn = new AtomicReference<>();
            AtomicReference<Connection> secondConn = new AtomicReference<>();

            // Get the connection and close it to simulate a stale connection
            pool.run(
                    connection -> {
                        firstConn.set(connection);
                        connection.close();
                        return null;
                    });

            // The pool should detect the closed connection and create a new one
            pool.run(
                    connection -> {
                        secondConn.set(connection);
                        return null;
                    });

            assertThat(secondConn.get()).isNotSameAs(firstConn.get());
            assertThat(secondConn.get().isClosed()).isFalse();
        } finally {
            pool.close();
        }
    }

    @Test
    public void testReplacedConnectionIsReturnedToPool() throws SQLException, InterruptedException {
        JdbcClientPool pool = createPool(1);
        try {
            AtomicReference<Connection> replacedConn = new AtomicReference<>();
            AtomicReference<Connection> thirdConn = new AtomicReference<>();

            // Close the connection to trigger replacement
            pool.run(
                    connection -> {
                        connection.close();
                        return null;
                    });

            // This call gets the replacement connection
            pool.run(
                    connection -> {
                        replacedConn.set(connection);
                        return null;
                    });

            // The replacement should be reused since it's valid
            pool.run(
                    connection -> {
                        thirdConn.set(connection);
                        return null;
                    });

            assertThat(thirdConn.get()).isSameAs(replacedConn.get());
        } finally {
            pool.close();
        }
    }

    @Test
    public void testActionIsExecutedOnValidConnection() throws SQLException, InterruptedException {
        JdbcClientPool pool = createPool(1);
        try {
            // Close the connection to simulate staleness
            pool.run(
                    connection -> {
                        connection.close();
                        return null;
                    });

            // The action should receive a valid connection and succeed
            boolean result =
                    pool.run(
                            connection -> {
                                // Execute a real SQL statement to verify the connection works
                                return connection.prepareStatement("SELECT 1").execute();
                            });

            assertThat(result).isTrue();
        } finally {
            pool.close();
        }
    }

    @Test
    public void testIsClosedReportsCorrectly() {
        JdbcClientPool pool = createPool(1);
        assertThat(pool.isClosed()).isFalse();
        pool.close();
        assertThat(pool.isClosed()).isTrue();
    }

    @Test
    public void testConnectionClosedWhenPoolClosedDuringAction() throws Exception {
        JdbcClientPool pool = createPool(1);
        AtomicReference<Connection> connRef = new AtomicReference<>();
        CountDownLatch actionStarted = new CountDownLatch(1);
        CountDownLatch poolClosed = new CountDownLatch(1);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> future =
                    executor.submit(
                            () -> {
                                try {
                                    pool.run(
                                            connection -> {
                                                connRef.set(connection);
                                                actionStarted.countDown();
                                                try {
                                                    poolClosed.await();
                                                } catch (InterruptedException e) {
                                                    Thread.currentThread().interrupt();
                                                }
                                                return null;
                                            });
                                } catch (Exception e) {
                                    // expected
                                }
                            });

            actionStarted.await();
            pool.close();
            poolClosed.countDown();
            future.get();

            assertThat(connRef.get().isClosed()).isTrue();
        } finally {
            executor.shutdown();
        }
    }

    @Test
    public void testPoolThrowsAfterClose() {
        JdbcClientPool pool = createPool(1);
        pool.close();

        assertThatThrownBy(
                        () ->
                                pool.run(
                                        connection -> {
                                            return null;
                                        }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed pool");
    }

    @Test
    public void testLockContextRecreatesPoolAfterClose() {
        String dbUrl =
                "jdbc:sqlite:file::memory:?ic" + UUID.randomUUID().toString().replace("-", "");
        Options options = new Options();
        options.set(CatalogOptions.CLIENT_POOL_SIZE, 1);
        options.set(CatalogOptions.URI.key(), dbUrl);

        JdbcCatalogLockContext context = new JdbcCatalogLockContext("test-catalog", options);

        JdbcClientPool firstPool = context.connections();
        assertThat(firstPool.isClosed()).isFalse();

        firstPool.close();
        assertThat(firstPool.isClosed()).isTrue();

        JdbcClientPool secondPool = context.connections();
        assertThat(secondPool).isNotSameAs(firstPool);
        assertThat(secondPool.isClosed()).isFalse();
        secondPool.close();
    }
}
