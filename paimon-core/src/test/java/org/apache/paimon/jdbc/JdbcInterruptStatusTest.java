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

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The JDBC catalog turns an {@link InterruptedException} into an unchecked exception in nine
 * places. Seven of them re-assert the interrupt before rethrowing; these tests cover the two that
 * did not, so the thread does not silently come back out of them looking un-cancelled.
 *
 * <p>Both cases drive the interrupt through a stubbed {@link JdbcClientPool} rather than through a
 * real one. For the constructor that means seeding {@link CachedJdbcClientPool}'s shared cache, so
 * no real connection is ever opened and the interrupt cannot be consumed by driver initialisation
 * before the code under test runs.
 */
class JdbcInterruptStatusTest {

    @TempDir Path tempDir;

    @AfterEach
    void tearDown() {
        CachedJdbcClientPool.resetCache();
        // These tests deliberately leave the flag set; clear it so it cannot leak into whatever
        // JUnit runs next on this thread.
        Thread.interrupted();
    }

    @Test
    void catalogConstructorKeepsTheInterruptStatus() throws Exception {
        Options options = catalogOptions();
        seedPoolCache(options, interruptingPool());

        assertThatThrownBy(
                        () ->
                                new JdbcCatalog(
                                        LocalFileIO.create(),
                                        "interrupt-test-catalog",
                                        CatalogContext.create(options),
                                        tempDir.toString()))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Interrupted in call to initialize");

        assertThat(Thread.currentThread().isInterrupted()).isTrue();
    }

    @Test
    void insertTableKeepsTheInterruptStatus() throws Exception {
        assertThatThrownBy(
                        () ->
                                JdbcUtils.insertTable(
                                        interruptingPool(), "catalog-key", "some_db", "some_table"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to insert table: some_table");

        assertThat(Thread.currentThread().isInterrupted()).isTrue();
    }

    private static JdbcClientPool interruptingPool() throws Exception {
        JdbcClientPool connections = mock(JdbcClientPool.class);
        when(connections.run(any())).thenThrow(new InterruptedException("interrupted"));
        return connections;
    }

    private Options catalogOptions() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogOptions.URI.key(), "jdbc:sqlite:file:interrupt-test?mode=memory");
        properties.put(JdbcCatalog.PROPERTY_PREFIX + "username", "user");
        properties.put(JdbcCatalog.PROPERTY_PREFIX + "password", "password");
        properties.put(CatalogOptions.WAREHOUSE.key(), tempDir.toString());
        return Options.fromMap(properties);
    }

    /**
     * Mirrors how {@link CachedJdbcClientPool} derives its key, so {@code get()} finds this pool.
     */
    private static void seedPoolCache(Options options, JdbcClientPool pool) {
        CachedJdbcClientPool.clientPools()
                .put(
                        CachedJdbcClientPool.Key.of(
                                options.get(CatalogOptions.URI),
                                options.get(JdbcCatalogOptions.CATALOG_KEY),
                                options.get(CatalogOptions.CLIENT_POOL_SIZE),
                                JdbcUtils.extractJdbcConfiguration(
                                        options.toMap(), JdbcCatalog.PROPERTY_PREFIX)),
                        pool);
    }
}
