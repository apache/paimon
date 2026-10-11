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
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.TestCatalogLockFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests JDBC locking through the catalog's own options and policy. */
class JdbcCatalogLockConfigurationTest {
    @TempDir Path directory;

    @ParameterizedTest
    @CsvSource({"false,,false", "true,,true", "false,true,true", "true,false,false"})
    void jdbcPreservesStorageDefaultAndExplicitOverride(
            boolean objectStore, Boolean enabled, boolean expected) throws Exception {
        Options options = options();
        options.set(JdbcCatalogOptions.LOCK_ACQUIRE_TIMEOUT, Duration.ofSeconds(3));
        options.set(JdbcCatalogOptions.LOCK_CHECK_MAX_SLEEP, Duration.ofMillis(17));
        if (enabled != null) {
            options.set(JdbcCatalogOptions.LOCK_ENABLED, enabled);
        }
        try (JdbcCatalog catalog = catalog(options, objectStore);
                Lock lock =
                        catalog.createLock(
                                Identifier.create("db", "table"), null, "writer", new Options())) {
            lock.runWithLock(
                    () -> {
                        assertThat(lockTableExists(catalog)).isEqualTo(expected);
                        if (expected) {
                            catalog.getConnections()
                                    .execute(
                                            connection -> {
                                                try (PreparedStatement statement =
                                                                connection.prepareStatement(
                                                                        "SELECT lock_owner, expire_time_seconds FROM paimon_distributed_locks");
                                                        ResultSet result =
                                                                statement.executeQuery()) {
                                                    assertThat(result.next()).isTrue();
                                                    assertThat(result.getString(1)).isNotEmpty();
                                                    assertThat(result.getLong(2)).isEqualTo(5);
                                                }
                                            });
                        }
                        return null;
                    });
            assertThat(JdbcCatalogLock.checkMaxSleep(options.toMap())).isEqualTo(17);
        }
    }

    @Test
    void configuredFactoryOverridesJdbcDefault() throws Exception {
        Options options = options();
        options.set(JdbcCatalogOptions.LOCK_ENABLED, true);
        options.set(JdbcCatalogOptions.LOCK_TYPE, TestCatalogLockFactory.IDENTIFIER);
        try (JdbcCatalog catalog = catalog(options, false);
                Lock lock =
                        catalog.createLock(
                                Identifier.create("db", "table"), null, "writer", new Options())) {
            assertThat(lock.runWithLock(() -> options.get("test.lock.held"))).isEqualTo("true");
            assertThat(
                            catalog.runWithLock(
                                    Identifier.create("db", "table"),
                                    () -> options.get("test.lock.held")))
                    .isEqualTo("true");
        }
        assertThat(options.get("test.lock.held")).isEqualTo("false");
        assertThat(options.get("test.lock.closed")).isEqualTo("true");
    }

    @Test
    void disabledJdbcLockDoesNotResolveFactoryOrCreateLeaseTable() throws Exception {
        Options options = options();
        options.set(JdbcCatalogOptions.LOCK_ENABLED, false);
        options.set(JdbcCatalogOptions.LOCK_TYPE, "not-installed");
        try (JdbcCatalog catalog = catalog(options, true);
                Lock lock =
                        catalog.createLock(
                                Identifier.create("db", "table"), null, "writer", new Options())) {
            assertThat(lock.runWithLock(() -> "published")).isEqualTo("published");
            assertThat(lockTableExists(catalog)).isFalse();
        }
    }

    private Options options() {
        Options options = new Options();
        options.set(CatalogOptions.URI, "jdbc:sqlite:" + directory.resolve("catalog.db"));
        return options;
    }

    private JdbcCatalog catalog(Options options, boolean objectStore) {
        FileIO fileIO = mock(FileIO.class);
        when(fileIO.isObjectStore()).thenReturn(objectStore);
        return new JdbcCatalog(
                fileIO, "configured", CatalogContext.create(options), directory.toString());
    }

    private boolean lockTableExists(JdbcCatalog catalog) throws Exception {
        return catalog.getConnections()
                .run(
                        connection -> {
                            try (ResultSet tables =
                                    connection
                                            .getMetaData()
                                            .getTables(
                                                    null, null, "paimon_distributed_locks", null)) {
                                return tables.next();
                            }
                        });
    }
}
