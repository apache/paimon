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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;

import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.sql.SQLException;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

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

/** Tests for {@link JdbcCatalog} with Postgres. */
public class PostgresqlCatalogTest {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresqlCatalogTest.class);

    public static final String DEFAULT_DB = "postgres";

    private static final String USER = "paimonuser";
    private static final String PASSWORD = "paimonpw";

    @TempDir java.nio.file.Path tempFile;
    protected String warehouse;
    protected FileIO fileIO;
    protected Catalog catalog;

    protected static final PostgreSQLContainer<?> POSTGRES_CONTAINER =
            new PostgreSQLContainer<>("postgres:13-alpine")
                    .withDatabaseName(DEFAULT_DB)
                    .withUsername(USER)
                    .withPassword(PASSWORD)
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @BeforeAll
    protected static void start() {
        LOG.info("Starting containers...");
        POSTGRES_CONTAINER.start();
        LOG.info("Containers are started.");
    }

    @AfterAll
    public static void stopContainers() {
        LOG.info("Stopping containers...");
        POSTGRES_CONTAINER.stop();
        LOG.info("Containers are stopped.");
    }

    @BeforeEach
    public void setUp() throws Exception {
        warehouse = tempFile.toUri().toString();
        Options catalogOptions = new Options();
        catalogOptions.set(CatalogOptions.WAREHOUSE, warehouse);
        CatalogContext catalogContext = CatalogContext.create(catalogOptions);
        fileIO = FileIO.get(new Path(warehouse), catalogContext);
        catalog = initCatalog(Maps.newHashMap());
    }

    private JdbcCatalog initCatalog(Map<String, String> props) {
        LOG.info("Init catalog {}", POSTGRES_CONTAINER.getJdbcUrl());

        Map<String, String> properties = Maps.newHashMap();
        properties.put(CatalogOptions.URI.key(), POSTGRES_CONTAINER.getJdbcUrl());

        properties.put(JdbcCatalog.PROPERTY_PREFIX + "user", USER);
        properties.put(JdbcCatalog.PROPERTY_PREFIX + "password", PASSWORD);
        properties.put(CatalogOptions.WAREHOUSE.key(), warehouse);
        properties.put(CatalogOptions.LOCK_ENABLED.key(), "true");
        properties.put(CatalogOptions.LOCK_TYPE.key(), "jdbc");
        properties.putAll(props);
        JdbcCatalog catalog =
                new JdbcCatalog(
                        fileIO,
                        "test-jdbc-postgres-catalog",
                        CatalogContext.create(Options.fromMap(properties)),
                        warehouse);
        assertThat(catalog.warehouse()).isEqualTo(warehouse);
        return catalog;
    }

    @Test
    public void testAcquireLockFail() throws SQLException, InterruptedException {
        String lockId = "jdbc.testDb.testTable";
        assertThat(JdbcUtils.acquire(((JdbcCatalog) catalog).getConnections(), lockId, 3000))
                .isTrue();
        assertThat(JdbcUtils.acquire(((JdbcCatalog) catalog).getConnections(), lockId, 3000))
                .isFalse();
        JdbcUtils.release(((JdbcCatalog) catalog).getConnections(), lockId);
    }

    @Test
    public void testCleanTimeoutLockAndAcquireLock() throws SQLException, InterruptedException {
        String lockId = "jdbc.testDb.testTable";
        assertThat(JdbcUtils.acquire(((JdbcCatalog) catalog).getConnections(), lockId, 1000))
                .isTrue();
        Thread.sleep(2000);
        assertThat(JdbcUtils.acquire(((JdbcCatalog) catalog).getConnections(), lockId, 1000))
                .isTrue();
        JdbcUtils.release(((JdbcCatalog) catalog).getConnections(), lockId);
    }
}
