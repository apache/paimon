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
import org.apache.paimon.catalog.CatalogTestBase;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;

import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/** Tests for {@link JdbcCatalog}. */
public class JdbcCatalogTest extends CatalogTestBase {

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        catalog = initCatalog(Maps.newHashMap());
    }

    private JdbcCatalog initCatalog(Map<String, String> props) {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(
                CatalogOptions.URI.key(),
                "jdbc:sqlite:file::memory:?ic" + UUID.randomUUID().toString().replace("-", ""));

        properties.put(JdbcCatalog.PROPERTY_PREFIX + "username", "user");
        properties.put(JdbcCatalog.PROPERTY_PREFIX + "password", "password");
        properties.put(CatalogOptions.WAREHOUSE.key(), warehouse);
        properties.put(CatalogOptions.LOCK_ENABLED.key(), "true");
        properties.put(CatalogOptions.LOCK_TYPE.key(), "jdbc");
        properties.putAll(props);
        JdbcCatalog catalog =
                new JdbcCatalog(
                        fileIO, "test-jdbc-catalog", Options.fromMap(properties), warehouse);
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
    }

    @Test
    public void testCleanTimeoutLockAndAcquireLock() throws SQLException, InterruptedException {
        String lockId = "jdbc.testDb.testTable";
        assertThat(JdbcUtils.acquire(((JdbcCatalog) catalog).getConnections(), lockId, 1000))
                .isTrue();
        Thread.sleep(2000);
        assertThat(JdbcUtils.acquire(((JdbcCatalog) catalog).getConnections(), lockId, 1000))
                .isTrue();
    }

    @Test
    public void testUpperCase() throws Exception {
        catalog.createDatabase("test_db", false);
        assertThatThrownBy(
                        () ->
                                catalog.createTable(
                                        Identifier.create("TEST_DB", "new_table"),
                                        DEFAULT_TABLE_SCHEMA,
                                        false))
                .isInstanceOf(Catalog.DatabaseNotExistException.class)
                .hasMessage("Database TEST_DB does not exist.");

        catalog.createTable(Identifier.create("test_db", "new_TABLE"), DEFAULT_TABLE_SCHEMA, false);
    }

    @Test
    public void testSerializeTable() throws Exception {
        catalog.createDatabase("test_db", false);
        catalog.createTable(Identifier.create("test_db", "table"), DEFAULT_TABLE_SCHEMA, false);
        Table table = catalog.getTable(new Identifier("test_db", "table"));
        assertDoesNotThrow(
                () -> {
                    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                            ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                        oos.writeObject(table);
                        oos.flush();
                    }
                });
    }

    @Test
    public void testAlterDatabase() throws Exception {
        this.alterDatabaseWhenSupportAlter();
    }
}
