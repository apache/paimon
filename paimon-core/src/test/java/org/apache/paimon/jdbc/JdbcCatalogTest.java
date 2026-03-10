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
import org.apache.paimon.catalog.CatalogTestBase;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;
import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
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
                        fileIO,
                        "test-jdbc-catalog",
                        CatalogContext.create(Options.fromMap(properties)),
                        warehouse);
        assertThat(catalog.warehouse()).isEqualTo(warehouse);
        return catalog;
    }

    @Override // ignore for lock error
    @Test
    public void testGetTable() throws Exception {}

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

    @Override
    protected boolean supportsAlterDatabase() {
        return true;
    }

    @Test
    public void testRepairTableNotExist() throws Exception {
        String databaseName = "repair_db";
        String tableName = "nonexistent_table";

        catalog.createDatabase(databaseName, false);
        Identifier identifier = Identifier.create(databaseName, tableName);

        // Test repair on non-existent table - should throw TableNotExistException
        assertThatThrownBy(() -> catalog.repairTable(identifier))
                .isInstanceOf(Catalog.TableNotExistException.class);
    }

    @Test
    public void testRepairTableWithSystemTable() {
        Identifier systemTableId = Identifier.create("sys", "system_table");

        // System tables should not be repairable
        assertThatThrownBy(() -> catalog.repairTable(systemTableId))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sys");
    }

    @Test
    public void testRepairTable() throws Exception {
        String databaseName = "fs_repair_db";
        String tableName = "fs_repair_table";

        // Create table normally (this creates both filesystem and JDBC entries)
        catalog.createDatabase(databaseName, false);
        Identifier identifier = Identifier.create(databaseName, tableName);
        catalog.createTable(identifier, DEFAULT_TABLE_SCHEMA, false);

        // Verify table exists in both places
        assertThat(catalog.listTables(databaseName)).contains(tableName);
        assertDoesNotThrow(() -> catalog.getTable(identifier));

        // Repair on existing table should work fine (idempotent operation)
        assertDoesNotThrow(() -> catalog.repairTable(identifier));

        // Table should still exist and be accessible
        assertThat(catalog.listTables(databaseName)).contains(tableName);
        assertDoesNotThrow(() -> catalog.getTable(identifier));

        // Test repair when table is missing from JDBC store
        JdbcCatalog jdbcCatalog = (JdbcCatalog) catalog;

        // Remove table from JDBC store but leave filesystem intact
        JdbcUtils.execute(
                jdbcCatalog.getConnections(),
                JdbcUtils.DROP_TABLE_SQL,
                jdbcCatalog.getCatalogKey(),
                databaseName,
                tableName);

        // Verify table is missing from JDBC catalog
        assertThat(catalog.listTables(databaseName)).doesNotContain(tableName);
        assertThatThrownBy(() -> catalog.getTable(identifier))
                .isInstanceOf(Catalog.TableNotExistException.class);

        // Repair the table - should recreate it in JDBC store
        assertDoesNotThrow(() -> catalog.repairTable(identifier));

        // Verify table is back in JDBC catalog after repair
        assertThat(catalog.listTables(databaseName)).contains(tableName);
        assertDoesNotThrow(() -> catalog.getTable(identifier));
    }

    @Test
    public void testRepairDatabase() throws Exception {
        String databaseName = "repair_database";

        // Create database and some tables
        catalog.createDatabase(databaseName, false);
        catalog.createTable(Identifier.create(databaseName, "table1"), DEFAULT_TABLE_SCHEMA, false);
        catalog.createTable(Identifier.create(databaseName, "table2"), DEFAULT_TABLE_SCHEMA, false);

        // Test repair database - should not throw exception and should work correctly
        assertDoesNotThrow(() -> catalog.repairDatabase(databaseName));

        // Verify tables still exist after repair
        List<String> tables = catalog.listTables(databaseName);
        assertThat(tables).containsExactlyInAnyOrder("table1", "table2");

        // Test repair when database is missing from JDBC store
        JdbcCatalog jdbcCatalog = (JdbcCatalog) catalog;

        // Remove database from JDBC store (this also removes tables)
        JdbcUtils.execute(
                jdbcCatalog.getConnections(),
                JdbcUtils.DELETE_TABLES_SQL,
                jdbcCatalog.getCatalogKey(),
                databaseName);
        JdbcUtils.execute(
                jdbcCatalog.getConnections(),
                JdbcUtils.DELETE_ALL_DATABASE_PROPERTIES_SQL,
                jdbcCatalog.getCatalogKey(),
                databaseName);

        // Verify database is missing from JDBC catalog
        assertThat(catalog.listDatabases()).doesNotContain(databaseName);
        assertThatThrownBy(() -> catalog.getDatabase(databaseName))
                .isInstanceOf(Catalog.DatabaseNotExistException.class);

        // Repair the database - should recreate database and tables in JDBC store
        assertDoesNotThrow(() -> catalog.repairDatabase(databaseName));

        // Verify database and tables are back in JDBC catalog after repair
        assertThat(catalog.listDatabases()).contains(databaseName);
        assertThat(catalog.listTables(databaseName)).containsExactlyInAnyOrder("table1", "table2");
        assertDoesNotThrow(() -> catalog.getDatabase(databaseName));
    }

    @Test
    public void testRepairDatabaseSystemDatabase() {
        // System database should not be repairable
        assertThatThrownBy(() -> catalog.repairDatabase("sys"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sys");
    }

    @Test
    public void testRepairDatabaseNotExists() throws Exception {
        String nonExistentDb = "non_existent_db";

        // Repairing a non-existent database should throw RuntimeException
        assertThatThrownBy(() -> catalog.repairDatabase(nonExistentDb))
                .isInstanceOf(RuntimeException.class);

        // Database should not exist after failed repair
        assertThat(catalog.listDatabases()).doesNotContain(nonExistentDb);
    }

    @Test
    public void testRepairCatalog() throws Exception {
        // Create multiple databases with tables
        String[] databases = {"repair_db1", "repair_db2", "repair_db3"};

        Schema schema =
                new Schema(
                        Lists.newArrayList(
                                new DataField(0, "id", DataTypes.INT()),
                                new DataField(1, "data", DataTypes.STRING())),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        "");

        for (String dbName : databases) {
            catalog.createDatabase(dbName, false);
            catalog.createTable(Identifier.create(dbName, "test_table"), schema, false);
        }

        // Test repair entire catalog - should not throw exception
        assertDoesNotThrow(() -> catalog.repairCatalog());

        // Verify all databases and tables still exist
        List<String> catalogDatabases = catalog.listDatabases();
        for (String dbName : databases) {
            assertThat(catalogDatabases).contains(dbName);
            assertThat(catalog.listTables(dbName)).contains("test_table");
        }
    }

    @Test
    public void testInsertTableUtility() throws Exception {
        String databaseName = "insert_test_db";
        String tableName = "insert_test_table";

        catalog.createDatabase(databaseName, false);

        JdbcCatalog jdbcCatalog = (JdbcCatalog) catalog;

        // Test insertTable utility method
        boolean result =
                JdbcUtils.insertTable(
                        jdbcCatalog.getConnections(),
                        jdbcCatalog.getCatalogKey(),
                        databaseName,
                        tableName);

        assertThat(result).isTrue();

        // Try inserting the same table again - should throw exception for duplicate
        assertThatThrownBy(
                        () ->
                                JdbcUtils.insertTable(
                                        jdbcCatalog.getConnections(),
                                        jdbcCatalog.getCatalogKey(),
                                        databaseName,
                                        tableName))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to insert table");
    }
}
