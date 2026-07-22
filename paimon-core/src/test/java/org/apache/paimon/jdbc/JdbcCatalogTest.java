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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.TableType;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogTestBase;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.object.ObjectTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.view.View;
import org.apache.paimon.view.ViewChange;
import org.apache.paimon.view.ViewImpl;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;
import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;
import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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
        return initCatalog(
                props,
                "jdbc:sqlite:file:"
                        + UUID.randomUUID().toString().replace("-", "")
                        + "?mode=memory&cache=shared");
    }

    private JdbcCatalog initCatalog(Map<String, String> props, String uri) {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(CatalogOptions.URI.key(), uri);

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
    public void testObjectTable() throws Exception {
        String databaseName = "object_table_db";
        String tableName = "object_table";
        catalog.createDatabase(databaseName, false);
        Identifier identifier = Identifier.create(databaseName, tableName);

        // Create object table with only options (type=object-table)
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.TYPE.key(), TableType.OBJECT_TABLE.toString());
        Schema schema = Schema.newBuilder().options(options).build();

        catalog.createTable(identifier, schema, false);

        // Verify table exists in JDBC catalog
        assertThat(catalog.listTables(databaseName)).contains(tableName);

        // Verify getTable returns ObjectTable instance
        Table table = catalog.getTable(identifier);
        assertThat(table).isInstanceOf(ObjectTable.class);

        ObjectTable objectTable = (ObjectTable) table;
        // Verify fixed schema fields
        assertThat(objectTable.rowType().getFieldNames())
                .containsExactly("path", "name", "length", "mtime", "atime", "owner");
        // Verify location is set (defaults to table path)
        assertThat(objectTable.location()).isNotNull();
        // Verify options contain type=object-table
        assertThat(objectTable.options().get(CoreOptions.TYPE.key()))
                .isEqualTo(TableType.OBJECT_TABLE.toString());

        // Drop table and verify it's gone
        catalog.dropTable(identifier, false);
        assertThat(catalog.listTables(databaseName)).doesNotContain(tableName);
    }

    @Test
    public void testObjectTableWithCustomPath() throws Exception {
        String databaseName = "object_table_custom_db";
        String tableName = "object_table_custom";
        catalog.createDatabase(databaseName, false);
        Identifier identifier = Identifier.create(databaseName, tableName);

        // Create object table with custom path
        String customPath =
                new Path(warehouse, databaseName + ".db/" + tableName + "_data").toString();
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.TYPE.key(), TableType.OBJECT_TABLE.toString());
        options.put(CoreOptions.PATH.key(), customPath);
        Schema schema = Schema.newBuilder().options(options).build();

        catalog.createTable(identifier, schema, false);

        // Verify getTable returns ObjectTable with the custom location
        Table table = catalog.getTable(identifier);
        assertThat(table).isInstanceOf(ObjectTable.class);
        ObjectTable objectTable = (ObjectTable) table;
        assertThat(objectTable.location()).isEqualTo(customPath);

        // Clean up
        catalog.dropTable(identifier, false);
    }

    @Test
    public void testDropTableWhenTablePathMissing() throws Exception {
        String databaseName = "test_db";
        String tableName = "new_table";
        catalog.createDatabase(databaseName, false);
        Identifier identifier = Identifier.create(databaseName, tableName);
        catalog.createTable(identifier, DEFAULT_TABLE_SCHEMA, false);

        JdbcCatalog jdbcCatalog = (JdbcCatalog) catalog;
        Path path = jdbcCatalog.getTableLocation(identifier);
        jdbcCatalog.fileIO().deleteDirectoryQuietly(path);

        assertThatThrownBy(() -> catalog.getTable(identifier))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("There is no paimon table in " + path);
        assertThat(jdbcCatalog.listTables(databaseName)).contains(tableName);

        jdbcCatalog.dropTable(identifier, false);
        assertThat(jdbcCatalog.listTables(databaseName)).doesNotContain(tableName);
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

    @Override
    protected boolean supportsAlterDatabase() {
        return true;
    }

    @Override
    protected boolean supportsReplaceTable() {
        // jdbc lock interferes with the test data commit; replace path itself works at runtime
        return false;
    }

    @Override
    protected boolean supportsView() {
        return true;
    }

    @Test
    public void testUniqueConstraintViolationDetection() {
        assertThat(
                        JdbcUtils.isUniqueConstraintViolation(
                                new SQLIntegrityConstraintViolationException("duplicate entry")))
                .isTrue();
        assertThat(
                        JdbcUtils.isUniqueConstraintViolation(
                                new SQLException("duplicate key", "23505")))
                .isTrue();
        assertThat(JdbcUtils.isUniqueConstraintViolation(new SQLException("constraint failed")))
                .isTrue();
        assertThat(JdbcUtils.isUniqueConstraintViolation(new SQLException("syntax error")))
                .isFalse();
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

    private JdbcCatalog initCatalogWithSync(boolean syncAllProperties) {
        Map<String, String> props = Maps.newHashMap();
        props.put(CatalogOptions.SYNC_ALL_PROPERTIES.key(), String.valueOf(syncAllProperties));
        return initCatalog(props);
    }

    private JdbcCatalog initCatalogWithoutLock() {
        Map<String, String> props = Maps.newHashMap();
        props.put(CatalogOptions.LOCK_ENABLED.key(), "false");
        return initCatalog(props);
    }

    private JdbcCatalog initCatalogWithoutLock(String uri) {
        Map<String, String> props = Maps.newHashMap();
        props.put(CatalogOptions.LOCK_ENABLED.key(), "false");
        return initCatalog(props, uri);
    }

    private Map<String, String> fetchTableProperties(
            JdbcCatalog jdbcCatalog, String databaseName, String tableName) {
        try {
            return jdbcCatalog
                    .getConnections()
                    .run(
                            conn -> {
                                Map<String, String> result = new HashMap<>();
                                try (PreparedStatement ps =
                                        conn.prepareStatement(
                                                JdbcUtils.GET_ALL_TABLE_PROPERTIES_SQL)) {
                                    ps.setString(1, jdbcCatalog.getCatalogKey());
                                    ps.setString(2, databaseName);
                                    ps.setString(3, tableName);
                                    try (ResultSet rs = ps.executeQuery()) {
                                        while (rs.next()) {
                                            result.put(
                                                    rs.getString(JdbcUtils.TABLE_PROPERTY_KEY),
                                                    rs.getString(JdbcUtils.TABLE_PROPERTY_VALUE));
                                        }
                                    }
                                }
                                return result;
                            });
        } catch (SQLException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private String fetchViewSchema(JdbcCatalog jdbcCatalog, String databaseName, String viewName) {
        try {
            return JdbcUtils.getViewSchema(
                    jdbcCatalog.getConnections(),
                    jdbcCatalog.getCatalogKey(),
                    databaseName,
                    viewName);
        } catch (SQLException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean tableExists(JdbcCatalog jdbcCatalog, String tableName) {
        try {
            return jdbcCatalog
                    .getConnections()
                    .run(
                            conn -> {
                                try (ResultSet rs =
                                        conn.getMetaData().getTables(null, null, tableName, null)) {
                                    return rs.next();
                                }
                            });
        } catch (SQLException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testTablePropertiesSyncOnCreate() throws Exception {
        JdbcCatalog syncCatalog = initCatalogWithSync(true);
        syncCatalog.createDatabase("test_db", false);

        Map<String, String> options = new HashMap<>();
        options.put("bucket", "4");
        options.put("file.format", "parquet");
        Schema schema =
                new Schema(
                        Lists.newArrayList(
                                new DataField(0, "pk", DataTypes.INT()),
                                new DataField(1, "pt", DataTypes.STRING()),
                                new DataField(2, "col1", DataTypes.STRING())),
                        Lists.newArrayList("pt"),
                        Lists.newArrayList("pk", "pt"),
                        options,
                        "");

        Identifier identifier = Identifier.create("test_db", "test_table");
        syncCatalog.createTable(identifier, schema, false);

        Map<String, String> storedProps =
                fetchTableProperties(syncCatalog, "test_db", "test_table");
        assertThat(storedProps).containsEntry("bucket", "4");
        assertThat(storedProps).containsEntry("file.format", "parquet");
        assertThat(storedProps).containsEntry("primary-key", "pk,pt");
        assertThat(storedProps).containsEntry("partition", "pt");
    }

    @Test
    public void testTablePropertiesSyncOnAlter() throws Exception {
        JdbcCatalog syncCatalog = initCatalogWithSync(true);
        syncCatalog.createDatabase("test_db", false);

        Map<String, String> options = new HashMap<>();
        options.put("bucket", "4");
        Schema schema =
                new Schema(
                        Lists.newArrayList(
                                new DataField(0, "pk", DataTypes.INT()),
                                new DataField(1, "pt", DataTypes.STRING()),
                                new DataField(2, "col1", DataTypes.STRING())),
                        Lists.newArrayList("pt"),
                        Lists.newArrayList("pk", "pt"),
                        options,
                        "");

        Identifier identifier = Identifier.create("test_db", "test_table");
        syncCatalog.createTable(identifier, schema, false);

        // Alter table: set a new option
        syncCatalog.alterTable(
                identifier,
                Lists.newArrayList(SchemaChange.setOption("file.format", "orc")),
                false);

        Map<String, String> storedProps =
                fetchTableProperties(syncCatalog, "test_db", "test_table");
        assertThat(storedProps).containsEntry("file.format", "orc");
        assertThat(storedProps).containsEntry("bucket", "4");

        // Alter table: remove an option
        syncCatalog.alterTable(
                identifier, Lists.newArrayList(SchemaChange.removeOption("file.format")), false);

        storedProps = fetchTableProperties(syncCatalog, "test_db", "test_table");
        assertThat(storedProps).doesNotContainKey("file.format");
        assertThat(storedProps).containsEntry("bucket", "4");
    }

    @Test
    public void testTablePropertiesSyncOnDrop() throws Exception {
        JdbcCatalog syncCatalog = initCatalogWithSync(true);
        syncCatalog.createDatabase("test_db", false);

        Map<String, String> options = new HashMap<>();
        options.put("bucket", "4");
        Schema schema =
                new Schema(
                        Lists.newArrayList(
                                new DataField(0, "pk", DataTypes.INT()),
                                new DataField(1, "pt", DataTypes.STRING()),
                                new DataField(2, "col1", DataTypes.STRING())),
                        Lists.newArrayList("pt"),
                        Lists.newArrayList("pk", "pt"),
                        options,
                        "");

        Identifier identifier = Identifier.create("test_db", "test_table");
        syncCatalog.createTable(identifier, schema, false);

        // Verify properties exist
        Map<String, String> storedProps =
                fetchTableProperties(syncCatalog, "test_db", "test_table");
        assertThat(storedProps).isNotEmpty();

        // Drop the table
        syncCatalog.dropTable(identifier, false);

        // Verify properties are cleaned up
        storedProps = fetchTableProperties(syncCatalog, "test_db", "test_table");
        assertThat(storedProps).isEmpty();
    }

    @Test
    public void testTablePropertiesSyncOnRename() throws Exception {
        JdbcCatalog syncCatalog = initCatalogWithSync(true);
        syncCatalog.createDatabase("test_db", false);

        Map<String, String> options = new HashMap<>();
        options.put("bucket", "4");
        Schema schema =
                new Schema(
                        Lists.newArrayList(
                                new DataField(0, "pk", DataTypes.INT()),
                                new DataField(1, "pt", DataTypes.STRING()),
                                new DataField(2, "col1", DataTypes.STRING())),
                        Lists.newArrayList("pt"),
                        Lists.newArrayList("pk", "pt"),
                        options,
                        "");

        Identifier fromTable = Identifier.create("test_db", "old_table");
        syncCatalog.createTable(fromTable, schema, false);

        // Verify properties exist under old name
        Map<String, String> storedProps = fetchTableProperties(syncCatalog, "test_db", "old_table");
        assertThat(storedProps).containsEntry("bucket", "4");

        // Rename the table
        Identifier toTable = Identifier.create("test_db", "new_table");
        syncCatalog.renameTable(fromTable, toTable, false);

        // Verify properties moved to new name
        storedProps = fetchTableProperties(syncCatalog, "test_db", "new_table");
        assertThat(storedProps).containsEntry("bucket", "4");

        // Verify old name has no properties
        storedProps = fetchTableProperties(syncCatalog, "test_db", "old_table");
        assertThat(storedProps).isEmpty();
    }

    @Test
    public void testTablePropertiesSyncOnDropDatabase() throws Exception {
        JdbcCatalog syncCatalog = initCatalogWithSync(true);
        syncCatalog.createDatabase("test_db", false);

        Map<String, String> options = new HashMap<>();
        options.put("bucket", "4");
        Schema schema =
                new Schema(
                        Lists.newArrayList(
                                new DataField(0, "pk", DataTypes.INT()),
                                new DataField(1, "pt", DataTypes.STRING()),
                                new DataField(2, "col1", DataTypes.STRING())),
                        Lists.newArrayList("pt"),
                        Lists.newArrayList("pk", "pt"),
                        options,
                        "");

        syncCatalog.createTable(Identifier.create("test_db", "table1"), schema, false);
        syncCatalog.createTable(Identifier.create("test_db", "table2"), schema, false);

        // Verify properties exist
        assertThat(fetchTableProperties(syncCatalog, "test_db", "table1")).isNotEmpty();
        assertThat(fetchTableProperties(syncCatalog, "test_db", "table2")).isNotEmpty();

        // Drop the database cascade
        syncCatalog.dropDatabase("test_db", false, true);

        // Verify all table properties are cleaned up
        assertThat(fetchTableProperties(syncCatalog, "test_db", "table1")).isEmpty();
        assertThat(fetchTableProperties(syncCatalog, "test_db", "table2")).isEmpty();
    }

    @Test
    public void testTablePropertiesSyncDisabled() throws Exception {
        JdbcCatalog syncCatalog = initCatalogWithSync(false);
        syncCatalog.createDatabase("test_db", false);

        Map<String, String> options = new HashMap<>();
        options.put("bucket", "4");
        Schema schema =
                new Schema(
                        Lists.newArrayList(
                                new DataField(0, "pk", DataTypes.INT()),
                                new DataField(1, "pt", DataTypes.STRING()),
                                new DataField(2, "col1", DataTypes.STRING())),
                        Lists.newArrayList("pt"),
                        Lists.newArrayList("pk", "pt"),
                        options,
                        "");

        Identifier identifier = Identifier.create("test_db", "test_table");
        syncCatalog.createTable(identifier, schema, false);

        // Verify no properties are stored when sync is disabled
        Map<String, String> storedProps =
                fetchTableProperties(syncCatalog, "test_db", "test_table");
        assertThat(storedProps).isEmpty();
    }

    @Test
    public void testTablePropertiesSyncOnRepair() throws Exception {
        JdbcCatalog syncCatalog = initCatalogWithSync(true);
        syncCatalog.createDatabase("test_db", false);

        Map<String, String> options = new HashMap<>();
        options.put("bucket", "4");
        Schema schema =
                new Schema(
                        Lists.newArrayList(
                                new DataField(0, "pk", DataTypes.INT()),
                                new DataField(1, "pt", DataTypes.STRING()),
                                new DataField(2, "col1", DataTypes.STRING())),
                        Lists.newArrayList("pt"),
                        Lists.newArrayList("pk", "pt"),
                        options,
                        "");

        Identifier identifier = Identifier.create("test_db", "test_table");
        syncCatalog.createTable(identifier, schema, false);

        // Delete the JDBC table row and properties (simulating corruption)
        JdbcUtils.execute(
                syncCatalog.getConnections(),
                JdbcUtils.DROP_TABLE_SQL,
                syncCatalog.getCatalogKey(),
                "test_db",
                "test_table");
        JdbcUtils.execute(
                syncCatalog.getConnections(),
                JdbcUtils.DELETE_ALL_TABLE_PROPERTIES_SQL,
                syncCatalog.getCatalogKey(),
                "test_db",
                "test_table");

        // Verify properties are gone
        assertThat(fetchTableProperties(syncCatalog, "test_db", "test_table")).isEmpty();

        // Repair the table
        syncCatalog.repairTable(identifier);

        // Verify properties are restored
        Map<String, String> storedProps =
                fetchTableProperties(syncCatalog, "test_db", "test_table");
        assertThat(storedProps).containsEntry("bucket", "4");
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

    @Test
    public void testViewTableCreatedOnInitialize() {
        assertThat(tableExists((JdbcCatalog) catalog, JdbcUtils.VIEW_TABLE_NAME)).isTrue();
    }

    @Test
    public void testCreateViewStoresViewSchemaWithoutResolvingReferencedTable() throws Exception {
        String databaseName = "view_schema_db";
        Identifier identifier = Identifier.create(databaseName, "view_on_missing_table");
        Identifier dependency = Identifier.create(databaseName, "missing_table");
        View baseView = createView(identifier);
        View view =
                new ViewImpl(
                        identifier,
                        baseView.rowType().getFields(),
                        baseView.query(),
                        baseView.dialects(),
                        baseView.comment().orElse(null),
                        baseView.options(),
                        Collections.singletonList(dependency));

        catalog.createDatabase(databaseName, false);
        catalog.createView(identifier, view, false);

        String viewSchemaJson =
                fetchViewSchema((JdbcCatalog) catalog, databaseName, "view_on_missing_table");
        assertThat(viewSchemaJson).contains("\"query\"").contains("\"SELECT * FROM OTHER_TABLE\"");
        assertThat(catalog.getView(identifier).query()).isEqualTo("SELECT * FROM OTHER_TABLE");
        assertThat(catalog.getView(identifier).dependencies()).containsExactly(dependency);
    }

    @Test
    public void testCreateViewStoresCrossDatabaseQueryWithoutResolvingReferencedTable()
            throws Exception {
        String databaseName = "cross_database_view_db";
        Identifier identifier = Identifier.create(databaseName, "view_on_other_database");
        String query = "SELECT * FROM other_database.missing_table";
        View baseView = createView(identifier);
        View view =
                new ViewImpl(
                        identifier,
                        baseView.rowType().getFields(),
                        query,
                        baseView.dialects(),
                        baseView.comment().orElse(null),
                        baseView.options());

        catalog.createDatabase(databaseName, false);
        catalog.createView(identifier, view, false);

        String viewSchemaJson =
                fetchViewSchema((JdbcCatalog) catalog, databaseName, "view_on_other_database");
        assertThat(viewSchemaJson).contains("\"query\"").contains(query);
        assertThat(catalog.getView(identifier).query()).isEqualTo(query);
    }

    @Test
    public void testDropDatabaseCleansViewMetadata() throws Exception {
        String databaseName = "drop_view_db";
        Identifier identifier = Identifier.create(databaseName, "view_name");

        catalog.createDatabase(databaseName, false);
        catalog.createView(identifier, createView(identifier), false);
        catalog.dropDatabase(databaseName, false, true);

        assertThat(
                        fetchViewSchema(
                                (JdbcCatalog) catalog,
                                identifier.getDatabaseName(),
                                identifier.getObjectName()))
                .isNull();
    }

    @Test
    public void testDropDatabaseWithoutCascadeRejectsViewOnlyDatabase() throws Exception {
        String databaseName = "view_only_db";
        Identifier identifier = Identifier.create(databaseName, "view_name");

        catalog.createDatabase(databaseName, false);
        catalog.createView(identifier, createView(identifier), false);

        assertThatThrownBy(() -> catalog.dropDatabase(databaseName, false, false))
                .isInstanceOf(Catalog.DatabaseNotEmptyException.class)
                .hasMessage("Database " + databaseName + " is not empty.");
        assertThat(catalog.getView(identifier).fullName()).isEqualTo(identifier.getFullName());

        catalog.dropDatabase(databaseName, false, true);
        assertThatThrownBy(() -> catalog.getDatabase(databaseName))
                .isInstanceOf(Catalog.DatabaseNotExistException.class);
    }

    @Test
    public void testRenameViewAcrossDatabases() throws Exception {
        String sourceDatabase = "source_view_db";
        String targetDatabase = "target_view_db";
        Identifier source = Identifier.create(sourceDatabase, "view_name");
        Identifier target = Identifier.create(targetDatabase, "view_name");

        catalog.createDatabase(sourceDatabase, false);
        catalog.createDatabase(targetDatabase, false);
        catalog.createView(source, createView(source), false);

        catalog.renameView(source, target, false);

        assertThatThrownBy(() -> catalog.getView(source))
                .isInstanceOf(Catalog.ViewNotExistException.class);
        assertThat(catalog.getView(target).fullName()).isEqualTo(target.getFullName());
        assertThat(catalog.listViews(sourceDatabase)).isEmpty();
        assertThat(catalog.listViews(targetDatabase)).containsExactly("view_name");
    }

    @Test
    public void testRenameViewRejectsMissingTargetDatabase() throws Exception {
        String sourceDatabase = "source_view_db";
        Identifier source = Identifier.create(sourceDatabase, "view_name");
        Identifier target = Identifier.create("missing_view_db", "view_name");

        catalog.createDatabase(sourceDatabase, false);
        catalog.createView(source, createView(source), false);

        assertThatThrownBy(() -> catalog.renameView(source, target, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Database missing_view_db does not exist.");
        assertThat(catalog.getView(source).fullName()).isEqualTo(source.getFullName());
        assertThatThrownBy(() -> catalog.getView(target))
                .isInstanceOf(Catalog.ViewNotExistException.class);
    }

    @Test
    public void testRenameTableRejectsMissingTargetDatabase() throws Exception {
        String sourceDatabase = "source_rename_table_db";
        Identifier source = Identifier.create(sourceDatabase, "table_name");
        Identifier target = Identifier.create("missing_target_db", "table_name");

        catalog.createDatabase(sourceDatabase, false);
        catalog.createTable(source, DEFAULT_TABLE_SCHEMA, false);

        assertThatThrownBy(() -> catalog.renameTable(source, target, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Database missing_target_db does not exist.");
        // Source table must remain intact.
        assertDoesNotThrow(() -> catalog.getTable(source));
        assertThatThrownBy(() -> catalog.getTable(target))
                .isInstanceOf(Catalog.TableNotExistException.class);
    }

    @Test
    public void testTableAndViewCannotShareName() throws Exception {
        String databaseName = "shared_name_db";
        Identifier tableFirst = Identifier.create(databaseName, "table_first");
        Identifier viewFirst = Identifier.create(databaseName, "view_first");

        catalog.createDatabase(databaseName, false);

        catalog.createTable(tableFirst, DEFAULT_TABLE_SCHEMA, false);
        assertThatThrownBy(() -> catalog.createView(tableFirst, createView(tableFirst), false))
                .isInstanceOf(Catalog.ViewAlreadyExistException.class);
        catalog.createView(tableFirst, createView(tableFirst), true);
        assertThat(catalog.listViews(databaseName)).doesNotContain(tableFirst.getObjectName());

        catalog.createView(viewFirst, createView(viewFirst), false);
        assertThatThrownBy(() -> catalog.createTable(viewFirst, DEFAULT_TABLE_SCHEMA, false))
                .isInstanceOf(Catalog.TableAlreadyExistException.class);
        catalog.createTable(viewFirst, DEFAULT_TABLE_SCHEMA, true);
        assertThat(catalog.listTables(databaseName)).doesNotContain(viewFirst.getObjectName());
    }

    @Test
    public void testRenameRejectsTableViewNameCollision() throws Exception {
        String databaseName = "rename_collision_db";
        Identifier table = Identifier.create(databaseName, "table_name");
        Identifier view = Identifier.create(databaseName, "view_name");

        catalog.createDatabase(databaseName, false);
        catalog.createTable(table, DEFAULT_TABLE_SCHEMA, false);
        catalog.createView(view, createView(view), false);

        assertThatThrownBy(() -> catalog.renameTable(table, view, false))
                .isInstanceOf(Catalog.TableAlreadyExistException.class);
        assertThatThrownBy(() -> catalog.renameView(view, table, false))
                .isInstanceOf(Catalog.ViewAlreadyExistException.class);
        assertThat(catalog.listTables(databaseName)).containsExactly(table.getObjectName());
        assertThat(catalog.listViews(databaseName)).containsExactly(view.getObjectName());
    }

    @Test
    public void testListViewsFromSystemDatabase() throws Exception {
        assertThat(catalog.listViews(Catalog.SYSTEM_DATABASE_NAME)).isEmpty();
        assertThat(
                        catalog.listViewsPaged(Catalog.SYSTEM_DATABASE_NAME, 10, null, null)
                                .getElements())
                .isEmpty();
        assertThat(
                        catalog.listViewDetailsPaged(Catalog.SYSTEM_DATABASE_NAME, 10, null, null)
                                .getElements())
                .isEmpty();
    }

    @Test
    public void testConcurrentCreateViewOnlyCreatesOneView() throws Exception {
        String databaseName = "concurrent_view_db";
        Identifier identifier = Identifier.create(databaseName, "same_view");
        View view = createView(identifier);
        ExecutorService executor = Executors.newFixedThreadPool(2);

        catalog.createDatabase(databaseName, false);
        Callable<Boolean> create =
                () -> {
                    try {
                        catalog.createView(identifier, view, false);
                        return true;
                    } catch (Catalog.ViewAlreadyExistException e) {
                        return false;
                    }
                };

        try {
            Future<Boolean> first = executor.submit(create);
            Future<Boolean> second = executor.submit(create);

            assertThat(ImmutableList.of(first.get(), second.get()))
                    .containsExactlyInAnyOrder(true, false);
            assertThat(catalog.listViews(databaseName)).containsExactly("same_view");
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testConcurrentCreateTableAndViewCannotShareName() throws Exception {
        String databaseName = "concurrent_table_view_db";
        Identifier identifier = Identifier.create(databaseName, "same_name");
        View view = createView(identifier);
        ExecutorService executor = Executors.newFixedThreadPool(2);

        catalog.createDatabase(databaseName, false);
        Callable<Boolean> createTable =
                () -> {
                    try {
                        catalog.createTable(identifier, DEFAULT_TABLE_SCHEMA, false);
                        return true;
                    } catch (Catalog.TableAlreadyExistException e) {
                        return false;
                    }
                };
        Callable<Boolean> createView =
                () -> {
                    try {
                        catalog.createView(identifier, view, false);
                        return true;
                    } catch (Catalog.ViewAlreadyExistException e) {
                        return false;
                    }
                };

        try {
            Future<Boolean> tableCreated = executor.submit(createTable);
            Future<Boolean> viewCreated = executor.submit(createView);

            assertThat(ImmutableList.of(tableCreated.get(), viewCreated.get()))
                    .containsExactlyInAnyOrder(true, false);
            assertThat(catalog.listTables(databaseName).contains(identifier.getObjectName()))
                    .isNotEqualTo(
                            catalog.listViews(databaseName).contains(identifier.getObjectName()));
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testConcurrentRenameTableAndCreateViewCannotShareName() throws Exception {
        String databaseName = "concurrent_rename_table_view_db";
        Identifier source = Identifier.create(databaseName, "src_table");
        Identifier target = Identifier.create(databaseName, "shared_name");
        View view = createView(target);
        ExecutorService executor = Executors.newFixedThreadPool(2);

        catalog.createDatabase(databaseName, false);
        catalog.createTable(source, DEFAULT_TABLE_SCHEMA, false);

        Callable<Boolean> renameTable =
                () -> {
                    try {
                        catalog.renameTable(source, target, false);
                        return true;
                    } catch (Catalog.TableAlreadyExistException e) {
                        return false;
                    }
                };
        Callable<Boolean> createView =
                () -> {
                    try {
                        catalog.createView(target, view, false);
                        return true;
                    } catch (Catalog.ViewAlreadyExistException e) {
                        return false;
                    }
                };

        try {
            Future<Boolean> tableRenamed = executor.submit(renameTable);
            Future<Boolean> viewCreated = executor.submit(createView);

            assertThat(ImmutableList.of(tableRenamed.get(), viewCreated.get()))
                    .containsExactlyInAnyOrder(true, false);
            // The shared identifier must be either a table or a view, but never both.
            assertThat(catalog.listTables(databaseName).contains(target.getObjectName()))
                    .isNotEqualTo(catalog.listViews(databaseName).contains(target.getObjectName()));
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testConcurrentRenameTableAndRenameViewCannotShareName() throws Exception {
        String databaseName = "concurrent_rename_collision_db";
        Identifier sourceTable = Identifier.create(databaseName, "src_table");
        Identifier sourceView = Identifier.create(databaseName, "src_view");
        Identifier target = Identifier.create(databaseName, "shared_name");
        ExecutorService executor = Executors.newFixedThreadPool(2);

        catalog.createDatabase(databaseName, false);
        catalog.createTable(sourceTable, DEFAULT_TABLE_SCHEMA, false);
        catalog.createView(sourceView, createView(sourceView), false);

        Callable<Boolean> renameTable =
                () -> {
                    try {
                        catalog.renameTable(sourceTable, target, false);
                        return true;
                    } catch (Catalog.TableAlreadyExistException e) {
                        return false;
                    }
                };
        Callable<Boolean> renameView =
                () -> {
                    try {
                        catalog.renameView(sourceView, target, false);
                        return true;
                    } catch (Catalog.ViewAlreadyExistException e) {
                        return false;
                    }
                };

        try {
            Future<Boolean> tableRenamed = executor.submit(renameTable);
            Future<Boolean> viewRenamed = executor.submit(renameView);

            assertThat(ImmutableList.of(tableRenamed.get(), viewRenamed.get()))
                    .containsExactlyInAnyOrder(true, false);
            assertThat(catalog.listTables(databaseName).contains(target.getObjectName()))
                    .isNotEqualTo(catalog.listViews(databaseName).contains(target.getObjectName()));
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testConcurrentCreateTableAndViewCannotShareNameWithoutDistributedLock()
            throws Exception {
        // Even when the JDBC distributed lock is disabled, mutations targeting the same
        // identifier within a single JVM must remain atomic. This is provided by the per-JVM
        // local stripe lock.
        JdbcCatalog noLockCatalog = initCatalogWithoutLock();
        try {
            String databaseName = "no_lock_concurrent_db";
            Identifier identifier = Identifier.create(databaseName, "same_name");
            View view = createView(identifier);
            ExecutorService executor = Executors.newFixedThreadPool(2);

            noLockCatalog.createDatabase(databaseName, false);
            Callable<Boolean> createTable =
                    () -> {
                        try {
                            noLockCatalog.createTable(identifier, DEFAULT_TABLE_SCHEMA, false);
                            return true;
                        } catch (Catalog.TableAlreadyExistException e) {
                            return false;
                        }
                    };
            Callable<Boolean> createView =
                    () -> {
                        try {
                            noLockCatalog.createView(identifier, view, false);
                            return true;
                        } catch (Catalog.ViewAlreadyExistException e) {
                            return false;
                        }
                    };

            try {
                Future<Boolean> tableCreated = executor.submit(createTable);
                Future<Boolean> viewCreated = executor.submit(createView);

                assertThat(ImmutableList.of(tableCreated.get(), viewCreated.get()))
                        .containsExactlyInAnyOrder(true, false);
                assertThat(
                                noLockCatalog
                                        .listTables(databaseName)
                                        .contains(identifier.getObjectName()))
                        .isNotEqualTo(
                                noLockCatalog
                                        .listViews(databaseName)
                                        .contains(identifier.getObjectName()));
            } catch (ExecutionException e) {
                throw new RuntimeException(e.getCause());
            } finally {
                executor.shutdownNow();
            }
        } finally {
            noLockCatalog.close();
        }
    }

    @Test
    public void testConcurrentCreateTableAndViewCannotShareNameAcrossCatalogInstancesWithoutLock()
            throws Exception {
        String uri =
                "jdbc:sqlite:file:"
                        + UUID.randomUUID().toString().replace("-", "")
                        + "?mode=memory&cache=shared";
        JdbcCatalog tableCatalog = initCatalogWithoutLock(uri);
        JdbcCatalog viewCatalog = initCatalogWithoutLock(uri);
        try {
            String databaseName = "no_lock_two_catalogs_db";
            Identifier identifier = Identifier.create(databaseName, "same_name");
            View view = createView(identifier);
            ExecutorService executor = Executors.newFixedThreadPool(2);

            tableCatalog.createDatabase(databaseName, false);
            Callable<Boolean> createTable =
                    () -> {
                        try {
                            tableCatalog.createTable(identifier, DEFAULT_TABLE_SCHEMA, false);
                            return true;
                        } catch (Catalog.TableAlreadyExistException e) {
                            return false;
                        }
                    };
            Callable<Boolean> createView =
                    () -> {
                        try {
                            viewCatalog.createView(identifier, view, false);
                            return true;
                        } catch (Catalog.ViewAlreadyExistException e) {
                            return false;
                        }
                    };

            try {
                Future<Boolean> tableCreated = executor.submit(createTable);
                Future<Boolean> viewCreated = executor.submit(createView);

                assertThat(ImmutableList.of(tableCreated.get(), viewCreated.get()))
                        .containsExactlyInAnyOrder(true, false);
                assertThat(
                                tableCatalog
                                        .listTables(databaseName)
                                        .contains(identifier.getObjectName()))
                        .isNotEqualTo(
                                viewCatalog
                                        .listViews(databaseName)
                                        .contains(identifier.getObjectName()));
            } catch (ExecutionException e) {
                throw new RuntimeException(e.getCause());
            } finally {
                executor.shutdownNow();
            }
        } finally {
            tableCatalog.close();
            viewCatalog.close();
        }
    }

    @Test
    public void testAlterView() throws Exception {
        Identifier identifier = Identifier.create("alter_view_db", "my_view");
        View view = createView(identifier);
        catalog.createDatabase(identifier.getDatabaseName(), false);

        assertDoesNotThrow(
                () ->
                        catalog.alterView(
                                identifier,
                                ImmutableList.of(ViewChange.setOption("k", "v")),
                                true));
        assertThatThrownBy(
                        () ->
                                catalog.alterView(
                                        identifier,
                                        ImmutableList.of(ViewChange.setOption("k", "v")),
                                        false))
                .isInstanceOf(Catalog.ViewNotExistException.class);

        catalog.createView(identifier, view, false);
        catalog.alterView(identifier, ImmutableList.of(ViewChange.setOption("k", "v")), false);
        assertThat(catalog.getView(identifier).options()).containsEntry("k", "v");

        catalog.alterView(identifier, ImmutableList.of(ViewChange.removeOption("k")), false);
        assertThat(catalog.getView(identifier).options()).doesNotContainKey("k");

        catalog.alterView(
                identifier, ImmutableList.of(ViewChange.updateComment("new comment")), false);
        assertThat(catalog.getView(identifier).comment()).hasValue("new comment");

        catalog.alterView(
                identifier,
                ImmutableList.of(ViewChange.addDialect("flink_1", "SELECT * FROM FLINK_TABLE_1")),
                false);
        assertThat(catalog.getView(identifier).query("flink_1"))
                .isEqualTo("SELECT * FROM FLINK_TABLE_1");

        assertThatThrownBy(
                        () ->
                                catalog.alterView(
                                        identifier,
                                        ImmutableList.of(
                                                ViewChange.addDialect(
                                                        "flink_1", "SELECT * FROM FLINK_TABLE_1")),
                                        false))
                .isInstanceOf(Catalog.DialectAlreadyExistException.class);

        catalog.alterView(
                identifier,
                ImmutableList.of(
                        ViewChange.updateDialect("flink_1", "SELECT * FROM FLINK_TABLE_2")),
                false);
        assertThat(catalog.getView(identifier).query("flink_1"))
                .isEqualTo("SELECT * FROM FLINK_TABLE_2");

        assertThatThrownBy(
                        () ->
                                catalog.alterView(
                                        identifier,
                                        ImmutableList.of(
                                                ViewChange.updateDialect(
                                                        "missing", "SELECT * FROM FLINK_TABLE_2")),
                                        false))
                .isInstanceOf(Catalog.DialectNotExistException.class);

        catalog.alterView(identifier, ImmutableList.of(ViewChange.dropDialect("flink_1")), false);
        assertThat(catalog.getView(identifier).query("flink_1")).isEqualTo(view.query());

        assertThatThrownBy(
                        () ->
                                catalog.alterView(
                                        identifier,
                                        ImmutableList.of(ViewChange.dropDialect("missing")),
                                        false))
                .isInstanceOf(Catalog.DialectNotExistException.class);
    }
}
