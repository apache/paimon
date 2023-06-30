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

package org.apache.paimon.flink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.AbstractCatalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link FlinkCatalog}. */
public class FlinkCatalogTest {

    private final ObjectPath path1 = new ObjectPath("db1", "t1");
    private final ObjectPath path3 = new ObjectPath("db1", "t2");
    private final ObjectPath nonExistDbPath = ObjectPath.fromString("non.exist");
    private final ObjectPath nonExistObjectPath = ObjectPath.fromString("db1.nonexist");
    private Catalog catalog;

    @TempDir public static java.nio.file.Path temporaryFolder;

    @BeforeEach
    public void beforeEach() throws IOException {
        String path = new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toString();
        Options conf = new Options();
        conf.setString("warehouse", path);
        catalog =
                FlinkCatalogFactory.createCatalog(
                        "test-catalog",
                        CatalogContext.create(conf),
                        FlinkCatalogTest.class.getClassLoader());
    }

    private ResolvedSchema createSchema() {
        return new ResolvedSchema(
                Arrays.asList(
                        Column.physical("first", DataTypes.STRING()),
                        Column.physical("second", DataTypes.INT()),
                        Column.physical("third", DataTypes.STRING())),
                Collections.emptyList(),
                null);
    }

    private List<String> createPartitionKeys() {
        return Arrays.asList("second", "third");
    }

    private CatalogTable createAnotherTable(Map<String, String> options) {
        // TODO support change schema, modify it to createAnotherSchema
        ResolvedSchema resolvedSchema = this.createSchema();
        CatalogTable origin =
                CatalogTable.of(
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                        "test comment",
                        Collections.emptyList(),
                        options);
        return new ResolvedCatalogTable(origin, resolvedSchema);
    }

    private CatalogTable createAnotherPartitionedTable(Map<String, String> options) {
        // TODO support change schema, modify it to createAnotherSchema
        ResolvedSchema resolvedSchema = this.createSchema();
        CatalogTable origin =
                CatalogTable.of(
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                        "test comment",
                        this.createPartitionKeys(),
                        options);
        return new ResolvedCatalogTable(origin, resolvedSchema);
    }

    private CatalogTable createTable(Map<String, String> options) {
        ResolvedSchema resolvedSchema = this.createSchema();
        CatalogTable origin =
                CatalogTable.of(
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                        "test comment",
                        Collections.emptyList(),
                        options);
        return new ResolvedCatalogTable(origin, resolvedSchema);
    }

    private CatalogTable createPartitionedTable(Map<String, String> options) {
        ResolvedSchema resolvedSchema = this.createSchema();
        CatalogTable origin =
                CatalogTable.of(
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                        "test comment",
                        this.createPartitionKeys(),
                        options);
        return new ResolvedCatalogTable(origin, resolvedSchema);
    }

    @ParameterizedTest
    @MethodSource("batchOptionProvider")
    public void testAlterTable(Map<String, String> options) throws Exception {
        catalog.createDatabase(path1.getDatabaseName(), null, false);
        CatalogTable table = this.createTable(options);
        catalog.createTable(this.path1, table, false);
        checkEquals(path1, table, (CatalogTable) catalog.getTable(this.path1));
        CatalogTable newTable = this.createAnotherTable(options);
        catalog.alterTable(this.path1, newTable, false);
        assertNotEquals(table, catalog.getTable(this.path1));
        checkEquals(path1, newTable, (CatalogTable) catalog.getTable(this.path1));
        catalog.dropTable(this.path1, false);

        // Not support views
    }

    @ParameterizedTest
    @MethodSource("batchOptionProvider")
    public void testListTables(Map<String, String> options) throws Exception {
        catalog.createDatabase(path1.getDatabaseName(), null, false);
        catalog.createTable(this.path1, this.createTable(options), false);
        catalog.createTable(this.path3, this.createTable(options), false);
        assertEquals(2L, catalog.listTables("db1").size());

        // Not support views
    }

    @Test
    public void testAlterTable_differentTypedTable() {
        // TODO support this
    }

    @ParameterizedTest
    @MethodSource("batchOptionProvider")
    public void testCreateFlinkTable(Map<String, String> options) {
        // create a flink table
        CatalogTable table = createTable(options);
        HashMap<String, String> newOptions = new HashMap<>(table.getOptions());
        newOptions.put("connector", "filesystem");
        CatalogTable newTable = table.copy(newOptions);

        assertThatThrownBy(() -> catalog.createTable(this.path1, newTable, false))
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining(
                        "Paimon Catalog only supports paimon tables ,"
                                + " and you don't need to specify  'connector'= '"
                                + FlinkCatalogFactory.IDENTIFIER
                                + "' when using Paimon Catalog\n"
                                + " You can create TEMPORARY table instead if you want to create the table of other connector.");
    }

    @ParameterizedTest
    @MethodSource("streamingOptionProvider")
    public void testCreateTable_Streaming(Map<String, String> options) throws Exception {
        catalog.createDatabase(path1.getDatabaseName(), null, false);
        CatalogTable table = createTable(options);
        catalog.createTable(path1, table, false);
        checkEquals(path1, table, (CatalogTable) catalog.getTable(path1));
    }

    @ParameterizedTest
    @MethodSource("batchOptionProvider")
    public void testAlterPartitionedTable(Map<String, String> options) throws Exception {
        catalog.createDatabase(path1.getDatabaseName(), null, false);
        CatalogTable table = this.createPartitionedTable(options);
        catalog.createTable(this.path1, table, false);
        checkEquals(path1, table, (CatalogTable) catalog.getTable(this.path1));
        CatalogTable newTable = this.createAnotherPartitionedTable(options);
        catalog.alterTable(this.path1, newTable, false);
        checkEquals(path1, newTable, (CatalogTable) catalog.getTable(this.path1));
    }

    @ParameterizedTest
    @MethodSource("batchOptionProvider")
    public void testCreateTable_Batch(Map<String, String> options) throws Exception {
        catalog.createDatabase(path1.getDatabaseName(), null, false);
        CatalogTable table = this.createTable(options);
        catalog.createTable(this.path1, table, false);
        CatalogBaseTable tableCreated = catalog.getTable(this.path1);
        checkEquals(path1, table, (CatalogTable) tableCreated);
        assertEquals("test comment", tableCreated.getDescription().get());
        List<String> tables = catalog.listTables("db1");
        assertEquals(1L, tables.size());
        assertEquals(this.path1.getObjectName(), tables.get(0));
        catalog.dropTable(this.path1, false);
    }

    @ParameterizedTest
    @MethodSource("batchOptionProvider")
    public void testCreateTable_TableAlreadyExist_ignored(Map<String, String> options)
            throws Exception {
        catalog.createDatabase(path1.getDatabaseName(), null, false);
        CatalogTable table = this.createTable(options);
        catalog.createTable(this.path1, table, false);
        checkEquals(path1, table, (CatalogTable) catalog.getTable(this.path1));
        catalog.createTable(this.path1, this.createAnotherTable(options), true);
        checkEquals(path1, table, (CatalogTable) catalog.getTable(this.path1));
    }

    @ParameterizedTest
    @MethodSource("batchOptionProvider")
    public void testCreatePartitionedTable_Batch(Map<String, String> options) throws Exception {
        catalog.createDatabase(path1.getDatabaseName(), null, false);
        CatalogTable table = this.createPartitionedTable(options);
        catalog.createTable(this.path1, table, false);
        checkEquals(path1, table, (CatalogTable) catalog.getTable(this.path1));
        List<String> tables = catalog.listTables("db1");
        assertEquals(1L, tables.size());
        assertEquals(this.path1.getObjectName(), tables.get(0));
    }

    @ParameterizedTest
    @MethodSource("batchOptionProvider")
    public void testDropDb_DatabaseNotEmptyException(Map<String, String> options) throws Exception {
        catalog.createDatabase(path1.getDatabaseName(), null, false);
        catalog.createTable(this.path1, this.createTable(options), false);
        assertThatThrownBy(
                        () -> {
                            catalog.dropDatabase("db1", true, false);
                        })
                .isInstanceOf(DatabaseNotEmptyException.class)
                .hasMessage("Database db1 in catalog test-catalog is not empty.");
    }

    @ParameterizedTest
    @MethodSource("batchOptionProvider")
    public void testTableExists(Map<String, String> options) throws Exception {
        catalog.createDatabase(path1.getDatabaseName(), null, false);
        assertFalse(catalog.tableExists(this.path1));
        catalog.createTable(this.path1, this.createTable(options), false);
        assertTrue(catalog.tableExists(this.path1));

        // system tables
        assertTrue(
                catalog.tableExists(
                        new ObjectPath(
                                path1.getDatabaseName(), path1.getObjectName() + "$snapshots")));
        assertFalse(
                catalog.tableExists(
                        new ObjectPath(
                                path1.getDatabaseName(), path1.getObjectName() + "$unknown")));
    }

    @ParameterizedTest
    @MethodSource("batchOptionProvider")
    public void testAlterTable_TableNotExist_ignored(Map<String, String> options) throws Exception {
        catalog.alterTable(this.nonExistObjectPath, this.createTable(options), true);
        assertFalse(catalog.tableExists(this.nonExistObjectPath));
    }

    @Test
    public void testDropTable_TableNotExist_ignored() throws Exception {
        catalog.dropTable(this.nonExistObjectPath, true);
    }

    @ParameterizedTest
    @MethodSource("batchOptionProvider")
    public void testCreateTable_TableAlreadyExistException(Map<String, String> options)
            throws Exception {
        catalog.createDatabase(path1.getDatabaseName(), null, false);
        catalog.createTable(this.path1, this.createTable(options), false);
        assertThatThrownBy(() -> catalog.createTable(this.path1, this.createTable(options), false))
                .isInstanceOf(TableAlreadyExistException.class)
                .hasMessage("Table (or view) db1.t1 already exists in Catalog test-catalog.");
    }

    @ParameterizedTest
    @MethodSource("batchOptionProvider")
    public void testDropTable_nonPartitionedTable(Map<String, String> options) throws Exception {
        catalog.createDatabase(path1.getDatabaseName(), null, false);
        catalog.createTable(this.path1, this.createTable(options), false);
        assertTrue(catalog.tableExists(this.path1));
        catalog.dropTable(this.path1, false);
        assertFalse(catalog.tableExists(this.path1));
    }

    @Test
    public void testGetTable_TableNotExistException() throws Exception {
        assertThatThrownBy(() -> catalog.getTable(this.nonExistObjectPath))
                .isInstanceOf(TableNotExistException.class)
                .hasMessage("Table (or view) db1.nonexist does not exist in Catalog test-catalog.");
    }

    @ParameterizedTest
    @MethodSource("batchOptionProvider")
    public void testDbExists(Map<String, String> options) throws Exception {
        catalog.createDatabase(path1.getDatabaseName(), null, false);
        catalog.createTable(this.path1, this.createTable(options), false);
        assertTrue(catalog.databaseExists("db1"));
    }

    @Test
    public void testGetDatabase() throws Exception {
        catalog.createDatabase(path1.getDatabaseName(), null, false);
        CatalogDatabase database = catalog.getDatabase(path1.getDatabaseName());
        assertThat(database.getProperties()).isEmpty();
        assertThat(database.getDescription()).isEmpty();
        assertThatThrownBy(() -> catalog.getDatabase(nonExistDbPath.getDatabaseName()))
                .isInstanceOf(DatabaseNotExistException.class)
                .hasMessageContaining("Database non does not exist in Catalog test-catalog.");
    }

    @Test
    public void testDropDb_DatabaseNotExist_Ignore() throws Exception {
        catalog.dropDatabase("db1", true, false);
    }

    @ParameterizedTest
    @MethodSource("batchOptionProvider")
    public void testAlterTable_TableNotExistException(Map<String, String> options)
            throws Exception {
        assertThatThrownBy(
                        () ->
                                catalog.alterTable(
                                        this.nonExistDbPath, this.createTable(options), false))
                .isInstanceOf(TableNotExistException.class)
                .hasMessage("Table (or view) non.exist does not exist in Catalog test-catalog.");
    }

    @Test
    public void testDropTable_TableNotExistException() throws Exception {
        assertThatThrownBy(() -> catalog.dropTable(this.nonExistDbPath, false))
                .isInstanceOf(TableNotExistException.class)
                .hasMessage("Table (or view) non.exist does not exist in Catalog test-catalog.");
    }

    @Test
    public void testCreateDb_Database() throws Exception {
        catalog.createDatabase(path1.getDatabaseName(), null, false);
        List<String> dbs = catalog.listDatabases();
        assertThat(dbs).hasSize(2);
        assertThat(new HashSet<>(dbs))
                .isEqualTo(
                        new HashSet<>(
                                Arrays.asList(
                                        path1.getDatabaseName(), catalog.getDefaultDatabase())));
    }

    @Test
    public void testCreateDb_DatabaseAlreadyExistException() throws Exception {
        catalog.createDatabase(path1.getDatabaseName(), null, false);

        assertThatThrownBy(() -> catalog.createDatabase(path1.getDatabaseName(), null, false))
                .isInstanceOf(DatabaseAlreadyExistException.class)
                .hasMessage("Database db1 already exists in Catalog test-catalog.");
    }

    @Test
    public void testCreateDb_DatabaseWithPropertiesException() {
        CatalogDatabaseImpl database =
                new CatalogDatabaseImpl(Collections.singletonMap("haa", "ccc"), null);
        assertThatThrownBy(() -> catalog.createDatabase(path1.getDatabaseName(), database, false))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Create database with properties is unsupported.");
    }

    @Test
    public void testCreateDb_DatabaseWithCommentException() {
        CatalogDatabaseImpl database = new CatalogDatabaseImpl(Collections.emptyMap(), "haha");
        assertThatThrownBy(() -> catalog.createDatabase(path1.getDatabaseName(), database, false))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Create database with description is unsupported.");
    }

    @ParameterizedTest
    @MethodSource("batchOptionProvider")
    public void testCreateTable_DatabaseNotExistException(Map<String, String> options) {
        assertThat(catalog.databaseExists(path1.getDatabaseName())).isFalse();

        assertThatThrownBy(
                        () -> catalog.createTable(nonExistObjectPath, createTable(options), false))
                .isInstanceOf(DatabaseNotExistException.class)
                .hasMessage("Database db1 does not exist in Catalog test-catalog.");
    }

    @Test
    public void testDropDb_DatabaseNotExistException() {
        assertThatThrownBy(() -> catalog.dropDatabase(path1.getDatabaseName(), false, false))
                .isInstanceOf(DatabaseNotExistException.class)
                .hasMessage("Database db1 does not exist in Catalog test-catalog.");
    }

    @Test
    public void testCreateTableWithColumnOptions() throws Exception {
        TableSchema schema =
                TableSchema.builder()
                        .field("pk", DataTypes.INT().notNull())
                        .field("test", DataTypes.INT())
                        .field("comp", DataTypes.INT(), "test + 1")
                        .primaryKey("pk")
                        .build();
        CatalogTable catalogTable = new CatalogTableImpl(schema, Collections.emptyMap(), "");

        catalog.createDatabase(path1.getDatabaseName(), null, false);
        catalog.createTable(path1, catalogTable, false);

        CatalogTable got = (CatalogTable) catalog.getTable(path1);
        TableSchema newSchema = got.getSchema();

        assertThat(schema.getTableColumns()).isEqualTo(newSchema.getTableColumns());
        assertThat(schema.getPrimaryKey().get().getColumns())
                .isEqualTo(newSchema.getPrimaryKey().get().getColumns());

        Map<String, String> expected = got.getOptions();
        expected.remove("path");
        assertThat(catalogTable.getOptions()).isEqualTo(expected);
    }

    private void checkEquals(ObjectPath path, CatalogTable t1, CatalogTable t2) {
        Path tablePath =
                ((AbstractCatalog) ((FlinkCatalog) catalog).catalog())
                        .getDataTableLocation(FlinkCatalog.toIdentifier(path));
        Map<String, String> options = new HashMap<>(t1.getOptions());
        options.put("path", tablePath.toString());
        t1 = ((ResolvedCatalogTable) t1).copy(options);
        checkEquals(t1, t2);
    }

    private static void checkEquals(CatalogTable t1, CatalogTable t2) {
        assertThat(t2.getTableKind()).isEqualTo(t1.getTableKind());
        assertThat(t2.getSchema()).isEqualTo(t1.getSchema());
        assertThat(t2.getComment()).isEqualTo(t1.getComment());
        assertThat(t2.getPartitionKeys()).isEqualTo(t1.getPartitionKeys());
        assertThat(t2.isPartitioned()).isEqualTo(t1.isPartitioned());
        assertThat(t2.getOptions()).isEqualTo(t1.getOptions());
    }

    static Stream<Map<String, String>> streamingOptionProvider() {
        return optionProvider(true);
    }

    static Stream<Map<String, String>> batchOptionProvider() {
        return optionProvider(false);
    }

    private static Stream<Map<String, String>> optionProvider(boolean isStreaming) {
        List<Map<String, String>> allOptions = new ArrayList<>();
        for (CoreOptions.StartupMode mode : CoreOptions.StartupMode.values()) {
            Map<String, String> options = new HashMap<>();
            options.put("is_streaming", String.valueOf(isStreaming));
            options.put("scan.mode", mode.toString());
            if (mode == CoreOptions.StartupMode.FROM_SNAPSHOT
                    || mode == CoreOptions.StartupMode.FROM_SNAPSHOT_FULL) {
                options.put("scan.snapshot-id", "1");
            } else if (mode == CoreOptions.StartupMode.FROM_TIMESTAMP) {
                options.put("scan.timestamp-millis", System.currentTimeMillis() + "");
            } else if (mode == CoreOptions.StartupMode.INCREMENTAL_TIMESTAMP) {
                options.put("incremental-between-timestamp", String.format("%s,%s", System.currentTimeMillis(), System.currentTimeMillis()));
            } else if (mode == CoreOptions.StartupMode.INCREMENTAL_SNAPSHOT) {
                options.put("incremental-between-snapshot", "2,5");
            }

            if (isStreaming && mode == CoreOptions.StartupMode.INCREMENTAL_SNAPSHOT) {
                continue;
            }
            allOptions.add(options);
        }
        return allOptions.stream();
    }
}
