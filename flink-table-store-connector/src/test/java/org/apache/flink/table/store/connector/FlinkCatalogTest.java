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

package org.apache.flink.table.store.connector;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTable;
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
import org.apache.flink.table.store.catalog.CatalogContext;
import org.apache.flink.table.store.file.catalog.AbstractCatalog;
import org.apache.flink.table.store.fs.Path;
import org.apache.flink.table.store.options.Options;

import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link FlinkCatalog}. */
public class FlinkCatalogTest {

    @Rule public ExpectedException exception = ExpectedException.none();

    private final ObjectPath path1 = new ObjectPath("db1", "t1");
    private final ObjectPath path3 = new ObjectPath("db1", "t2");
    private final ObjectPath nonExistDbPath = ObjectPath.fromString("non.exist");
    private final ObjectPath nonExistObjectPath = ObjectPath.fromString("db1.nonexist");
    private Catalog catalog;

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    @Before
    public void beforeEach() throws IOException {
        String path = TEMPORARY_FOLDER.newFolder().toURI().toString();
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

    private CatalogTable createStreamingTable() {
        ResolvedSchema resolvedSchema = this.createSchema();
        CatalogTable origin =
                CatalogTable.of(
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                        "test comment",
                        Collections.emptyList(),
                        this.getStreamingTableProperties());
        return new ResolvedCatalogTable(origin, resolvedSchema);
    }

    private List<String> createPartitionKeys() {
        return Arrays.asList("second", "third");
    }

    private Map<String, String> getBatchTableProperties() {
        return new HashMap<String, String>() {
            {
                this.put("is_streaming", "false");
            }
        };
    }

    private Map<String, String> getStreamingTableProperties() {
        return new HashMap<String, String>() {
            {
                this.put("is_streaming", "true");
            }
        };
    }

    private CatalogTable createAnotherTable() {
        // TODO support change schema, modify it to createAnotherSchema
        ResolvedSchema resolvedSchema = this.createSchema();
        CatalogTable origin =
                CatalogTable.of(
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                        "test comment",
                        Collections.emptyList(),
                        this.getBatchTableProperties());
        return new ResolvedCatalogTable(origin, resolvedSchema);
    }

    private CatalogTable createAnotherPartitionedTable() {
        // TODO support change schema, modify it to createAnotherSchema
        ResolvedSchema resolvedSchema = this.createSchema();
        CatalogTable origin =
                CatalogTable.of(
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                        "test comment",
                        this.createPartitionKeys(),
                        this.getBatchTableProperties());
        return new ResolvedCatalogTable(origin, resolvedSchema);
    }

    private CatalogTable createTable() {
        ResolvedSchema resolvedSchema = this.createSchema();
        CatalogTable origin =
                CatalogTable.of(
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                        "test comment",
                        Collections.emptyList(),
                        this.getBatchTableProperties());
        return new ResolvedCatalogTable(origin, resolvedSchema);
    }

    private CatalogTable createPartitionedTable() {
        ResolvedSchema resolvedSchema = this.createSchema();
        CatalogTable origin =
                CatalogTable.of(
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                        "test comment",
                        this.createPartitionKeys(),
                        this.getBatchTableProperties());
        return new ResolvedCatalogTable(origin, resolvedSchema);
    }

    @Test
    public void testAlterTable() throws Exception {
        catalog.createDatabase(path1.getDatabaseName(), null, false);
        CatalogTable table = this.createTable();
        catalog.createTable(this.path1, table, false);
        checkEquals(path1, table, (CatalogTable) catalog.getTable(this.path1));
        CatalogTable newTable = this.createAnotherTable();
        catalog.alterTable(this.path1, newTable, false);
        Assert.assertNotEquals(table, catalog.getTable(this.path1));
        checkEquals(path1, newTable, (CatalogTable) catalog.getTable(this.path1));
        catalog.dropTable(this.path1, false);

        // Not support views
    }

    @Test
    public void testListTables() throws Exception {
        catalog.createDatabase(path1.getDatabaseName(), null, false);
        catalog.createTable(this.path1, this.createTable(), false);
        catalog.createTable(this.path3, this.createTable(), false);
        Assert.assertEquals(2L, catalog.listTables("db1").size());

        // Not support views
    }

    @Test
    public void testAlterTable_differentTypedTable() {
        // TODO support this
    }

    @Test
    public void testCreateFlinkTable() {
        // create a flink table
        CatalogTable table = createTable();
        HashMap<String, String> newOptions = new HashMap<>(table.getOptions());
        newOptions.put("connector", "filesystem");
        CatalogTable newTable = table.copy(newOptions);

        assertThatThrownBy(() -> catalog.createTable(this.path1, newTable, false))
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining(
                        "Table Store Catalog only supports table store tables,"
                                + " not 'filesystem' connector. You can create TEMPORARY table instead.");
    }

    @Test
    public void testCreateTable_Streaming() throws Exception {
        catalog.createDatabase(path1.getDatabaseName(), null, false);
        CatalogTable table = createStreamingTable();
        catalog.createTable(path1, table, false);
        checkEquals(path1, table, (CatalogTable) catalog.getTable(path1));
    }

    @Test
    public void testAlterPartitionedTable() throws Exception {
        catalog.createDatabase(path1.getDatabaseName(), null, false);
        CatalogTable table = this.createPartitionedTable();
        catalog.createTable(this.path1, table, false);
        checkEquals(path1, table, (CatalogTable) catalog.getTable(this.path1));
        CatalogTable newTable = this.createAnotherPartitionedTable();
        catalog.alterTable(this.path1, newTable, false);
        checkEquals(path1, newTable, (CatalogTable) catalog.getTable(this.path1));
    }

    @Test
    public void testCreateTable_Batch() throws Exception {
        catalog.createDatabase(path1.getDatabaseName(), null, false);
        CatalogTable table = this.createTable();
        catalog.createTable(this.path1, table, false);
        CatalogBaseTable tableCreated = catalog.getTable(this.path1);
        checkEquals(path1, table, (CatalogTable) tableCreated);
        Assert.assertEquals("test comment", tableCreated.getDescription().get());
        List<String> tables = catalog.listTables("db1");
        Assert.assertEquals(1L, tables.size());
        Assert.assertEquals(this.path1.getObjectName(), tables.get(0));
        catalog.dropTable(this.path1, false);
    }

    @Test
    public void testCreateTable_TableAlreadyExist_ignored() throws Exception {
        catalog.createDatabase(path1.getDatabaseName(), null, false);
        CatalogTable table = this.createTable();
        catalog.createTable(this.path1, table, false);
        checkEquals(path1, table, (CatalogTable) catalog.getTable(this.path1));
        catalog.createTable(this.path1, this.createAnotherTable(), true);
        checkEquals(path1, table, (CatalogTable) catalog.getTable(this.path1));
    }

    @Test
    public void testCreatePartitionedTable_Batch() throws Exception {
        catalog.createDatabase(path1.getDatabaseName(), null, false);
        CatalogTable table = this.createPartitionedTable();
        catalog.createTable(this.path1, table, false);
        checkEquals(path1, table, (CatalogTable) catalog.getTable(this.path1));
        List<String> tables = catalog.listTables("db1");
        Assert.assertEquals(1L, tables.size());
        Assert.assertEquals(this.path1.getObjectName(), tables.get(0));
    }

    @Test
    public void testDropDb_DatabaseNotEmptyException() throws Exception {
        catalog.createDatabase(path1.getDatabaseName(), null, false);
        catalog.createTable(this.path1, this.createTable(), false);
        this.exception.expect(DatabaseNotEmptyException.class);
        this.exception.expectMessage("Database db1 in catalog test-catalog is not empty");
        catalog.dropDatabase("db1", true, false);
    }

    @Test
    public void testTableExists() throws Exception {
        catalog.createDatabase(path1.getDatabaseName(), null, false);
        Assert.assertFalse(catalog.tableExists(this.path1));
        catalog.createTable(this.path1, this.createTable(), false);
        Assert.assertTrue(catalog.tableExists(this.path1));

        // system tables
        Assert.assertTrue(
                catalog.tableExists(
                        new ObjectPath(
                                path1.getDatabaseName(), path1.getObjectName() + "$snapshots")));
        Assert.assertFalse(
                catalog.tableExists(
                        new ObjectPath(
                                path1.getDatabaseName(), path1.getObjectName() + "$unknown")));
    }

    @Test
    public void testAlterTable_TableNotExist_ignored() throws Exception {
        catalog.alterTable(this.nonExistObjectPath, this.createTable(), true);
        Assert.assertFalse(catalog.tableExists(this.nonExistObjectPath));
    }

    @Test
    public void testDropTable_TableNotExist_ignored() throws Exception {
        catalog.dropTable(this.nonExistObjectPath, true);
    }

    @Test
    public void testCreateTable_TableAlreadyExistException() throws Exception {
        catalog.createDatabase(path1.getDatabaseName(), null, false);
        catalog.createTable(this.path1, this.createTable(), false);
        this.exception.expect(TableAlreadyExistException.class);
        this.exception.expectMessage("Table (or view) db1.t1 already exists in Catalog");
        catalog.createTable(this.path1, this.createTable(), false);
    }

    @Test
    public void testDropTable_nonPartitionedTable() throws Exception {
        catalog.createDatabase(path1.getDatabaseName(), null, false);
        catalog.createTable(this.path1, this.createTable(), false);
        Assert.assertTrue(catalog.tableExists(this.path1));
        catalog.dropTable(this.path1, false);
        Assert.assertFalse(catalog.tableExists(this.path1));
    }

    @Test
    public void testGetTable_TableNotExistException() throws Exception {
        this.exception.expect(TableNotExistException.class);
        this.exception.expectMessage("Table (or view) db1.nonexist does not exist in Catalog");
        catalog.getTable(this.nonExistObjectPath);
    }

    @Test
    public void testDbExists() throws Exception {
        catalog.createDatabase(path1.getDatabaseName(), null, false);
        catalog.createTable(this.path1, this.createTable(), false);
        Assert.assertTrue(catalog.databaseExists("db1"));
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

    @Test
    public void testAlterTable_TableNotExistException() throws Exception {
        this.exception.expect(TableNotExistException.class);
        this.exception.expectMessage("Table (or view) non.exist does not exist in Catalog");
        catalog.alterTable(this.nonExistDbPath, this.createTable(), false);
    }

    @Test
    public void testDropTable_TableNotExistException() throws Exception {
        this.exception.expect(TableNotExistException.class);
        this.exception.expectMessage("Table (or view) non.exist does not exist in Catalog");
        catalog.dropTable(this.nonExistDbPath, false);
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

    @Test
    public void testCreateTable_DatabaseNotExistException() {
        assertThat(catalog.databaseExists(path1.getDatabaseName())).isFalse();

        assertThatThrownBy(() -> catalog.createTable(nonExistObjectPath, createTable(), false))
                .isInstanceOf(DatabaseNotExistException.class)
                .hasMessage("Database db1 does not exist in Catalog test-catalog.");
    }

    @Test
    public void testDropDb_DatabaseNotExistException() {
        assertThatThrownBy(() -> catalog.dropDatabase(path1.getDatabaseName(), false, false))
                .isInstanceOf(DatabaseNotExistException.class)
                .hasMessage("Database db1 does not exist in Catalog test-catalog.");
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
}
