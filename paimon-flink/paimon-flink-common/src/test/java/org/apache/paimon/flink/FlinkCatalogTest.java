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
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.log.LogSinkProvider;
import org.apache.paimon.flink.log.LogSourceProvider;
import org.apache.paimon.flink.log.LogStoreRegister;
import org.apache.paimon.flink.log.LogStoreTableFactory;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.utils.ResolvedExpressionMock;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.apache.paimon.CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS;
import static org.apache.paimon.flink.FlinkCatalogOptions.DISABLE_CREATE_TABLE_IN_DEFAULT_DB;
import static org.apache.paimon.flink.FlinkCatalogOptions.LOG_SYSTEM_AUTO_REGISTER;
import static org.apache.paimon.flink.FlinkConnectorOptions.LOG_SYSTEM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatCollection;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link FlinkCatalog}. */
public class FlinkCatalogTest {
    private static final String TESTING_LOG_STORE = "testing";

    private final ObjectPath path1 = new ObjectPath("db1", "t1");
    private final ObjectPath path3 = new ObjectPath("db1", "t2");

    private final ObjectPath tableInDefaultDb = new ObjectPath("default", "t1");

    private final ObjectPath tableInDefaultDb1 = new ObjectPath("default-db", "t1");
    private final ObjectPath nonExistDbPath = ObjectPath.fromString("non.exist");
    private final ObjectPath nonExistObjectPath = ObjectPath.fromString("db1.nonexist");
    private Catalog catalog;

    @TempDir public static java.nio.file.Path temporaryFolder;

    @BeforeEach
    public void beforeEach() throws IOException {
        String path = new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toString();
        Options conf = new Options();
        conf.setString("warehouse", path);
        conf.set(LOG_SYSTEM_AUTO_REGISTER, true);
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
                        Column.physical("third", DataTypes.STRING()),
                        Column.physical(
                                "four",
                                DataTypes.ROW(
                                        DataTypes.FIELD("f1", DataTypes.STRING()),
                                        DataTypes.FIELD("f2", DataTypes.INT()),
                                        DataTypes.FIELD(
                                                "f3",
                                                DataTypes.MAP(
                                                        DataTypes.STRING(), DataTypes.INT()))))),
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
        checkCreateTable(path1, table, (CatalogTable) catalog.getTable(this.path1));
        CatalogTable newTable = this.createAnotherTable(options);
        catalog.alterTable(this.path1, newTable, false);
        assertThat(catalog.getTable(this.path1)).isNotEqualTo(table);
        checkAlterTable(path1, newTable, (CatalogTable) catalog.getTable(this.path1));
        catalog.dropTable(this.path1, false);

        // Not support views
    }

    @ParameterizedTest
    @MethodSource("batchOptionProvider")
    public void testListTables(Map<String, String> options) throws Exception {
        catalog.createDatabase(path1.getDatabaseName(), null, false);
        catalog.createTable(this.path1, this.createTable(options), false);
        catalog.createTable(this.path3, this.createTable(options), false);
        assertThat(catalog.listTables("db1").size()).isEqualTo(2L);

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
                .hasMessageContaining("Paimon Catalog only supports paimon tables");
    }

    @ParameterizedTest
    @MethodSource("streamingOptionProvider")
    public void testCreateTable_Streaming(Map<String, String> options) throws Exception {
        catalog.createDatabase(path1.getDatabaseName(), null, false);
        CatalogTable table = createTable(options);
        catalog.createTable(path1, table, false);
        checkCreateTable(path1, table, (CatalogTable) catalog.getTable(path1));
    }

    @ParameterizedTest
    @MethodSource("batchOptionProvider")
    public void testAlterPartitionedTable(Map<String, String> options) throws Exception {
        catalog.createDatabase(path1.getDatabaseName(), null, false);
        CatalogTable table = this.createPartitionedTable(options);
        catalog.createTable(this.path1, table, false);
        checkCreateTable(path1, table, (CatalogTable) catalog.getTable(this.path1));
        CatalogTable newTable = this.createAnotherPartitionedTable(options);
        catalog.alterTable(this.path1, newTable, false);
        checkAlterTable(path1, newTable, (CatalogTable) catalog.getTable(this.path1));
    }

    @ParameterizedTest
    @MethodSource("batchOptionProvider")
    public void testCreateTable_Batch(Map<String, String> options) throws Exception {
        catalog.createDatabase(path1.getDatabaseName(), null, false);
        CatalogTable table = this.createTable(options);
        catalog.createTable(this.path1, table, false);
        CatalogBaseTable tableCreated = catalog.getTable(this.path1);
        checkCreateTable(path1, table, (CatalogTable) tableCreated);
        assertThat(tableCreated.getDescription().get()).isEqualTo("test comment");
        List<String> tables = catalog.listTables("db1");
        assertThat(tables.size()).isEqualTo(1L);
        assertThat(tables.get(0)).isEqualTo(this.path1.getObjectName());
        catalog.dropTable(this.path1, false);
    }

    @ParameterizedTest
    @MethodSource("batchOptionProvider")
    public void testCreateTable_TableAlreadyExist_ignored(Map<String, String> options)
            throws Exception {
        catalog.createDatabase(path1.getDatabaseName(), null, false);
        CatalogTable table = this.createTable(options);
        catalog.createTable(this.path1, table, false);
        checkCreateTable(path1, table, (CatalogTable) catalog.getTable(this.path1));
        catalog.createTable(this.path1, this.createAnotherTable(options), true);
        checkCreateTable(path1, table, (CatalogTable) catalog.getTable(this.path1));
    }

    @ParameterizedTest
    @MethodSource("batchOptionProvider")
    public void testCreatePartitionedTable_Batch(Map<String, String> options) throws Exception {
        catalog.createDatabase(path1.getDatabaseName(), null, false);
        CatalogTable table = this.createPartitionedTable(options);
        catalog.createTable(this.path1, table, false);
        checkCreateTable(path1, table, (CatalogTable) catalog.getTable(this.path1));
        List<String> tables = catalog.listTables("db1");
        assertThat(tables.size()).isEqualTo(1L);
        assertThat(tables.get(0)).isEqualTo(this.path1.getObjectName());
    }

    @ParameterizedTest
    @MethodSource("batchOptionProvider")
    public void testDropDb_DatabaseNotEmptyException(Map<String, String> options) throws Exception {
        catalog.createDatabase(path1.getDatabaseName(), null, false);
        catalog.createTable(this.path1, this.createTable(options), false);
        assertThatThrownBy(() -> catalog.dropDatabase("db1", true, false))
                .isInstanceOf(DatabaseNotEmptyException.class)
                .hasMessage("Database db1 in catalog test-catalog is not empty.");
    }

    @ParameterizedTest
    @MethodSource("batchOptionProvider")
    public void testTableExists(Map<String, String> options) throws Exception {
        catalog.createDatabase(path1.getDatabaseName(), null, false);
        assertThat(catalog.tableExists(this.path1)).isFalse();
        catalog.createTable(this.path1, this.createTable(options), false);
        assertThat(catalog.tableExists(this.path1)).isTrue();

        // system tables
        assertThat(
                        catalog.tableExists(
                                new ObjectPath(
                                        path1.getDatabaseName(),
                                        path1.getObjectName() + "$snapshots")))
                .isTrue();
        assertThat(
                        catalog.tableExists(
                                new ObjectPath(
                                        path1.getDatabaseName(),
                                        path1.getObjectName() + "$unknown")))
                .isFalse();
    }

    @ParameterizedTest
    @MethodSource("batchOptionProvider")
    public void testAlterTable_TableNotExist_ignored(Map<String, String> options) throws Exception {
        catalog.alterTable(this.nonExistObjectPath, this.createTable(options), true);
        assertThat(catalog.tableExists(this.nonExistObjectPath)).isFalse();
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
        assertThat(catalog.tableExists(this.path1)).isTrue();
        catalog.dropTable(this.path1, false);
        assertThat(catalog.tableExists(this.path1)).isFalse();
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
        assertThat(catalog.databaseExists("db1")).isTrue();
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
    public void testCreateDb_DatabaseWithProperties() throws Exception {
        CatalogDatabaseImpl database =
                new CatalogDatabaseImpl(Collections.singletonMap("haa", "ccc"), null);
        catalog.createDatabase(path1.getDatabaseName(), database, false);
        assertThat(catalog.databaseExists(path1.getDatabaseName())).isTrue();
        // TODO filesystem catalog will ignore all properties
        assertThat(catalog.getDatabase(path1.getDatabaseName()).getProperties().isEmpty()).isTrue();

        // File system catalog doesn't support path for database.
        CatalogDatabaseImpl databaseWithPath =
                new CatalogDatabaseImpl(Collections.singletonMap("location", "/tmp"), null);
        assertThatThrownBy(
                        () ->
                                catalog.createDatabase(
                                        "test-database-with-location", databaseWithPath, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Cannot specify location for a database when using fileSystem catalog.");
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
        ResolvedExpression expression =
                new ResolvedExpressionMock(DataTypes.INT(), () -> "test + 1");
        ResolvedSchema resolvedSchema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("pk", DataTypes.INT().notNull()),
                                Column.physical("test", DataTypes.INT()),
                                Column.computed("comp", expression)),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey("pk", ImmutableList.of("pk")));

        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT().notNull())
                        .column("test", DataTypes.INT())
                        .columnByExpression("comp", "test + 1")
                        .primaryKey("pk")
                        .build();

        CatalogTable catalogTable =
                new ResolvedCatalogTable(
                        CatalogTable.of(schema, "", Collections.emptyList(), new HashMap<>()),
                        resolvedSchema);

        catalog.createDatabase(path1.getDatabaseName(), null, false);
        catalog.createTable(path1, catalogTable, false);

        CatalogTable got = (CatalogTable) catalog.getTable(path1);
        Schema newSchema = got.getUnresolvedSchema();

        assertThat(schema.getColumns()).isEqualTo(newSchema.getColumns());
        assertThat(schema.getPrimaryKey().get().getColumnNames())
                .isEqualTo(newSchema.getPrimaryKey().get().getColumnNames());

        Map<String, String> expected = got.getOptions();
        expected.remove("path");
        expected.remove(FlinkCatalogOptions.REGISTER_TIMEOUT.key());
        assertThat(catalogTable.getOptions()).isEqualTo(expected);
    }

    @Test
    public void testCreateTableWithLogSystemRegister() throws Exception {
        catalog.createDatabase(path1.getDatabaseName(), null, false);

        ResolvedExpression expression =
                new ResolvedExpressionMock(DataTypes.INT(), () -> "test + 1");
        ResolvedSchema resolvedSchema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("pk", DataTypes.INT().notNull()),
                                Column.physical("test", DataTypes.INT()),
                                Column.computed("comp", expression)),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey("pk", ImmutableList.of("pk")));

        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT().notNull())
                        .column("test", DataTypes.INT())
                        .columnByExpression("comp", "test + 1")
                        .primaryKey("pk")
                        .build();

        Map<String, String> options = new HashMap<>();
        CatalogTable catalogTable1 =
                new ResolvedCatalogTable(
                        CatalogTable.of(schema, "", Collections.emptyList(), options),
                        resolvedSchema);
        catalog.createTable(path1, catalogTable1, false);
        CatalogBaseTable storedTable1 = catalog.getTable(path1);
        assertThat(storedTable1.getOptions().containsKey("testing.log.store.topic")).isFalse();

        options.put(LOG_SYSTEM.key(), TESTING_LOG_STORE);
        CatalogTable catalogTable2 =
                new ResolvedCatalogTable(
                        CatalogTable.of(schema, "", Collections.emptyList(), options),
                        resolvedSchema);
        catalog.createTable(path3, catalogTable2, false);

        CatalogBaseTable storedTable2 = catalog.getTable(path3);
        assertThat(storedTable2.getOptions().get("testing.log.store.topic"))
                .isEqualTo(String.format("%s-topic", path3.getObjectName()));
        assertThatThrownBy(() -> catalog.dropTable(path3, true))
                .hasMessage("Check unregister log store topic here.");
    }

    @Test
    public void testDisableCreateTableInDefaultDB()
            throws TableAlreadyExistException, DatabaseNotExistException,
                    DatabaseAlreadyExistException {
        String path = new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toString();
        Options conf = new Options();
        conf.setString("warehouse", path);
        conf.set(LOG_SYSTEM_AUTO_REGISTER, true);
        conf.set(DISABLE_CREATE_TABLE_IN_DEFAULT_DB, true);
        Catalog catalog =
                FlinkCatalogFactory.createCatalog(
                        "test-ddl-catalog",
                        CatalogContext.create(conf),
                        FlinkCatalogTest.class.getClassLoader());

        assertThatThrownBy(
                        () ->
                                catalog.createTable(
                                        tableInDefaultDb,
                                        this.createTable(new HashMap<>(0)),
                                        false))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage(
                        "Creating table in default database is disabled, please specify a database name.");
        assertThatCollection(catalog.listDatabases()).isEmpty();

        catalog.createDatabase("db1", null, false);
        assertThatCode(() -> catalog.createTable(path1, this.createTable(new HashMap<>(0)), false))
                .doesNotThrowAnyException();
        assertThat(catalog.listDatabases()).containsExactlyInAnyOrder("db1");

        conf.set(FlinkCatalogOptions.DEFAULT_DATABASE, "default-db");
        Catalog catalog1 =
                FlinkCatalogFactory.createCatalog(
                        "test-ddl-catalog1",
                        CatalogContext.create(conf),
                        FlinkCatalogTest.class.getClassLoader());

        assertThatThrownBy(
                        () ->
                                catalog1.createTable(
                                        tableInDefaultDb1,
                                        this.createTable(new HashMap<>(0)),
                                        false))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage(
                        "Creating table in default database is disabled, please specify a database name.");
    }

    @Test
    void testCreateTableFromTableDescriptor() throws Exception {
        catalog.createDatabase(path1.getDatabaseName(), null, false);

        final ResolvedSchema resolvedSchema = this.createSchema();
        final TableDescriptor tableDescriptor =
                TableDescriptor.forConnector("paimon")
                        .schema(Schema.newBuilder().fromResolvedSchema(resolvedSchema).build())
                        .build();
        final CatalogTable catalogTable =
                new ResolvedCatalogTable(tableDescriptor.toCatalogTable(), resolvedSchema);
        catalog.createTable(path1, catalogTable, false);
        checkCreateTable(path1, catalogTable, (CatalogTable) catalog.getTable(path1));
    }

    private void checkCreateTable(ObjectPath path, CatalogTable expected, CatalogTable actual) {
        checkEquals(
                path,
                expected,
                actual,
                Collections.singletonMap(
                        FlinkCatalogOptions.REGISTER_TIMEOUT.key(),
                        FlinkCatalogOptions.REGISTER_TIMEOUT.defaultValue().toString()),
                Collections.singleton(CONNECTOR.key()));
    }

    private void checkAlterTable(ObjectPath path, CatalogTable expected, CatalogTable actual) {
        checkEquals(path, expected, actual, Collections.emptyMap(), Collections.emptySet());
    }

    private void checkEquals(
            ObjectPath path,
            CatalogTable t1,
            CatalogTable t2,
            Map<String, String> optionsToAdd,
            Set<String> optionsToRemove) {
        Path tablePath =
                ((AbstractCatalog) ((FlinkCatalog) catalog).catalog())
                        .getDataTableLocation(FlinkCatalog.toIdentifier(path));
        Map<String, String> options = new HashMap<>(t1.getOptions());
        options.put("path", tablePath.toString());
        options.putAll(optionsToAdd);
        optionsToRemove.forEach(options::remove);
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
            } else if (mode == CoreOptions.StartupMode.FROM_FILE_CREATION_TIME) {
                options.put(SCAN_FILE_CREATION_TIME_MILLIS.key(), System.currentTimeMillis() + "");
            } else if (mode == CoreOptions.StartupMode.INCREMENTAL) {
                options.put("incremental-between", "2,5");
            }

            if (isStreaming && mode == CoreOptions.StartupMode.INCREMENTAL) {
                continue;
            }
            allOptions.add(options);
        }
        return allOptions.stream();
    }

    /** Testing log store register factory to create {@link TestingLogStoreRegister}. */
    public static class TestingLogSoreRegisterFactory implements LogStoreTableFactory {

        @Override
        public String identifier() {
            return TESTING_LOG_STORE;
        }

        @Override
        public LogSourceProvider createSourceProvider(
                DynamicTableFactory.Context context,
                DynamicTableSource.Context sourceContext,
                @Nullable int[][] projectFields) {
            throw new UnsupportedOperationException();
        }

        @Override
        public LogSinkProvider createSinkProvider(
                DynamicTableFactory.Context context, DynamicTableSink.Context sinkContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public LogStoreRegister createRegister(RegisterContext context) {
            return new TestingLogStoreRegister(context.getIdentifier());
        }
    }

    /** Testing log store register. */
    private static class TestingLogStoreRegister implements LogStoreRegister {
        private final Identifier table;

        private TestingLogStoreRegister(Identifier table) {
            this.table = table;
        }

        @Override
        public Map<String, String> registerTopic() {
            return Collections.singletonMap(
                    "testing.log.store.topic", String.format("%s-topic", table.getObjectName()));
        }

        @Override
        public void unRegisterTopic() {
            throw new UnsupportedOperationException("Check unregister log store topic here.");
        }
    }
}
