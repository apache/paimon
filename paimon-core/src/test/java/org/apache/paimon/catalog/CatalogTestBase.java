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

package org.apache.paimon.catalog;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;
import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Base test class of paimon catalog in {@link Catalog}. */
public abstract class CatalogTestBase {

    @TempDir java.nio.file.Path tempFile;
    protected String warehouse;
    protected FileIO fileIO;
    protected Catalog catalog;

    protected static final Schema DEFAULT_TABLE_SCHEMA =
            new Schema(
                    Lists.newArrayList(
                            new DataField(0, "pk", DataTypes.INT()),
                            new DataField(1, "col1", DataTypes.STRING()),
                            new DataField(2, "col2", DataTypes.STRING())),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Maps.newHashMap(),
                    "");

    @BeforeEach
    public void setUp() throws Exception {
        warehouse = tempFile.toUri().toString();
        Options catalogOptions = new Options();
        catalogOptions.set(CatalogOptions.WAREHOUSE, warehouse);
        CatalogContext catalogContext = CatalogContext.create(catalogOptions);
        fileIO = FileIO.get(new Path(warehouse), catalogContext);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (catalog != null) {
            catalog.close();
        }
    }

    @Test
    public void testListDatabasesWhenNoDatabases() {
        List<String> databases = catalog.listDatabases();
        assertThat(databases).isEqualTo(new ArrayList<>());
    }

    @Test
    public void testListDatabases() throws Exception {
        catalog.createDatabase("db1", false);
        catalog.createDatabase("db2", false);
        catalog.createDatabase("db3", false);

        List<String> databases = catalog.listDatabases();
        assertThat(databases).contains("db1", "db2", "db3");
    }

    @Test
    public void testDatabaseExistsWhenExists() throws Exception {
        // Database exists returns true when the database exists
        catalog.createDatabase("test_db", false);
        boolean exists = catalog.databaseExists("test_db");
        assertThat(exists).isTrue();

        // Database exists returns false when the database does not exist
        exists = catalog.databaseExists("non_existing_db");
        assertThat(exists).isFalse();
    }

    @Test
    public void testCreateDatabase() throws Exception {
        // Create database creates a new database when it does not exist
        catalog.createDatabase("new_db", false);
        boolean exists = catalog.databaseExists("new_db");
        assertThat(exists).isTrue();

        catalog.createDatabase("existing_db", false);

        // Create database throws DatabaseAlreadyExistException when database already exists and
        // ignoreIfExists is false
        assertThatExceptionOfType(Catalog.DatabaseAlreadyExistException.class)
                .isThrownBy(() -> catalog.createDatabase("existing_db", false))
                .withMessage("Database existing_db already exists.");

        // Create database does not throw exception when database already exists and ignoreIfExists
        // is true
        assertThatCode(() -> catalog.createDatabase("existing_db", true))
                .doesNotThrowAnyException();
    }

    @Test
    public void testDropDatabase() throws Exception {
        // Drop database deletes the database when it exists and there are no tables
        catalog.createDatabase("db_to_drop", false);
        catalog.dropDatabase("db_to_drop", false, false);
        boolean exists = catalog.databaseExists("db_to_drop");
        assertThat(exists).isFalse();

        // Drop database does not throw exception when database does not exist and ignoreIfNotExists
        // is true
        assertThatCode(() -> catalog.dropDatabase("non_existing_db", true, false))
                .doesNotThrowAnyException();

        // Drop database deletes all tables in the database when cascade is true
        catalog.createDatabase("db_to_drop", false);
        catalog.createTable(Identifier.create("db_to_drop", "table1"), DEFAULT_TABLE_SCHEMA, false);
        catalog.createTable(Identifier.create("db_to_drop", "table2"), DEFAULT_TABLE_SCHEMA, false);

        catalog.dropDatabase("db_to_drop", false, true);
        exists = catalog.databaseExists("db_to_drop");
        assertThat(exists).isFalse();

        // Drop database throws DatabaseNotEmptyException when cascade is false and there are tables
        // in the database
        catalog.createDatabase("db_with_tables", false);
        catalog.createTable(
                Identifier.create("db_with_tables", "table1"), DEFAULT_TABLE_SCHEMA, false);

        assertThatExceptionOfType(Catalog.DatabaseNotEmptyException.class)
                .isThrownBy(() -> catalog.dropDatabase("db_with_tables", false, false))
                .withMessage("Database db_with_tables is not empty.");
    }

    @Test
    public void testListTables() throws Exception {
        // List tables returns an empty list when there are no tables in the database
        catalog.createDatabase("test_db", false);
        List<String> tables = catalog.listTables("test_db");
        assertThat(tables).isEmpty();

        // List tables returns a list with the names of all tables in the database
        catalog.createTable(Identifier.create("test_db", "table1"), DEFAULT_TABLE_SCHEMA, false);
        catalog.createTable(Identifier.create("test_db", "table2"), DEFAULT_TABLE_SCHEMA, false);
        catalog.createTable(Identifier.create("test_db", "table3"), DEFAULT_TABLE_SCHEMA, false);

        tables = catalog.listTables("test_db");
        assertThat(tables).containsExactlyInAnyOrder("table1", "table2", "table3");
    }

    @Test
    public void testTableExists() throws Exception {
        // Table exists returns true when the table exists in the database
        catalog.createDatabase("test_db", false);
        Identifier identifier = Identifier.create("test_db", "test_table");
        catalog.createTable(identifier, DEFAULT_TABLE_SCHEMA, false);

        boolean exists = catalog.tableExists(identifier);
        assertThat(exists).isTrue();

        // Table exists returns false when the table does not exist in the database
        exists = catalog.tableExists(Identifier.create("non_existing_db", "non_existing_table"));
        assertThat(exists).isFalse();
    }

    @Test
    public void testCreateTable() throws Exception {
        catalog.createDatabase("test_db", false);
        // Create table creates a new table when it does not exist
        Identifier identifier = Identifier.create("test_db", "new_table");
        Schema schema =
                Schema.newBuilder()
                        .column("pk1", DataTypes.INT())
                        .column("pk2", DataTypes.STRING())
                        .column("pk3", DataTypes.STRING())
                        .column(
                                "col1",
                                DataTypes.ROW(
                                        DataTypes.STRING(),
                                        DataTypes.BIGINT(),
                                        DataTypes.TIMESTAMP(),
                                        DataTypes.ARRAY(DataTypes.STRING())))
                        .column("col2", DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT()))
                        .column("col3", DataTypes.ARRAY(DataTypes.ROW(DataTypes.STRING())))
                        .partitionKeys("pk1", "pk2")
                        .primaryKey("pk1", "pk2", "pk3")
                        .build();

        // Create table throws Exception when auto-create = true.
        schema.options().put(CoreOptions.AUTO_CREATE.key(), Boolean.TRUE.toString());
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> catalog.createTable(identifier, schema, false))
                .withMessage("The value of auto-create property should be false.");
        schema.options().remove(CoreOptions.AUTO_CREATE.key());

        catalog.createTable(identifier, schema, false);
        boolean exists = catalog.tableExists(identifier);
        assertThat(exists).isTrue();

        // Create table throws Exception when table is system table
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                catalog.createTable(
                                        Identifier.create("test_db", "$system_table"),
                                        DEFAULT_TABLE_SCHEMA,
                                        false))
                .withMessage(
                        "Cannot 'createTable' for system table 'Identifier{database='test_db', object='$system_table'}', please use data table.");

        // Create table throws DatabaseNotExistException when database does not exist
        assertThatExceptionOfType(Catalog.DatabaseNotExistException.class)
                .isThrownBy(
                        () ->
                                catalog.createTable(
                                        Identifier.create("non_existing_db", "test_table"),
                                        DEFAULT_TABLE_SCHEMA,
                                        false))
                .withMessage("Database non_existing_db does not exist.");

        // Create table throws TableAlreadyExistException when table already exists and
        // ignoreIfExists is false
        Identifier existingTable = Identifier.create("test_db", "existing_table");
        catalog.createTable(
                existingTable,
                new Schema(
                        Lists.newArrayList(new DataField(0, "col1", DataTypes.STRING())),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Maps.newHashMap(),
                        ""),
                false);
        assertThatExceptionOfType(Catalog.TableAlreadyExistException.class)
                .isThrownBy(
                        () ->
                                catalog.createTable(
                                        existingTable,
                                        new Schema(
                                                Lists.newArrayList(
                                                        new DataField(
                                                                0, "col2", DataTypes.STRING())),
                                                Collections.emptyList(),
                                                Collections.emptyList(),
                                                Maps.newHashMap(),
                                                ""),
                                        false))
                .withMessage("Table test_db.existing_table already exists.");

        // Create table does not throw exception when table already exists and ignoreIfExists is
        // true
        assertThatCode(
                        () ->
                                catalog.createTable(
                                        existingTable,
                                        new Schema(
                                                Lists.newArrayList(
                                                        new DataField(
                                                                0, "col2", DataTypes.STRING())),
                                                Collections.emptyList(),
                                                Collections.emptyList(),
                                                Maps.newHashMap(),
                                                ""),
                                        true))
                .doesNotThrowAnyException();
    }

    @Test
    public void testGetTable() throws Exception {
        catalog.createDatabase("test_db", false);

        // Get system and data table when the table exists
        Identifier identifier = Identifier.create("test_db", "test_table");
        catalog.createTable(identifier, DEFAULT_TABLE_SCHEMA, false);
        Table systemTable = catalog.getTable(Identifier.create("test_db", "test_table$snapshots"));
        assertThat(systemTable).isNotNull();
        Table dataTable = catalog.getTable(identifier);
        assertThat(dataTable).isNotNull();

        // Get system table throws Exception when table contains multiple '$' separator
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                catalog.getTable(
                                        Identifier.create(
                                                "test_db", "test_table$snapshots$snapshots")))
                .withMessage(
                        "System table can only contain one '$' separator, but this is: test_table$snapshots$snapshots");

        // Get system table throws TableNotExistException when data table does not exist
        assertThatExceptionOfType(Catalog.TableNotExistException.class)
                .isThrownBy(
                        () ->
                                catalog.getTable(
                                        Identifier.create(
                                                "test_db", "non_existing_table$snapshots")))
                .withMessage("Table test_db.non_existing_table does not exist.");

        // Get system table throws TableNotExistException when system table type does not exist
        assertThatExceptionOfType(Catalog.TableNotExistException.class)
                .isThrownBy(
                        () ->
                                catalog.getTable(
                                        Identifier.create("test_db", "non_existing_table$schema1")))
                .withMessage("Table test_db.non_existing_table does not exist.");

        // Get data table throws TableNotExistException when table does not exist
        assertThatExceptionOfType(Catalog.TableNotExistException.class)
                .isThrownBy(
                        () -> catalog.getTable(Identifier.create("test_db", "non_existing_table")))
                .withMessage("Table test_db.non_existing_table does not exist.");

        // Get data table throws TableNotExistException when database does not exist
        assertThatExceptionOfType(Catalog.TableNotExistException.class)
                .isThrownBy(
                        () -> catalog.getTable(Identifier.create("non_existing_db", "test_table")))
                .withMessage("Table non_existing_db.test_table does not exist.");
    }

    @Test
    public void testDropTable() throws Exception {
        catalog.createDatabase("test_db", false);

        // Drop table deletes the table when it exists
        Identifier identifier = Identifier.create("test_db", "table_to_drop");
        catalog.createTable(identifier, DEFAULT_TABLE_SCHEMA, false);
        catalog.dropTable(identifier, false);
        boolean exists = catalog.tableExists(identifier);
        assertThat(exists).isFalse();

        // Drop table throws Exception when table is system table
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                catalog.dropTable(
                                        Identifier.create("test_db", "$system_table"), false))
                .withMessage(
                        "Cannot 'dropTable' for system table 'Identifier{database='test_db', object='$system_table'}', please use data table.");

        // Drop table throws TableNotExistException when table does not exist and ignoreIfNotExists
        // is false
        Identifier nonExistingTable = Identifier.create("test_db", "non_existing_table");
        assertThatExceptionOfType(Catalog.TableNotExistException.class)
                .isThrownBy(() -> catalog.dropTable(nonExistingTable, false))
                .withMessage("Table test_db.non_existing_table does not exist.");

        // Drop table does not throw exception when table does not exist and ignoreIfNotExists is
        // true
        assertThatCode(() -> catalog.dropTable(nonExistingTable, true)).doesNotThrowAnyException();
    }

    @Test
    public void testRenameTable() throws Exception {
        catalog.createDatabase("test_db", false);

        // Rename table renames an existing table
        Identifier fromTable = Identifier.create("test_db", "test_table");
        catalog.createTable(fromTable, DEFAULT_TABLE_SCHEMA, false);
        Identifier toTable = Identifier.create("test_db", "new_table");
        catalog.renameTable(fromTable, toTable, false);
        assertThat(catalog.tableExists(fromTable)).isFalse();
        assertThat(catalog.tableExists(toTable)).isTrue();

        // Rename table throws Exception when original or target table is system table
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                catalog.renameTable(
                                        Identifier.create("test_db", "$system_table"),
                                        toTable,
                                        false))
                .withMessage(
                        "Cannot 'renameTable' for system table 'Identifier{database='test_db', object='$system_table'}', please use data table.");

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                catalog.renameTable(
                                        fromTable,
                                        Identifier.create("test_db", "$system_table"),
                                        false))
                .withMessage(
                        "Cannot 'renameTable' for system table 'Identifier{database='test_db', object='$system_table'}', please use data table.");

        // Rename table throws TableNotExistException when table does not exist
        assertThatExceptionOfType(Catalog.TableNotExistException.class)
                .isThrownBy(
                        () ->
                                catalog.renameTable(
                                        Identifier.create("test_db", "non_existing_table"),
                                        Identifier.create("test_db", "new_table"),
                                        false))
                .withMessage("Table test_db.non_existing_table does not exist.");
    }

    @Test
    public void testAlterTable() throws Exception {
        catalog.createDatabase("test_db", false);

        // Alter table adds a new column to an existing table
        Identifier identifier = Identifier.create("test_db", "test_table");
        catalog.createTable(
                identifier,
                new Schema(
                        Lists.newArrayList(new DataField(0, "col1", DataTypes.STRING())),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Maps.newHashMap(),
                        ""),
                false);
        catalog.alterTable(
                identifier,
                Lists.newArrayList(
                        SchemaChange.addColumn("col2", DataTypes.DATE()),
                        SchemaChange.addColumn("col3", DataTypes.STRING(), "col3 field")),
                false);
        Table table = catalog.getTable(identifier);
        assertThat(table.rowType().getFields()).hasSize(3);
        int index = table.rowType().getFieldIndex("col2");
        int index2 = table.rowType().getFieldIndex("col3");
        assertThat(index).isEqualTo(1);
        assertThat(index2).isEqualTo(2);
        assertThat(table.rowType().getTypeAt(index)).isEqualTo(DataTypes.DATE());
        assertThat(table.rowType().getTypeAt(index2)).isEqualTo(DataTypes.STRING());
        assertThat(table.rowType().getFields().get(2).description()).isEqualTo("col3 field");

        // Alter table throws Exception when table is system table
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                catalog.alterTable(
                                        Identifier.create("test_db", "$system_table"),
                                        Lists.newArrayList(
                                                SchemaChange.addColumn("col2", DataTypes.DATE())),
                                        false))
                .withMessage(
                        "Cannot 'alterTable' for system table 'Identifier{database='test_db', object='$system_table'}', please use data table.");

        // Alter table throws TableNotExistException when table does not exist
        assertThatExceptionOfType(Catalog.TableNotExistException.class)
                .isThrownBy(
                        () ->
                                catalog.alterTable(
                                        Identifier.create("test_db", "non_existing_table"),
                                        Lists.newArrayList(
                                                SchemaChange.addColumn("col3", DataTypes.INT())),
                                        false))
                .withMessage("Table test_db.non_existing_table does not exist.");

        // Alter table adds a column throws ColumnAlreadyExistException when column already exists
        assertThatThrownBy(
                        () ->
                                catalog.alterTable(
                                        identifier,
                                        Lists.newArrayList(
                                                SchemaChange.addColumn("col1", DataTypes.INT())),
                                        false))
                .satisfies(
                        anyCauseMatches(
                                Catalog.ColumnAlreadyExistException.class,
                                "Column col1 already exists in the test_db.test_table table."));
    }

    @Test
    public void testAlterTableRenameColumn() throws Exception {
        catalog.createDatabase("test_db", false);

        // Alter table renames a column in an existing table
        Identifier identifier = Identifier.create("test_db", "test_table");
        catalog.createTable(
                identifier,
                new Schema(
                        Lists.newArrayList(new DataField(0, "col1", DataTypes.STRING())),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Maps.newHashMap(),
                        ""),
                false);
        catalog.alterTable(
                identifier,
                Lists.newArrayList(SchemaChange.renameColumn("col1", "new_col1")),
                false);
        Table table = catalog.getTable(identifier);

        assertThat(table.rowType().getFields()).hasSize(1);
        assertThat(table.rowType().getFieldIndex("col1")).isLessThan(0);
        assertThat(table.rowType().getFieldIndex("new_col1")).isEqualTo(0);

        // Alter table renames a new column throws ColumnAlreadyExistException when column already
        // exists
        assertThatThrownBy(
                        () ->
                                catalog.alterTable(
                                        identifier,
                                        Lists.newArrayList(
                                                SchemaChange.renameColumn("col1", "new_col1")),
                                        false))
                .satisfies(
                        anyCauseMatches(
                                Catalog.ColumnAlreadyExistException.class,
                                "Column col1 already exists in the test_db.test_table table."));

        // Alter table renames a column throws ColumnNotExistException when column does not exist
        assertThatThrownBy(
                        () ->
                                catalog.alterTable(
                                        identifier,
                                        Lists.newArrayList(
                                                SchemaChange.renameColumn(
                                                        "non_existing_col", "new_col2")),
                                        false))
                .satisfies(
                        anyCauseMatches(
                                Catalog.ColumnNotExistException.class,
                                "Column [non_existing_col] does not exist in the test_db.test_table table."));
    }

    @Test
    public void testAlterTableDropColumn() throws Exception {
        catalog.createDatabase("test_db", false);

        // Alter table drop a column in an existing table
        Identifier identifier = Identifier.create("test_db", "test_table");
        catalog.createTable(
                identifier,
                new Schema(
                        Lists.newArrayList(
                                new DataField(0, "col1", DataTypes.STRING()),
                                new DataField(1, "col2", DataTypes.STRING())),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Maps.newHashMap(),
                        ""),
                false);
        catalog.alterTable(identifier, Lists.newArrayList(SchemaChange.dropColumn("col1")), false);
        Table table = catalog.getTable(identifier);

        assertThat(table.rowType().getFields()).hasSize(1);
        assertThat(table.rowType().getFieldIndex("col1")).isLessThan(0);

        // Alter table drop all fields throws Exception
        assertThatThrownBy(
                        () ->
                                catalog.alterTable(
                                        identifier,
                                        Lists.newArrayList(SchemaChange.dropColumn("col2")),
                                        false))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class, "Cannot drop all fields in table"));

        // Alter table drop a column throws ColumnNotExistException when column does not exist
        assertThatThrownBy(
                        () ->
                                catalog.alterTable(
                                        identifier,
                                        Lists.newArrayList(
                                                SchemaChange.dropColumn("non_existing_col")),
                                        false))
                .satisfies(
                        anyCauseMatches(
                                Catalog.ColumnNotExistException.class,
                                "Column non_existing_col does not exist in the test_db.test_table table."));
    }

    @Test
    public void testAlterTableUpdateColumnType() throws Exception {
        catalog.createDatabase("test_db", false);

        // Alter table update a column type in an existing table
        Identifier identifier = Identifier.create("test_db", "test_table");
        catalog.createTable(
                identifier,
                new Schema(
                        Lists.newArrayList(
                                new DataField(0, "dt", DataTypes.STRING()),
                                new DataField(1, "col1", DataTypes.BIGINT(), "col1 field")),
                        Lists.newArrayList("dt"),
                        Collections.emptyList(),
                        Maps.newHashMap(),
                        ""),
                false);
        catalog.alterTable(
                identifier,
                Lists.newArrayList(SchemaChange.updateColumnType("col1", DataTypes.DOUBLE())),
                false);
        Table table = catalog.getTable(identifier);

        assertThat(table.rowType().getFieldIndex("col1")).isEqualTo(1);
        assertThat(table.rowType().getTypeAt(1)).isEqualTo(DataTypes.DOUBLE());
        assertThat(table.rowType().getFields().get(1).description()).isEqualTo("col1 field");

        // Alter table update a column type throws Exception when column data type does not support
        // cast
        assertThatThrownBy(
                        () ->
                                catalog.alterTable(
                                        identifier,
                                        Lists.newArrayList(
                                                SchemaChange.updateColumnType(
                                                        "col1", DataTypes.DATE())),
                                        false))
                .satisfies(
                        anyCauseMatches(
                                IllegalStateException.class,
                                "Column type col1[DOUBLE] cannot be converted to DATE without loosing information."));

        // Alter table update a column type throws ColumnNotExistException when column does not
        // exist
        assertThatThrownBy(
                        () ->
                                catalog.alterTable(
                                        identifier,
                                        Lists.newArrayList(
                                                SchemaChange.updateColumnType(
                                                        "non_existing_col", DataTypes.INT())),
                                        false))
                .satisfies(
                        anyCauseMatches(
                                Catalog.ColumnNotExistException.class,
                                "Column [non_existing_col] does not exist in the test_db.test_table table."));
        // Alter table update a column type throws Exception when column is partition columns
        assertThatThrownBy(
                        () ->
                                catalog.alterTable(
                                        identifier,
                                        Lists.newArrayList(
                                                SchemaChange.updateColumnType(
                                                        "dt", DataTypes.DATE())),
                                        false))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Cannot update partition column [dt] type in the table"));
    }

    @Test
    public void testAlterTableUpdateColumnComment() throws Exception {
        catalog.createDatabase("test_db", false);

        // Alter table update a column comment in an existing table
        Identifier identifier = Identifier.create("test_db", "test_table");
        catalog.createTable(
                identifier,
                new Schema(
                        Lists.newArrayList(
                                new DataField(0, "col1", DataTypes.STRING(), "field1"),
                                new DataField(1, "col2", DataTypes.STRING(), "field2"),
                                new DataField(
                                        2,
                                        "col3",
                                        DataTypes.ROW(
                                                new DataField(4, "f1", DataTypes.STRING(), "f1"),
                                                new DataField(5, "f2", DataTypes.STRING(), "f2"),
                                                new DataField(6, "f3", DataTypes.STRING(), "f3")),
                                        "field3")),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Maps.newHashMap(),
                        ""),
                false);

        catalog.alterTable(
                identifier,
                Lists.newArrayList(SchemaChange.updateColumnComment("col2", "col2 field")),
                false);

        // Update nested column
        String[] fields = new String[] {"col3", "f1"};
        catalog.alterTable(
                identifier,
                Lists.newArrayList(SchemaChange.updateColumnComment(fields, "col3 f1 field")),
                false);

        Table table = catalog.getTable(identifier);
        assertThat(table.rowType().getFields().get(1).description()).isEqualTo("col2 field");
        RowType rowType = (RowType) table.rowType().getFields().get(2).type();
        assertThat(rowType.getFields().get(0).description()).isEqualTo("col3 f1 field");

        // Alter table update a column comment throws Exception when column does not exist
        assertThatThrownBy(
                        () ->
                                catalog.alterTable(
                                        identifier,
                                        Lists.newArrayList(
                                                SchemaChange.updateColumnComment(
                                                        new String[] {"non_existing_col"}, "")),
                                        false))
                .satisfies(
                        anyCauseMatches(
                                Catalog.ColumnNotExistException.class,
                                "Column [non_existing_col] does not exist in the test_db.test_table table."));
    }

    @Test
    public void testAlterTableUpdateColumnNullability() throws Exception {
        catalog.createDatabase("test_db", false);

        // Alter table update a column nullability in an existing table
        Identifier identifier = Identifier.create("test_db", "test_table");
        catalog.createTable(
                identifier,
                new Schema(
                        Lists.newArrayList(
                                new DataField(0, "col1", DataTypes.STRING(), "field1"),
                                new DataField(1, "col2", DataTypes.STRING(), "field2"),
                                new DataField(
                                        2,
                                        "col3",
                                        DataTypes.ROW(
                                                new DataField(4, "f1", DataTypes.STRING(), "f1"),
                                                new DataField(5, "f2", DataTypes.STRING(), "f2"),
                                                new DataField(6, "f3", DataTypes.STRING(), "f3")),
                                        "field3")),
                        Lists.newArrayList("col1"),
                        Lists.newArrayList("col1", "col2"),
                        Maps.newHashMap(),
                        ""),
                false);

        catalog.alterTable(
                identifier,
                Lists.newArrayList(SchemaChange.updateColumnNullability("col1", false)),
                false);

        // Update nested column
        String[] fields = new String[] {"col3", "f1"};
        catalog.alterTable(
                identifier,
                Lists.newArrayList(SchemaChange.updateColumnNullability(fields, false)),
                false);

        Table table = catalog.getTable(identifier);
        assertThat(table.rowType().getFields().get(0).type().isNullable()).isEqualTo(false);

        // Alter table update a column nullability throws Exception when column does not exist
        assertThatThrownBy(
                        () ->
                                catalog.alterTable(
                                        identifier,
                                        Lists.newArrayList(
                                                SchemaChange.updateColumnNullability(
                                                        new String[] {"non_existing_col"}, false)),
                                        false))
                .satisfies(
                        anyCauseMatches(
                                Catalog.ColumnNotExistException.class,
                                "Column [non_existing_col] does not exist in the test_db.test_table table."));

        // Alter table update a column nullability throws Exception when column is pk columns
        assertThatThrownBy(
                        () ->
                                catalog.alterTable(
                                        identifier,
                                        Lists.newArrayList(
                                                SchemaChange.updateColumnNullability(
                                                        new String[] {"col2"}, true)),
                                        false))
                .satisfies(
                        anyCauseMatches(
                                UnsupportedOperationException.class,
                                "Cannot change nullability of primary key"));
    }

    @Test
    public void testAlterTableUpdateComment() throws Exception {
        catalog.createDatabase("test_db", false);

        Identifier identifier = Identifier.create("test_db", "test_table");
        catalog.createTable(
                identifier,
                new Schema(
                        Lists.newArrayList(
                                new DataField(0, "col1", DataTypes.STRING(), "field1"),
                                new DataField(1, "col2", DataTypes.STRING(), "field2")),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Maps.newHashMap(),
                        "comment"),
                false);

        catalog.alterTable(
                identifier, Lists.newArrayList(SchemaChange.updateComment("new comment")), false);

        Table table = catalog.getTable(identifier);
        assertThat(table.comment().isPresent() && table.comment().get().equals("new comment"))
                .isTrue();

        // drop comment
        catalog.alterTable(identifier, Lists.newArrayList(SchemaChange.updateComment(null)), false);

        table = catalog.getTable(identifier);
        assertThat(table.comment().isPresent()).isFalse();
    }
}
