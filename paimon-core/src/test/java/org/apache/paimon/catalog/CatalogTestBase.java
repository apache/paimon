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
import org.apache.paimon.PagedList;
import org.apache.paimon.TableType;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.format.HadoopCompressionType;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.ResolvingFileIO;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.view.View;
import org.apache.paimon.view.ViewImpl;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;
import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;
import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.METASTORE_PARTITIONED_TABLE;
import static org.apache.paimon.CoreOptions.METASTORE_TAG_TO_PARTITION;
import static org.apache.paimon.CoreOptions.TYPE;
import static org.apache.paimon.catalog.Catalog.SYSTEM_DATABASE_NAME;
import static org.apache.paimon.table.system.AllTableOptionsTable.ALL_TABLE_OPTIONS;
import static org.apache.paimon.table.system.CatalogOptionsTable.CATALOG_OPTIONS;
import static org.apache.paimon.table.system.SystemTableLoader.GLOBAL_SYSTEM_TABLES;
import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
        fileIO = new ResolvingFileIO();
        fileIO.configure(catalogContext);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (catalog != null) {
            List<String> dbs = catalog.listDatabases();
            for (String db : dbs) {
                try {
                    catalog.dropDatabase(db, true, true);
                } catch (Exception ignored) {
                }
            }
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
    public void testDuplicatedDatabaseAfterCreatingTable() throws Exception {
        catalog.createDatabase("test_db", false);
        Identifier identifier = Identifier.create("test_db", "new_table");
        Schema schema = Schema.newBuilder().column("pk1", DataTypes.INT()).build();
        catalog.createTable(identifier, schema, false);

        List<String> databases = catalog.listDatabases();
        List<String> distinctDatabases = databases.stream().distinct().collect(Collectors.toList());
        assertEquals(distinctDatabases.size(), databases.size());
    }

    @Test
    public void testCreateDatabase() throws Exception {
        // Create database creates a new database when it does not exist
        catalog.createDatabase("new_db", false);
        catalog.getDatabase("new_db");

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
    public void testAlterDatabase() throws Exception {
        if (!supportsAlterDatabase()) {
            return;
        }
        // Alter database
        String databaseName = "db_to_alter";
        catalog.createDatabase(databaseName, false);
        String key = "key1";
        String key2 = "key2";
        // Add property
        catalog.alterDatabase(
                databaseName,
                Lists.newArrayList(
                        PropertyChange.setProperty(key, "value"),
                        PropertyChange.setProperty(key2, "value")),
                false);
        Database db = catalog.getDatabase(databaseName);
        assertEquals("value", db.options().get(key));
        assertEquals("value", db.options().get(key2));
        // Update property
        catalog.alterDatabase(
                databaseName,
                Lists.newArrayList(
                        PropertyChange.setProperty(key, "value1"),
                        PropertyChange.setProperty(key2, "value1")),
                false);
        db = catalog.getDatabase(databaseName);
        assertEquals("value1", db.options().get(key));
        assertEquals("value1", db.options().get(key2));
        // remove property
        catalog.alterDatabase(
                databaseName,
                Lists.newArrayList(
                        PropertyChange.removeProperty(key), PropertyChange.removeProperty(key2)),
                false);
        db = catalog.getDatabase(databaseName);
        assertFalse(db.options().containsKey(key));
        assertFalse(db.options().containsKey(key2));
        // Remove non-existent property
        catalog.alterDatabase(
                databaseName,
                Lists.newArrayList(
                        PropertyChange.removeProperty(key), PropertyChange.removeProperty(key2)),
                false);
        db = catalog.getDatabase(databaseName);
        assertFalse(db.options().containsKey(key));
        assertFalse(db.options().containsKey(key2));
    }

    @Test
    public void testDropDatabase() throws Exception {
        // Drop database deletes the database when it exists and there are no tables
        catalog.createDatabase("db_to_drop", false);
        catalog.dropDatabase("db_to_drop", false, false);
        assertThatThrownBy(() -> catalog.getDatabase("db_to_drop"))
                .isInstanceOf(Catalog.DatabaseNotExistException.class);

        // Drop database does not throw exception when database does not exist and ignoreIfNotExists
        // is true
        assertThatCode(() -> catalog.dropDatabase("non_existing_db", true, false))
                .doesNotThrowAnyException();

        // Drop database deletes all tables in the database when cascade is true
        catalog.createDatabase("db_to_drop", false);
        catalog.createTable(Identifier.create("db_to_drop", "table1"), DEFAULT_TABLE_SCHEMA, false);
        catalog.createTable(Identifier.create("db_to_drop", "table2"), DEFAULT_TABLE_SCHEMA, false);

        catalog.dropDatabase("db_to_drop", false, true);
        assertThatThrownBy(() -> catalog.getDatabase("db_to_drop"))
                .isInstanceOf(Catalog.DatabaseNotExistException.class);

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

        // List tables throws DatabaseNotExistException when the database does not exist
        assertThatExceptionOfType(Catalog.DatabaseNotExistException.class)
                .isThrownBy(() -> catalog.listTables("non_existing_db"));
    }

    @Test
    public void testListTablesPaged() throws Exception {
        // List tables paged returns an empty list when there are no tables in the database
        String databaseName = "tables_paged_db";
        catalog.createDatabase(databaseName, false);
        PagedList<String> pagedTables =
                catalog.listTablesPaged(databaseName, null, null, null, null);
        assertThat(pagedTables.getElements()).isEmpty();
        assertNull(pagedTables.getNextPageToken());

        String[] tableNames = {"table1", "table2", "table3", "abd", "def", "opr"};
        for (String tableName : tableNames) {
            catalog.createTable(
                    Identifier.create(databaseName, tableName), DEFAULT_TABLE_SCHEMA, false);
        }

        // List tables paged returns a list with the names of all tables in the database in all
        // catalogs except RestCatalog
        // even if the maxResults or pageToken is not null
        pagedTables = catalog.listTablesPaged(databaseName, null, null, null, null);
        assertPagedTables(pagedTables, tableNames);

        int maxResults = 2;
        pagedTables = catalog.listTablesPaged(databaseName, maxResults, null, null, null);
        assertPagedTables(pagedTables, tableNames);

        String pageToken = "table1";
        pagedTables = catalog.listTablesPaged(databaseName, maxResults, pageToken, null, null);
        assertPagedTables(pagedTables, tableNames);

        maxResults = 8;
        pagedTables = catalog.listTablesPaged(databaseName, maxResults, null, null, null);
        assertPagedTables(pagedTables, tableNames);

        pagedTables = catalog.listTablesPaged(databaseName, maxResults, pageToken, null, null);
        assertPagedTables(pagedTables, tableNames);

        // List tables throws DatabaseNotExistException when the database does not exist
        final int finalMaxResults = maxResults;
        assertThatExceptionOfType(Catalog.DatabaseNotExistException.class)
                .isThrownBy(
                        () ->
                                catalog.listTablesPaged(
                                        "non_existing_db", finalMaxResults, pageToken, null, null));
    }

    @Test
    public void testListTableDetailsPaged() throws Exception {
        // List table details returns an empty list when there are no tables in the database
        String databaseName = "table_details_paged_db";
        catalog.createDatabase(databaseName, false);
        PagedList<Table> pagedTableDetails =
                catalog.listTableDetailsPaged(databaseName, null, null, null, null);
        assertThat(pagedTableDetails.getElements()).isEmpty();
        assertNull(pagedTableDetails.getNextPageToken());

        // List table details paged returns a list with all table in the database in all catalogs
        // except RestCatalog
        // even if the maxResults or pageToken is not null
        String[] tableNames = {"table1", "table2", "table3", "abd", "def", "opr"};
        for (String tableName : tableNames) {
            catalog.createTable(
                    Identifier.create(databaseName, tableName), DEFAULT_TABLE_SCHEMA, false);
        }

        pagedTableDetails = catalog.listTableDetailsPaged(databaseName, null, null, null, null);
        assertPagedTableDetails(pagedTableDetails, tableNames.length, tableNames);
        assertNull(pagedTableDetails.getNextPageToken());

        int maxResults = 2;
        pagedTableDetails =
                catalog.listTableDetailsPaged(databaseName, maxResults, null, null, null);
        assertPagedTableDetails(pagedTableDetails, tableNames.length, tableNames);
        assertNull(pagedTableDetails.getNextPageToken());

        String pageToken = "table1";
        pagedTableDetails =
                catalog.listTableDetailsPaged(databaseName, maxResults, pageToken, null, null);
        assertPagedTableDetails(pagedTableDetails, tableNames.length, tableNames);
        assertNull(pagedTableDetails.getNextPageToken());

        maxResults = 8;
        pagedTableDetails =
                catalog.listTableDetailsPaged(databaseName, maxResults, null, null, null);
        assertPagedTableDetails(pagedTableDetails, tableNames.length, tableNames);
        assertNull(pagedTableDetails.getNextPageToken());

        pagedTableDetails =
                catalog.listTableDetailsPaged(databaseName, maxResults, pageToken, null, null);
        assertPagedTableDetails(pagedTableDetails, tableNames.length, tableNames);
        assertNull(pagedTableDetails.getNextPageToken());

        // List table details throws DatabaseNotExistException when the database does not exist
        final int finalMaxResults = maxResults;
        assertThatExceptionOfType(Catalog.DatabaseNotExistException.class)
                .isThrownBy(
                        () ->
                                catalog.listTableDetailsPaged(
                                        "non_existing_db", finalMaxResults, pageToken, null, null));
    }

    @Test
    public void testListTablesPagedGlobally() throws Exception {
        // List table paged globally throws UnsupportedOperationException if current catalog does
        // not
        // supportsListObjectsPaged or current catalog does not supportsListByPattern
        String databaseName = "list_tables_paged_globally_db";
        catalog.createDatabase(databaseName, false);
        if (!catalog.supportsListObjectsPaged() || !catalog.supportsListByPattern()) {
            Assertions.assertThrows(
                    UnsupportedOperationException.class,
                    () -> catalog.listTablesPagedGlobally(databaseName, null, null, null));
        }

        String[] tableNames = {"table1", "table2", "table3", "abd", "def", "opr"};
        for (String tableName : tableNames) {
            catalog.createTable(
                    Identifier.create(databaseName, tableName), DEFAULT_TABLE_SCHEMA, false);
        }

        if (!catalog.supportsListObjectsPaged() || !catalog.supportsListByPattern()) {
            Assertions.assertThrows(
                    UnsupportedOperationException.class,
                    () -> catalog.listTablesPagedGlobally(null, null, null, null));
            Assertions.assertThrows(
                    UnsupportedOperationException.class,
                    () -> catalog.listTablesPagedGlobally(databaseName, null, null, null));
            Assertions.assertThrows(
                    UnsupportedOperationException.class,
                    () -> catalog.listTablesPagedGlobally(null, null, 100, null));
            Assertions.assertThrows(
                    UnsupportedOperationException.class,
                    () -> catalog.listTablesPagedGlobally(databaseName, "abc", null, null));
            Assertions.assertThrows(
                    UnsupportedOperationException.class,
                    () -> catalog.listTablesPagedGlobally(databaseName, "abc", null, "table"));
        }
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

        // Create table throws Exception when type = format-table.
        if (supportsFormatTable()) {
            schema.options().put(CoreOptions.TYPE.key(), TableType.FORMAT_TABLE.toString());
            schema.options().put(CoreOptions.PRIMARY_KEY.key(), "a");
            assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(() -> catalog.createTable(identifier, schema, false))
                    .withMessage("Cannot define primary-key for format table.");
            schema.options().remove(CoreOptions.TYPE.key());
            schema.options().remove(CoreOptions.PRIMARY_KEY.key());
        }

        // Create table and check the schema
        schema.options().put("k1", "v1");
        catalog.createTable(identifier, schema, false);
        FileStoreTable dataTable = (FileStoreTable) catalog.getTable(identifier);
        assertThat(dataTable.schema().toSchema().fields()).isEqualTo(schema.fields());
        assertThat(dataTable.schema().toSchema().partitionKeys()).isEqualTo(schema.partitionKeys());
        assertThat(dataTable.schema().toSchema().comment()).isEqualTo(schema.comment());
        assertThat(dataTable.schema().toSchema().primaryKeys()).isEqualTo(schema.primaryKeys());
        for (Map.Entry<String, String> option : schema.options().entrySet()) {
            assertThat(dataTable.options().get(option.getKey())).isEqualTo(option.getValue());
        }

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
        // Create table throws IleaArgumentException when some table options are not set correctly
        schema.options()
                .put(
                        CoreOptions.MERGE_ENGINE.key(),
                        CoreOptions.MergeEngine.DEDUPLICATE.toString());
        schema.options().put(CoreOptions.IGNORE_DELETE.key(), "max");
        assertThatCode(
                        () ->
                                catalog.createTable(
                                        Identifier.create("test_db", "wrong_table"), schema, false))
                .hasRootCauseInstanceOf(IllegalArgumentException.class);

        // conflict options
        Schema conflictOptionsSchema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .options(ImmutableMap.of("changelog-producer", "input"))
                        .build();
        assertThatThrownBy(
                        () ->
                                catalog.createTable(
                                        Identifier.create("test_db", "conflict_options_table"),
                                        conflictOptionsSchema,
                                        false))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    public void testFormatTableFileCompression() throws Exception {
        if (!supportsFormatTable()) {
            return;
        }
        String dbName = "test_format_table_file_compression";
        catalog.createDatabase(dbName, true);
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f1", DataTypes.INT());
        schemaBuilder.option("type", "format-table");
        Pair[] format2ExpectDefaultFileCompression = {
            Pair.of("csv", "none"),
            Pair.of("parquet", "snappy"),
            Pair.of("json", "none"),
            Pair.of("orc", "zstd")
        };
        for (Pair<String, String> format2Compression : format2ExpectDefaultFileCompression) {
            Identifier identifier =
                    Identifier.create(
                            dbName,
                            "partition_table_file_compression_" + format2Compression.getKey());
            schemaBuilder.option("file.format", format2Compression.getKey());
            catalog.createTable(identifier, schemaBuilder.build(), true);
            String fileCompression =
                    new CoreOptions(catalog.getTable(identifier).options())
                            .formatTableFileCompression();

            assertEquals(fileCompression, format2Compression.getValue());
        }
        // table has option file.compression
        String expectFileCompression = "gzip";
        schemaBuilder.option("file.format", "csv");
        schemaBuilder.option("file.compression", expectFileCompression);
        Identifier identifier = Identifier.create(dbName, "partition_table_file_compression_a");
        catalog.createTable(identifier, schemaBuilder.build(), true);
        String fileCompression =
                new CoreOptions(catalog.getTable(identifier).options())
                        .formatTableFileCompression();
        assertEquals(fileCompression, expectFileCompression);

        // table has option format-table.file.compression
        schemaBuilder.option("format-table.file.compression", expectFileCompression);
        identifier = Identifier.create(dbName, "partition_table_file_compression_b");
        catalog.createTable(identifier, schemaBuilder.build(), true);
        fileCompression =
                new CoreOptions(catalog.getTable(identifier).options())
                        .formatTableFileCompression();
        assertEquals(fileCompression, expectFileCompression);
    }

    @Test
    public void testFormatTableOnlyPartitionValueRead() throws Exception {
        if (!supportsFormatTable()) {
            return;
        }
        Random random = new Random();
        String dbName = "test_db";
        catalog.createDatabase(dbName, true);
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f1", DataTypes.INT());
        schemaBuilder.column("f2", DataTypes.INT());
        schemaBuilder.column("dt", DataTypes.INT());
        schemaBuilder.column("dt2", DataTypes.VARCHAR(64));
        schemaBuilder.partitionKeys("dt", "dt2");
        schemaBuilder.option("type", "format-table");
        schemaBuilder.option("format-table.partition-path-only-value", "true");
        Pair[] format2Compressions = {
            Pair.of("csv", HadoopCompressionType.GZIP),
            Pair.of("parquet", HadoopCompressionType.ZSTD),
            Pair.of("json", HadoopCompressionType.GZIP),
            Pair.of("orc", HadoopCompressionType.ZSTD)
        };
        int dtPartitionValue = 10;
        String dt2PartitionValue = "2022-01-01";
        for (Pair<String, HadoopCompressionType> format2Compression : format2Compressions) {
            Identifier identifier =
                    Identifier.create(dbName, "partition_table_" + format2Compression.getKey());
            schemaBuilder.option("file.compression", format2Compression.getValue().value());
            schemaBuilder.option("file.format", format2Compression.getKey());
            catalog.createTable(identifier, schemaBuilder.build(), true);
            FormatTable table = (FormatTable) catalog.getTable(identifier);
            int size = 5;
            InternalRow[] datas = new InternalRow[size];
            for (int j = 0; j < size; j++) {
                datas[j] =
                        GenericRow.of(
                                random.nextInt(),
                                random.nextInt(),
                                dtPartitionValue,
                                BinaryString.fromString(dt2PartitionValue));
            }
            writeAndCheckCommitFormatTable(table, datas, null);
            List<InternalRow> readAllData = read(table, null, null, null, null);
            assertThat(readAllData).containsExactlyInAnyOrder(datas);
            Map<String, String> partitionSpec = new HashMap<>();
            partitionSpec.put("dt", "" + dtPartitionValue + 1);
            partitionSpec.put("dt2", dt2PartitionValue + 1);
            List<InternalRow> readFilterData = read(table, null, null, partitionSpec, null);
            assertThat(readFilterData).isEmpty();
            catalog.dropTable(identifier, true);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testFormatTableReadAndWrite(boolean partitioned) throws Exception {
        if (!supportsFormatTable()) {
            return;
        }
        Random random = new Random();
        String dbName = "test_db";
        catalog.createDatabase(dbName, true);
        int partitionValue = 10;
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f1", DataTypes.INT());
        schemaBuilder.column("f2", DataTypes.INT());
        schemaBuilder.column("dt", DataTypes.INT());
        schemaBuilder.option("type", "format-table");
        schemaBuilder.option("target-file-size", "1 kb");
        Pair[] format2Compressions = {
            Pair.of("csv", HadoopCompressionType.GZIP),
            Pair.of("parquet", HadoopCompressionType.ZSTD),
            Pair.of("json", HadoopCompressionType.GZIP),
            Pair.of("orc", HadoopCompressionType.ZSTD)
        };
        for (Pair<String, HadoopCompressionType> format2Compression : format2Compressions) {
            if (partitioned) {
                schemaBuilder.partitionKeys("dt");
            }
            Identifier identifier =
                    Identifier.create(dbName, "table_" + format2Compression.getKey());
            schemaBuilder.option("file.format", format2Compression.getKey());
            schemaBuilder.option("file.compression", format2Compression.getValue().value());
            catalog.createTable(identifier, schemaBuilder.build(), true);
            FormatTable table = (FormatTable) catalog.getTable(identifier);
            int[] projection = new int[] {1, 2};
            PredicateBuilder builder = new PredicateBuilder(table.rowType());
            Predicate predicate = builder.greaterOrEqual(1, 10);
            int size = 2000;
            int checkSize = 3;
            InternalRow[] datas = new InternalRow[size];
            InternalRow[] checkDatas = new InternalRow[checkSize];
            for (int j = 0; j < size; j++) {
                int f1 = random.nextInt();
                int f2 = j < checkSize ? random.nextInt(10) + 10 : random.nextInt(10);
                datas[j] = GenericRow.of(f1, f2, partitionValue);
                if (j < checkSize) {
                    checkDatas[j] = GenericRow.of(f2, partitionValue);
                }
            }
            InternalRow dataWithDiffPartition =
                    GenericRow.of(random.nextInt(), random.nextInt(), 11);
            Map<String, String> partitionSpec = null;
            int dataSize = size;
            if (partitioned) {
                writeAndCheckCommitFormatTable(table, datas, dataWithDiffPartition);
                dataSize = size + 1;
                partitionSpec = new HashMap<>();
                partitionSpec.put("dt", "" + partitionValue);
            } else {
                writeAndCheckCommitFormatTable(table, datas, null);
            }
            List<InternalRow> readFilterData =
                    read(table, predicate, projection, partitionSpec, null);
            assertThat(readFilterData).containsExactlyInAnyOrder(checkDatas);
            List<InternalRow> readallData = read(table, null, null, null, null);
            assertThat(readallData).hasSize(dataSize);
            int limit = checkSize - 1;
            List<InternalRow> readLimitData =
                    read(table, predicate, projection, partitionSpec, limit);
            assertThat(readLimitData).hasSize(limit);
            if (partitioned) {
                List<InternalRow> readAllData = read(table, null, null, null, null);
                assertThat(readAllData).hasSize(size + 1);
                PredicateBuilder partitionFilterBuilder = new PredicateBuilder(table.rowType());
                Predicate partitionFilterPredicate =
                        partitionFilterBuilder.equal(2, partitionValue);
                List<InternalRow> readPartitionAndNoPartitionFilterData =
                        read(table, partitionFilterPredicate, projection, null, null);
                assertThat(readPartitionAndNoPartitionFilterData).hasSize(size);
            }
            catalog.dropTable(identifier, true);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testFormatTableOverwrite(boolean partitionPathOnlyValue) throws Exception {
        if (!supportsFormatTable()) {
            return;
        }
        String dbName = "format_overwrite_db";
        catalog.createDatabase(dbName, true);

        Identifier id = Identifier.create(dbName, "format_overwrite_table");
        Schema nonPartitionedSchema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .options(getFormatTableOptions())
                        .option("file.format", "csv")
                        .option("file.compression", HadoopCompressionType.GZIP.value())
                        .option(
                                "format-table.partition-path-only-value",
                                "" + partitionPathOnlyValue)
                        .build();
        catalog.createTable(id, nonPartitionedSchema, true);
        FormatTable nonPartitionedTable = (FormatTable) catalog.getTable(id);
        BatchWriteBuilder nonPartitionedTableWriteBuilder =
                nonPartitionedTable.newBatchWriteBuilder();
        try (BatchTableWrite write = nonPartitionedTableWriteBuilder.newWrite();
                BatchTableCommit commit = nonPartitionedTableWriteBuilder.newCommit()) {
            write.write(GenericRow.of(1, 10));
            write.write(GenericRow.of(2, 20));
            commit.commit(write.prepareCommit());
        }

        try (BatchTableWrite write = nonPartitionedTableWriteBuilder.newWrite();
                BatchTableCommit commit =
                        nonPartitionedTableWriteBuilder.withOverwrite().newCommit()) {
            write.write(GenericRow.of(3, 30));
            commit.commit(write.prepareCommit());
        }

        List<InternalRow> fullOverwriteRows = read(nonPartitionedTable, null, null, null, null);
        assertThat(fullOverwriteRows).containsExactlyInAnyOrder(GenericRow.of(3, 30));
        catalog.dropTable(id, true);

        Identifier pid = Identifier.create(dbName, "format_overwrite_partitioned");
        Schema partitionedSchema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .column("year", DataTypes.INT())
                        .column("month", DataTypes.INT())
                        .partitionKeys("year", "month")
                        .options(getFormatTableOptions())
                        .option("file.format", "csv")
                        .option("file.compression", HadoopCompressionType.GZIP.value())
                        .option(
                                "format-table.partition-path-only-value",
                                "" + partitionPathOnlyValue)
                        .build();
        catalog.createTable(pid, partitionedSchema, true);
        FormatTable partitionedTable = (FormatTable) catalog.getTable(pid);
        BatchWriteBuilder partitionedTableWriteBuilder = partitionedTable.newBatchWriteBuilder();
        try (BatchTableWrite write = partitionedTableWriteBuilder.newWrite();
                BatchTableCommit commit = partitionedTableWriteBuilder.newCommit()) {
            write.write(GenericRow.of(1, 100, 2024, 10));
            write.write(GenericRow.of(2, 200, 2025, 10));
            write.write(GenericRow.of(3, 300, 2025, 11));
            commit.commit(write.prepareCommit());
        }

        Map<String, String> staticPartition = new HashMap<>();
        staticPartition.put("year", "2024");
        staticPartition.put("month", "10");
        try (BatchTableWrite write = partitionedTableWriteBuilder.newWrite();
                BatchTableCommit commit =
                        partitionedTableWriteBuilder.withOverwrite(staticPartition).newCommit()) {
            write.write(GenericRow.of(10, 1000, 2024, 10));
            commit.commit(write.prepareCommit());
        }

        List<InternalRow> partitionOverwriteRows = read(partitionedTable, null, null, null, null);
        assertThat(partitionOverwriteRows)
                .containsExactlyInAnyOrder(
                        GenericRow.of(10, 1000, 2024, 10),
                        GenericRow.of(2, 200, 2025, 10),
                        GenericRow.of(3, 300, 2025, 11));

        staticPartition = new HashMap<>();
        staticPartition.put("year", "2025");
        try (BatchTableWrite write = partitionedTableWriteBuilder.newWrite();
                BatchTableCommit commit =
                        partitionedTableWriteBuilder.withOverwrite(staticPartition).newCommit()) {
            write.write(GenericRow.of(10, 1000, 2025, 10));
            commit.commit(write.prepareCommit());
        }

        partitionOverwriteRows = read(partitionedTable, null, null, null, null);
        assertThat(partitionOverwriteRows)
                .containsExactlyInAnyOrder(
                        GenericRow.of(10, 1000, 2024, 10), GenericRow.of(10, 1000, 2025, 10));

        try (BatchTableWrite write = partitionedTableWriteBuilder.newWrite()) {
            write.write(GenericRow.of(10, 1000, 2025, 10));
            assertThrows(
                    RuntimeException.class,
                    () -> {
                        Map<String, String> staticOverwritePartition = new HashMap<>();
                        staticOverwritePartition.put("month", "10");
                        partitionedTableWriteBuilder
                                .withOverwrite(staticOverwritePartition)
                                .newCommit();
                    });
        }
        catalog.dropTable(pid, true);
    }

    @Test
    public void testFormatTableSplitRead() throws Exception {
        if (!supportsFormatTable()) {
            return;
        }
        Pair[] format2Compressions = {
            Pair.of("csv", HadoopCompressionType.NONE),
            Pair.of("json", HadoopCompressionType.NONE),
            Pair.of("csv", HadoopCompressionType.GZIP),
            Pair.of("json", HadoopCompressionType.GZIP),
            Pair.of("parquet", HadoopCompressionType.ZSTD)
        };
        for (Pair<String, HadoopCompressionType> format2Compression : format2Compressions) {
            String format = format2Compression.getKey();
            String compression = format2Compression.getValue().value();
            String dbName = format + "_split_db_" + compression;
            catalog.createDatabase(dbName, true);

            Identifier id = Identifier.create(dbName, format + "_split_table_" + compression);
            Schema schema =
                    Schema.newBuilder()
                            .column("id", DataTypes.INT())
                            .column("name", DataTypes.STRING())
                            .column("score", DataTypes.DOUBLE())
                            .options(getFormatTableOptions())
                            .option("file.format", format)
                            .option("source.split.target-size", "54 B")
                            .option("file.compression", compression.toString())
                            .build();
            catalog.createTable(id, schema, true);
            FormatTable table = (FormatTable) catalog.getTable(id);
            int size = 50;
            InternalRow[] datas = new InternalRow[size];
            for (int i = 0; i < size; i++) {
                datas[i] = GenericRow.of(i, BinaryString.fromString("User" + i), 85.5 + (i % 15));
            }
            writeAndCheckCommitFormatTable(table, datas, null);
            List<InternalRow> allRows = read(table, null, null, null, null);
            assertThat(allRows).containsExactlyInAnyOrder(datas);
        }
    }

    private void writeAndCheckCommitFormatTable(
            FormatTable table, InternalRow[] datas, InternalRow dataWithDiffPartition)
            throws Exception {
        try (BatchTableWrite write = table.newBatchWriteBuilder().newWrite();
                BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            for (InternalRow row : datas) {
                write.write(row);
            }
            if (dataWithDiffPartition != null) {
                write.write(dataWithDiffPartition);
            }
            List<CommitMessage> committers = write.prepareCommit();
            List<InternalRow> readData = read(table, null, null, null, null);
            assertThat(readData).isEmpty();
            commit.commit(committers);
        }
    }

    @SafeVarargs
    protected final List<InternalRow> read(
            Table table,
            Predicate predicate,
            @Nullable int[] projection,
            @Nullable Map<String, String> partitionSpec,
            @Nullable Integer limit,
            Pair<ConfigOption<?>, String>... dynamicOptions)
            throws Exception {
        Map<String, String> options = new HashMap<>();
        for (Pair<ConfigOption<?>, String> pair : dynamicOptions) {
            options.put(pair.getKey().key(), pair.getValue());
        }
        table = table.copy(options);
        ReadBuilder readBuilder = table.newReadBuilder();
        if (projection != null) {
            readBuilder.withProjection(projection);
        }
        readBuilder.withPartitionFilter(partitionSpec);
        readBuilder.withFilter(predicate);
        if (limit != null) {
            readBuilder.withLimit(limit);
        }
        TableScan scan = readBuilder.newScan();
        try (RecordReader<InternalRow> reader =
                readBuilder.newRead().executeFilter().createReader(scan.plan())) {
            InternalRowSerializer serializer = new InternalRowSerializer(readBuilder.readType());
            List<InternalRow> rows = new ArrayList<>();
            reader.forEachRemaining(
                    row -> {
                        rows.add(serializer.copy(row));
                    });
            return rows;
        }
    }

    @Test
    public void testGetTable() throws Exception {
        catalog.createDatabase("test_db", false);

        // Get system and data table when the table exists
        Identifier identifier = Identifier.create("test_db", "test_table");
        catalog.createTable(identifier, DEFAULT_TABLE_SCHEMA, false);
        Table systemTable = catalog.getTable(Identifier.create("test_db", "test_table$snapshots"));
        assertThat(systemTable).isNotNull();
        Table systemTableCheckWithBranch =
                catalog.getTable(new Identifier("test_db", "test_table", "main", "snapshots"));
        assertThat(systemTableCheckWithBranch).isNotNull();
        Table dataTable = catalog.getTable(identifier);
        assertThat(dataTable).isNotNull();

        // Get manifests system table and read it
        Table table = catalog.getTable(Identifier.create("test_db", "test_table"));
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(
                    GenericRow.of(1, BinaryString.fromString("2"), BinaryString.fromString("3")));
            commit.commit(write.prepareCommit());
        }
        Table manifestsTable =
                catalog.getTable(Identifier.create("test_db", "test_table$manifests"));
        ReadBuilder readBuilder = manifestsTable.newReadBuilder();
        List<String> manifestFiles = new ArrayList<>();
        readBuilder
                .newRead()
                .createReader(readBuilder.newScan().plan())
                .forEachRemaining(r -> manifestFiles.add(r.getString(0).toString()));
        assertThat(manifestFiles).hasSize(1);

        // Get system table throws TableNotExistException when data table does not exist
        assertThatExceptionOfType(Catalog.TableNotExistException.class)
                .isThrownBy(
                        () ->
                                catalog.getTable(
                                        Identifier.create(
                                                "test_db", "non_existing_table$snapshots")))
                .withMessage("Table test_db.non_existing_table$snapshots does not exist.");

        // Get system table throws TableNotExistException when system table type does not exist
        assertThatExceptionOfType(Catalog.TableNotExistException.class)
                .isThrownBy(
                        () ->
                                catalog.getTable(
                                        Identifier.create("test_db", "non_existing_table$schema1")))
                .withMessage("Table test_db.non_existing_table$schema1 does not exist.");

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

        Table allTableOptionsTable =
                catalog.getTable(Identifier.create(SYSTEM_DATABASE_NAME, ALL_TABLE_OPTIONS));
        assertThat(allTableOptionsTable).isNotNull();
        Table catalogOptionsTable =
                catalog.getTable(Identifier.create(SYSTEM_DATABASE_NAME, CATALOG_OPTIONS));
        assertThat(catalogOptionsTable).isNotNull();
        assertThatExceptionOfType(Catalog.TableNotExistException.class)
                .isThrownBy(
                        () -> catalog.getTable(Identifier.create(SYSTEM_DATABASE_NAME, "1111")));

        List<String> sysTables = catalog.listTables(SYSTEM_DATABASE_NAME);
        assertThat(sysTables).containsAll(GLOBAL_SYSTEM_TABLES);

        assertThat(catalog.listViews(SYSTEM_DATABASE_NAME)).isEmpty();
    }

    @Test
    public void testDropTable() throws Exception {
        catalog.createDatabase("test_db", false);

        // Drop table deletes the table when it exists
        Identifier identifier = Identifier.create("test_db", "table_to_drop");
        catalog.createTable(identifier, DEFAULT_TABLE_SCHEMA, false);
        catalog.dropTable(identifier, false);
        assertThatThrownBy(() -> catalog.getTable(identifier))
                .isInstanceOf(Catalog.TableNotExistException.class);

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
        assertThatThrownBy(() -> catalog.getTable(fromTable))
                .isInstanceOf(Catalog.TableNotExistException.class);
        catalog.getTable(toTable);

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
    public void testAlterDataTable() throws Exception {
        baseAlterTable(Maps.newHashMap());
    }

    @Test
    public void testAlterMaterializedTable() throws Exception {
        Map<String, String> initOptions = Maps.newHashMap();
        initOptions.put(CoreOptions.TYPE.key(), TableType.MATERIALIZED_TABLE.toString());
        baseAlterTable(initOptions);
    }

    @Test
    public void testView() throws Exception {
        if (!supportsView()) {
            return;
        }
        Identifier identifier = new Identifier("view_db", "my_view");
        View view = createView(identifier);

        assertThatThrownBy(() -> catalog.createView(identifier, view, false))
                .isInstanceOf(Catalog.DatabaseNotExistException.class);

        assertThatThrownBy(() -> catalog.listViews(identifier.getDatabaseName()))
                .isInstanceOf(Catalog.DatabaseNotExistException.class);

        catalog.createDatabase(identifier.getDatabaseName(), false);

        assertThatThrownBy(() -> catalog.getView(identifier))
                .isInstanceOf(Catalog.ViewNotExistException.class);

        catalog.createView(identifier, view, false);

        View catalogView = catalog.getView(identifier);
        assertThat(catalogView.fullName()).isEqualTo(view.fullName());
        assertThat(catalogView.rowType()).isEqualTo(view.rowType());
        assertThat(catalogView.query()).isEqualTo(view.query());
        assertThat(catalogView.dialects()).isEqualTo(view.dialects());
        assertThat(catalogView.comment()).isEqualTo(view.comment());
        assertThat(catalogView.options()).containsAllEntriesOf(view.options());

        List<String> views = catalog.listViews(identifier.getDatabaseName());
        assertThat(views).containsOnly(identifier.getObjectName());

        catalog.createView(identifier, view, true);
        assertThatThrownBy(() -> catalog.createView(identifier, view, false))
                .isInstanceOf(Catalog.ViewAlreadyExistException.class);

        Identifier newIdentifier = new Identifier("view_db", "new_view");
        catalog.renameView(new Identifier("view_db", "unknown"), newIdentifier, true);
        assertThatThrownBy(
                        () ->
                                catalog.renameView(
                                        new Identifier("view_db", "unknown"), newIdentifier, false))
                .isInstanceOf(Catalog.ViewNotExistException.class);
        catalog.renameView(identifier, newIdentifier, false);

        catalog.dropView(newIdentifier, true);
        assertThatThrownBy(() -> catalog.dropView(newIdentifier, false))
                .isInstanceOf(Catalog.ViewNotExistException.class);
    }

    @Test
    public void testListViewsPaged() throws Exception {
        if (!supportsView()) {
            return;
        }

        // List views returns an empty list when there are no views in the database
        String databaseName = "views_paged_db";
        catalog.createDatabase(databaseName, false);
        PagedList<String> pagedViews = catalog.listViewsPaged(databaseName, null, null, null);
        assertThat(pagedViews.getElements()).isEmpty();
        assertNull(pagedViews.getNextPageToken());

        // List views paged returns a list with the names of all views in the database in all
        // catalogs except RestCatalog
        // even if the maxResults or pageToken is not null
        View view = buildView(databaseName);
        String[] viewNames = {"view1", "view2", "view3", "abd", "def", "opr"};
        for (String viewName : viewNames) {
            catalog.createView(Identifier.create(databaseName, viewName), view, false);
        }

        pagedViews = catalog.listViewsPaged(databaseName, null, null, null);
        assertPagedViews(pagedViews, viewNames);

        int maxResults = 2;
        pagedViews = catalog.listViewsPaged(databaseName, maxResults, null, null);
        assertPagedViews(pagedViews, viewNames);

        String pageToken = "view1";
        pagedViews = catalog.listViewsPaged(databaseName, maxResults, pageToken, null);
        assertPagedViews(pagedViews, viewNames);

        maxResults = 8;
        pagedViews = catalog.listViewsPaged(databaseName, maxResults, null, null);
        assertPagedViews(pagedViews, viewNames);

        pagedViews = catalog.listViewsPaged(databaseName, maxResults, pageToken, null);
        assertPagedViews(pagedViews, viewNames);

        // List views throws DatabaseNotExistException when the database does not exist
        final int finalMaxResults = maxResults;
        assertThatExceptionOfType(Catalog.DatabaseNotExistException.class)
                .isThrownBy(
                        () ->
                                catalog.listViewsPaged(
                                        "non_existing_db", finalMaxResults, pageToken, null));

        assertThatExceptionOfType(UnsupportedOperationException.class)
                .isThrownBy(
                        () ->
                                catalog.listViewsPaged(
                                        databaseName, finalMaxResults, pageToken, "view%"));
    }

    @Test
    public void testListViewDetailsPaged() throws Exception {
        if (!supportsView()) {
            return;
        }

        // List views returns an empty list when there are no views in the database
        String databaseName = "view_details_paged_db";
        catalog.createDatabase(databaseName, false);
        PagedList<View> pagedViewDetailsPaged =
                catalog.listViewDetailsPaged(databaseName, null, null, null);
        assertThat(pagedViewDetailsPaged.getElements()).isEmpty();
        assertNull(pagedViewDetailsPaged.getNextPageToken());

        // List view details paged returns a list with all view in the database in all catalogs
        // except RestCatalog
        // even if the maxResults or pageToken is not null
        View view = buildView(databaseName);
        String[] viewNames = {"view1", "view2", "view3", "abd", "def", "opr"};
        for (String viewName : viewNames) {
            catalog.createView(Identifier.create(databaseName, viewName), view, false);
        }

        pagedViewDetailsPaged = catalog.listViewDetailsPaged(databaseName, null, null, null);
        assertPagedViewDetails(pagedViewDetailsPaged, view, viewNames.length, viewNames);
        assertNull(pagedViewDetailsPaged.getNextPageToken());

        int maxResults = 2;
        pagedViewDetailsPaged = catalog.listViewDetailsPaged(databaseName, maxResults, null, null);
        assertPagedViewDetails(pagedViewDetailsPaged, view, viewNames.length, viewNames);
        assertNull(pagedViewDetailsPaged.getNextPageToken());

        String pageToken = "view1";
        pagedViewDetailsPaged =
                catalog.listViewDetailsPaged(databaseName, maxResults, pageToken, null);
        assertPagedViewDetails(pagedViewDetailsPaged, view, viewNames.length, viewNames);
        assertNull(pagedViewDetailsPaged.getNextPageToken());

        maxResults = 8;
        pagedViewDetailsPaged = catalog.listViewDetailsPaged(databaseName, maxResults, null, null);
        assertPagedViewDetails(pagedViewDetailsPaged, view, viewNames.length, viewNames);
        assertNull(pagedViewDetailsPaged.getNextPageToken());

        pagedViewDetailsPaged =
                catalog.listViewDetailsPaged(databaseName, maxResults, pageToken, null);
        assertPagedViewDetails(pagedViewDetailsPaged, view, viewNames.length, viewNames);
        assertNull(pagedViewDetailsPaged.getNextPageToken());

        // List view details throws DatabaseNotExistException when the database does not exist
        final int finalMaxResults = maxResults;
        assertThatExceptionOfType(Catalog.DatabaseNotExistException.class)
                .isThrownBy(
                        () ->
                                catalog.listViewDetailsPaged(
                                        "non_existing_db", finalMaxResults, pageToken, null));
    }

    @Test
    public void testListViewsPagedGlobally() throws Exception {
        if (!supportsView()) {
            return;
        }

        // List view paged globally throws UnsupportedOperationException if current catalog does not
        // supportsListObjectsPaged or odes not supportsListByPattern
        String databaseName = "list_views_paged_globally_db";
        catalog.createDatabase(databaseName, false);
        if (!catalog.supportsListObjectsPaged() || !catalog.supportsListByPattern()) {
            Assertions.assertThrows(
                    UnsupportedOperationException.class,
                    () -> catalog.listViewsPagedGlobally(databaseName, null, null, null));
        }

        View view = buildView(databaseName);
        String[] viewNames = {"view1", "view2", "view3", "abd", "def", "opr"};
        for (String viewName : viewNames) {
            catalog.createView(Identifier.create(databaseName, viewName), view, false);
        }

        if (!catalog.supportsListObjectsPaged() || !catalog.supportsListByPattern()) {
            Assertions.assertThrows(
                    UnsupportedOperationException.class,
                    () -> catalog.listViewsPagedGlobally(null, null, null, null));
            Assertions.assertThrows(
                    UnsupportedOperationException.class,
                    () -> catalog.listViewsPagedGlobally(databaseName, null, null, null));
            Assertions.assertThrows(
                    UnsupportedOperationException.class,
                    () -> catalog.listViewsPagedGlobally(null, null, 100, null));
            Assertions.assertThrows(
                    UnsupportedOperationException.class,
                    () -> catalog.listViewsPagedGlobally(databaseName, "abc", null, null));
            Assertions.assertThrows(
                    UnsupportedOperationException.class,
                    () -> catalog.listViewsPagedGlobally(databaseName, "abc", null, "view"));
        }
    }

    @Test
    public void testFormatTable() throws Exception {
        if (!supportsFormatTable()) {
            return;
        }

        Identifier identifier = new Identifier("format_db", "my_format");
        catalog.createDatabase(identifier.getDatabaseName(), false);

        // create table
        Schema schema =
                Schema.newBuilder()
                        .column("str", DataTypes.STRING())
                        .column("int", DataTypes.INT())
                        .options(getFormatTableOptions())
                        .option("file.format", "csv")
                        .option("remove-key", "value")
                        .build();
        catalog.createTable(identifier, schema, false);
        assertThat(catalog.listTables(identifier.getDatabaseName()))
                .contains(identifier.getTableName());
        assertThat(catalog.getTable(identifier)).isInstanceOf(FormatTable.class);

        // alter table
        if (supportsAlterFormatTable()) {
            baseAlterTable(getFormatTableOptions());
        } else {
            SchemaChange schemaChange = SchemaChange.addColumn("new_col", DataTypes.STRING());
            assertThatThrownBy(() -> catalog.alterTable(identifier, schemaChange, false))
                    .hasMessageContaining("Only data table support alter table.");
        }

        // drop table
        catalog.dropTable(identifier, false);
        assertThatThrownBy(() -> catalog.getTable(identifier))
                .isInstanceOf(Catalog.TableNotExistException.class);

        // rename table
        catalog.createTable(identifier, schema, false);
        Identifier newIdentifier = new Identifier("format_db", "new_format");
        catalog.renameTable(identifier, newIdentifier, false);
        assertThatThrownBy(() -> catalog.getTable(identifier))
                .isInstanceOf(Catalog.TableNotExistException.class);
        assertThat(catalog.getTable(newIdentifier)).isInstanceOf(FormatTable.class);
    }

    @Test
    public void testTableUUID() throws Exception {
        catalog.createDatabase("test_db", false);
        Identifier identifier = Identifier.create("test_db", "test_table");
        catalog.createTable(identifier, DEFAULT_TABLE_SCHEMA, false);
        Table table = catalog.getTable(identifier);
        String uuid = table.uuid();
        assertThat(uuid).startsWith(identifier.getFullName() + ".");
        assertThat(Long.parseLong(uuid.substring((identifier.getFullName() + ".").length())))
                .isGreaterThan(0);
    }

    @Test
    public void testPartitions() throws Exception {
        if (!supportPartitions()) {
            return;
        }
        String databaseName = "testPartitionTable";
        List<Map<String, String>> partitionSpecs =
                Arrays.asList(
                        Collections.singletonMap("dt", "20250101"),
                        Collections.singletonMap("dt", "20250102"));
        catalog.dropDatabase(databaseName, true, true);
        catalog.createDatabase(databaseName, true);
        Identifier identifier = Identifier.create(databaseName, "table");
        catalog.createTable(
                identifier,
                Schema.newBuilder()
                        .option(METASTORE_PARTITIONED_TABLE.key(), "true")
                        .option(METASTORE_TAG_TO_PARTITION.key(), "dt")
                        .column("col", DataTypes.INT())
                        .column("dt", DataTypes.STRING())
                        .partitionKeys("dt")
                        .build(),
                true);

        BatchWriteBuilder writeBuilder = catalog.getTable(identifier).newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (Map<String, String> partitionSpec : partitionSpecs) {
                write.write(GenericRow.of(0, BinaryString.fromString(partitionSpec.get("dt"))));
            }
            commit.commit(write.prepareCommit());
        }

        assertThat(catalog.listPartitions(identifier).stream().map(Partition::spec))
                .containsExactlyInAnyOrder(partitionSpecs.get(0), partitionSpecs.get(1));

        assertDoesNotThrow(() -> catalog.markDonePartitions(identifier, partitionSpecs));

        catalog.dropPartitions(identifier, partitionSpecs);

        assertThat(catalog.listPartitions(identifier)).isEmpty();

        assertThatExceptionOfType(Catalog.TableNotExistException.class)
                .isThrownBy(
                        () ->
                                catalog.listPartitions(
                                        Identifier.create(databaseName, "non_existing_table")));
        assertThatExceptionOfType(Catalog.TableNotExistException.class)
                .isThrownBy(
                        () ->
                                catalog.markDonePartitions(
                                        Identifier.create(databaseName, "non_existing_table"),
                                        partitionSpecs));
    }

    @Test
    public void testListPartitionsPaged() throws Exception {
        if (!supportPartitions()) {
            return;
        }
        String databaseName = "partitions_paged_db";
        List<Map<String, String>> partitionSpecs =
                Arrays.asList(
                        Collections.singletonMap("dt", "20250101"),
                        Collections.singletonMap("dt", "20250102"),
                        Collections.singletonMap("dt", "20240102"),
                        Collections.singletonMap("dt", "20260101"),
                        Collections.singletonMap("dt", "20250104"),
                        Collections.singletonMap("dt", "20250103"));
        catalog.dropDatabase(databaseName, true, true);
        catalog.createDatabase(databaseName, true);
        Identifier identifier = Identifier.create(databaseName, "table");
        catalog.createTable(
                identifier,
                Schema.newBuilder()
                        .option(METASTORE_PARTITIONED_TABLE.key(), "true")
                        .option(METASTORE_TAG_TO_PARTITION.key(), "dt")
                        .column("col", DataTypes.INT())
                        .column("dt", DataTypes.STRING())
                        .partitionKeys("dt")
                        .build(),
                true);

        BatchWriteBuilder writeBuilder = catalog.getTable(identifier).newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (Map<String, String> partitionSpec : partitionSpecs) {
                write.write(GenericRow.of(0, BinaryString.fromString(partitionSpec.get("dt"))));
            }
            commit.commit(write.prepareCommit());
        }

        // List partitions paged returns a list with all partitions of the table in all catalogs
        // except RestCatalog even
        // if the maxResults or pageToken is not null
        PagedList<Partition> pagedPartitions =
                catalog.listPartitionsPaged(identifier, null, null, null);
        Map[] specs = partitionSpecs.toArray(new Map[0]);
        assertPagedPartitions(pagedPartitions, specs.length, specs);

        int maxResults = 2;
        pagedPartitions = catalog.listPartitionsPaged(identifier, maxResults, null, null);
        assertPagedPartitions(pagedPartitions, specs.length, specs);

        String pageToken = "dt=20250101";
        pagedPartitions = catalog.listPartitionsPaged(identifier, maxResults, pageToken, null);
        assertPagedPartitions(pagedPartitions, specs.length, specs);

        maxResults = 8;
        pagedPartitions = catalog.listPartitionsPaged(identifier, maxResults, null, null);
        assertPagedPartitions(pagedPartitions, specs.length, specs);

        pagedPartitions = catalog.listPartitionsPaged(identifier, maxResults, pageToken, null);
        assertPagedPartitions(pagedPartitions, specs.length, specs);

        // List partitions throws TableNotExistException when the table does not exist
        final int finalMaxResults = maxResults;
        assertThatExceptionOfType(Catalog.TableNotExistException.class)
                .isThrownBy(
                        () ->
                                catalog.listPartitionsPaged(
                                        Identifier.create(databaseName, "non_existing_table"),
                                        finalMaxResults,
                                        pageToken,
                                        null));

        assertThrows(
                UnsupportedOperationException.class,
                () -> catalog.listPartitionsPaged(identifier, null, null, "dt=_0101"));

        assertThrows(
                UnsupportedOperationException.class,
                () -> catalog.listPartitionsPaged(identifier, null, null, "dt=0101_"));

        assertThrows(
                UnsupportedOperationException.class,
                () -> catalog.listPartitionsPaged(identifier, null, null, "dt=%0101"));

        assertThrows(
                UnsupportedOperationException.class,
                () -> catalog.listPartitionsPaged(identifier, null, null, "dt=0101%"));

        assertThrows(
                UnsupportedOperationException.class,
                () -> catalog.listPartitionsPaged(identifier, null, null, "dt=0101"));
    }

    protected boolean supportsAlterDatabase() {
        return false;
    }

    protected boolean supportsFormatTable() {
        return false;
    }

    protected boolean supportsAlterFormatTable() {
        return false;
    }

    protected boolean supportsView() {
        return false;
    }

    protected boolean supportsViewDialects() {
        return true;
    }

    protected void checkPartition(Partition expected, Partition actual) {
        assertThat(actual).isEqualTo(expected);
    }

    protected boolean supportPartitions() {
        return false;
    }

    protected boolean supportPagedList() {
        return false;
    }

    private void assertPagedTables(PagedList<String> tablePagedList, String... tableNames) {
        List<String> tables = tablePagedList.getElements();
        if (supportPagedList()) {
            assertThat(tables).containsExactly(tableNames);
        } else {
            assertThat(tables).containsExactlyInAnyOrder(tableNames);
        }
        assertNull(tablePagedList.getNextPageToken());
    }

    protected void assertPagedTableDetails(
            PagedList<Table> tableDetailsPagedList, int size, String... tableNames) {
        List<Table> tableDetails = tableDetailsPagedList.getElements();
        assertEquals(size, tableDetails.size());
        if (supportPagedList()) {
            assertThat(tableDetails.stream().map(Table::name).collect(Collectors.toList()))
                    .containsExactly(tableNames);
        } else {
            assertThat(tableDetails.stream().map(Table::name).collect(Collectors.toList()))
                    .containsExactlyInAnyOrder(tableNames);
        }
        List<Schema> schemas =
                tableDetails.stream()
                        .filter(table -> table instanceof FileStoreTable)
                        .map(table -> (FileStoreTable) table)
                        .map(FileStoreTable::schema)
                        .map(TableSchema::toSchema)
                        .collect(Collectors.toList());
        assertThat(
                        schemas.stream()
                                .map(Schema::fields)
                                .allMatch(fields -> DEFAULT_TABLE_SCHEMA.fields().equals(fields)))
                .isTrue();
        assertThat(
                        schemas.stream()
                                .map(Schema::partitionKeys)
                                .allMatch(
                                        partitionKeys ->
                                                DEFAULT_TABLE_SCHEMA
                                                        .partitionKeys()
                                                        .equals(partitionKeys)))
                .isTrue();
        assertThat(
                        schemas.stream()
                                .map(Schema::primaryKeys)
                                .allMatch(
                                        primaryKeys ->
                                                DEFAULT_TABLE_SCHEMA
                                                        .primaryKeys()
                                                        .equals(primaryKeys)))
                .isTrue();
        assertThat(
                        schemas.stream()
                                .map(Schema::options)
                                .allMatch(
                                        options ->
                                                DEFAULT_TABLE_SCHEMA.options().size()
                                                        <= options.size()))
                .isTrue(); // output schema has path
        assertThat(
                        schemas.stream()
                                .map(Schema::comment)
                                .allMatch(
                                        comment -> DEFAULT_TABLE_SCHEMA.comment().equals(comment)))
                .isTrue();
    }

    protected View buildView(String databaseName) {
        Identifier identifier = new Identifier(databaseName, "my_view");
        RowType rowType = RowType.builder().field("str", DataTypes.STRING()).build();
        String query = "SELECT * FROM OTHER_TABLE";

        Map<String, String> dialects = new HashMap<>();
        if (supportsViewDialects()) {
            dialects.put("spark", "SELECT * FROM SPARK_TABLE");
        }
        return new ViewImpl(
                identifier, rowType.getFields(), query, dialects, null, new HashMap<>());
    }

    protected void assertPagedViews(PagedList<String> viewPagedList, String... viewNames) {
        List<String> views = viewPagedList.getElements();
        if (supportPagedList()) {
            assertThat(views).containsExactly(viewNames);
        } else {
            assertThat(views).containsExactlyInAnyOrder(viewNames);
        }
    }

    protected void assertPagedViewDetails(
            PagedList<View> viewDetailsPagedList, View view, int size, String... viewNames) {
        List<View> viewDetails = viewDetailsPagedList.getElements();
        assertEquals(size, viewDetails.size());
        if (supportPagedList()) {
            assertThat(viewDetails.stream().map(View::name).collect(Collectors.toList()))
                    .containsExactlyInAnyOrder(viewNames);
        } else {
            assertThat(viewDetails.stream().map(View::name).collect(Collectors.toList()))
                    .containsExactlyInAnyOrder(viewNames);
        }
        assertThat(
                        viewDetails.stream()
                                .map(View::rowType)
                                .allMatch(rowType -> view.rowType().equals(rowType)))
                .isTrue();
        assertThat(
                        viewDetails.stream()
                                .map(View::query)
                                .allMatch(query -> view.query().equals(query)))
                .isTrue();
        assertThat(
                        viewDetails.stream()
                                .map(View::dialects)
                                .allMatch(dialects -> view.dialects().equals(dialects)))
                .isTrue();
        assertThat(
                        viewDetails.stream()
                                .map(View::comment)
                                .allMatch(comment -> view.comment().equals(comment)))
                .isTrue();
        assertThat(
                        viewDetails.stream()
                                .map(View::options)
                                .allMatch(options -> view.options().size() <= options.size()))
                .isTrue(); // last ddl time
    }

    @SafeVarargs
    protected final void assertPagedPartitions(
            PagedList<Partition> partitionsPagedList,
            int size,
            Map<String, String>... partitionSpecs) {
        List<Partition> partitions = partitionsPagedList.getElements();
        assertEquals(size, partitions.size());
        if (supportPagedList()) {
            assertThat(partitions.stream().map(Partition::spec)).containsExactly(partitionSpecs);
        } else {
            assertThat(partitions.stream().map(Partition::spec))
                    .containsExactlyInAnyOrder(partitionSpecs);
        }
    }

    protected Map<String, String> getFormatTableOptions() {
        Map<String, String> options = new HashMap<>(1);
        options.put("type", "format-table");
        return options;
    }

    protected View createView(Identifier identifier) {
        RowType rowType =
                RowType.builder()
                        .field("str", DataTypes.STRING())
                        .field("int", DataTypes.INT())
                        .build();
        String query = "SELECT * FROM OTHER_TABLE";
        String comment = "it is my view";
        Map<String, String> options = new HashMap<>();
        options.put("key1", "v1");
        options.put("key2", "v2");

        Map<String, String> dialects = new HashMap<>();
        if (supportsViewDialects()) {
            dialects.put("flink", "SELECT * FROM FLINK_TABLE");
            dialects.put("spark", "SELECT * FROM SPARK_TABLE");
        }
        return new ViewImpl(identifier, rowType.getFields(), query, dialects, comment, options);
    }

    private void baseAlterTable(Map<String, String> initOptions) throws Exception {
        catalog.createDatabase("test_db", true);

        Identifier identifier = Identifier.create("test_db", "test_table");
        catalog.createTable(
                identifier,
                new Schema(
                        Lists.newArrayList(new DataField(0, "col1", DataTypes.STRING(), "field1")),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        initOptions,
                        "comment"),
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

        // conflict options
        if (Options.fromMap(table.options()).get(TYPE) == TableType.MATERIALIZED_TABLE
                || Options.fromMap(table.options()).get(TYPE) == TableType.TABLE) {
            assertThatThrownBy(
                            () ->
                                    catalog.alterTable(
                                            identifier,
                                            Lists.newArrayList(
                                                    SchemaChange.setOption(
                                                            "changelog-producer", "input")),
                                            false))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining(
                            "Can not set changelog-producer on table without primary keys");
        }

        catalog.alterTable(
                identifier, Lists.newArrayList(SchemaChange.updateComment("new comment")), false);

        table = catalog.getTable(identifier);
        assertThat(table.comment().isPresent() && table.comment().get().equals("new comment"))
                .isTrue();

        // Alter table renames a column in an existing table;
        catalog.alterTable(
                identifier,
                Lists.newArrayList(SchemaChange.renameColumn("col1", "new_col1")),
                false);
        table = catalog.getTable(identifier);
        assertThat(table.rowType().getFieldIndex("col1")).isLessThan(0);
        assertThat(table.rowType().getFieldIndex("new_col1")).isEqualTo(0);

        // Alter table renames a new column throws ColumnAlreadyExistException when column already
        // exists
        assertThatThrownBy(
                        () ->
                                catalog.alterTable(
                                        identifier,
                                        Lists.newArrayList(
                                                SchemaChange.renameColumn("col2", "new_col1")),
                                        false))
                .isInstanceOf(Catalog.ColumnAlreadyExistException.class);

        // Alter table renames a column throws ColumnNotExistException when column does not exist
        assertThatThrownBy(
                        () ->
                                catalog.alterTable(
                                        identifier,
                                        Lists.newArrayList(
                                                SchemaChange.renameColumn(
                                                        "non_existing_col", "new_col2")),
                                        false))
                .isInstanceOf(Catalog.ColumnNotExistException.class);
        catalog.dropTable(identifier, true);

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
        table = catalog.getTable(identifier);

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

        // drop comment
        catalog.alterTable(identifier, Lists.newArrayList(SchemaChange.updateComment(null)), false);

        table = catalog.getTable(identifier);
        assertThat(table.comment().isPresent()).isFalse();
        catalog.dropTable(identifier, false);

        // Alter table update a column type in an existing table
        catalog.createTable(
                identifier,
                new Schema(
                        Lists.newArrayList(
                                new DataField(0, "dt", DataTypes.STRING()),
                                new DataField(1, "col1", DataTypes.BIGINT(), "col1 field")),
                        Lists.newArrayList("dt"),
                        Collections.emptyList(),
                        initOptions,
                        ""),
                false);
        catalog.alterTable(
                identifier,
                Lists.newArrayList(SchemaChange.updateColumnType("col1", DataTypes.DOUBLE())),
                false);
        table = catalog.getTable(identifier);

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
                                "Column non_existing_col does not exist in the test_db.test_table table."));
        // Alter table update a column type throws Exception when column is partition columns
        assertThatThrownBy(
                        () ->
                                catalog.alterTable(
                                        identifier,
                                        Lists.newArrayList(
                                                SchemaChange.updateColumnType(
                                                        "dt", DataTypes.DATE())),
                                        false))
                .satisfies(anyCauseMatches("Cannot update partition column: [dt]"));
        catalog.dropTable(identifier, false);

        // Alter table update a column comment in an existing table
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
                        initOptions,
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

        table = catalog.getTable(identifier);
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
                                "Column non_existing_col does not exist in the test_db.test_table table."));
        catalog.dropTable(identifier, false);

        // Alter table update a column nullability in an existing table
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
                Lists.newArrayList(
                        SchemaChange.setOption(
                                CoreOptions.DISABLE_ALTER_COLUMN_NULL_TO_NOT_NULL.key(), "false")),
                false);

        catalog.alterTable(
                identifier,
                Lists.newArrayList(SchemaChange.updateColumnNullability("col1", false)),
                false);

        // Update nested column
        fields = new String[] {"col3", "f1"};
        catalog.alterTable(
                identifier,
                Lists.newArrayList(SchemaChange.updateColumnNullability(fields, false)),
                false);

        table = catalog.getTable(identifier);
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
                                "Column non_existing_col does not exist in the test_db.test_table table."));

        // Alter table update a column nullability throws Exception when column is pk columns
        assertThatThrownBy(
                        () ->
                                catalog.alterTable(
                                        identifier,
                                        Lists.newArrayList(
                                                SchemaChange.updateColumnNullability(
                                                        new String[] {"col2"}, true)),
                                        false))
                .satisfies(anyCauseMatches("Cannot change nullability of primary key"));
        catalog.dropTable(identifier, false);
    }
}
