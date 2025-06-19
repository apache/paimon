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

package org.apache.paimon.rest;

import org.apache.paimon.PagedList;
import org.apache.paimon.Snapshot;
import org.apache.paimon.TableType;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogTestBase;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.PropertyChange;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.function.Function;
import org.apache.paimon.function.FunctionChange;
import org.apache.paimon.function.FunctionDefinition;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.rest.auth.DLFToken;
import org.apache.paimon.rest.exceptions.BadRequestException;
import org.apache.paimon.rest.responses.ConfigResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.TableSnapshot;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.view.View;
import org.apache.paimon.view.ViewChange;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;
import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;
import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.paimon.CoreOptions.METASTORE_PARTITIONED_TABLE;
import static org.apache.paimon.CoreOptions.METASTORE_TAG_TO_PARTITION;
import static org.apache.paimon.CoreOptions.QUERY_AUTH_ENABLED;
import static org.apache.paimon.catalog.Catalog.SYSTEM_DATABASE_NAME;
import static org.apache.paimon.rest.RESTApi.PAGE_TOKEN;
import static org.apache.paimon.rest.auth.DLFToken.TOKEN_DATE_FORMATTER;
import static org.apache.paimon.utils.SnapshotManagerTest.createSnapshotWithMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Base test class for {@link RESTCatalog}. */
public abstract class RESTCatalogTest extends CatalogTestBase {

    protected ConfigResponse config;
    protected Options options = new Options();
    protected RESTCatalog restCatalog;

    @Test
    public void testListDatabases() throws Exception {
        super.testListDatabases();

        String[] dbNames = {"db4", "db5", "db1", "db2", "db3"};
        String[] sortedDbNames = Arrays.stream(dbNames).sorted().toArray(String[]::new);

        // List databases returns a list with the sorted names of all databases in the rest catalog
        for (String dbName : dbNames) {
            catalog.createDatabase(dbName, true);
        }
        List<String> databases = catalog.listDatabases();
        assertThat(databases).containsExactly(sortedDbNames);
    }

    @Test
    void testListDatabasesPaged() throws Catalog.DatabaseAlreadyExistException {
        // List databases paged returns an empty list when there are no databases in the catalog
        PagedList<String> pagedDatabases = catalog.listDatabasesPaged(null, null, null);
        assertThat(pagedDatabases.getElements()).isEmpty();
        assertNull(pagedDatabases.getNextPageToken());

        String[] dbNames = {"ghj", "db1", "db2", "db3", "ert", "db_name"};
        for (String dbName : dbNames) {
            catalog.createDatabase(dbName, true);
        }

        // when maxResults is null or 0, the page length is set to a server configured value
        String[] sortedDbNames = Arrays.stream(dbNames).sorted().toArray(String[]::new);
        pagedDatabases = catalog.listDatabasesPaged(null, null, null);
        List<String> dbs = pagedDatabases.getElements();
        assertThat(dbs).containsExactly(sortedDbNames);
        assertNull(pagedDatabases.getNextPageToken());

        // when maxResults is greater than 0, the page length is the minimum of this value and a
        // server configured value
        // when pageToken is null, will list tables from the beginning
        int maxResults = 2;
        pagedDatabases = catalog.listDatabasesPaged(maxResults, null, null);
        dbs = pagedDatabases.getElements();
        assertEquals(maxResults, dbs.size());
        assertThat(dbs).containsExactly("db1", "db2");
        assertEquals("db2", pagedDatabases.getNextPageToken());

        // when pageToken is not null, will list tables from the pageToken (exclusive)
        pagedDatabases =
                catalog.listDatabasesPaged(maxResults, pagedDatabases.getNextPageToken(), null);
        dbs = pagedDatabases.getElements();
        assertEquals(maxResults, dbs.size());
        assertThat(dbs).containsExactly("db3", "db_name");

        pagedDatabases =
                catalog.listDatabasesPaged(maxResults, pagedDatabases.getNextPageToken(), null);
        dbs = pagedDatabases.getElements();
        assertEquals(2, dbs.size());
        assertThat(dbs).containsExactly("ert", "ghj");

        pagedDatabases =
                catalog.listDatabasesPaged(maxResults, pagedDatabases.getNextPageToken(), null);
        dbs = pagedDatabases.getElements();
        assertTrue(dbs.isEmpty());
        assertNull(pagedDatabases.getNextPageToken());

        maxResults = 8;
        pagedDatabases = catalog.listDatabasesPaged(maxResults, null, null);
        dbs = pagedDatabases.getElements();
        String[] expectedTableNames = Arrays.stream(dbNames).sorted().toArray(String[]::new);
        assertThat(dbs).containsExactly(expectedTableNames);
        assertNull(pagedDatabases.getNextPageToken());

        pagedDatabases = catalog.listDatabasesPaged(maxResults, "ddd", null);
        dbs = pagedDatabases.getElements();
        assertEquals(2, dbs.size());
        assertThat(dbs).containsExactly("ert", "ghj");
        assertNull(pagedDatabases.getNextPageToken());

        pagedDatabases = catalog.listDatabasesPaged(maxResults, null, "db%");
        dbs = pagedDatabases.getElements();
        assertEquals(4, dbs.size());
        assertThat(dbs).containsExactly("db1", "db2", "db3", "db_name");
        assertNull(pagedDatabases.getNextPageToken());

        pagedDatabases = catalog.listDatabasesPaged(maxResults, null, "db");
        dbs = pagedDatabases.getElements();
        assertTrue(dbs.isEmpty());
        assertNull(pagedDatabases.getNextPageToken());

        pagedDatabases = catalog.listDatabasesPaged(maxResults, null, "db_");
        dbs = pagedDatabases.getElements();
        assertTrue(dbs.isEmpty());
        assertNull(pagedDatabases.getNextPageToken());

        pagedDatabases = catalog.listDatabasesPaged(maxResults, null, "db_%");
        dbs = pagedDatabases.getElements();
        assertEquals(1, dbs.size());
        assertThat(dbs).containsExactly("db_name");
        assertNull(pagedDatabases.getNextPageToken());
    }

    @Test
    void testDatabaseApiWhenNoPermission() {
        String database = "test_no_permission_db";
        revokeDatabasePermission(database);
        assertThrows(
                Catalog.DatabaseNoPermissionException.class,
                () -> catalog.createDatabase(database, false, Maps.newHashMap()));
        assertThrows(
                Catalog.DatabaseNoPermissionException.class, () -> catalog.getDatabase(database));
        assertThrows(
                Catalog.DatabaseNoPermissionException.class,
                () -> catalog.dropDatabase(database, false, false));
        assertThrows(
                Catalog.DatabaseNoPermissionException.class,
                () ->
                        catalog.alterDatabase(
                                database,
                                Lists.newArrayList(PropertyChange.setProperty("key1", "value1")),
                                false));
    }

    @Test
    void testApiWhenDatabaseNoExistAndNotIgnore() {
        String database = "test_no_exist_db";
        assertThrows(
                Catalog.DatabaseNotExistException.class,
                () -> catalog.dropDatabase(database, false, false));
        assertThrows(
                Catalog.DatabaseNotExistException.class,
                () ->
                        catalog.alterDatabase(
                                database,
                                Lists.newArrayList(PropertyChange.setProperty("key1", "value1")),
                                false));
        assertThrows(
                Catalog.DatabaseNotExistException.class,
                () -> catalog.listTablesPaged(database, 100, null, null));
        assertThrows(
                Catalog.DatabaseNotExistException.class,
                () -> catalog.listTableDetailsPaged(database, 100, null, null));
    }

    @Test
    void testGetSystemDatabase() throws Catalog.DatabaseNotExistException {
        assertThat(catalog.getDatabase(SYSTEM_DATABASE_NAME).name())
                .isEqualTo(SYSTEM_DATABASE_NAME);
    }

    @Test
    void testApiWhenTableNoPermission() throws Exception {
        Identifier identifier = Identifier.create("test_table_db", "no_permission_table");
        createTable(identifier, Maps.newHashMap(), Lists.newArrayList("col1"));
        revokeTablePermission(identifier);
        assertThrows(Catalog.TableNoPermissionException.class, () -> catalog.getTable(identifier));
        assertThrows(
                Catalog.TableNoPermissionException.class,
                () ->
                        catalog.alterTable(
                                identifier,
                                Lists.newArrayList(
                                        SchemaChange.addColumn("col2", DataTypes.DATE())),
                                false));
        assertThrows(
                Catalog.TableNoPermissionException.class,
                () -> catalog.dropTable(identifier, false));
        assertThrows(
                Catalog.TableNoPermissionException.class,
                () ->
                        catalog.renameTable(
                                identifier,
                                Identifier.create("test_table_db", "no_permission_table2"),
                                false));
        assertThrows(
                Catalog.TableNoPermissionException.class, () -> catalog.listPartitions(identifier));
        assertThrows(
                Catalog.TableNoPermissionException.class,
                () -> catalog.listPartitionsPaged(identifier, 100, null, null));
        assertThrows(
                Catalog.TableNoPermissionException.class,
                () -> restCatalog.createBranch(identifier, "test_branch", null));
        assertThrows(
                Catalog.TableNoPermissionException.class,
                () -> restCatalog.listBranches(identifier));
        assertThrows(
                Catalog.TableNoPermissionException.class,
                () -> restCatalog.dropBranch(identifier, "test_branch"));
        assertThrows(
                Catalog.TableNoPermissionException.class,
                () -> restCatalog.fastForward(identifier, "test_branch"));
        assertThrows(
                Catalog.TableNoPermissionException.class,
                () -> restCatalog.loadTableToken(identifier));
        assertThrows(
                Catalog.TableNoPermissionException.class,
                () -> restCatalog.loadSnapshot(identifier));
        assertThrows(
                Catalog.TableNoPermissionException.class,
                () ->
                        restCatalog.commitSnapshot(
                                identifier,
                                "",
                                createSnapshotWithMillis(1L, System.currentTimeMillis()),
                                new ArrayList<PartitionStatistics>()));
    }

    @Test
    void renameWhenTargetTableExist() throws Exception {
        Identifier identifier = Identifier.create("test_table_db", "rename_table");
        Identifier targetIdentifier = Identifier.create("test_table_db", "target_table");
        createTable(identifier, Maps.newHashMap(), Lists.newArrayList("col1"));
        createTable(targetIdentifier, Maps.newHashMap(), Lists.newArrayList("col1"));
        assertThrows(
                Catalog.TableAlreadyExistException.class,
                () -> catalog.renameTable(identifier, targetIdentifier, false));
    }

    @Test
    public void testListTables() throws Exception {
        super.testListTables();

        String databaseName = "tables_db";
        String[] tableNames = {"table4", "table5", "table1", "table2", "table3"};
        String[] sortedTableNames = Arrays.stream(tableNames).sorted().toArray(String[]::new);
        Options options = new Options(this.catalog.options());
        restCatalog.createDatabase(databaseName, false);
        List<String> restTables = restCatalog.listTables(databaseName);
        assertThat(restTables).isEmpty();

        // List tables returns a list with the names of all tables in the database

        for (String tableName : tableNames) {
            restCatalog.createTable(
                    Identifier.create(databaseName, tableName), DEFAULT_TABLE_SCHEMA, false);
        }
        restTables = restCatalog.listTables(databaseName);
        assertThat(restTables).containsExactly(sortedTableNames);

        // List tables throws DatabaseNotExistException when the database does not exist
        assertThatExceptionOfType(Catalog.DatabaseNotExistException.class)
                .isThrownBy(() -> restCatalog.listTables("non_existing_db"));
    }

    @Test
    public void testListTablesPaged() throws Exception {
        // List tables paged returns an empty list when there are no tables in the database
        String databaseName = "tables_paged_db";
        catalog.createDatabase(databaseName, false);
        PagedList<String> pagedTables = catalog.listTablesPaged(databaseName, null, null, null);
        assertThat(pagedTables.getElements()).isEmpty();
        assertNull(pagedTables.getNextPageToken());

        String[] tableNames = {"table1", "table2", "table3", "abd", "def", "opr", "table_name"};
        for (String tableName : tableNames) {
            catalog.createTable(
                    Identifier.create(databaseName, tableName), DEFAULT_TABLE_SCHEMA, false);
        }

        // when maxResults is null or 0, the page length is set to a server configured value
        String[] sortedTableNames = Arrays.stream(tableNames).sorted().toArray(String[]::new);
        pagedTables = catalog.listTablesPaged(databaseName, null, null, null);
        List<String> tables = pagedTables.getElements();
        assertThat(tables).containsExactly(sortedTableNames);
        assertNull(pagedTables.getNextPageToken());

        // when maxResults is greater than 0, the page length is the minimum of this value and a
        // server configured value
        // when pageToken is null, will list tables from the beginning
        int maxResults = 2;
        pagedTables = catalog.listTablesPaged(databaseName, maxResults, null, null);
        tables = pagedTables.getElements();
        assertEquals(maxResults, tables.size());
        assertThat(tables).containsExactly("abd", "def");
        assertEquals("def", pagedTables.getNextPageToken());

        // when pageToken is not null, will list tables from the pageToken (exclusive)
        pagedTables =
                catalog.listTablesPaged(
                        databaseName, maxResults, pagedTables.getNextPageToken(), null);
        tables = pagedTables.getElements();
        assertEquals(maxResults, tables.size());
        assertThat(tables).containsExactly("opr", "table1");
        assertEquals("table1", pagedTables.getNextPageToken());

        pagedTables =
                catalog.listTablesPaged(
                        databaseName, maxResults, pagedTables.getNextPageToken(), null);
        tables = pagedTables.getElements();
        assertEquals(maxResults, tables.size());
        assertThat(tables).containsExactly("table2", "table3");
        assertEquals("table3", pagedTables.getNextPageToken());

        pagedTables =
                catalog.listTablesPaged(
                        databaseName, maxResults, pagedTables.getNextPageToken(), null);
        tables = pagedTables.getElements();
        assertEquals(1, tables.size());
        assertNull(pagedTables.getNextPageToken());

        maxResults = 8;
        pagedTables = catalog.listTablesPaged(databaseName, maxResults, null, null);
        tables = pagedTables.getElements();
        assertThat(tables).containsExactly(sortedTableNames);
        assertNull(pagedTables.getNextPageToken());

        pagedTables = catalog.listTablesPaged(databaseName, maxResults, "table1", null);
        tables = pagedTables.getElements();
        assertEquals(3, tables.size());
        assertThat(tables).containsExactly("table2", "table3", "table_name");
        assertNull(pagedTables.getNextPageToken());

        // List tables throws DatabaseNotExistException when the database does not exist
        assertThatExceptionOfType(Catalog.DatabaseNotExistException.class)
                .isThrownBy(() -> catalog.listTables("non_existing_db"));

        pagedTables = catalog.listTablesPaged(databaseName, null, null, "table%");
        tables = pagedTables.getElements();
        assertEquals(4, tables.size());
        assertThat(tables).containsExactly("table1", "table2", "table3", "table_name");
        assertNull(pagedTables.getNextPageToken());

        pagedTables = catalog.listTablesPaged(databaseName, null, null, "table_");
        tables = pagedTables.getElements();
        assertTrue(tables.isEmpty());
        assertNull(pagedTables.getNextPageToken());

        pagedTables = catalog.listTablesPaged(databaseName, null, null, "table_%");
        tables = pagedTables.getElements();
        assertEquals(1, tables.size());
        assertThat(tables).containsExactly("table_name");
        assertNull(pagedTables.getNextPageToken());

        pagedTables = catalog.listTablesPaged(databaseName, null, null, "table_name");
        tables = pagedTables.getElements();
        assertEquals(1, tables.size());
        assertThat(tables).containsExactly("table_name");
        assertNull(pagedTables.getNextPageToken());

        Assertions.assertThrows(
                BadRequestException.class,
                () -> catalog.listTablesPaged(databaseName, null, null, "%table"));

        Assertions.assertThrows(
                BadRequestException.class,
                () -> catalog.listTablesPaged(databaseName, null, null, "ta%le"));
    }

    @Test
    public void testListTableDetailsPaged() throws Exception {
        // List table details returns an empty list when there are no tables in the database
        String databaseName = "table_details_paged_db";
        catalog.createDatabase(databaseName, false);
        PagedList<Table> pagedTableDetails =
                catalog.listTableDetailsPaged(databaseName, null, null, null);
        assertThat(pagedTableDetails.getElements()).isEmpty();
        assertNull(pagedTableDetails.getNextPageToken());

        String[] tableNames = {"table1", "table2", "table3", "abd", "def", "opr", "table_name"};
        String[] expectedTableNames = Arrays.stream(tableNames).sorted().toArray(String[]::new);
        for (String tableName : tableNames) {
            catalog.createTable(
                    Identifier.create(databaseName, tableName), DEFAULT_TABLE_SCHEMA, false);
        }

        pagedTableDetails = catalog.listTableDetailsPaged(databaseName, null, null, null);
        assertPagedTableDetails(pagedTableDetails, tableNames.length, expectedTableNames);
        assertNull(pagedTableDetails.getNextPageToken());

        int maxResults = 2;
        pagedTableDetails = catalog.listTableDetailsPaged(databaseName, maxResults, null, null);
        assertPagedTableDetails(pagedTableDetails, maxResults, "abd", "def");
        assertEquals("def", pagedTableDetails.getNextPageToken());

        pagedTableDetails =
                catalog.listTableDetailsPaged(
                        databaseName, maxResults, pagedTableDetails.getNextPageToken(), null);
        assertPagedTableDetails(pagedTableDetails, maxResults, "opr", "table1");
        assertEquals("table1", pagedTableDetails.getNextPageToken());

        pagedTableDetails =
                catalog.listTableDetailsPaged(
                        databaseName, maxResults, pagedTableDetails.getNextPageToken(), null);
        assertPagedTableDetails(pagedTableDetails, maxResults, "table2", "table3");
        assertEquals("table3", pagedTableDetails.getNextPageToken());

        pagedTableDetails =
                catalog.listTableDetailsPaged(
                        databaseName, maxResults, pagedTableDetails.getNextPageToken(), null);
        assertEquals(1, pagedTableDetails.getElements().size());
        assertNull(pagedTableDetails.getNextPageToken());

        maxResults = 8;
        pagedTableDetails = catalog.listTableDetailsPaged(databaseName, maxResults, null, null);
        assertPagedTableDetails(
                pagedTableDetails, Math.min(maxResults, tableNames.length), expectedTableNames);
        assertNull(pagedTableDetails.getNextPageToken());

        String pageToken = "table1";
        pagedTableDetails =
                catalog.listTableDetailsPaged(databaseName, maxResults, pageToken, null);
        assertPagedTableDetails(pagedTableDetails, 3, "table2", "table3", "table_name");
        assertNull(pagedTableDetails.getNextPageToken());

        // List table details throws DatabaseNotExistException when the database does not exist
        final int finalMaxResults = maxResults;
        assertThatExceptionOfType(Catalog.DatabaseNotExistException.class)
                .isThrownBy(
                        () ->
                                catalog.listTableDetailsPaged(
                                        "non_existing_db", finalMaxResults, pageToken, null));

        // List tables throws DatabaseNotExistException when the database does not exist
        assertThatExceptionOfType(Catalog.DatabaseNotExistException.class)
                .isThrownBy(() -> catalog.listTables("non_existing_db"));

        pagedTableDetails = catalog.listTableDetailsPaged(databaseName, null, null, "table%");
        assertPagedTableDetails(pagedTableDetails, 4, "table1", "table2", "table3", "table_name");
        assertNull(pagedTableDetails.getNextPageToken());

        pagedTableDetails = catalog.listTableDetailsPaged(databaseName, null, null, "table_");
        Assertions.assertTrue(pagedTableDetails.getElements().isEmpty());
        assertNull(pagedTableDetails.getNextPageToken());

        pagedTableDetails = catalog.listTableDetailsPaged(databaseName, null, null, "table_%");
        assertPagedTableDetails(pagedTableDetails, 1, "table_name");
        assertNull(pagedTableDetails.getNextPageToken());

        Assertions.assertThrows(
                BadRequestException.class,
                () -> catalog.listTableDetailsPaged(databaseName, null, null, "ta%le"));

        Assertions.assertThrows(
                BadRequestException.class,
                () -> catalog.listTableDetailsPaged(databaseName, null, null, "%tale"));
    }

    @Test
    public void testListTablesPagedGlobally() throws Exception {
        // List table paged globally returns an empty list when there are no tables in the catalog

        PagedList<Identifier> pagedTables = catalog.listTablesPagedGlobally(null, null, null, null);
        assertThat(pagedTables.getElements()).isEmpty();
        assertNull(pagedTables.getNextPageToken());

        String databaseName = "list_tables_paged_globally db";
        String databaseName2 = "sample";
        String databaseNamePattern = "list_tables_paged_globally%";
        String[] tableNames = {
            "table1", "table2", "table3", "abd", "def", "opr", "format_table", "table_name"
        };
        prepareDataForListTablesPagedGlobally(databaseName, databaseName2, tableNames);

        Identifier[] expectedTableNames =
                Arrays.stream(tableNames)
                        .map(tableName -> Identifier.create(databaseName, tableName))
                        .toArray(Identifier[]::new);
        Identifier[] fullTableNames = Arrays.copyOf(expectedTableNames, tableNames.length + 1);
        fullTableNames[tableNames.length] = Identifier.create(databaseName2, "table1");

        pagedTables = catalog.listTablesPagedGlobally(databaseNamePattern, null, null, null);
        assertThat(pagedTables.getElements()).containsExactlyInAnyOrder(expectedTableNames);
        assertNull(pagedTables.getNextPageToken());

        assertListTablesPagedGloballyWithLoop(databaseNamePattern, expectedTableNames);
        assertListTablesPagedGloballyWithLoop(null, fullTableNames);

        assertListTablesPagedGloballyWithTablePattern(
                databaseName, databaseNamePattern, expectedTableNames);
    }

    protected void prepareDataForListTablesPagedGlobally(
            String databaseName, String databaseName2, String[] tableNames)
            throws Catalog.DatabaseAlreadyExistException, Catalog.TableAlreadyExistException,
                    Catalog.DatabaseNotExistException {
        catalog.createDatabase(databaseName, false);
        catalog.createDatabase(databaseName2, false);

        Map<String, String> options = new HashMap<>();
        options.put("type", TableType.FORMAT_TABLE.toString());

        Schema formatTableSchema =
                new Schema(
                        Lists.newArrayList(
                                new DataField(0, "pk", DataTypes.INT()),
                                new DataField(1, "col1", DataTypes.STRING()),
                                new DataField(2, "col2", DataTypes.STRING())),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        options,
                        "");

        for (String tableName : tableNames) {
            if (StringUtils.equals(tableName, "format_table")) {
                catalog.createTable(
                        Identifier.create(databaseName, tableName), formatTableSchema, false);
            } else {
                catalog.createTable(
                        Identifier.create(databaseName, tableName), DEFAULT_TABLE_SCHEMA, false);
            }
        }

        catalog.createTable(
                Identifier.create(databaseName2, "table1"), DEFAULT_TABLE_SCHEMA, false);
    }

    protected void assertListTablesPagedGloballyWithLoop(
            String databaseNamePattern, Identifier[] expectedTableNames) {
        int maxResults = 2;
        PagedList<Identifier> pagedTables;
        List<Identifier> tables = new ArrayList<>();
        String pageToken = null;
        do {
            pagedTables =
                    catalog.listTablesPagedGlobally(
                            databaseNamePattern, null, maxResults, pageToken);
            pageToken = pagedTables.getNextPageToken();
            if (pagedTables.getElements() != null) {
                tables.addAll(pagedTables.getElements());
            }
            if (pageToken == null
                    || pagedTables.getElements() == null
                    || pagedTables.getElements().isEmpty()) {
                break;
            }
        } while (StringUtils.isNotEmpty(pageToken));
        assertEquals(expectedTableNames.length, tables.size());
        assertThat(tables).containsExactlyInAnyOrder(expectedTableNames);
        assertNull(pagedTables.getNextPageToken());
    }

    protected void assertListTablesPagedGloballyWithTablePattern(
            String databaseName, String databaseNamePattern, Identifier[] expectedTableNames) {
        int maxResults = 9;
        PagedList<Identifier> pagedTables =
                catalog.listTablesPagedGlobally(databaseNamePattern, null, maxResults, null);
        assertEquals(
                Math.min(maxResults, expectedTableNames.length), pagedTables.getElements().size());
        assertThat(pagedTables.getElements()).containsExactlyInAnyOrder(expectedTableNames);
        assertNull(pagedTables.getNextPageToken());

        pagedTables = catalog.listTablesPagedGlobally(databaseNamePattern, "table%", null, null);
        assertEquals(4, pagedTables.getElements().size());
        assertThat(pagedTables.getElements())
                .containsExactlyInAnyOrder(
                        Identifier.create(databaseName, "table1"),
                        Identifier.create(databaseName, "table2"),
                        Identifier.create(databaseName, "table3"),
                        Identifier.create(databaseName, "table_name"));
        assertNull(pagedTables.getNextPageToken());

        pagedTables = catalog.listTablesPagedGlobally(databaseNamePattern, "table_", null, null);
        assertTrue(pagedTables.getElements().isEmpty());
        assertNull(pagedTables.getNextPageToken());

        pagedTables = catalog.listTablesPagedGlobally(databaseNamePattern, "table_%", null, null);
        assertEquals(1, pagedTables.getElements().size());
        assertThat(pagedTables.getElements())
                .containsExactlyInAnyOrder(Identifier.create(databaseName, "table_name"));
        assertNull(pagedTables.getNextPageToken());

        pagedTables = catalog.listTablesPagedGlobally(databaseNamePattern, "tabl_", null, null);
        assertTrue(pagedTables.getElements().isEmpty());
        assertNull(pagedTables.getNextPageToken());

        Assertions.assertThrows(
                BadRequestException.class,
                () -> catalog.listTablesPagedGlobally(databaseNamePattern, "ta%le", null, null));

        Assertions.assertThrows(
                BadRequestException.class,
                () -> catalog.listTablesPagedGlobally(databaseNamePattern, "%tale", null, null));
    }

    private String buildFullName(String database, String tableName) {
        return String.format("%s.%s", database, tableName);
    }

    @Test
    void testListViews() throws Exception {
        String databaseName = "views_paged_db";
        List<String> views;
        String[] viewNames = new String[] {"view1", "view2", "view3", "abd", "def", "opr", "xyz"};
        String[] sortedViewNames = Arrays.stream(viewNames).sorted().toArray(String[]::new);
        // List tables returns an empty list when there are no tables in the database
        restCatalog.createDatabase(databaseName, false);
        views = restCatalog.listViews(databaseName);
        assertThat(views).isEmpty();

        View view = buildView(databaseName);

        for (String viewName : viewNames) {
            restCatalog.createView(Identifier.create(databaseName, viewName), view, false);
        }

        // when maxResults is null or 0, the page length is set to a server configured value
        assertThat(restCatalog.listViews(databaseName)).containsExactly(sortedViewNames);
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
        String[] viewNames = {"view1", "view2", "view3", "abd", "def", "opr", "view_name"};
        String[] sortedViewNames = Arrays.stream(viewNames).sorted().toArray(String[]::new);
        for (String viewName : viewNames) {
            catalog.createView(Identifier.create(databaseName, viewName), view, false);
        }

        pagedViews = catalog.listViewsPaged(databaseName, null, null, null);
        assertThat(pagedViews.getElements()).containsExactly(sortedViewNames);
        assertNull(pagedViews.getNextPageToken());

        int maxResults = 2;
        pagedViews = catalog.listViewsPaged(databaseName, maxResults, null, null);
        assertPagedViews(pagedViews, "abd", "def");
        assertEquals("def", pagedViews.getNextPageToken());

        pagedViews =
                catalog.listViewsPaged(
                        databaseName, maxResults, pagedViews.getNextPageToken(), null);
        assertPagedViews(pagedViews, "opr", "view1");
        assertEquals("view1", pagedViews.getNextPageToken());

        pagedViews =
                catalog.listViewsPaged(
                        databaseName, maxResults, pagedViews.getNextPageToken(), null);
        assertPagedViews(pagedViews, "view2", "view3");
        assertEquals("view3", pagedViews.getNextPageToken());

        maxResults = 8;
        String[] expectedViewNames = Arrays.stream(viewNames).sorted().toArray(String[]::new);
        pagedViews = catalog.listViewsPaged(databaseName, maxResults, null, null);
        assertPagedViews(pagedViews, expectedViewNames);
        assertNull(pagedViews.getNextPageToken());

        String pageToken = "view1";
        pagedViews = catalog.listViewsPaged(databaseName, maxResults, pageToken, null);
        assertPagedViews(pagedViews, "view2", "view3", "view_name");
        assertNull(pagedViews.getNextPageToken());

        // List views throws DatabaseNotExistException when the database does not exist
        final int finalMaxResults = 9;
        assertThatExceptionOfType(Catalog.DatabaseNotExistException.class)
                .isThrownBy(
                        () ->
                                catalog.listViewsPaged(
                                        "non_existing_db", finalMaxResults, pageToken, null));

        pagedViews = catalog.listViewsPaged(databaseName, null, null, "view%");
        assertPagedViews(pagedViews, "view1", "view2", "view3", "view_name");
        assertNull(pagedViews.getNextPageToken());

        pagedViews = catalog.listViewsPaged(databaseName, null, null, "view_%");
        assertPagedViews(pagedViews, "view_name");
        assertNull(pagedViews.getNextPageToken());

        pagedViews = catalog.listViewsPaged(databaseName, null, null, "view_");
        assertTrue(pagedViews.getElements().isEmpty());
        assertNull(pagedViews.getNextPageToken());

        Assertions.assertThrows(
                BadRequestException.class,
                () -> catalog.listViewsPaged(databaseName, null, null, "vi%ew"));

        Assertions.assertThrows(
                BadRequestException.class,
                () -> catalog.listViewsPaged(databaseName, null, null, "%view"));
    }

    @Test
    public void testListViewDetailsPaged() throws Exception {
        // List view details returns an empty list when there are no views in the database
        String databaseName = "view_details_paged_db";
        catalog.createDatabase(databaseName, false);
        PagedList<View> pagedViewDetails =
                catalog.listViewDetailsPaged(databaseName, null, null, null);
        assertThat(pagedViewDetails.getElements()).isEmpty();
        assertNull(pagedViewDetails.getNextPageToken());

        String[] viewNames = {"view1", "view2", "view3", "abd", "def", "opr", "view_name"};
        View view = buildView(databaseName);
        for (String viewName : viewNames) {
            catalog.createView(Identifier.create(databaseName, viewName), view, false);
        }

        pagedViewDetails = catalog.listViewDetailsPaged(databaseName, null, null, null);
        assertPagedViewDetails(pagedViewDetails, view, viewNames.length, viewNames);
        assertNull(pagedViewDetails.getNextPageToken());

        int maxResults = 2;
        pagedViewDetails = catalog.listViewDetailsPaged(databaseName, maxResults, null, null);
        assertPagedViewDetails(pagedViewDetails, view, maxResults, "abd", "def");
        assertEquals("def", pagedViewDetails.getNextPageToken());

        pagedViewDetails =
                catalog.listViewDetailsPaged(
                        databaseName, maxResults, pagedViewDetails.getNextPageToken(), null);
        assertPagedViewDetails(pagedViewDetails, view, maxResults, "opr", "view1");
        assertEquals("view1", pagedViewDetails.getNextPageToken());

        pagedViewDetails =
                catalog.listViewDetailsPaged(
                        databaseName, maxResults, pagedViewDetails.getNextPageToken(), null);
        assertPagedViewDetails(pagedViewDetails, view, maxResults, "view2", "view3");
        assertEquals("view3", pagedViewDetails.getNextPageToken());

        pagedViewDetails =
                catalog.listViewDetailsPaged(
                        databaseName, maxResults, pagedViewDetails.getNextPageToken(), null);
        assertEquals(1, pagedViewDetails.getElements().size());
        assertNull(pagedViewDetails.getNextPageToken());

        maxResults = 8;
        pagedViewDetails = catalog.listViewDetailsPaged(databaseName, maxResults, null, null);
        String[] expectedViewNames = Arrays.stream(viewNames).sorted().toArray(String[]::new);
        assertPagedViewDetails(
                pagedViewDetails,
                view,
                Math.min(maxResults, expectedViewNames.length),
                expectedViewNames);
        assertNull(pagedViewDetails.getNextPageToken());

        String pageToken = "view1";
        pagedViewDetails = catalog.listViewDetailsPaged(databaseName, maxResults, pageToken, null);
        assertPagedViewDetails(pagedViewDetails, view, 3, "view2", "view3", "view_name");
        assertNull(pagedViewDetails.getNextPageToken());

        // List view details throws DatabaseNotExistException when the database does not exist
        final int finalMaxResults = maxResults;
        assertThatExceptionOfType(Catalog.DatabaseNotExistException.class)
                .isThrownBy(
                        () ->
                                catalog.listViewDetailsPaged(
                                        "non_existing_db", finalMaxResults, pageToken, null));

        pagedViewDetails = catalog.listViewDetailsPaged(databaseName, null, null, "view%");
        assertPagedViewDetails(pagedViewDetails, view, 4, "view1", "view2", "view3", "view_name");
        assertNull(pagedViewDetails.getNextPageToken());

        pagedViewDetails = catalog.listViewDetailsPaged(databaseName, null, null, "view_");
        Assertions.assertTrue(pagedViewDetails.getElements().isEmpty());
        assertNull(pagedViewDetails.getNextPageToken());

        pagedViewDetails = catalog.listViewDetailsPaged(databaseName, null, null, "view_%");
        assertPagedViewDetails(pagedViewDetails, view, 1, "view_name");
        assertNull(pagedViewDetails.getNextPageToken());

        Assertions.assertThrows(
                BadRequestException.class,
                () -> catalog.listViewDetailsPaged(databaseName, null, null, "vi%ew"));

        Assertions.assertThrows(
                BadRequestException.class,
                () -> catalog.listViewDetailsPaged(databaseName, null, null, "%view"));
    }

    @Test
    public void testListViewsPagedGlobally() throws Exception {
        // list views paged globally returns an empty list when there are no views in the catalog

        PagedList<Identifier> pagedViews = catalog.listViewsPagedGlobally(null, null, null, null);
        assertThat(pagedViews.getElements()).isEmpty();
        assertNull(pagedViews.getNextPageToken());

        String databaseName = "list_views_paged_globally_db";
        String databaseName2 = "sample";
        String databaseNamePattern = "list_views_paged_globally%";
        String[] viewNames = {"view1", "view2", "view3", "abd", "def", "opr", "view_name"};
        prepareDataForListViewsPagedGlobally(databaseName, databaseName2, viewNames);

        Identifier[] expectedViewNames =
                Arrays.stream(viewNames)
                        .map(viewName -> Identifier.create(databaseName, viewName))
                        .toArray(Identifier[]::new);
        Identifier[] fullTableNames = Arrays.copyOf(expectedViewNames, viewNames.length + 1);
        fullTableNames[viewNames.length] = Identifier.create(databaseName2, "view1");

        pagedViews = catalog.listViewsPagedGlobally(databaseNamePattern, null, null, null);
        assertEquals(expectedViewNames.length, pagedViews.getElements().size());
        assertThat(pagedViews.getElements()).containsExactlyInAnyOrder(expectedViewNames);
        assertNull(pagedViews.getNextPageToken());

        assertListViewsPagedGloballyWithLoop(databaseNamePattern, expectedViewNames);
        assertListViewsPagedGloballyWithLoop(null, fullTableNames);

        assertListViewsPagedGloballyWithViewPattern(
                databaseName, databaseNamePattern, expectedViewNames);
    }

    protected void prepareDataForListViewsPagedGlobally(
            String databaseName, String databaseName2, String[] viewNames)
            throws Catalog.DatabaseAlreadyExistException, Catalog.DatabaseNotExistException,
                    Catalog.ViewAlreadyExistException {
        catalog.createDatabase(databaseName, false);
        catalog.createDatabase(databaseName2, false);

        View view = buildView(databaseName);
        for (String viewName : viewNames) {
            catalog.createView(Identifier.create(databaseName, viewName), view, false);
        }

        catalog.createView(Identifier.create(databaseName2, "view1"), view, false);
    }

    protected void assertListViewsPagedGloballyWithLoop(
            String databaseNamePattern, Identifier[] expectedViewNames) {
        int maxResults = 2;
        PagedList<Identifier> pagedViews;
        List<Identifier> views = new ArrayList<>();
        String pageToken = null;
        do {
            pagedViews =
                    catalog.listViewsPagedGlobally(
                            databaseNamePattern, null, maxResults, pageToken);
            pageToken = pagedViews.getNextPageToken();
            if (pagedViews.getElements() != null) {
                views.addAll(pagedViews.getElements());
            }
            if (pageToken == null
                    || pagedViews.getElements() == null
                    || pagedViews.getElements().isEmpty()) {
                break;
            }
        } while (StringUtils.isNotEmpty(pageToken));
        assertEquals(expectedViewNames.length, views.size());
        assertThat(views).containsExactlyInAnyOrder(expectedViewNames);
        assertNull(pagedViews.getNextPageToken());
    }

    protected void assertListViewsPagedGloballyWithViewPattern(
            String databaseName, String databaseNamePattern, Identifier[] expectedViewNames) {
        int maxResults = 8;
        PagedList<Identifier> pagedViews =
                catalog.listViewsPagedGlobally(databaseNamePattern, null, maxResults, null);
        assertEquals(
                Math.min(maxResults, expectedViewNames.length), pagedViews.getElements().size());
        assertThat(pagedViews.getElements()).containsExactlyInAnyOrder(expectedViewNames);
        assertNull(pagedViews.getNextPageToken());

        pagedViews = catalog.listViewsPagedGlobally(databaseNamePattern, "view%", null, null);
        assertEquals(4, pagedViews.getElements().size());
        assertThat(pagedViews.getElements())
                .containsExactlyInAnyOrder(
                        Identifier.create(databaseName, "view1"),
                        Identifier.create(databaseName, "view2"),
                        Identifier.create(databaseName, "view3"),
                        Identifier.create(databaseName, "view_name"));
        assertNull(pagedViews.getNextPageToken());

        pagedViews = catalog.listViewsPagedGlobally(databaseNamePattern, "view_", null, null);
        assertTrue(pagedViews.getElements().isEmpty());
        assertNull(pagedViews.getNextPageToken());

        pagedViews = catalog.listViewsPagedGlobally(databaseNamePattern, "view_%", null, null);
        assertEquals(1, pagedViews.getElements().size());
        assertThat(pagedViews.getElements())
                .containsExactlyInAnyOrder(Identifier.create(databaseName, "view_name"));
        assertNull(pagedViews.getNextPageToken());

        Assertions.assertThrows(
                BadRequestException.class,
                () -> catalog.listViewsPagedGlobally(databaseNamePattern, "vi%ew", null, null));

        Assertions.assertThrows(
                BadRequestException.class,
                () -> catalog.listViewsPagedGlobally(databaseNamePattern, "%view", null, null));
    }

    @Test
    void testListPartitionsWhenMetastorePartitionedIsTrue() throws Exception {
        if (!supportPartitions()) {
            return;
        }

        String branchName = "test_branch";
        Identifier identifier = Identifier.create("test_db", "test_table");
        Identifier branchIdentifier = new Identifier("test_db", "test_table", branchName);
        assertThrows(
                Catalog.TableNotExistException.class, () -> restCatalog.listPartitions(identifier));
        restCatalog.createDatabase(identifier.getDatabaseName(), true);
        restCatalog.createTable(
                identifier,
                new Schema(
                        Lists.newArrayList(
                                new DataField(0, "col1", DataTypes.INT()),
                                new DataField(1, "dt", DataTypes.STRING())),
                        Arrays.asList("dt"),
                        Collections.emptyList(),
                        ImmutableMap.of(METASTORE_PARTITIONED_TABLE.key(), "" + true),
                        ""),
                true);
        List<Partition> result = catalog.listPartitions(identifier);
        assertEquals(0, result.size());
        List<Map<String, String>> partitionSpecs =
                Arrays.asList(singletonMap("dt", "20250101"), singletonMap("dt", "20250102"));
        restCatalog.createBranch(identifier, branchName, null);

        BatchWriteBuilder writeBuilder = catalog.getTable(branchIdentifier).newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (Map<String, String> partitionSpec : partitionSpecs) {
                write.write(GenericRow.of(0, BinaryString.fromString(partitionSpec.get("dt"))));
            }
            commit.commit(write.prepareCommit());
        }
        assertThat(catalog.listPartitions(branchIdentifier).stream().map(Partition::spec))
                .containsExactlyInAnyOrder(partitionSpecs.get(0), partitionSpecs.get(1));
    }

    @Test
    void testListPartitionsFromFile() throws Exception {
        Identifier identifier = Identifier.create("test_db", "test_table");
        createTable(identifier, Maps.newHashMap(), Lists.newArrayList("col1"));
        List<Partition> result = catalog.listPartitions(identifier);
        assertEquals(0, result.size());
    }

    @Test
    void testListPartitions() throws Exception {
        innerTestListPartitions(true);
    }

    @Test
    void testListPartitionsNonMetastore() throws Exception {
        innerTestListPartitions(false);
    }

    private void innerTestListPartitions(boolean metastore) throws Exception {
        if (!supportPartitions()) {
            return;
        }
        List<Map<String, String>> partitionSpecs =
                Arrays.asList(
                        singletonMap("dt", "20250101"),
                        singletonMap("dt", "20250102"),
                        singletonMap("dt", "20240102"),
                        singletonMap("dt", "20260101"),
                        singletonMap("dt", "20250104"),
                        singletonMap("dt", "20250103"));
        Map[] sortedSpecs =
                partitionSpecs.stream()
                        .sorted((o1, o2) -> o2.get("dt").compareTo(o1.get("dt")))
                        .toArray(Map[]::new);

        String databaseName = "partitions_db" + metastore;
        Identifier identifier = Identifier.create(databaseName, "table");
        Schema schema =
                Schema.newBuilder()
                        .option(METASTORE_PARTITIONED_TABLE.key(), Boolean.toString(metastore))
                        .option(METASTORE_TAG_TO_PARTITION.key(), "dt")
                        .column("col", DataTypes.INT())
                        .column("dt", DataTypes.STRING())
                        .partitionKeys("dt")
                        .build();

        restCatalog.createDatabase(databaseName, true);
        restCatalog.createTable(identifier, schema, true);

        BatchWriteBuilder writeBuilder = catalog.getTable(identifier).newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (Map<String, String> partitionSpec : partitionSpecs) {
                write.write(GenericRow.of(0, BinaryString.fromString(partitionSpec.get("dt"))));
            }
            commit.commit(write.prepareCommit());
        }

        List<Partition> restPartitions = restCatalog.listPartitions(identifier);
        if (metastore) {
            assertThat(restPartitions.stream().map(Partition::spec)).containsExactly(sortedSpecs);
        } else {
            assertThat(restPartitions.stream().map(Partition::spec))
                    .containsExactlyInAnyOrder(sortedSpecs);
        }
    }

    @Test
    public void testListPartitionsPaged() throws Exception {
        if (!supportPartitions()) {
            return;
        }

        String databaseName = "partitions_paged_db";
        List<Map<String, String>> partitionSpecs =
                Arrays.asList(
                        singletonMap("dt", "20250101"),
                        singletonMap("dt", "20250102"),
                        singletonMap("dt", "20240102"),
                        singletonMap("dt", "20260101"),
                        singletonMap("dt", "20250104"),
                        singletonMap("dt", "20250103"),
                        singletonMap("dt", "2025010_test"));
        catalog.dropDatabase(databaseName, true, true);
        catalog.createDatabase(databaseName, true);
        Identifier identifier = Identifier.create(databaseName, "table");

        assertThrows(
                Catalog.TableNotExistException.class,
                () -> catalog.listPartitionsPaged(identifier, 10, "dt=20250101", null));

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
        PagedList<Partition> pagedPartitions =
                catalog.listPartitionsPaged(identifier, null, null, null);
        Map[] sortedSpecs =
                partitionSpecs.stream()
                        .sorted((o1, o2) -> o2.get("dt").compareTo(o1.get("dt")))
                        .toArray(Map[]::new);
        assertPagedPartitions(pagedPartitions, partitionSpecs.size(), sortedSpecs);

        int maxResults = 2;
        pagedPartitions = catalog.listPartitionsPaged(identifier, maxResults, null, null);
        assertPagedPartitions(pagedPartitions, maxResults, sortedSpecs[0], sortedSpecs[1]);
        assertEquals(sortedSpecs[1].toString(), "{" + pagedPartitions.getNextPageToken() + "}");

        pagedPartitions =
                catalog.listPartitionsPaged(
                        identifier, maxResults, pagedPartitions.getNextPageToken(), null);
        assertPagedPartitions(pagedPartitions, maxResults, sortedSpecs[2], sortedSpecs[3]);
        assertEquals(sortedSpecs[3].toString(), "{" + pagedPartitions.getNextPageToken() + "}");

        pagedPartitions =
                catalog.listPartitionsPaged(
                        identifier, maxResults, pagedPartitions.getNextPageToken(), null);
        assertPagedPartitions(pagedPartitions, maxResults, sortedSpecs[4], sortedSpecs[5]);
        assertEquals(sortedSpecs[5].toString(), "{" + pagedPartitions.getNextPageToken() + "}");

        pagedPartitions =
                catalog.listPartitionsPaged(
                        identifier, maxResults, pagedPartitions.getNextPageToken(), null);
        assertPagedPartitions(pagedPartitions, 1, sortedSpecs[6]);
        assertNull(pagedPartitions.getNextPageToken());

        maxResults = 8;
        pagedPartitions = catalog.listPartitionsPaged(identifier, maxResults, null, null);

        assertPagedPartitions(
                pagedPartitions, Math.min(maxResults, partitionSpecs.size()), sortedSpecs);
        assertNull(pagedPartitions.getNextPageToken());

        pagedPartitions = catalog.listPartitionsPaged(identifier, maxResults, null, "dt=2025");
        assertTrue(pagedPartitions.getElements().isEmpty());
        assertNull(pagedPartitions.getNextPageToken());

        pagedPartitions = catalog.listPartitionsPaged(identifier, maxResults, null, "dt=2025%");
        assertPagedPartitions(
                pagedPartitions,
                5,
                partitionSpecs.get(6),
                partitionSpecs.get(4),
                partitionSpecs.get(5),
                partitionSpecs.get(1),
                partitionSpecs.get(0));
        assertNull(pagedPartitions.getNextPageToken());

        pagedPartitions = catalog.listPartitionsPaged(identifier, maxResults, null, "dt=2025010_%");
        assertPagedPartitions(pagedPartitions, 1, partitionSpecs.get(6));
        assertNull(pagedPartitions.getNextPageToken());

        pagedPartitions = catalog.listPartitionsPaged(identifier, maxResults, null, "dt=2025010_");
        assertTrue(pagedPartitions.getElements().isEmpty());
        assertNull(pagedPartitions.getNextPageToken());

        assertThrows(
                BadRequestException.class,
                () -> catalog.listPartitionsPaged(identifier, null, null, "dt=%0101"));

        assertThrows(
                BadRequestException.class,
                () -> catalog.listPartitionsPaged(identifier, null, null, "dt=01%01"));
    }

    @Test
    public void testListPartitionsPagedWithMultiLevel() throws Exception {
        if (!supportPartitions()) {
            return;
        }

        String databaseName = "partitions_paged_db";
        Map<String, String> partitionSpec =
                new HashMap<String, String>() {
                    {
                        put("dt", "20250101");
                        put("col", "0");
                    }
                };

        Map<String, String> partitionSpec2 =
                new HashMap<String, String>() {
                    {
                        put("dt", "20250102");
                        put("col", "0");
                    }
                };
        List<Map<String, String>> partitionSpecs = Arrays.asList(partitionSpec, partitionSpec2);
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
                        .partitionKeys("dt", "col")
                        .build(),
                true);

        BatchWriteBuilder writeBuilder = catalog.getTable(identifier).newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (Map<String, String> partition : partitionSpecs) {
                write.write(GenericRow.of(0, BinaryString.fromString(partition.get("dt"))));
            }
            commit.commit(write.prepareCommit());
        }
        PagedList<Partition> pagedPartitions =
                catalog.listPartitionsPaged(identifier, null, null, "dt=20250101/col=0");
        assertPagedPartitions(pagedPartitions, 1, partitionSpecs.get(0));

        pagedPartitions = catalog.listPartitionsPaged(identifier, null, null, "dt=20250102%");
        assertPagedPartitions(pagedPartitions, 1, partitionSpecs.get(1));
    }

    @Test
    void testRefreshFileIO() throws Exception {
        this.catalog = newRestCatalogWithDataToken();
        List<Identifier> identifiers =
                Lists.newArrayList(
                        Identifier.create("test_db_a", "test_table_a"),
                        Identifier.create("test_db_b", "test_table_b"),
                        Identifier.create("test_db_c", "test_table_c"));
        for (Identifier identifier : identifiers) {
            createTable(identifier, Maps.newHashMap(), Lists.newArrayList("col1"));
            FileStoreTable fileStoreTable = (FileStoreTable) catalog.getTable(identifier);
            assertEquals(true, fileStoreTable.fileIO().exists(fileStoreTable.location()));

            RESTTokenFileIO fileIO = (RESTTokenFileIO) fileStoreTable.fileIO();
            RESTToken fileDataToken = fileIO.validToken();
            RESTToken serverDataToken = getDataTokenFromRestServer(identifier);
            assertEquals(serverDataToken, fileDataToken);
        }
    }

    @Test
    void testRefreshFileIOWhenExpired() throws Exception {
        this.catalog = newRestCatalogWithDataToken();
        Identifier identifier =
                Identifier.create("test_data_token", "table_for_testing_date_token");
        RESTToken expiredDataToken =
                new RESTToken(
                        ImmutableMap.of("akId", "akId", "akSecret", UUID.randomUUID().toString()),
                        System.currentTimeMillis() + 3600_000L);
        setDataTokenToRestServerForMock(identifier, expiredDataToken);
        createTable(identifier, Maps.newHashMap(), Lists.newArrayList("col1"));
        FileStoreTable fileStoreTable = (FileStoreTable) catalog.getTable(identifier);
        RESTTokenFileIO fileIO = (RESTTokenFileIO) fileStoreTable.fileIO();
        RESTToken fileDataToken = fileIO.validToken();
        assertEquals(expiredDataToken, fileDataToken);
        RESTToken newDataToken =
                new RESTToken(
                        ImmutableMap.of("akId", "akId", "akSecret", UUID.randomUUID().toString()),
                        System.currentTimeMillis() + 4000_000L);
        setDataTokenToRestServerForMock(identifier, newDataToken);
        RESTToken nextFileDataToken = fileIO.validToken();
        assertEquals(newDataToken, nextFileDataToken);
        assertEquals(true, nextFileDataToken.expireAtMillis() - fileDataToken.expireAtMillis() > 0);
    }

    @Test
    void testSnapshotFromREST() throws Exception {
        RESTCatalog catalog = (RESTCatalog) this.catalog;
        Identifier hasSnapshotTableIdentifier = Identifier.create("test_db_a", "my_snapshot_table");

        assertThrows(
                Catalog.TableNotExistException.class,
                () -> restCatalog.loadSnapshot(hasSnapshotTableIdentifier));

        assertThrows(
                Catalog.TableNotExistException.class,
                () ->
                        restCatalog.commitSnapshot(
                                hasSnapshotTableIdentifier,
                                "",
                                createSnapshotWithMillis(1L, System.currentTimeMillis()),
                                new ArrayList<>()));

        createTable(hasSnapshotTableIdentifier, Maps.newHashMap(), Lists.newArrayList("col1"));

        assertThrows(
                Catalog.TableNotExistException.class,
                () ->
                        restCatalog.commitSnapshot(
                                hasSnapshotTableIdentifier,
                                "unknown_id",
                                createSnapshotWithMillis(1L, System.currentTimeMillis()),
                                new ArrayList<>()));

        long id = 10086;
        long millis = System.currentTimeMillis();
        updateSnapshotOnRestServer(
                hasSnapshotTableIdentifier, createSnapshotWithMillis(id, millis), 1, 2, 3, 4);
        Optional<TableSnapshot> snapshot = catalog.loadSnapshot(hasSnapshotTableIdentifier);
        assertThat(snapshot).isPresent();
        assertThat(snapshot.get().snapshot().id()).isEqualTo(id);
        assertThat(snapshot.get().snapshot().timeMillis()).isEqualTo(millis);
        assertThat(snapshot.get().recordCount()).isEqualTo(1);
        assertThat(snapshot.get().fileSizeInBytes()).isEqualTo(2);
        assertThat(snapshot.get().fileCount()).isEqualTo(3);
        assertThat(snapshot.get().lastFileCreationTime()).isEqualTo(4);

        // drop table then create table
        catalog.dropTable(hasSnapshotTableIdentifier, true);
        createTable(hasSnapshotTableIdentifier, Maps.newHashMap(), Lists.newArrayList("col1"));
        snapshot = catalog.loadSnapshot(hasSnapshotTableIdentifier);
        assertThat(snapshot).isEmpty();
        updateSnapshotOnRestServer(
                hasSnapshotTableIdentifier, createSnapshotWithMillis(id, millis), 5, 6, 7, 8);
        snapshot = catalog.loadSnapshot(hasSnapshotTableIdentifier);
        assertThat(snapshot.get().recordCount()).isEqualTo(5);

        // test no snapshot
        catalog.loadSnapshot(hasSnapshotTableIdentifier);
        createTable(hasSnapshotTableIdentifier, Maps.newHashMap(), Lists.newArrayList("col1"));
        Identifier noSnapshotTableIdentifier = Identifier.create("test_db_a_1", "unknown");
        createTable(noSnapshotTableIdentifier, Maps.newHashMap(), Lists.newArrayList("col1"));
        snapshot = catalog.loadSnapshot(noSnapshotTableIdentifier);
        assertThat(snapshot).isEmpty();
    }

    @Test
    public void testTableRollback() throws Exception {
        Identifier identifier = Identifier.create("test_rollback", "table_for_rollback");
        createTable(identifier, Maps.newHashMap(), Lists.newArrayList("col1"));
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
        StreamTableWrite write = table.newWrite("commitUser");
        StreamTableCommit commit = table.newCommit("commitUser");
        for (int i = 0; i < 10; i++) {
            GenericRow record = GenericRow.of(i);
            write.write(record);
            commit.commit(i, write.prepareCommit(false, i));
            table.createTag("tag-" + i);
        }
        write.close();
        commit.close();
        long rollbackToSnapshotId = 4;
        table.rollbackTo(rollbackToSnapshotId);
        assertThat(table.snapshotManager().snapshot(rollbackToSnapshotId))
                .isEqualTo(restCatalog.loadSnapshot(identifier).get().snapshot());
        assertThat(table.tagManager().tagExists("tag-" + (rollbackToSnapshotId + 2))).isFalse();
        assertThat(table.snapshotManager().snapshotExists(rollbackToSnapshotId + 1)).isFalse();

        assertThrows(
                IllegalArgumentException.class, () -> table.rollbackTo(rollbackToSnapshotId + 1));

        String rollbackToTagName = "tag-" + (rollbackToSnapshotId - 1);
        table.rollbackTo(rollbackToTagName);
        Snapshot tagSnapshot = table.tagManager().getOrThrow(rollbackToTagName).trimToSnapshot();
        assertThat(tagSnapshot).isEqualTo(restCatalog.loadSnapshot(identifier).get().snapshot());
    }

    @Test
    public void testDataTokenExpired() throws Exception {
        this.catalog = newRestCatalogWithDataToken();
        Identifier identifier =
                Identifier.create("test_data_token", "table_for_expired_date_token");
        createTable(identifier, Maps.newHashMap(), Lists.newArrayList("col1"));
        RESTToken expiredDataToken =
                new RESTToken(
                        ImmutableMap.of(
                                "akId", "akId-expire", "akSecret", UUID.randomUUID().toString()),
                        System.currentTimeMillis() - 100_000);
        setDataTokenToRestServerForMock(identifier, expiredDataToken);
        FileStoreTable tableTestWrite = (FileStoreTable) catalog.getTable(identifier);
        List<Integer> data = Lists.newArrayList(12);
        Exception exception =
                assertThrows(UncheckedIOException.class, () -> batchWrite(tableTestWrite, data));
        assertEquals(RESTTestFileIO.TOKEN_EXPIRED_MSG, exception.getCause().getMessage());
        RESTToken dataToken =
                new RESTToken(
                        ImmutableMap.of("akId", "akId", "akSecret", UUID.randomUUID().toString()),
                        System.currentTimeMillis() + 100_000);
        setDataTokenToRestServerForMock(identifier, dataToken);
        batchWrite(tableTestWrite, data);
        List<String> actual = batchRead(tableTestWrite);
        assertThat(actual).containsExactlyInAnyOrder("+I[12]");
    }

    @Test
    public void testDataTokenUnExistInServer() throws Exception {
        this.catalog = newRestCatalogWithDataToken();
        Identifier identifier =
                Identifier.create("test_data_token", "table_for_un_exist_date_token");
        createTable(identifier, Maps.newHashMap(), Lists.newArrayList("col1"));
        FileStoreTable tableTestWrite = (FileStoreTable) catalog.getTable(identifier);
        RESTTokenFileIO restTokenFileIO = (RESTTokenFileIO) tableTestWrite.fileIO();
        List<Integer> data = Lists.newArrayList(12);
        // as RESTTokenFileIO is lazy so we need to call isObjectStore() to init fileIO
        restTokenFileIO.isObjectStore();
        resetDataTokenOnRestServer(identifier);
        Exception exception =
                assertThrows(UncheckedIOException.class, () -> batchWrite(tableTestWrite, data));
        assertEquals(RESTTestFileIO.TOKEN_UN_EXIST_MSG, exception.getCause().getMessage());
    }

    @Test
    public void testBatchRecordsWrite() throws Exception {
        Identifier tableIdentifier = Identifier.create("my_db", "my_table");
        createTable(tableIdentifier, Maps.newHashMap(), Lists.newArrayList("col1"));
        FileStoreTable tableTestWrite = (FileStoreTable) catalog.getTable(tableIdentifier);
        // write
        batchWrite(tableTestWrite, Lists.newArrayList(12, 5, 18));

        // read
        List<String> result = batchRead(tableTestWrite);
        assertThat(result).containsExactlyInAnyOrder("+I[5]", "+I[12]", "+I[18]");
    }

    @Test
    public void testBranchBatchRecordsWrite() throws Exception {
        Identifier tableIdentifier = Identifier.create("my_db", "my_table");

        Identifier tableBranchIdentifier =
                new Identifier(
                        tableIdentifier.getDatabaseName(),
                        tableIdentifier.getTableName(),
                        "branch1");
        createTable(tableIdentifier, Maps.newHashMap(), Lists.newArrayList("col1"));
        FileStoreTable tableTestWrite = (FileStoreTable) catalog.getTable(tableIdentifier);
        // write
        batchWrite(tableTestWrite, Lists.newArrayList(12, 5, 18));
        restCatalog.createBranch(tableIdentifier, tableBranchIdentifier.getBranchName(), null);
        FileStoreTable branchTableTestWrite =
                (FileStoreTable) catalog.getTable(tableBranchIdentifier);
        batchWrite(branchTableTestWrite, Lists.newArrayList(1, 9, 2));
        // read
        List<String> result = batchRead(tableTestWrite);
        List<String> branchResult = batchRead(branchTableTestWrite);
        assertThat(result).containsExactlyInAnyOrder("+I[5]", "+I[12]", "+I[18]");
        assertThat(branchResult)
                .containsExactlyInAnyOrder("+I[5]", "+I[12]", "+I[18]", "+I[2]", "+I[1]", "+I[9]");
    }

    @Test
    void testBranches() throws Exception {
        String databaseName = "testBranchTable";
        catalog.dropDatabase(databaseName, true, true);
        catalog.createDatabase(databaseName, true);
        Identifier identifier = Identifier.create(databaseName, "table");

        assertThrows(
                Catalog.TableNotExistException.class,
                () -> restCatalog.createBranch(identifier, "my_branch", null));

        assertThrows(
                Catalog.TableNotExistException.class, () -> restCatalog.listBranches(identifier));

        catalog.createTable(
                identifier, Schema.newBuilder().column("col", DataTypes.INT()).build(), true);
        assertThrows(
                Catalog.TagNotExistException.class,
                () -> restCatalog.createBranch(identifier, "my_branch", "tag"));
        restCatalog.createBranch(identifier, "my_branch", null);
        Identifier branchIdentifier = new Identifier(databaseName, "table", "my_branch");
        assertThat(restCatalog.getTable(branchIdentifier)).isNotNull();
        assertThrows(
                Catalog.BranchAlreadyExistException.class,
                () -> restCatalog.createBranch(identifier, "my_branch", null));
        assertThat(restCatalog.listBranches(identifier)).containsOnly("my_branch");
        restCatalog.dropBranch(identifier, "my_branch");

        assertThrows(
                Catalog.BranchNotExistException.class,
                () -> restCatalog.dropBranch(identifier, "no_exist_branch"));
        assertThrows(
                Catalog.BranchNotExistException.class,
                () -> restCatalog.fastForward(identifier, "no_exist_branch"));
        assertThat(restCatalog.listBranches(identifier)).isEmpty();
    }

    @Test
    void testListDataFromPageApiWhenLastPageTokenIsNull() {
        List<Integer> testData = ImmutableList.of(1, 2, 3, 4, 5, 6, 7);
        int maxResults = 2;
        AtomicInteger fetchTimes = new AtomicInteger(0);
        List<Integer> fetchData =
                restCatalog
                        .api()
                        .listDataFromPageApi(
                                queryParams ->
                                        generateTestPagedResponse(
                                                queryParams,
                                                testData,
                                                maxResults,
                                                fetchTimes,
                                                true));
        assertEquals(fetchTimes.get(), 4);
        assertThat(fetchData).containsSequence(testData);
    }

    @Test
    void testListDataFromPageApiWhenLastPageTokenIsNotNullAndDataIsNull() {
        List<Integer> testData = ImmutableList.of(1, 2, 3, 4, 5, 6);
        int maxResults = 2;
        AtomicInteger fetchTimes = new AtomicInteger(0);
        List<Integer> fetchData =
                restCatalog
                        .api()
                        .listDataFromPageApi(
                                queryParams -> {
                                    return generateTestPagedResponse(
                                            queryParams, testData, maxResults, fetchTimes, false);
                                });

        assertEquals(fetchTimes.get(), testData.size() / maxResults + 1);
        assertThat(fetchData).containsSequence(testData);
    }

    @Test
    void testAlterView() throws Exception {
        Identifier identifier = new Identifier("rest_catalog_db", "my_view");
        View view = createView(identifier);
        catalog.createDatabase(identifier.getDatabaseName(), false);
        ViewChange.AddDialect addDialect =
                (ViewChange.AddDialect)
                        ViewChange.addDialect("flink_1", "SELECT * FROM FLINK_TABLE_1");
        assertDoesNotThrow(() -> catalog.alterView(identifier, ImmutableList.of(addDialect), true));
        assertThrows(
                Catalog.ViewNotExistException.class,
                () -> catalog.alterView(identifier, ImmutableList.of(addDialect), false));
        catalog.createView(identifier, view, false);
        // set options
        String key = UUID.randomUUID().toString();
        String value = UUID.randomUUID().toString();
        ViewChange setOption = ViewChange.setOption(key, value);
        catalog.alterView(identifier, ImmutableList.of(setOption), false);
        View catalogView = catalog.getView(identifier);
        assertThat(catalogView.options().get(key)).isEqualTo(value);

        // remove options
        catalog.alterView(identifier, ImmutableList.of(ViewChange.removeOption(key)), false);
        catalogView = catalog.getView(identifier);
        assertThat(catalogView.options().containsKey(key)).isEqualTo(false);

        // update comment
        String newComment = "new comment";
        catalog.alterView(
                identifier, ImmutableList.of(ViewChange.updateComment(newComment)), false);
        catalogView = catalog.getView(identifier);
        assertThat(catalogView.comment().get()).isEqualTo(newComment);
        // add dialect
        catalog.alterView(identifier, ImmutableList.of(addDialect), false);
        catalogView = catalog.getView(identifier);
        assertThat(catalogView.query(addDialect.dialect())).isEqualTo(addDialect.query());
        assertThrows(
                Catalog.DialectAlreadyExistException.class,
                () -> catalog.alterView(identifier, ImmutableList.of(addDialect), false));

        // update dialect
        ViewChange.UpdateDialect updateDialect =
                (ViewChange.UpdateDialect)
                        ViewChange.updateDialect("flink_1", "SELECT * FROM FLINK_TABLE_2");
        catalog.alterView(identifier, ImmutableList.of(updateDialect), false);
        catalogView = catalog.getView(identifier);
        assertThat(catalogView.query(updateDialect.dialect())).isEqualTo(updateDialect.query());
        assertThrows(
                Catalog.DialectNotExistException.class,
                () ->
                        catalog.alterView(
                                identifier,
                                ImmutableList.of(
                                        ViewChange.updateDialect(
                                                "no_exist", "SELECT * FROM FLINK_TABLE_2")),
                                false));

        // drop dialect
        ViewChange.DropDialect dropDialect =
                (ViewChange.DropDialect) ViewChange.dropDialect(updateDialect.dialect());
        catalog.alterView(identifier, ImmutableList.of(dropDialect), false);
        catalogView = catalog.getView(identifier);
        assertThat(catalogView.query(dropDialect.dialect())).isEqualTo(catalogView.query());
        assertThrows(
                Catalog.DialectNotExistException.class,
                () ->
                        catalog.alterView(
                                identifier,
                                ImmutableList.of(ViewChange.dropDialect("no_exist")),
                                false));
    }

    @Test
    void testFunction() throws Exception {
        Identifier identifierWithSlash = new Identifier("rest_catalog_db", "function/");
        catalog.createDatabase(identifierWithSlash.getDatabaseName(), false);
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        catalog.createFunction(
                                identifierWithSlash,
                                MockRESTMessage.function(identifierWithSlash),
                                false));
        assertThrows(
                Catalog.FunctionNotExistException.class,
                () -> catalog.getFunction(identifierWithSlash));
        assertThrows(
                IllegalArgumentException.class,
                () -> catalog.dropFunction(identifierWithSlash, true));

        Identifier identifierWithoutAlphabet = new Identifier("rest_catalog_db", "-");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        catalog.createFunction(
                                identifierWithoutAlphabet,
                                MockRESTMessage.function(identifierWithoutAlphabet),
                                false));
        assertThrows(
                Catalog.FunctionNotExistException.class,
                () -> catalog.getFunction(identifierWithoutAlphabet));
        assertThrows(
                IllegalArgumentException.class,
                () -> catalog.dropFunction(identifierWithoutAlphabet, true));

        Identifier identifier = Identifier.fromString("rest_catalog_db.function.na_me-01");
        Function function = MockRESTMessage.function(identifier);

        catalog.createFunction(identifier, function, true);
        assertThrows(
                Catalog.FunctionAlreadyExistException.class,
                () -> catalog.createFunction(identifier, function, false));

        assertThat(catalog.listFunctions(identifier.getDatabaseName()).contains(function.name()))
                .isTrue();

        Function getFunction = catalog.getFunction(identifier);
        assertThat(getFunction.name()).isEqualTo(function.name());
        for (String dialect : function.definitions().keySet()) {
            assertThat(getFunction.definition(dialect)).isEqualTo(function.definition(dialect));
        }
        catalog.dropFunction(identifier, true);

        assertThat(catalog.listFunctions(identifier.getDatabaseName()).contains(function.name()))
                .isFalse();
        assertThrows(
                Catalog.FunctionNotExistException.class,
                () -> catalog.dropFunction(identifier, false));
        assertThrows(
                Catalog.FunctionNotExistException.class, () -> catalog.getFunction(identifier));
    }

    @Test
    void testListFunctions() throws Exception {
        String db1 = "db_rest_catalog_db";
        String db2 = "db2_rest_catalog";
        Identifier identifier = new Identifier(db1, "list_function");
        Identifier identifier1 = new Identifier(db1, "function");
        Identifier identifier2 = new Identifier(db2, "list_function");
        Identifier identifier3 = new Identifier(db2, "function");
        catalog.createDatabase(db1, false);
        catalog.createDatabase(db2, false);
        catalog.createFunction(identifier, MockRESTMessage.function(identifier), true);
        catalog.createFunction(identifier1, MockRESTMessage.function(identifier1), true);
        catalog.createFunction(identifier2, MockRESTMessage.function(identifier2), true);
        catalog.createFunction(identifier3, MockRESTMessage.function(identifier3), true);
        assertThat(catalog.listFunctionsPaged(db1, null, null, null).getElements())
                .containsExactlyInAnyOrder(identifier.getObjectName(), identifier1.getObjectName());
        assertThat(catalog.listFunctionsPaged(db1, 1, null, null).getElements())
                .containsAnyOf(identifier.getObjectName(), identifier1.getObjectName());
        assertThat(
                        catalog.listFunctionsPaged(db1, 1, identifier1.getObjectName(), null)
                                .getElements())
                .containsExactlyInAnyOrder(identifier.getObjectName());
        assertThat(catalog.listFunctionsPaged(db1, null, null, "func%").getElements())
                .containsExactlyInAnyOrder(identifier1.getObjectName());
        assertThat(
                        catalog.listFunctionsPagedGlobally("db2_rest%", "func%", null, null)
                                .getElements())
                .containsExactlyInAnyOrder(identifier3);
        assertThat(catalog.listFunctionsPagedGlobally("db2_rest%", null, 1, null).getElements())
                .containsAnyOf(identifier2, identifier3);
        assertThat(
                        catalog.listFunctionsPagedGlobally(
                                        "db2_rest%", null, 1, identifier3.getFullName())
                                .getElements())
                .containsExactlyInAnyOrder(identifier2);

        assertThat(
                        catalog.listFunctionDetailsPaged(db1, 1, null, null).getElements().stream()
                                .map(f -> f.fullName())
                                .collect(Collectors.toList()))
                .containsAnyOf(identifier.getFullName(), identifier1.getFullName());

        assertThat(
                        catalog.listFunctionDetailsPaged(db2, 4, null, "func%").getElements()
                                .stream()
                                .map(f -> f.fullName())
                                .collect(Collectors.toList()))
                .containsExactly(identifier3.getFullName());

        assertThat(
                        catalog.listFunctionDetailsPaged(db2, 1, identifier3.getObjectName(), null)
                                .getElements().stream()
                                .map(f -> f.fullName())
                                .collect(Collectors.toList()))
                .contains(identifier2.getFullName());
    }

    @Test
    void testAlterFunction() throws Exception {
        Identifier identifier = new Identifier("rest_catalog_db", "alter_function_name");
        catalog.createDatabase(identifier.getDatabaseName(), false);
        Function function = MockRESTMessage.function(identifier);
        FunctionDefinition definition = FunctionDefinition.sql("x * y + 1");
        FunctionChange.AddDefinition addDefinition =
                (FunctionChange.AddDefinition) FunctionChange.addDefinition("flink_1", definition);
        assertDoesNotThrow(
                () -> catalog.alterFunction(identifier, ImmutableList.of(addDefinition), true));
        assertThrows(
                Catalog.FunctionNotExistException.class,
                () -> catalog.alterFunction(identifier, ImmutableList.of(addDefinition), false));
        catalog.createFunction(identifier, function, true);
        // set options
        String key = UUID.randomUUID().toString();
        String value = UUID.randomUUID().toString();
        FunctionChange setOption = FunctionChange.setOption(key, value);
        catalog.alterFunction(identifier, ImmutableList.of(setOption), false);
        Function catalogFunction = catalog.getFunction(identifier);
        assertThat(catalogFunction.options().get(key)).isEqualTo(value);

        // remove options
        catalog.alterFunction(
                identifier, ImmutableList.of(FunctionChange.removeOption(key)), false);
        catalogFunction = catalog.getFunction(identifier);
        assertThat(catalogFunction.options().containsKey(key)).isEqualTo(false);

        // update comment
        String newComment = "new comment";
        catalog.alterFunction(
                identifier, ImmutableList.of(FunctionChange.updateComment(newComment)), false);
        catalogFunction = catalog.getFunction(identifier);
        assertThat(catalogFunction.comment()).isEqualTo(newComment);
        // add definition
        catalog.alterFunction(identifier, ImmutableList.of(addDefinition), false);
        catalogFunction = catalog.getFunction(identifier);
        assertThat(catalogFunction.definition(addDefinition.name()))
                .isEqualTo(addDefinition.definition());
        assertThrows(
                Catalog.DefinitionAlreadyExistException.class,
                () -> catalog.alterFunction(identifier, ImmutableList.of(addDefinition), false));

        // update definition
        FunctionChange.UpdateDefinition updateDefinition =
                (FunctionChange.UpdateDefinition)
                        FunctionChange.updateDefinition("flink_1", definition);
        catalog.alterFunction(identifier, ImmutableList.of(updateDefinition), false);
        catalogFunction = catalog.getFunction(identifier);
        assertThat(catalogFunction.definition(updateDefinition.name()))
                .isEqualTo(updateDefinition.definition());
        assertThrows(
                Catalog.DefinitionNotExistException.class,
                () ->
                        catalog.alterFunction(
                                identifier,
                                ImmutableList.of(
                                        FunctionChange.updateDefinition("no_exist", definition)),
                                false));

        // drop dialect
        FunctionChange.DropDefinition dropDefinition =
                (FunctionChange.DropDefinition)
                        FunctionChange.dropDefinition(updateDefinition.name());
        catalog.alterFunction(identifier, ImmutableList.of(dropDefinition), false);
        catalogFunction = catalog.getFunction(identifier);
        assertThat(catalogFunction.definition(updateDefinition.name())).isNull();

        assertThrows(
                Catalog.DefinitionNotExistException.class,
                () -> catalog.alterFunction(identifier, ImmutableList.of(dropDefinition), false));
    }

    @Test
    public void testValidateFunctionName() throws Exception {
        assertDoesNotThrow(() -> RESTFunctionValidator.checkFunctionName("a"));
        assertDoesNotThrow(() -> RESTFunctionValidator.checkFunctionName("a1_"));
        assertDoesNotThrow(() -> RESTFunctionValidator.checkFunctionName("a-b_c"));
        assertDoesNotThrow(() -> RESTFunctionValidator.checkFunctionName("a-b_c.1"));

        assertThrows(
                IllegalArgumentException.class,
                () -> RESTFunctionValidator.checkFunctionName("a\\/b"));
        assertThrows(
                IllegalArgumentException.class,
                () -> RESTFunctionValidator.checkFunctionName("a$?b"));
        assertThrows(
                IllegalArgumentException.class,
                () -> RESTFunctionValidator.checkFunctionName("a@b"));
        assertThrows(
                IllegalArgumentException.class,
                () -> RESTFunctionValidator.checkFunctionName("a*b"));
        assertThrows(
                IllegalArgumentException.class,
                () -> RESTFunctionValidator.checkFunctionName("123"));
        assertThrows(
                IllegalArgumentException.class,
                () -> RESTFunctionValidator.checkFunctionName("_-"));
        assertThrows(
                IllegalArgumentException.class, () -> RESTFunctionValidator.checkFunctionName(""));
        assertThrows(
                IllegalArgumentException.class,
                () -> RESTFunctionValidator.checkFunctionName(null));
    }

    @Test
    void testTableAuth() throws Exception {
        Identifier identifier = Identifier.create("test_table_db", "auth_table");
        catalog.createDatabase(identifier.getDatabaseName(), true);
        catalog.createTable(
                identifier,
                new Schema(
                        Lists.newArrayList(
                                new DataField(0, "col1", DataTypes.INT()),
                                new DataField(1, "col2", DataTypes.INT())),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        singletonMap(QUERY_AUTH_ENABLED.key(), "true"),
                        ""),
                true);
        authTableColumns(identifier, singletonList("col2"));
        Table table = catalog.getTable(identifier);

        assertThatThrownBy(() -> table.newReadBuilder().newScan().plan())
                .hasMessageContaining("Table test_table_db.auth_table has no permission.");

        // no exception
        table.newReadBuilder().withProjection(new int[] {1}).newScan().plan();
    }

    @Test
    void testSnapshotMethods() throws Exception {
        Identifier identifier = Identifier.create("test_table_db", "snapshots_table");
        catalog.createDatabase(identifier.getDatabaseName(), true);
        catalog.createTable(
                identifier,
                new Schema(
                        Lists.newArrayList(new DataField(0, "col", DataTypes.INT())),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        emptyMap(),
                        ""),
                true);
        Table table = catalog.getTable(identifier);

        assertThat(catalog.loadSnapshot(identifier, "EARLIEST")).isEmpty();
        assertThat(catalog.loadSnapshot(identifier, "LATEST")).isEmpty();

        batchWrite(table, singletonList(1));
        batchWrite(table, singletonList(1));
        batchWrite(table, singletonList(1));
        batchWrite(table, singletonList(1));

        assertThat(catalog.listSnapshotsPaged(identifier, null, null).getElements())
                .containsExactlyInAnyOrder(
                        table.snapshot(1), table.snapshot(2), table.snapshot(3), table.snapshot(4));

        assertThat(catalog.loadSnapshot(identifier, "3"))
                .isPresent()
                .get()
                .isEqualTo(table.snapshot(3));

        assertThat(catalog.loadSnapshot(identifier, "EARLIEST"))
                .isPresent()
                .get()
                .isEqualTo(table.snapshot(1));

        assertThat(catalog.loadSnapshot(identifier, "8")).isEmpty();

        assertThat(catalog.loadSnapshot(identifier, "LATEST"))
                .isPresent()
                .get()
                .isEqualTo(table.snapshot(4));

        table.createTag("MY_TAG", 2);
        assertThat(catalog.loadSnapshot(identifier, "MY_TAG"))
                .isPresent()
                .get()
                .isEqualTo(table.snapshot(2));

        assertThat(catalog.loadSnapshot(identifier, "NONE_TAG")).isEmpty();

        assertThat(catalog.loadSnapshot(identifier, "15")).isEmpty();

        // test more snapshots
        for (int i = 0; i < 10; i++) {
            batchWrite(table, singletonList(1));
        }
        RESTApi api = ((RESTCatalog) catalog).api();
        List<Snapshot> snapshots =
                PagedList.listAllFromPagedApi(
                        token -> api.listSnapshotsPaged(identifier, null, token));
        assertThat(snapshots)
                .containsExactlyInAnyOrder(
                        table.snapshot(1),
                        table.snapshot(2),
                        table.snapshot(3),
                        table.snapshot(4),
                        table.snapshot(5),
                        table.snapshot(6),
                        table.snapshot(7),
                        table.snapshot(8),
                        table.snapshot(9),
                        table.snapshot(10),
                        table.snapshot(11),
                        table.snapshot(12),
                        table.snapshot(13),
                        table.snapshot(14));

        // expire snapshots
        SnapshotManager snapshotManager = ((FileStoreTable) table).snapshotManager();
        snapshotManager.deleteSnapshot(1);
        snapshotManager.deleteSnapshot(2);
        snapshots =
                PagedList.listAllFromPagedApi(
                        token -> api.listSnapshotsPaged(identifier, null, token));
        assertThat(snapshots)
                .containsExactlyInAnyOrder(
                        table.snapshot(3),
                        table.snapshot(4),
                        table.snapshot(5),
                        table.snapshot(6),
                        table.snapshot(7),
                        table.snapshot(8),
                        table.snapshot(9),
                        table.snapshot(10),
                        table.snapshot(11),
                        table.snapshot(12),
                        table.snapshot(13),
                        table.snapshot(14));
    }

    private TestPagedResponse generateTestPagedResponse(
            Map<String, String> queryParams,
            List<Integer> testData,
            int maxResults,
            AtomicInteger fetchTimes,
            boolean supportPageTokenNull) {
        String nextToken = queryParams.getOrDefault(PAGE_TOKEN, null);
        fetchTimes.incrementAndGet();
        if (nextToken == null) {
            return new TestPagedResponse(maxResults + "", testData.subList(0, maxResults));
        } else {
            int index = Integer.parseInt(nextToken);
            if (index >= testData.size()) {
                return new TestPagedResponse(null, null);
            } else {
                int endIndex = Math.min((index + maxResults), testData.size());
                String nextPageToken =
                        supportPageTokenNull && endIndex >= (testData.size())
                                ? null
                                : endIndex + "";
                return new TestPagedResponse(nextPageToken, testData.subList(index, endIndex));
            }
        }
    }

    @Override
    protected boolean supportsFormatTable() {
        return true;
    }

    @Override
    protected boolean supportPartitions() {
        return true;
    }

    @Override
    protected boolean supportsView() {
        return true;
    }

    @Override
    protected boolean supportPagedList() {
        return true;
    }

    @Override
    protected boolean supportsAlterDatabase() {
        return true;
    }

    // TODO implement this
    @Override
    @Test
    public void testTableUUID() {}

    protected void createTable(
            Identifier identifier, Map<String, String> options, List<String> partitionKeys)
            throws Exception {
        catalog.createDatabase(identifier.getDatabaseName(), true);
        catalog.createTable(
                identifier,
                new Schema(
                        Lists.newArrayList(new DataField(0, "col1", DataTypes.INT())),
                        partitionKeys,
                        Collections.emptyList(),
                        options,
                        ""),
                true);
    }

    protected abstract Catalog newRestCatalogWithDataToken() throws IOException;

    protected abstract void revokeTablePermission(Identifier identifier);

    protected abstract void authTableColumns(Identifier identifier, List<String> columns);

    protected abstract void revokeDatabasePermission(String database);

    protected abstract RESTToken getDataTokenFromRestServer(Identifier identifier);

    protected abstract void setDataTokenToRestServerForMock(
            Identifier identifier, RESTToken expiredDataToken);

    protected abstract void resetDataTokenOnRestServer(Identifier identifier);

    protected abstract void updateSnapshotOnRestServer(
            Identifier identifier,
            Snapshot snapshot,
            long recordCount,
            long fileSizeInBytes,
            long fileCount,
            long lastFileCreationTime);

    protected void batchWrite(Table table, List<Integer> data) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        BatchTableWrite write = writeBuilder.newWrite();
        for (Integer i : data) {
            GenericRow record = GenericRow.of(i);
            write.write(record);
        }
        List<CommitMessage> messages = write.prepareCommit();
        BatchTableCommit commit = writeBuilder.newCommit();
        commit.commit(messages);
        write.close();
        commit.close();
    }

    protected List<String> batchRead(Table table) throws IOException {
        ReadBuilder readBuilder = table.newReadBuilder();
        List<Split> splits = readBuilder.newScan().plan().splits();
        TableRead read = readBuilder.newRead();
        RecordReader<InternalRow> reader = read.createReader(splits);
        List<String> result = new ArrayList<>();
        reader.forEachRemaining(
                row -> {
                    String rowStr =
                            String.format("%s[%d]", row.getRowKind().shortString(), row.getInt(0));
                    result.add(rowStr);
                });
        return result;
    }

    protected void generateTokenAndWriteToFile(String tokenPath) throws IOException {
        File tokenFile = new File(tokenPath);
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        String expiration = now.format(TOKEN_DATE_FORMATTER);
        String secret = UUID.randomUUID().toString();
        DLFToken token = new DLFToken("accessKeyId", secret, "securityToken", expiration);
        String tokenStr = RESTApi.toJson(token);
        FileUtils.writeStringToFile(tokenFile, tokenStr);
    }
}
