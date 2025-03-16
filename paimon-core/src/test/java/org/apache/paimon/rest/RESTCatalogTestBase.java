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
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogTestBase;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.PropertyChange;
import org.apache.paimon.catalog.SupportsBranches;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.rest.auth.DLFToken;
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
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.view.View;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;
import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;
import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.paimon.CoreOptions.METASTORE_PARTITIONED_TABLE;
import static org.apache.paimon.CoreOptions.METASTORE_TAG_TO_PARTITION;
import static org.apache.paimon.catalog.Catalog.SYSTEM_DATABASE_NAME;
import static org.apache.paimon.rest.RESTCatalog.PAGE_TOKEN;
import static org.apache.paimon.rest.auth.DLFAuthProvider.TOKEN_DATE_FORMATTER;
import static org.apache.paimon.utils.SnapshotManagerTest.createSnapshotWithMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Base test class for {@link RESTCatalog}. */
public abstract class RESTCatalogTestBase extends CatalogTestBase {

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
        PagedList<String> pagedDatabases = catalog.listDatabasesPaged(null, null);
        assertThat(pagedDatabases.getElements()).isEmpty();
        assertNull(pagedDatabases.getNextPageToken());

        String[] dbNames = {"ghj", "db1", "db2", "db3", "ert"};
        for (String dbName : dbNames) {
            catalog.createDatabase(dbName, true);
        }

        // when maxResults is null or 0, the page length is set to a server configured value
        String[] sortedDbNames = Arrays.stream(dbNames).sorted().toArray(String[]::new);
        pagedDatabases = catalog.listDatabasesPaged(null, null);
        List<String> dbs = pagedDatabases.getElements();
        assertThat(dbs).containsExactly(sortedDbNames);
        assertNull(pagedDatabases.getNextPageToken());

        // when maxResults is greater than 0, the page length is the minimum of this value and a
        // server configured value
        // when pageToken is null, will list tables from the beginning
        int maxResults = 2;
        pagedDatabases = catalog.listDatabasesPaged(maxResults, null);
        dbs = pagedDatabases.getElements();
        assertEquals(maxResults, dbs.size());
        assertThat(dbs).containsExactly("db1", "db2");
        assertEquals("db2", pagedDatabases.getNextPageToken());

        // when pageToken is not null, will list tables from the pageToken (exclusive)
        pagedDatabases = catalog.listDatabasesPaged(maxResults, pagedDatabases.getNextPageToken());
        dbs = pagedDatabases.getElements();
        assertEquals(maxResults, dbs.size());
        assertThat(dbs).containsExactly("db3", "ert");
        assertEquals("ert", pagedDatabases.getNextPageToken());

        pagedDatabases = catalog.listDatabasesPaged(maxResults, pagedDatabases.getNextPageToken());
        dbs = pagedDatabases.getElements();
        assertEquals(1, dbs.size());
        assertThat(dbs).containsExactly("ghj");
        assertNull(pagedDatabases.getNextPageToken());

        maxResults = 8;
        pagedDatabases = catalog.listDatabasesPaged(maxResults, null);
        dbs = pagedDatabases.getElements();
        String[] expectedTableNames = Arrays.stream(dbNames).sorted().toArray(String[]::new);
        assertThat(dbs).containsExactly(expectedTableNames);
        assertNull(pagedDatabases.getNextPageToken());

        pagedDatabases = catalog.listDatabasesPaged(maxResults, "ddd");
        dbs = pagedDatabases.getElements();
        assertEquals(2, dbs.size());
        assertThat(dbs).containsExactly("ert", "ghj");
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
                () -> catalog.listTablesPaged(database, 100, null));
        assertThrows(
                Catalog.DatabaseNotExistException.class,
                () -> catalog.listTableDetailsPaged(database, 100, null));
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
                () -> catalog.listPartitionsPaged(identifier, 100, null));
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
                                createSnapshotWithMillis(1L, System.currentTimeMillis()),
                                new ArrayList<Partition>()));
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
        PagedList<String> pagedTables = catalog.listTablesPaged(databaseName, null, null);
        assertThat(pagedTables.getElements()).isEmpty();
        assertNull(pagedTables.getNextPageToken());

        String[] tableNames = {"table1", "table2", "table3", "abd", "def", "opr"};
        for (String tableName : tableNames) {
            catalog.createTable(
                    Identifier.create(databaseName, tableName), DEFAULT_TABLE_SCHEMA, false);
        }

        // when maxResults is null or 0, the page length is set to a server configured value
        String[] sortedTableNames = Arrays.stream(tableNames).sorted().toArray(String[]::new);
        pagedTables = catalog.listTablesPaged(databaseName, null, null);
        List<String> tables = pagedTables.getElements();
        assertThat(tables).containsExactly(sortedTableNames);
        assertNull(pagedTables.getNextPageToken());

        // when maxResults is greater than 0, the page length is the minimum of this value and a
        // server configured value
        // when pageToken is null, will list tables from the beginning
        int maxResults = 2;
        pagedTables = catalog.listTablesPaged(databaseName, maxResults, null);
        tables = pagedTables.getElements();
        assertEquals(maxResults, tables.size());
        assertThat(tables).containsExactly("abd", "def");
        assertEquals("def", pagedTables.getNextPageToken());

        // when pageToken is not null, will list tables from the pageToken (exclusive)
        pagedTables =
                catalog.listTablesPaged(databaseName, maxResults, pagedTables.getNextPageToken());
        tables = pagedTables.getElements();
        assertEquals(maxResults, tables.size());
        assertThat(tables).containsExactly("opr", "table1");
        assertEquals("table1", pagedTables.getNextPageToken());

        pagedTables =
                catalog.listTablesPaged(databaseName, maxResults, pagedTables.getNextPageToken());
        tables = pagedTables.getElements();
        assertEquals(maxResults, tables.size());
        assertThat(tables).containsExactly("table2", "table3");
        assertEquals("table3", pagedTables.getNextPageToken());

        pagedTables =
                catalog.listTablesPaged(databaseName, maxResults, pagedTables.getNextPageToken());
        tables = pagedTables.getElements();
        assertEquals(0, tables.size());
        assertNull(pagedTables.getNextPageToken());

        maxResults = 8;
        pagedTables = catalog.listTablesPaged(databaseName, maxResults, null);
        tables = pagedTables.getElements();
        assertThat(tables).containsExactly(sortedTableNames);
        assertNull(pagedTables.getNextPageToken());

        pagedTables = catalog.listTablesPaged(databaseName, maxResults, "table1");
        tables = pagedTables.getElements();
        assertEquals(2, tables.size());
        assertThat(tables).containsExactly("table2", "table3");
        assertNull(pagedTables.getNextPageToken());

        // List tables throws DatabaseNotExistException when the database does not exist
        assertThatExceptionOfType(Catalog.DatabaseNotExistException.class)
                .isThrownBy(() -> catalog.listTables("non_existing_db"));
    }

    @Test
    public void testListTableDetailsPaged() throws Exception {
        // List table details returns an empty list when there are no tables in the database
        String databaseName = "table_details_paged_db";
        catalog.createDatabase(databaseName, false);
        PagedList<Table> pagedTableDetails =
                catalog.listTableDetailsPaged(databaseName, null, null);
        assertThat(pagedTableDetails.getElements()).isEmpty();
        assertNull(pagedTableDetails.getNextPageToken());

        String[] tableNames = {"table1", "table2", "table3", "abd", "def", "opr"};
        String[] expectedTableNames = Arrays.stream(tableNames).sorted().toArray(String[]::new);
        for (String tableName : tableNames) {
            catalog.createTable(
                    Identifier.create(databaseName, tableName), DEFAULT_TABLE_SCHEMA, false);
        }

        pagedTableDetails = catalog.listTableDetailsPaged(databaseName, null, null);
        assertPagedTableDetails(pagedTableDetails, tableNames.length, expectedTableNames);
        assertNull(pagedTableDetails.getNextPageToken());

        int maxResults = 2;
        pagedTableDetails = catalog.listTableDetailsPaged(databaseName, maxResults, null);
        assertPagedTableDetails(pagedTableDetails, maxResults, "abd", "def");
        assertEquals("def", pagedTableDetails.getNextPageToken());

        pagedTableDetails =
                catalog.listTableDetailsPaged(
                        databaseName, maxResults, pagedTableDetails.getNextPageToken());
        assertPagedTableDetails(pagedTableDetails, maxResults, "opr", "table1");
        assertEquals("table1", pagedTableDetails.getNextPageToken());

        pagedTableDetails =
                catalog.listTableDetailsPaged(
                        databaseName, maxResults, pagedTableDetails.getNextPageToken());
        assertPagedTableDetails(pagedTableDetails, maxResults, "table2", "table3");
        assertEquals("table3", pagedTableDetails.getNextPageToken());

        pagedTableDetails =
                catalog.listTableDetailsPaged(
                        databaseName, maxResults, pagedTableDetails.getNextPageToken());
        assertEquals(0, pagedTableDetails.getElements().size());
        assertNull(pagedTableDetails.getNextPageToken());

        maxResults = 8;
        pagedTableDetails = catalog.listTableDetailsPaged(databaseName, maxResults, null);
        assertPagedTableDetails(
                pagedTableDetails, Math.min(maxResults, tableNames.length), expectedTableNames);
        assertNull(pagedTableDetails.getNextPageToken());

        String pageToken = "table1";
        pagedTableDetails = catalog.listTableDetailsPaged(databaseName, maxResults, pageToken);
        assertPagedTableDetails(pagedTableDetails, 2, "table2", "table3");
        assertNull(pagedTableDetails.getNextPageToken());

        // List table details throws DatabaseNotExistException when the database does not exist
        final int finalMaxResults = maxResults;
        assertThatExceptionOfType(Catalog.DatabaseNotExistException.class)
                .isThrownBy(
                        () ->
                                catalog.listTableDetailsPaged(
                                        "non_existing_db", finalMaxResults, pageToken));
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
        PagedList<String> pagedViews = catalog.listViewsPaged(databaseName, null, null);
        assertThat(pagedViews.getElements()).isEmpty();
        assertNull(pagedViews.getNextPageToken());

        // List views paged returns a list with the names of all views in the database in all
        // catalogs except RestCatalog
        // even if the maxResults or pageToken is not null
        View view = buildView(databaseName);
        String[] viewNames = {"view1", "view2", "view3", "abd", "def", "opr"};
        String[] sortedViewNames = Arrays.stream(viewNames).sorted().toArray(String[]::new);
        for (String viewName : viewNames) {
            catalog.createView(Identifier.create(databaseName, viewName), view, false);
        }

        pagedViews = catalog.listViewsPaged(databaseName, null, null);
        assertThat(pagedViews.getElements()).containsExactly(sortedViewNames);
        assertNull(pagedViews.getNextPageToken());

        int maxResults = 2;
        pagedViews = catalog.listViewsPaged(databaseName, maxResults, null);
        assertPagedViews(pagedViews, "abd", "def");
        assertEquals("def", pagedViews.getNextPageToken());

        pagedViews =
                catalog.listViewsPaged(databaseName, maxResults, pagedViews.getNextPageToken());
        assertPagedViews(pagedViews, "opr", "view1");
        assertEquals("view1", pagedViews.getNextPageToken());

        pagedViews =
                catalog.listViewsPaged(databaseName, maxResults, pagedViews.getNextPageToken());
        assertPagedViews(pagedViews, "view2", "view3");
        assertEquals("view3", pagedViews.getNextPageToken());

        maxResults = 8;
        String[] expectedViewNames = Arrays.stream(viewNames).sorted().toArray(String[]::new);
        pagedViews = catalog.listViewsPaged(databaseName, maxResults, null);
        assertPagedViews(pagedViews, expectedViewNames);
        assertNull(pagedViews.getNextPageToken());

        String pageToken = "view1";
        pagedViews = catalog.listViewsPaged(databaseName, maxResults, pageToken);
        assertPagedViews(pagedViews, "view2", "view3");
        assertNull(pagedViews.getNextPageToken());

        // List views throws DatabaseNotExistException when the database does not exist
        final int finalMaxResults = 9;
        assertThatExceptionOfType(Catalog.DatabaseNotExistException.class)
                .isThrownBy(
                        () ->
                                catalog.listViewsPaged(
                                        "non_existing_db", finalMaxResults, pageToken));
    }

    @Test
    public void testListViewDetailsPaged() throws Exception {
        // List view details returns an empty list when there are no views in the database
        String databaseName = "view_details_paged_db";
        catalog.createDatabase(databaseName, false);
        PagedList<View> pagedViewDetails = catalog.listViewDetailsPaged(databaseName, null, null);
        assertThat(pagedViewDetails.getElements()).isEmpty();
        assertNull(pagedViewDetails.getNextPageToken());

        String[] viewNames = {"view1", "view2", "view3", "abd", "def", "opr"};
        View view = buildView(databaseName);
        for (String viewName : viewNames) {
            catalog.createView(Identifier.create(databaseName, viewName), view, false);
        }

        pagedViewDetails = catalog.listViewDetailsPaged(databaseName, null, null);
        assertPagedViewDetails(pagedViewDetails, view, viewNames.length, viewNames);
        assertNull(pagedViewDetails.getNextPageToken());

        int maxResults = 2;
        pagedViewDetails = catalog.listViewDetailsPaged(databaseName, maxResults, null);
        assertPagedViewDetails(pagedViewDetails, view, maxResults, "abd", "def");
        assertEquals("def", pagedViewDetails.getNextPageToken());

        pagedViewDetails =
                catalog.listViewDetailsPaged(
                        databaseName, maxResults, pagedViewDetails.getNextPageToken());
        assertPagedViewDetails(pagedViewDetails, view, maxResults, "opr", "view1");
        assertEquals("view1", pagedViewDetails.getNextPageToken());

        pagedViewDetails =
                catalog.listViewDetailsPaged(
                        databaseName, maxResults, pagedViewDetails.getNextPageToken());
        assertPagedViewDetails(pagedViewDetails, view, maxResults, "view2", "view3");
        assertEquals("view3", pagedViewDetails.getNextPageToken());

        pagedViewDetails =
                catalog.listViewDetailsPaged(
                        databaseName, maxResults, pagedViewDetails.getNextPageToken());
        assertEquals(0, pagedViewDetails.getElements().size());
        assertNull(pagedViewDetails.getNextPageToken());

        maxResults = 8;
        pagedViewDetails = catalog.listViewDetailsPaged(databaseName, maxResults, null);
        String[] expectedViewNames = Arrays.stream(viewNames).sorted().toArray(String[]::new);
        assertPagedViewDetails(
                pagedViewDetails,
                view,
                Math.min(maxResults, expectedViewNames.length),
                expectedViewNames);
        assertNull(pagedViewDetails.getNextPageToken());

        String pageToken = "view1";
        pagedViewDetails = catalog.listViewDetailsPaged(databaseName, maxResults, pageToken);
        assertPagedViewDetails(pagedViewDetails, view, 2, "view2", "view3");
        assertNull(pagedViewDetails.getNextPageToken());

        // List view details throws DatabaseNotExistException when the database does not exist
        final int finalMaxResults = maxResults;
        assertThatExceptionOfType(Catalog.DatabaseNotExistException.class)
                .isThrownBy(
                        () ->
                                catalog.listViewDetailsPaged(
                                        "non_existing_db", finalMaxResults, pageToken));
    }

    @Test
    void testListPartitionsWhenMetastorePartitionedIsTrue() throws Exception {
        String branchName = "test_branch";
        Identifier identifier = Identifier.create("test_db", "test_table");
        Identifier branchIdentifier = new Identifier("test_db", "test_table", branchName);
        assertThrows(
                Catalog.TableNotExistException.class, () -> restCatalog.listPartitions(identifier));

        createTable(
                identifier,
                ImmutableMap.of(METASTORE_PARTITIONED_TABLE.key(), "" + true),
                Lists.newArrayList("col1"));
        List<Partition> result = catalog.listPartitions(identifier);
        assertEquals(0, result.size());
        List<Map<String, String>> partitionSpecs =
                Arrays.asList(
                        Collections.singletonMap("dt", "20250101"),
                        Collections.singletonMap("dt", "20250102"));
        restCatalog.createBranch(identifier, branchName, null);
        restCatalog.createPartitions(branchIdentifier, Lists.newArrayList(partitionSpecs));
        assertThat(catalog.listPartitions(identifier).stream().map(Partition::spec))
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
        List<Map<String, String>> partitionSpecs =
                Arrays.asList(
                        Collections.singletonMap("dt", "20250101"),
                        Collections.singletonMap("dt", "20250102"),
                        Collections.singletonMap("dt", "20240102"),
                        Collections.singletonMap("dt", "20260101"),
                        Collections.singletonMap("dt", "20250104"),
                        Collections.singletonMap("dt", "20250103"));
        Map[] sortedSpecs =
                partitionSpecs.stream()
                        .sorted(Comparator.comparing(i -> i.get("dt")))
                        .toArray(Map[]::new);

        String databaseName = "partitions_db";
        Identifier identifier = Identifier.create(databaseName, "table");
        Schema schema =
                Schema.newBuilder()
                        .option(METASTORE_PARTITIONED_TABLE.key(), "true")
                        .option(METASTORE_TAG_TO_PARTITION.key(), "dt")
                        .column("col", DataTypes.INT())
                        .column("dt", DataTypes.STRING())
                        .partitionKeys("dt")
                        .build();

        restCatalog.createDatabase(databaseName, true);
        restCatalog.createTable(identifier, schema, true);
        restCatalog.createPartitions(identifier, partitionSpecs);

        List<Partition> restPartitions = restCatalog.listPartitions(identifier);
        assertThat(restPartitions.stream().map(Partition::spec)).containsExactly(sortedSpecs);
    }

    @Test
    public void testListPartitionsPaged() throws Exception {
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

        assertThrows(
                Catalog.TableNotExistException.class,
                () -> catalog.listPartitionsPaged(identifier, 10, "dt=20250101"));

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

        catalog.createPartitions(identifier, partitionSpecs);
        PagedList<Partition> pagedPartitions = catalog.listPartitionsPaged(identifier, null, null);
        Map[] sortedSpecs =
                partitionSpecs.stream()
                        .sorted(Comparator.comparing(i -> i.get("dt")))
                        .toArray(Map[]::new);
        assertPagedPartitions(pagedPartitions, partitionSpecs.size(), sortedSpecs);

        int maxResults = 2;
        pagedPartitions = catalog.listPartitionsPaged(identifier, maxResults, null);
        assertPagedPartitions(
                pagedPartitions, maxResults, partitionSpecs.get(2), partitionSpecs.get(0));
        assertEquals("dt=20250101", pagedPartitions.getNextPageToken());

        pagedPartitions =
                catalog.listPartitionsPaged(
                        identifier, maxResults, pagedPartitions.getNextPageToken());
        assertPagedPartitions(
                pagedPartitions, maxResults, partitionSpecs.get(1), partitionSpecs.get(5));
        assertEquals("dt=20250103", pagedPartitions.getNextPageToken());

        pagedPartitions =
                catalog.listPartitionsPaged(
                        identifier, maxResults, pagedPartitions.getNextPageToken());
        assertPagedPartitions(
                pagedPartitions, maxResults, partitionSpecs.get(4), partitionSpecs.get(3));
        assertEquals("dt=20260101", pagedPartitions.getNextPageToken());

        pagedPartitions =
                catalog.listPartitionsPaged(
                        identifier, maxResults, pagedPartitions.getNextPageToken());
        assertThat(pagedPartitions.getElements()).isEmpty();
        assertNull(pagedPartitions.getNextPageToken());

        maxResults = 8;
        pagedPartitions = catalog.listPartitionsPaged(identifier, maxResults, null);

        assertPagedPartitions(
                pagedPartitions, Math.min(maxResults, partitionSpecs.size()), sortedSpecs);
        assertNull(pagedPartitions.getNextPageToken());

        pagedPartitions = catalog.listPartitionsPaged(identifier, maxResults, "dt=20250101");
        assertPagedPartitions(
                pagedPartitions,
                4,
                partitionSpecs.get(1),
                partitionSpecs.get(5),
                partitionSpecs.get(4),
                partitionSpecs.get(3));
        assertNull(pagedPartitions.getNextPageToken());
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
                        System.currentTimeMillis());
        setDataTokenToRestServerForMock(identifier, expiredDataToken);
        createTable(identifier, Maps.newHashMap(), Lists.newArrayList("col1"));
        FileStoreTable fileStoreTable = (FileStoreTable) catalog.getTable(identifier);
        RESTTokenFileIO fileIO = (RESTTokenFileIO) fileStoreTable.fileIO();
        RESTToken fileDataToken = fileIO.validToken();
        assertEquals(expiredDataToken, fileDataToken);
        RESTToken newDataToken =
                new RESTToken(
                        ImmutableMap.of("akId", "akId", "akSecret", UUID.randomUUID().toString()),
                        System.currentTimeMillis() + 100_000);
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
                                createSnapshotWithMillis(1L, System.currentTimeMillis()),
                                new ArrayList<Partition>()));

        createTable(hasSnapshotTableIdentifier, Maps.newHashMap(), Lists.newArrayList("col1"));
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
        Identifier noSnapshotTableIdentifier = Identifier.create("test_db_a_1", "unknown");
        createTable(noSnapshotTableIdentifier, Maps.newHashMap(), Lists.newArrayList("col1"));
        snapshot = catalog.loadSnapshot(noSnapshotTableIdentifier);
        assertThat(snapshot).isEmpty();
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
                SupportsBranches.TagNotExistException.class,
                () -> restCatalog.createBranch(identifier, "my_branch", "tag"));
        restCatalog.createBranch(identifier, "my_branch", null);
        Identifier branchIdentifier = new Identifier(databaseName, "table", "my_branch");
        assertThat(restCatalog.getTable(branchIdentifier)).isNotNull();
        assertThrows(
                SupportsBranches.BranchAlreadyExistException.class,
                () -> restCatalog.createBranch(identifier, "my_branch", null));
        assertThat(restCatalog.listBranches(identifier)).containsOnly("my_branch");
        restCatalog.dropBranch(identifier, "my_branch");

        assertThrows(
                SupportsBranches.BranchNotExistException.class,
                () -> restCatalog.dropBranch(identifier, "no_exist_branch"));
        assertThrows(
                SupportsBranches.BranchNotExistException.class,
                () -> restCatalog.fastForward(identifier, "no_exist_branch"));
        assertThat(restCatalog.listBranches(identifier)).isEmpty();
    }

    @Test
    void testListDataFromPageApiWhenLastPageTokenIsNull() {
        List<Integer> testData = ImmutableList.of(1, 2, 3, 4, 5, 6, 7);
        int maxResults = 2;
        AtomicInteger fetchTimes = new AtomicInteger(0);
        List<Integer> fetchData =
                restCatalog.listDataFromPageApi(
                        queryParams -> {
                            return generateTestPagedResponse(
                                    queryParams, testData, maxResults, fetchTimes, true);
                        });
        assertEquals(fetchTimes.get(), 4);
        assertThat(fetchData).containsSequence(testData);
    }

    @Test
    void testListDataFromPageApiWhenLastPageTokenIsNotNullAndDataIsNull() {
        List<Integer> testData = ImmutableList.of(1, 2, 3, 4, 5, 6);
        int maxResults = 2;
        AtomicInteger fetchTimes = new AtomicInteger(0);
        List<Integer> fetchData =
                restCatalog.listDataFromPageApi(
                        queryParams -> {
                            return generateTestPagedResponse(
                                    queryParams, testData, maxResults, fetchTimes, false);
                        });

        assertEquals(fetchTimes.get(), testData.size() / maxResults + 1);
        assertThat(fetchData).containsSequence(testData);
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

    protected abstract Catalog newRestCatalogWithDataToken();

    protected abstract void revokeTablePermission(Identifier identifier);

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

    protected void batchWrite(FileStoreTable tableTestWrite, List<Integer> data) throws Exception {
        BatchWriteBuilder writeBuilder = tableTestWrite.newBatchWriteBuilder();
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

    protected List<String> batchRead(FileStoreTable tableTestWrite) throws IOException {
        ReadBuilder readBuilder = tableTestWrite.newReadBuilder();
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
        String tokenStr = RESTObjectMapper.OBJECT_MAPPER.writeValueAsString(token);
        FileUtils.writeStringToFile(tokenFile, tokenStr);
    }
}
