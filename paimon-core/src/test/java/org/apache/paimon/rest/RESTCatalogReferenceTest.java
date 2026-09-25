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

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.CachingCatalog;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.SnapshotCommit;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.rest.exceptions.AlreadyExistsException;
import org.apache.paimon.rest.requests.CommitTableRequest;
import org.apache.paimon.rest.requests.CreateTableRequest;
import org.apache.paimon.rest.responses.GetSchemaResponse;
import org.apache.paimon.rest.responses.GetTableResponse;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.SnapshotManager;

import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.paimon.CoreOptions.BRANCH;
import static org.apache.paimon.rest.RESTCatalogOptions.TOKEN;
import static org.apache.paimon.rest.RESTCatalogOptions.TOKEN_PROVIDER;
import static org.apache.paimon.rest.RESTCatalogOptions.URI;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies reference scope through ordinary table APIs, serialization and storage commits. */
class RESTCatalogReferenceTest {

    private static final String DATABASE = "training db";
    private static final Identifier TABLE = Identifier.create(DATABASE, "features");
    private static final String DATABASE_PATH = "/v1/catalog%2Fid/databases/training+db";
    private static final String SNAPSHOT_JSON =
            "{\"version\":3,\"id\":7,\"schemaId\":2,\"uuid\":\"snapshot-7\","
                    + "\"commitKind\":\"APPEND\",\"commitUser\":\"writer\",\"commitIdentifier\":1,"
                    + "\"timeMillis\":1000,\"totalRecordCount\":3,\"deltaRecordCount\":3}";

    @TempDir Path tempDir;

    private MockWebServer server;
    private RESTCatalog catalog;

    @BeforeEach
    void setUp() throws Exception {
        server = new MockWebServer();
        server.start();
        enqueue(
                200,
                "{\"defaults\":{},\"overrides\":{\"prefix\":\"catalog/id\","
                        + "\"header.X-Catalog-Context\":\"configured\"}}");
        Options options = new Options();
        options.set(URI, server.url("/").toString());
        options.set(TOKEN_PROVIDER, "bear");
        options.set(TOKEN, "test-token");
        catalog = new RESTCatalog(CatalogContext.create(options));
        assertThat(server.takeRequest(10, TimeUnit.SECONDS).getPath()).isEqualTo("/v1/config");
    }

    @AfterEach
    void tearDown() throws Exception {
        catalog.close();
        server.shutdown();
    }

    @ParameterizedTest
    @ValueSource(strings = {"$branch_experiment", "$tag_train_v1"})
    void testTableAndSerializedLoaderKeepReference(String reference) throws Exception {
        String database = DATABASE + reference;
        Identifier selected = Identifier.create(database, "features");
        String scope = DATABASE_PATH + reference.replace("$", "%24");
        enqueue(200, "{\"tables\":[\"features\",\"labels\"]}");
        assertThat(catalog.listTables(database)).containsExactly("features", "labels");
        takeRequest("GET", scope + "/tables");

        enqueue(200, tableResponse(database, "physical-experiment", 2));
        FileStoreTable table = (FileStoreTable) catalog.getTable(selected);
        assertThat(table.catalogEnvironment().identifier()).isEqualTo(selected);
        assertThat(table.snapshotManager().branch()).isEqualTo("physical-experiment");
        assertThat(table.schema().id()).isEqualTo(2);
        takeRequest("GET", scope + "/tables/features");

        // A task receives a serialized table. Its identifier must retain the database suffix.
        FileStoreTable restored = InstantiationUtil.clone(table);
        enqueue(200, "{\"snapshot\":{\"snapshot\":" + SNAPSHOT_JSON + "}}");
        assertThat(restored.snapshotManager().latestSnapshot().id()).isEqualTo(7);
        takeRequest("GET", scope + "/tables/features/snapshot");

        RESTCatalog loaded = InstantiationUtil.clone(catalog.catalogLoader()).load();
        enqueue(
                200,
                RESTApi.toJson(
                        new GetSchemaResponse(
                                TableSchema.create(2, schema("physical-experiment")))));
        assertThat(loaded.loadSchema(selected, "LATEST").get().id()).isEqualTo(2);
        takeRequest("GET", scope + "/tables/features/schemas/LATEST");

        // The same catalog also loads the ordinary database without reference state.
        enqueue(200, tableResponse("main"));
        FileStoreTable main = (FileStoreTable) catalog.getTable(TABLE);
        assertThat(main.snapshotManager().branch()).isEqualTo("main");
        takeRequest("GET", DATABASE_PATH + "/tables/features");
        assertThat(server.getRequestCount()).isEqualTo(6);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "$branch_main", "$branch_experiment"})
    void testStorageCommitUsesLogicalTableAndExistingBody(String reference) throws Exception {
        Identifier selected = Identifier.create(DATABASE + reference, "features");
        String scope = DATABASE_PATH + reference.replace("$", "%24");
        enqueue(200, tableResponse(selected.getDatabaseName(), "physical-experiment", 2));
        FileStoreTable table = InstantiationUtil.clone((FileStoreTable) catalog.getTable(selected));
        takeRequest("GET", scope + "/tables/features");

        Snapshot snapshot = Snapshot.fromJson(SNAPSHOT_JSON);
        enqueue(200, "{\"success\":true}");
        try (SnapshotCommit commit =
                table.catalogEnvironment().snapshotCommit(table.snapshotManager())) {
            assertThat(
                            commit.commit(
                                    "snapshot-6",
                                    snapshot,
                                    table.snapshotManager().branch(),
                                    emptyList()))
                    .isTrue();
        }
        RecordedRequest request = takeRequest("POST", scope + "/tables/features/commit");
        CommitTableRequest body =
                RESTApi.fromJson(request.getBody().readUtf8(), CommitTableRequest.class);
        assertThat(body.getTableId()).isEqualTo("table-id");
        assertThat(body.getBaseSnapshotUuid()).isEqualTo("snapshot-6");
        assertThat(body.getSnapshot()).isEqualTo(snapshot);
        assertThat(body.getStatistics()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testStorageCommitPreservesExplicitTableBranch(boolean dynamicBranch) throws Exception {
        Identifier selected =
                Identifier.create(DATABASE, dynamicBranch ? "features" : "features$branch_dev");
        enqueue(200, tableResponse(DATABASE, dynamicBranch ? "physical-main" : "dev", 2));
        FileStoreTable table = InstantiationUtil.clone((FileStoreTable) catalog.getTable(selected));
        takeRequest(
                "GET",
                DATABASE_PATH + "/tables/" + RESTUtil.encodeString(selected.getObjectName()));
        if (dynamicBranch) {
            table =
                    InstantiationUtil.clone(
                            table.copy(java.util.Collections.singletonMap(BRANCH.key(), "dev")));
        }

        enqueue(200, "{\"success\":true}");
        try (SnapshotCommit commit =
                table.catalogEnvironment().snapshotCommit(table.snapshotManager())) {
            assertThat(
                            commit.commit(
                                    "snapshot-6",
                                    Snapshot.fromJson(SNAPSHOT_JSON),
                                    table.snapshotManager().branch(),
                                    emptyList()))
                    .isTrue();
        }
        takeRequest("POST", DATABASE_PATH + "/tables/features%24branch_dev/commit");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testMainAliasesInvalidateTogether(boolean explicitMain) throws Exception {
        CachingCatalog cached = new CachingCatalog(catalog, new Options());
        Identifier selected =
                Identifier.create(DATABASE + (explicitMain ? "$branch_main" : ""), "features");
        Identifier other =
                Identifier.create(DATABASE + (explicitMain ? "" : "$branch_main"), "features");
        String selectedPath =
                DATABASE_PATH + (explicitMain ? "%24branch_main" : "") + "/tables/features";
        for (Identifier identifier : new Identifier[] {selected, other}) {
            enqueue(200, tableResponse(identifier.getDatabaseName(), "physical-main", 2));
            cached.getTable(identifier);
            takeRequest(
                    "GET",
                    new ResourcePaths("catalog/id")
                            .table(identifier.getDatabaseName(), identifier.getObjectName()));
        }

        Identifier dev = Identifier.create(DATABASE + "$branch_dev", "features");
        Identifier tag = Identifier.create(DATABASE + "$tag_baseline", "features");
        enqueue(200, tableResponse(dev.getDatabaseName(), "physical-dev", 2));
        FileStoreTable devTable = (FileStoreTable) cached.getTable(dev);
        takeRequest("GET", DATABASE_PATH + "%24branch_dev/tables/features");
        enqueue(200, tableResponse(tag.getDatabaseName(), "frozen-baseline", 2));
        FileStoreTable tagTable = (FileStoreTable) cached.getTable(tag);
        takeRequest("GET", DATABASE_PATH + "%24tag_baseline/tables/features");

        enqueue(200, "");
        cached.alterTable(
                selected,
                java.util.Collections.singletonList(SchemaChange.setOption("comment", "updated")),
                false);
        takeRequest("POST", selectedPath);
        assertMainAliasesReload(cached, selected, other, 3);

        // Forward is followed by explicit invalidation, which must refresh both main aliases.
        cached.invalidateTable(selected);
        assertMainAliasesReload(cached, selected, other, 4);

        enqueue(200, "");
        cached.dropTable(selected, false);
        takeRequest("DELETE", selectedPath);
        for (Identifier identifier : new Identifier[] {selected, other}) {
            enqueue(404, "{\"code\":404,\"resourceType\":\"TABLE\",\"message\":\"missing\"}");
            assertThatThrownBy(() -> cached.getTable(identifier))
                    .isInstanceOf(Catalog.TableNotExistException.class);
            takeRequest(
                    "GET",
                    new ResourcePaths("catalog/id")
                            .table(identifier.getDatabaseName(), identifier.getObjectName()));
        }
        assertThat(cached.getTable(dev)).isSameAs(devTable);
        assertThat(cached.getTable(tag)).isSameAs(tagTable);
    }

    @Test
    void testStorageCommitAfterSwitchingBranchAndCopyingBack() throws Exception {
        enqueue(200, tableResponse("main"));
        FileStoreTable main = (FileStoreTable) catalog.getTable(TABLE);
        takeRequest("GET", DATABASE_PATH + "/tables/features");
        main.schemaManager().copyWithBranch("dev").createTable(schema("dev"));
        FileStoreTable copied =
                InstantiationUtil.clone(
                        main.switchToBranch("dev")
                                .copy(java.util.Collections.singletonMap(BRANCH.key(), "main")));

        enqueue(200, "{\"success\":true}");
        try (SnapshotCommit commit =
                copied.catalogEnvironment().snapshotCommit(copied.snapshotManager())) {
            assertThat(
                            commit.commit(
                                    "snapshot-6",
                                    Snapshot.fromJson(SNAPSHOT_JSON),
                                    copied.snapshotManager().branch(),
                                    emptyList()))
                    .isTrue();
        }
        takeRequest("POST", DATABASE_PATH + "/tables/features/commit");
    }

    private void assertMainAliasesReload(
            CachingCatalog cached, Identifier selected, Identifier other, long schemaId)
            throws Exception {
        for (Identifier identifier : new Identifier[] {selected, other}) {
            enqueue(200, tableResponse(identifier.getDatabaseName(), "physical-main", schemaId));
            assertThat(((FileStoreTable) cached.getTable(identifier)).schema().id())
                    .isEqualTo(schemaId);
            takeRequest(
                    "GET",
                    new ResourcePaths("catalog/id")
                            .table(identifier.getDatabaseName(), identifier.getObjectName()));
        }
    }

    @Test
    void testReadFollowUpsAndPaginationReuseProtocol() throws Exception {
        RESTApi api = catalog.api();
        String database = DATABASE + "$tag_train_v1";
        Identifier selected = Identifier.create(database, "features");
        String scope = DATABASE_PATH + "%24tag_train_v1";
        String tablePath = scope + "/tables/features";
        enqueue(200, "{\"tables\":[\"features\"],\"nextPageToken\":\"next\"}");
        assertThat(api.listTablesPaged(database, 1, null, "feat%", null).getNextPageToken())
                .isEqualTo("next");
        RecordedRequest first = takeRequest("GET", scope + "/tables");
        assertThat(first.getRequestUrl().queryParameter("tableNamePattern")).isEqualTo("feat%");
        enqueue(200, "{\"tables\":[\"labels\"]}");
        assertThat(api.listTablesPaged(database, 1, "next", null, null).getElements())
                .containsExactly("labels");
        assertThat(
                        takeRequest("GET", scope + "/tables")
                                .getRequestUrl()
                                .queryParameter("pageToken"))
                .isEqualTo("next");

        enqueue(
                200,
                "{\"tableDetails\":[" + tableResponse(database, "physical-experiment", 2) + "]}");
        GetTableResponse details = api.listTableDetails(database).get(0);
        assertThat(details.getName()).isEqualTo("features");
        assertThat(details.getDatabase()).isEqualTo(database);
        takeRequest("GET", scope + "/table-details");

        enqueue(200, "{\"snapshot\":" + SNAPSHOT_JSON + "}");
        assertThat(api.loadSnapshot(selected, "LATEST").id()).isEqualTo(7);
        takeRequest("GET", tablePath + "/snapshots/LATEST");
        enqueue(200, "{\"snapshots\":[" + SNAPSHOT_JSON + "]}");
        assertThat(api.listSnapshotsPaged(selected, 10, null).getElements().get(0).id())
                .isEqualTo(7);
        takeRequest("GET", tablePath + "/snapshots");

        TableSchema schema = TableSchema.create(2, schema("physical-experiment"));
        enqueue(200, "{\"schemas\":[" + RESTApi.toJson(schema) + "]}");
        assertThat(api.listSchemasPaged(selected, 10, null).getElements()).containsExactly(schema);
        takeRequest("GET", tablePath + "/schemas");

        enqueue(200, "{\"token\":{\"key\":\"value\"},\"expiresAtMillis\":1234}");
        assertThat(api.loadTableToken(selected).getToken()).containsEntry("key", "value");
        takeRequest("GET", tablePath + "/token");
        enqueue(200, "{\"filter\":[],\"columnMasking\":{}}");
        api.authTableQuery(selected, singletonList("id"));
        assertThat(takeRequest("POST", tablePath + "/auth").getBody().readUtf8())
                .isEqualTo("{\"select\":[\"id\"]}");
    }

    @Test
    void testTableMutationsReuseRequestBodies() throws Exception {
        Identifier selected = Identifier.create(DATABASE + "$branch_experiment", "features");
        RESTApi api = catalog.api();
        for (Identifier identifier : new Identifier[] {TABLE, selected}) {
            enqueue(200, "{}");
            api.createTable(identifier, schema("main"));
            enqueue(200, "{}");
            api.alterTable(identifier, singletonList(SchemaChange.setOption("key", "value")));
            enqueue(200, "{}");
            api.dropTable(identifier);
        }
        RecordedRequest[] original = {
            takeRequest("POST", DATABASE_PATH + "/tables"),
            takeRequest("POST", DATABASE_PATH + "/tables/features"),
            takeRequest("DELETE", DATABASE_PATH + "/tables/features")
        };
        String scope = DATABASE_PATH + "%24branch_experiment";
        RecordedRequest[] referenced = {
            takeRequest("POST", scope + "/tables"),
            takeRequest("POST", scope + "/tables/features"),
            takeRequest("DELETE", scope + "/tables/features")
        };
        CreateTableRequest plain =
                RESTApi.fromJson(original[0].getBody().readUtf8(), CreateTableRequest.class);
        CreateTableRequest branch =
                RESTApi.fromJson(referenced[0].getBody().readUtf8(), CreateTableRequest.class);
        assertThat(plain.getIdentifier()).isEqualTo(TABLE);
        assertThat(branch.getIdentifier()).isEqualTo(selected);
        assertThat(branch.getSchema()).isEqualTo(plain.getSchema());
        for (int i = 1; i < original.length; i++) {
            assertThat(referenced[i].getBody().readUtf8())
                    .isEqualTo(original[i].getBody().readUtf8());
        }
    }

    @Test
    void testErrorsDoNotFallBackToDefaultBranch() throws Exception {
        Identifier selected = Identifier.create(DATABASE + "$tag_train_v1", "features");
        enqueue(404, "{\"message\":\"reference missing\",\"code\":404}");
        assertThatThrownBy(() -> catalog.getTable(selected))
                .isInstanceOf(Catalog.TableNotExistException.class);
        takeRequest("GET", DATABASE_PATH + "%24tag_train_v1/tables/features");
        enqueue(403, "{\"message\":\"tag is immutable\",\"code\":403}");
        assertThatThrownBy(
                        () ->
                                catalog.commitSnapshot(
                                        selected,
                                        "table-id",
                                        null,
                                        Snapshot.fromJson(SNAPSHOT_JSON),
                                        emptyList()))
                .isInstanceOf(Catalog.TableNoPermissionException.class)
                .hasMessageContaining("tag is immutable");
        takeRequest("POST", DATABASE_PATH + "%24tag_train_v1/tables/features/commit");
        assertThat(server.getRequestCount()).isEqualTo(3);
    }

    @Test
    void testReferenceErrorsAreNotIgnoredByTableDdl() throws Exception {
        Identifier selected = Identifier.create(DATABASE + "$branch_experiment", "features");
        enqueue(
                409,
                "{\"code\":409,\"resourceType\":\"BRANCH\",\"message\":\"branch is not writable\"}");
        assertThatThrownBy(() -> catalog.createTable(selected, schema("main"), true))
                .isInstanceOf(AlreadyExistsException.class)
                .hasMessageContaining("not writable");
        takeRequest("POST", DATABASE_PATH + "%24branch_experiment/tables");

        enqueue(409, "{\"code\":409,\"resourceType\":\"TABLE\",\"message\":\"table exists\"}");
        catalog.createTable(selected, schema("main"), true);
        takeRequest("POST", DATABASE_PATH + "%24branch_experiment/tables");

        for (boolean ignore : new boolean[] {false, true}) {
            enqueue(
                    404,
                    "{\"code\":404,\"resourceType\":\"BRANCH\",\"message\":\"branch missing\"}");
            assertThatThrownBy(
                            () ->
                                    catalog.alterTable(
                                            selected,
                                            singletonList(SchemaChange.setOption("key", "value")),
                                            ignore))
                    .isInstanceOf(org.apache.paimon.rest.exceptions.NoSuchResourceException.class)
                    .hasMessageContaining("branch missing");
            takeRequest("POST", DATABASE_PATH + "%24branch_experiment/tables/features");
        }
    }

    @Test
    void testViewProbesAllowTableOnlyReferenceNamespaces() throws Exception {
        String database = DATABASE + "$branch_experiment";
        String databasePath = DATABASE_PATH + "%24branch_experiment";
        for (int i = 0; i < 4; i++) {
            enqueue(200, "{\"name\":\"training db$branch_experiment\",\"options\":{}}");
        }
        assertThat(catalog.listViews(database)).isEmpty();
        assertThat(catalog.listViewsPaged(database, 10, null, null).getElements()).isEmpty();
        assertThat(catalog.listViewDetailsPaged(database, 10, null, null).getElements()).isEmpty();
        assertThatThrownBy(() -> catalog.getView(Identifier.create(database, "features")))
                .isInstanceOf(Catalog.ViewNotExistException.class);
        for (int i = 0; i < 4; i++) {
            takeRequest("GET", databasePath);
        }
        enqueue(404, "{\"code\":404,\"resourceType\":\"BRANCH\",\"message\":\"branch missing\"}");
        assertThatThrownBy(() -> catalog.listViews(database))
                .isInstanceOf(Catalog.DatabaseNotExistException.class);
        takeRequest("GET", databasePath);
    }

    @Test
    void testUnsupportedDatabaseOperationsAndMixedSelectorsDoNotSendRequests() {
        RESTApi api = catalog.api();
        String database = DATABASE + "$branch_experiment";
        Identifier selected = Identifier.create(database, "features");
        Identifier mixed = new Identifier(database, "features", "other");
        assertThatThrownBy(() -> api.getTable(mixed)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> api.createTable(mixed, schema("main")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> api.listTables(DATABASE + "$tag_"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> api.renameTable(selected, TABLE))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> api.renameTable(TABLE, selected))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> api.createBranch(selected, "nested", null))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> api.createDatabase(database, java.util.Collections.emptyMap()))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(
                        () ->
                                api.alterDatabase(
                                        database, emptyList(), java.util.Collections.emptyMap()))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> api.dropDatabase(database))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> catalog.dropDatabase(database, true, false))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> catalog.dropDatabase(database, true, true))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> catalog.treeManagement().listBranches(database))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(server.getRequestCount()).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(strings = {"$branch_experiment", "$tag_train_v1"})
    void testDatabaseLookupPreservesVirtualName(String suffix) throws Exception {
        String database = DATABASE + suffix;
        enqueue(
                200,
                "{\"name\":\"" + database + "\",\"location\":\"file:///training\",\"options\":{}}");
        assertThat(catalog.getDatabase(database).name()).isEqualTo(database);
        takeRequest("GET", DATABASE_PATH + suffix.replace("$", "%24"));
        enqueue(404, "{\"code\":404,\"message\":\"reference missing\"}");
        assertThatThrownBy(() -> catalog.getDatabase(database))
                .isInstanceOf(Catalog.DatabaseNotExistException.class);
        takeRequest("GET", DATABASE_PATH + suffix.replace("$", "%24"));
        assertThat(server.getRequestCount()).isEqualTo(3);
    }

    @Test
    void testBatchReadWriteAndPinnedTagWithRealDataFiles() throws Exception {
        // A small stateful fixture resolves references; production reference lifecycle is separate.
        org.apache.paimon.fs.Path location =
                new org.apache.paimon.fs.Path(tempDir.resolve("features").toUri());
        LocalFileIO fileIO = LocalFileIO.create();
        for (String branch : new String[] {"main", "physical-experiment", "frozen-train-v1"}) {
            new FileSystemSchemaManager(fileIO, location, branch).createTable(schema(branch));
        }
        Map<String, Snapshot> snapshots = new ConcurrentHashMap<>();
        ConcurrentLinkedQueue<String> unexpected = new ConcurrentLinkedQueue<>();
        server.setDispatcher(
                new Dispatcher() {
                    @Override
                    public MockResponse dispatch(RecordedRequest request) {
                        try {
                            String route = request.getRequestUrl().encodedPath();
                            String prefix = "/v1/catalog%2Fid/databases/";
                            if (!route.startsWith(prefix)) {
                                unexpected.add(route);
                                return response(500, "{}");
                            }
                            String[] parts = route.substring(prefix.length()).split("/");
                            DatabaseIdentifier database =
                                    DatabaseIdentifier.parse(RESTUtil.decodeString(parts[0]));
                            if (!database.getDatabaseName().equals(DATABASE)) {
                                unexpected.add(route);
                                return response(500, "{}");
                            }
                            String reference =
                                    database.getReference() == null
                                            ? "main"
                                            : database.getReference().getName();
                            if (parts.length < 3
                                    || !parts[1].equals("tables")
                                    || !parts[2].equals("features")) {
                                unexpected.add(route);
                                return response(500, "{}");
                            }
                            String branch =
                                    reference.equals("main")
                                            ? "main"
                                            : reference.equals("train_v1")
                                                    ? "frozen-train-v1"
                                                    : "physical-experiment";
                            if (request.getMethod().equals("GET") && parts.length == 3) {
                                return response(
                                        200,
                                        tableResponse(RESTUtil.decodeString(parts[0]), branch, 0));
                            }
                            if (request.getMethod().equals("GET")
                                    && parts.length == 4
                                    && parts[3].equals("snapshot")) {
                                Snapshot snapshot = snapshots.get(reference);
                                return snapshot == null
                                        ? response(
                                                404,
                                                "{\"code\":404,\"resourceType\":\"SNAPSHOT\",\"message\":\"empty table\"}")
                                        : response(
                                                200,
                                                "{\"snapshot\":{\"snapshot\":"
                                                        + snapshot.toJson()
                                                        + "}}");
                            }
                            if (request.getMethod().equals("POST")
                                    && parts.length == 4
                                    && parts[3].equals("commit")) {
                                if (reference.equals("train_v1")) {
                                    return response(
                                            403, "{\"code\":403,\"message\":\"tag is immutable\"}");
                                }
                                CommitTableRequest commit =
                                        RESTApi.fromJson(
                                                request.getBody().readUtf8(),
                                                CommitTableRequest.class);
                                Snapshot snapshot = commit.getSnapshot();
                                fileIO.overwriteFileUtf8(
                                        new SnapshotManager(fileIO, location, branch, null, null)
                                                .snapshotPath(snapshot.id()),
                                        snapshot.toJson());
                                snapshots.put(reference, snapshot);
                                return response(200, "{\"success\":true}");
                            }
                            unexpected.add(route);
                            return response(500, "{}");
                        } catch (Exception e) {
                            unexpected.add(e.toString());
                            return response(500, "{}");
                        }
                    }
                });

        Identifier main = Identifier.create(DATABASE + "$branch_main", "features");
        Identifier experiment = Identifier.create(DATABASE + "$branch_experiment", "features");
        writeRows(main, 10);
        writeRows(experiment, 20);
        snapshots.put("train_v1", snapshots.get("experiment"));
        // REST latest alone cannot constrain native metadata reads. Return frozen backing metadata.
        fileIO.overwriteFileUtf8(
                new SnapshotManager(fileIO, location, "frozen-train-v1", null, null)
                        .snapshotPath(snapshots.get("train_v1").id()),
                snapshots.get("train_v1").toJson());
        Identifier tag = Identifier.create(DATABASE + "$tag_train_v1", "features");
        assertThat(readRows(tag)).containsExactly(20);

        writeRows(experiment, 30);
        assertThat(readRows(main)).containsExactly(10);
        assertThat(readRows(experiment)).containsExactlyInAnyOrder(20, 30);
        // A newly loaded tag table must not follow the source branch's latest snapshot.
        assertThat(readRows(tag)).containsExactly(20);
        assertThatThrownBy(() -> writeRows(tag, 99)).hasStackTraceContaining("tag is immutable");
        assertThat(readRows(tag)).containsExactly(20);
        assertThat(snapshots.get("train_v1").id()).isEqualTo(1);
        assertThat(snapshots.get("experiment").id()).isEqualTo(2);
        new FileSystemSchemaManager(fileIO, location, "physical-experiment")
                .commitChanges(SchemaChange.addColumn("later", DataTypes.INT()));
        FileStoreTable frozen = (FileStoreTable) catalog.getTable(tag);
        assertThat(frozen.copyWithLatestSchema().schema().id()).isZero();
        assertThat(frozen.schemaManager().listAll()).hasSize(1);
        assertThatThrownBy(
                        () ->
                                frozen.copy(
                                                java.util.Collections.singletonMap(
                                                        "scan.snapshot-id", "2"))
                                        .newReadBuilder()
                                        .newScan()
                                        .plan())
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(unexpected).isEmpty();
    }

    private void writeRows(Identifier selected, int value) throws Exception {
        FileStoreTable table = InstantiationUtil.clone((FileStoreTable) catalog.getTable(selected));
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            write.write(GenericRow.of(value));
            commit.commit(write.prepareCommit());
        }
    }

    private List<Integer> readRows(Identifier selected) throws Exception {
        FileStoreTable table = InstantiationUtil.clone((FileStoreTable) catalog.getTable(selected));
        ReadBuilder builder = table.newReadBuilder();
        List<Integer> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                builder.newRead().createReader(builder.newScan().plan().splits())) {
            reader.forEachRemaining(row -> rows.add(row.getInt(0)));
        }
        return rows;
    }

    private Schema schema(String branch) {
        return Schema.newBuilder()
                .column("id", DataTypes.INT())
                .option("bucket", "-1")
                .option("commit.max-retries", "0")
                .option(BRANCH.key(), branch)
                .build();
    }

    private String tableResponse(String branch) throws Exception {
        return tableResponse(DATABASE, branch, 2);
    }

    private String tableResponse(String database, String branch, long schemaId) throws Exception {
        return RESTApi.toJson(
                new GetTableResponse(
                        "table-id",
                        database,
                        "features",
                        tempDir.resolve("features").toUri().toString(),
                        false,
                        schemaId,
                        schema(branch),
                        null,
                        0,
                        null,
                        0,
                        null));
    }

    private void enqueue(int status, String body) {
        server.enqueue(response(status, body));
    }

    private MockResponse response(int status, String body) {
        return new MockResponse()
                .setResponseCode(status)
                .setHeader("Content-Type", "application/json")
                .setBody(body);
    }

    private RecordedRequest takeRequest(String method, String path) throws Exception {
        RecordedRequest request = server.takeRequest(10, TimeUnit.SECONDS);
        assertThat(request).isNotNull();
        assertThat(request.getMethod()).isEqualTo(method);
        assertThat(request.getRequestUrl().encodedPath()).isEqualTo(path);
        assertThat(request.getHeader("Authorization")).isEqualTo("Bearer test-token");
        assertThat(request.getHeader("X-Catalog-Context")).isEqualTo("configured");
        assertThat(request.getHeader("Paimon-Reference")).isNull();
        return request;
    }
}
