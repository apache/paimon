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
    @ValueSource(strings = {"experiment", "train_v1"})
    void testTableAndSerializedLoaderKeepReference(String reference) throws Exception {
        RESTCatalog scoped = catalog.withReference(DATABASE, reference);
        String scope = DATABASE_PATH + "/trees/" + reference;
        enqueue(200, "{\"tables\":[\"features\",\"labels\"]}");
        assertThat(scoped.listTables(DATABASE)).containsExactly("features", "labels");
        takeRequest("GET", scope + "/tables");

        enqueue(200, tableResponse("physical-experiment"));
        FileStoreTable table = (FileStoreTable) scoped.getTable(TABLE);
        assertThat(table.catalogEnvironment().identifier()).isEqualTo(TABLE);
        assertThat(table.snapshotManager().branch()).isEqualTo("physical-experiment");
        assertThat(table.schema().id()).isEqualTo(2);
        takeRequest("GET", scope + "/tables/features");

        // A task receives a serialized table. Its snapshot loader must still address the tree.
        FileStoreTable restored = InstantiationUtil.clone(table);
        enqueue(200, "{\"snapshot\":{\"snapshot\":" + SNAPSHOT_JSON + "}}");
        assertThat(restored.snapshotManager().latestSnapshot().id()).isEqualTo(7);
        takeRequest("GET", scope + "/tables/features/snapshot");

        RESTCatalog loaded = InstantiationUtil.clone(scoped.catalogLoader()).load();
        enqueue(
                200,
                RESTApi.toJson(
                        new GetSchemaResponse(
                                TableSchema.create(2, schema("physical-experiment")))));
        assertThat(loaded.loadSchema(TABLE, "LATEST").get().id()).isEqualTo(2);
        takeRequest("GET", scope + "/tables/features/schemas/LATEST");

        // Binding another catalog must not change the original catalog's route or metadata.
        enqueue(200, tableResponse("main"));
        FileStoreTable main = (FileStoreTable) catalog.getTable(TABLE);
        assertThat(main.snapshotManager().branch()).isEqualTo("main");
        takeRequest("GET", DATABASE_PATH + "/tables/features");
        assertThat(server.getRequestCount()).isEqualTo(6);
    }

    @Test
    void testStorageCommitUsesLogicalTableAndExistingBody() throws Exception {
        RESTCatalog scoped = catalog.withReference(DATABASE, "experiment");
        enqueue(200, tableResponse("physical-experiment"));
        FileStoreTable table = InstantiationUtil.clone((FileStoreTable) scoped.getTable(TABLE));
        takeRequest("GET", DATABASE_PATH + "/trees/experiment/tables/features");

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
        RecordedRequest request =
                takeRequest("POST", DATABASE_PATH + "/trees/experiment/tables/features/commit");
        CommitTableRequest body =
                RESTApi.fromJson(request.getBody().readUtf8(), CommitTableRequest.class);
        assertThat(body.getTableId()).isEqualTo("table-id");
        assertThat(body.getBaseSnapshotUuid()).isEqualTo("snapshot-6");
        assertThat(body.getSnapshot()).isEqualTo(snapshot);
        assertThat(body.getStatistics()).isEmpty();
    }

    @Test
    void testReadFollowUpsAndPaginationReuseProtocol() throws Exception {
        RESTApi scoped = catalog.api().withReference(DATABASE, "train_v1");
        String scope = DATABASE_PATH + "/trees/train_v1";
        String tablePath = scope + "/tables/features";
        enqueue(200, "{\"tables\":[\"features\"],\"nextPageToken\":\"next\"}");
        assertThat(scoped.listTablesPaged(DATABASE, 1, null, "feat%", null).getNextPageToken())
                .isEqualTo("next");
        RecordedRequest first = takeRequest("GET", scope + "/tables");
        assertThat(first.getRequestUrl().queryParameter("tableNamePattern")).isEqualTo("feat%");
        enqueue(200, "{\"tables\":[\"labels\"]}");
        assertThat(scoped.listTablesPaged(DATABASE, 1, "next", null, null).getElements())
                .containsExactly("labels");
        assertThat(
                        takeRequest("GET", scope + "/tables")
                                .getRequestUrl()
                                .queryParameter("pageToken"))
                .isEqualTo("next");

        enqueue(200, "{\"tableDetails\":[" + tableResponse("physical-experiment") + "]}");
        assertThat(scoped.listTableDetails(DATABASE).get(0).getName()).isEqualTo("features");
        takeRequest("GET", scope + "/table-details");

        enqueue(200, "{\"snapshot\":" + SNAPSHOT_JSON + "}");
        assertThat(scoped.loadSnapshot(TABLE, "LATEST").id()).isEqualTo(7);
        takeRequest("GET", tablePath + "/snapshots/LATEST");
        enqueue(200, "{\"snapshots\":[" + SNAPSHOT_JSON + "]}");
        assertThat(scoped.listSnapshotsPaged(TABLE, 10, null).getElements().get(0).id())
                .isEqualTo(7);
        takeRequest("GET", tablePath + "/snapshots");

        TableSchema schema = TableSchema.create(2, schema("physical-experiment"));
        enqueue(200, "{\"schemas\":[" + RESTApi.toJson(schema) + "]}");
        assertThat(scoped.listSchemasPaged(TABLE, 10, null).getElements()).containsExactly(schema);
        takeRequest("GET", tablePath + "/schemas");

        enqueue(200, "{\"token\":{\"key\":\"value\"},\"expiresAtMillis\":1234}");
        assertThat(scoped.loadTableToken(TABLE).getToken()).containsEntry("key", "value");
        takeRequest("GET", tablePath + "/token");
        enqueue(200, "{\"filter\":[],\"columnMasking\":{}}");
        scoped.authTableQuery(TABLE, singletonList("id"));
        assertThat(takeRequest("POST", tablePath + "/auth").getBody().readUtf8())
                .isEqualTo("{\"select\":[\"id\"]}");
    }

    @Test
    void testTableMutationsReuseRequestBodies() throws Exception {
        RESTApi scoped = catalog.api().withReference(DATABASE, "experiment");
        for (RESTApi api : new RESTApi[] {catalog.api(), scoped}) {
            enqueue(200, "{}");
            api.createTable(TABLE, schema("main"));
            enqueue(200, "{}");
            api.alterTable(TABLE, singletonList(SchemaChange.setOption("key", "value")));
            enqueue(200, "{}");
            api.dropTable(TABLE);
        }
        RecordedRequest[] original = {
            takeRequest("POST", DATABASE_PATH + "/tables"),
            takeRequest("POST", DATABASE_PATH + "/tables/features"),
            takeRequest("DELETE", DATABASE_PATH + "/tables/features")
        };
        String scope = DATABASE_PATH + "/trees/experiment";
        RecordedRequest[] referenced = {
            takeRequest("POST", scope + "/tables"),
            takeRequest("POST", scope + "/tables/features"),
            takeRequest("DELETE", scope + "/tables/features")
        };
        for (int i = 0; i < original.length; i++) {
            assertThat(referenced[i].getBody().readUtf8())
                    .isEqualTo(original[i].getBody().readUtf8());
        }
    }

    @Test
    void testErrorsDoNotFallBackToDefaultBranch() throws Exception {
        RESTCatalog scoped = catalog.withReference(DATABASE, "train_v1");
        enqueue(404, "{\"message\":\"reference missing\",\"code\":404}");
        assertThatThrownBy(() -> scoped.getTable(TABLE))
                .isInstanceOf(Catalog.TableNotExistException.class);
        takeRequest("GET", DATABASE_PATH + "/trees/train_v1/tables/features");
        enqueue(409, "{\"message\":\"tag is immutable\",\"code\":409}");
        assertThatThrownBy(
                        () ->
                                scoped.commitSnapshot(
                                        TABLE,
                                        "table-id",
                                        null,
                                        Snapshot.fromJson(SNAPSHOT_JSON),
                                        emptyList()))
                .isInstanceOf(AlreadyExistsException.class)
                .hasMessageContaining("tag is immutable");
        takeRequest("POST", DATABASE_PATH + "/trees/train_v1/tables/features/commit");
        assertThat(server.getRequestCount()).isEqualTo(3);
    }

    @Test
    void testUnsupportedSelectorsNeverSendAnUnscopedRequest() {
        RESTApi scoped = catalog.api().withReference(DATABASE, "experiment");
        assertThatThrownBy(() -> catalog.withReference(DATABASE, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> scoped.withReference(DATABASE, "../main"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> scoped.listTables("other"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(DATABASE);
        assertThatThrownBy(() -> scoped.getTable(new Identifier(DATABASE, "features", "other")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("branch suffixes");
        assertThatThrownBy(() -> scoped.getTableById("table-id"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> scoped.listTablesPagedGlobally(null, null, null, null))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> scoped.renameTable(TABLE, Identifier.create(DATABASE, "renamed")))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> scoped.createBranch(TABLE, "nested", null))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(server.getRequestCount()).isEqualTo(1);
    }

    @Test
    void testBatchReadWriteAndPinnedTagWithRealDataFiles() throws Exception {
        // A small stateful fixture resolves references; production reference lifecycle is separate.
        org.apache.paimon.fs.Path location =
                new org.apache.paimon.fs.Path(tempDir.resolve("features").toUri());
        LocalFileIO fileIO = LocalFileIO.create();
        for (String branch : new String[] {"main", "physical-experiment"}) {
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
                            String prefix = DATABASE_PATH + "/trees/";
                            if (!route.startsWith(prefix)) {
                                unexpected.add(route);
                                return response(500, "{}");
                            }
                            String[] parts = route.substring(prefix.length()).split("/");
                            String reference = parts[0];
                            if (parts.length < 3
                                    || !parts[1].equals("tables")
                                    || !parts[2].equals("features")) {
                                unexpected.add(route);
                                return response(500, "{}");
                            }
                            String branch =
                                    reference.equals("main") ? "main" : "physical-experiment";
                            if (request.getMethod().equals("GET") && parts.length == 3) {
                                return response(200, tableResponse(branch, 0));
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
                                            409, "{\"code\":409,\"message\":\"tag is immutable\"}");
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

        RESTCatalog main = catalog.withReference(DATABASE, "main");
        RESTCatalog experiment = catalog.withReference(DATABASE, "experiment");
        writeRows(main, 10);
        writeRows(experiment, 20);
        snapshots.put("train_v1", snapshots.get("experiment"));
        RESTCatalog tag = catalog.withReference(DATABASE, "train_v1");
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
        assertThat(unexpected).isEmpty();
    }

    private void writeRows(RESTCatalog scoped, int value) throws Exception {
        FileStoreTable table = InstantiationUtil.clone((FileStoreTable) scoped.getTable(TABLE));
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            write.write(GenericRow.of(value));
            commit.commit(write.prepareCommit());
        }
    }

    private List<Integer> readRows(RESTCatalog scoped) throws Exception {
        FileStoreTable table = InstantiationUtil.clone((FileStoreTable) scoped.getTable(TABLE));
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
                .option(BRANCH.key(), branch)
                .build();
    }

    private String tableResponse(String branch) throws Exception {
        return tableResponse(branch, 2);
    }

    private String tableResponse(String branch, long schemaId) throws Exception {
        return RESTApi.toJson(
                new GetTableResponse(
                        "table-id",
                        DATABASE,
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
