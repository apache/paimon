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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.function.FunctionChange;
import org.apache.paimon.function.FunctionImpl;
import org.apache.paimon.management.ColumnMask;
import org.apache.paimon.management.DataPolicy;
import org.apache.paimon.management.ListPermissionsRequest;
import org.apache.paimon.management.ListPoliciesRequest;
import org.apache.paimon.management.PermissionAssignment;
import org.apache.paimon.management.PermissionResource;
import org.apache.paimon.management.PolicyType;
import org.apache.paimon.management.ResourceType;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.auth.DLFOpenApiV4Signer;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.Instant;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.view.SemanticViewDefinition;
import org.apache.paimon.view.ViewChange;
import org.apache.paimon.view.ViewSchema;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;
import static org.apache.paimon.rest.RESTCatalogInternalOptions.PREFIX;
import static org.apache.paimon.rest.RESTCatalogOptions.DLF_ACCESS_KEY_ID;
import static org.apache.paimon.rest.RESTCatalogOptions.DLF_ACCESS_KEY_SECRET;
import static org.apache.paimon.rest.RESTCatalogOptions.DLF_REGION;
import static org.apache.paimon.rest.RESTCatalogOptions.DLF_SIGNING_ALGORITHM;
import static org.apache.paimon.rest.RESTCatalogOptions.TOKEN_PROVIDER;
import static org.apache.paimon.rest.RESTCatalogOptions.URI;
import static org.assertj.core.api.Assertions.assertThat;

/** Every {@link RESTApi} call names its API, and the ACS4 signer sends it as x-acs-action. */
class RESTApiNamesTest {

    private static final String DB = "/v1/catalog/databases/db";
    private static final String TABLE = DB + "/tables/t";
    private static final Identifier ID = Identifier.create("db", "t");
    private static final Map<String, String> PARTITION = Collections.singletonMap("dt", "1");

    private final List<String[]> requests = new CopyOnWriteArrayList<>();
    private HttpServer server;
    private Options options;
    private RESTApi api;

    @BeforeEach
    void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/v1/",
                exchange -> {
                    requests.add(
                            new String[] {
                                exchange.getRequestMethod(),
                                exchange.getRequestURI().getRawPath(),
                                exchange.getRequestHeaders().getFirst("x-acs-action")
                            });
                    byte[] data = "{}".getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(200, data.length);
                    try (OutputStream output = exchange.getResponseBody()) {
                        output.write(data);
                    } finally {
                        exchange.close();
                    }
                });
        server.start();

        options = new Options();
        options.set(URI, "http://127.0.0.1:" + server.getAddress().getPort());
        options.set(PREFIX, "catalog");
        options.set(TOKEN_PROVIDER, "dlf");
        options.set(DLF_ACCESS_KEY_ID, "akId");
        options.set(DLF_ACCESS_KEY_SECRET, "akSecret");
        options.set(DLF_REGION, "cn-hangzhou");
        options.set(DLF_SIGNING_ALGORITHM, DLFOpenApiV4Signer.IDENTIFIER);
        api = new RESTApi(options, false);
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void testConfig() {
        options.set(WAREHOUSE, "wh");
        expect("GET", "/v1/config", RESTApiNames.GET_CONFIG, () -> new RESTApi(options));
    }

    @Test
    void testDatabases() {
        String dbs = "/v1/catalog/databases";
        expect("GET", dbs, RESTApiNames.LIST_DATABASES, () -> api.listDatabases());
        expect(
                "GET",
                dbs,
                RESTApiNames.LIST_DATABASES,
                () -> api.listDatabasesPaged(10, null, "d%"));
        expect(
                "POST",
                dbs,
                RESTApiNames.CREATE_DATABASE,
                () -> api.createDatabase("db", Collections.emptyMap()));
        expect("GET", DB, RESTApiNames.GET_DATABASE, () -> api.getDatabase("db"));
        expect(
                "POST",
                DB,
                RESTApiNames.ALTER_DATABASE,
                () -> api.alterDatabase("db", Collections.emptyList(), PARTITION));
        expect("DELETE", DB, RESTApiNames.DROP_DATABASE, () -> api.dropDatabase("db"));
    }

    @Test
    void testTables() {
        Schema schema = Schema.newBuilder().column("id", DataTypes.INT()).build();
        expect("GET", DB + "/tables", RESTApiNames.LIST_TABLES, () -> api.listTables("db"));
        expect(
                "GET",
                DB + "/tables",
                RESTApiNames.LIST_TABLES,
                () -> api.listTablesPaged("db", 10, null, "t%", null));
        expect(
                "GET",
                DB + "/table-details",
                RESTApiNames.LIST_TABLE_DETAILS,
                () -> api.listTableDetails("db"));
        expect(
                "GET",
                DB + "/table-details",
                RESTApiNames.LIST_TABLE_DETAILS,
                () -> api.listTableDetailsPaged("db", 10, null, null, null));
        expect(
                "GET",
                "/v1/catalog/tables",
                RESTApiNames.LIST_TABLES_GLOBALLY,
                () -> api.listTablesPagedGlobally("d%", "t%", 10, null));
        expect(
                "POST",
                DB + "/tables",
                RESTApiNames.CREATE_TABLE,
                () -> api.createTable(ID, schema));
        expect(
                "POST",
                DB + "/register",
                RESTApiNames.REGISTER_TABLE,
                () -> api.registerTable(ID, "oss://bucket/t"));
        expect("GET", TABLE, RESTApiNames.GET_TABLE, () -> api.getTable(ID));
        expect(
                "GET",
                "/v1/catalog/tables/id/tbl-1",
                RESTApiNames.GET_TABLE_BY_ID,
                () -> api.getTableById("tbl-1"));
        expect(
                "POST",
                TABLE,
                RESTApiNames.ALTER_TABLE,
                () ->
                        api.alterTable(
                                ID, Collections.singletonList(SchemaChange.setOption("k", "v"))));
        expect(
                "POST",
                TABLE + "/replace",
                RESTApiNames.REPLACE_TABLE,
                () -> api.replaceTable(ID, schema));
        expect(
                "POST",
                "/v1/catalog/tables/rename",
                RESTApiNames.RENAME_TABLE,
                () -> api.renameTable(ID, Identifier.create("db", "t2")));
        expect("DELETE", TABLE, RESTApiNames.DROP_TABLE, () -> api.dropTable(ID));
        expect(
                "POST",
                TABLE + "/auth",
                RESTApiNames.AUTH_TABLE_QUERY,
                () -> api.authTableQuery(ID, Collections.singletonList("id")));
        expect("GET", TABLE + "/token", RESTApiNames.GET_TABLE_TOKEN, () -> api.loadTableToken(ID));
    }

    @Test
    void testSnapshotsSchemasAndConsumers() {
        expect(
                "POST",
                TABLE + "/commit",
                RESTApiNames.COMMIT_TABLE,
                () -> api.commitSnapshot(ID, "uuid", null, null, Collections.emptyList()));
        expect(
                "GET",
                TABLE + "/snapshot",
                RESTApiNames.GET_TABLE_SNAPSHOT,
                () -> api.loadSnapshot(ID));
        expect(
                "GET",
                TABLE + "/snapshots/3",
                RESTApiNames.GET_VERSION_SNAPSHOT,
                () -> api.loadSnapshot(ID, "3"));
        expect(
                "GET",
                TABLE + "/snapshots",
                RESTApiNames.LIST_SNAPSHOTS,
                () -> api.listSnapshotsPaged(ID, 10, null));
        expect(
                "POST",
                TABLE + "/rollback",
                RESTApiNames.ROLLBACK_TO_SNAPSHOT,
                () -> api.rollbackTo(ID, Instant.snapshot(1L)));
        expect(
                "POST",
                TABLE + "/rollback",
                RESTApiNames.ROLLBACK_TO_SNAPSHOT,
                () -> api.rollbackTo(ID, Instant.snapshot(1L), 2L));
        expect("GET", TABLE + "/schemas/1", RESTApiNames.GET_SCHEMA, () -> api.loadSchema(ID, "1"));
        expect(
                "GET",
                TABLE + "/schemas",
                RESTApiNames.LIST_SCHEMAS,
                () -> api.listSchemasPaged(ID, 10, null));
        expect(
                "POST",
                TABLE + "/rollback-schema",
                RESTApiNames.ROLLBACK_SCHEMA,
                () -> api.rollbackSchema(ID, 1L));
        expect(
                "GET",
                TABLE + "/consumers",
                RESTApiNames.LIST_CONSUMERS,
                () -> api.listConsumersPaged(ID, 10, null));
        expect(
                "POST",
                TABLE + "/consumers/reset",
                RESTApiNames.RESET_CONSUMER,
                () -> api.resetConsumer(ID, "c1", 1L));
    }

    @Test
    void testPartitions() {
        List<Map<String, String>> partitions = Collections.singletonList(PARTITION);
        expect(
                "GET",
                TABLE + "/partitions",
                RESTApiNames.LIST_PARTITIONS,
                () -> api.listPartitions(ID));
        expect(
                "GET",
                TABLE + "/partitions",
                RESTApiNames.LIST_PARTITIONS,
                () -> api.listPartitionsPaged(ID, 10, null, "dt%"));
        expect(
                "POST",
                TABLE + "/partitions",
                RESTApiNames.CREATE_PARTITIONS,
                () -> api.createPartitions(ID, partitions, true, null, false, null));
        expect(
                "POST",
                TABLE + "/partitions/drop",
                RESTApiNames.DROP_PARTITIONS,
                () -> api.dropPartitions(ID, partitions, true));
        expect(
                "POST",
                TABLE + "/partitions/mark",
                RESTApiNames.MARK_DONE_PARTITIONS,
                () -> api.markDonePartitions(ID, partitions));
        expect(
                "POST",
                TABLE + "/partitions/list-by-names",
                RESTApiNames.LIST_PARTITIONS_BY_NAMES,
                () -> api.listPartitionsByNames(ID, partitions));
        expect(
                "POST",
                TABLE + "/partitions/list-by-filter",
                RESTApiNames.LIST_PARTITIONS_BY_FILTER,
                () -> api.listPartitionsByFilterPaged(ID, "{}", 10, null, null));
    }

    @Test
    void testBranchesAndTags() {
        expect("GET", TABLE + "/branches", RESTApiNames.LIST_BRANCHES, () -> api.listBranches(ID));
        expect(
                "POST",
                TABLE + "/branches",
                RESTApiNames.CREATE_BRANCH,
                () -> api.createBranch(ID, "b", null));
        expect(
                "DELETE",
                TABLE + "/branches/b",
                RESTApiNames.DROP_BRANCH,
                () -> api.dropBranch(ID, "b"));
        expect(
                "POST",
                TABLE + "/branches/b/forward",
                RESTApiNames.FAST_FORWARD_BRANCH,
                () -> api.fastForward(ID, "b"));
        expect(
                "GET",
                TABLE + "/tags",
                RESTApiNames.LIST_TAGS,
                () -> api.listTagsPaged(ID, 10, null, null));
        expect(
                "POST",
                TABLE + "/tags",
                RESTApiNames.CREATE_TAG,
                () -> api.createTag(ID, "tag", 1L, null));
        expect("GET", TABLE + "/tags/tag", RESTApiNames.GET_TAG, () -> api.getTag(ID, "tag"));
        expect(
                "DELETE",
                TABLE + "/tags/tag",
                RESTApiNames.DROP_TAG,
                () -> api.deleteTag(ID, "tag"));
    }

    @Test
    void testViews() {
        Identifier view = Identifier.create("db", "v");
        ViewSchema schema =
                new ViewSchema(
                        Collections.emptyList(),
                        "SELECT 1",
                        Collections.emptyMap(),
                        null,
                        Collections.emptyMap());
        expect("GET", DB + "/views", RESTApiNames.LIST_VIEWS, () -> api.listViews("db"));
        expect(
                "GET",
                DB + "/views",
                RESTApiNames.LIST_VIEWS,
                () -> api.listViewsPaged("db", 10, null, null));
        expect(
                "GET",
                DB + "/view-details",
                RESTApiNames.LIST_VIEW_DETAILS,
                () -> api.listViewDetailsPaged("db", 10, null, null));
        expect(
                "GET",
                "/v1/catalog/views",
                RESTApiNames.LIST_VIEWS_GLOBALLY,
                () -> api.listViewsPagedGlobally(null, null, 10, null));
        expect("POST", DB + "/views", RESTApiNames.CREATE_VIEW, () -> api.createView(view, schema));
        expect("GET", DB + "/views/v", RESTApiNames.GET_VIEW, () -> api.getView(view));
        expect(
                "POST",
                DB + "/views/v",
                RESTApiNames.ALTER_VIEW,
                () ->
                        api.alterView(
                                view, Collections.singletonList(ViewChange.setOption("k", "v"))));
        expect(
                "POST",
                "/v1/catalog/views/rename",
                RESTApiNames.RENAME_VIEW,
                () -> api.renameView(view, Identifier.create("db", "v2")));
        expect("DELETE", DB + "/views/v", RESTApiNames.DROP_VIEW, () -> api.dropView(view));
    }

    @Test
    void testFunctions() {
        Identifier function = Identifier.create("db", "f");
        expect(
                "GET",
                DB + "/functions",
                RESTApiNames.LIST_FUNCTIONS,
                () -> api.listFunctions("db"));
        expect(
                "GET",
                DB + "/functions",
                RESTApiNames.LIST_FUNCTIONS,
                () -> api.listFunctionsPaged("db", 10, null, null));
        expect(
                "GET",
                DB + "/function-details",
                RESTApiNames.LIST_FUNCTION_DETAILS,
                () -> api.listFunctionDetailsPaged("db", 10, null, null));
        expect(
                "GET",
                "/v1/catalog/functions",
                RESTApiNames.LIST_FUNCTIONS_GLOBALLY,
                () -> api.listFunctionsPagedGlobally(null, null, 10, null));
        expect(
                "POST",
                DB + "/functions",
                RESTApiNames.CREATE_FUNCTION,
                () ->
                        api.createFunction(
                                function, new FunctionImpl(function, Collections.emptyMap())));
        expect(
                "GET",
                DB + "/functions/f",
                RESTApiNames.GET_FUNCTION,
                () -> api.getFunction(function));
        expect(
                "POST",
                DB + "/functions/f",
                RESTApiNames.ALTER_FUNCTION,
                () ->
                        api.alterFunction(
                                function,
                                Collections.singletonList(FunctionChange.setOption("k", "v"))));
        expect(
                "DELETE",
                DB + "/functions/f",
                RESTApiNames.DROP_FUNCTION,
                () -> api.dropFunction(function));
    }

    @Test
    void testPermissionsAndPolicies() {
        PermissionResource table =
                new PermissionResource(ResourceType.TABLE, "db", "t", null, null);
        expect(
                "GET",
                "/v1/catalog/permissions",
                RESTApiNames.LIST_PERMISSION_ASSIGNMENTS,
                () ->
                        api.listPermissions(
                                new ListPermissionsRequest(
                                        ResourceType.TABLE,
                                        "db",
                                        "t",
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        10)));
        expect(
                "POST",
                "/v1/catalog/permissions/grant",
                RESTApiNames.GRANT_PERMISSION_ASSIGNMENT,
                () -> api.grantPermission(new PermissionAssignment(table, "SELECT", "u", null)));
        expect(
                "POST",
                "/v1/catalog/permissions/revoke",
                RESTApiNames.REVOKE_PERMISSION_ASSIGNMENT,
                () -> api.revokePermission(table, "SELECT", "u"));
        expect(
                "GET",
                TABLE + "/policies",
                RESTApiNames.LIST_POLICIES,
                () -> api.listPolicies(new ListPoliciesRequest(table, null, null, null, null, 10)));
        expect(
                "POST",
                TABLE + "/policies",
                RESTApiNames.CREATE_POLICY,
                () ->
                        api.createPolicy(
                                DataPolicy.columnMask(table, new ColumnMask("id", "{}"), "u")));
        expect(
                "POST",
                TABLE + "/policies/drop",
                RESTApiNames.DROP_POLICY,
                () -> api.dropPolicy(table, PolicyType.COLUMN_MASKING, "u", "id", true));
    }

    @Test
    void testLabelsAndSemanticViews() {
        String labels = "/v1/catalog/labels/TABLE/db.t";
        Identifier semanticView = Identifier.create("db", "sv");
        expect("GET", labels, RESTApiNames.LIST_LABELS, () -> api.listLabels("TABLE", "db.t"));
        expect(
                "GET",
                labels,
                RESTApiNames.LIST_LABELS,
                () -> api.listLabelsPaged("TABLE", "db.t", 10, null));
        expect(
                "POST",
                labels + "/k",
                RESTApiNames.UPSERT_LABEL,
                () -> api.upsertLabel("TABLE", "db.t", "k", "v"));
        expect(
                "GET",
                labels + "/k",
                RESTApiNames.GET_LABEL,
                () -> api.getLabel("TABLE", "db.t", "k"));
        expect(
                "DELETE",
                labels + "/k",
                RESTApiNames.DELETE_LABEL,
                () -> api.deleteLabel("TABLE", "db.t", "k"));
        expect(
                "GET",
                DB + "/semantic-views",
                RESTApiNames.LIST_SEMANTIC_VIEWS,
                () -> api.listSemanticViews("db"));
        expect(
                "GET",
                DB + "/semantic-views",
                RESTApiNames.LIST_SEMANTIC_VIEWS,
                () -> api.listSemanticViewsPaged("db", 10, null));
        expect(
                "POST",
                DB + "/semantic-views/sv",
                RESTApiNames.UPSERT_SEMANTIC_VIEW,
                () ->
                        api.upsertSemanticView(
                                semanticView, new SemanticViewDefinition("provider-yaml", "a: b")));
        expect(
                "GET",
                DB + "/semantic-views/sv",
                RESTApiNames.GET_SEMANTIC_VIEW,
                () -> api.getSemanticView(semanticView));
        expect(
                "DELETE",
                DB + "/semantic-views/sv",
                RESTApiNames.DELETE_SEMANTIC_VIEW,
                () -> api.deleteSemanticView(semanticView));
    }

    /** Stub replies do not parse into every response type, so only the outgoing requests count. */
    private void expect(String method, String path, String apiName, Runnable call) {
        requests.clear();
        try {
            call.run();
        } catch (RuntimeException ignored) {
            // the request has already been recorded by then
        }
        assertThat(requests).as("%s %s", method, path).isNotEmpty();
        for (String[] request : requests) {
            assertThat(request).containsExactly(method, path, apiName);
        }
    }
}
