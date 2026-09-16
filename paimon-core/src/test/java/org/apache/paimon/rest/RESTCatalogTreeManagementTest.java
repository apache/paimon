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
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.management.TreeManagement;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.exceptions.AlreadyExistsException;
import org.apache.paimon.rest.exceptions.NoSuchResourceException;
import org.apache.paimon.rest.exceptions.NotImplementedException;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;
import static org.apache.paimon.rest.DatabaseReferenceType.BRANCH;
import static org.apache.paimon.rest.DatabaseReferenceType.TAG;
import static org.apache.paimon.rest.RESTCatalogOptions.TOKEN;
import static org.apache.paimon.rest.RESTCatalogOptions.TOKEN_PROVIDER;
import static org.apache.paimon.rest.RESTCatalogOptions.URI;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises database tree management through a configured REST catalog and its HTTP client. */
class RESTCatalogTreeManagementTest {

    private static final String DATABASE = "training db";
    private static final String TREES_PATH = "/v1/catalog%2Fid/databases/training+db/trees";
    private static final String MAIN_JSON = "{\"type\":\"BRANCH\",\"name\":\"main\"}";
    private static final String BRANCH_JSON = "{\"type\":\"BRANCH\",\"name\":\"exp-1\"}";
    private static final String TAG_JSON = "{\"type\":\"TAG\",\"name\":\"train-v1\"}";

    private MockWebServer server;
    private RESTCatalog catalog;
    private TreeManagement trees;

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
        options.set(WAREHOUSE, "warehouse-id");
        options.set(TOKEN_PROVIDER, "bear");
        options.set(TOKEN, "test-token");
        catalog = new RESTCatalog(CatalogContext.create(options));
        trees = catalog.treeManagement();

        RecordedRequest config = server.takeRequest(10, TimeUnit.SECONDS);
        assertThat(config).isNotNull();
        assertThat(config.getRequestUrl().encodedPath()).isEqualTo("/v1/config");
        assertThat(config.getRequestUrl().queryParameter("warehouse")).isEqualTo("warehouse-id");
        assertThat(server.getRequestCount()).isEqualTo(1);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (catalog != null) {
            catalog.close();
        }
        if (server != null) {
            server.shutdown();
        }
    }

    @ParameterizedTest
    @EnumSource(DatabaseReferenceType.class)
    void testBranchAndTagOperationsUseCatalogConfiguration(DatabaseReferenceType sourceType)
            throws Exception {
        DatabaseReference main = new DatabaseReference(BRANCH, "main");
        DatabaseReference branch = new DatabaseReference(BRANCH, "exp-1");
        DatabaseReference tag = new DatabaseReference(TAG, "train-v1");

        enqueue(200, "{\"reference\":" + MAIN_JSON + "}");
        assertThat(trees.getReference(DATABASE, "main")).isEqualTo(main);
        takeRequest("GET", TREES_PATH + "/main");

        enqueue(200, "{\"reference\":" + BRANCH_JSON + "}");
        assertThat(trees.createReference(DATABASE, "exp-1", BRANCH, main)).isEqualTo(branch);
        RecordedRequest createBranch = takeRequest("POST", TREES_PATH);
        assertBody(
                createBranch,
                "{\"name\":\"exp-1\",\"type\":\"BRANCH\",\"source\":" + MAIN_JSON + "}");

        enqueue(200, "{\"reference\":" + TAG_JSON + "}");
        assertThat(trees.createReference(DATABASE, "train-v1", TAG, branch)).isEqualTo(tag);
        RecordedRequest createTag = takeRequest("POST", TREES_PATH);
        assertBody(
                createTag,
                "{\"name\":\"train-v1\",\"type\":\"TAG\",\"source\":" + BRANCH_JSON + "}");

        enqueue(200, "{\"reference\":" + MAIN_JSON + "}");
        DatabaseReference source = sourceType == BRANCH ? branch : tag;
        assertThat(trees.fastForwardBranch(DATABASE, "main", source)).isEqualTo(main);
        RecordedRequest fastForward = takeRequest("POST", TREES_PATH + "/main/forward");
        assertBody(
                fastForward,
                "{\"source\":" + (sourceType == BRANCH ? BRANCH_JSON : TAG_JSON) + "}");

        enqueue(200, "{\"reference\":" + BRANCH_JSON + "}");
        assertThat(trees.deleteReference(DATABASE, "exp-1", BRANCH)).isEqualTo(branch);
        RecordedRequest deleteBranch = takeRequest("DELETE", TREES_PATH + "/exp-1");
        assertBody(deleteBranch, "{\"type\":\"BRANCH\"}");

        enqueue(200, "{\"reference\":" + TAG_JSON + "}");
        assertThat(trees.deleteReference(DATABASE, "train-v1", null)).isEqualTo(tag);
        assertBody(takeRequest("DELETE", TREES_PATH + "/train-v1"), "{}");
        assertThat(server.getRequestCount()).isEqualTo(7);
    }

    @Test
    void testListPagesPreserveFilterAndTokens() throws Exception {
        enqueue(200, "{\"references\":[" + TAG_JSON + "],\"nextPageToken\":\"next +/%?&\"}");
        PagedList<DatabaseReference> page =
                trees.listReferencesPaged(DATABASE, TAG, 10, "start +/%");
        assertThat(page.getElements()).containsExactly(new DatabaseReference(TAG, "train-v1"));
        assertThat(page.getNextPageToken()).isEqualTo("next +/%?&");
        RecordedRequest paged = takeRequest("GET", TREES_PATH);
        assertThat(paged.getRequestUrl().queryParameter("type")).isEqualTo("tag");
        assertThat(paged.getRequestUrl().queryParameter("maxResults")).isEqualTo("10");
        assertThat(paged.getRequestUrl().queryParameter("pageToken")).isEqualTo("start +/%");

        enqueue(200, "{\"references\":[" + MAIN_JSON + "],\"nextPageToken\":\"next +/%?&\"}");
        enqueue(200, "{\"references\":[" + BRANCH_JSON + "]}");
        PagedList<DatabaseReference> firstPage =
                trees.listReferencesPaged(DATABASE, BRANCH, null, null);
        assertThat(firstPage.getElements()).containsExactly(new DatabaseReference(BRANCH, "main"));
        assertThat(firstPage.getNextPageToken()).isEqualTo("next +/%?&");
        RecordedRequest first = takeRequest("GET", TREES_PATH);
        assertThat(first.getRequestUrl().queryParameter("type")).isEqualTo("branch");
        assertThat(first.getRequestUrl().queryParameter("pageToken")).isNull();
        PagedList<DatabaseReference> secondPage =
                trees.listReferencesPaged(DATABASE, BRANCH, null, firstPage.getNextPageToken());
        assertThat(secondPage.getElements())
                .containsExactly(new DatabaseReference(BRANCH, "exp-1"));
        assertThat(secondPage.getNextPageToken()).isNull();
        RecordedRequest second = takeRequest("GET", TREES_PATH);
        assertThat(second.getRequestUrl().queryParameter("type")).isEqualTo("branch");
        assertThat(second.getRequestUrl().queryParameter("pageToken")).isEqualTo("next +/%?&");
        assertThat(second.getRequestUrl().queryParameter("maxResults")).isNull();
    }

    @ParameterizedTest
    @EnumSource(DatabaseReferenceType.class)
    void testMergeUsesCatalogConfiguration(DatabaseReferenceType sourceType) throws Exception {
        enqueue(200, "{\"reference\":" + MAIN_JSON + "}");
        DatabaseReference source = new DatabaseReference(sourceType, "experiment");

        assertThat(trees.mergeBranch(DATABASE, "main", source))
                .isEqualTo(new DatabaseReference(BRANCH, "main"));

        RecordedRequest merge = takeRequest("POST", TREES_PATH + "/main/merge");
        assertBody(
                merge,
                sourceType == BRANCH
                        ? "{\"source\":{\"type\":\"BRANCH\",\"name\":\"experiment\"}}"
                        : "{\"source\":{\"type\":\"TAG\",\"name\":\"experiment\"}}");
        assertThat(server.getRequestCount()).isEqualTo(2);
    }

    @Test
    void testMergeErrorsPreserveDetails() throws Exception {
        DatabaseReference source = new DatabaseReference(BRANCH, "experiment");
        enqueue(
                409,
                "{\"code\":409,\"message\":\"Conflicting changes to table features\","
                        + "\"resourceType\":\"TABLE\",\"resourceName\":\"training db.features\"}");
        assertThatThrownBy(() -> trees.mergeBranch(DATABASE, "main", source))
                .isInstanceOfSatisfying(
                        AlreadyExistsException.class,
                        conflict -> {
                            assertThat(conflict.resourceType()).isEqualTo("TABLE");
                            assertThat(conflict.resourceName()).isEqualTo("training db.features");
                        })
                .hasMessageContaining("Conflicting changes to table features");
        takeRequest("POST", TREES_PATH + "/main/merge");

        enqueue(404, "{\"code\":404,\"message\":\"source reference missing\"}");
        assertThatThrownBy(() -> trees.mergeBranch(DATABASE, "main", source))
                .isInstanceOf(NoSuchResourceException.class)
                .hasMessageContaining("source reference missing");
        takeRequest("POST", TREES_PATH + "/main/merge");

        enqueue(501, "{\"code\":501,\"message\":\"merge unsupported\"}");
        assertThatThrownBy(() -> trees.mergeBranch(DATABASE, "main", source))
                .isInstanceOf(NotImplementedException.class)
                .hasMessageContaining("merge unsupported");
        takeRequest("POST", TREES_PATH + "/main/merge");
        assertThat(server.getRequestCount()).isEqualTo(4);
    }

    @Test
    void testListAllTypesAndEmptyReferences() throws Exception {
        enqueue(200, "{\"references\":[" + MAIN_JSON + "," + TAG_JSON + "]}");
        assertThat(trees.listReferencesPaged(DATABASE, null, null, null).getElements())
                .containsExactly(
                        new DatabaseReference(BRANCH, "main"),
                        new DatabaseReference(TAG, "train-v1"));
        assertThat(takeRequest("GET", TREES_PATH).getRequestUrl().query()).isNull();

        enqueue(200, "{\"references\":[]}");
        PagedList<DatabaseReference> emptyPage =
                trees.listReferencesPaged(DATABASE, null, null, null);
        assertThat(emptyPage.getElements()).isEmpty();
        assertThat(emptyPage.getNextPageToken()).isNull();
        takeRequest("GET", TREES_PATH);
        assertThat(server.getRequestCount()).isEqualTo(3);
    }

    @Test
    void testErrorsPropagate() {
        enqueue(404, "{\"code\":404,\"message\":\"reference missing\"}");
        assertThatThrownBy(() -> trees.getReference(DATABASE, "missing"))
                .isInstanceOf(NoSuchResourceException.class)
                .hasMessageContaining("reference missing");

        enqueue(409, "{\"code\":409,\"message\":\"reference already exists\"}");
        assertThatThrownBy(
                        () ->
                                trees.createReference(
                                        DATABASE,
                                        "exp-1",
                                        BRANCH,
                                        new DatabaseReference(BRANCH, "main")))
                .isInstanceOf(AlreadyExistsException.class)
                .hasMessageContaining("reference already exists");

        enqueue(501, "{\"code\":501,\"message\":\"trees unsupported\"}");
        assertThatThrownBy(() -> trees.listReferencesPaged(DATABASE, null, null, null))
                .isInstanceOf(NotImplementedException.class)
                .hasMessageContaining("trees unsupported");
        assertThat(server.getRequestCount()).isEqualTo(4);
    }

    private void enqueue(int status, String body) {
        server.enqueue(
                new MockResponse()
                        .setResponseCode(status)
                        .setHeader("Content-Type", "application/json")
                        .setBody(body));
    }

    private RecordedRequest takeRequest(String method, String path) throws Exception {
        RecordedRequest request = server.takeRequest(10, TimeUnit.SECONDS);
        assertThat(request).isNotNull();
        assertThat(request.getMethod()).isEqualTo(method);
        assertThat(request.getRequestUrl().encodedPath()).isEqualTo(path);
        assertThat(request.getHeader("Authorization")).isEqualTo("Bearer test-token");
        assertThat(request.getHeader("X-Catalog-Context")).isEqualTo("configured");
        return request;
    }

    private static void assertBody(RecordedRequest request, String expectedJson) throws Exception {
        assertThat(request.getRequestUrl().query()).isNull();
        assertThat(RESTApi.fromJson(request.getBody().readUtf8(), Map.class))
                .isEqualTo(RESTApi.fromJson(expectedJson, Map.class));
    }
}
