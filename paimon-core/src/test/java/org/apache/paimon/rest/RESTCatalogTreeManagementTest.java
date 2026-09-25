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
import org.apache.paimon.rest.exceptions.BadRequestException;
import org.apache.paimon.rest.exceptions.NoSuchResourceException;
import org.apache.paimon.rest.exceptions.NotImplementedException;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;
import static org.apache.paimon.rest.RESTCatalogOptions.TOKEN;
import static org.apache.paimon.rest.RESTCatalogOptions.TOKEN_PROVIDER;
import static org.apache.paimon.rest.RESTCatalogOptions.URI;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises database tree management through a configured REST catalog and its HTTP client. */
class RESTCatalogTreeManagementTest {

    private static final String DATABASE = "training db";
    private static final String DATABASE_PATH = "/v1/catalog%2Fid/databases/training+db";

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

    @Test
    void testBranchAndTagOperationsUseCatalogConfiguration() throws Exception {
        enqueue(200, "{\"branches\":[\"main\",\"experiment\"]}");
        assertThat(trees.listBranches(DATABASE)).containsExactly("main", "experiment");
        takeRequest("GET", DATABASE_PATH + "/branches");

        enqueue(200, "");
        trees.createTag(DATABASE, "baseline", null, null);
        assertBody(
                takeRequest("POST", DATABASE_PATH + "/tags"),
                "{\"tagName\":\"baseline\",\"fromBranch\":null,\"timeRetained\":null}");

        enqueue(200, "");
        trees.createBranch(DATABASE, "experiment", "baseline");
        assertBody(
                takeRequest("POST", DATABASE_PATH + "/branches"),
                "{\"branch\":\"experiment\",\"fromTag\":\"baseline\"}");

        enqueue(200, "");
        trees.createTag(DATABASE, "train-v1", "experiment", "7d");
        assertBody(
                takeRequest("POST", DATABASE_PATH + "/tags"),
                "{\"tagName\":\"train-v1\",\"fromBranch\":\"experiment\",\"timeRetained\":\"7d\"}");

        enqueue(200, "{\"tagName\":\"train-v1\",\"fromBranch\":\"experiment\"}");
        assertThat(trees.getTag(DATABASE, "train-v1").fromBranch()).isEqualTo("experiment");
        takeRequest("GET", DATABASE_PATH + "/tags/train-v1");

        enqueue(200, "");
        trees.fastForward(DATABASE, "experiment");
        assertBody(takeRequest("POST", DATABASE_PATH + "/branches/experiment/forward"), "{}");

        enqueue(200, "");
        trees.dropBranch(DATABASE, "experiment");
        assertThat(takeRequest("DELETE", DATABASE_PATH + "/branches/experiment").getBodySize())
                .isZero();

        enqueue(200, "");
        trees.deleteTag(DATABASE, "train-v1");
        assertThat(takeRequest("DELETE", DATABASE_PATH + "/tags/train-v1").getBodySize()).isZero();
        assertThat(server.getRequestCount()).isEqualTo(9);
    }

    @Test
    void testTagPagesPreserveFilterAndTokens() throws Exception {
        enqueue(200, "{\"tags\":[\"train-v1\"],\"nextPageToken\":\"next +/%?&\"}");
        PagedList<String> page = trees.listTagsPaged(DATABASE, 10, "start +/%", "train-");
        assertThat(page.getElements()).containsExactly("train-v1");
        assertThat(page.getNextPageToken()).isEqualTo("next +/%?&");
        RecordedRequest first = takeRequest("GET", DATABASE_PATH + "/tags");
        assertThat(first.getRequestUrl().queryParameter("maxResults")).isEqualTo("10");
        assertThat(first.getRequestUrl().queryParameter("pageToken")).isEqualTo("start +/%");
        assertThat(first.getRequestUrl().queryParameter("tagNamePrefix")).isEqualTo("train-");

        enqueue(200, "{\"tags\":[]}");
        PagedList<String> last =
                trees.listTagsPaged(DATABASE, null, page.getNextPageToken(), "train-");
        assertThat(last.getElements()).isEmpty();
        assertThat(last.getNextPageToken()).isNull();
        RecordedRequest second = takeRequest("GET", DATABASE_PATH + "/tags");
        assertThat(second.getRequestUrl().queryParameter("pageToken")).isEqualTo("next +/%?&");
        assertThat(second.getRequestUrl().queryParameter("tagNamePrefix")).isEqualTo("train-");
        assertThat(second.getRequestUrl().queryParameter("maxResults")).isNull();
    }

    @Test
    void testErrorsKeepTableBranchAndTagConventions() throws Exception {
        server.enqueue(
                new MockResponse()
                        .setResponseCode(409)
                        .setHeader("Content-Type", "application/json")
                        .setHeader("x-request-id", "branch-request")
                        .setBody(
                                "{\"resourceType\":\"BRANCH\",\"resourceName\":\"experiment\",\"message\":\"branch exists\"}"));
        assertThatThrownBy(() -> trees.createBranch(DATABASE, "experiment", null))
                .isExactlyInstanceOf(AlreadyExistsException.class)
                .hasMessageContaining("branch exists")
                .hasMessageContaining("branch-request");
        takeRequest("POST", DATABASE_PATH + "/branches");

        enqueue(
                404,
                "{\"code\":404,\"resourceType\":\"TAG\",\"resourceName\":\"baseline\",\"message\":\"tag missing\"}");
        assertThatThrownBy(() -> trees.createBranch(DATABASE, "experiment", "baseline"))
                .isInstanceOfSatisfying(
                        NoSuchResourceException.class,
                        e -> {
                            assertThat(e.resourceType()).isEqualTo("TAG");
                            assertThat(e.resourceName()).isEqualTo("baseline");
                        });
        takeRequest("POST", DATABASE_PATH + "/branches");

        enqueue(400, "{\"code\":400,\"message\":\"source table has no snapshot\"}");
        assertThatThrownBy(() -> trees.fastForward(DATABASE, "empty"))
                .isInstanceOf(BadRequestException.class)
                .hasMessageContaining("no snapshot");
        takeRequest("POST", DATABASE_PATH + "/branches/empty/forward");

        enqueue(404, "{\"code\":404,\"message\":\"branch missing\"}");
        assertThatThrownBy(() -> trees.fastForward(DATABASE, "missing"))
                .isInstanceOf(NoSuchResourceException.class)
                .hasMessageContaining("branch missing");
        takeRequest("POST", DATABASE_PATH + "/branches/missing/forward");

        enqueue(501, "{\"code\":501,\"message\":\"forward unsupported\"}");
        assertThatThrownBy(() -> trees.fastForward(DATABASE, "experiment"))
                .isInstanceOf(NotImplementedException.class)
                .hasMessageContaining("forward unsupported");
        takeRequest("POST", DATABASE_PATH + "/branches/experiment/forward");
        assertThat(server.getRequestCount()).isEqualTo(6);
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
