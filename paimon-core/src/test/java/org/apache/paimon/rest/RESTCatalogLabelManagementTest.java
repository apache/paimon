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
import org.apache.paimon.management.Label;
import org.apache.paimon.management.LabelManagement;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.exceptions.ForbiddenException;
import org.apache.paimon.rest.exceptions.NoSuchResourceException;
import org.apache.paimon.rest.exceptions.NotImplementedException;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;
import static org.apache.paimon.rest.RESTCatalogOptions.TOKEN;
import static org.apache.paimon.rest.RESTCatalogOptions.TOKEN_PROVIDER;
import static org.apache.paimon.rest.RESTCatalogOptions.URI;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

/** Exercises label management through a configured REST catalog and its HTTP client. */
class RESTCatalogLabelManagementTest {

    private static final String BASE_PATH = "/v1/catalog%2Fid/labels";
    private static final String LABEL_JSON =
            "{\"entityType\":\"TABLE\",\"entityName\":\"sales.orders\","
                    + "\"key\":\"domain\",\"value\":\"sales\"}";

    private MockWebServer server;
    private RESTCatalog catalog;
    private LabelManagement labels;

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
        labels = catalog.labelManagement();

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
    void testUpsertReadAndDeleteUseCatalogConfiguration() throws Exception {
        String entityName = "sales db.model/v1";
        String key = "domain/华";
        String path = BASE_PATH + "/MODEL_VERSION/sales+db.model%2Fv1/domain%2F%E5%8D%8E";

        for (String value : Arrays.asList("sales", "")) {
            enqueue(200, "");
            labels.upsertLabel("MODEL_VERSION", entityName, key, value);
            RecordedRequest request = takeRequest("POST", path);
            assertThat(RESTApi.fromJson(request.getBody().readUtf8(), Map.class))
                    .isEqualTo(Collections.singletonMap("value", value));
        }

        enqueue(
                200,
                "{\"entityType\":\"MODEL_VERSION\",\"entityName\":\"sales db.model/v1\","
                        + "\"key\":\"domain/华\",\"value\":\"\"}");
        Label label = labels.getLabel("MODEL_VERSION", entityName, key);
        takeRequest("GET", path);
        assertThat(label.getEntityType()).isEqualTo("MODEL_VERSION");
        assertThat(label.getEntityName()).isEqualTo(entityName);
        assertThat(label.getKey()).isEqualTo(key);
        assertThat(label.getValue()).isEmpty();

        for (int i = 0; i < 2; i++) {
            enqueue(200, "");
            labels.deleteLabel("MODEL_VERSION", entityName, key);
            assertThat(takeRequest("DELETE", path).getBodySize()).isZero();
        }
        assertThat(server.getRequestCount()).isEqualTo(6);
    }

    @Test
    void testListPagePreservesBindingsAndContinuationToken() throws Exception {
        enqueue(200, "{\"labels\":[" + LABEL_JSON + "],\"nextPageToken\":\"next +/%?&\"}");

        PagedList<Label> page = labels.listLabelsPaged("TABLE", "sales.orders", 1000, "start +/%");
        assertThat(page.getElements())
                .extracting(
                        Label::getEntityType, Label::getEntityName, Label::getKey, Label::getValue)
                .containsExactly(tuple("TABLE", "sales.orders", "domain", "sales"));
        assertThat(page.getNextPageToken()).isEqualTo("next +/%?&");
        RecordedRequest request = takeRequest("GET", BASE_PATH + "/TABLE/sales.orders");
        assertThat(request.getRequestUrl().queryParameter("maxResults")).isEqualTo("1000");
        assertThat(request.getRequestUrl().queryParameter("pageToken")).isEqualTo("start +/%");
    }

    @Test
    void testListAllFollowsOpaqueTokens() throws Exception {
        enqueue(200, "{\"labels\":[" + LABEL_JSON + "],\"nextPageToken\":\"next +/%?&\"}");
        enqueue(200, "{\"labels\":[" + LABEL_JSON.replace("domain", "owner") + "]}");

        List<Label> all = labels.listLabels("TABLE", "sales.orders");
        assertThat(all).extracting(Label::getKey).containsExactly("domain", "owner");
        assertThat(takeRequest("GET", BASE_PATH + "/TABLE/sales.orders").getRequestUrl().query())
                .isNull();
        RecordedRequest second = takeRequest("GET", BASE_PATH + "/TABLE/sales.orders");
        assertThat(second.getRequestUrl().queryParameter("pageToken")).isEqualTo("next +/%?&");
        assertThat(second.getRequestUrl().queryParameter("maxResults")).isNull();
        assertThat(server.getRequestCount()).isEqualTo(3);
    }

    @Test
    void testEmptyEntityLabelsTerminatePagination() throws Exception {
        enqueue(200, "{\"labels\":[]}");
        PagedList<Label> page = labels.listLabelsPaged("COLUMN", "sales.orders.id", 1, "last");
        assertThat(page.getElements()).isEmpty();
        assertThat(page.getNextPageToken()).isNull();
        takeRequest("GET", BASE_PATH + "/COLUMN/sales.orders.id");

        enqueue(200, "{\"labels\":[]}");
        assertThat(labels.listLabels("TABLE", "sales.empty")).isEmpty();
        takeRequest("GET", BASE_PATH + "/TABLE/sales.empty");
        assertThat(server.getRequestCount()).isEqualTo(3);
    }

    @Test
    void testMissingEntityAndAccessErrorsPropagate() {
        List<Consumer<LabelManagement>> operations =
                Arrays.asList(
                        management -> management.upsertLabel("TABLE", "missing", "domain", "sales"),
                        management -> management.getLabel("TABLE", "missing", "domain"),
                        management -> management.listLabels("TABLE", "missing"),
                        management -> management.listLabelsPaged("TABLE", "missing", null, null),
                        management -> management.deleteLabel("TABLE", "missing", "domain"));
        for (Consumer<LabelManagement> operation : operations) {
            enqueue(
                    404,
                    "{\"code\":404,\"message\":\"entity missing\",\"resourceType\":\"TABLE\"}");
            assertThatThrownBy(() -> operation.accept(labels))
                    .isInstanceOf(NoSuchResourceException.class)
                    .hasMessageContaining("entity missing");
        }

        enqueue(403, "{\"code\":403,\"message\":\"label access denied\"}");
        assertThatThrownBy(() -> labels.upsertLabel("TABLE", "sales.orders", "domain", "sales"))
                .isInstanceOf(ForbiddenException.class);
        enqueue(501, "{\"code\":501,\"message\":\"labels unsupported\"}");
        assertThatThrownBy(() -> labels.listLabels("TABLE", "sales.orders"))
                .isInstanceOf(NotImplementedException.class);
        assertThat(server.getRequestCount()).isEqualTo(operations.size() + 3);
    }

    @Test
    void testInvalidInputsFailBeforeHttp() {
        assertThatThrownBy(() -> labels.upsertLabel("TABLE", "sales.orders", "domain", null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> labels.getLabel("", "sales.orders", "domain"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> labels.listLabels("TABLE", " "))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> labels.deleteLabel("TABLE", "sales.orders", null))
                .isInstanceOf(IllegalArgumentException.class);
        for (int limit : new int[] {0, 1001}) {
            assertThatThrownBy(() -> labels.listLabelsPaged("TABLE", "sales.orders", limit, null))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThat(server.getRequestCount()).isEqualTo(1);
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
}
