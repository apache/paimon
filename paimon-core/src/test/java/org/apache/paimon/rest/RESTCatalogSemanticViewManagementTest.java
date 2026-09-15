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
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.management.SemanticViewManagement;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.responses.GetSemanticViewResponse;
import org.apache.paimon.view.SemanticView;
import org.apache.paimon.view.SemanticViewDefinition;

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

/** Exercises semantic view management through a configured REST catalog and its HTTP client. */
class RESTCatalogSemanticViewManagementTest {

    private static final String BASE_PATH = "/v1/catalog%2Fid/databases/sales/semantic-views";
    private static final Identifier ID = Identifier.create("sales", "revenue");
    private static final SemanticViewDefinition DEFINITION =
            new SemanticViewDefinition("provider-yaml", "source: orders\n");

    private MockWebServer server;
    private RESTCatalog catalog;
    private SemanticViewManagement models;

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
        models = catalog.semanticViewManagement();

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
    void testLifecycleUsesSharedConfigurationAndReturnedCanonicalIdentity() throws Exception {
        String response =
                RESTApi.toJson(
                        new GetSemanticViewResponse(
                                "revenue", "opaque:metric/42", DEFINITION, "r2"));
        enqueue(200, response);
        SemanticView model = models.upsertSemanticView(ID, DEFINITION);
        Map<?, ?> body =
                RESTApi.fromJson(
                        takeRequest("POST", BASE_PATH + "/revenue").getBody().readUtf8(),
                        Map.class);
        assertThat(body.keySet()).extracting(Object::toString).containsExactly("definition");
        assertModel(model);

        enqueue(200, response);
        assertModel(models.upsertSemanticView(ID, DEFINITION, "r1"));
        assertThat(
                        RESTApi.fromJson(
                                takeRequest("POST", BASE_PATH + "/revenue").getBody().readUtf8(),
                                Map.class))
                .containsEntry("expectedRevision", "r1");

        enqueue(200, response);
        assertModel(models.getSemanticView(ID));
        takeRequest("GET", BASE_PATH + "/revenue");

        enqueue(200, "");
        catalog.labelManagement().upsertLabel("VIEW", model.getEntityName(), "domain", "sales");
        takeRequest("POST", "/v1/catalog%2Fid/labels/VIEW/opaque%3Ametric%2F42/domain");

        enqueue(200, "");
        models.deleteSemanticView(ID, model.getRevision());
        RecordedRequest conditional = takeRequest("DELETE", BASE_PATH + "/revenue");
        assertThat(conditional.getRequestUrl().queryParameter("expectedRevision")).isEqualTo("r2");
        assertThat(conditional.getBodySize()).isZero();
        enqueue(200, "");
        models.deleteSemanticView(ID);
        assertThat(takeRequest("DELETE", BASE_PATH + "/revenue").getRequestUrl().query()).isNull();
        assertThat(server.getRequestCount()).isEqualTo(7);
    }

    @Test
    void testPagedAndFullListingThroughManagement() throws Exception {
        enqueue(200, "{\"semanticViews\":[\"one\"],\"nextPageToken\":\"next +/%\"}");
        PagedList<String> page = models.listSemanticViewsPaged("sales", 1, "start +/%");
        assertThat(page.getElements()).containsExactly("one");
        assertThat(page.getNextPageToken()).isEqualTo("next +/%");
        RecordedRequest request = takeRequest("GET", BASE_PATH);
        assertThat(request.getRequestUrl().queryParameter("maxResults")).isEqualTo("1");
        assertThat(request.getRequestUrl().queryParameter("pageToken")).isEqualTo("start +/%");
        enqueue(200, "{\"semanticViews\":[\"one\"],\"nextPageToken\":\"next +/%\"}");
        enqueue(200, "{\"semanticViews\":[\"two\"]}");
        assertThat(models.listSemanticViews("sales")).containsExactly("one", "two");
        assertThat(takeRequest("GET", BASE_PATH).getRequestUrl().query()).isNull();
        assertThat(takeRequest("GET", BASE_PATH).getRequestUrl().queryParameter("pageToken"))
                .isEqualTo("next +/%");
        assertThat(server.getRequestCount()).isEqualTo(4);
    }

    private static void assertModel(SemanticView model) {
        assertThat(model.getName()).isEqualTo("revenue");
        assertThat(model.getEntityName()).isEqualTo("opaque:metric/42");
        assertThat(model.getDefinition()).isEqualTo(DEFINITION);
        assertThat(model.getRevision()).isEqualTo("r2");
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
