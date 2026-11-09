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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.Model;
import org.apache.paimon.catalog.ModelField;
import org.apache.paimon.catalog.ProviderModelDefinition;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.exceptions.NoSuchResourceException;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** REST catalog tests for database-scoped model metadata. */
class RESTCatalogModelTest {

    @Test
    void testModelCrudUsesTheSharedRestContract() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.start();
            Catalog catalog = catalog(server);

            Identifier identifier = Identifier.create("quality", "qwen_vl");
            Model metadata = new Model(identifier, "Video quality model", definition());

            server.enqueue(json(modelJson()));
            Model created = catalog.createModel(identifier, metadata, false);
            assertThat(created.fullName()).isEqualTo("quality.qwen_vl");
            RecordedRequest create = server.takeRequest(10, TimeUnit.SECONDS);
            assertThat(create.getMethod()).isEqualTo("POST");
            assertThat(create.getPath()).isEqualTo("/v1/catalog/databases/quality/models");
            Map<String, Object> body = RESTApi.fromJson(create.getBody().readUtf8(), Map.class);
            assertThat(body).containsOnlyKeys("name", "comment", "definition");
            Map<String, Object> definition = (Map<String, Object>) body.get("definition");
            assertThat(definition).containsEntry("type", "provider").doesNotContainKey("outputs");

            server.enqueue(json("{\"models\":[]}"));
            assertThat(catalog.listModels("quality")).isEmpty();
            assertThat(server.takeRequest(10, TimeUnit.SECONDS).getMethod()).isEqualTo("GET");

            server.enqueue(json(""));
            catalog.dropModel(identifier, false);
            RecordedRequest drop = server.takeRequest(10, TimeUnit.SECONDS);
            assertThat(drop.getMethod()).isEqualTo("DELETE");
            assertThat(drop.getPath()).isEqualTo("/v1/catalog/databases/quality/models/qwen_vl");
        }
    }

    @Test
    void testDatabaseAndModelNotFoundAreDistinct() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.start();
            Catalog catalog = catalog(server);
            Identifier identifier = Identifier.create("quality", "qwen_vl");

            server.enqueue(notFound("DATABASE", "quality"));
            assertThatThrownBy(() -> catalog.getModel(identifier))
                    .isInstanceOf(Catalog.DatabaseNotExistException.class);

            server.enqueue(notFound("MODEL", "qwen_vl"));
            assertThatThrownBy(() -> catalog.getModel(identifier))
                    .isInstanceOf(Catalog.ModelNotExistException.class);

            server.enqueue(notFound("DATABASE", "quality"));
            assertThatThrownBy(() -> catalog.dropModel(identifier, true))
                    .isInstanceOf(Catalog.DatabaseNotExistException.class);

            server.enqueue(notFound("MODEL", "qwen_vl"));
            catalog.dropModel(identifier, true);

            server.enqueue(new MockResponse().setResponseCode(404).setBody("Not Found"));
            assertThatThrownBy(() -> catalog.dropModel(identifier, true))
                    .isInstanceOf(NoSuchResourceException.class);
        }
    }

    @Test
    void testModelPathsRejectDotSegmentsBeforeSending() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.start();
            Catalog catalog = catalog(server);

            for (String segment : new String[] {".", ".."}) {
                Identifier invalidModel = Identifier.create("quality", segment);
                Model metadata = new Model(invalidModel, null, definition());
                assertThatThrownBy(() -> catalog.createModel(invalidModel, metadata, false))
                        .isInstanceOf(IllegalArgumentException.class);
                assertThatThrownBy(() -> catalog.getModel(invalidModel))
                        .isInstanceOf(IllegalArgumentException.class);
                assertThatThrownBy(() -> catalog.dropModel(invalidModel, false))
                        .isInstanceOf(IllegalArgumentException.class);
                assertThatThrownBy(() -> catalog.listModels(segment))
                        .isInstanceOf(IllegalArgumentException.class);

                Identifier invalidDatabase = Identifier.create(segment, "qwen_vl");
                assertThatThrownBy(() -> catalog.getModel(invalidDatabase))
                        .isInstanceOf(IllegalArgumentException.class);
            }

            assertThat(server.getRequestCount()).isZero();
        }
    }

    @Test
    void testListModelsFollowsCursorAfterEmptyPage() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.start();
            Catalog catalog = catalog(server);
            server.enqueue(json("{\"models\":[],\"nextPageToken\":\"second\"}"));
            server.enqueue(json("{\"models\":[" + modelJson() + "]}"));

            assertThat(catalog.listModels("quality"))
                    .extracting(Model::name)
                    .containsExactly("qwen_vl");
            assertThat(server.takeRequest(10, TimeUnit.SECONDS).getPath())
                    .isEqualTo("/v1/catalog/databases/quality/models");
            assertThat(server.takeRequest(10, TimeUnit.SECONDS).getPath())
                    .isEqualTo("/v1/catalog/databases/quality/models?pageToken=second");
        }
    }

    private static Catalog catalog(MockWebServer server) {
        Options options = new Options();
        options.set(RESTCatalogOptions.URI, server.url("/").toString());
        options.set(RESTCatalogOptions.TOKEN_PROVIDER, "bear");
        options.set(RESTCatalogOptions.TOKEN, "token");
        options.set(RESTCatalogInternalOptions.PREFIX, "catalog");
        return new RESTCatalog(CatalogContext.create(options), false);
    }

    private static ProviderModelDefinition definition() {
        List<ModelField> inputs =
                Collections.singletonList(new ModelField("video", "BLOB", "video"));
        return new ProviderModelDefinition(
                "bailian", "qwen3.8-flash", inputs, null, Collections.emptyMap());
    }

    private static String modelJson() {
        return "{\"name\":\"qwen_vl\",\"comment\":\"Video quality model\","
                + "\"definition\":{\"type\":\"provider\",\"provider\":\"bailian\","
                + "\"providerModel\":\"qwen3.8-flash\",\"inputs\":[{\"name\":\"video\","
                + "\"dataType\":\"BLOB\",\"modality\":\"video\"}],\"options\":{}}}";
    }

    private static MockResponse json(String body) {
        return new MockResponse()
                .setResponseCode(200)
                .setHeader("Content-Type", "application/json")
                .setBody(body);
    }

    private static MockResponse notFound(String resourceType, String resourceName) {
        return json("{\"resourceType\":\""
                        + resourceType
                        + "\",\"resourceName\":\""
                        + resourceName
                        + "\",\"message\":\"not found\",\"code\":404}")
                .setResponseCode(404);
    }
}
