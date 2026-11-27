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
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.Prediction;
import org.apache.paimon.catalog.PredictionSpec;
import org.apache.paimon.catalog.PredictionStatus;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.exceptions.NoSuchResourceException;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** REST catalog tests for durable asynchronous predictions. */
class RESTCatalogPredictionTest {

    @Test
    void testPredictionLifecycleUsesSharedRestContract() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.start();
            Catalog catalog = catalog(server);
            Identifier table = Identifier.create("quality", "clips");

            server.enqueue(json(predictionJson("PENDING"), 202));
            Prediction submitted = catalog.submitPrediction(table, spec());
            assertThat(submitted.predictionId()).isEqualTo("prediction-1");
            RecordedRequest submit = server.takeRequest(10, TimeUnit.SECONDS);
            assertThat(submit.getPath()).isEqualTo("/v1/catalog/predictions");
            Map<String, Object> request = RESTApi.fromJson(submit.getBody().readUtf8(), Map.class);
            assertThat(request.get("idempotencyToken")).isInstanceOf(String.class);
            assertThat((String) request.get("idempotencyToken")).isNotBlank();
            assertThat(request)
                    .doesNotContainKeys("resultColumns", "mergeKeyColumns", "outputSchema");
            assertThat((Map<String, Object>) request.get("source"))
                    .containsEntry("database", "quality")
                    .containsEntry("table", "clips");

            server.enqueue(
                    json(
                            "{\"predictions\":["
                                    + predictionJson("RUNNING")
                                    + "],\"nextPageToken\":\"next\"}",
                            200));
            PagedList<Prediction> page =
                    catalog.listPredictions(
                            table, "daily-quality", PredictionStatus.State.RUNNING, 10, "page-1");
            assertThat(page.getElements())
                    .extracting(Prediction::predictionId)
                    .containsExactly("prediction-1");
            assertThat(page.getNextPageToken()).isEqualTo("next");
            String listPath = server.takeRequest(10, TimeUnit.SECONDS).getPath();
            assertThat(listPath)
                    .contains("database=quality")
                    .contains("table=clips")
                    .contains("name=daily-quality")
                    .contains("state=RUNNING")
                    .contains("maxResults=10")
                    .contains("pageToken=page-1");

            server.enqueue(json(terminatedPredictionJson("CANCELED"), 200));
            PredictionStatus cancelled = catalog.cancelPrediction("prediction/1").status();
            assertThat(cancelled.state()).isEqualTo(PredictionStatus.State.TERMINATED);
            assertThat(cancelled.terminationDetails().code())
                    .isEqualTo(PredictionStatus.TerminationCode.CANCELED);
            assertThat(server.takeRequest(10, TimeUnit.SECONDS).getPath())
                    .isEqualTo("/v1/catalog/predictions/prediction%2F1/cancel");
        }
    }

    @Test
    void testPredictionNotFoundPreservesResourceType() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.start();
            Catalog catalog = catalog(server);
            Identifier table = Identifier.create("quality", "clips");

            server.enqueue(notFound("DATABASE", "models"));
            assertThatThrownBy(() -> catalog.submitPrediction(table, spec()))
                    .isInstanceOf(Catalog.DatabaseNotExistException.class)
                    .hasMessage("Database models does not exist.");

            server.enqueue(notFound("MODEL", "qwen_vl"));
            assertThatThrownBy(() -> catalog.submitPrediction(table, spec()))
                    .isInstanceOf(Catalog.ModelNotExistException.class);

            server.enqueue(notFound("TABLE", "clips"));
            assertThatThrownBy(() -> catalog.submitPrediction(table, spec()))
                    .isInstanceOf(Catalog.TableNotExistException.class);

            server.enqueue(untypedNotFound());
            assertThatThrownBy(() -> catalog.submitPrediction(table, spec()))
                    .isInstanceOf(NoSuchResourceException.class);

            server.enqueue(notFound("PREDICTION", "prediction-1"));
            assertThatThrownBy(() -> catalog.getPrediction("prediction-1"))
                    .isInstanceOf(Catalog.PredictionNotExistException.class);

            server.enqueue(untypedNotFound());
            assertThatThrownBy(() -> catalog.getPrediction("prediction-1"))
                    .isInstanceOf(NoSuchResourceException.class);

            server.enqueue(untypedNotFound());
            assertThatThrownBy(() -> catalog.cancelPrediction("prediction-1"))
                    .isInstanceOf(NoSuchResourceException.class);
        }
    }

    @Test
    void testPredictionRejectsBranchAndSystemTable() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.start();
            Catalog catalog = catalog(server);

            assertThatThrownBy(
                            () ->
                                    catalog.submitPrediction(
                                            new Identifier("quality", "clips", "dev"), spec()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("branch");
            assertThatThrownBy(
                            () ->
                                    catalog.submitPrediction(
                                            new Identifier("quality", "clips", null, "snapshots"),
                                            spec()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("system table");
        }
    }

    private static PredictionSpec spec() {
        return new PredictionSpec(
                "daily-quality",
                Identifier.create("quality", "qwen_vl"),
                Collections.singletonMap("video", "content"),
                null,
                "Classify",
                null,
                Collections.emptyMap(),
                null);
    }

    private static Catalog catalog(MockWebServer server) {
        Options options = new Options();
        options.set(RESTCatalogOptions.URI, server.url("/").toString());
        options.set(RESTCatalogOptions.TOKEN_PROVIDER, "bear");
        options.set(RESTCatalogOptions.TOKEN, "token");
        options.set(RESTCatalogInternalOptions.PREFIX, "catalog");
        return new RESTCatalog(CatalogContext.create(options), false);
    }

    private static String predictionJson(String status) {
        return "{\"predictionId\":\"prediction-1\",\"source\":{\"database\":\"quality\","
                + "\"table\":\"clips\"},\"name\":\"daily-quality\",\"status\":{\"state\":\""
                + status
                + "\"},\"snapshotId\":7,\"model\":{\"database\":\"quality\","
                + "\"object\":\"qwen_vl\"}}";
    }

    private static String terminatedPredictionJson(String code) {
        return "{\"predictionId\":\"prediction-1\",\"source\":{\"database\":\"quality\","
                + "\"table\":\"clips\"},\"name\":\"daily-quality\","
                + "\"status\":{\"state\":\"TERMINATED\",\"terminationDetails\":{\"code\":\""
                + code
                + "\"}},\"snapshotId\":7,\"model\":{\"database\":\"quality\","
                + "\"object\":\"qwen_vl\"},"
                + "\"output\":{\"table\":{"
                + "\"database\":\"system\",\"table\":\"results\"}},"
                + "\"startTime\":10,\"endTime\":20}";
    }

    private static MockResponse json(String body, int code) {
        return new MockResponse()
                .setResponseCode(code)
                .setHeader("Content-Type", "application/json")
                .setBody(body);
    }

    private static MockResponse notFound(String resourceType, String resourceName) {
        return json(
                "{\"resourceType\":\""
                        + resourceType
                        + "\",\"resourceName\":\""
                        + resourceName
                        + "\",\"message\":\"not found\",\"code\":404}",
                404);
    }

    private static MockResponse untypedNotFound() {
        return json("{\"message\":\"route not found\",\"code\":404}", 404);
    }
}
