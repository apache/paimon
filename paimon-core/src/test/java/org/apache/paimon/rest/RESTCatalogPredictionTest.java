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

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** REST catalog tests for durable asynchronous table predictions. */
class RESTCatalogPredictionTest {

    @Test
    void testPredictionLifecycleUsesSharedRestContract() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.start();
            Catalog catalog = catalog(server);
            Identifier table = Identifier.create("quality", "clips");

            server.enqueue(json(predictionJson("PENDING"), 202));
            Prediction submitted = catalog.submitPrediction(table, spec());
            assertThat(submitted.jobId()).isEqualTo("job-1");
            RecordedRequest submit = server.takeRequest(10, TimeUnit.SECONDS);
            assertThat(submit.getPath())
                    .isEqualTo("/v1/catalog/databases/quality/tables/clips/predictions");
            assertThat(RESTApi.fromJson(submit.getBody().readUtf8(), Map.class))
                    .containsEntry("requestId", "request-1");

            server.enqueue(
                    json(
                            "{\"predictions\":["
                                    + predictionJson("RUNNING")
                                    + "],\"nextPageToken\":\"next\"}",
                            200));
            PagedList<Prediction> page =
                    catalog.listPredictions(
                            table, "daily-quality", PredictionStatus.RUNNING, 10, "page-1");
            assertThat(page.getElements()).extracting(Prediction::jobId).containsExactly("job-1");
            assertThat(page.getNextPageToken()).isEqualTo("next");
            String listPath = server.takeRequest(10, TimeUnit.SECONDS).getPath();
            assertThat(listPath)
                    .contains("name=daily-quality")
                    .contains("status=RUNNING")
                    .contains("maxResults=10")
                    .contains("pageToken=page-1");

            server.enqueue(json(predictionJson("CANCELLED"), 200));
            assertThat(catalog.cancelPrediction(table, "job/1").status())
                    .isEqualTo(PredictionStatus.CANCELLED);
            assertThat(server.takeRequest(10, TimeUnit.SECONDS).getPath())
                    .isEqualTo(
                            "/v1/catalog/databases/quality/tables/clips/predictions/"
                                    + "job%2F1/cancel");
        }
    }

    @Test
    void testPredictionNotFoundPreservesResourceType() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.start();
            Catalog catalog = catalog(server);
            Identifier table = Identifier.create("quality", "clips");

            server.enqueue(notFound("MODEL", "qwen_vl"));
            assertThatThrownBy(() -> catalog.submitPrediction(table, spec()))
                    .isInstanceOf(Catalog.ModelNotExistException.class);

            server.enqueue(notFound("TABLE", "clips"));
            assertThatThrownBy(() -> catalog.getPrediction(table, "job-1"))
                    .isInstanceOf(Catalog.TableNotExistException.class);

            server.enqueue(notFound("PREDICTION", "job-1"));
            assertThatThrownBy(() -> catalog.getPrediction(table, "job-1"))
                    .isInstanceOf(Catalog.PredictionNotExistException.class);
        }
    }

    private static PredictionSpec spec() {
        return new PredictionSpec(
                "daily-quality",
                "request-1",
                "quality.qwen_vl",
                Collections.singletonMap("video", "content"),
                Collections.singletonList("clip_id"),
                "Classify",
                Collections.singletonMap("result", "STRING"),
                null,
                null,
                null,
                Collections.emptyMap());
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
        return "{\"jobId\":\"job-1\",\"name\":\"daily-quality\",\"status\":\""
                + status
                + "\",\"snapshotId\":7,\"model\":\"quality.qwen_vl\","
                + "\"modelVersion\":\"1\",\"outputTable\":\"system.results\","
                + "\"createdAt\":10}";
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
}
