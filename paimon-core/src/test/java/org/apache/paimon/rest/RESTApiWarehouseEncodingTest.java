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

import org.apache.paimon.options.Options;
import org.apache.paimon.rest.responses.ConfigResponse;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;
import static org.junit.jupiter.api.Assertions.assertEquals;

class RESTApiWarehouseEncodingTest {

    private MockWebServer server;

    @AfterEach
    void tearDown() throws Exception {
        if (server != null) {
            server.shutdown();
        }
    }

    @Test
    void testWarehouseQueryParameterNotDoubleEncoded() throws Exception {
        // A warehouse path containing ':' and '/' exposes the encoding bug.
        String warehouse = "file:///tmp/paimon-warehouse";

        server = new MockWebServer();
        server.start();

        ConfigResponse config =
                new ConfigResponse(
                        ImmutableMap.of(
                                WAREHOUSE.key(),
                                warehouse,
                                RESTCatalogInternalOptions.PREFIX.key(),
                                "paimon"),
                        ImmutableMap.of());
        server.enqueue(
                new MockResponse()
                        .setResponseCode(200)
                        .setBody(RESTApi.toJson(config))
                        .addHeader("Content-Type", "application/json"));

        Options options = new Options();
        options.set(RESTCatalogOptions.URI, server.url("/").toString());
        options.set(WAREHOUSE, warehouse);
        options.set(RESTCatalogOptions.TOKEN, "token");
        options.set(RESTCatalogOptions.TOKEN_PROVIDER, "bear");

        // Triggers the /v1/config request inside the RESTApi constructor.
        new RESTApi(options);

        RecordedRequest request = server.takeRequest(10, TimeUnit.SECONDS);
        String receivedWarehouse = request.getRequestUrl().queryParameter(WAREHOUSE.key());

        // The query parameter is decoded once by the HTTP layer, so it must equal the original
        // value. Double encoding on the client side makes it differ.
        assertEquals(warehouse, receivedWarehouse, "warehouse query param was double-encoded");
    }
}
