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

import org.apache.paimon.rest.requests.ConfigRequest;
import org.apache.paimon.rest.responses.ConfigResponse;

import okhttp3.Headers;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import retrofit2.Response;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/** REST catalog api test. */
public class RESTCatalogApiTest {
    private MockWebServer mockWebServer;
    private RESTCatalogApi apiService;
    private final String initToken = "init_token";

    @Before
    public void setUp() throws IOException {
        mockWebServer = new MockWebServer();
        mockWebServer.start();
        String baseUrl = mockWebServer.url("/").toString();
        apiService = (new HttpClient(baseUrl, initToken)).getClient();
    }

    @After
    public void tearDown() throws IOException {
        mockWebServer.shutdown();
    }

    @Test
    public void testGetConfig() throws IOException {
        String mockResponse = "{\"defaults\": {\"a\": \"b\"}}";
        MockResponse mockResponseObj =
                new MockResponse()
                        .setBody(mockResponse)
                        .addHeader("Content-Type", "application/json");
        mockWebServer.enqueue(mockResponseObj);
        ConfigRequest request = new ConfigRequest();
        Response<ConfigResponse> response = apiService.getConfig(request).execute();
        ConfigResponse data = response.body();
        Headers headers = response.headers();
        assertEquals("b", data.getDefaults().get("a"));
        assertEquals("Bearer " + initToken, headers.get("Authorization"));
    }

    @Test
    public void testNeedAuth() throws IOException {
        String mockResponse = "{\"defaults\": {\"a\": \"b\"}}";
        MockResponse mockResponseObj401 =
                new MockResponse()
                        .setBody(mockResponse)
                        .setResponseCode(401)
                        .addHeader("Content-Type", "application/json");
        MockResponse mockResponseObj200 =
                new MockResponse()
                        .setBody(mockResponse)
                        .addHeader("Content-Type", "application/json");
        mockWebServer.enqueue(mockResponseObj401);
        mockWebServer.enqueue(mockResponseObj200);
        ConfigRequest request = new ConfigRequest();
        Response<ConfigResponse> response = apiService.getConfig(request).execute();
        ConfigResponse data = response.body();
        Headers headers = response.headers();
        assertEquals("b", data.getDefaults().get("a"));
        assertNotEquals("Bearer " + initToken, headers.get("Authorization"));
    }
}
