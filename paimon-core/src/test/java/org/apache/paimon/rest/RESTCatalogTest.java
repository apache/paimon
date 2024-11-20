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

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/** REST catalog api test. */
public class RESTCatalogTest {
    private MockWebServer mockWebServer;
    private RESTCatalog restCatalog;
    private final String initToken = "init_token";

    @Before
    public void setUp() throws IOException {
        mockWebServer = new MockWebServer();
        mockWebServer.start();
        String baseUrl = mockWebServer.url("").toString();
        Options options = new Options();
        options.set(RESTCatalogOptions.ENDPOINT, baseUrl);
        options.set(RESTCatalogOptions.TOKEN, initToken);
        options.set(RESTCatalogOptions.THREAD_POOL_SIZE, 1);
        restCatalog = new RESTCatalog(options);
    }

    @After
    public void tearDown() throws IOException {
        mockWebServer.shutdown();
    }

    @Test
    public void testGetConfig() {
        String mockResponse = "{\"defaults\": {\"a\": \"b\"}}";
        MockResponse mockResponseObj =
                new MockResponse()
                        .setBody(mockResponse)
                        .addHeader("Content-Type", "application/json");
        mockWebServer.enqueue(mockResponseObj);
        Map<String, String> response = restCatalog.options();
        assertEquals("b", response.get("a"));
    }
}
