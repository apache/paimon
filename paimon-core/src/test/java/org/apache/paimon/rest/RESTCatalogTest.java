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

import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/** Test for REST Catalog. */
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
        options.set(RESTCatalogOptions.URI, baseUrl);
        options.set(RESTCatalogOptions.TOKEN, initToken);
        options.set(RESTCatalogOptions.THREAD_POOL_SIZE, 1);
        mockOptions(RESTCatalogInternalOptions.PREFIX.key(), "prefix");
        restCatalog = new RESTCatalog(options);
    }

    @After
    public void tearDown() throws IOException {
        mockWebServer.shutdown();
    }

    @Test
    public void testInitFailWhenDefineWarehouse() {
        Options options = new Options();
        options.set(CatalogOptions.WAREHOUSE, "/a/b/c");
        assertThrows(IllegalArgumentException.class, () -> new RESTCatalog(options));
    }

    @Test
    public void testGetConfig() {
        String key = "a";
        String value = "b";
        mockOptions(key, value);
        Map<String, String> header = new HashMap<>();
        Map<String, String> response = restCatalog.fetchOptionsFromServer(header, new HashMap<>());
        assertEquals(value, response.get(key));
    }

    private void mockOptions(String key, String value) {
        String mockResponse = String.format("{\"defaults\": {\"%s\": \"%s\"}}", key, value);
        MockResponse mockResponseObj =
                new MockResponse()
                        .setBody(mockResponse)
                        .addHeader("Content-Type", "application/json");
        mockWebServer.enqueue(mockResponseObj);
    }
}
