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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.After;
import org.junit.Before;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Test for {@link HttpClient}. */
public class HttpClientTest {
    private MockWebServer mockWebServer;
    private HttpClient httpClient;
    private ObjectMapper objectMapper = RESTObjectMapper.create();
    private ErrorHandler errorHandler;

    @Before
    public void setUp() throws IOException {
        mockWebServer = new MockWebServer();
        mockWebServer.start();
        String baseUrl = mockWebServer.url("").toString();
        errorHandler = mock(ErrorHandler.class);
        HttpClientBuildParameter httpClientBuildParameter =
                new HttpClientBuildParameter(baseUrl, 1000, 1000, objectMapper, 1, errorHandler);
        httpClient = new HttpClient(httpClientBuildParameter);
    }

    @After
    public void tearDown() throws IOException {
        mockWebServer.shutdown();
    }

    @Test
    public void testPostSuccess() throws Exception {
        MockRESTData mockResponseData = new MockRESTData("test");
        String mockResponse = objectMapper.writeValueAsString(mockResponseData);
        MockResponse mockResponseObj =
                new MockResponse()
                        .setBody(mockResponse)
                        .addHeader("Content-Type", "application/json");
        mockWebServer.enqueue(mockResponseObj);
        MockRESTData response =
                httpClient.post("/test", mockResponseData, MockRESTData.class, headers("token"));
        verify(errorHandler, times(0)).accept(any());
        assertEquals(mockResponseData.data(), response.data());
    }

    private Map<String, String> headers(String token) {
        // todo: need refresh token
        Map<String, String> header = new HashMap<>();
        header.put("Authorization", "Bearer " + token);
        return header;
    }
}
