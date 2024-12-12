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

import org.apache.paimon.rest.auth.BearTokenCredentialsProvider;
import org.apache.paimon.rest.auth.CredentialsProvider;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

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
    private MockRESTData mockResponseData;
    private String mockResponseDataStr;
    private Map<String, String> headers;
    private static final String MOCK_PATH = "/v1/api/mock";
    private static final String TOKEN = "token";

    @Before
    public void setUp() throws IOException {
        mockWebServer = new MockWebServer();
        mockWebServer.start();
        String baseUrl = mockWebServer.url("").toString();
        errorHandler = mock(ErrorHandler.class);
        HttpClientOptions httpClientOptions =
                new HttpClientOptions(
                        baseUrl,
                        Optional.of(Duration.ofSeconds(3)),
                        Optional.of(Duration.ofSeconds(3)),
                        objectMapper,
                        1,
                        errorHandler);
        mockResponseData = new MockRESTData(MOCK_PATH);
        mockResponseDataStr = objectMapper.writeValueAsString(mockResponseData);
        httpClient = new HttpClient(httpClientOptions);
        CredentialsProvider credentialsProvider = new BearTokenCredentialsProvider(TOKEN);
        headers = credentialsProvider.authHeader();
    }

    @After
    public void tearDown() throws IOException {
        mockWebServer.shutdown();
    }

    @Test
    public void testGetSuccess() {
        mockHttpCallWithCode(mockResponseDataStr, 200);
        MockRESTData response = httpClient.get(MOCK_PATH, MockRESTData.class, headers);
        verify(errorHandler, times(0)).accept(any());
        assertEquals(mockResponseData.data(), response.data());
    }

    @Test
    public void testGetFail() {
        mockHttpCallWithCode(mockResponseDataStr, 400);
        httpClient.get(MOCK_PATH, MockRESTData.class, headers);
        verify(errorHandler, times(1)).accept(any());
    }

    @Test
    public void testPostSuccess() {
        mockHttpCallWithCode(mockResponseDataStr, 200);
        MockRESTData response =
                httpClient.post(MOCK_PATH, mockResponseData, MockRESTData.class, headers);
        verify(errorHandler, times(0)).accept(any());
        assertEquals(mockResponseData.data(), response.data());
    }

    @Test
    public void testPostFail() {
        mockHttpCallWithCode(mockResponseDataStr, 400);
        httpClient.post(MOCK_PATH, mockResponseData, MockRESTData.class, headers);
        verify(errorHandler, times(1)).accept(any());
    }

    @Test
    public void testDeleteSuccess() {
        mockHttpCallWithCode(mockResponseDataStr, 200);
        MockRESTData response = httpClient.delete(MOCK_PATH, headers);
        verify(errorHandler, times(0)).accept(any());
    }

    @Test
    public void testDeleteFail() {
        mockHttpCallWithCode(mockResponseDataStr, 400);
        httpClient.delete(MOCK_PATH, headers);
        verify(errorHandler, times(1)).accept(any());
    }

    private Map<String, String> headers(String token) {
        Map<String, String> header = new HashMap<>();
        header.put("Authorization", "Bearer " + token);
        return header;
    }

    private void mockHttpCallWithCode(String body, Integer code) {
        MockResponse mockResponseObj = generateMockResponse(body, code);
        mockWebServer.enqueue(mockResponseObj);
    }

    private MockResponse generateMockResponse(String data, Integer code) {
        return new MockResponse()
                .setResponseCode(code)
                .setBody(data)
                .addHeader("Content-Type", "application/json");
    }
}
