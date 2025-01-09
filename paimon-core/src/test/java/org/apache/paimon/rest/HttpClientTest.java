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
import org.apache.paimon.rest.exceptions.BadRequestException;
import org.apache.paimon.rest.responses.ErrorResponse;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/** Test for {@link HttpClient}. */
public class HttpClientTest {

    private static final String MOCK_PATH = "/v1/api/mock";
    private static final String TOKEN = "token";
    private static final ObjectMapper OBJECT_MAPPER = RESTObjectMapper.create();

    private MockWebServer mockWebServer;
    private HttpClient httpClient;
    private ErrorHandler errorHandler;
    private MockRESTData mockResponseData;
    private String mockResponseDataStr;
    private ErrorResponse errorResponse;
    private String errorResponseStr;
    private Map<String, String> headers;

    @Before
    public void setUp() throws IOException {
        mockWebServer = new MockWebServer();
        mockWebServer.start();
        String baseUrl = mockWebServer.url("").toString();
        errorHandler = DefaultErrorHandler.getInstance();
        HttpClientOptions httpClientOptions =
                new HttpClientOptions(baseUrl, Duration.ofSeconds(3), Duration.ofSeconds(3), 1);
        mockResponseData = new MockRESTData(MOCK_PATH);
        mockResponseDataStr = OBJECT_MAPPER.writeValueAsString(mockResponseData);
        errorResponse = new ErrorResponse("test", "test", "test", 400);
        errorResponseStr = OBJECT_MAPPER.writeValueAsString(errorResponse);
        httpClient = new HttpClient(httpClientOptions);
        httpClient.setErrorHandler(errorHandler);
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
        assertEquals(mockResponseData.data(), response.data());
    }

    @Test
    public void testGetFail() {
        mockHttpCallWithCode(errorResponseStr, 400);
        assertThrows(
                BadRequestException.class,
                () -> httpClient.get(MOCK_PATH, MockRESTData.class, headers));
    }

    @Test
    public void testPostSuccess() {
        mockHttpCallWithCode(mockResponseDataStr, 200);
        MockRESTData response =
                httpClient.post(MOCK_PATH, mockResponseData, MockRESTData.class, headers);
        assertEquals(mockResponseData.data(), response.data());
    }

    @Test
    public void testPostFail() {
        mockHttpCallWithCode(errorResponseStr, 400);
        assertThrows(
                BadRequestException.class,
                () -> httpClient.post(MOCK_PATH, mockResponseData, ErrorResponse.class, headers));
    }

    @Test
    public void testDeleteSuccess() {
        mockHttpCallWithCode(mockResponseDataStr, 200);
        assertDoesNotThrow(() -> httpClient.delete(MOCK_PATH, headers));
    }

    @Test
    public void testDeleteFail() {
        mockHttpCallWithCode(errorResponseStr, 400);
        assertThrows(BadRequestException.class, () -> httpClient.delete(MOCK_PATH, headers));
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
