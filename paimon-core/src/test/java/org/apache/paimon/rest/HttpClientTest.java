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

import org.apache.paimon.rest.auth.AuthProvider;
import org.apache.paimon.rest.auth.BearTokenAuthProvider;
import org.apache.paimon.rest.exceptions.BadRequestException;
import org.apache.paimon.rest.responses.ErrorResponse;
import org.apache.paimon.rest.responses.ErrorResponseResourceType;

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

    private TestHttpWebServer server;
    private HttpClient httpClient;
    private ErrorHandler errorHandler;
    private MockRESTData mockResponseData;
    private String mockResponseDataStr;
    private String errorResponseStr;
    private Map<String, String> headers;

    @Before
    public void setUp() throws Exception {
        server = new TestHttpWebServer(MOCK_PATH);
        server.start();
        errorHandler = DefaultErrorHandler.getInstance();
        HttpClientOptions httpClientOptions =
                new HttpClientOptions(server.getBaseUrl(), Duration.ofSeconds(3), 1, 10, 2);
        mockResponseData = new MockRESTData(MOCK_PATH);
        mockResponseDataStr = server.createResponseBody(mockResponseData);
        errorResponseStr =
                server.createResponseBody(
                        new ErrorResponse(ErrorResponseResourceType.DATABASE, "test", "test", 400));
        httpClient = new HttpClient(httpClientOptions);
        httpClient.setErrorHandler(errorHandler);
        AuthProvider authProvider = new BearTokenAuthProvider(TOKEN);
        headers = authProvider.authHeader();
    }

    @After
    public void tearDown() throws IOException {
        server.stop();
    }

    @Test
    public void testGetSuccess() {
        server.enqueueResponse(mockResponseDataStr, 200);
        MockRESTData response = httpClient.get(MOCK_PATH, MockRESTData.class, headers);
        assertEquals(mockResponseData.data(), response.data());
    }

    @Test
    public void testGetFail() {
        server.enqueueResponse(errorResponseStr, 400);
        assertThrows(
                BadRequestException.class,
                () -> httpClient.get(MOCK_PATH, MockRESTData.class, headers));
    }

    @Test
    public void testPostSuccess() {
        server.enqueueResponse(mockResponseDataStr, 200);
        MockRESTData response =
                httpClient.post(MOCK_PATH, mockResponseData, MockRESTData.class, headers);
        assertEquals(mockResponseData.data(), response.data());
    }

    @Test
    public void testPostFail() {
        server.enqueueResponse(errorResponseStr, 400);
        assertThrows(
                BadRequestException.class,
                () -> httpClient.post(MOCK_PATH, mockResponseData, ErrorResponse.class, headers));
    }

    @Test
    public void testDeleteSuccess() {
        server.enqueueResponse(mockResponseDataStr, 200);
        assertDoesNotThrow(() -> httpClient.delete(MOCK_PATH, headers));
    }

    @Test
    public void testDeleteFail() {
        server.enqueueResponse(errorResponseStr, 400);
        assertThrows(BadRequestException.class, () -> httpClient.delete(MOCK_PATH, headers));
    }

    @Test
    public void testRetry() {
        HttpClient httpClient =
                new HttpClient(
                        new HttpClientOptions(
                                server.getBaseUrl(), Duration.ofSeconds(30), 1, 10, 2));
        server.enqueueResponse(mockResponseDataStr, 429);
        server.enqueueResponse(mockResponseDataStr, 200);
        assertDoesNotThrow(() -> httpClient.get(MOCK_PATH, MockRESTData.class, headers));
    }
}
