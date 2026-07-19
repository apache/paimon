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
import org.apache.paimon.rest.auth.RESTAuthFunction;
import org.apache.paimon.rest.auth.RESTAuthParameter;
import org.apache.paimon.rest.exceptions.BadRequestException;
import org.apache.paimon.rest.exceptions.RESTException;
import org.apache.paimon.rest.responses.ErrorResponse;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import okhttp3.mockwebserver.RecordedRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
    private RESTAuthFunction restAuthFunction;

    @Before
    public void setUp() throws Exception {
        server = new TestHttpWebServer(MOCK_PATH);
        server.start();
        errorHandler = DefaultErrorHandler.getInstance();
        mockResponseData = new MockRESTData(MOCK_PATH);
        mockResponseDataStr = server.createResponseBody(mockResponseData);
        errorResponseStr =
                server.createResponseBody(
                        new ErrorResponse(
                                ErrorResponse.RESOURCE_TYPE_DATABASE, "test", "test", 400));
        httpClient = new HttpClient(server.getBaseUrl());
        httpClient.setErrorHandler(errorHandler);
        AuthProvider authProvider = new BearTokenAuthProvider(TOKEN);
        headers = new HashMap<>();
        restAuthFunction = new RESTAuthFunction(headers, authProvider);
    }

    @After
    public void tearDown() throws IOException {
        server.stop();
    }

    @Test
    public void testServerUriSchema() {
        assertEquals("http://localhost", (new HttpClient("localhost")).uri());
        assertEquals("http://localhost", (new HttpClient("http://localhost")).uri());
        assertEquals("https://localhost", (new HttpClient("https://localhost")).uri());
    }

    @Test
    public void testGetSuccess() {
        server.enqueueResponse(mockResponseDataStr, 200);
        MockRESTData response = httpClient.get(MOCK_PATH, MockRESTData.class, restAuthFunction);
        assertEquals(mockResponseData.data(), response.data());
    }

    @Test
    public void testGetSuccessWithQueryParams() {
        server.enqueueResponse(mockResponseDataStr, 200);
        Map<String, String> queryParams =
                ImmutableMap.of("maxResults", "10", "pageToken", "abc=123");
        MockRESTData response =
                httpClient.get(MOCK_PATH, queryParams, MockRESTData.class, restAuthFunction);
        assertEquals(mockResponseData.data(), response.data());

        server.enqueueResponse(mockResponseDataStr, 200);
        queryParams = ImmutableMap.of("pageToken", "abc");
        response = httpClient.get(MOCK_PATH, queryParams, MockRESTData.class, restAuthFunction);
        assertEquals(mockResponseData.data(), response.data());

        server.enqueueResponse(mockResponseDataStr, 200);
        queryParams = ImmutableMap.of("maxResults", "10");
        response = httpClient.get(MOCK_PATH, queryParams, MockRESTData.class, restAuthFunction);
        assertEquals(mockResponseData.data(), response.data());

        server.enqueueResponse(mockResponseDataStr, 200);
        queryParams = new HashMap<>();
        response = httpClient.get(MOCK_PATH, queryParams, MockRESTData.class, restAuthFunction);
        assertEquals(mockResponseData.data(), response.data());
    }

    @Test
    public void testGetFail() {
        server.enqueueResponse(errorResponseStr, 400);
        assertThrows(
                BadRequestException.class,
                () -> httpClient.get(MOCK_PATH, MockRESTData.class, restAuthFunction));
    }

    @Test
    public void testErrorResponseMessageIsRedacted() throws Exception {
        // A parsed ErrorResponse whose message carries a secret must not leak it.
        String secret = "SUPERSECRETVALUE9999";
        String body =
                server.createResponseBody(
                        new ErrorResponse(
                                ErrorResponse.RESOURCE_TYPE_DATABASE,
                                "test",
                                "token=" + secret,
                                400));
        server.enqueueResponse(body, 400);
        BadRequestException e =
                assertThrows(
                        BadRequestException.class,
                        () -> httpClient.get(MOCK_PATH, MockRESTData.class, restAuthFunction));
        assertFalse(e.getMessage().contains(secret));
    }

    @Test
    public void testUnparseableErrorBodyIsNotEchoed() {
        // A non-JSON error body cannot be reliably sanitized, so it must not be echoed at all.
        String secret = "SUPERSECRETVALUE9999";
        server.enqueueResponse("token=" + secret + " <<not json>>", 400);
        BadRequestException e =
                assertThrows(
                        BadRequestException.class,
                        () -> httpClient.get(MOCK_PATH, MockRESTData.class, restAuthFunction));
        assertFalse(e.getMessage().contains(secret));
    }

    @Test
    public void testSuccessResponseParseFailureDoesNotLeakBody() {
        // A 2xx body that fails to deserialize (e.g. a token response) must not surface the
        // raw body or Jackson's source snippet.
        String secret = "SUPERSECRETVALUE9999";
        String body = "{\"data\": \"token=" + secret + "\" INVALID_JSON}";
        server.enqueueResponse(body, 200);
        RESTException e =
                assertThrows(
                        RESTException.class,
                        () -> httpClient.get(MOCK_PATH, MockRESTData.class, restAuthFunction));
        assertFalse(e.getMessage().contains(secret));
    }

    @Test
    public void testPostSuccess() {
        server.enqueueResponse(mockResponseDataStr, 200);
        MockRESTData response =
                httpClient.post(MOCK_PATH, mockResponseData, MockRESTData.class, restAuthFunction);
        assertEquals(mockResponseData.data(), response.data());
    }

    @Test
    public void testPostFail() {
        server.enqueueResponse(errorResponseStr, 400);
        assertThrows(
                BadRequestException.class,
                () ->
                        httpClient.post(
                                MOCK_PATH,
                                mockResponseData,
                                ErrorResponse.class,
                                restAuthFunction));
    }

    @Test
    public void testDeleteSuccess() {
        server.enqueueResponse(mockResponseDataStr, 200);
        assertDoesNotThrow(() -> httpClient.delete(MOCK_PATH, restAuthFunction));
    }

    @Test
    public void testDeleteFail() {
        server.enqueueResponse(errorResponseStr, 400);
        assertThrows(
                BadRequestException.class, () -> httpClient.delete(MOCK_PATH, restAuthFunction));
    }

    @Test
    public void testDeleteWithDataSuccess() {
        server.enqueueResponse(mockResponseDataStr, 200);
        Assertions.assertDoesNotThrow(
                () -> httpClient.delete(MOCK_PATH, mockResponseData, restAuthFunction));
    }

    @Test
    public void testDeleteWithDataFail() {
        server.enqueueResponse(errorResponseStr, 400);
        assertThrows(
                BadRequestException.class,
                () -> httpClient.delete(MOCK_PATH, mockResponseData, restAuthFunction));
    }

    @Test
    public void testRetry() {
        HttpClient httpClient = new HttpClient(server.getBaseUrl());
        server.enqueueResponse(mockResponseDataStr, 429);
        server.enqueueResponse(mockResponseDataStr, 200);
        Assertions.assertDoesNotThrow(
                () -> httpClient.get(MOCK_PATH, MockRESTData.class, restAuthFunction));
    }

    @Test
    public void testUrl() {
        String queryKey = "pageToken";
        RESTAuthParameter restAuthParameter =
                new RESTAuthParameter(
                        "/api/v1/tables/my_table$schemas",
                        ImmutableMap.of(queryKey, "dt=20230101"),
                        "GET",
                        "");
        String url =
                (new HttpClient("http://a.b.c:8080"))
                        .getRequestUrl(
                                "/api/v1/tables/my_table$schemas",
                                ImmutableMap.of("pageToken", "dt=20230101"));
        assertEquals(
                "http://a.b.c:8080/api/v1/tables/my_table$schemas?pageToken=dt%3D20230101", url);
        Map<String, String> queryParameters = getParameters(url);
        assertEquals(restAuthParameter.parameters().get(queryKey), queryParameters.get(queryKey));
    }

    private Map<String, String> getParameters(String path) {
        String[] paths = path.split("\\?");
        if (paths.length == 1) {
            return Collections.emptyMap();
        }
        String query = paths[1];
        Map<String, String> parameters =
                Arrays.stream(query.split("&"))
                        .map(pair -> pair.split("=", 2))
                        .collect(
                                Collectors.toMap(
                                        pair -> pair[0].trim(), // key
                                        pair -> pair[1].trim(), // value
                                        (existing, replacement) -> existing // handle duplicates
                                        ));
        return parameters;
    }

    @Test
    public void testGetWithUnparsableJsonErrorResponse() {
        // A JSON body that parses to an ErrorResponse with no message must NOT be echoed (it may
        // carry secrets); it is reported as an empty message, not "unparseable".
        String jsonWithUppercaseFields =
                "{\"Message\":\"Your request is denied as lack of ssl protect.\","
                        + "\"Code\":\"InvalidProtocol.NeedSsl\"}";
        server.enqueueResponse(jsonWithUppercaseFields, 403);

        try {
            httpClient.get(MOCK_PATH, MockRESTData.class, restAuthFunction);
            Assertions.fail("Expected exception to be thrown");
        } catch (Exception e) {
            Assertions.assertFalse(
                    e.getMessage().contains("Your request is denied as lack of ssl protect")
                            || e.getMessage().contains(jsonWithUppercaseFields),
                    "Raw response body must not be echoed");
            Assertions.assertTrue(
                    e.getMessage().contains("Empty error message"),
                    "Parsed-but-empty message must not be labelled unparseable");
        }
    }

    @Test
    public void testPostWithNonJsonErrorResponse() {
        // A non-JSON (plain text) error body must NOT be echoed; only a generic message remains.
        String plainTextResponse = "Internal Server Error: Database connection failed";
        server.enqueueResponse(plainTextResponse, 500);

        try {
            httpClient.post(MOCK_PATH, mockResponseData, MockRESTData.class, restAuthFunction);
            Assertions.fail("Expected exception to be thrown");
        } catch (Exception e) {
            Assertions.assertFalse(
                    e.getMessage().contains(plainTextResponse)
                            || e.getMessage().contains("Database connection failed"),
                    "Raw response body must not be echoed");
            Assertions.assertTrue(
                    e.getMessage().contains("500"), "Message should carry the HTTP status");
        }
    }

    @Test
    public void testGetWithMalformedUrlDoesNotLeakCredentials() {
        // Malformed URL throws in the constructor before exec(); the raw URL must not leak.
        String secret = "QUERY_SECRET";
        IllegalArgumentException e =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                httpClient.get(
                                        "/x?sig=" + secret + " bad",
                                        MockRESTData.class,
                                        restAuthFunction));
        assertFalse(e.getMessage().contains(secret));
    }

    @Test
    public void testPostWithMalformedUrlDoesNotLeakCredentials() {
        String secret = "QUERY_SECRET";
        IllegalArgumentException e =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                httpClient.post(
                                        "/x?sig=" + secret + " bad",
                                        mockResponseData,
                                        MockRESTData.class,
                                        restAuthFunction));
        assertFalse(e.getMessage().contains(secret));
    }

    @Test
    public void testPostSetsJsonContentType() throws Exception {
        server.enqueueResponse(mockResponseDataStr, 200);
        httpClient.post(MOCK_PATH, mockResponseData, MockRESTData.class, restAuthFunction);
        RecordedRequest request = server.takeRequest(10, TimeUnit.SECONDS);
        String contentType = request.getHeader("Content-Type");
        Assertions.assertNotNull(contentType, "POST request must carry a Content-Type header");
        Assertions.assertTrue(
                contentType.contains("application/json"),
                "POST body must be sent as application/json, but was: " + contentType);
    }
}
