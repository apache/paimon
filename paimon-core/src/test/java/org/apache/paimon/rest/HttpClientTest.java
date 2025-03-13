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
import org.apache.paimon.rest.auth.AuthSession;
import org.apache.paimon.rest.auth.BearTokenAuthProvider;
import org.apache.paimon.rest.auth.RESTAuthFunction;
import org.apache.paimon.rest.auth.RESTAuthParameter;
import org.apache.paimon.rest.exceptions.BadRequestException;
import org.apache.paimon.rest.responses.ErrorResponse;
import org.apache.paimon.rest.responses.ErrorResponseResourceType;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import okhttp3.Headers;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static okhttp3.ConnectionSpec.CLEARTEXT;
import static okhttp3.ConnectionSpec.COMPATIBLE_TLS;
import static okhttp3.ConnectionSpec.MODERN_TLS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

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
                        new ErrorResponse(ErrorResponseResourceType.DATABASE, "test", "test", 400));
        httpClient = new HttpClient(server.getBaseUrl());
        httpClient.setErrorHandler(errorHandler);
        AuthProvider authProvider = new BearTokenAuthProvider(TOKEN);
        AuthSession authSession = new AuthSession(authProvider);
        headers = new HashMap<>();
        restAuthFunction = new RESTAuthFunction(headers, authSession);
    }

    @After
    public void tearDown() throws IOException {
        server.stop();
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
        assertDoesNotThrow(() -> httpClient.delete(MOCK_PATH, mockResponseData, restAuthFunction));
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
        assertDoesNotThrow(() -> httpClient.get(MOCK_PATH, MockRESTData.class, restAuthFunction));
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
                HttpClient.getRequestUrl(
                        "http://a.b.c:8080",
                        "/api/v1/tables/my_table$schemas",
                        ImmutableMap.of("pageToken", "dt=20230101"));
        assertEquals(
                "http://a.b.c:8080/api/v1/tables/my_table$schemas?pageToken=dt%3D20230101", url);
        Map<String, String> queryParameters = getParameters(url);
        assertEquals(restAuthParameter.parameters().get(queryKey), queryParameters.get(queryKey));
    }

    @Test
    public void testLoggingInterceptorWithRetry() throws IOException {
        AtomicInteger retryCount = new AtomicInteger(0);
        LoggingInterceptor loggingInterceptor = spy(new LoggingInterceptor());
        doAnswer(
                        invocation -> {
                            retryCount.incrementAndGet();
                            Object argument = invocation.getArguments()[0];
                            Interceptor.Chain chain = (Interceptor.Chain) argument;
                            return chain.proceed(chain.request());
                        })
                .when(loggingInterceptor)
                .intercept(any());
        OkHttpClient okHttpClient =
                new OkHttpClient.Builder()
                        .retryOnConnectionFailure(true)
                        .connectionSpecs(Arrays.asList(MODERN_TLS, COMPATIBLE_TLS, CLEARTEXT))
                        .addInterceptor(new ExponentialHttpRetryInterceptor(5))
                        .addInterceptor(loggingInterceptor)
                        .connectTimeout(Duration.ofMinutes(3))
                        .readTimeout(Duration.ofMinutes(2))
                        .build();
        server.enqueueResponse(mockResponseDataStr, 429);
        server.enqueueResponse(mockResponseDataStr, 429);
        server.enqueueResponse(mockResponseDataStr, 200);
        Request request =
                new Request.Builder()
                        .url(HttpClient.getRequestUrl(server.getBaseUrl(), MOCK_PATH, null))
                        .get()
                        .headers(Headers.of())
                        .build();
        Response response = okHttpClient.newCall(request).execute();
        assertEquals(200, response.code());
        assertEquals(3, retryCount.get());
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
}
