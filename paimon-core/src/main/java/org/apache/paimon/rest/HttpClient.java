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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.exceptions.RESTException;
import org.apache.paimon.rest.responses.ErrorResponse;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import okhttp3.ConnectionPool;
import okhttp3.Dispatcher;
import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import static okhttp3.ConnectionSpec.CLEARTEXT;
import static okhttp3.ConnectionSpec.COMPATIBLE_TLS;
import static okhttp3.ConnectionSpec.MODERN_TLS;
import static org.apache.paimon.rest.RESTObjectMapper.OBJECT_MAPPER;
import static org.apache.paimon.utils.ThreadPoolUtils.createCachedThreadPool;

/** HTTP client for REST catalog. */
public class HttpClient implements RESTClient {

    private static final String THREAD_NAME = "REST-CATALOG-HTTP-CLIENT-THREAD-POOL";
    private static final MediaType MEDIA_TYPE = MediaType.parse("application/json");
    private static final int CONNECTION_KEEP_ALIVE_DURATION_MS = 300_000;

    private final OkHttpClient okHttpClient;
    private final String uri;

    private ErrorHandler errorHandler;

    public HttpClient(Options options) {
        this(HttpClientOptions.create(options));
    }

    public HttpClient(HttpClientOptions httpClientOptions) {
        if (httpClientOptions.uri() != null && httpClientOptions.uri().endsWith("/")) {
            this.uri = httpClientOptions.uri().substring(0, httpClientOptions.uri().length() - 1);
        } else {
            this.uri = httpClientOptions.uri();
        }
        this.okHttpClient = createHttpClient(httpClientOptions);
        this.errorHandler = DefaultErrorHandler.getInstance();
    }

    @VisibleForTesting
    void setErrorHandler(ErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
    }

    @Override
    public <T extends RESTResponse> T get(
            String path, Class<T> responseType, Map<String, String> headers) {
        Request request =
                new Request.Builder()
                        .url(getRequestUrl(path))
                        .get()
                        .headers(Headers.of(headers))
                        .build();
        return exec(request, responseType);
    }

    @Override
    public <T extends RESTResponse> T post(
            String path, RESTRequest body, Map<String, String> headers) {
        return post(path, body, null, headers);
    }

    @Override
    public <T extends RESTResponse> T post(
            String path, RESTRequest body, Class<T> responseType, Map<String, String> headers) {
        try {
            RequestBody requestBody = buildRequestBody(body);
            Request request =
                    new Request.Builder()
                            .url(getRequestUrl(path))
                            .post(requestBody)
                            .headers(Headers.of(headers))
                            .build();
            return exec(request, responseType);
        } catch (JsonProcessingException e) {
            throw new RESTException(e, "build request failed.");
        }
    }

    @Override
    public <T extends RESTResponse> T delete(String path, Map<String, String> headers) {
        Request request =
                new Request.Builder()
                        .url(getRequestUrl(path))
                        .delete()
                        .headers(Headers.of(headers))
                        .build();
        return exec(request, null);
    }

    @Override
    public <T extends RESTResponse> T delete(
            String path, RESTRequest body, Map<String, String> headers) {
        try {
            RequestBody requestBody = buildRequestBody(body);
            Request request =
                    new Request.Builder()
                            .url(getRequestUrl(path))
                            .delete(requestBody)
                            .headers(Headers.of(headers))
                            .build();
            return exec(request, null);
        } catch (JsonProcessingException e) {
            throw new RESTException(e, "build request failed.");
        }
    }

    @Override
    public void close() throws IOException {
        okHttpClient.dispatcher().cancelAll();
        okHttpClient.connectionPool().evictAll();
    }

    private <T extends RESTResponse> T exec(Request request, Class<T> responseType) {
        try (Response response = okHttpClient.newCall(request).execute()) {
            String responseBodyStr = response.body() != null ? response.body().string() : null;
            if (!response.isSuccessful()) {
                ErrorResponse error;
                try {
                    error = OBJECT_MAPPER.readValue(responseBodyStr, ErrorResponse.class);
                } catch (JsonProcessingException e) {
                    error =
                            new ErrorResponse(
                                    null,
                                    null,
                                    responseBodyStr != null
                                            ? responseBodyStr
                                            : "response body is null",
                                    response.code());
                }
                errorHandler.accept(error);
            }
            if (responseType != null && responseBodyStr != null) {
                return OBJECT_MAPPER.readValue(responseBodyStr, responseType);
            } else if (responseType == null) {
                return null;
            } else {
                throw new RESTException("response body is null.");
            }
        } catch (RESTException e) {
            throw e;
        } catch (Exception e) {
            throw new RESTException(e, "rest exception");
        }
    }

    private RequestBody buildRequestBody(RESTRequest body) throws JsonProcessingException {
        return RequestBody.create(OBJECT_MAPPER.writeValueAsBytes(body), MEDIA_TYPE);
    }

    private String getRequestUrl(String path) {
        return StringUtils.isNullOrWhitespaceOnly(path) ? uri : uri + path;
    }

    private static OkHttpClient createHttpClient(HttpClientOptions httpClientOptions) {
        BlockingQueue<Runnable> workQueue = new SynchronousQueue<>();
        ExecutorService executorService =
                createCachedThreadPool(httpClientOptions.threadPoolSize(), THREAD_NAME, workQueue);
        ConnectionPool connectionPool =
                new ConnectionPool(
                        httpClientOptions.maxConnections(),
                        CONNECTION_KEEP_ALIVE_DURATION_MS,
                        TimeUnit.MILLISECONDS);
        Dispatcher dispatcher = new Dispatcher(executorService);
        // set max requests per host use max connections
        dispatcher.setMaxRequestsPerHost(httpClientOptions.maxConnections());
        OkHttpClient.Builder builder =
                new OkHttpClient.Builder()
                        .dispatcher(dispatcher)
                        .retryOnConnectionFailure(true)
                        .connectionPool(connectionPool)
                        .connectionSpecs(Arrays.asList(MODERN_TLS, COMPATIBLE_TLS, CLEARTEXT))
                        .addInterceptor(
                                new ExponentialHttpRetryInterceptor(
                                        httpClientOptions.maxRetries()));
        httpClientOptions
                .connectTimeout()
                .ifPresent(
                        timeoutDuration -> {
                            builder.connectTimeout(timeoutDuration);
                            builder.readTimeout(timeoutDuration);
                        });

        return builder.build();
    }
}
