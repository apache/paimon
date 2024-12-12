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

import org.apache.paimon.rest.exceptions.RESTException;
import org.apache.paimon.rest.responses.ErrorResponse;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

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

import static okhttp3.ConnectionSpec.CLEARTEXT;
import static okhttp3.ConnectionSpec.COMPATIBLE_TLS;
import static okhttp3.ConnectionSpec.MODERN_TLS;
import static org.apache.paimon.utils.ThreadPoolUtils.createCachedThreadPool;

/** HTTP client for REST catalog. */
public class HttpClient implements RESTClient {

    private final OkHttpClient okHttpClient;
    private final String uri;
    private final ObjectMapper mapper;
    private final ErrorHandler errorHandler;

    private static final String THREAD_NAME = "REST-CATALOG-HTTP-CLIENT-THREAD-POOL";
    private static final MediaType MEDIA_TYPE = MediaType.parse("application/json");

    public HttpClient(HttpClientOptions httpClientOptions) {
        this.uri = httpClientOptions.uri();
        this.mapper = httpClientOptions.mapper();
        this.okHttpClient = createHttpClient(httpClientOptions);
        this.errorHandler = httpClientOptions.errorHandler();
    }

    @Override
    public <T extends RESTResponse> T get(
            String path, Class<T> responseType, Map<String, String> headers) {
        try {
            Request request =
                    new Request.Builder()
                            .url(uri + path)
                            .get()
                            .headers(Headers.of(headers))
                            .build();
            return exec(request, responseType);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <T extends RESTResponse> T post(
            String path, RESTRequest body, Class<T> responseType, Map<String, String> headers) {
        try {
            RequestBody requestBody = buildRequestBody(body);
            Request request =
                    new Request.Builder()
                            .url(uri + path)
                            .post(requestBody)
                            .headers(Headers.of(headers))
                            .build();
            return exec(request, responseType);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <T extends RESTResponse> T delete(String path, Map<String, String> headers) {
        try {
            Request request =
                    new Request.Builder()
                            .url(uri + path)
                            .delete()
                            .headers(Headers.of(headers))
                            .build();
            return exec(request, null);
        } catch (Exception e) {
            throw new RuntimeException(e);
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
                ErrorResponse error =
                        new ErrorResponse(
                                responseBodyStr != null ? responseBodyStr : "response body is null",
                                response.code());
                errorHandler.accept(error);
            }
            if (responseType != null && responseBodyStr != null) {
                return mapper.readValue(responseBodyStr, responseType);
            } else if (responseType == null) {
                return null;
            } else {
                throw new RESTException("response body is null.");
            }
        } catch (Exception e) {
            throw new RESTException(e, "rest exception");
        }
    }

    private RequestBody buildRequestBody(RESTRequest body) throws JsonProcessingException {
        return RequestBody.create(mapper.writeValueAsBytes(body), MEDIA_TYPE);
    }

    private static OkHttpClient createHttpClient(HttpClientOptions httpClientOptions) {
        BlockingQueue<Runnable> workQueue = new SynchronousQueue<>();
        ExecutorService executorService =
                createCachedThreadPool(httpClientOptions.threadPoolSize(), THREAD_NAME, workQueue);

        OkHttpClient.Builder builder =
                new OkHttpClient.Builder()
                        .dispatcher(new Dispatcher(executorService))
                        .retryOnConnectionFailure(true)
                        .connectionSpecs(Arrays.asList(MODERN_TLS, COMPATIBLE_TLS, CLEARTEXT));
        httpClientOptions.connectTimeout().ifPresent(builder::connectTimeout);
        httpClientOptions.readTimeout().ifPresent(builder::readTimeout);

        return builder.build();
    }
}
