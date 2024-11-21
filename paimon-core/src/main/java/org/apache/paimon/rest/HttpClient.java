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

import org.apache.paimon.shade.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static okhttp3.ConnectionSpec.CLEARTEXT;
import static okhttp3.ConnectionSpec.COMPATIBLE_TLS;
import static okhttp3.ConnectionSpec.MODERN_TLS;

/** HTTP client for REST catalog. */
public class HttpClient implements RESTClient {

    private final OkHttpClient okHttpClient;
    private final String endpoint;
    private final ObjectMapper mapper;
    private final ErrorHandler errorHandler;

    public HttpClient(HttpClientBuildParameter httpClientBuildParameter) {
        this.endpoint = httpClientBuildParameter.endpoint();
        this.mapper = httpClientBuildParameter.mapper();
        this.okHttpClient = createHttpClient(httpClientBuildParameter);
        this.errorHandler = httpClientBuildParameter.errorHandler();
    }

    @Override
    public <T extends RESTResponse> T post(
            String path, RESTRequest body, Class<T> responseType, Map<String, String> headers) {
        try {
            RequestBody requestBody = buildRequestBody(body);
            Request request =
                    new Request.Builder()
                            .url(endpoint + path)
                            .post(requestBody)
                            .headers(Headers.of(headers))
                            .build();
            return exec(request, responseType);
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
                                response.code(),
                                null);
                errorHandler.accept(error);
            }
            if (responseBodyStr == null) {
                throw new RESTException("response body is null.");
            }
            return mapper.readValue(responseBodyStr, responseType);
        } catch (Exception e) {
            throw new RESTException(e, "rest exception");
        }
    }

    private RequestBody buildRequestBody(RESTRequest body) throws JsonProcessingException {
        return RequestBody.create(
                MediaType.parse("application/json"), mapper.writeValueAsString(body));
    }

    private static OkHttpClient createHttpClient(
            HttpClientBuildParameter httpClientBuildParameter) {
        ExecutorService executorService =
                new ThreadPoolExecutor(
                        httpClientBuildParameter.threadPoolSize(),
                        httpClientBuildParameter.threadPoolSize(),
                        60,
                        TimeUnit.SECONDS,
                        new SynchronousQueue<>(),
                        new ThreadFactoryBuilder()
                                .setDaemon(true)
                                .setNameFormat("rest catalog http client %d")
                                .build());

        OkHttpClient.Builder builder =
                new OkHttpClient.Builder()
                        .connectTimeout(
                                httpClientBuildParameter.connectTimeoutMillis(),
                                TimeUnit.MILLISECONDS)
                        .readTimeout(
                                httpClientBuildParameter.readTimeoutMillis(), TimeUnit.MILLISECONDS)
                        .dispatcher(new Dispatcher(executorService))
                        .retryOnConnectionFailure(true)
                        .connectionSpecs(Arrays.asList(MODERN_TLS, COMPATIBLE_TLS, CLEARTEXT));

        return builder.build();
    }
}
