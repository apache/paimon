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
import org.apache.paimon.rest.auth.RESTAuthFunction;
import org.apache.paimon.rest.auth.RESTAuthParameter;
import org.apache.paimon.rest.exceptions.RESTException;
import org.apache.paimon.rest.responses.ErrorResponse;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import okhttp3.Headers;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

import static okhttp3.ConnectionSpec.CLEARTEXT;
import static okhttp3.ConnectionSpec.COMPATIBLE_TLS;
import static okhttp3.ConnectionSpec.MODERN_TLS;
import static org.apache.paimon.rest.RESTObjectMapper.OBJECT_MAPPER;

/** HTTP client for REST catalog. */
public class HttpClient implements RESTClient {

    private static final OkHttpClient HTTP_CLIENT =
            new OkHttpClient.Builder()
                    .retryOnConnectionFailure(true)
                    .connectionSpecs(Arrays.asList(MODERN_TLS, COMPATIBLE_TLS, CLEARTEXT))
                    .addInterceptor(new ExponentialHttpRetryInterceptor(5))
                    .addInterceptor(new LoggingInterceptor())
                    .connectTimeout(Duration.ofMinutes(3))
                    .readTimeout(Duration.ofMinutes(3))
                    .build();

    private static final MediaType MEDIA_TYPE = MediaType.parse("application/json");

    private final String uri;

    private ErrorHandler errorHandler;

    public HttpClient(String uri) {
        if (uri != null && uri.endsWith("/")) {
            this.uri = uri.substring(0, uri.length() - 1);
        } else {
            this.uri = uri;
        }
        this.errorHandler = DefaultErrorHandler.getInstance();
    }

    @VisibleForTesting
    void setErrorHandler(ErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
    }

    @Override
    public <T extends RESTResponse> T get(
            String path, Class<T> responseType, RESTAuthFunction restAuthFunction) {
        Map<String, String> authHeaders = getHeaders(path, "GET", "", restAuthFunction);
        Request request =
                new Request.Builder()
                        .url(getRequestUrl(uri, path, null))
                        .get()
                        .headers(Headers.of(authHeaders))
                        .build();
        return exec(request, responseType);
    }

    @Override
    public <T extends RESTResponse> T get(
            String path,
            Map<String, String> queryParams,
            Class<T> responseType,
            RESTAuthFunction restAuthFunction) {
        Map<String, String> authHeaders =
                getHeaders(path, queryParams, "GET", "", restAuthFunction);
        Request request =
                new Request.Builder()
                        .url(getRequestUrl(uri, path, queryParams))
                        .get()
                        .headers(Headers.of(authHeaders))
                        .build();
        return exec(request, responseType);
    }

    @Override
    public <T extends RESTResponse> T post(
            String path, RESTRequest body, RESTAuthFunction restAuthFunction) {
        return post(path, body, null, restAuthFunction);
    }

    @Override
    public <T extends RESTResponse> T post(
            String path,
            RESTRequest body,
            Class<T> responseType,
            RESTAuthFunction restAuthFunction) {
        try {
            String bodyStr = OBJECT_MAPPER.writeValueAsString(body);
            Map<String, String> authHeaders = getHeaders(path, "POST", bodyStr, restAuthFunction);
            RequestBody requestBody = buildRequestBody(bodyStr);
            Request request =
                    new Request.Builder()
                            .url(getRequestUrl(uri, path, null))
                            .post(requestBody)
                            .headers(Headers.of(authHeaders))
                            .build();
            return exec(request, responseType);
        } catch (JsonProcessingException e) {
            throw new RESTException(e, "build request failed.");
        }
    }

    @Override
    public <T extends RESTResponse> T delete(String path, RESTAuthFunction restAuthFunction) {
        Map<String, String> authHeaders = getHeaders(path, "DELETE", "", restAuthFunction);
        Request request =
                new Request.Builder()
                        .url(getRequestUrl(uri, path, null))
                        .delete()
                        .headers(Headers.of(authHeaders))
                        .build();
        return exec(request, null);
    }

    @Override
    public <T extends RESTResponse> T delete(
            String path, RESTRequest body, RESTAuthFunction restAuthFunction) {
        try {
            String bodyStr = OBJECT_MAPPER.writeValueAsString(body);
            Map<String, String> authHeaders = getHeaders(path, "DELETE", bodyStr, restAuthFunction);
            RequestBody requestBody = buildRequestBody(bodyStr);
            Request request =
                    new Request.Builder()
                            .url(getRequestUrl(uri, path, null))
                            .delete(requestBody)
                            .headers(Headers.of(authHeaders))
                            .build();
            return exec(request, null);
        } catch (JsonProcessingException e) {
            throw new RESTException(e, "build request failed.");
        }
    }

    @VisibleForTesting
    protected static String getRequestUrl(
            String uri, String path, Map<String, String> queryParams) {
        String fullPath = StringUtils.isNullOrWhitespaceOnly(path) ? uri : uri + path;
        if (queryParams != null && !queryParams.isEmpty()) {
            HttpUrl httpUrl = HttpUrl.parse(fullPath);
            HttpUrl.Builder builder = httpUrl.newBuilder();
            queryParams.forEach(builder::addQueryParameter);
            fullPath = builder.build().toString();
        }
        return fullPath;
    }

    private <T extends RESTResponse> T exec(Request request, Class<T> responseType) {
        try (Response response = HTTP_CLIENT.newCall(request).execute()) {
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

    private static RequestBody buildRequestBody(String body) throws JsonProcessingException {
        return RequestBody.create(body.getBytes(StandardCharsets.UTF_8), MEDIA_TYPE);
    }

    private static Map<String, String> getHeaders(
            String path,
            String method,
            String data,
            Function<RESTAuthParameter, Map<String, String>> headerFunction) {

        return getHeaders(path, Collections.emptyMap(), method, data, headerFunction);
    }

    private static Map<String, String> getHeaders(
            String path,
            Map<String, String> queryParams,
            String method,
            String data,
            Function<RESTAuthParameter, Map<String, String>> headerFunction) {
        RESTAuthParameter restAuthParameter =
                new RESTAuthParameter(path, queryParams, method, data);
        return headerFunction.apply(restAuthParameter);
    }

    @Override
    public void close() {}
}
