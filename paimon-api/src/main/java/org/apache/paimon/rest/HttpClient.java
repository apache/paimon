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
import org.apache.paimon.rest.interceptor.LoggingInterceptor;
import org.apache.paimon.rest.responses.ErrorResponse;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import org.apache.hc.client5.http.classic.methods.HttpDelete;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicHeader;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static org.apache.paimon.rest.HttpClientUtils.DEFAULT_HTTP_CLIENT;

/** Apache HTTP client for REST catalog. */
public class HttpClient implements RESTClient {

    private final String uri;

    private ErrorHandler errorHandler;

    public HttpClient(String uri) {
        this.uri = normalizeUri(uri);
        this.errorHandler = DefaultErrorHandler.getInstance();
    }

    @Override
    public <T extends RESTResponse> T get(
            String path, Class<T> responseType, RESTAuthFunction restAuthFunction) {
        Header[] authHeaders = getHeaders(path, "GET", "", restAuthFunction);
        HttpGet httpGet = new HttpGet(getRequestUrl(path, null));
        httpGet.setHeaders(authHeaders);
        return exec(httpGet, responseType);
    }

    @Override
    public <T extends RESTResponse> T get(
            String path,
            Map<String, String> queryParams,
            Class<T> responseType,
            RESTAuthFunction restAuthFunction) {
        Header[] authHeaders = getHeaders(path, queryParams, "GET", "", restAuthFunction);
        HttpGet httpGet = new HttpGet(getRequestUrl(path, queryParams));
        httpGet.setHeaders(authHeaders);
        return exec(httpGet, responseType);
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
        HttpPost httpPost = new HttpPost(getRequestUrl(path, null));
        String encodedBody = RESTUtil.encodedBody(body);
        if (encodedBody != null) {
            httpPost.setEntity(new StringEntity(encodedBody));
        }
        Header[] authHeaders = getHeaders(path, "POST", encodedBody, restAuthFunction);
        httpPost.setHeaders(authHeaders);
        return exec(httpPost, responseType);
    }

    @Override
    public <T extends RESTResponse> T delete(String path, RESTAuthFunction restAuthFunction) {
        return delete(path, null, restAuthFunction);
    }

    @Override
    public <T extends RESTResponse> T delete(
            String path, RESTRequest body, RESTAuthFunction restAuthFunction) {
        HttpDelete httpDelete = new HttpDelete(getRequestUrl(path, null));
        String encodedBody = RESTUtil.encodedBody(body);
        if (encodedBody != null) {
            httpDelete.setEntity(new StringEntity(encodedBody));
        }
        Header[] authHeaders = getHeaders(path, "DELETE", encodedBody, restAuthFunction);
        httpDelete.setHeaders(authHeaders);
        return exec(httpDelete, null);
    }

    @VisibleForTesting
    void setErrorHandler(ErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
    }

    private <T extends RESTResponse> T exec(HttpUriRequestBase request, Class<T> responseType) {
        try {
            return DEFAULT_HTTP_CLIENT.execute(
                    request,
                    response -> {
                        String responseBodyStr = RESTUtil.extractResponseBodyAsString(response);
                        if (!RESTUtil.isSuccessful(response)) {
                            ErrorResponse error;
                            try {
                                error = RESTApi.fromJson(responseBodyStr, ErrorResponse.class);
                            } catch (JsonProcessingException e) {
                                error =
                                        new ErrorResponse(
                                                null,
                                                null,
                                                responseBodyStr != null
                                                        ? responseBodyStr
                                                        : "response body is null",
                                                response.getCode());
                            }
                            errorHandler.accept(error, extractRequestId(response));
                        }
                        if (responseType != null && responseBodyStr != null) {
                            return RESTApi.fromJson(responseBodyStr, responseType);
                        } else if (responseType == null) {
                            return null;
                        } else {
                            throw new RESTException("response body is null.");
                        }
                    });
        } catch (IOException e) {
            throw new RESTException(
                    e, "Error occurred while processing %s request", request.getMethod());
        }
    }

    private String normalizeUri(String rawUri) {
        if (StringUtils.isEmpty(rawUri)) {
            throw new IllegalArgumentException("uri is empty which must be defined.");
        }

        String normalized =
                rawUri.endsWith("/") ? rawUri.substring(0, rawUri.length() - 1) : rawUri;

        if (!normalized.startsWith("http://") && !normalized.startsWith("https://")) {
            normalized = String.format("http://%s", normalized);
        }

        return normalized;
    }

    @VisibleForTesting
    protected String getRequestUrl(String path, Map<String, String> queryParams) {
        String fullPath = StringUtils.isNullOrWhitespaceOnly(path) ? uri : uri + path;
        return RESTUtil.buildRequestUrl(fullPath, queryParams);
    }

    @VisibleForTesting
    public String uri() {
        return uri;
    }

    private static String extractRequestId(ClassicHttpResponse response) {
        Header header = response.getFirstHeader(LoggingInterceptor.REQUEST_ID_KEY);
        if (header != null && header.getValue() != null) {
            return header.getValue();
        }

        // look for any header containing "request-id"
        return Arrays.stream(response.getHeaders())
                .filter(
                        h ->
                                h.getName() != null
                                        && h.getName().toLowerCase().contains("request-id"))
                .map(Header::getValue)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(LoggingInterceptor.DEFAULT_REQUEST_ID);
    }

    private static Header[] getHeaders(
            String path,
            String method,
            String data,
            Function<RESTAuthParameter, Map<String, String>> headerFunction) {
        return getHeaders(path, Collections.emptyMap(), method, data, headerFunction);
    }

    private static Header[] getHeaders(
            String path,
            Map<String, String> queryParams,
            String method,
            String data,
            Function<RESTAuthParameter, Map<String, String>> headerFunction) {
        if (headerFunction == null) {
            return new Header[0];
        }
        RESTAuthParameter restAuthParameter =
                new RESTAuthParameter(path, queryParams, method, data);
        Map<String, String> headers = headerFunction.apply(restAuthParameter);
        return headers.entrySet().stream()
                .map(entry -> new BasicHeader(entry.getKey(), entry.getValue()))
                .toArray(Header[]::new);
    }
}
