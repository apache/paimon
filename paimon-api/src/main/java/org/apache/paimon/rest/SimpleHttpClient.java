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

import org.apache.paimon.utils.StringUtils;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicHeader;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

import static org.apache.paimon.rest.HttpClientUtils.createBuilder;

/** A simple client to wrap {@link CloseableHttpClient}. */
public class SimpleHttpClient implements Closeable {

    public static final SimpleHttpClient INSTANCE = new SimpleHttpClient();

    private final CloseableHttpClient client;

    private SimpleHttpClient() {
        this.client = createBuilder().build();
    }

    public String post(String url, Object body, Map<String, String> headers) throws IOException {
        HttpPost httpPost = new HttpPost(url);
        if (headers != null) {
            httpPost.setHeaders(
                    headers.entrySet().stream()
                            .map(entry -> new BasicHeader(entry.getKey(), entry.getValue()))
                            .toArray(Header[]::new));
        }
        String encodedBody = RESTUtil.encodedBody(body);
        if (encodedBody != null) {
            httpPost.setEntity(new StringEntity(encodedBody));
        }

        return exec(httpPost);
    }

    public String get(String url, Map<String, String> queryParams, Map<String, String> headers)
            throws IOException {
        HttpGet httpGet = new HttpGet(RESTUtil.buildRequestUrl(url, queryParams));
        if (headers != null) {
            httpGet.setHeaders(
                    headers.entrySet().stream()
                            .map(entry -> new BasicHeader(entry.getKey(), entry.getValue()))
                            .toArray(Header[]::new));
        }
        return exec(httpGet);
    }

    public String get(String url) throws IOException {
        return get(url, null, null);
    }

    private String exec(HttpUriRequestBase request) {
        try {
            return client.execute(
                    request,
                    response -> {
                        String responseBodyStr = RESTUtil.extractResponseBodyAsString(response);

                        if (StringUtils.isNullOrWhitespaceOnly(responseBodyStr)
                                || !RESTUtil.isSuccessful(response)) {
                            throw new RuntimeException(
                                    RESTUtil.isSuccessful(response)
                                            ? "ResponseBody is null or empty."
                                            : String.format(
                                                    "Response is not successful, response is %s",
                                                    response));
                        }
                        return responseBodyStr;
                    });
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to convert HTTP response body to string, error : " + e.getMessage());
        }
    }

    @Override
    public void close() throws IOException {
        try {
            client.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
