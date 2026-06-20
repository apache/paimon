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

import org.apache.paimon.rest.interceptor.LoggingInterceptor;
import org.apache.paimon.rest.interceptor.TimingInterceptor;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpHead;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.hc.client5.http.ssl.DefaultClientTlsStrategy;
import org.apache.hc.client5.http.ssl.HttpsSupport;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.reactor.ssl.SSLBufferMode;
import org.apache.hc.core5.ssl.SSLContexts;
import org.apache.hc.core5.util.Timeout;

import java.io.IOException;
import java.io.InputStream;

/** Utils for {@link HttpClientBuilder}. */
public class HttpClientUtils {

    public static final CloseableHttpClient DEFAULT_HTTP_CLIENT = createLoggingBuilder().build();

    public static HttpClientBuilder createLoggingBuilder() {
        HttpClientBuilder clientBuilder = createBuilder();
        clientBuilder
                .addRequestInterceptorFirst(new TimingInterceptor())
                .addResponseInterceptorLast(new LoggingInterceptor());
        return clientBuilder;
    }

    public static HttpClientBuilder createBuilder() {
        HttpClientBuilder clientBuilder = HttpClients.custom();
        RequestConfig requestConfig =
                RequestConfig.custom()
                        .setConnectionRequestTimeout(Timeout.ofMinutes(3))
                        .setResponseTimeout(Timeout.ofMinutes(3))
                        .build();
        clientBuilder.setDefaultRequestConfig(requestConfig);

        clientBuilder.setConnectionManager(configureConnectionManager());
        clientBuilder.setRetryStrategy(new ExponentialHttpRequestRetryStrategy(5));
        return clientBuilder;
    }

    private static HttpClientConnectionManager configureConnectionManager() {
        PoolingHttpClientConnectionManagerBuilder connectionManagerBuilder =
                PoolingHttpClientConnectionManagerBuilder.create();
        connectionManagerBuilder.useSystemProperties().setMaxConnTotal(100).setMaxConnPerRoute(100);

        // support TLS
        String[] tlsProtocols = {"TLSv1.2", "TLSv1.3"};
        connectionManagerBuilder.setTlsSocketStrategy(
                new DefaultClientTlsStrategy(
                        SSLContexts.createDefault(),
                        tlsProtocols,
                        null,
                        SSLBufferMode.STATIC,
                        HttpsSupport.getDefaultHostnameVerifier()));

        return connectionManagerBuilder.build();
    }

    public static InputStream getAsInputStream(String uri) throws IOException {
        HttpGet httpGet = new HttpGet(uri);
        CloseableHttpResponse response = DEFAULT_HTTP_CLIENT.execute(httpGet);
        if (response.getCode() != 200) {
            throw new RuntimeException("HTTP error code: " + response.getCode());
        }
        return response.getEntity().getContent();
    }

    /**
     * Checks whether an HTTP resource exists. HEAD is attempted first; when HEAD does not return
     * 200, a lightweight GET with {@code Range: bytes=0-0} is used to verify readability. This
     * avoids treating signed or GET-only URLs as missing when HEAD is rejected or returns a
     * different status than GET. HTTP 416 from the range probe indicates a zero-length resource.
     */
    public static boolean exists(String uri) throws IOException {
        int headStatusCode = headStatusCode(uri);
        if (headStatusCode == HttpStatus.SC_OK) {
            return true;
        }
        int rangeStatusCode = getRangeStatusCode(uri);
        if (rangeStatusCode == HttpStatus.SC_OK
                || rangeStatusCode == HttpStatus.SC_PARTIAL_CONTENT
                || rangeStatusCode == HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE) {
            return true;
        }
        if (rangeStatusCode == HttpStatus.SC_NOT_FOUND) {
            return false;
        }
        throw new IOException(
                "Unexpected HTTP status code: " + rangeStatusCode + " for uri: " + uri);
    }

    private static int headStatusCode(String uri) throws IOException {
        HttpHead httpHead = new HttpHead(uri);
        try (CloseableHttpResponse response = DEFAULT_HTTP_CLIENT.execute(httpHead)) {
            return response.getCode();
        }
    }

    private static int getRangeStatusCode(String uri) throws IOException {
        HttpGet httpGet = new HttpGet(uri);
        httpGet.addHeader("Range", "bytes=0-0");
        try (CloseableHttpResponse response = DEFAULT_HTTP_CLIENT.execute(httpGet)) {
            return response.getCode();
        }
    }
}
