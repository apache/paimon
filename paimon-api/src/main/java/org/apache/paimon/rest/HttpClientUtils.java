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
import org.apache.paimon.utils.SensitiveConfigUtils;

import org.apache.hc.client5.http.classic.methods.HttpDelete;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpHead;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.hc.client5.http.ssl.DefaultClientTlsStrategy;
import org.apache.hc.client5.http.ssl.HttpsSupport;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.reactor.ssl.SSLBufferMode;
import org.apache.hc.core5.ssl.SSLContexts;
import org.apache.hc.core5.util.Timeout;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Function;

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
        HttpGet httpGet = newHttpGet(uri);
        CloseableHttpResponse response = execute(httpGet, uri);
        int statusCode = response.getCode();
        if (statusCode != HttpStatus.SC_OK) {
            try {
                throw httpError(statusCode);
            } finally {
                response.close();
            }
        }
        return response.getEntity().getContent();
    }

    /**
     * Checks whether an HTTP resource exists. HEAD is attempted first; when HEAD does not return
     * 200, a lightweight GET with {@code Range: bytes=0-0} is used to verify readability. This
     * avoids treating signed or GET-only URLs as missing when HEAD is rejected or returns a
     * different status than GET.
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
                "Unexpected HTTP status code: "
                        + rangeStatusCode
                        + " for uri: "
                        + SensitiveConfigUtils.sanitizeUri(uri));
    }

    public static boolean isNotFoundError(Throwable throwable) {
        Integer statusCode = getHttpStatusCode(throwable);
        return statusCode != null && statusCode == HttpStatus.SC_NOT_FOUND;
    }

    public static boolean isInvalidUriException(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof java.net.URISyntaxException) {
                return true;
            }
            if (current instanceof IllegalArgumentException
                    && current.getMessage() != null
                    && (current.getMessage().contains("Illegal character")
                            || current.getMessage()
                                    .startsWith(SensitiveConfigUtils.INVALID_URI_MESSAGE_PREFIX))) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    public static Integer getHttpStatusCode(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current.getMessage() != null) {
                Integer statusCode = parseHttpStatusCode(current.getMessage());
                if (statusCode != null) {
                    return statusCode;
                }
            }
            current = current.getCause();
        }
        return null;
    }

    private static Integer parseHttpStatusCode(String message) {
        if (message.startsWith("HTTP error code: ")) {
            return parseStatusCodeSuffix(message.substring("HTTP error code: ".length()));
        }
        if (message.startsWith("Unexpected HTTP status code: ")) {
            int end = message.indexOf(' ', "Unexpected HTTP status code: ".length());
            String statusText =
                    end < 0
                            ? message.substring("Unexpected HTTP status code: ".length())
                            : message.substring("Unexpected HTTP status code: ".length(), end);
            return parseStatusCodeSuffix(statusText);
        }
        return null;
    }

    private static Integer parseStatusCodeSuffix(String statusText) {
        try {
            return Integer.parseInt(statusText.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static int headStatusCode(String uri) throws IOException {
        HttpHead httpHead = newHttpHead(uri);
        try (CloseableHttpResponse response = execute(httpHead, uri)) {
            return response.getCode();
        }
    }

    private static int getRangeStatusCode(String uri) throws IOException {
        HttpGet httpGet = newHttpGet(uri);
        httpGet.addHeader("Range", "bytes=0-0");
        try (CloseableHttpResponse response = execute(httpGet, uri)) {
            return response.getCode();
        }
    }

    /**
     * Executes a request, converting any execute-stage failure into an exception that carries
     * neither the original message nor cause. Redirect and protocol errors (e.g. "Circular redirect
     * to &lt;Location&gt;") echo the target URL, which for a signed URL is a credential; only the
     * sanitized request URI is reported.
     */
    private static CloseableHttpResponse execute(ClassicHttpRequest request, String uri)
            throws IOException {
        try {
            return DEFAULT_HTTP_CLIENT.execute(request);
        } catch (IOException | RuntimeException e) {
            throw new IOException(
                    "HTTP request failed for uri: " + SensitiveConfigUtils.sanitizeUri(uri));
        }
    }

    public static HttpGet newHttpGet(String uri) {
        return newRequest(uri, HttpGet::new);
    }

    public static HttpHead newHttpHead(String uri) {
        return newRequest(uri, HttpHead::new);
    }

    public static HttpPost newHttpPost(String uri) {
        return newRequest(uri, HttpPost::new);
    }

    public static HttpDelete newHttpDelete(String uri) {
        return newRequest(uri, HttpDelete::new);
    }

    /** A malformed URL leaks the raw URL from the constructor; sanitize it. */
    private static <T> T newRequest(String uri, Function<String, T> constructor) {
        try {
            return constructor.apply(uri);
        } catch (RuntimeException e) {
            throw SensitiveConfigUtils.invalidUri(uri);
        }
    }

    private static RuntimeException httpError(int statusCode) {
        return new RuntimeException("HTTP error code: " + statusCode);
    }
}
