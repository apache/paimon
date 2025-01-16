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

import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.hc.core5.http.HttpHeaders;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLException;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.UnknownHostException;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ExponentialHttpRetryInterceptor}. */
class ExponentialHttpRetryInterceptorTest {

    private final int maxRetries = 5;
    private final ExponentialHttpRetryInterceptor interceptor =
            new ExponentialHttpRetryInterceptor(maxRetries);

    @Test
    void testNeedRetryByMethod() {

        assertThat(interceptor.needRetry("GET", new IOException(), 1)).isTrue();
        assertThat(interceptor.needRetry("HEAD", new IOException(), 1)).isTrue();
        assertThat(interceptor.needRetry("PUT", new IOException(), 1)).isTrue();
        assertThat(interceptor.needRetry("DELETE", new IOException(), 1)).isTrue();
        assertThat(interceptor.needRetry("TRACE", new IOException(), 1)).isTrue();
        assertThat(interceptor.needRetry("OPTIONS", new IOException(), 1)).isTrue();

        assertThat(interceptor.needRetry("POST", new IOException(), 1)).isFalse();
        assertThat(interceptor.needRetry("PATCH", new IOException(), 1)).isFalse();
        assertThat(interceptor.needRetry("CONNECT", new IOException(), 1)).isFalse();
        assertThat(interceptor.needRetry("GET", new IOException(), maxRetries + 1)).isFalse();
    }

    @Test
    void testNeedRetryByException() {

        assertThat(interceptor.needRetry("GET", new InterruptedIOException(), 1)).isFalse();
        assertThat(interceptor.needRetry("GET", new UnknownHostException(), 1)).isFalse();
        assertThat(interceptor.needRetry("GET", new ConnectException(), 1)).isFalse();
        assertThat(interceptor.needRetry("GET", new NoRouteToHostException(), 1)).isFalse();
        assertThat(interceptor.needRetry("GET", new SSLException("error"), 1)).isFalse();

        assertThat(interceptor.needRetry("GET", new IOException("error"), 1)).isTrue();
        assertThat(interceptor.needRetry("GET", new IOException("error"), maxRetries + 1))
                .isFalse();
    }

    @Test
    void testRetryByResponse() {

        assertThat(interceptor.needRetry(createResponse(429), 1)).isTrue();
        assertThat(interceptor.needRetry(createResponse(503), 1)).isTrue();
        assertThat(interceptor.needRetry(createResponse(502), 1)).isTrue();
        assertThat(interceptor.needRetry(createResponse(504), 1)).isTrue();

        assertThat(interceptor.needRetry(createResponse(500), 1)).isFalse();
        assertThat(interceptor.needRetry(createResponse(404), 1)).isFalse();
        assertThat(interceptor.needRetry(createResponse(200), 1)).isFalse();
    }

    @Test
    void invalidRetryAfterHeader() {
        Response response = createResponse(429, "Stuff");

        assertThat(interceptor.getRetryIntervalInMilliseconds(response, 3)).isBetween(4000L, 5000L);
    }

    @Test
    void validRetryAfterHeader() {
        long retryAfter = 3;
        Response response = createResponse(429, retryAfter + "");
        assertThat(interceptor.getRetryIntervalInMilliseconds(response, 3))
                .isEqualTo(retryAfter * 1000);
    }

    @Test
    void exponentialRetry() {
        ExponentialHttpRetryInterceptor interceptor = new ExponentialHttpRetryInterceptor(10);
        Response response = createResponse(429, "Stuff");

        // note that the upper limit includes ~10% variability
        assertThat(interceptor.getRetryIntervalInMilliseconds(response, 0)).isEqualTo(0);
        assertThat(interceptor.getRetryIntervalInMilliseconds(response, 1)).isBetween(1000L, 2000L);
        assertThat(interceptor.getRetryIntervalInMilliseconds(response, 2)).isBetween(2000L, 3000L);
        assertThat(interceptor.getRetryIntervalInMilliseconds(response, 3)).isBetween(4000L, 5000L);
        assertThat(interceptor.getRetryIntervalInMilliseconds(response, 4)).isBetween(8000L, 9000L);
        assertThat(interceptor.getRetryIntervalInMilliseconds(response, 5))
                .isBetween(16000L, 18000L);
        assertThat(interceptor.getRetryIntervalInMilliseconds(response, 6))
                .isBetween(32000L, 36000L);
        assertThat(interceptor.getRetryIntervalInMilliseconds(response, 7))
                .isBetween(64000L, 72000L);
        assertThat(interceptor.getRetryIntervalInMilliseconds(response, 10))
                .isBetween(64000L, 72000L);
    }

    private static Response createResponse(int httpCode) {
        return createResponse(httpCode, "");
    }

    private static Response createResponse(int httpCode, String retryAfter) {
        return new Response.Builder()
                .code(httpCode)
                .message("message")
                .protocol(Protocol.HTTP_1_1)
                .request(new Request.Builder().url("http://localhost").build())
                .addHeader(HttpHeaders.RETRY_AFTER, retryAfter)
                .build();
    }
}
