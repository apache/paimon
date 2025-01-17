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

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableSet;
import org.apache.paimon.shade.guava30.com.google.common.net.HttpHeaders;

import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;

import javax.net.ssl.SSLException;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.UnknownHostException;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Defines exponential HTTP request retry interceptor.
 *
 * <p>The following retrievable IOException
 *
 * <ul>
 *   <li>InterruptedIOException
 *   <li>UnknownHostException
 *   <li>ConnectException
 *   <li>NoRouteToHostException
 *   <li>SSLException
 * </ul>
 *
 * <p>The following retrievable HTTP status codes are defined:
 *
 * <ul>
 *   <li>TOO_MANY_REQUESTS (429)
 *   <li>BAD_GATEWAY (502)
 *   <li>SERVICE_UNAVAILABLE (503)
 *   <li>GATEWAY_TIMEOUT (504)
 * </ul>
 *
 * <p>The following retrievable HTTP method which is idempotent are defined:
 *
 * <ul>
 *   <li>GET
 *   <li>HEAD
 *   <li>PUT
 *   <li>DELETE
 *   <li>TRACE
 *   <li>OPTIONS
 * </ul>
 */
public class ExponentialHttpRetryInterceptor implements Interceptor {

    private final int maxRetries;
    private final Set<Class<? extends IOException>> nonRetriableExceptions;
    private final Set<Integer> retrievableCodes;
    private final Set<String> retrievableMethods;

    public ExponentialHttpRetryInterceptor(int maxRetries) {
        this.maxRetries = maxRetries;
        this.retrievableMethods =
                ImmutableSet.of("GET", "HEAD", "PUT", "DELETE", "TRACE", "OPTIONS");
        this.retrievableCodes = ImmutableSet.of(429, 502, 503, 504);
        this.nonRetriableExceptions =
                ImmutableSet.of(
                        InterruptedIOException.class,
                        UnknownHostException.class,
                        ConnectException.class,
                        NoRouteToHostException.class,
                        SSLException.class);
    }

    @Override
    public Response intercept(Chain chain) throws IOException {
        Request request = chain.request();
        Response response = null;

        for (int retryCount = 1; ; retryCount++) {
            try {
                response = chain.proceed(request);
            } catch (IOException e) {
                if (needRetry(request.method(), e, retryCount)) {
                    wait(response, retryCount);
                    continue;
                }
            }
            if (needRetry(response, retryCount)) {
                if (response != null) {
                    response.close();
                }
                wait(response, retryCount);
            } else {
                return response;
            }
        }
    }

    public boolean needRetry(Response response, int execCount) {
        if (execCount > maxRetries) {
            return false;
        }
        return response == null
                || (!response.isSuccessful() && retrievableCodes.contains(response.code()));
    }

    public boolean needRetry(String method, IOException e, int execCount) {
        if (execCount > maxRetries) {
            return false;
        }
        if (!retrievableMethods.contains(method)) {
            return false;
        }
        if (nonRetriableExceptions.contains(e.getClass())) {
            return false;
        } else {
            for (Class<? extends IOException> rejectException : nonRetriableExceptions) {
                if (rejectException.isInstance(e)) {
                    return false;
                }
            }
        }
        return true;
    }

    public long getRetryIntervalInMilliseconds(Response response, int execCount) {
        // a server may send a 429 / 503 with a Retry-After header
        // https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Retry-After
        String retryAfterStrInSecond =
                response == null ? null : response.header(HttpHeaders.RETRY_AFTER);
        Long retryAfter = null;
        if (retryAfterStrInSecond != null) {
            try {
                retryAfter = Long.parseLong(retryAfterStrInSecond) * 1000;
            } catch (Throwable ignore) {
            }

            if (retryAfter != null && retryAfter > 0) {
                return retryAfter;
            }
        }

        int delayMillis = 1000 * (int) Math.min(Math.pow(2.0, (long) execCount - 1.0), 64.0);
        int jitter = ThreadLocalRandom.current().nextInt(Math.max(1, (int) (delayMillis * 0.1)));

        return delayMillis + jitter;
    }

    private void wait(Response response, int retryCount) throws InterruptedIOException {
        try {
            Thread.sleep(getRetryIntervalInMilliseconds(response, retryCount));
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException();
        }
    }
}
