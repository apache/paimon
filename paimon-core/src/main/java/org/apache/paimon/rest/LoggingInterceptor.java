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

import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** Defines HTTP request log interceptor. */
public class LoggingInterceptor implements Interceptor {

    private static final Logger LOG = LoggerFactory.getLogger(LoggingInterceptor.class);

    public static final String REQUEST_ID_KEY = "x-request-id";
    public static final String DEFAULT_REQUEST_ID = "unknown";

    @Override
    public Response intercept(Chain chain) throws IOException {
        Request request = chain.request();
        long startTime = System.nanoTime();
        Response response = chain.proceed(request);
        long durationMs = (System.nanoTime() - startTime) / 1_000_000;
        String requestId = response.header(REQUEST_ID_KEY, DEFAULT_REQUEST_ID);
        LOG.info(
                "[rest] requestId:{} method:{} url:{} duration:{}ms",
                requestId,
                request.method(),
                request.url(),
                durationMs);
        return response;
    }
}
