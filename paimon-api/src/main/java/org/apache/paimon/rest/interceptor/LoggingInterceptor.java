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

package org.apache.paimon.rest.interceptor;

import org.apache.hc.core5.http.EntityDetails;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.HttpResponseInterceptor;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.hc.core5.http.protocol.HttpCoreContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;

/** Defines HTTP request log interceptor. */
public class LoggingInterceptor implements HttpResponseInterceptor {

    private static final Logger LOG = LoggerFactory.getLogger(LoggingInterceptor.class);
    public static final String REQUEST_ID_KEY = "x-request-id";
    public static final String DEFAULT_REQUEST_ID = "unknown";
    public static final String REQUEST_START_TIME_KEY = "request-start-time";

    @Override
    public void process(
            HttpResponse httpResponse, EntityDetails entityDetails, HttpContext httpContext) {
        HttpCoreContext coreContext = HttpCoreContext.cast(httpContext);
        HttpRequest request = coreContext.getRequest();
        Long startTime = (Long) coreContext.getAttribute(REQUEST_START_TIME_KEY);
        long durationMs = System.currentTimeMillis() - startTime;

        String requestId =
                httpResponse.getHeaders(REQUEST_ID_KEY).length > 0
                        ? httpResponse.getFirstHeader(REQUEST_ID_KEY).getValue()
                        : DEFAULT_REQUEST_ID;

        try {
            LOG.info(
                    "[rest] requestId:{} method:{} url:{} duration:{}ms",
                    requestId,
                    request.getMethod(),
                    request.getUri(),
                    durationMs);
        } catch (URISyntaxException e) {
            LOG.warn("Failed to log rest request: {}", e.getMessage());
        }
    }
}
