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
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpRequestInterceptor;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.hc.core5.http.protocol.HttpCoreContext;

import java.io.IOException;

import static org.apache.paimon.rest.interceptor.LoggingInterceptor.REQUEST_START_TIME_KEY;

/** Defines HTTP request log interceptor for record request start time. */
public class TimingInterceptor implements HttpRequestInterceptor {
    @Override
    public void process(
            HttpRequest httpRequest, EntityDetails entityDetails, HttpContext httpContext)
            throws HttpException, IOException {
        HttpCoreContext coreContext = HttpCoreContext.cast(httpContext);
        coreContext.setAttribute(REQUEST_START_TIME_KEY, System.currentTimeMillis());
    }
}
