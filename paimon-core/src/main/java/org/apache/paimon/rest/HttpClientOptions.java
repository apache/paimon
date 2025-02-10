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

import org.apache.paimon.options.Options;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Optional;

/** Options for Http Client. */
public class HttpClientOptions {

    private final String uri;
    @Nullable private final Duration connectTimeout;
    private final int threadPoolSize;
    private final int maxConnections;
    private final int maxRetries;

    public HttpClientOptions(
            String uri,
            @Nullable Duration connectTimeout,
            int threadPoolSize,
            int maxConnections,
            int maxRetries) {
        this.uri = uri;
        this.connectTimeout = connectTimeout;
        this.threadPoolSize = threadPoolSize;
        this.maxConnections = maxConnections;
        this.maxRetries = maxRetries;
    }

    public static HttpClientOptions create(Options options) {
        return new HttpClientOptions(
                options.get(RESTCatalogOptions.URI),
                options.get(RESTCatalogOptions.CONNECTION_TIMEOUT),
                options.get(RESTCatalogOptions.THREAD_POOL_SIZE),
                options.get(RESTCatalogOptions.MAX_CONNECTIONS),
                options.get(RESTCatalogOptions.MAX_RETIES));
    }

    public String uri() {
        return uri;
    }

    public Optional<Duration> connectTimeout() {
        return Optional.ofNullable(connectTimeout);
    }

    public int threadPoolSize() {
        return threadPoolSize;
    }

    public int maxConnections() {
        return maxConnections;
    }

    public int maxRetries() {
        return Math.max(maxRetries, 0);
    }
}
