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
    @Nullable private final Duration readTimeout;
    private final int threadPoolSize;
    private final int maxConnections;
    private final int maxConnectionsPerRoute;
    private final int maxRetries;

    public HttpClientOptions(
            String uri,
            @Nullable Duration connectTimeout,
            @Nullable Duration readTimeout,
            int threadPoolSize,
            int maxConnections,
            int maxConnectionsPerRoute,
            int maxRetries) {
        this.uri = uri;
        this.connectTimeout = connectTimeout;
        this.readTimeout = readTimeout;
        this.threadPoolSize = threadPoolSize;
        this.maxConnections = maxConnections;
        this.maxConnectionsPerRoute = maxConnectionsPerRoute;
        this.maxRetries = maxRetries;
    }

    public static HttpClientOptions create(Options options) {
        return new HttpClientOptions(
                options.get(RESTCatalogOptions.URI),
                options.get(RESTCatalogOptions.CONNECTION_TIMEOUT),
                options.get(RESTCatalogOptions.READ_TIMEOUT),
                options.get(RESTCatalogOptions.THREAD_POOL_SIZE),
                options.get(RESTCatalogOptions.MAX_CONNECTIONS),
                options.get(RESTCatalogOptions.MAX_CONNECTIONS_PER_ROUTE),
                options.get(RESTCatalogOptions.MAX_RETIES));
    }

    public String uri() {
        return uri;
    }

    public Optional<Duration> connectTimeout() {
        return Optional.ofNullable(connectTimeout);
    }

    public Optional<Duration> readTimeout() {
        return Optional.ofNullable(readTimeout);
    }

    public int threadPoolSize() {
        return threadPoolSize;
    }

    public int maxConnections() {
        return maxConnections;
    }

    public int maxConnectionsPerRoute() {
        return maxConnectionsPerRoute;
    }

    public int maxRetries() {
        return maxRetries;
    }
}
