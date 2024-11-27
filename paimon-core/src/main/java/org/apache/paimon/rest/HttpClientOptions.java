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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.Duration;

/** HTTP client build parameter. */
public class HttpClientOptions {

    private final URI endpoint;
    private final Duration connectTimeout;
    private final Duration readTimeout;
    private final ObjectMapper mapper;
    private final int threadPoolSize;
    private final ErrorHandler errorHandler;
    private final int queueSize;

    public HttpClientOptions(
            URI endpoint,
            Duration connectTimeout,
            Duration readTimeout,
            ObjectMapper mapper,
            int threadPoolSize,
            int queueSize,
            ErrorHandler errorHandler) {
        this.endpoint = endpoint;
        this.connectTimeout = connectTimeout;
        this.readTimeout = readTimeout;
        this.mapper = mapper;
        this.threadPoolSize = threadPoolSize;
        this.errorHandler = errorHandler;
        this.queueSize = queueSize;
    }

    public URI endpoint() {
        return endpoint;
    }

    public Duration connectTimeout() {
        return connectTimeout;
    }

    public Duration readTimeout() {
        return readTimeout;
    }

    public ObjectMapper mapper() {
        return mapper;
    }

    public int threadPoolSize() {
        return threadPoolSize;
    }

    public int queueSize() {
        return queueSize;
    }

    public ErrorHandler errorHandler() {
        return errorHandler;
    }
}
