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

import java.time.Duration;
import java.util.Optional;

/** HTTP client build parameter. */
public class HttpClientOptions {

    private final String uri;
    private final Optional<Duration> connectTimeout;
    private final Optional<Duration> readTimeout;
    private final ObjectMapper mapper;
    private final int threadPoolSize;
    private final ErrorHandler errorHandler;

    public HttpClientOptions(
            String uri,
            Optional<Duration> connectTimeout,
            Optional<Duration> readTimeout,
            ObjectMapper mapper,
            int threadPoolSize,
            ErrorHandler errorHandler) {
        this.uri = uri;
        this.connectTimeout = connectTimeout;
        this.readTimeout = readTimeout;
        this.mapper = mapper;
        this.threadPoolSize = threadPoolSize;
        this.errorHandler = errorHandler;
    }

    public String uri() {
        return uri;
    }

    public Optional<Duration> connectTimeout() {
        return connectTimeout;
    }

    public Optional<Duration> readTimeout() {
        return readTimeout;
    }

    public ObjectMapper mapper() {
        return mapper;
    }

    public int threadPoolSize() {
        return threadPoolSize;
    }

    public ErrorHandler errorHandler() {
        return errorHandler;
    }
}
