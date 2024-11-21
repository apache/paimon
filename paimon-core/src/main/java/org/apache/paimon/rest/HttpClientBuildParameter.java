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

/** HTTP client build parameter. */
public class HttpClientBuildParameter {
    private final String endpoint;
    private final int connectTimeoutMillis;
    private final int readTimeoutMillis;
    private final ObjectMapper mapper;
    private final int threadPoolSize;

    public HttpClientBuildParameter(
            String endpoint,
            int connectTimeoutMillis,
            int readTimeoutMillis,
            ObjectMapper mapper,
            int threadPoolSize) {
        this.endpoint = endpoint;
        this.connectTimeoutMillis = connectTimeoutMillis;
        this.readTimeoutMillis = readTimeoutMillis;
        this.mapper = mapper;
        this.threadPoolSize = threadPoolSize;
    }

    public String endpoint() {
        return endpoint;
    }

    public int connectTimeoutMillis() {
        return connectTimeoutMillis;
    }

    public int readTimeoutMillis() {
        return readTimeoutMillis;
    }

    public ObjectMapper mapper() {
        return mapper;
    }

    public int threadPoolSize() {
        return threadPoolSize;
    }
}
