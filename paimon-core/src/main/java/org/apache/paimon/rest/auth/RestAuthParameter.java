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

package org.apache.paimon.rest.auth;

import java.util.Map;

/** RestAuthParameter for building rest auth header. */
public class RestAuthParameter {
    private final String host;
    private final String path;
    private final String method;
    private final Map<String, String> query;
    private final Map<String, String> headers;

    public RestAuthParameter(
            String host,
            String path,
            String method,
            Map<String, String> query,
            Map<String, String> headers) {
        this.host = host;
        this.path = path;
        this.method = method;
        this.query = query;
        this.headers = headers;
    }

    public String host() {
        return host;
    }

    public String path() {
        return path;
    }

    public String method() {
        return method;
    }

    public Map<String, String> query() {
        return query;
    }

    public Map<String, String> headers() {
        return headers;
    }
}
