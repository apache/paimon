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

package org.apache.paimon.utils;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.QueryStringDecoder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Parsing and managing request parameters from an HttpRequest. */
public class RequestParameterUtil {

    private HttpRequest request;

    private final Map<String, List<String>> params = new HashMap<>();

    public RequestParameterUtil(HttpRequest httpRequest) {
        this.request = httpRequest;
        this.parse();
    }

    private void parse() {
        QueryStringDecoder decoder = new QueryStringDecoder(this.request.uri());
        this.params.putAll(decoder.parameters());
    }

    public List<String> getParameter(String paramName) {
        return this.params.get(paramName);
    }

    public String getSingleParameter(String paramName) {
        return this.params.get(paramName).get(0);
    }

    public String[] getParameterValues(String paramName) {
        return this.params.get(paramName).toArray(new String[10]);
    }

    public Map<String, List<String>> getParams() {
        return params;
    }
}
