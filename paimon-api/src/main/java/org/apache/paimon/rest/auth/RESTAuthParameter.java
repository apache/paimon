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

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.rest.RESTUtil.encodeString;

/** RestAuthParameter for building rest auth header. */
public class RESTAuthParameter {

    private final String resourcePath;
    private final Map<String, String> parameters;
    private final String method;
    private final String data;
    @Nullable private final String apiName;

    public RESTAuthParameter(
            String resourcePath, Map<String, String> parameters, String method, String data) {
        this(resourcePath, encode(parameters), method, data, null);
    }

    private RESTAuthParameter(
            String resourcePath,
            Map<String, String> encodedParameters,
            String method,
            String data,
            @Nullable String apiName) {
        this.resourcePath = resourcePath;
        this.parameters = encodedParameters;
        this.method = method;
        this.data = data;
        this.apiName = apiName;
    }

    private static Map<String, String> encode(Map<String, String> parameters) {
        Map<String, String> encoded = new HashMap<>();
        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            encoded.put(entry.getKey(), encodeString(entry.getValue()));
        }
        return encoded;
    }

    /** Returns a copy naming the API this request calls; parameters are not encoded again. */
    public RESTAuthParameter withApiName(@Nullable String apiName) {
        return new RESTAuthParameter(resourcePath, parameters, method, data, apiName);
    }

    public String resourcePath() {
        return resourcePath;
    }

    public Map<String, String> parameters() {
        return parameters;
    }

    public String method() {
        return method;
    }

    public String data() {
        return data;
    }

    /** The API this request calls, as its request class declares in {@code API_NAME}. */
    @Nullable
    public String apiName() {
        return apiName;
    }
}
