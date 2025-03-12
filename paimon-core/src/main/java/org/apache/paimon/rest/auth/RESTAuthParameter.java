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

import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.rest.RESTUtil.encodeString;

/** RestAuthParameter for building rest auth header. */
public class RESTAuthParameter {

    private final String resourcePath;
    private final Map<String, String> parameters;
    private final String method;
    private final String data;

    public RESTAuthParameter(
            String resourcePath, Map<String, String> parameters, String method, String data) {
        this.resourcePath = resourcePath;
        this.parameters = new HashMap<>();
        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            this.parameters.put(entry.getKey(), encodeString(entry.getValue()));
        }
        this.method = method;
        this.data = data;
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
}
