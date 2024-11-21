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

import java.util.StringJoiner;

/** Resource paths for REST catalog. */
public class ResourcePaths {
    private static final StringJoiner SLASH = new StringJoiner("/");
    public static final String V1_CONFIG = "/api/v1/{prefix}/config";

    public static ResourcePaths forCatalogProperties(String prefix) {
        return new ResourcePaths(prefix);
    }

    private final String prefix;

    public ResourcePaths(String prefix) {
        this.prefix = prefix;
    }

    public String config() {
        return SLASH.add("v1").add(prefix).add("config").toString();
    }
}
