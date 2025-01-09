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

import java.util.StringJoiner;

/** Resource paths for REST catalog. */
public class ResourcePaths {

    public static final String V1 = "/v1";
    public static final String SLASH = "/";
    public static final String V1_CONFIG = V1 + "/config";

    public static ResourcePaths forCatalogProperties(Options options) {
        return new ResourcePaths(options.get(RESTCatalogInternalOptions.PREFIX));
    }

    private final String prefix;

    public ResourcePaths(String prefix) {
        this.prefix = prefix;
    }

    public String databases() {
        return new StringJoiner(SLASH).add(V1).add(prefix).add("databases").toString();
    }

    public String database(String databaseName) {
        return new StringJoiner(SLASH)
                .add(V1)
                .add(prefix)
                .add("databases")
                .add(databaseName)
                .toString();
    }

    public String databaseProperties(String databaseName) {
        return new StringJoiner(SLASH)
                .add(V1)
                .add(prefix)
                .add("databases")
                .add(databaseName)
                .add("properties")
                .toString();
    }

    public String tables(String databaseName) {
        return new StringJoiner(SLASH)
                .add(V1)
                .add(prefix)
                .add("databases")
                .add(databaseName)
                .add("tables")
                .toString();
    }

    public String table(String databaseName, String tableName) {
        return new StringJoiner(SLASH)
                .add(V1)
                .add(prefix)
                .add("databases")
                .add(databaseName)
                .add("tables")
                .add(tableName)
                .toString();
    }

    public String renameTable(String databaseName, String tableName) {
        return new StringJoiner(SLASH)
                .add(V1)
                .add(prefix)
                .add("databases")
                .add(databaseName)
                .add("tables")
                .add(tableName)
                .add("rename")
                .toString();
    }

    public String partitions(String databaseName, String tableName) {
        return new StringJoiner(SLASH)
                .add(V1)
                .add(prefix)
                .add("databases")
                .add(databaseName)
                .add("tables")
                .add(tableName)
                .add("partitions")
                .toString();
    }
}
