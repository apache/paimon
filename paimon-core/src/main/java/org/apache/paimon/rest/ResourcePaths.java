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

import org.apache.paimon.shade.guava30.com.google.common.base.Joiner;

/** Resource paths for REST catalog. */
public class ResourcePaths {

    private static final Joiner SLASH = Joiner.on("/").skipNulls();
    private static final String V1 = "/v1";
    private static final String DATABASES = "databases";
    private static final String TABLES = "tables";
    private static final String PARTITIONS = "partitions";
    private static final String BRANCHES = "branches";
    private static final String VIEWS = "views";
    public static final String QUERY_PARAMETER_WAREHOUSE_KEY = "warehouse";

    public static String config(String warehouse) {
        return String.format("%s/config?%s=%s", V1, QUERY_PARAMETER_WAREHOUSE_KEY, warehouse);
    }

    public static ResourcePaths forCatalogProperties(Options options) {
        return new ResourcePaths(options.get(RESTCatalogInternalOptions.PREFIX));
    }

    private final String prefix;

    public ResourcePaths(String prefix) {
        this.prefix = prefix;
    }

    public String databases() {
        return SLASH.join(V1, prefix, DATABASES);
    }

    public String database(String databaseName) {
        return SLASH.join(V1, prefix, DATABASES, databaseName);
    }

    public String databaseProperties(String databaseName) {
        return SLASH.join(V1, prefix, DATABASES, databaseName, "properties");
    }

    public String tables(String databaseName) {
        return SLASH.join(V1, prefix, DATABASES, databaseName, TABLES);
    }

    public String table(String databaseName, String tableName) {
        return SLASH.join(V1, prefix, DATABASES, databaseName, TABLES, tableName);
    }

    public String renameTable(String databaseName) {
        return SLASH.join(V1, prefix, DATABASES, databaseName, TABLES, "rename");
    }

    public String commitTable(String databaseName) {
        return SLASH.join(V1, prefix, DATABASES, databaseName, TABLES, "commit");
    }

    public String tableToken(String databaseName, String tableName) {
        return SLASH.join(V1, prefix, DATABASES, databaseName, TABLES, tableName, "token");
    }

    public String tableSnapshot(String databaseName, String tableName) {
        return SLASH.join(V1, prefix, DATABASES, databaseName, TABLES, tableName, "snapshot");
    }

    public String partitions(String databaseName, String tableName) {
        return SLASH.join(V1, prefix, DATABASES, databaseName, TABLES, tableName, PARTITIONS);
    }

    public String dropPartitions(String databaseName, String tableName) {
        return SLASH.join(
                V1, prefix, DATABASES, databaseName, TABLES, tableName, PARTITIONS, "drop");
    }

    public String alterPartitions(String databaseName, String tableName) {
        return SLASH.join(
                V1, prefix, DATABASES, databaseName, TABLES, tableName, PARTITIONS, "alter");
    }

    public String markDonePartitions(String databaseName, String tableName) {
        return SLASH.join(
                V1, prefix, DATABASES, databaseName, TABLES, tableName, PARTITIONS, "mark");
    }

    public String branches(String databaseName, String tableName) {
        return SLASH.join(V1, prefix, DATABASES, databaseName, TABLES, tableName, BRANCHES);
    }

    public String branch(String databaseName, String tableName, String branchName) {
        return SLASH.join(
                V1, prefix, DATABASES, databaseName, TABLES, tableName, BRANCHES, branchName);
    }

    public String forwardBranch(String databaseName, String tableName) {
        return SLASH.join(
                V1, prefix, DATABASES, databaseName, TABLES, tableName, BRANCHES, "forward");
    }

    public String views(String databaseName) {
        return SLASH.join(V1, prefix, DATABASES, databaseName, VIEWS);
    }

    public String view(String databaseName, String viewName) {
        return SLASH.join(V1, prefix, DATABASES, databaseName, VIEWS, viewName);
    }

    public String renameView(String databaseName) {
        return SLASH.join(V1, prefix, DATABASES, databaseName, VIEWS, "rename");
    }
}
