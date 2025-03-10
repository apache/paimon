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

import static org.apache.paimon.rest.RESTUtil.encodeString;

/** Resource paths for REST catalog. */
public class ResourcePaths {

    private static final Joiner SLASH = Joiner.on("/").skipNulls();
    private static final String V1 = "/v1";
    private static final String DATABASES = "databases";
    private static final String TABLES = "tables";
    private static final String PARTITIONS = "partitions";
    private static final String BRANCHES = "branches";
    private static final String VIEWS = "views";
    private static final String TABLE_DETAILS = "table-details";
    private static final String VIEW_DETAILS = "view-details";

    public static String config() {
        return String.format("%s/config", V1);
    }

    public static ResourcePaths forCatalogProperties(Options options) {
        return new ResourcePaths(options.get(RESTCatalogInternalOptions.PREFIX));
    }

    private final String prefix;

    public ResourcePaths(String prefix) {
        this.prefix = encodeString(prefix);
    }

    public String databases() {
        return SLASH.join(V1, prefix, DATABASES);
    }

    public String database(String databaseName) {
        return SLASH.join(V1, prefix, DATABASES, encodeString(databaseName));
    }

    public String databaseProperties(String databaseName) {
        return SLASH.join(V1, prefix, DATABASES, encodeString(databaseName), "properties");
    }

    public String tables(String databaseName) {
        return SLASH.join(V1, prefix, DATABASES, encodeString(databaseName), TABLES);
    }

    public String tableDetails(String databaseName) {
        return SLASH.join(V1, prefix, DATABASES, encodeString(databaseName), TABLE_DETAILS);
    }

    public String table(String databaseName, String objectName) {
        return SLASH.join(
                V1,
                prefix,
                DATABASES,
                encodeString(databaseName),
                TABLES,
                encodeString(objectName));
    }

    public String renameTable() {
        return SLASH.join(V1, prefix, TABLES, "rename");
    }

    public String commitTable(String databaseName, String objectName) {
        return SLASH.join(
                V1,
                prefix,
                DATABASES,
                encodeString(databaseName),
                TABLES,
                encodeString(objectName),
                "commit");
    }

    public String tableToken(String databaseName, String objectName) {
        return SLASH.join(
                V1,
                prefix,
                DATABASES,
                encodeString(databaseName),
                TABLES,
                encodeString(objectName),
                "token");
    }

    public String tableSnapshot(String databaseName, String objectName) {
        return SLASH.join(
                V1,
                prefix,
                DATABASES,
                encodeString(databaseName),
                TABLES,
                encodeString(objectName),
                "snapshot");
    }

    public String partitions(String databaseName, String objectName) {
        return SLASH.join(
                V1,
                prefix,
                DATABASES,
                encodeString(databaseName),
                TABLES,
                encodeString(objectName),
                PARTITIONS);
    }

    public String dropPartitions(String databaseName, String objectName) {
        return SLASH.join(
                V1,
                prefix,
                DATABASES,
                encodeString(databaseName),
                TABLES,
                encodeString(objectName),
                PARTITIONS,
                "drop");
    }

    public String alterPartitions(String databaseName, String objectName) {
        return SLASH.join(
                V1,
                prefix,
                DATABASES,
                encodeString(databaseName),
                TABLES,
                encodeString(objectName),
                PARTITIONS,
                "alter");
    }

    public String markDonePartitions(String databaseName, String objectName) {
        return SLASH.join(
                V1,
                prefix,
                DATABASES,
                encodeString(databaseName),
                TABLES,
                encodeString(objectName),
                PARTITIONS,
                "mark");
    }

    public String branches(String databaseName, String objectName) {
        return SLASH.join(
                V1,
                prefix,
                DATABASES,
                encodeString(databaseName),
                TABLES,
                encodeString(objectName),
                BRANCHES);
    }

    public String branch(String databaseName, String objectName, String branchName) {
        return SLASH.join(
                V1,
                prefix,
                DATABASES,
                encodeString(databaseName),
                TABLES,
                encodeString(objectName),
                BRANCHES,
                encodeString(branchName));
    }

    public String forwardBranch(String databaseName, String tableName, String branch) {
        return SLASH.join(
                V1,
                prefix,
                DATABASES,
                encodeString(databaseName),
                TABLES,
                encodeString(tableName),
                BRANCHES,
                encodeString(branch),
                "forward");
    }

    public String views(String databaseName) {
        return SLASH.join(V1, prefix, DATABASES, encodeString(databaseName), VIEWS);
    }

    public String viewDetails(String databaseName) {
        return SLASH.join(V1, prefix, DATABASES, encodeString(databaseName), VIEW_DETAILS);
    }

    public String view(String databaseName, String viewName) {
        return SLASH.join(
                V1, prefix, DATABASES, encodeString(databaseName), VIEWS, encodeString(viewName));
    }

    public String renameView() {
        return SLASH.join(V1, prefix, VIEWS, "rename");
    }
}
