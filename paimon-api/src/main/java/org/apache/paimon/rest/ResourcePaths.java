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

import org.apache.paimon.annotation.Experimental;
import org.apache.paimon.management.PermissionResource;
import org.apache.paimon.options.Options;

import org.apache.paimon.shade.guava30.com.google.common.base.Joiner;

import static org.apache.paimon.rest.RESTUtil.encodeString;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Resource paths for REST catalog. */
public class ResourcePaths {

    protected static final String V1 = "/v1";
    protected static final String DATABASES = "databases";
    protected static final String TABLES = "tables";
    protected static final String PARTITIONS = "partitions";
    protected static final String BRANCHES = "branches";
    protected static final String TAGS = "tags";
    protected static final String TREES = "trees";
    protected static final String SNAPSHOTS = "snapshots";
    protected static final String CONSUMERS = "consumers";
    protected static final String SCHEMAS = "schemas";
    protected static final String VIEWS = "views";
    protected static final String SEMANTIC_VIEWS = "semantic-views";
    protected static final String TABLE_DETAILS = "table-details";
    protected static final String VIEW_DETAILS = "view-details";
    protected static final String ROLLBACK = "rollback";
    protected static final String REGISTER = "register";
    protected static final String FUNCTIONS = "functions";
    protected static final String FUNCTION_DETAILS = "function-details";
    protected static final String PERMISSIONS = "permissions";
    protected static final String POLICIES = "policies";
    protected static final String LABELS = "labels";
    protected static final String ID = "id";

    private static final Joiner SLASH = Joiner.on("/").skipNulls();

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

    /** Labels attached to one entity, whose canonical name is encoded as a single segment. */
    @Experimental
    public String labels(String entityType, String entityName) {
        checkArgument(
                entityType != null && !entityType.trim().isEmpty(), "entityType must not be blank");
        checkArgument(
                entityName != null && !entityName.trim().isEmpty(), "entityName must not be blank");
        return SLASH.join(
                V1, prefix, LABELS, encodePathSegment(entityType), encodePathSegment(entityName));
    }

    @Experimental
    public String label(String entityType, String entityName, String key) {
        checkArgument(key != null && !key.trim().isEmpty(), "key must not be blank");
        return SLASH.join(labels(entityType, entityName), encodePathSegment(key));
    }

    private static String encodePathSegment(String value) {
        // Form encoding leaves dot segments unchanged, but they must be treated as names here.
        if (".".equals(value) || "..".equals(value)) {
            return value.replace(".", "%2E");
        }
        return encodeString(value);
    }

    /** Semantic view names are encoded as independent path segments. */
    @Experimental
    public String semanticViews(String database) {
        checkArgument(database != null && !database.trim().isEmpty(), "database must not be blank");
        DatabaseIdentifier.checkNoReference(database, "semanticViews");
        return SLASH.join(V1, prefix, DATABASES, encodePathSegment(database), SEMANTIC_VIEWS);
    }

    @Experimental
    public String semanticView(String database, String semanticView) {
        checkArgument(
                semanticView != null && !semanticView.trim().isEmpty(),
                "semanticView must not be blank");
        return SLASH.join(semanticViews(database), encodePathSegment(semanticView));
    }

    @Experimental
    public String permissions() {
        return SLASH.join(V1, prefix, PERMISSIONS);
    }

    @Experimental
    public String grantPermission() {
        return SLASH.join(permissions(), "grant");
    }

    @Experimental
    public String revokePermission() {
        return SLASH.join(permissions(), "revoke");
    }

    /** Policy collection nested below its attachment resource. */
    @Experimental
    public String policies(PermissionResource resource) {
        resource.validatePolicyAttachment();
        DatabaseIdentifier.checkNoReference(resource.getDatabase(), "policies");
        return SLASH.join(table(resource.getDatabase(), resource.getTable()), POLICIES);
    }

    /** Action endpoint for dropping one policy from its attachment resource. */
    @Experimental
    public String dropPolicy(PermissionResource resource) {
        return SLASH.join(policies(resource), "drop");
    }

    public String databases() {
        return SLASH.join(V1, prefix, DATABASES);
    }

    public String database(String databaseName) {
        DatabaseIdentifier.parse(databaseName);
        return SLASH.join(V1, prefix, DATABASES, encodeString(databaseName));
    }

    /** Database-level branches and immutable tags. */
    @Experimental
    public String databaseTrees(String databaseName) {
        DatabaseIdentifier.checkNoReference(databaseName, "tree management");
        return SLASH.join(database(databaseName), TREES);
    }

    /** One named database-level branch or immutable tag. */
    @Experimental
    public String databaseTree(String databaseName, String referenceName) {
        return SLASH.join(databaseTrees(databaseName), encodeString(referenceName));
    }

    /** Action endpoint for merging a branch or tag into a database-level branch. */
    @Experimental
    public String mergeDatabaseBranch(String databaseName, String branch) {
        return SLASH.join(databaseTree(databaseName, branch), "merge");
    }

    public String tables(String databaseName) {
        return SLASH.join(database(databaseName), TABLES);
    }

    public String tableDetails(String databaseName) {
        return SLASH.join(database(databaseName), TABLE_DETAILS);
    }

    public String tables() {
        return SLASH.join(V1, prefix, TABLES);
    }

    public String table(String tableId) {
        return SLASH.join(V1, prefix, TABLES, ID, encodeString(tableId));
    }

    public String table(String databaseName, String objectName) {
        DatabaseIdentifier.checkTableName(databaseName, objectName);
        return SLASH.join(tables(databaseName), encodeString(objectName));
    }

    public String renameTable() {
        return SLASH.join(V1, prefix, TABLES, "rename");
    }

    public String replaceTable(String databaseName, String objectName) {
        DatabaseIdentifier.checkNoReference(databaseName, "replaceTable");
        return SLASH.join(
                V1,
                prefix,
                DATABASES,
                encodeString(databaseName),
                TABLES,
                encodeString(objectName),
                "replace");
    }

    public String commitTable(String databaseName, String objectName) {
        return SLASH.join(table(databaseName, objectName), "commit");
    }

    public String rollbackTable(String databaseName, String objectName) {
        DatabaseIdentifier.checkNoReference(databaseName, "rollbackTable");
        return SLASH.join(
                V1,
                prefix,
                DATABASES,
                encodeString(databaseName),
                TABLES,
                encodeString(objectName),
                ROLLBACK);
    }

    public String rollbackSchemaTable(String databaseName, String objectName) {
        DatabaseIdentifier.checkNoReference(databaseName, "rollbackSchemaTable");
        return SLASH.join(
                V1,
                prefix,
                DATABASES,
                encodeString(databaseName),
                TABLES,
                encodeString(objectName),
                "rollback-schema");
    }

    public String registerTable(String databaseName) {
        DatabaseIdentifier.checkNoReference(databaseName, "registerTable");
        return SLASH.join(V1, prefix, DATABASES, encodeString(databaseName), REGISTER);
    }

    public String tableToken(String databaseName, String objectName) {
        return SLASH.join(table(databaseName, objectName), "token");
    }

    public String tableSnapshot(String databaseName, String objectName) {
        return SLASH.join(table(databaseName, objectName), "snapshot");
    }

    public String tableSnapshot(String databaseName, String objectName, String version) {
        return SLASH.join(snapshots(databaseName, objectName), encodeString(version));
    }

    public String snapshots(String databaseName, String objectName) {
        return SLASH.join(table(databaseName, objectName), SNAPSHOTS);
    }

    public String schemas(String databaseName, String objectName) {
        return SLASH.join(table(databaseName, objectName), SCHEMAS);
    }

    public String schemas(String databaseName, String objectName, String version) {
        return SLASH.join(schemas(databaseName, objectName), encodeString(version));
    }

    public String authTable(String databaseName, String objectName) {
        return SLASH.join(table(databaseName, objectName), "auth");
    }

    public String partitions(String databaseName, String objectName) {
        DatabaseIdentifier.checkNoReference(databaseName, "partitions");
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
        DatabaseIdentifier.checkNoReference(databaseName, "dropPartitions");
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

    public String markDonePartitions(String databaseName, String objectName) {
        DatabaseIdentifier.checkNoReference(databaseName, "markDonePartitions");
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

    public String listPartitionsByNames(String databaseName, String objectName) {
        DatabaseIdentifier.checkNoReference(databaseName, "listPartitionsByNames");
        return SLASH.join(
                V1,
                prefix,
                DATABASES,
                encodeString(databaseName),
                TABLES,
                encodeString(objectName),
                PARTITIONS,
                "list-by-names");
    }

    public String listPartitionsByFilter(String databaseName, String objectName) {
        DatabaseIdentifier.checkNoReference(databaseName, "listPartitionsByFilter");
        return SLASH.join(
                V1,
                prefix,
                DATABASES,
                encodeString(databaseName),
                TABLES,
                encodeString(objectName),
                PARTITIONS,
                "list-by-filter");
    }

    public String branches(String databaseName, String objectName) {
        DatabaseIdentifier.checkNoReference(databaseName, "branches");
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
        DatabaseIdentifier.checkNoReference(databaseName, "branch");
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
        DatabaseIdentifier.checkNoReference(databaseName, "forwardBranch");
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

    public String tags(String databaseName, String objectName) {
        DatabaseIdentifier.checkNoReference(databaseName, "tags");
        return SLASH.join(
                V1,
                prefix,
                DATABASES,
                encodeString(databaseName),
                TABLES,
                encodeString(objectName),
                TAGS);
    }

    public String consumers(String databaseName, String objectName) {
        DatabaseIdentifier.checkNoReference(databaseName, "consumers");
        return SLASH.join(
                V1,
                prefix,
                DATABASES,
                encodeString(databaseName),
                TABLES,
                encodeString(objectName),
                CONSUMERS);
    }

    public String resetConsumer(String databaseName, String objectName) {
        DatabaseIdentifier.checkNoReference(databaseName, "resetConsumer");
        return SLASH.join(
                V1,
                prefix,
                DATABASES,
                encodeString(databaseName),
                TABLES,
                encodeString(objectName),
                CONSUMERS,
                "reset");
    }

    public String tag(String databaseName, String objectName, String tagName) {
        DatabaseIdentifier.checkNoReference(databaseName, "tag");
        return SLASH.join(
                V1,
                prefix,
                DATABASES,
                encodeString(databaseName),
                TABLES,
                encodeString(objectName),
                TAGS,
                encodeString(tagName));
    }

    public String views(String databaseName) {
        DatabaseIdentifier.checkNoReference(databaseName, "views");
        return SLASH.join(V1, prefix, DATABASES, encodeString(databaseName), VIEWS);
    }

    public String viewDetails(String databaseName) {
        DatabaseIdentifier.checkNoReference(databaseName, "viewDetails");
        return SLASH.join(V1, prefix, DATABASES, encodeString(databaseName), VIEW_DETAILS);
    }

    public String views() {
        return SLASH.join(V1, prefix, VIEWS);
    }

    public String view(String databaseName, String viewName) {
        DatabaseIdentifier.checkNoReference(databaseName, "view");
        return SLASH.join(
                V1, prefix, DATABASES, encodeString(databaseName), VIEWS, encodeString(viewName));
    }

    public String renameView() {
        return SLASH.join(V1, prefix, VIEWS, "rename");
    }

    public String functions(String databaseName) {
        DatabaseIdentifier.checkNoReference(databaseName, "functions");
        return SLASH.join(V1, prefix, DATABASES, encodeString(databaseName), FUNCTIONS);
    }

    public String functions() {
        return SLASH.join(V1, prefix, FUNCTIONS);
    }

    public String functionDetails(String databaseName) {
        DatabaseIdentifier.checkNoReference(databaseName, "functionDetails");
        return SLASH.join(V1, prefix, DATABASES, encodeString(databaseName), FUNCTION_DETAILS);
    }

    public String function(String databaseName, String functionName) {
        DatabaseIdentifier.checkNoReference(databaseName, "function");
        return SLASH.join(
                V1,
                prefix,
                DATABASES,
                encodeString(databaseName),
                FUNCTIONS,
                encodeString(functionName));
    }
}
