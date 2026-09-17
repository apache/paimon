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

/**
 * Names of the APIs {@link RESTApi} calls. Where DLF OpenAPI (DlfNext 2026-01-18) registers an API,
 * the name is the one it is registered under.
 */
public final class RESTApiNames {

    public static final String GET_CONFIG = "GetConfig";

    public static final String LIST_DATABASES = "ListDatabases";
    public static final String CREATE_DATABASE = "CreateDatabase";
    public static final String GET_DATABASE = "GetDatabase";
    public static final String ALTER_DATABASE = "AlterDatabase";
    public static final String DROP_DATABASE = "DropDatabase";

    public static final String LIST_TABLES = "ListTables";
    public static final String LIST_TABLE_DETAILS = "ListTableDetails";
    public static final String LIST_TABLES_GLOBALLY = "ListTablesGlobally";
    public static final String CREATE_TABLE = "CreateTable";
    public static final String REGISTER_TABLE = "RegisterTable";
    public static final String GET_TABLE = "GetTable";
    public static final String GET_TABLE_BY_ID = "GetTableById";
    public static final String ALTER_TABLE = "AlterTable";
    public static final String REPLACE_TABLE = "ReplaceTable";
    public static final String RENAME_TABLE = "RenameTable";
    public static final String DROP_TABLE = "DropTable";
    public static final String AUTH_TABLE_QUERY = "AuthTableQuery";
    public static final String GET_TABLE_TOKEN = "GetTableToken";

    public static final String COMMIT_TABLE = "CommitTable";
    public static final String GET_TABLE_SNAPSHOT = "GetTableSnapshot";
    public static final String GET_VERSION_SNAPSHOT = "GetVersionSnapshot";
    public static final String LIST_SNAPSHOTS = "ListSnapshots";
    public static final String ROLLBACK_TO_SNAPSHOT = "RollbackToSnapshot";
    public static final String GET_SCHEMA = "GetSchema";
    public static final String LIST_SCHEMAS = "ListSchemas";
    public static final String ROLLBACK_SCHEMA = "RollbackSchema";
    public static final String LIST_CONSUMERS = "ListConsumers";
    public static final String RESET_CONSUMER = "ResetConsumer";

    public static final String LIST_PARTITIONS = "ListPartitions";
    public static final String CREATE_PARTITIONS = "CreatePartitions";
    public static final String DROP_PARTITIONS = "DropPartitions";
    public static final String MARK_DONE_PARTITIONS = "MarkDonePartitions";
    public static final String LIST_PARTITIONS_BY_NAMES = "ListPartitionsByNames";
    public static final String LIST_PARTITIONS_BY_FILTER = "ListPartitionsByFilter";

    public static final String LIST_BRANCHES = "ListBranches";
    public static final String CREATE_BRANCH = "CreateBranch";
    public static final String DROP_BRANCH = "DropBranch";
    public static final String FAST_FORWARD_BRANCH = "FastForwardBranch";

    public static final String LIST_TAGS = "ListTags";
    public static final String CREATE_TAG = "CreateTag";
    public static final String GET_TAG = "GetTag";
    public static final String DROP_TAG = "DropTag";

    public static final String LIST_VIEWS = "ListViews";
    public static final String LIST_VIEW_DETAILS = "ListViewDetails";
    public static final String LIST_VIEWS_GLOBALLY = "ListViewsGlobally";
    public static final String CREATE_VIEW = "CreateView";
    public static final String GET_VIEW = "GetView";
    public static final String ALTER_VIEW = "AlterView";
    public static final String RENAME_VIEW = "RenameView";
    public static final String DROP_VIEW = "DropView";

    public static final String LIST_FUNCTIONS = "ListFunctions";
    public static final String LIST_FUNCTION_DETAILS = "ListFunctionDetails";
    public static final String LIST_FUNCTIONS_GLOBALLY = "ListFunctionsGlobally";
    public static final String CREATE_FUNCTION = "CreateFunction";
    public static final String GET_FUNCTION = "GetFunction";
    public static final String ALTER_FUNCTION = "AlterFunction";
    public static final String DROP_FUNCTION = "DropFunction";

    public static final String LIST_PERMISSION_ASSIGNMENTS = "ListPermissionAssignments";
    public static final String GRANT_PERMISSION_ASSIGNMENT = "GrantPermissionAssignment";
    public static final String REVOKE_PERMISSION_ASSIGNMENT = "RevokePermissionAssignment";
    public static final String LIST_POLICIES = "ListPolicies";
    public static final String CREATE_POLICY = "CreatePolicy";
    public static final String DROP_POLICY = "DropPolicy";

    public static final String LIST_LABELS = "ListLabels";
    public static final String GET_LABEL = "GetLabel";
    public static final String UPSERT_LABEL = "UpsertLabel";
    public static final String DELETE_LABEL = "DeleteLabel";

    public static final String LIST_SEMANTIC_VIEWS = "ListSemanticViews";
    public static final String GET_SEMANTIC_VIEW = "GetSemanticView";
    public static final String UPSERT_SEMANTIC_VIEW = "UpsertSemanticView";
    public static final String DELETE_SEMANTIC_VIEW = "DeleteSemanticView";

    private RESTApiNames() {}
}
