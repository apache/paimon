# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


class RESTApiNames:
    """Names of the APIs RESTApi calls. Where DLF OpenAPI (DlfNext 2026-01-18) registers an API,
    the name is the one it is registered under. Mirrors the Java RESTApiNames."""

    GET_CONFIG = "GetConfig"

    LIST_DATABASES = "ListDatabases"
    CREATE_DATABASE = "CreateDatabase"
    GET_DATABASE = "GetDatabase"
    ALTER_DATABASE = "AlterDatabase"
    DROP_DATABASE = "DropDatabase"

    LIST_TABLES = "ListTables"
    LIST_TABLE_DETAILS = "ListTableDetails"
    LIST_TABLES_GLOBALLY = "ListTablesGlobally"
    CREATE_TABLE = "CreateTable"
    REGISTER_TABLE = "RegisterTable"
    GET_TABLE = "GetTable"
    GET_TABLE_BY_ID = "GetTableById"
    ALTER_TABLE = "AlterTable"
    REPLACE_TABLE = "ReplaceTable"
    RENAME_TABLE = "RenameTable"
    DROP_TABLE = "DropTable"
    AUTH_TABLE_QUERY = "AuthTableQuery"
    GET_TABLE_TOKEN = "GetTableToken"

    COMMIT_TABLE = "CommitTable"
    GET_TABLE_SNAPSHOT = "GetTableSnapshot"
    GET_VERSION_SNAPSHOT = "GetVersionSnapshot"
    LIST_SNAPSHOTS = "ListSnapshots"
    ROLLBACK_TO_SNAPSHOT = "RollbackToSnapshot"
    GET_SCHEMA = "GetSchema"
    LIST_SCHEMAS = "ListSchemas"
    ROLLBACK_SCHEMA = "RollbackSchema"
    LIST_CONSUMERS = "ListConsumers"
    RESET_CONSUMER = "ResetConsumer"

    LIST_PARTITIONS = "ListPartitions"
    CREATE_PARTITIONS = "CreatePartitions"
    DROP_PARTITIONS = "DropPartitions"
    MARK_DONE_PARTITIONS = "MarkDonePartitions"
    LIST_PARTITIONS_BY_NAMES = "ListPartitionsByNames"
    LIST_PARTITIONS_BY_FILTER = "ListPartitionsByFilter"

    LIST_BRANCHES = "ListBranches"
    CREATE_BRANCH = "CreateBranch"
    DROP_BRANCH = "DropBranch"
    FAST_FORWARD_BRANCH = "FastForwardBranch"
    RENAME_BRANCH = "RenameBranch"

    LIST_TAGS = "ListTags"
    CREATE_TAG = "CreateTag"
    GET_TAG = "GetTag"
    DROP_TAG = "DropTag"

    LIST_VIEWS = "ListViews"
    LIST_VIEW_DETAILS = "ListViewDetails"
    LIST_VIEWS_GLOBALLY = "ListViewsGlobally"
    CREATE_VIEW = "CreateView"
    GET_VIEW = "GetView"
    ALTER_VIEW = "AlterView"
    RENAME_VIEW = "RenameView"
    DROP_VIEW = "DropView"

    LIST_FUNCTIONS = "ListFunctions"
    LIST_FUNCTION_DETAILS = "ListFunctionDetails"
    LIST_FUNCTIONS_GLOBALLY = "ListFunctionsGlobally"
    CREATE_FUNCTION = "CreateFunction"
    GET_FUNCTION = "GetFunction"
    ALTER_FUNCTION = "AlterFunction"
    DROP_FUNCTION = "DropFunction"

    LIST_PERMISSION_ASSIGNMENTS = "ListPermissionAssignments"
    GRANT_PERMISSION_ASSIGNMENT = "GrantPermissionAssignment"
    REVOKE_PERMISSION_ASSIGNMENT = "RevokePermissionAssignment"
    LIST_POLICIES = "ListPolicies"
    CREATE_POLICY = "CreatePolicy"
    DROP_POLICY = "DropPolicy"

    LIST_LABELS = "ListLabels"
    GET_LABEL = "GetLabel"
    UPSERT_LABEL = "UpsertLabel"
    DELETE_LABEL = "DeleteLabel"

    LIST_SEMANTIC_VIEWS = "ListSemanticViews"
    GET_SEMANTIC_VIEW = "GetSemanticView"
    UPSERT_SEMANTIC_VIEW = "UpsertSemanticView"
    DELETE_SEMANTIC_VIEW = "DeleteSemanticView"
