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

"""Maps a REST request to the POP action it is registered under in DlfNext 2026-01-18."""

from typing import Dict, List, Optional, Tuple

# Method, path template and action; a {name} segment matches any one segment.
OPERATIONS: Tuple[Tuple[str, str, str], ...] = (
    ("GET", "/v1/config", "GetConfig"),
    ("GET", "/v1/{prefix}/databases", "ListDatabases"),
    ("POST", "/v1/{prefix}/databases", "CreateDatabase"),
    ("GET", "/v1/{prefix}/databases/{database}", "GetDatabase"),
    ("POST", "/v1/{prefix}/databases/{database}", "AlterDatabase"),
    ("DELETE", "/v1/{prefix}/databases/{database}", "DropDatabase"),
    ("POST", "/v1/{prefix}/databases/{database}/register", "RegisterTable"),
    ("GET", "/v1/{prefix}/databases/{database}/tables", "ListTables"),
    ("POST", "/v1/{prefix}/databases/{database}/tables", "CreateTable"),
    ("GET", "/v1/{prefix}/databases/{database}/table-details", "ListTableDetails"),
    ("GET", "/v1/{prefix}/databases/{database}/tables/{table}", "GetTable"),
    ("POST", "/v1/{prefix}/databases/{database}/tables/{table}", "AlterTable"),
    ("DELETE", "/v1/{prefix}/databases/{database}/tables/{table}", "DropTable"),
    ("POST", "/v1/{prefix}/tables/rename", "RenameTable"),
    ("POST", "/v1/{prefix}/databases/{database}/tables/{table}/auth", "AuthTableQuery"),
    ("POST", "/v1/{prefix}/databases/{database}/tables/{table}/commit", "CommitTable"),
    ("GET", "/v1/{prefix}/databases/{database}/tables/{table}/token", "GetTableToken"),
    ("POST", "/v1/{prefix}/databases/{database}/tables/{table}/rollback", "RollbackToSnapshot"),
    ("GET", "/v1/{prefix}/databases/{database}/tables/{table}/snapshot", "GetTableSnapshot"),
    ("GET", "/v1/{prefix}/databases/{database}/tables/{table}/snapshots", "ListSnapshots"),
    ("GET", "/v1/{prefix}/databases/{database}/tables/{table}/snapshots/{version}", "GetVersionSnapshot"),
    ("GET", "/v1/{prefix}/databases/{database}/tables/{table}/partitions", "ListPartitions"),
    ("POST", "/v1/{prefix}/databases/{database}/tables/{table}/partitions", "CreatePartitions"),
    ("POST", "/v1/{prefix}/databases/{database}/tables/{table}/partitions/drop", "DropPartitions"),
    ("POST", "/v1/{prefix}/databases/{database}/tables/{table}/partitions/mark", "MarkDonePartitions"),
    ("POST", "/v1/{prefix}/databases/{database}/tables/{table}/partitions/list-by-names",
     "ListPartitionsByNames"),
    ("POST", "/v1/{prefix}/databases/{database}/tables/{table}/partitions/list-by-filter",
     "ListPartitionsByFilter"),
    ("GET", "/v1/{prefix}/databases/{database}/tables/{table}/branches", "ListBranches"),
    ("POST", "/v1/{prefix}/databases/{database}/tables/{table}/branches", "CreateBranch"),
    ("DELETE", "/v1/{prefix}/databases/{database}/tables/{table}/branches/{branch}", "DropBranch"),
    ("POST", "/v1/{prefix}/databases/{database}/tables/{table}/branches/{branch}/forward",
     "FastForwardBranch"),
    ("GET", "/v1/{prefix}/databases/{database}/tables/{table}/tags", "ListTags"),
    ("POST", "/v1/{prefix}/databases/{database}/tables/{table}/tags", "CreateTag"),
    ("GET", "/v1/{prefix}/databases/{database}/tables/{table}/tags/{tag}", "GetTag"),
    ("DELETE", "/v1/{prefix}/databases/{database}/tables/{table}/tags/{tag}", "DropTag"),
    ("GET", "/v1/{prefix}/databases/{database}/tables/{table}/policies", "ListPolicies"),
    ("POST", "/v1/{prefix}/databases/{database}/tables/{table}/policies", "CreatePolicy"),
    ("POST", "/v1/{prefix}/databases/{database}/tables/{table}/policies/drop", "DropPolicy"),
    ("GET", "/v1/{prefix}/databases/{database}/views", "ListViews"),
    ("POST", "/v1/{prefix}/databases/{database}/views", "CreateView"),
    ("GET", "/v1/{prefix}/databases/{database}/view-details", "ListViewDetails"),
    ("GET", "/v1/{prefix}/databases/{database}/views/{view}", "GetView"),
    ("POST", "/v1/{prefix}/databases/{database}/views/{view}", "AlterView"),
    ("DELETE", "/v1/{prefix}/databases/{database}/views/{view}", "DropView"),
    ("POST", "/v1/{prefix}/views/rename", "RenameView"),
    ("GET", "/v1/{prefix}/databases/{database}/functions", "ListFunctions"),
    ("POST", "/v1/{prefix}/databases/{database}/functions", "CreateFunction"),
    ("GET", "/v1/{prefix}/databases/{database}/functions/{function}", "GetFunction"),
    ("POST", "/v1/{prefix}/databases/{database}/functions/{function}", "AlterFunction"),
    ("DELETE", "/v1/{prefix}/databases/{database}/functions/{function}", "DropFunction"),
    ("GET", "/v1/{prefix}/permissions", "ListPermissionAssignments"),
    ("POST", "/v1/{prefix}/permissions/grant", "GrantPermissionAssignment"),
    ("POST", "/v1/{prefix}/permissions/revoke", "RevokePermissionAssignment"),
)


def _index() -> Dict[str, List[Tuple[List[str], str]]]:
    by_method: Dict[str, List[Tuple[List[str], str]]] = {}
    for method, template, action in OPERATIONS:
        by_method.setdefault(method, []).append((template.split("/"), action))
    return by_method


_BY_METHOD = _index()


def resolve_action(method: Optional[str], path: Optional[str]) -> Optional[str]:
    """Returns the action for the request, or None when no operation is registered for it."""
    if method is None or path is None:
        return None
    segments = path.split("/")
    for template, action in _BY_METHOD.get(method.upper(), []):
        if len(template) == len(segments) and all(
                segment != "" if part.startswith("{") else part == segment
                for part, segment in zip(template, segments)):
            return action
    return None
