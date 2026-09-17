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

from abc import ABC
from dataclasses import dataclass
from typing import ClassVar, Dict, List, Optional

from pypaimon.common.identifier import Identifier
from pypaimon.common.json_util import json_field
from pypaimon.function.function_change import FunctionChange
from pypaimon.function.function_definition import FunctionDefinition
from pypaimon.schema.data_types import DataField
from pypaimon.schema.schema import Schema
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.snapshot.snapshot_commit import PartitionStatistics
from pypaimon.table.instant import Instant


class RESTRequest(ABC):
    """RESTRequest"""

    # The API this request calls, which signers may send with it; None if it names none.
    API_NAME: ClassVar[Optional[str]] = None

    def api_name(self) -> str:
        """The API this request calls, from its API_NAME."""
        if self.API_NAME is None:
            raise NotImplementedError("%s does not name the API it calls" % type(self).__name__)
        return self.API_NAME


@dataclass
class CreateDatabaseRequest(RESTRequest):
    API_NAME = "CreateDatabase"

    FIELD_NAME = "name"
    FIELD_OPTIONS = "options"

    name: str = json_field(FIELD_NAME)
    options: Dict[str, str] = json_field(FIELD_OPTIONS)


@dataclass
class AlterDatabaseRequest(RESTRequest):
    API_NAME = "AlterDatabase"

    FIELD_REMOVALS = "removals"
    FIELD_UPDATES = "updates"

    removals: List[str] = json_field(FIELD_REMOVALS)
    updates: Dict[str, str] = json_field(FIELD_UPDATES)


@dataclass
class RenameTableRequest(RESTRequest):
    API_NAME = "RenameTable"

    FIELD_SOURCE = "source"
    FIELD_DESTINATION = "destination"

    source: Identifier = json_field(FIELD_SOURCE)
    destination: Identifier = json_field(FIELD_DESTINATION)


@dataclass
class CreateTableRequest(RESTRequest):
    API_NAME = "CreateTable"

    FIELD_IDENTIFIER = "identifier"
    FIELD_SCHEMA = "schema"

    identifier: Identifier = json_field(FIELD_IDENTIFIER)
    schema: Schema = json_field(FIELD_SCHEMA)


@dataclass
class CommitTableRequest(RESTRequest):
    API_NAME = "CommitTable"

    FIELD_TABLE_ID = "tableId"
    FIELD_BASE_SNAPSHOT_UUID = "baseSnapshotUuid"
    FIELD_SNAPSHOT = "snapshot"
    FIELD_STATISTICS = "statistics"

    table_id: Optional[str] = json_field(FIELD_TABLE_ID)
    snapshot: Snapshot = json_field(FIELD_SNAPSHOT)
    statistics: List[PartitionStatistics] = json_field(FIELD_STATISTICS)
    base_snapshot_uuid: Optional[str] = json_field(
        FIELD_BASE_SNAPSHOT_UUID, default=None
    )


@dataclass
class AlterTableRequest(RESTRequest):
    API_NAME = "AlterTable"

    FIELD_CHANGES = "changes"

    changes: List[SchemaChange] = json_field(FIELD_CHANGES)


@dataclass
class RollbackTableRequest(RESTRequest):
    API_NAME = "RollbackToSnapshot"

    FIELD_INSTANT = "instant"
    FIELD_FROM_SNAPSHOT = "fromSnapshot"

    instant: Instant = json_field(FIELD_INSTANT)
    from_snapshot: Optional[int] = json_field(FIELD_FROM_SNAPSHOT)


@dataclass
class CreateFunctionRequest(RESTRequest):
    API_NAME = "CreateFunction"

    FIELD_NAME = "name"
    FIELD_INPUT_PARAMS = "inputParams"
    FIELD_RETURN_PARAMS = "returnParams"
    FIELD_DETERMINISTIC = "deterministic"
    FIELD_DEFINITIONS = "definitions"
    FIELD_COMMENT = "comment"
    FIELD_OPTIONS = "options"

    name: str = json_field(FIELD_NAME)
    input_params: Optional[List[DataField]] = json_field(FIELD_INPUT_PARAMS, default=None)
    return_params: Optional[List[DataField]] = json_field(FIELD_RETURN_PARAMS, default=None)
    deterministic: bool = json_field(FIELD_DETERMINISTIC, default=False)
    definitions: Optional[Dict[str, FunctionDefinition]] = json_field(FIELD_DEFINITIONS, default=None)
    comment: Optional[str] = json_field(FIELD_COMMENT, default=None)
    options: Optional[Dict[str, str]] = json_field(FIELD_OPTIONS, default=None)

    def to_dict(self) -> Dict:
        result = {
            self.FIELD_NAME: self.name,
            self.FIELD_DETERMINISTIC: self.deterministic,
        }
        if self.input_params is not None:
            result[self.FIELD_INPUT_PARAMS] = [
                p.to_dict() if hasattr(p, 'to_dict') else p for p in self.input_params
            ]
        else:
            result[self.FIELD_INPUT_PARAMS] = None
        if self.return_params is not None:
            result[self.FIELD_RETURN_PARAMS] = [
                p.to_dict() if hasattr(p, 'to_dict') else p for p in self.return_params
            ]
        else:
            result[self.FIELD_RETURN_PARAMS] = None
        if self.definitions is not None:
            result[self.FIELD_DEFINITIONS] = {
                k: v.to_dict() if hasattr(v, 'to_dict') else v
                for k, v in self.definitions.items()
            }
        else:
            result[self.FIELD_DEFINITIONS] = None
        result[self.FIELD_COMMENT] = self.comment
        result[self.FIELD_OPTIONS] = self.options
        return result


@dataclass
class AlterFunctionRequest(RESTRequest):
    API_NAME = "AlterFunction"

    FIELD_CHANGES = "changes"

    changes: List[FunctionChange] = json_field(FIELD_CHANGES)

    def to_dict(self) -> Dict:
        return {
            self.FIELD_CHANGES: [c.to_dict() for c in self.changes]
        }


# Wire DTO for ``POST /databases/{db}/tables/{tbl}/tags``. Mirrors Java
# ``CreateTagRequest`` (paimon-api/.../rest/requests/CreateTagRequest.java) — only
# three fields are serialized. ``ignoreIfExists`` is intentionally NOT included
# here; it is a client-side flag handled by ``RESTCatalog.create_tag``, not part
# of the wire format.
@dataclass
class CreateTagRequest(RESTRequest):
    API_NAME = "CreateTag"

    FIELD_TAG_NAME = "tagName"
    FIELD_SNAPSHOT_ID = "snapshotId"
    FIELD_TIME_RETAINED = "timeRetained"

    tag_name: str = json_field(FIELD_TAG_NAME)
    snapshot_id: Optional[int] = json_field(FIELD_SNAPSHOT_ID, default=None)
    time_retained: Optional[str] = json_field(FIELD_TIME_RETAINED, default=None)


@dataclass
class CreatePartitionsRequest(RESTRequest):
    API_NAME = "CreatePartitions"

    FIELD_PARTITION_SPECS = "partitionSpecs"
    FIELD_IGNORE_IF_EXISTS = "ignoreIfExists"

    partition_specs: List[Dict[str, str]] = json_field(FIELD_PARTITION_SPECS)
    ignore_if_exists: Optional[bool] = json_field(FIELD_IGNORE_IF_EXISTS, default=True)

    def __post_init__(self):
        if self.ignore_if_exists is None:
            self.ignore_if_exists = True


# Branch CRUD wire DTOs. Mirrors Java requests in
# paimon-api/.../rest/requests/.
@dataclass
class CreateBranchRequest(RESTRequest):
    API_NAME = "CreateBranch"

    FIELD_BRANCH = "branch"
    FIELD_FROM_TAG = "fromTag"

    branch: str = json_field(FIELD_BRANCH)
    from_tag: Optional[str] = json_field(FIELD_FROM_TAG, default=None)


@dataclass
class RenameBranchRequest(RESTRequest):
    API_NAME = "RenameBranch"

    FIELD_TO_BRANCH = "toBranch"

    to_branch: str = json_field(FIELD_TO_BRANCH)


@dataclass
class ForwardBranchRequest(RESTRequest):
    """Empty body request; serializes to ``{}`` per Java ForwardBranchRequest."""
    API_NAME = "FastForwardBranch"


# Names the APIs whose requests are sent without a body.
class GetConfigRequest:
    API_NAME = "GetConfig"


class ListDatabasesRequest:
    API_NAME = "ListDatabases"


class GetDatabaseRequest:
    API_NAME = "GetDatabase"


class DropDatabaseRequest:
    API_NAME = "DropDatabase"


class ListTablesRequest:
    API_NAME = "ListTables"


class GetTableRequest:
    API_NAME = "GetTable"


class DropTableRequest:
    API_NAME = "DropTable"


class GetTableTokenRequest:
    API_NAME = "GetTableToken"


class GetTableSnapshotRequest:
    API_NAME = "GetTableSnapshot"


class ListPartitionsRequest:
    API_NAME = "ListPartitions"


class ListBranchesRequest:
    API_NAME = "ListBranches"


class DropBranchRequest:
    API_NAME = "DropBranch"


class ListTagsRequest:
    API_NAME = "ListTags"


class GetTagRequest:
    API_NAME = "GetTag"


class DropTagRequest:
    API_NAME = "DropTag"


class ListFunctionsRequest:
    API_NAME = "ListFunctions"


class ListFunctionDetailsRequest:
    API_NAME = "ListFunctionDetails"


class ListFunctionsGloballyRequest:
    API_NAME = "ListFunctionsGlobally"


class GetFunctionRequest:
    API_NAME = "GetFunction"


class DropFunctionRequest:
    API_NAME = "DropFunction"
