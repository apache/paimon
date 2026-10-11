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
from typing import Dict, List, Optional

from pypaimon.common.identifier import Identifier
from pypaimon.common.json_util import json_field
from pypaimon.function.function_change import FunctionChange
from pypaimon.function.function_definition import FunctionDefinition
from pypaimon.management.column_mask import ColumnMask
from pypaimon.management.data_policy import DataPolicy
from pypaimon.management.java_string import is_blank
from pypaimon.management.permission_access import PermissionAccess
from pypaimon.management.permission_assignment import PermissionAssignment
from pypaimon.management.permission_columns import PermissionColumns
from pypaimon.management.permission_resource import PermissionResource
from pypaimon.management.policy_type import PolicyType
from pypaimon.management.row_filter import RowFilter
from pypaimon.schema.data_types import DataField
from pypaimon.schema.schema import Schema
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.snapshot.snapshot_commit import PartitionStatistics
from pypaimon.table.instant import Instant


class RESTRequest(ABC):
    """RESTRequest"""


@dataclass
class CreateDatabaseRequest(RESTRequest):
    FIELD_NAME = "name"
    FIELD_OPTIONS = "options"

    name: str = json_field(FIELD_NAME)
    options: Optional[Dict[str, str]] = json_field(FIELD_OPTIONS)


@dataclass
class AlterDatabaseRequest(RESTRequest):
    FIELD_REMOVALS = "removals"
    FIELD_UPDATES = "updates"

    removals: List[str] = json_field(FIELD_REMOVALS)
    updates: Dict[str, str] = json_field(FIELD_UPDATES)


@dataclass
class RenameTableRequest(RESTRequest):
    FIELD_SOURCE = "source"
    FIELD_DESTINATION = "destination"

    source: Identifier = json_field(FIELD_SOURCE)
    destination: Identifier = json_field(FIELD_DESTINATION)


@dataclass
class CreateTableRequest(RESTRequest):
    FIELD_IDENTIFIER = "identifier"
    FIELD_SCHEMA = "schema"

    identifier: Identifier = json_field(FIELD_IDENTIFIER)
    schema: Schema = json_field(FIELD_SCHEMA)


@dataclass
class CommitTableRequest(RESTRequest):
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
    FIELD_CHANGES = "changes"

    changes: List[SchemaChange] = json_field(FIELD_CHANGES)


@dataclass
class RollbackTableRequest(RESTRequest):
    FIELD_INSTANT = "instant"
    FIELD_FROM_SNAPSHOT = "fromSnapshot"

    instant: Instant = json_field(FIELD_INSTANT)
    from_snapshot: Optional[int] = json_field(FIELD_FROM_SNAPSHOT)


@dataclass
class CreateFunctionRequest(RESTRequest):
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
    FIELD_TAG_NAME = "tagName"
    FIELD_SNAPSHOT_ID = "snapshotId"
    FIELD_TIME_RETAINED = "timeRetained"

    tag_name: str = json_field(FIELD_TAG_NAME)
    snapshot_id: Optional[int] = json_field(FIELD_SNAPSHOT_ID, default=None)
    time_retained: Optional[str] = json_field(FIELD_TIME_RETAINED, default=None)


@dataclass
class CreatePartitionsRequest(RESTRequest):
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
    FIELD_BRANCH = "branch"
    FIELD_FROM_TAG = "fromTag"

    branch: str = json_field(FIELD_BRANCH)
    from_tag: Optional[str] = json_field(FIELD_FROM_TAG, default=None)


@dataclass
class RenameBranchRequest(RESTRequest):
    FIELD_TO_BRANCH = "toBranch"

    to_branch: str = json_field(FIELD_TO_BRANCH)


@dataclass
class ForwardBranchRequest(RESTRequest):
    """Empty body request; serializes to ``{}`` per Java ForwardBranchRequest."""
    pass


class GrantPermissionRequest(RESTRequest):

    def __init__(self, assignment: PermissionAssignment):
        self._assignment = assignment

    def assignment(self) -> PermissionAssignment:
        return self._assignment

    def get_resource(self) -> PermissionResource:
        return self._assignment.get_resource()

    def get_access(self) -> str:
        return self._assignment.get_access()

    def get_principal(self) -> str:
        return self._assignment.get_principal()

    def get_columns(self) -> Optional[PermissionColumns]:
        return self._assignment.get_columns()

    def get_expire_time(self) -> Optional[str]:
        return self._assignment.get_expire_time()

    def to_dict(self) -> dict:
        return self._assignment.to_dict()

    @classmethod
    def from_dict(cls, data: dict) -> "GrantPermissionRequest":
        resource = data.get(PermissionAssignment.FIELD_RESOURCE)
        columns = data.get(PermissionAssignment.FIELD_COLUMNS)
        return cls(PermissionAssignment(
            None if resource is None else PermissionResource.from_dict(resource),
            data.get(PermissionAssignment.FIELD_ACCESS),
            data.get(PermissionAssignment.FIELD_PRINCIPAL),
            None if columns is None else PermissionColumns.from_dict(columns),
            data.get(PermissionAssignment.FIELD_EXPIRE_TIME)))


class RevokePermissionRequest(RESTRequest):

    FIELD_RESOURCE = "resource"
    FIELD_ACCESS = "access"
    FIELD_PRINCIPAL = "principal"

    def __init__(self, resource: PermissionResource, access: str, principal: str):
        self._resource = resource
        self._access = PermissionAccess.canonicalize_for(resource, access)
        self._principal = PermissionAssignment.validate_principal(principal)

    def get_resource(self) -> PermissionResource:
        return self._resource

    def get_access(self) -> str:
        return self._access

    def get_principal(self) -> str:
        return self._principal

    def to_dict(self) -> dict:
        return {
            self.FIELD_RESOURCE: self._resource.to_dict(),
            self.FIELD_ACCESS: self._access,
            self.FIELD_PRINCIPAL: self._principal,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "RevokePermissionRequest":
        resource = data.get(cls.FIELD_RESOURCE)
        return cls(None if resource is None else PermissionResource.from_dict(resource),
                   data.get(cls.FIELD_ACCESS), data.get(cls.FIELD_PRINCIPAL))


class PolicyRequest(RESTRequest):
    FIELD_ROW_FILTER = "rowFilter"
    FIELD_COLUMN_MASK = "columnMask"
    FIELD_PRINCIPAL = "principal"

    def __init__(self, row_filter: Optional[RowFilter], column_mask: Optional[ColumnMask],
                 principal: str):
        self._row_filter = row_filter
        self._column_mask = column_mask
        self._principal = principal

    @classmethod
    def from_policy(cls, policy: DataPolicy) -> "PolicyRequest":
        return cls(policy.get_row_filter(), policy.get_column_mask(), policy.get_principal())

    def policy(self, resource: PermissionResource) -> DataPolicy:
        return DataPolicy(resource, self._row_filter, self._column_mask, self._principal)

    def get_row_filter(self) -> Optional[RowFilter]:
        return self._row_filter

    def get_column_mask(self) -> Optional[ColumnMask]:
        return self._column_mask

    def get_principal(self) -> str:
        return self._principal

    def to_dict(self) -> dict:
        result = {}
        if self._row_filter is not None:
            result[self.FIELD_ROW_FILTER] = self._row_filter.to_dict()
        if self._column_mask is not None:
            result[self.FIELD_COLUMN_MASK] = self._column_mask.to_dict()
        result[self.FIELD_PRINCIPAL] = self._principal
        return result

    @classmethod
    def from_dict(cls, data: dict) -> "PolicyRequest":
        row_filter = data.get(cls.FIELD_ROW_FILTER)
        column_mask = data.get(cls.FIELD_COLUMN_MASK)
        return cls(None if row_filter is None else RowFilter.from_dict(row_filter),
                   None if column_mask is None else ColumnMask.from_dict(column_mask),
                   data.get(cls.FIELD_PRINCIPAL))


class DropPolicyRequest(RESTRequest):
    FIELD_TYPE = "type"
    FIELD_PRINCIPAL = "principal"
    FIELD_COLUMN = "column"

    def __init__(self, policy_type: PolicyType, principal: str, column: Optional[str] = None):
        if policy_type is None:
            raise ValueError("policy type cannot be null")
        self._type = policy_type
        self._principal = PermissionAssignment.validate_principal(principal)
        if policy_type == PolicyType.ROW_FILTER:
            if not is_blank(column):
                raise ValueError("ROW_FILTER identity cannot contain a column.")
            self._column = None
        else:
            if is_blank(column):
                raise ValueError("column is required for COLUMN_MASKING identity.")
            self._column = column

    def get_type(self) -> PolicyType:
        return self._type

    def get_principal(self) -> str:
        return self._principal

    def get_column(self) -> Optional[str]:
        return self._column

    def to_dict(self) -> dict:
        result = {self.FIELD_TYPE: self._type.name, self.FIELD_PRINCIPAL: self._principal}
        if self._column is not None:
            result[self.FIELD_COLUMN] = self._column
        return result

    @classmethod
    def from_dict(cls, data: dict) -> "DropPolicyRequest":
        policy_type = data.get(cls.FIELD_TYPE)
        # Jackson matches enum names exactly.
        if policy_type is not None and policy_type not in PolicyType.__members__:
            raise ValueError("Unknown policy type '{}'.".format(policy_type))
        return cls(None if policy_type is None else PolicyType[policy_type],
                   data.get(cls.FIELD_PRINCIPAL), data.get(cls.FIELD_COLUMN))
