"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Generic, List, Optional

from pypaimon.common.identifier import Identifier
from pypaimon.common.json_util import T, json_field
from pypaimon.common.options import Options
from pypaimon.schema.data_types import DataField
from pypaimon.schema.schema import Schema
from pypaimon.snapshot.snapshot_commit import PartitionStatistics
from pypaimon.snapshot.table_snapshot import TableSnapshot


@dataclass
class PagedList(Generic[T]):
    elements: List[T]
    next_page_token: Optional[str] = None


class RESTResponse(ABC):
    """RESTResponse"""


@dataclass
class ErrorResponse(RESTResponse):
    """Error response"""
    RESOURCE_TYPE_DATABASE = "DATABASE"
    RESOURCE_TYPE_TABLE = "TABLE"
    RESOURCE_TYPE_VIEW = "VIEW"
    RESOURCE_TYPE_FUNCTION = "FUNCTION"
    RESOURCE_TYPE_COLUMN = "COLUMN"
    RESOURCE_TYPE_SNAPSHOT = "SNAPSHOT"
    RESOURCE_TYPE_TAG = "TAG"
    RESOURCE_TYPE_BRANCH = "BRANCH"
    RESOURCE_TYPE_DEFINITION = "DEFINITION"
    RESOURCE_TYPE_DIALECT = "DIALECT"

    resource_type: Optional[str] = json_field("resourceType", default=None)
    resource_name: Optional[str] = json_field("resourceName", default=None)
    message: Optional[str] = json_field("message", default=None)
    code: Optional[int] = json_field("code", default=None)

    def __init__(
            self,
            resource_type: Optional[str] = None,
            resource_name: Optional[str] = None,
            message: Optional[str] = None,
            code: Optional[int] = None,
    ):
        self.resource_type = resource_type
        self.resource_name = resource_name
        self.message = message
        self.code = code


@dataclass
class AuditRESTResponse(RESTResponse):
    FIELD_OWNER = "owner"
    FIELD_CREATED_AT = "createdAt"
    FIELD_CREATED_BY = "createdBy"
    FIELD_UPDATED_AT = "updatedAt"
    FIELD_UPDATED_BY = "updatedBy"

    owner: Optional[str] = json_field(FIELD_OWNER, default=None)
    created_at: Optional[int] = json_field(FIELD_CREATED_AT, default=None)
    created_by: Optional[str] = json_field(FIELD_CREATED_BY, default=None)
    updated_at: Optional[int] = json_field(FIELD_UPDATED_AT, default=None)
    updated_by: Optional[str] = json_field(FIELD_UPDATED_BY, default=None)

    def get_owner(self) -> Optional[str]:
        return self.owner

    def get_created_at(self) -> Optional[int]:
        return self.created_at

    def get_created_by(self) -> Optional[str]:
        return self.created_by

    def get_updated_at(self) -> Optional[int]:
        return self.updated_at

    def get_updated_by(self) -> Optional[str]:
        return self.updated_by

    def put_audit_options_to(self, options: Dict[str, str]) -> None:
        """Puts audit-related options into the provided dictionary."""
        options[self.FIELD_OWNER] = self.get_owner()
        options[self.FIELD_CREATED_BY] = str(self.get_created_by())
        options[self.FIELD_CREATED_AT] = str(self.get_created_at())
        options[self.FIELD_UPDATED_BY] = str(self.get_updated_by())
        options[self.FIELD_UPDATED_AT] = str(self.get_updated_at())


class PagedResponse(RESTResponse, Generic[T]):
    FIELD_NEXT_PAGE_TOKEN = "nextPageToken"

    @abstractmethod
    def data(self) -> List[T]:
        """data"""

    @abstractmethod
    def get_next_page_token(self) -> str:
        """get_next_page_token"""


@dataclass
class ListDatabasesResponse(PagedResponse[str]):
    FIELD_DATABASES = "databases"

    databases: List[str] = json_field(FIELD_DATABASES)
    next_page_token: str = json_field(PagedResponse.FIELD_NEXT_PAGE_TOKEN)

    def data(self) -> List[str]:
        return self.databases

    def get_next_page_token(self) -> str:
        return self.next_page_token


@dataclass
class Partition(PartitionStatistics):
    """Partition data model matching Java org.apache.paimon.partition.Partition."""

    FIELD_DONE = "done"
    FIELD_OPTIONS = "options"

    done: bool = json_field(FIELD_DONE, default=False)
    created_at: Optional[int] = json_field("createdAt", default=None)
    created_by: Optional[str] = json_field("createdBy", default=None)
    updated_at: Optional[int] = json_field("updatedAt", default=None)
    updated_by: Optional[str] = json_field("updatedBy", default=None)
    options: Optional[Dict[str, str]] = json_field(FIELD_OPTIONS, default=None)


@dataclass
class ListPartitionsResponse(PagedResponse['Partition']):
    """Response for listing partitions."""
    FIELD_PARTITIONS = "partitions"

    partitions: Optional[List[Partition]] = json_field(FIELD_PARTITIONS)
    next_page_token: Optional[str] = json_field(
        PagedResponse.FIELD_NEXT_PAGE_TOKEN)

    def data(self) -> Optional[List[Partition]]:
        return self.partitions

    def get_next_page_token(self) -> Optional[str]:
        return self.next_page_token


@dataclass
class ListTablesResponse(PagedResponse[str]):
    FIELD_TABLES = "tables"

    tables: Optional[List[str]] = json_field(FIELD_TABLES)
    next_page_token: Optional[str] = json_field(
        PagedResponse.FIELD_NEXT_PAGE_TOKEN)

    def data(self) -> Optional[List[str]]:
        return self.tables

    def get_next_page_token(self) -> Optional[str]:
        return self.next_page_token


@dataclass
class GetTableResponse(AuditRESTResponse):
    """Response for getting table"""

    # Field constants for JSON serialization
    FIELD_ID = "id"
    FIELD_NAME = "name"
    FIELD_PATH = "path"
    FIELD_IS_EXTERNAL = "isExternal"
    FIELD_SCHEMA_ID = "schemaId"
    FIELD_SCHEMA = "schema"

    id: Optional[str] = json_field(FIELD_ID, default=None)
    name: Optional[str] = json_field(FIELD_NAME, default=None)
    path: Optional[str] = json_field(FIELD_PATH, default=None)
    is_external: Optional[bool] = json_field(FIELD_IS_EXTERNAL, default=None)
    schema_id: Optional[int] = json_field(FIELD_SCHEMA_ID, default=None)
    schema: Optional[Schema] = json_field(FIELD_SCHEMA, default=None)

    def __init__(
            self,
            id: str,
            name: str,
            path: str,
            is_external: bool,
            schema_id: int,
            schema: Schema,
            owner: Optional[str] = None,
            created_at: Optional[int] = None,
            created_by: Optional[str] = None,
            updated_at: Optional[int] = None,
            updated_by: Optional[str] = None,
    ):
        super().__init__(owner, created_at, created_by, updated_at, updated_by)
        self.id = id
        self.name = name
        self.path = path
        self.is_external = is_external
        self.schema_id = schema_id
        self.schema = schema

    def get_id(self) -> str:
        return self.id

    def get_name(self) -> str:
        return self.name

    def get_path(self) -> str:
        return self.path

    def get_is_external(self) -> bool:
        return self.is_external

    def get_schema_id(self) -> int:
        return self.schema_id

    def get_schema(self) -> Schema:
        return self.schema


@dataclass
class GetDatabaseResponse(AuditRESTResponse):
    FIELD_ID = "id"
    FIELD_NAME = "name"
    FIELD_LOCATION = "location"
    FIELD_OPTIONS = "options"

    id: Optional[str] = json_field(FIELD_ID, default=None)
    name: Optional[str] = json_field(FIELD_NAME, default=None)
    location: Optional[str] = json_field(FIELD_LOCATION, default=None)
    options: Optional[Dict[str, str]] = json_field(
        FIELD_OPTIONS, default_factory=Dict)

    def __init__(
            self,
            id: Optional[str] = None,
            name: Optional[str] = None,
            location: Optional[str] = None,
            options: Optional[Dict[str, str]] = None,
            owner: Optional[str] = None,
            created_at: Optional[int] = None,
            created_by: Optional[str] = None,
            updated_at: Optional[int] = None,
            updated_by: Optional[str] = None,
    ):
        super().__init__(owner, created_at, created_by, updated_at, updated_by)
        self.id = id
        self.name = name
        self.location = location
        self.options = options or {}

    def get_id(self) -> Optional[str]:
        return self.id

    def get_name(self) -> Optional[str]:
        return self.name

    def get_location(self) -> Optional[str]:
        return self.location

    def get_options(self) -> Dict[str, str]:
        return self.options or {}


@dataclass
class ConfigResponse(RESTResponse):
    FILED_DEFAULTS = "defaults"

    defaults: Dict[str, str] = json_field(FILED_DEFAULTS)

    def merge(self, options: Options) -> Options:
        merged = options.copy()
        merged.data.update(self.defaults)
        return merged


@dataclass
class GetTableTokenResponse(RESTResponse):
    FIELD_TOKEN = "token"
    FIELD_EXPIRES_AT_MILLIS = "expiresAtMillis"

    token: Dict[str, str] = json_field(FIELD_TOKEN, default=None)
    expires_at_millis: Optional[int] = json_field(FIELD_EXPIRES_AT_MILLIS, default=None)


@dataclass
class CommitTableResponse(RESTResponse):
    FIELD_SUCCESS = "success"

    success: bool = json_field(FIELD_SUCCESS, default=False)

    def is_success(self) -> bool:
        return self.success


@dataclass
class GetTableSnapshotResponse(RESTResponse):
    """Response for getting table snapshot."""

    FIELD_SNAPSHOT = "snapshot"

    snapshot: Optional[TableSnapshot] = json_field(FIELD_SNAPSHOT, default=None)

    def __init__(self, snapshot: Optional[TableSnapshot] = None):
        self.snapshot = snapshot

    def get_snapshot(self) -> Optional[TableSnapshot]:
        return self.snapshot


@dataclass
class GetFunctionResponse(AuditRESTResponse):
    """Response for getting a function."""
    FIELD_UUID = "uuid"
    FIELD_NAME = "name"
    FIELD_INPUT_PARAMS = "inputParams"
    FIELD_RETURN_PARAMS = "returnParams"
    FIELD_DETERMINISTIC = "deterministic"
    FIELD_DEFINITIONS = "definitions"
    FIELD_COMMENT = "comment"
    FIELD_OPTIONS = "options"

    uuid: Optional[str] = json_field(FIELD_UUID, default=None)
    name: Optional[str] = json_field(FIELD_NAME, default=None)
    input_params: Optional[List[DataField]] = json_field(FIELD_INPUT_PARAMS, default=None)
    return_params: Optional[List[DataField]] = json_field(FIELD_RETURN_PARAMS, default=None)
    deterministic: bool = json_field(FIELD_DETERMINISTIC, default=False)
    definitions: Optional[Dict[str, 'FunctionDefinition']] = json_field(FIELD_DEFINITIONS, default=None)
    comment: Optional[str] = json_field(FIELD_COMMENT, default=None)
    options: Optional[Dict[str, str]] = json_field(FIELD_OPTIONS, default=None)

    def __init__(
        self,
        uuid: Optional[str] = None,
        name: Optional[str] = None,
        input_params: Optional[List[DataField]] = None,
        return_params: Optional[List[DataField]] = None,
        deterministic: bool = False,
        definitions: Optional[Dict[str, 'FunctionDefinition']] = None,
        comment: Optional[str] = None,
        options: Optional[Dict[str, str]] = None,
        owner: Optional[str] = None,
        created_at: Optional[int] = None,
        created_by: Optional[str] = None,
        updated_at: Optional[int] = None,
        updated_by: Optional[str] = None,
    ):
        super().__init__(owner, created_at, created_by, updated_at, updated_by)
        self.uuid = uuid
        self.name = name
        self.input_params = input_params
        self.return_params = return_params
        self.deterministic = deterministic
        self.definitions = definitions
        self.comment = comment
        self.options = options

    def to_function(self, identifier):
        from pypaimon.function.function import FunctionImpl
        return FunctionImpl(
            identifier=identifier,
            input_params=self.input_params,
            return_params=self.return_params,
            deterministic=self.deterministic,
            definitions=self.definitions or {},
            comment=self.comment,
            options=self.options or {},
        )

    @staticmethod
    def _parse_data_fields(raw: Optional[list]) -> Optional[List[DataField]]:
        if raw is None:
            return None
        return [DataField.from_dict(f) if isinstance(f, dict) else f for f in raw]

    @staticmethod
    def _parse_definitions(raw) -> Optional[Dict[str, 'FunctionDefinition']]:
        from pypaimon.function.function_definition import FunctionDefinition
        if raw is None:
            return None
        return {
            k: FunctionDefinition.from_dict(v) if isinstance(v, dict) else v
            for k, v in raw.items()
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "GetFunctionResponse":
        return cls(
            uuid=data.get("uuid"),
            name=data.get("name"),
            input_params=cls._parse_data_fields(data.get("inputParams")),
            return_params=cls._parse_data_fields(data.get("returnParams")),
            deterministic=data.get("deterministic", False),
            definitions=cls._parse_definitions(data.get("definitions")),
            comment=data.get("comment"),
            options=data.get("options"),
            owner=data.get("owner"),
            created_at=data.get("createdAt"),
            created_by=data.get("createdBy"),
            updated_at=data.get("updatedAt"),
            updated_by=data.get("updatedBy"),
        )

    def to_dict(self) -> Dict:
        result = {}
        if self.uuid is not None:
            result["uuid"] = self.uuid
        result["name"] = self.name
        result["inputParams"] = (
            [p.to_dict() if hasattr(p, 'to_dict') else p for p in self.input_params]
            if self.input_params is not None else None
        )
        result["returnParams"] = (
            [p.to_dict() if hasattr(p, 'to_dict') else p for p in self.return_params]
            if self.return_params is not None else None
        )
        result["deterministic"] = self.deterministic
        if self.definitions is not None:
            result["definitions"] = {
                k: v.to_dict() if hasattr(v, 'to_dict') else v
                for k, v in self.definitions.items()
            }
        else:
            result["definitions"] = None
        result["comment"] = self.comment
        result["options"] = self.options
        if self.owner is not None:
            result["owner"] = self.owner
        if self.created_at is not None:
            result["createdAt"] = self.created_at
        if self.created_by is not None:
            result["createdBy"] = self.created_by
        if self.updated_at is not None:
            result["updatedAt"] = self.updated_at
        if self.updated_by is not None:
            result["updatedBy"] = self.updated_by
        return result


@dataclass
class ListFunctionsResponse(PagedResponse[str]):
    """Response for listing functions."""
    FIELD_FUNCTIONS = "functions"

    functions: Optional[List[str]] = json_field(FIELD_FUNCTIONS, default=None)
    next_page_token: Optional[str] = json_field(
        PagedResponse.FIELD_NEXT_PAGE_TOKEN, default=None)

    def data(self) -> Optional[List[str]]:
        return self.functions

    def get_next_page_token(self) -> Optional[str]:
        return self.next_page_token


@dataclass
class ListFunctionDetailsResponse(PagedResponse['GetFunctionResponse']):
    """Response for listing function details."""
    FIELD_FUNCTION_DETAILS = "functionDetails"

    function_details: Optional[List[GetFunctionResponse]] = json_field(
        FIELD_FUNCTION_DETAILS, default=None)
    next_page_token: Optional[str] = json_field(
        PagedResponse.FIELD_NEXT_PAGE_TOKEN, default=None)

    def data(self) -> Optional[List[GetFunctionResponse]]:
        return self.function_details

    def get_next_page_token(self) -> Optional[str]:
        return self.next_page_token

    @classmethod
    def from_dict(cls, data: Dict) -> "ListFunctionDetailsResponse":
        details = data.get("functionDetails")
        if details is not None:
            details = [GetFunctionResponse.from_dict(d) for d in details]
        return cls(
            function_details=details,
            next_page_token=data.get("nextPageToken"),
        )

    def to_dict(self) -> Dict:
        result = {}
        if self.function_details is not None:
            result["functionDetails"] = [d.to_dict() for d in self.function_details]
        else:
            result["functionDetails"] = None
        result["nextPageToken"] = self.next_page_token
        return result


@dataclass
class ListFunctionsGloballyResponse(PagedResponse[Identifier]):
    """Response for listing functions globally across databases."""
    FIELD_FUNCTIONS = "functions"

    functions: Optional[List[Identifier]] = json_field(FIELD_FUNCTIONS, default=None)
    next_page_token: Optional[str] = json_field(
        PagedResponse.FIELD_NEXT_PAGE_TOKEN, default=None)

    def data(self) -> Optional[List[Identifier]]:
        return self.functions

    def get_next_page_token(self) -> Optional[str]:
        return self.next_page_token

    @classmethod
    def from_dict(cls, data: Dict) -> "ListFunctionsGloballyResponse":
        functions = data.get("functions")
        if functions is not None:
            functions = [
                Identifier.from_string(f) if isinstance(f, str) else
                Identifier.create(f.get("database"), f.get("object"))
                if isinstance(f, dict) else f
                for f in functions
            ]
        return cls(
            functions=functions,
            next_page_token=data.get("nextPageToken"),
        )

    def to_dict(self) -> Dict:
        result = {}
        if self.functions is not None:
            result["functions"] = [
                {"database": f.get_database_name(), "object": f.get_object_name()}
                for f in self.functions
            ]
        else:
            result["functions"] = None
        result["nextPageToken"] = self.next_page_token
        return result
