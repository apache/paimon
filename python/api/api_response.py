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
from typing import Dict, Optional, Any, Generic, TypeVar, List
from dataclasses import dataclass, field, asdict
import json

T = TypeVar('T')


@dataclass
class PagedList(Generic[T]):
    elements: List[T]
    next_page_token: Optional[str] = None


class RESTResponse(ABC):
    pass


@dataclass
class ErrorResponse(RESTResponse):
    FIELD_RESOURCE_TYPE: "resourceType"
    FIELD_RESOURCE_NAME: "resourceName"
    FIELD_MESSAGE: "message"
    FIELD_CODE: "code"

    resource_type: Optional[str] = None
    resource_name: Optional[str] = None
    message: Optional[str] = None
    code: Optional[int] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ErrorResponse':
        return cls(
            resource_type=data.get(cls.FIELD_RESOURCE_TYPE),
            resource_name=data.get(cls.FIELD_RESOURCE_NAME),
            message=data.get(cls.FIELD_MESSAGE),
            code=data.get(cls.FIELD_CODE),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            self.FIELD_RESOURCE_TYPE: self.resource_type,
            self.FIELD_RESOURCE_NAME: self.resource_name,
            self.FIELD_MESSAGE: self.message,
            self.FIELD_CODE: self.code
        }


@dataclass
class AuditRESTResponse(RESTResponse):
    FIELD_OWNER = "owner"
    FIELD_CREATED_AT = "createdAt"
    FIELD_CREATED_BY = "createdBy"
    FIELD_UPDATED_AT = "updatedAt"
    FIELD_UPDATED_BY = "updatedBy"

    owner: Optional[str] = None
    created_at: Optional[int] = None
    created_by: Optional[str] = None
    updated_at: Optional[int] = None
    updated_by: Optional[str] = None

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


class PagedResponse(RESTResponse, Generic[T]):
    FIELD_NEXT_PAGE_TOKEN = "nextPageToken"

    @abstractmethod
    def data(self) -> List[T]:
        pass

    @abstractmethod
    def get_next_page_token(self) -> str:
        pass


@dataclass
class ListDatabasesResponse(PagedResponse[str]):
    FIELD_DATABASES = "databases"

    databases: List[str]
    next_page_token: str

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ListDatabasesResponse':
        return cls(
            databases=data.get(cls.FIELD_DATABASES),
            next_page_token=data.get(cls.FIELD_NEXT_PAGE_TOKEN)
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            self.FIELD_DATABASES: self.databases,
            self.FIELD_NEXT_PAGE_TOKEN: self.next_page_token
        }

    def data(self) -> List[str]:
        return self.databases

    def get_next_page_token(self) -> str:
        return self.next_page_token


@dataclass
class ListTablesResponse(PagedResponse[str]):
    FIELD_TABLES = "tables"

    tables: Optional[List[str]]
    next_page_token: Optional[str]

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ListTablesResponse':
        return cls(
            tables=data.get(cls.FIELD_TABLES),
            next_page_token=data.get(cls.FIELD_NEXT_PAGE_TOKEN)
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            self.FIELD_TABLES: self.tables,
            self.FIELD_NEXT_PAGE_TOKEN: self.next_page_token
        }

    def data(self) -> Optional[List[str]]:
        return self.tables

    def get_next_page_token(self) -> Optional[str]:
        return self.next_page_token


# Mock classes for Paimon entities
@dataclass
class Identifier:
    """Table/View/Function identifier"""
    database_name: str
    object_name: str
    branch_name: Optional[str] = None

    @classmethod
    def create(cls, database_name: str, object_name: str) -> 'Identifier':
        return cls(database_name, object_name)

    @classmethod
    def from_string(cls, full_name: str) -> 'Identifier':
        parts = full_name.split('.')
        if len(parts) == 2:
            return cls(parts[0], parts[1])
        elif len(parts) == 3:
            return cls(parts[0], parts[1], parts[2])
        else:
            raise ValueError(f"Invalid identifier format: {full_name}")

    def get_full_name(self) -> str:
        if self.branch_name:
            return f"{self.database_name}.{self.object_name}.{self.branch_name}"
        return f"{self.database_name}.{self.object_name}"

    def get_database_name(self) -> str:
        return self.database_name

    def get_table_name(self) -> str:
        return self.object_name

    def get_object_name(self) -> str:
        return self.object_name

    def get_branch_name(self) -> Optional[str]:
        return self.branch_name

    def is_system_table(self) -> bool:
        return self.object_name.startswith('$')


@dataclass
class Schema:
    """Table schema"""
    fields: List[Dict[str, Any]]
    partition_keys: List[str] = field(default_factory=list)
    primary_keys: List[str] = field(default_factory=list)
    options: Dict[str, str] = field(default_factory=dict)
    comment: Optional[str] = None

    def field_names(self) -> List[str]:
        return [field['name'] for field in self.fields]


@dataclass
class TableSchema:
    """Table schema with ID"""
    id: int
    fields: List[Dict[str, Any]]
    highest_field_id: int
    partition_keys: List[str]
    primary_keys: List[str]
    options: Dict[str, str]
    comment: Optional[str]

    def to_schema(self) -> Schema:
        return Schema(
            fields=self.fields,
            partition_keys=self.partition_keys,
            primary_keys=self.primary_keys,
            options=self.options,
            comment=self.comment
        )


@dataclass
class TableMetadata:
    """Table metadata"""
    schema: TableSchema
    is_external: bool
    uuid: str


@dataclass
class Snapshot:
    """Table snapshot"""
    id: int
    schema_id: int
    base_manifest_list: str
    delta_manifest_list: str
    changelog_manifest_list: Optional[str]
    commit_user: str
    commit_identifier: int
    commit_kind: str
    time_millis: int
    log_offsets: Dict[int, int]
    total_record_count: int
    delta_record_count: int
    changelog_record_count: int
    watermark: Optional[int]


@dataclass
class TableSnapshot:
    """Table snapshot with statistics"""
    snapshot: Snapshot
    record_count: int
    file_size_in_bytes: int
    file_count: int
    last_file_creation_time: int


@dataclass
class Partition:
    """Table partition"""
    spec: Dict[str, str]
    record_count: int
    file_size_in_bytes: int
    file_count: int
    last_file_creation_time: int
    done: bool


@dataclass
class RESTToken:
    """REST authentication token"""
    token: Dict[str, str]
    expire_at_millis: int


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

    id: Optional[str] = None
    name: Optional[str] = None
    path: Optional[str] = None
    is_external: Optional[bool] = None
    schema_id: Optional[int] = None
    schema: Optional[Any] = None

    def __init__(self,
                 id: str,
                 name: str,
                 path: str,
                 is_external: bool,
                 schema_id: int,
                 schema: Any,
                 owner: Optional[str] = None,
                 created_at: Optional[int] = None,
                 created_by: Optional[str] = None,
                 updated_at: Optional[int] = None,
                 updated_by: Optional[str] = None):
        super().__init__(owner, created_at, created_by, updated_at, updated_by)
        self.id = id
        self.name = name
        self.path = path
        self.is_external = is_external
        self.schema_id = schema_id
        self.schema = schema

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        return data

    def to_dict(self) -> Dict[str, Any]:
        result = {
            self.FIELD_ID: self.id,
            self.FIELD_NAME: self.name,
            self.FIELD_PATH: self.path,
            self.FIELD_IS_EXTERNAL: self.is_external,
            self.FIELD_SCHEMA_ID: self.schema_id,
            self.FIELD_SCHEMA: self.schema
        }
        if self.owner is not None:
            result[self.FIELD_OWNER] = self.owner
        if self.created_at is not None:
            result[self.FIELD_CREATED_AT] = self.created_at
        if self.created_by is not None:
            result[self.FIELD_CREATED_BY] = self.created_by
        if self.updated_at is not None:
            result[self.FIELD_UPDATED_AT] = self.updated_at
        if self.updated_by is not None:
            result[self.FIELD_UPDATED_BY] = self.updated_by

        return result


@dataclass
class GetDatabaseResponse(AuditRESTResponse):
    FIELD_ID = "id"
    FIELD_NAME = "name"
    FIELD_LOCATION = "location"
    FIELD_OPTIONS = "options"

    id: Optional[str] = None
    name: Optional[str] = None
    location: Optional[str] = None
    options: Optional[Dict[str, str]] = field(default_factory=dict)

    def __init__(self,
                 id: Optional[str] = None,
                 name: Optional[str] = None,
                 location: Optional[str] = None,
                 options: Optional[Dict[str, str]] = None,
                 owner: Optional[str] = None,
                 created_at: Optional[int] = None,
                 created_by: Optional[str] = None,
                 updated_at: Optional[int] = None,
                 updated_by: Optional[str] = None):
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

    def to_dict(self) -> Dict[str, Any]:
        result = {
            self.FIELD_ID: self.id,
            self.FIELD_NAME: self.name,
            self.FIELD_LOCATION: self.location,
            self.FIELD_OPTIONS: self.options
        }

        if self.owner is not None:
            result[self.FIELD_OWNER] = self.owner
        if self.created_at is not None:
            result[self.FIELD_CREATED_AT] = self.created_at
        if self.created_by is not None:
            result[self.FIELD_CREATED_BY] = self.created_by
        if self.updated_at is not None:
            result[self.FIELD_UPDATED_AT] = self.updated_at
        if self.updated_by is not None:
            result[self.FIELD_UPDATED_BY] = self.updated_by

        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'GetDatabaseResponse':
        return cls(
            id=data.get(cls.FIELD_ID),
            name=data.get(cls.FIELD_NAME),
            location=data.get(cls.FIELD_LOCATION),
            options=data.get(cls.FIELD_OPTIONS, {}),
            owner=data.get(cls.FIELD_OWNER),
            created_at=data.get(cls.FIELD_CREATED_AT),
            created_by=data.get(cls.FIELD_CREATED_BY),
            updated_at=data.get(cls.FIELD_UPDATED_AT),
            updated_by=data.get(cls.FIELD_UPDATED_BY)
        )


@dataclass
class ConfigResponse(RESTResponse):
    FILED_DEFAULTS = "defaults"

    defaults: Dict[str, str]

    @classmethod
    def from_dict(cls, data: Dict[str, str]) -> 'ConfigResponse':
        return cls(defaults=data.get(cls.FILED_DEFAULTS))

    def to_dict(self) -> Dict[str, Any]:
        return {self.FILED_DEFAULTS: self.defaults}

    @classmethod
    def from_json(cls, json_str: str) -> 'ConfigResponse':
        data = json.loads(json_str)
        return cls.from_dict(data)

    def merge(self, options: Dict[str, str]) -> Dict[str, str]:
        merged = options.copy()
        merged.update(self.defaults)
        return merged
