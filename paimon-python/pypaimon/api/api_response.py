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

from pypaimon.common.json_util import T, json_field
from pypaimon.common.options import Options
from pypaimon.schema.schema import Schema


@dataclass
class PagedList(Generic[T]):
    elements: List[T]
    next_page_token: Optional[str] = None


class RESTResponse(ABC):
    """RESTResponse"""


@dataclass
class ErrorResponse(RESTResponse):
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
