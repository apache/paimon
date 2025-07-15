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
from typing import Dict, Optional, Any, Generic, List
from dataclasses import dataclass, field
from api.typedef import T


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

    def __init__(self,
                 resource_type: Optional[str] = None,
                 resource_name: Optional[str] = None,
                 message: Optional[str] = None,
                 code: Optional[int] = None):
        self.resource_type = resource_type
        self.resource_name = resource_name
        self.message = message
        self.code = code

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


@dataclass
class PaimonDataType:
    FIELD_TYPE = "type"
    FIELD_ELEMENT = "element"
    FIELD_FIELDS = "fields"
    FIELD_KEY = "key"
    FIELD_VALUE = "value"

    type: str
    element: Optional['PaimonDataType'] = None
    fields: List['DataField'] = field(default_factory=list)
    key: Optional['PaimonDataType'] = None
    value: Optional['PaimonDataType'] = None

    @classmethod
    def from_dict(cls, data: Any) -> 'PaimonDataType':
        if isinstance(data, dict):
            element = data.get(cls.FIELD_ELEMENT, None)
            fields = data.get(cls.FIELD_FIELDS, None)
            key = data.get(cls.FIELD_KEY, None)
            value = data.get(cls.FIELD_VALUE, None)
            if element is not None:
                element = PaimonDataType.from_dict(data.get(cls.FIELD_ELEMENT)),
            if fields is not None:
                fields = list(map(lambda f: DataField.from_dict(f), fields)),
            if key is not None:
                key = PaimonDataType.from_dict(key)
            if value is not None:
                value = PaimonDataType.from_dict(value)
            return cls(
                type=data.get(cls.FIELD_TYPE),
                element=element,
                fields=fields,
                key=key,
                value=value,
            )
        else:
            return cls(type=data)

    def to_dict(self) -> Any:
        if self.element is None and self.fields is None and self.key:
            return self.type
        if self.element is not None:
            return {self.FIELD_TYPE: self.type, self.FIELD_ELEMENT: self.element}
        elif self.fields is not None:
            return {self.FIELD_TYPE: self.type, self.FIELD_FIELDS: self.fields}
        elif self.value is not None:
            return {self.FIELD_TYPE: self.type, self.FIELD_KEY: self.key, self.FIELD_VALUE: self.value}
        elif self.key is not None and self.value is None:
            return {self.FIELD_TYPE: self.type, self.FIELD_KEY: self.key}


@dataclass
class DataField:
    FIELD_ID = "id"
    FIELD_NAME = "name"
    FIELD_TYPE = "type"
    FIELD_DESCRIPTION = "description"

    description: str
    id: int
    name: str
    type: PaimonDataType

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DataField':
        return cls(
            id=data.get(cls.FIELD_ID),
            name=data.get(cls.FIELD_NAME),
            type=PaimonDataType.from_dict(data.get(cls.FIELD_TYPE)),
            description=data.get(cls.FIELD_DESCRIPTION),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            self.FIELD_ID: self.id,
            self.FIELD_NAME: self.name,
            self.FIELD_TYPE: PaimonDataType.to_dict(self.type),
            self.FIELD_DESCRIPTION: self.description
        }


@dataclass
class Schema:
    FIELD_FIELDS = "fields"
    FIELD_PARTITION_KEYS = "partitionKeys"
    FIELD_PRIMARY_KEYS = "primaryKeys"
    FIELD_OPTIONS = "options"
    FIELD_COMMENT = "comment"

    fields: List[DataField] = field(default_factory=list)
    partition_keys: List[str] = field(default_factory=list)
    primary_keys: List[str] = field(default_factory=list)
    options: Dict[str, str] = field(default_factory=dict)
    comment: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Schema':
        return cls(
            fields=list(map(lambda f: DataField.from_dict(f), data.get(cls.FIELD_FIELDS))),
            partition_keys=data.get(cls.FIELD_PARTITION_KEYS),
            primary_keys=data.get(cls.FIELD_PRIMARY_KEYS),
            options=data.get(cls.FIELD_OPTIONS),
            comment=data.get(cls.FIELD_COMMENT)
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            self.FIELD_FIELDS: list(map(lambda f: DataField.to_dict(f), self.fields)),
            self.FIELD_PARTITION_KEYS: self.partition_keys,
            self.FIELD_PRIMARY_KEYS: self.primary_keys,
            self.FIELD_OPTIONS: self.options,
            self.FIELD_COMMENT: self.comment
        }


@dataclass
class TableSchema:
    """Table schema with ID"""
    id: int
    fields: List[DataField]
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
    schema: Optional[Schema] = None

    def __init__(self,
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
                 updated_by: Optional[str] = None):
        super().__init__(owner, created_at, created_by, updated_at, updated_by)
        self.id = id
        self.name = name
        self.path = path
        self.is_external = is_external
        self.schema_id = schema_id
        self.schema = schema

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'GetTableResponse':
        return cls(
            id=data.get(cls.FIELD_ID),
            name=data.get(cls.FIELD_NAME),
            path=data.get(cls.FIELD_PATH),
            is_external=data.get(cls.FIELD_IS_EXTERNAL),
            schema_id=data.get(cls.FIELD_SCHEMA_ID),
            schema=Schema.from_dict(data.get(cls.FIELD_SCHEMA)),
            owner=data.get(cls.FIELD_OWNER),
            created_at=data.get(cls.FIELD_CREATED_AT),
            created_by=data.get(cls.FIELD_CREATED_BY),
            updated_at=data.get(cls.FIELD_UPDATED_AT),
            updated_by=data.get(cls.FIELD_UPDATED_BY)
        )

    def to_dict(self) -> Dict[str, Any]:
        result = {
            self.FIELD_ID: self.id,
            self.FIELD_NAME: self.name,
            self.FIELD_PATH: self.path,
            self.FIELD_IS_EXTERNAL: self.is_external,
            self.FIELD_SCHEMA_ID: self.schema_id,
            self.FIELD_SCHEMA: Schema.to_dict(self.schema)
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

    def merge(self, options: Dict[str, str]) -> Dict[str, str]:
        merged = options.copy()
        merged.update(self.defaults)
        return merged
