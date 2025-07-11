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
from dataclasses import dataclass, field
import json
from datetime import datetime
from api_resquest import Identifier

T = TypeVar('T')

@dataclass
class PagedList(Generic[T]):
    elements: List[T]
    next_page_token: Optional[str] = None


class RESTResponse(ABC):
    pass


@dataclass
class ErrorResponse(RESTResponse):
    resource_type: Optional[str] = None
    resource_name: Optional[str] = None
    message: Optional[str] = None
    code: Optional[int] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ErrorResponse':
        return cls(
            resource_type=data.get('resourceType'),
            resource_name=data.get('resourceName'),
            message=data.get('message'),
            code=data.get('code'),
        )

    @classmethod
    def from_json(cls, json_str: str) -> 'ErrorResponse':
        data = json.loads(json_str)
        return cls.from_dict(data)


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

    def get_created_datetime(self) -> Optional[datetime]:
        if self.created_at:
            return datetime.fromtimestamp(self.created_at / 1000)
        return None

    def get_updated_datetime(self) -> Optional[datetime]:
        if self.updated_at:
            return datetime.fromtimestamp(self.updated_at / 1000)
        return None


class PagedResponse(RESTResponse, Generic[T]):

    @abstractmethod
    def data(self) -> List[T]:
        pass

    @abstractmethod
    def get_next_page_token(self) -> str:
        pass


@dataclass
class ListDatabasesResponse(PagedResponse[str]):
    databases: List[str]
    next_page_token: str

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ListDatabasesResponse':
        return cls(
            databases=data.get('databases'),
            next_page_token=data.get('nextPageToken')
        )

    @classmethod
    def from_json(cls, json_str: str) -> 'ListDatabasesResponse':
        data = json.loads(json_str)
        return cls.from_dict(data)

    def data(self) -> List[str]:
        return self.databases

    def get_next_page_token(self) -> str:
        return self.next_page_token


@dataclass
class ListTablesResponse(PagedResponse[str]):
    tables: Optional[List[str]]
    next_page_token: Optional[str]

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ListTablesResponse':
        return cls(
            tables=data.get('tables'),
            next_page_token=data.get('nextPageToken')
        )

    @classmethod
    def from_json(cls, json_str: str) -> 'ListTablesResponse':
        data = json.loads(json_str)
        return cls.from_dict(data)

    def data(self) -> Optional[List[str]]:
        return self.tables

    def get_next_page_token(self) -> Optional[str]:
        return self.next_page_token


@dataclass
class GetTableResponse(RESTResponse):
    identifier: Identifier
    schema: Any
    properties: Dict[str, str]

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        return data

    @classmethod
    def from_json(cls, json_str: str) -> Dict[str, Any]:
        data = json.loads(json_str)
        return cls.from_dict(data)


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

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2)

    @classmethod
    def from_json(cls, json_str: str) -> 'GetDatabaseResponse':
        data = json.loads(json_str)
        return cls.from_dict(data)

    def __str__(self) -> str:
        return f"GetDatabaseResponse(id={self.id}, name={self.name}, location={self.location})"

    def __repr__(self) -> str:
        return (f"GetDatabaseResponse(id={self.id!r}, name={self.name!r}, "
                f"location={self.location!r}, options={self.options!r}, "
                f"owner={self.owner!r}, created_at={self.created_at}, "
                f"created_by={self.created_by!r}, updated_at={self.updated_at}, "
                f"updated_by={self.updated_by!r})")


# 具体的响应类
@dataclass
class ConfigResponse(RESTResponse):
    defaults: Dict[str, Any]

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ConfigResponse':
        return cls(
            defaults=data.get('defaults')
        )

    @classmethod
    def from_json(cls, json_str: str) -> 'ConfigResponse':
        data = json.loads(json_str)
        return cls.from_dict(data)

    def merge(self, options: Dict[str, Any]) -> Dict[str, Any]:
        merged = options.copy()
        merged.update(self.defaults)
        return merged


class JSONSerializableGetDatabaseResponse(GetDatabaseResponse):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __json__(self) -> Dict[str, Any]:
        return self.to_dict()

    @classmethod
    def __from_json__(cls, data: Dict[str, Any]) -> 'JSONSerializableGetDatabaseResponse':
        return cls.from_dict(data)
