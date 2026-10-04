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
from typing import Any, Dict, Optional, Union

from pypaimon.management.java_string import is_blank
from pypaimon.management.resource_type import ResourceType


class PermissionResource:
    FIELD_TYPE = "type"
    FIELD_DATABASE = "database"
    FIELD_TABLE = "table"
    FIELD_FUNCTION = "function"
    FIELD_VIEW = "view"

    def __init__(self,
                 resource_type: Union[ResourceType, str],
                 database: Optional[str] = None,
                 table: Optional[str] = None,
                 function: Optional[str] = None,
                 view: Optional[str] = None):
        if not isinstance(resource_type, ResourceType):
            resource_type = ResourceType.from_string(resource_type)
        if resource_type is None:
            raise ValueError("resource type cannot be null")
        _validate(resource_type, database, table, function, view)
        self._type = resource_type
        self._database = _blank_to_none(database)
        self._table = _blank_to_none(table)
        self._function = _blank_to_none(function)
        self._view = _blank_to_none(view)

    def get_type(self) -> ResourceType:
        return self._type

    def get_database(self) -> Optional[str]:
        return self._database

    def get_table(self) -> Optional[str]:
        return self._table

    def get_function(self) -> Optional[str]:
        return self._function

    def get_view(self) -> Optional[str]:
        return self._view

    def to_dict(self) -> Dict[str, Any]:
        result = {self.FIELD_TYPE: self._type.name}
        for name, value in ((self.FIELD_DATABASE, self._database),
                            (self.FIELD_TABLE, self._table),
                            (self.FIELD_FUNCTION, self._function),
                            (self.FIELD_VIEW, self._view)):
            if value is not None:
                result[name] = value
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PermissionResource":
        return cls(data.get(cls.FIELD_TYPE), data.get(cls.FIELD_DATABASE),
                   data.get(cls.FIELD_TABLE), data.get(cls.FIELD_FUNCTION),
                   data.get(cls.FIELD_VIEW))

    def __eq__(self, other):
        if not isinstance(other, PermissionResource):
            return False
        return self._key() == other._key()

    def __hash__(self):
        return hash(self._key())

    def _key(self):
        return self._type, self._database, self._table, self._function, self._view


def _validate(resource_type, database, table, function, view):
    name = resource_type.name
    if resource_type in (ResourceType.CATALOG, ResourceType.CATALOG_ALL):
        _check(is_blank(database) and is_blank(table) and is_blank(function) and is_blank(view),
               "{} resource cannot contain object identifiers.".format(name))
    elif resource_type in (ResourceType.DATABASE, ResourceType.DATABASE_ALL):
        _check(not is_blank(database), "database is required for {} resource.".format(name))
        _check(is_blank(table) and is_blank(function) and is_blank(view),
               "{} resource cannot contain table, function, or view.".format(name))
    elif resource_type in (ResourceType.TABLE, ResourceType.COLUMN):
        _check(not is_blank(database), "database is required for {} resource.".format(name))
        _check(not is_blank(table), "table is required for {} resource.".format(name))
        _check(is_blank(function) and is_blank(view),
               "{} resource cannot contain function or view.".format(name))
    elif resource_type == ResourceType.FUNCTION:
        _check(not is_blank(database), "database is required for FUNCTION resource.")
        _check(not is_blank(function), "function is required for FUNCTION resource.")
        _check(is_blank(table) and is_blank(view), "FUNCTION resource cannot contain table or view.")
    else:
        _check(not is_blank(database), "database is required for VIEW resource.")
        _check(not is_blank(view), "view is required for VIEW resource.")
        _check(is_blank(table) and is_blank(function), "VIEW resource cannot contain table or function.")


def _check(condition: bool, message: str):
    if not condition:
        raise ValueError(message)


def _blank_to_none(value: Optional[str]) -> Optional[str]:
    return None if is_blank(value) else value
