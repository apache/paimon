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
from typing import Any, Dict, Iterable, Optional, Tuple

from pypaimon.management.java_string import is_blank


class PermissionColumns:
    FIELD_COLUMN_NAMES = "columnNames"
    FIELD_EXCLUDED_COLUMN_NAMES = "excludedColumnNames"

    def __init__(self,
                 column_names: Optional[Iterable[str]] = None,
                 excluded_column_names: Optional[Iterable[str]] = None):
        if (column_names is None) == (excluded_column_names is None):
            raise ValueError(
                "columns must contain exactly one of columnNames or excludedColumnNames.")
        self._column_names = _immutable_non_empty(column_names, self.FIELD_COLUMN_NAMES)
        self._excluded_column_names = _immutable_non_empty(
            excluded_column_names, self.FIELD_EXCLUDED_COLUMN_NAMES)

    def get_column_names(self) -> Optional[Tuple[str, ...]]:
        return self._column_names

    def get_excluded_column_names(self) -> Optional[Tuple[str, ...]]:
        return self._excluded_column_names

    def to_dict(self) -> Dict[str, Any]:
        if self._column_names is not None:
            return {self.FIELD_COLUMN_NAMES: list(self._column_names)}
        return {self.FIELD_EXCLUDED_COLUMN_NAMES: list(self._excluded_column_names)}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PermissionColumns":
        return cls(data.get(cls.FIELD_COLUMN_NAMES), data.get(cls.FIELD_EXCLUDED_COLUMN_NAMES))

    def __eq__(self, other):
        if not isinstance(other, PermissionColumns):
            return False
        return (self._column_names == other._column_names
                and self._excluded_column_names == other._excluded_column_names)

    def __hash__(self):
        return hash((self._column_names, self._excluded_column_names))


def _immutable_non_empty(columns: Optional[Iterable[str]],
                         field_name: str) -> Optional[Tuple[str, ...]]:
    if columns is None:
        return None
    # A string would split into single-letter column names.
    if isinstance(columns, str):
        raise TypeError("{} must be a list of column names, not a string.".format(field_name))
    columns = tuple(columns)
    if not columns:
        raise ValueError("{} cannot be empty.".format(field_name))
    for column in columns:
        if is_blank(column):
            raise ValueError("{} cannot contain an empty column name.".format(field_name))
    if len(set(columns)) != len(columns):
        raise ValueError("{} cannot contain duplicate column names.".format(field_name))
    return columns
