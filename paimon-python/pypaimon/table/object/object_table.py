# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Dict, List, Optional

from pypaimon.common.file_io import FileIO
from pypaimon.common.identifier import Identifier
from pypaimon.schema.table_schema import TableSchema
from pypaimon.table.table import Table


# Fixed schema matching Java ObjectTable.SCHEMA
OBJECT_TABLE_FIELD_NAMES = ["path", "name", "length", "mtime", "atime", "owner"]


class ObjectTable(Table):
    """An object table refers to a directory that contains multiple objects (files).

    Object table provides metadata indexes for unstructured data objects in this
    directory, allowing users to analyze unstructured data in Object Storage.

    This is a read-only table. Write operations are not supported.
    """

    def __init__(
        self,
        file_io: FileIO,
        identifier: Identifier,
        table_schema: TableSchema,
        location: str,
        options: Optional[Dict[str, str]] = None,
        comment: Optional[str] = None,
    ):
        self.file_io = file_io
        self.identifier = identifier
        self._table_schema = table_schema
        self._location = location.rstrip("/")
        self._options = options or dict(table_schema.options)
        self.comment = comment
        self.partition_keys: List[str] = []
        self.primary_keys: List[str] = []

    def name(self) -> str:
        return self.identifier.get_table_name()

    def full_name(self) -> str:
        return self.identifier.get_full_name()

    @property
    def table_schema(self) -> TableSchema:
        return self._table_schema

    @table_schema.setter
    def table_schema(self, value: TableSchema):
        self._table_schema = value

    def location(self) -> str:
        return self._location

    def options(self) -> Dict[str, str]:
        return self._options

    def copy(self, dynamic_options: Dict[str, str]) -> "ObjectTable":
        new_options = dict(self._options)
        new_options.update(dynamic_options or {})
        return ObjectTable(
            file_io=self.file_io,
            identifier=self.identifier,
            table_schema=self._table_schema,
            location=self._location,
            options=new_options,
            comment=self.comment,
        )

    def new_read_builder(self):
        from pypaimon.table.object.object_read_builder import ObjectReadBuilder
        return ObjectReadBuilder(self)

    def new_batch_write_builder(self):
        raise NotImplementedError(
            "ObjectTable is read-only and does not support batch write."
        )

    def new_stream_write_builder(self):
        raise NotImplementedError(
            "ObjectTable is read-only and does not support stream write."
        )
