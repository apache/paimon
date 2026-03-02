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


class IcebergTable(Table):
    """Metadata-only Iceberg table.

    Paimon Python currently exposes Iceberg table metadata, but does not
    support read / write operations on Iceberg tables.
    """

    def __init__(
        self,
        file_io: FileIO,
        identifier: Identifier,
        table_schema: TableSchema,
        location: str,
        options: Optional[Dict[str, str]] = None,
        comment: Optional[str] = None,
        uuid: Optional[str] = None,
    ):
        self.file_io = file_io
        self.identifier = identifier
        self._table_schema = table_schema
        self._location = location.rstrip("/")
        self._options = options or dict(table_schema.options)
        self.comment = comment
        self._uuid = uuid

        self.fields = table_schema.fields
        self.field_names = [f.name for f in self.fields]
        self.partition_keys: List[str] = table_schema.partition_keys or []
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

    def uuid(self) -> str:
        return self._uuid or self.full_name()

    def copy(self, dynamic_options: Dict[str, str]) -> "IcebergTable":
        new_options = dict(self._options)
        new_options.update(dynamic_options or {})
        return IcebergTable(
            file_io=self.file_io,
            identifier=self.identifier,
            table_schema=self._table_schema,
            location=self._location,
            options=new_options,
            comment=self.comment,
            uuid=self._uuid,
        )

    def new_read_builder(self):
        raise NotImplementedError(
            "IcebergTable does not support read operation in paimon-python yet."
        )

    def new_batch_write_builder(self):
        raise NotImplementedError(
            "IcebergTable does not support batch write operation in paimon-python yet."
        )

    def new_stream_write_builder(self):
        raise NotImplementedError(
            "IcebergTable does not support stream write operation in paimon-python yet."
        )
