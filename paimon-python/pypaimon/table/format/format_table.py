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

from enum import Enum
from typing import Dict, List, Optional

from pypaimon.common.file_io import FileIO
from pypaimon.common.identifier import Identifier
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options

PARTITION_DEFAULT_NAME_KEY = "partition.default-name"
PARTITION_DEFAULT_NAME_DEFAULT = "__DEFAULT_PARTITION__"
from pypaimon.schema.table_schema import TableSchema
from pypaimon.table.table import Table


class Format(str, Enum):
    ORC = "orc"
    PARQUET = "parquet"
    CSV = "csv"
    TEXT = "text"
    JSON = "json"

    @classmethod
    def parse(cls, file_format: str) -> "Format":
        s = (file_format or "parquet").strip().upper()
        try:
            return cls[s]
        except KeyError:
            raise ValueError(
                f"Format table unsupported file format: {file_format}. "
                f"Supported: {[f.name for f in cls]}"
            )


class FormatTable(Table):
    def __init__(
        self,
        file_io: FileIO,
        identifier: Identifier,
        table_schema: TableSchema,
        location: str,
        format: Format,
        options: Optional[Dict[str, str]] = None,
        comment: Optional[str] = None,
    ):
        self.file_io = file_io
        self.identifier = identifier
        self._table_schema = table_schema
        self._location = location.rstrip("/")
        self._format = format
        self.options = options or dict(table_schema.options)
        self.comment = comment
        self.fields = table_schema.fields
        self.field_names = [f.name for f in self.fields]
        self.partition_keys = table_schema.partition_keys or []
        self.primary_keys: List[str] = []  # format table has no primary key
        self._core_options = CoreOptions(Options(self.options))

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

    def format(self) -> Format:
        return self._format

    def options(self) -> Dict[str, str]:
        return self.options

    def default_part_name(self) -> str:
        return self.options.get(PARTITION_DEFAULT_NAME_KEY, PARTITION_DEFAULT_NAME_DEFAULT)

    def new_read_builder(self):
        from pypaimon.table.format.format_read_builder import FormatReadBuilder
        return FormatReadBuilder(self)

    def new_batch_write_builder(self):
        from pypaimon.table.format.format_batch_write_builder import FormatBatchWriteBuilder
        return FormatBatchWriteBuilder(self)

    def new_stream_write_builder(self):
        raise NotImplementedError("Format table does not support stream write.")
