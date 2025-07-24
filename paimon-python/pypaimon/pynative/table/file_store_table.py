################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

from pathlib import Path

from pypaimon.api import Identifier
from pypaimon.api.table_schema import TableSchema
from pypaimon.pynative.common.file_io import FileIO


from pypaimon.pynative.table.schema_manager import SchemaManager
from pypaimon.pynative.table.table import Table


class FileStoreTable(Table):
    def __init__(self, file_io: FileIO, identifier: Identifier, table_path: Path,
                 table_schema: TableSchema):
        self.file_io = file_io
        self.identifier = identifier
        self.table_path = table_path

        self.fields = table_schema.fields
        self.primary_keys = table_schema.primary_keys
        self.partition_keys = table_schema.partition_keys

        self.options = table_schema.options
        self.table_schema = table_schema
        self.schema_manager = SchemaManager(file_io, table_path)
        self.is_primary_key_table = bool(self.primary_keys)
