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
from typing import Optional, List

from pypaimon.common.file_io import FileIO
from pypaimon.common.json_util import JSON
from pypaimon.schema.schema import Schema
from pypaimon.schema.table_schema import TableSchema


class SchemaManager:

    def __init__(self, file_io: FileIO, table_path: str):
        self.schema_prefix = "schema-"
        self.file_io = file_io
        self.table_path = table_path
        self.schema_path = f"{table_path.rstrip('/')}/schema"
        self.schema_cache = {}

    def latest(self) -> Optional['TableSchema']:
        try:
            versions = self._list_versioned_files()
            if not versions:
                return None

            max_version = max(versions)
            return self.get_schema(max_version)
        except Exception as e:
            raise RuntimeError(f"Failed to load schema from path: {self.schema_path}") from e

    def create_table(self, schema: Schema) -> TableSchema:
        while True:
            latest = self.latest()
            if latest is not None:
                raise RuntimeError("Schema in filesystem exists, creation is not allowed.")

            table_schema = TableSchema.from_schema(schema_id=0, schema=schema)
            success = self.commit(table_schema)
            if success:
                return table_schema

    def commit(self, new_schema: TableSchema) -> bool:
        schema_path = self._to_schema_path(new_schema.id)
        try:
            result = self.file_io.try_to_write_atomic(schema_path, JSON.to_json(new_schema, indent=2))
            if result:
                self.schema_cache[new_schema.id] = new_schema
            return result
        except Exception as e:
            raise RuntimeError(f"Failed to commit schema: {e}") from e

    def _to_schema_path(self, schema_id: int) -> str:
        return f"{self.schema_path.rstrip('/')}/{self.schema_prefix}{schema_id}"

    def get_schema(self, schema_id: int) -> Optional[TableSchema]:
        if schema_id not in self.schema_cache:
            schema_path = self._to_schema_path(schema_id)
            if not self.file_io.exists(schema_path):
                return None
            schema = TableSchema.from_path(self.file_io, schema_path)
            self.schema_cache[schema_id] = schema
        return self.schema_cache[schema_id]

    def _list_versioned_files(self) -> List[int]:
        if not self.file_io.exists(self.schema_path):
            return []

        statuses = self.file_io.list_status(self.schema_path)
        if statuses is None:
            return []

        versions = []
        for status in statuses:
            name = status.path.split('/')[-1]
            if name.startswith(self.schema_prefix):
                try:
                    version = int(name[len(self.schema_prefix):])
                    versions.append(version)
                except ValueError:
                    continue
        return versions
