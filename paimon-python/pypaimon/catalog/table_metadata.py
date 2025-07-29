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
from typing import Optional

from pypaimon.schema.table_schema import TableSchema


class TableMetadata:

    def __init__(self, schema: TableSchema, is_external: bool, uuid: Optional[str] = None):
        self._schema = schema
        self._is_external = is_external
        self._uuid = uuid

    @property
    def schema(self) -> TableSchema:
        return self._schema

    @property
    def is_external(self) -> bool:
        return self._is_external

    @property
    def uuid(self) -> Optional[str]:
        return self._uuid
