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

import time
from typing import List, Dict, Optional

from pypaimon.api.schema import Schema
from pypaimon.api.data_types import DataField


class TableSchema:

    def __init__(self, id: int, fields: List[DataField], highest_field_id: int,
                 partition_keys: List[str], primary_keys: List[str], options: Dict[str, str],
                 comment: Optional[str] = None, time_millis: Optional[int] = None):
        self.id = id
        self.fields = fields
        self.highest_field_id = highest_field_id
        self.partition_keys = partition_keys or []
        self.primary_keys = primary_keys or []
        self.options = options or {}
        self.comment = comment
        self.time_millis = time_millis if time_millis is not None else int(time.time() * 1000)

    def to_schema(self) -> Schema:
        # pa_schema = schema_util.convert_data_fields_to_pa_schema(self.fields)
        return Schema(
            fields=self.fields,
            partition_keys=self.partition_keys,
            primary_keys=self.primary_keys,
            options=self.options,
            comment=self.comment
        )

    @staticmethod
    def create(schema_id: int, schema: Schema) -> "TableSchema":
        fields: List[DataField] = schema.fields

        partition_keys: List[str] = schema.partition_keys

        primary_keys: List[str] = schema.primary_keys

        options: Dict[str, str] = schema.options

        highest_field_id: int = None

        return TableSchema(
            schema_id,
            fields,
            highest_field_id,
            partition_keys,
            primary_keys,
            options,
            schema.comment,
            int(time.time())
        )

    def copy(self, new_options: Optional[Dict[str, str]] = None) -> "TableSchema":
        return TableSchema(
            id=self.id,
            fields=self.fields,
            highest_field_id=self.highest_field_id,
            partition_keys=self.partition_keys,
            primary_keys=self.primary_keys,
            options=new_options,
            comment=self.comment,
            time_millis=self.time_millis
        )
