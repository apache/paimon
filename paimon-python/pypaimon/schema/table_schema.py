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

import json
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.file_io import FileIO
from pypaimon.common.json_util import json_field
from pypaimon.schema.data_types import DataField
from pypaimon.schema.schema import Schema


@dataclass
class TableSchema:
    PAIMON_07_VERSION = 1
    PAIMON_08_VERSION = 2
    CURRENT_VERSION = 3

    FIELD_VERSION = "version"
    FIELD_ID = "id"
    FIELD_FIELDS = "fields"
    FIELD_HIGHEST_FIELD_ID = "highestFieldId"
    FIELD_PARTITION_KEYS = "partitionKeys"
    FIELD_PRIMARY_KEYS = "primaryKeys"
    FIELD_OPTIONS = "options"
    FIELD_COMMENT = "comment"
    FIELD_TIME_MILLIS = "timeMillis"

    version: int = json_field(FIELD_VERSION, default=CURRENT_VERSION)
    id: int = json_field(FIELD_ID, default=0)
    fields: List[DataField] = json_field(FIELD_FIELDS, default_factory=list)
    highest_field_id: int = json_field("highestFieldId", default=0)
    partition_keys: List[str] = json_field(
        FIELD_PARTITION_KEYS, default_factory=list)
    primary_keys: List[str] = json_field(
        FIELD_PRIMARY_KEYS, default_factory=list)
    options: Dict[str, str] = json_field(FIELD_OPTIONS, default_factory=dict)
    comment: Optional[str] = json_field(FIELD_COMMENT, default=None)
    time_millis: int = json_field("timeMillis", default_factory=lambda: int(time.time() * 1000))

    def cross_partition_update(self) -> bool:
        if not self.primary_keys or not self.partition_keys:
            return False

        # Check if primary keys contain all partition keys
        # Return True if they don't contain all (cross-partition update)
        return not all(pk in self.primary_keys for pk in self.partition_keys)

    def to_schema(self) -> Schema:
        return Schema(
            fields=self.fields,
            partition_keys=self.partition_keys,
            primary_keys=self.primary_keys,
            options=self.options,
            comment=self.comment
        )

    @staticmethod
    def from_schema(schema_id: int, schema: Schema) -> "TableSchema":
        fields: List[DataField] = schema.fields
        partition_keys: List[str] = schema.partition_keys
        primary_keys: List[str] = schema.primary_keys
        options: Dict[str, str] = schema.options
        highest_field_id: int = max(field.id for field in fields)

        return TableSchema(
            TableSchema.CURRENT_VERSION,
            schema_id,
            fields,
            highest_field_id,
            partition_keys,
            primary_keys,
            options,
            schema.comment
        )

    @staticmethod
    def from_path(file_io: FileIO, schema_path: str):
        try:
            json_str = file_io.read_file_utf8(schema_path)
            return TableSchema.from_json(json_str)
        except FileNotFoundError as e:
            raise RuntimeError(f"Schema file not found: {schema_path}") from e
        except Exception as e:
            raise RuntimeError(f"Failed to read schema from {schema_path}") from e

    @staticmethod
    def from_json(json_str: str):
        try:
            data = json.loads(json_str)
            version = data.get(TableSchema.FIELD_VERSION, TableSchema.PAIMON_07_VERSION)
            fields = [DataField.from_dict(field) for field in data[TableSchema.FIELD_FIELDS]]
            options = data[TableSchema.FIELD_OPTIONS]
            if version <= TableSchema.PAIMON_07_VERSION and CoreOptions.BUCKET.key() not in options:
                options[CoreOptions.BUCKET.key()] = "1"
            if version <= TableSchema.PAIMON_08_VERSION and CoreOptions.FILE_FORMAT.key() not in options:
                options[CoreOptions.FILE_FORMAT.key()] = "orc"

            return TableSchema(
                version=version,
                id=data[TableSchema.FIELD_ID],
                fields=fields,
                highest_field_id=data[TableSchema.FIELD_HIGHEST_FIELD_ID],
                partition_keys=data[TableSchema.FIELD_PARTITION_KEYS],
                primary_keys=data[TableSchema.FIELD_PRIMARY_KEYS],
                options=options or {},
                comment=data.get(TableSchema.FIELD_COMMENT),
                time_millis=data.get(TableSchema.FIELD_TIME_MILLIS)
            )
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Invalid JSON format: {json_str}") from e
        except KeyError as e:
            raise RuntimeError(f"Missing required field in schema JSON: {e}") from e
        except Exception as e:
            raise RuntimeError(f"Failed to parse schema from JSON: {e}") from e

    def copy(self, new_options: Optional[Dict[str, str]] = None) -> "TableSchema":
        return TableSchema(
            version=self.version,
            id=self.id,
            fields=self.fields,
            highest_field_id=self.highest_field_id,
            partition_keys=self.partition_keys,
            primary_keys=self.primary_keys,
            options=new_options,
            comment=self.comment,
            time_millis=self.time_millis
        )
