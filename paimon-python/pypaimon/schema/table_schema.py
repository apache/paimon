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
from pathlib import Path
from typing import List, Dict, Optional

import pyarrow

from pypaimon.api import data_types
from pypaimon.api.core_options import CoreOptions
from pypaimon.common.file_io import FileIO
from pypaimon.schema.schema import Schema
from pypaimon.api.data_types import DataField


class TableSchema:
    PAIMON_07_VERSION = 1
    PAIMON_08_VERSION = 2
    CURRENT_VERSION = 3

    def __init__(self, version: int, id: int, fields: List[DataField], highest_field_id: int,
                 partition_keys: List[str], primary_keys: List[str], options: Dict[str, str],
                 comment: Optional[str] = None, time_millis: Optional[int] = None):
        self.version = version
        self.id = id
        self.fields = fields
        self.highest_field_id = highest_field_id
        self.partition_keys = partition_keys or []
        self.primary_keys = primary_keys or []
        self.options = options or {}
        self.comment = comment
        self.time_millis = time_millis if time_millis is not None else int(time.time() * 1000)

    def to_schema(self) -> Schema:
        try:
            pa_fields = []
            for field in self.fields:
                pa_fields.append(field.to_pyarrow_field())
            pyarrow.schema(pa_fields)
        except Exception as e:
            print(e)
        return Schema(
            fields=self.fields,
            partition_keys=self.partition_keys,
            primary_keys=self.primary_keys,
            options=self.options,
            comment=self.comment
        )

    @staticmethod
    def from_path(file_io: FileIO, schema_path: Path):
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

            version = data.get("version", TableSchema.PAIMON_07_VERSION)
            options = data["options"]
            if version <= TableSchema.PAIMON_07_VERSION and CoreOptions.BUCKET not in options:
                options[CoreOptions.BUCKET] = "1"
            if version <= TableSchema.PAIMON_08_VERSION and CoreOptions.FILE_FORMAT not in options:
                options[CoreOptions.FILE_FORMAT] = "orc"
            fields = [DataField.from_dict(field) for field in data["fields"]]

            return TableSchema(
                version=version,
                id=data["id"],
                fields=fields,
                highest_field_id=data["highestFieldId"],
                partition_keys=data["partitionKeys"],
                primary_keys=data["primaryKeys"],
                options=options,
                comment=data.get("comment"),
                time_millis=data.get("timeMillis")
            )
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Invalid JSON format: {json_str}") from e
        except KeyError as e:
            raise RuntimeError(f"Missing required field in schema JSON: {e}") from e
        except Exception as e:
            raise RuntimeError(f"Failed to parse schema from JSON: {e}") from e

    @staticmethod
    def from_schema(schema_id: int, schema: Schema) -> "TableSchema":
        fields: List[DataField] = schema.fields
        if not schema.fields:
            fields = data_types.parse_data_fields_from_pyarrow_schema(schema.pa_schema)
        partition_keys: List[str] = schema.partition_keys
        primary_keys: List[str] = schema.primary_keys
        options: Dict[str, str] = schema.options
        highest_field_id: int = None  # max(field.id for field in fields)

        return TableSchema(
            TableSchema.CURRENT_VERSION,
            schema_id,
            fields,
            highest_field_id,
            partition_keys,
            primary_keys,
            options,
            schema.comment,
            int(time.time())
        )

    def to_json(self) -> str:
        data = {
            "version": self.version,
            "id": self.id,
            "fields": [field.to_dict() for field in self.fields],
            "highestFieldId": self.highest_field_id,
            "partitionKeys": self.partition_keys,
            "primaryKeys": self.primary_keys,
            "options": self.options,
            "timeMillis": self.time_millis
        }
        if self.comment is not None:
            data["comment"] = self.comment
        return json.dumps(data, indent=2, ensure_ascii=False)

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

    def get_primary_key_fields(self) -> List[DataField]:
        if not self.primary_keys:
            return []
        field_map = {field.name: field for field in self.fields}
        return [field_map[name] for name in self.primary_keys if name in field_map]

    def get_partition_key_fields(self) -> List[DataField]:
        if not self.partition_keys:
            return []
        field_map = {field.name: field for field in self.fields}
        return [field_map[name] for name in self.partition_keys if name in field_map]

    def get_trimmed_primary_key_fields(self) -> List[DataField]:
        if not self.primary_keys or not self.partition_keys:
            return self.get_primary_key_fields()
        adjusted = [pk for pk in self.primary_keys if pk not in self.partition_keys]
        field_map = {field.name: field for field in self.fields}
        return [field_map[name] for name in adjusted if name in field_map]
