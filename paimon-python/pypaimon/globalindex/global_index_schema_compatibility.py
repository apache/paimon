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

"""Validates global indexes against the current table schema."""

from typing import Collection, Dict, List, Set

from pypaimon.schema.data_types import DataTypeParser


def filter_compatible_global_indexes(table, entries: Collection) -> List:
    """Keep entries whose indexed fields have the same logical types."""
    current_schema = table.table_schema
    current_fields = _fields_by_id(current_schema.fields)
    historical_fields = {current_schema.id: current_fields}
    missing_schema_ids: Set[int] = set()
    compatibility_cache = {}
    compatible = []

    for entry in entries:
        global_index = entry.index_file.global_index_meta
        schema_id = entry.schema_id
        if global_index is None or schema_id is None:
            continue

        fields = historical_fields.get(schema_id)
        if fields is None and schema_id not in missing_schema_ids:
            historical_schema = table.schema_manager.get_schema(schema_id)
            if historical_schema is None:
                missing_schema_ids.add(schema_id)
            else:
                fields = _fields_by_id(historical_schema.fields)
                historical_fields[schema_id] = fields

        if fields is None:
            continue
        field_ids = tuple(
            [global_index.index_field_id]
            + list(global_index.extra_field_ids or [])
        )
        cache_key = (schema_id, field_ids)
        if cache_key not in compatibility_cache:
            compatibility_cache[cache_key] = _compatible_indexed_fields(
                field_ids, fields, current_fields)
        if compatibility_cache[cache_key]:
            compatible.append(entry)

    return compatible


def _compatible_indexed_fields(field_ids, historical_fields, current_fields):
    for field_id in field_ids:
        historical_field = historical_fields.get(field_id)
        current_field = current_fields.get(field_id)
        if historical_field is None or current_field is None:
            return False
        if not _equals_ignore_nullable(
                historical_field.type, current_field.type):
            return False
    return True


def _fields_by_id(fields) -> Dict:
    return {field.id: field for field in fields}


def _equals_ignore_nullable(left, right) -> bool:
    left_copy = DataTypeParser.parse_data_type(left.to_dict())
    right_copy = DataTypeParser.parse_data_type(right.to_dict())
    left_copy.nullable = True
    right_copy.nullable = True
    return left_copy == right_copy
