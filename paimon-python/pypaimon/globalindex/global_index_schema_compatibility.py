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

from typing import Collection, Dict, List, Set, Tuple

from pypaimon.schema.data_types import DataTypeParser


def filter_compatible_global_indexes(table, entries: Collection) -> List:
    """Keep entries whose indexed fields have the same logical types."""
    return partition_global_indexes_by_compatibility(table, entries)[0]


def partition_global_indexes_by_compatibility(
    table, entries: Collection
) -> Tuple[List, List]:
    """Group manifest entries by compatibility with the current schema."""
    checker = _CompatibilityChecker(table)
    compatible = []
    incompatible = []

    for entry in entries:
        target = (compatible if checker.is_compatible(
            entry.index_file, entry.schema_id) else incompatible)
        target.append(entry)
    return compatible, incompatible


def filter_compatible_global_index_files(table, index_files: Collection) -> List:
    """Keep index files compatible with the current schema."""
    checker = _CompatibilityChecker(table)
    return [
        index_file for index_file in index_files
        if checker.is_compatible(
            index_file, getattr(index_file, "schema_id", None))
    ]


class _CompatibilityChecker:

    def __init__(self, table):
        self._table = table
        current_schema = table.table_schema
        self._current_fields = _fields_by_id(current_schema.fields)
        self._historical_fields = {
            current_schema.id: self._current_fields,
        }
        self._missing_schema_ids: Set[int] = set()
        self._compatibility_cache = {}

    def is_compatible(self, index_file, schema_id) -> bool:
        global_index = index_file.global_index_meta
        if global_index is None or schema_id is None:
            return False

        fields = self._historical_fields.get(schema_id)
        if fields is None and schema_id not in self._missing_schema_ids:
            historical_schema = self._table.schema_manager.get_schema(schema_id)
            if historical_schema is None:
                self._missing_schema_ids.add(schema_id)
            else:
                fields = _fields_by_id(historical_schema.fields)
                self._historical_fields[schema_id] = fields

        if fields is None:
            return False
        field_ids = tuple(
            [global_index.index_field_id]
            + list(global_index.extra_field_ids or [])
        )
        cache_key = (schema_id, field_ids)
        if cache_key not in self._compatibility_cache:
            self._compatibility_cache[cache_key] = _compatible_indexed_fields(
                field_ids, fields, self._current_fields)
        return self._compatibility_cache[cache_key]


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
