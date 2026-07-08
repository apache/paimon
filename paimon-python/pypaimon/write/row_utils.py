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

from typing import Any, Dict, Iterable, List

import pyarrow as pa

from pypaimon.schema.data_types import DataField, PyarrowFieldParser
from pypaimon.table.row.blob import Blob
from pypaimon.table.row.internal_row import InternalRow
from pypaimon.table.row.vector import Vector


def is_blob_field(field: DataField) -> bool:
    return getattr(field.type, 'type', None) == 'BLOB'


def row_to_named_values(
        row: InternalRow, table_fields: List[DataField]) -> Dict[str, Any]:
    if hasattr(row, 'fields') and getattr(row, 'fields') is not None:
        row_fields = getattr(row, 'fields')
        return {
            field.name: row.get_field(i)
            for i, field in enumerate(row_fields)
        }

    if len(row) != len(table_fields):
        raise ValueError(
            "Rows without field metadata must have the same arity as the "
            f"table schema: {len(row)} != {len(table_fields)}."
        )
    return {
        field.name: row.get_field(i)
        for i, field in enumerate(table_fields)
    }


def require_columns(
        values_by_name: Dict[str, Any],
        column_names: Iterable[str],
        context: str) -> None:
    missing = [
        column_name
        for column_name in column_names
        if column_name not in values_by_name
    ]
    if missing:
        raise ValueError(
            f"{context} requires row field(s) {missing}, "
            f"but row only contains {list(values_by_name.keys())}."
        )


def value_for_arrow(value: Any) -> Any:
    if isinstance(value, Vector):
        return value.to_list()
    if isinstance(value, Blob):
        raise ValueError(
            "Blob values cannot be converted to Arrow without materializing "
            "the stream. Use a Row-aware blob write path."
        )
    return value


def row_values_to_arrow_table(
        values_by_name: Dict[str, Any],
        fields: List[DataField],
        column_names: List[str]) -> pa.Table:
    field_by_name = {field.name: field for field in fields}
    selected_fields = [field_by_name[name] for name in column_names]
    schema = PyarrowFieldParser.from_paimon_schema(selected_fields)
    data = {
        name: [value_for_arrow(values_by_name[name])]
        for name in column_names
    }
    return pa.Table.from_pydict(data, schema=schema)
