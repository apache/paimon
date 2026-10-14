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

from collections.abc import Mapping
from typing import Any, Dict, Iterable, List

import pyarrow as pa

from pypaimon.schema.data_types import DataField, PyarrowFieldParser, is_blob_file_field
from pypaimon.table.row.blob import Blob, BlobView
from pypaimon.table.row.internal_row import InternalRow
from pypaimon.table.row.vector import Vector


def validate_write_columns(write_cols):
    """Validate field selections before changing a live writer's configuration."""
    names = list(write_cols)
    if not names:
        raise ValueError('write columns must not be empty')
    seen = set()
    for name in names:
        if name in seen:
            raise ValueError("Duplicate write column '%s'" % name)
        seen.add(name)
    return names


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


def inline_blob_value(value: Any, descriptor: bool, view: bool) -> Any:
    """Serialize row objects to inline references without opening payloads."""
    if descriptor and isinstance(value, Blob):
        try:
            return value.to_descriptor().serialize()
        except Exception as error:
            raise ValueError(
                "blob-descriptor-field row values must be serialized "
                "BlobDescriptor bytes or a Blob with a descriptor."
            ) from error
    if view and isinstance(value, BlobView):
        return value.view_struct.serialize()
    return value


def _inline_reference_bytes(value: Any, option: str, struct_name: str) -> bytes:
    if hasattr(value, 'as_py'):
        value = value.as_py()
    if isinstance(value, str):
        value = value.encode('utf-8')
    if not isinstance(value, (bytes, bytearray)):
        raise ValueError(
            "%s requires blob field value to be a serialized %s." % (option, struct_name))
    return bytes(value)


def validate_inline_blob_fields(data: pa.RecordBatch, descriptor_fields, view_fields):
    """Reject raw payloads in fields that store serialized BLOB references inline."""
    from pypaimon.table.row.blob import BlobDescriptor, BlobViewStruct

    for field_name in descriptor_fields:
        if field_name not in data.schema.names:
            continue
        for value in data.column(data.schema.get_field_index(field_name)).to_pylist():
            if value is None:
                continue
            descriptor_bytes = _inline_reference_bytes(
                value, 'blob-descriptor-field', 'BlobDescriptor')
            # Like Java, schema-declared descriptors use the prefix parser.
            # Exact wire length is only needed when detecting descriptors
            # among arbitrary payload bytes.
            try:
                BlobDescriptor.deserialize(descriptor_bytes)
            except Exception as e:
                raise ValueError(
                    "blob-descriptor-field requires blob field value to be a serialized "
                    "BlobDescriptor."
                ) from e

    for field_name in view_fields:
        if field_name not in data.schema.names:
            continue
        for value in data.column(data.schema.get_field_index(field_name)).to_pylist():
            if value is None:
                continue
            view_bytes = _inline_reference_bytes(value, 'blob-view-field', 'BlobViewStruct')
            try:
                view_struct = BlobViewStruct.deserialize(view_bytes)
                if view_struct.serialize() != view_bytes:
                    raise ValueError("BlobViewStruct payload contains trailing bytes.")
            except Exception as e:
                raise ValueError(
                    "blob-view-field requires blob field value to be a serialized "
                    "BlobViewStruct."
                ) from e


def value_for_arrow(value: Any, field: DataField) -> Any:
    if isinstance(value, Vector):
        return value.to_list()
    if isinstance(value, Blob) or (
        is_blob_file_field(field)
        and _contains_blob_value(value)
    ):
        raise ValueError(
            "Blob values cannot be converted to Arrow without materializing "
            "the stream. Use a Row-aware blob write path."
        )
    return value


def _contains_blob_value(value: Any) -> bool:
    if isinstance(value, Blob):
        return True
    if isinstance(value, Mapping):
        return any(_contains_blob_value(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(_contains_blob_value(item) for item in value)
    return False


def row_values_to_arrow_table(
        values_by_name: Dict[str, Any],
        fields: List[DataField],
        column_names: List[str]) -> pa.Table:
    field_by_name = {field.name: field for field in fields}
    selected_fields = [field_by_name[name] for name in column_names]
    schema = PyarrowFieldParser.from_paimon_schema(selected_fields)
    data = {
        name: [value_for_arrow(values_by_name[name], field_by_name[name])]
        for name in column_names
    }
    return pa.Table.from_pydict(data, schema=schema)
