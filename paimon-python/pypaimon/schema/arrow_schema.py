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

"""STRING compatibility and canonical Arrow input for core writers."""

import pyarrow as pa


def _normalize_string_type(data_type):
    if pa.types.is_large_string(data_type):
        return pa.string()
    if pa.types.is_struct(data_type):
        return pa.struct([field.with_type(_normalize_string_type(field.type)) for field in data_type])
    if (pa.types.is_list(data_type) or pa.types.is_large_list(data_type)
            or pa.types.is_fixed_size_list(data_type)):
        field = data_type.value_field.with_type(_normalize_string_type(data_type.value_type))
        if pa.types.is_large_list(data_type):
            return pa.large_list(field)
        if pa.types.is_fixed_size_list(data_type):
            return pa.list_(field, data_type.list_size)
        return pa.list_(field)
    if pa.types.is_map(data_type):
        # Arrow 6/12 have no MapType.keys_sorted property, but type equality
        # includes the sorting flag. Preserve it along with the child fields.
        sorted_keys = data_type == pa.map_(data_type.key_field, data_type.item_field, keys_sorted=True)
        return pa.map_(
            data_type.key_field.with_type(_normalize_string_type(data_type.key_type)),
            data_type.item_field.with_type(_normalize_string_type(data_type.item_type)),
            keys_sorted=sorted_keys)
    return data_type


def arrow_schemas_compatible(source, target, check_top_level_nullability=True, allow_binary_compatibility=False):
    """Compare ordered fields, ignoring metadata and STRING offset width.

    The writer opts into its existing top-level nullability and binary /
    fixed_size_binary policies. BLOB and numeric types remain distinct.
    """
    if source.names != target.names:
        return False
    for actual, expected in zip(source, target):
        if check_top_level_nullability and actual.nullable != expected.nullable:
            return False
        if actual.type == expected.type or _normalize_string_type(actual.type) == _normalize_string_type(expected.type):
            continue
        if allow_binary_compatibility and all(
                pa.types.is_binary(t) or pa.types.is_fixed_size_binary(t)
                for t in (actual.type, expected.type)):
            continue
        return False
    return True


def normalize_arrow_strings(data):
    """Safely normalize large_string input before routing or buffering rows.

    Only STRING layouts change. Keep all other types, names, nullability and
    metadata. Conversion limits and nested cast support belong to PyArrow;
    unsupported inputs fail before reaching a data writer.
    """
    schema = pa.schema([
        field.with_type(_normalize_string_type(field.type)) for field in data.schema
    ], metadata=data.schema.metadata)
    if data.schema.equals(schema, check_metadata=False):
        return data
    try:
        if isinstance(data, pa.RecordBatch):
            # RecordBatch.cast is unavailable in Arrow 6/12. Casting through
            # Table produces one chunk per column, or no chunks for empty input.
            table = pa.Table.from_batches([data]).cast(schema, safe=True)
            arrays = [column.chunk(0) if column.num_chunks else pa.array([], type=field.type)
                      for column, field in zip(table.columns, schema)]
            return pa.RecordBatch.from_arrays(arrays, schema=schema)
        return data.cast(schema, safe=True)
    except (ValueError, TypeError, NotImplementedError) as error:
        raise ValueError(
            "Cannot convert large_string input to string with PyArrow %s: %s"
            % (pa.__version__, error)) from error
