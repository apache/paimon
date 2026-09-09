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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Shared Arrow schema validation for multimodal format importers."""

import pyarrow as pa
import pyarrow.compute as pc


def strict_arrow_table(
        data,
        target_schema,
        source_path,
        batch_index,
        format_name):
    if isinstance(data, pa.RecordBatch):
        table = pa.Table.from_batches([data])
    elif isinstance(data, pa.Table):
        table = data
    else:
        raise ValueError(
            "%s transform must return Arrow data or an iterable of Arrow data."
            % format_name)

    missing = [
        name for name in target_schema.names if name not in table.column_names
    ]
    if missing:
        raise ValueError(
            "%s batch %d from %s is missing columns: %s"
            % (format_name, batch_index, source_path, missing))
    extra = [
        name for name in table.column_names if name not in target_schema.names
    ]
    if extra:
        raise ValueError(
            "%s batch %d from %s has unexpected columns: %s"
            % (format_name, batch_index, source_path, extra))
    if table.column_names != target_schema.names:
        raise ValueError(
            "%s batch %d from %s has columns in the wrong order: %s; "
            "expected %s."
            % (format_name, batch_index, source_path, table.column_names,
               target_schema.names))
    try:
        _validate_nested_nullability(table, target_schema)
        if table.schema.equals(target_schema, check_metadata=False):
            return table
        casted = table.cast(target_schema, safe=True)
        _validate_nested_nullability(casted, target_schema)
        return casted
    except (ValueError, TypeError, NotImplementedError) as error:
        raise ValueError(
            "%s batch %d from %s cannot be converted to the table schema: %s"
            % (format_name, batch_index, source_path, error)) from error


def _validate_nested_nullability(table, schema):
    for field, column in zip(schema, table.columns):
        for chunk in column.chunks:
            _validate_array_nullability(chunk, field, field.name)


def _validate_array_nullability(array, field, path):
    if not field.nullable and array.null_count:
        raise ValueError(
            "non-nullable field %s contains %d null value(s)"
            % (path, array.null_count))

    target_type = field.type
    source_type = array.type
    if (pa.types.is_list(target_type)
            or pa.types.is_large_list(target_type)
            or pa.types.is_fixed_size_list(target_type)):
        if not (pa.types.is_list(source_type)
                or pa.types.is_large_list(source_type)
                or pa.types.is_fixed_size_list(source_type)):
            return
        _validate_array_nullability(
            pc.list_flatten(array),
            target_type.value_field,
            "%s.%s" % (path, target_type.value_field.name),
        )
        return

    if pa.types.is_map(target_type):
        if not pa.types.is_map(source_type):
            return
        start = array.offsets[0].as_py()
        stop = array.offsets[-1].as_py()
        length = stop - start
        offsets = pc.subtract(
            array.offsets,
            pa.scalar(start, type=array.offsets.type),
        )
        entries = pa.StructArray.from_arrays(
            [array.keys.slice(start, length),
             array.items.slice(start, length)],
            fields=[source_type.key_field, source_type.item_field],
        )
        logical_entries = pc.list_flatten(pa.ListArray.from_arrays(
            offsets,
            entries,
            mask=pc.is_null(array),
        ))
        _validate_array_nullability(
            logical_entries.field(0), target_type.key_field,
            "%s.%s" % (path, target_type.key_field.name))
        _validate_array_nullability(
            logical_entries.field(1), target_type.item_field,
            "%s.%s" % (path, target_type.item_field.name))
        return

    if pa.types.is_struct(target_type):
        if not pa.types.is_struct(source_type):
            return
        parent_valid = pc.is_valid(array) if array.null_count else None
        for index, child_field in enumerate(target_type):
            child = array.field(index)
            if parent_valid is not None:
                child = pc.filter(child, parent_valid)
            _validate_array_nullability(
                child,
                child_field,
                "%s.%s" % (path, child_field.name),
            )
