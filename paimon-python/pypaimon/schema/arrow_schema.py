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
import pyarrow.compute as pc


def _normalize_string_type(data_type, ignore_child_nullability=False):
    def child(field):
        field = field.with_type(_normalize_string_type(field.type, ignore_child_nullability))
        return field.with_nullable(True) if ignore_child_nullability else field

    if pa.types.is_large_string(data_type):
        return pa.string()
    if pa.types.is_struct(data_type):
        return pa.struct([child(field) for field in data_type])
    if (pa.types.is_list(data_type) or pa.types.is_large_list(data_type)
            or pa.types.is_fixed_size_list(data_type)):
        field = child(data_type.value_field)
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
            data_type.key_field.with_type(_normalize_string_type(data_type.key_type, ignore_child_nullability)),
            child(data_type.item_field),
            keys_sorted=sorted_keys)
    return data_type


def arrow_schemas_compatible(source, target, check_top_level_nullability=True, allow_binary_compatibility=False,
                             check_nested_nullability=True):
    """Compare ordered fields, ignoring metadata and STRING offset width.

    The writer opts into its existing top-level nullability and binary /
    fixed_size_binary policies. BLOB and numeric types remain distinct.
    """
    if source.names != target.names:
        return False
    for actual, expected in zip(source, target):
        if check_top_level_nullability and actual.nullable != expected.nullable:
            return False
        if (actual.type == expected.type
                or _normalize_string_type(actual.type, not check_nested_nullability)
                == _normalize_string_type(expected.type, not check_nested_nullability)):
            continue
        if allow_binary_compatibility and all(
                pa.types.is_binary(t) or pa.types.is_fixed_size_binary(t)
                for t in (actual.type, expected.type)):
            continue
        return False
    return True


def normalize_arrow_write_nullability(data, target):
    """Restore declared child nullability after checking visible input values.

    Runtime read schemas relax nested nullability. Validate only children of
    non-NULL parents before restoring those declarations, independently of the
    optional Native runtime. This does not convert physical Arrow types.
    """
    options = dict(check_top_level_nullability=False, allow_binary_compatibility=True)
    if (arrow_schemas_compatible(data.schema, target, **options)
            or not arrow_schemas_compatible(data.schema, target, check_nested_nullability=False, **options)):
        return data
    arrays, fields = [], []
    for column, field, expected in zip(data.columns, data.schema, target):
        data_type = _write_child_nullability(column.type, expected.type)
        if data_type != column.type:
            chunks = column.chunks if isinstance(column, pa.ChunkedArray) else [column]
            normalized = []
            for chunk in chunks:
                _check_write_nullability(chunk, expected.with_nullable(True), field.name)
                normalized.append(_with_child_nullability(chunk, data_type))
            column = (pa.chunked_array(normalized, type=data_type)
                      if isinstance(column, pa.ChunkedArray) else normalized[0])
        arrays.append(column)
        fields.append(field.with_type(column.type))
    schema = pa.schema(fields, metadata=data.schema.metadata)
    if isinstance(data, pa.RecordBatch):
        return pa.RecordBatch.from_arrays(arrays, schema=schema)
    return pa.Table.from_arrays(arrays, schema=schema)


def _write_child_nullability(source, target):
    """Copy child declarations without changing layouts or scalar types."""
    def child(actual, expected):
        return expected.with_type(_write_child_nullability(actual.type, expected.type))

    if pa.types.is_struct(source) and pa.types.is_struct(target):
        return pa.struct([child(actual, expected) for actual, expected in zip(source, target)])
    if ((pa.types.is_list(source) or pa.types.is_large_list(source) or pa.types.is_fixed_size_list(source))
            and (pa.types.is_list(target) or pa.types.is_large_list(target) or pa.types.is_fixed_size_list(target))):
        field = child(source.value_field, target.value_field)
        if pa.types.is_large_list(source):
            return pa.large_list(field)
        if pa.types.is_fixed_size_list(source):
            return pa.list_(field, source.list_size)
        return pa.list_(field)
    if pa.types.is_map(source) and pa.types.is_map(target):
        sorted_keys = source == pa.map_(source.key_field, source.item_field, keys_sorted=True)
        return pa.map_(child(source.key_field, target.key_field), child(source.item_field, target.item_field),
                       keys_sorted=sorted_keys)
    return source


def _check_write_nullability(array, field, path):
    if not field.nullable and array.null_count:
        raise ValueError("Input schema isn't consistent with table schema: non-nullable field %s contains NULL" % path)
    data_type = field.type
    if pa.types.is_struct(data_type):
        valid = pc.is_valid(array) if array.null_count else None
        for index, child in enumerate(data_type):
            values = array.field(index)
            if valid is not None:
                values = pc.filter(values, valid)
            _check_write_nullability(values, child, path + '.' + child.name)
    elif (pa.types.is_list(data_type) or pa.types.is_large_list(data_type)
          or pa.types.is_fixed_size_list(data_type)):
        child = data_type.value_field
        _check_write_nullability(pc.list_flatten(array), child, path + '.' + child.name)
    elif pa.types.is_map(data_type):
        # Keys/items expose the full backing array, including values outside a
        # slice or hidden by NULL maps. A temporary list selects visible entries.
        start, stop = array.offsets[0].as_py(), array.offsets[-1].as_py()
        entries = pa.StructArray.from_arrays(
            [array.keys.slice(start, stop - start), array.items.slice(start, stop - start)],
            fields=[array.type.key_field, array.type.item_field])
        offsets = pc.subtract(array.offsets, pa.scalar(start, type=array.offsets.type))
        entries = pc.list_flatten(pa.ListArray.from_arrays(offsets, entries, mask=pc.is_null(array)))
        for index, child in enumerate((data_type.key_field, data_type.item_field)):
            _check_write_nullability(entries.field(index), child, path + '.' + child.name)


def _with_child_nullability(array, data_type):
    """Retype nested wrappers while retaining payloads, slices and validity."""
    if array.type == data_type:
        return array
    if pa.types.is_struct(data_type):
        # StructArray.field() already applies the parent's slice. Construct an
        # offset-zero wrapper so the offset is not applied to children twice.
        children = [_with_child_nullability(array.field(index), child.type)
                    for index, child in enumerate(data_type)]
        return pa.StructArray.from_arrays(children, fields=list(data_type), mask=pc.is_null(array))
    if pa.types.is_map(data_type):
        entries = pa.StructArray.from_arrays(
            [_with_child_nullability(array.keys, data_type.key_type),
             _with_child_nullability(array.items, data_type.item_type)],
            fields=[data_type.key_field, data_type.item_field])
        return pa.Array.from_buffers(data_type, len(array), array.buffers()[:2],
                                     children=[entries], offset=array.offset, null_count=array.null_count)
    if (pa.types.is_list(data_type) or pa.types.is_large_list(data_type)
            or pa.types.is_fixed_size_list(data_type)):
        children = [_with_child_nullability(array.values, data_type.value_type)]
        buffers = array.buffers()[:1 if pa.types.is_fixed_size_list(data_type) else 2]
        return pa.Array.from_buffers(data_type, len(array), buffers, children=children,
                                     offset=array.offset, null_count=array.null_count)
    return array


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


def project_arrow_batch(data, column_names):
    """Project batch fields in order while retaining field/schema metadata."""
    indices = [data.schema.get_field_index(name) for name in column_names]
    missing = [name for name, index in zip(column_names, indices) if index < 0]
    if missing:
        raise KeyError('Columns not found in record batch: %s' % missing)
    if not indices:
        # With no child arrays, from_arrays cannot infer the batch row count.
        rows = pa.StructArray.from_buffers(pa.struct([]), data.num_rows, [None])
        return pa.RecordBatch.from_struct_array(rows).replace_schema_metadata(data.schema.metadata)
    schema = pa.schema([data.schema.field(index) for index in indices], metadata=data.schema.metadata)
    return pa.RecordBatch.from_arrays([data.column(index) for index in indices], schema=schema)
