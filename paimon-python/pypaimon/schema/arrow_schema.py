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

"""Arrow compatibility and layout conversion at Paimon's data boundaries.

Paimon STRING accepts both Arrow string offset widths. This does not make
arbitrary Arrow-castable types compatible: in particular, binary and
large_binary represent different Paimon types (BYTES and BLOB).
"""

import pyarrow as pa
import pyarrow.compute as pc


def _is_string(data_type):
    return pa.types.is_string(data_type) or pa.types.is_large_string(data_type)


def _is_list(data_type):
    return (pa.types.is_list(data_type) or pa.types.is_large_list(data_type)
            or pa.types.is_fixed_size_list(data_type))


def _map_string_layout(data_type, other_type=None, promote=False):
    # Retain the first type's structure, names, nullability, and metadata.
    # Without a partner this is a comparison-only canonical form. Otherwise
    # adopt the partner's string widths, or widen differing widths for concat.
    if other_type is not None and data_type == other_type:
        return data_type
    if _is_string(data_type):
        if other_type is None:
            return pa.string()
        if _is_string(other_type):
            return pa.large_string() if promote and data_type != other_type else other_type
        return data_type
    if pa.types.is_struct(data_type):
        def partner(field):
            if other_type is not None and pa.types.is_struct(other_type):
                index = other_type.get_field_index(field.name)
                if index >= 0:
                    return other_type[index].type
            return None
        return pa.struct([
            field.with_type(_map_string_layout(field.type, partner(field), promote))
            if other_type is None or partner(field) is not None else field
            for field in data_type
        ])
    if _is_list(data_type):
        if other_type is not None and not _is_list(other_type):
            return data_type
        field = data_type.value_field.with_type(_map_string_layout(
            data_type.value_type,
            other_type.value_type if other_type is not None else None, promote))
        if pa.types.is_large_list(data_type):
            return pa.large_list(field)
        if pa.types.is_fixed_size_list(data_type):
            return pa.list_(field, data_type.list_size)
        return pa.list_(field)
    if pa.types.is_map(data_type):
        if other_type is not None and not pa.types.is_map(other_type):
            return data_type
        return pa.map_(
            data_type.key_field.with_type(_map_string_layout(
                data_type.key_type, other_type.key_type if other_type is not None else None, promote)),
            data_type.item_field.with_type(_map_string_layout(
                data_type.item_type, other_type.item_type if other_type is not None else None, promote)),
            keys_sorted=data_type.keys_sorted,
        )
    return data_type


def arrow_types_compatible(source, target):
    """Compare logical types, allowing only different STRING layouts.

    Nested field names, order and nullability remain part of the contract.
    This function neither converts values nor permits numeric promotions.
    """
    return source == target or _map_string_layout(source) == _map_string_layout(target)


def arrow_schemas_compatible(source, target, check_top_level_nullability=True, allow_binary_compatibility=False):
    """Compare ordered fields without comparing metadata.

    The core writer historically ignores top-level nullability and accepts
    binary/fixed_size_binary interchangeably. Its callers opt into those
    policies explicitly; neither policy applies recursively or to BLOB.
    """
    if source.names != target.names:
        return False
    for actual, expected in zip(source, target):
        if check_top_level_nullability and actual.nullable != expected.nullable:
            return False
        if arrow_types_compatible(actual.type, expected.type):
            continue
        if allow_binary_compatibility and all(
                pa.types.is_binary(t) or pa.types.is_fixed_size_binary(t)
                for t in (actual.type, expected.type)):
            continue
        return False
    return True


def schema_with_source_string_layout(target, source):
    """Use target fields and value types but retain source STRING layouts."""
    fields = []
    for field in target:
        index = source.get_field_index(field.name)
        fields.append(field.with_type(_map_string_layout(field.type, source[index].type))
                      if index >= 0 else field)
    return pa.schema(fields, metadata=target.metadata)


def merge_arrow_schemas(left, right):
    """Choose a concat layout for compatible schemas, widening STRING only.

    Callers must first check ``arrow_schemas_compatible``. Metadata comes
    from the left schema, following Arrow concat semantics.
    """
    return pa.schema([
        field.with_type(_map_string_layout(field.type, right[i].type, promote=True))
        for i, field in enumerate(left)
    ], metadata=left.metadata)


def cast_arrow_array(array, target_type, safe=True, preserve_string_layout=False):
    """Cast values explicitly, with checked STRING offset conversion.

    Input adapters can retain the source layout while casting other types.
    Readers request the canonical output layout. Even when numeric schema
    evolution allows truncation (safe=False), string offsets are checked.
    """
    if preserve_string_layout:
        target_type = _map_string_layout(target_type, array.type)
    if array.type == target_type:
        return array
    if not safe:
        layout_type = _map_string_layout(array.type, target_type)
        if array.type != layout_type:
            array = array.cast(layout_type, safe=True)
            if array.type == target_type:
                return array
    return array.cast(target_type, safe=safe)


def prepare_arrow_input(data, target_schema, validate_nullability=False):
    """Safely convert ordered Arrow input while retaining STRING layouts.

    This is an explicit value-conversion boundary, not a compatibility test.
    Column selection, reordering and missing-column policy belong to callers.
    The result keeps the input container kind (Table or RecordBatch).
    """
    if data.schema.names != target_schema.names:
        raise ValueError("Arrow columns must match the target schema in order")
    if validate_nullability:
        _validate_nested_nullability(data, target_schema)
    layout_schema = schema_with_source_string_layout(target_schema, data.schema)
    if data.schema.equals(layout_schema, check_metadata=False):
        return data
    # Container.cast also enforces top-level NOT NULL, unlike from_arrays.
    result = data.cast(layout_schema, safe=True)
    if validate_nullability:
        _validate_nested_nullability(result, target_schema)
    return result


def _validate_nested_nullability(data, schema):
    for field, column in zip(schema, data.columns):
        chunks = column.chunks if isinstance(column, pa.ChunkedArray) else [column]
        for chunk in chunks:
            _validate_array_nullability(chunk, field, field.name)


def _validate_array_nullability(array, field, path):
    if not field.nullable and array.null_count:
        raise ValueError(
            "non-nullable field %s contains %d null value(s)"
            % (path, array.null_count))

    target_type = field.type
    source_type = array.type
    if _is_list(target_type):
        if not _is_list(source_type):
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
        for child_field in target_type:
            index = source_type.get_field_index(child_field.name)
            if index < 0:
                # Let Arrow reject or pad missing children during the cast;
                # the post-cast validation checks any resulting nulls.
                continue
            child = array.field(index)
            if parent_valid is not None:
                child = pc.filter(child, parent_valid)
            _validate_array_nullability(
                child,
                child_field,
                "%s.%s" % (path, child_field.name),
            )
