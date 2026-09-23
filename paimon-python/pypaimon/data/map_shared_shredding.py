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

"""Metadata and read support for the shared-shredding MAP layout."""

import json
import struct
from copy import copy
from typing import Dict, List, Optional, Sequence, Set, Tuple, Union, cast

import pyarrow as pa
import pyarrow.compute as pc


_STORAGE_LAYOUT = b"paimon.map.storage-layout"
_VERSION = b"paimon.map.shared-shredding.version"
_FIELD_DICT = b"paimon.map.shared-shredding.field-dict"
_FIELD_DICT_COMPRESSION = b"paimon.map.shared-shredding.field-dict-compression"
_FIELD_DICT_ORIGINAL_SIZE = b"paimon.map.shared-shredding.field-dict-original-size"
_NUM_COLUMNS = b"paimon.map.shared-shredding.num-columns"
_FIELD_COLUMNS = b"paimon.map.shared-shredding.field-columns"
_OVERFLOW_SET = b"paimon.map.shared-shredding.overflow-set"
_MAX_ROW_WIDTH = b"paimon.map.shared-shredding.max-row-width"
_FIELD_MAPPING = "__field_mapping"
_OVERFLOW = "__overflow"
_PHYSICAL_COLUMN_PREFIX = "__col_"
_SELECTED_KEYS_PREFIX = "__PAIMON_MAP_SELECTED_KEYS:"
_SELECTED_KEYS_DELIMITER = ";"
_VECTORIZED_MIN_ROWS = 1024
_VECTORIZED_MAX_CANDIDATE_PAIRS = 16

_SelectedKeyMetadata = Tuple[Dict[int, str], Dict[int, List[int]], Set[int], int]
_FieldMappings = List[Optional[List[Optional[int]]]]


def is_shared_shredding(field: pa.Field) -> bool:
    metadata = field.metadata
    return metadata is not None and metadata.get(_STORAGE_LAYOUT) == b"shared-shredding"


def parse_shared_shredding_metadata(field: pa.Field):
    metadata = field.metadata or {}
    version = _required_int(metadata, _VERSION)
    if version != 1:
        raise ValueError(
            "Unsupported shared-shredding metadata version: {}".format(version))

    original_size = _required_int(metadata, _FIELD_DICT_ORIGINAL_SIZE)
    compression = metadata.get(_FIELD_DICT_COMPRESSION, b"zstd").decode("utf-8").lower()
    encoded_dict = _required(metadata, _FIELD_DICT).decode("utf-8").encode("latin-1")
    field_dict = json.loads(
        _decompress(encoded_dict, original_size, compression).decode("utf-8"))
    if not isinstance(field_dict, dict):
        raise ValueError("Shared-shredding field dictionary must be an object")
    if not all(
            isinstance(name, str) and isinstance(field_id, int)
            for name, field_id in field_dict.items()):
        raise ValueError("Shared-shredding field dictionary is malformed")
    name_by_id = {field_id: name for name, field_id in field_dict.items()}
    num_columns = _required_int(metadata, _NUM_COLUMNS)
    if num_columns < 0:
        raise ValueError("Shared-shredding column count must not be negative")
    return name_by_id, num_columns


def parse_shared_shredding_selection_metadata(field: pa.Field):
    """Return file-local placement metadata required for key pruning."""
    name_by_id, num_columns = parse_shared_shredding_metadata(field)
    metadata = field.metadata or {}
    field_columns_json = json.loads(
        _required(metadata, _FIELD_COLUMNS).decode("utf-8"))
    if not isinstance(field_columns_json, dict):
        raise ValueError("Shared-shredding field columns must be an object")
    try:
        field_to_columns = {
            int(field_id): list(columns)
            for field_id, columns in field_columns_json.items()
        }
    except (TypeError, ValueError):
        raise ValueError("Shared-shredding field columns are malformed")
    if not all(
            isinstance(column, int) and 0 <= column < num_columns
            for columns in field_to_columns.values()
            if isinstance(columns, list)
            for column in columns):
        raise ValueError("Shared-shredding field columns are malformed")
    if not all(isinstance(columns, list)
               for columns in field_to_columns.values()):
        raise ValueError("Shared-shredding field columns are malformed")

    overflow_json = json.loads(
        _required(metadata, _OVERFLOW_SET).decode("utf-8"))
    if (not isinstance(overflow_json, list)
            or not all(isinstance(field_id, int)
                       for field_id in overflow_json)):
        raise ValueError("Shared-shredding overflow set is malformed")
    return name_by_id, field_to_columns, set(overflow_json), num_columns


def shared_shredding_metadata(
        name_to_id, field_to_columns, overflow_set, num_columns,
        max_row_width, compression):
    """Build Java-compatible Arrow field metadata for one data file."""
    compression = compression.lower()
    if compression not in ("none", "lz4", "zstd"):
        raise ValueError(
            "MAP shared-shredding only supports none/lz4/zstd compression, "
            "but is {}.".format(compression))
    field_dict = json.dumps(
        dict(sorted(name_to_id.items())),
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    encoded_dict = _compress(field_dict, compression)
    columns = {
        str(field_id): sorted(column_ids)
        for field_id, column_ids in sorted(field_to_columns.items())
    }
    return {
        _STORAGE_LAYOUT: b"shared-shredding",
        _VERSION: b"1",
        _FIELD_DICT: encoded_dict.decode("latin-1").encode("utf-8"),
        _FIELD_DICT_COMPRESSION: compression.encode("utf-8"),
        _FIELD_DICT_ORIGINAL_SIZE: str(len(field_dict)).encode("utf-8"),
        _FIELD_COLUMNS: json.dumps(
            columns, separators=(",", ":"), sort_keys=True).encode("utf-8"),
        _OVERFLOW_SET: json.dumps(
            sorted(overflow_set), separators=(",", ":")).encode("utf-8"),
        _NUM_COLUMNS: str(num_columns).encode("utf-8"),
        _MAX_ROW_WIDTH: str(max_row_width).encode("utf-8"),
    }


def map_selected_keys(description: str) -> List[str]:
    if not description or not description.startswith(_SELECTED_KEYS_PREFIX):
        raise ValueError("Invalid selected-key MAP metadata: {}".format(
            description))
    return description[len(_SELECTED_KEYS_PREFIX):].split(
        _SELECTED_KEYS_DELIMITER)


def is_map_selected_keys_field(field) -> bool:
    from pypaimon.schema.data_types import RowType

    return (
        isinstance(field.type, RowType)
        and field.description is not None
        and field.description.startswith(_SELECTED_KEYS_PREFIX)
    )


def map_selected_keys_field(field, keys, value_type=None):
    """Build the temporary ROW used by selected-key MAP reads."""
    from pypaimon.schema.data_types import DataField, MapType, RowType

    if not keys:
        raise ValueError("Selected MAP keys must not be empty")
    if len(set(keys)) != len(keys):
        raise ValueError("Selected MAP keys must not contain duplicates")
    for key in keys:
        if not isinstance(key, str):
            raise TypeError("Selected MAP keys must be strings")
        if _SELECTED_KEYS_DELIMITER in key:
            raise ValueError(
                "Selected MAP key must not contain '{}': {}".format(
                    _SELECTED_KEYS_DELIMITER, key))
        if key.startswith(_SELECTED_KEYS_PREFIX):
            raise ValueError(
                "Selected MAP key must not start with metadata prefix: {}".format(
                    key))

    if value_type is None:
        if not isinstance(field.type, MapType):
            raise TypeError("Selected-key projection requires a MAP field")
        value_type = field.type.value
    children = []
    for index, key in enumerate(keys):
        child_type = copy(value_type)
        child_type.nullable = True
        children.append(DataField(index, key, child_type))
    return DataField(
        field.id,
        field.name,
        RowType(field.type.nullable, children),
        _SELECTED_KEYS_PREFIX + _SELECTED_KEYS_DELIMITER.join(keys),
    )


def assemble_shared_shredding_map(
        column: pa.StructArray,
        map_type: pa.MapType,
        name_by_id: Dict[int, str],
        num_columns: int) -> pa.MapArray:
    """Restore one physical shared-shredding struct as a logical MAP."""
    if not pa.types.is_struct(column.type):
        raise TypeError("Shared-shredding MAP must be stored as a struct")

    field_names = [field.name for field in column.type]
    if not field_names or field_names[0] != _FIELD_MAPPING:
        raise ValueError(
            "Shared-shredding physical struct must start with {}".format(
                _FIELD_MAPPING))

    physical_columns = [None] * num_columns
    overflow = None
    for position, field_name in enumerate(field_names[1:], 1):
        if field_name == _OVERFLOW:
            if position != len(field_names) - 1:
                raise ValueError("Shared-shredding overflow must be the last field")
            overflow = column.field(position)
            continue
        if not field_name.startswith(_PHYSICAL_COLUMN_PREFIX):
            raise ValueError(
                "Unexpected shared-shredding physical field: {}".format(field_name))
        try:
            physical_index = int(field_name[len(_PHYSICAL_COLUMN_PREFIX):])
        except ValueError:
            raise ValueError(
                "Unexpected shared-shredding physical field: {}".format(field_name))
        if physical_index < 0 or physical_index >= num_columns:
            raise ValueError(
                "Shared-shredding physical column {} exceeds metadata column count {}".format(
                    physical_index, num_columns))
        if physical_columns[physical_index] is not None:
            raise ValueError(
                "Duplicate shared-shredding physical column {}".format(
                    physical_index))
        physical_columns[physical_index] = column.field(position)

    mapping_column = column.field(0)
    if not (
            pa.types.is_list(mapping_column.type)
            or pa.types.is_large_list(mapping_column.type)):
        raise TypeError("Shared-shredding field mapping must be an array")
    mapping = mapping_column.to_pylist()
    null_rows = column.is_null().to_pylist()
    overflow_offsets = None
    overflow_keys = None
    overflow_values = None
    if overflow is not None:
        if not pa.types.is_map(overflow.type):
            raise TypeError("Shared-shredding overflow field must be a map")
        overflow_offsets, overflow_start, overflow_end = _normalized_offsets(overflow)
        overflow_nulls = overflow.is_null().to_pylist()
        overflow_keys = overflow.keys.slice(
            overflow_start, overflow_end - overflow_start).to_pylist()
        overflow_values = overflow.items.slice(
            overflow_start, overflow_end - overflow_start)

    sources = list(physical_columns)
    if overflow_values is not None:
        sources.append(overflow_values)
    selected_indices = [[] for _ in sources]
    entry_sources = []
    entry_positions = []
    keys = []
    offsets = [0]

    for row in range(len(column)):
        if null_rows[row]:
            offsets[-1] = None
            offsets.append(len(keys))
            continue

        row_mapping = mapping[row]
        if row_mapping is None or len(row_mapping) != num_columns:
            raise ValueError(
                "Shared-shredding field mapping length must equal {}".format(
                    num_columns))
        for physical_index, field_id in enumerate(row_mapping):
            if field_id is None:
                raise ValueError(
                    "Shared-shredding field mapping must not contain null")
            name = name_by_id.get(field_id)
            if field_id < 0 or name is None:
                continue
            if physical_columns[physical_index] is None:
                raise ValueError(
                    "Missing shared-shredding physical column {}".format(
                        physical_index))
            _append_entry(
                keys, entry_sources, entry_positions, selected_indices,
                name, physical_index, row)

        if overflow_offsets is not None and not overflow_nulls[row]:
            overflow_source = len(sources) - 1
            for item_index in range(
                    overflow_offsets[row], overflow_offsets[row + 1]):
                name = name_by_id.get(overflow_keys[item_index])
                if name is not None:
                    _append_entry(
                        keys, entry_sources, entry_positions, selected_indices,
                        name, overflow_source, item_index)
        offsets.append(len(keys))

    selected_values = []
    source_bases = []
    for source, indices in zip(sources, selected_indices):
        source_bases.append(sum(len(values) for values in selected_values))
        if indices:
            selected = pc.take(source, pa.array(indices, type=pa.int64()))
            selected_values.append(
                _restore_orc_temporal_values(selected, map_type.item_type))
        else:
            selected_values.append(pa.array([], type=map_type.item_type))

    if selected_values:
        value_pool = pa.concat_arrays(selected_values)
        value_indices = [
            source_bases[source] + position
            for source, position in zip(entry_sources, entry_positions)
        ]
        values = pc.take(value_pool, pa.array(value_indices, type=pa.int64()))
    else:
        values = pa.array([], type=map_type.item_type)

    result = pa.MapArray.from_arrays(
        pa.array(offsets, type=pa.int32()),
        pa.array(keys, type=map_type.key_type),
        values,
    )
    entries = pa.StructArray.from_arrays(
        [result.keys, result.items],
        fields=[map_type.key_field, map_type.item_field],
    )
    return pa.Array.from_buffers(
        map_type,
        len(result),
        result.buffers()[:2],
        null_count=result.null_count,
        children=[entries],
    )


def shared_shredding_selected_paths(
        field_name: str, selected_keys: List[str], metadata) -> List[str]:
    """Return the physical leaf paths needed for selected literal keys."""
    name_by_id, field_to_columns, overflow_set, _ = metadata
    id_by_name = {name: field_id for field_id, name in name_by_id.items()}
    columns = set()
    include_overflow = False
    for key in selected_keys:
        field_id = id_by_name.get(key)
        if field_id is None:
            continue
        columns.update(field_to_columns.get(field_id, ()))
        include_overflow = include_overflow or field_id in overflow_set
    paths = ["{}.{}".format(field_name, _FIELD_MAPPING)]
    paths.extend(
        "{}.{}{}".format(field_name, _PHYSICAL_COLUMN_PREFIX, index)
        for index in sorted(columns)
    )
    if include_overflow:
        paths.append("{}.{}".format(field_name, _OVERFLOW))
    return paths


def assemble_shared_shredding_selected_keys(
        column: pa.StructArray,
        selected_keys: List[str],
        value_type: pa.DataType,
        metadata: _SelectedKeyMetadata) -> pa.StructArray:
    """Materialize selected MAP values from a pruned physical struct."""
    if not pa.types.is_struct(column.type):
        raise TypeError("Shared-shredding MAP must be stored as a struct")
    name_by_id, field_to_columns, overflow_set, num_columns = metadata
    id_by_name = {name: field_id for field_id, name in name_by_id.items()}

    field_names = [field.name for field in column.type]
    if not field_names or field_names[0] != _FIELD_MAPPING:
        raise ValueError(
            "Shared-shredding physical struct must start with {}".format(_FIELD_MAPPING))
    physical_columns = {}
    overflow: Optional[pa.MapArray] = None
    for position, field_name in enumerate(field_names[1:], 1):
        child = column.field(position)
        if field_name == _OVERFLOW:
            if not isinstance(child, pa.MapArray):
                raise TypeError("Shared-shredding overflow field must be a map")
            overflow = child
        elif field_name.startswith(_PHYSICAL_COLUMN_PREFIX):
            try:
                physical_columns[int(field_name[len(_PHYSICAL_COLUMN_PREFIX):])] = child
            except ValueError:
                raise ValueError("Unexpected shared-shredding physical field: {}".format(field_name))
        else:
            raise ValueError("Unexpected shared-shredding physical field: {}".format(field_name))

    mapping_column = column.field(0)
    requested_ids = [id_by_name[key] for key in selected_keys if key in id_by_name]
    candidate_count = sum(len(field_to_columns.get(field_id, ())) for field_id in requested_ids)
    mapping_rows: _FieldMappings = []
    null_rows = []
    if (isinstance(mapping_column, (pa.ListArray, pa.LargeListArray))
            and _use_vectorized_selected_keys(value_type, len(column), candidate_count)):
        vectorized = True
        valid_rows = column.is_valid()
        if requested_ids:
            _validate_selected_key_mapping(mapping_column, valid_rows, num_columns)
        # Sliced offsets still address the original values. Mask null parents
        # before gathering, since their child lists may be empty.
        mapping_offsets = pc.if_else(valid_rows, mapping_column.offsets[:-1], None)
        mapping_values = mapping_column.values
        row_indices = pa.array(range(len(column)), type=pa.int64())
    else:
        vectorized = False
        mapping_rows = column.field(0).to_pylist()
        null_rows = column.is_null().to_pylist()

    # Use one value pool, with a direct physical-column -> starting-offset map.
    value_arrays = []
    column_bases = {}
    next_base = 0
    for physical_index in sorted(physical_columns):
        values = _restore_orc_temporal_values(physical_columns[physical_index], value_type)
        column_bases[physical_index] = next_base
        value_arrays.append(values)
        next_base += len(values)

    overflow_base = None
    overflow_offsets: List[int] = []
    overflow_keys: List[Optional[int]] = []
    overflow_nulls: List[Optional[bool]] = []
    if overflow is not None:
        # Unlike mapping values, overflow values are sliced into the pool, so
        # their offsets must be relative to that slice.
        overflow_offsets, start, end = _normalized_offsets(overflow)
        overflow_keys = overflow.keys.slice(start, end - start).to_pylist()
        overflow_nulls = overflow.is_null().to_pylist()
        overflow_base = next_base
        value_arrays.append(_restore_orc_temporal_values(
            overflow.items.slice(start, end - start), value_type))
    value_pool = (pa.concat_arrays(value_arrays)
                  if value_arrays else pa.array([], type=value_type))

    children = []
    for key in selected_keys:
        field_id = id_by_name.get(key)
        if field_id is None:
            children.append(pc.take(value_pool, pa.nulls(len(column), type=pa.int64())))
            continue
        candidate_columns = field_to_columns.get(field_id, ())
        selected_rows: List[Optional[int]]
        if vectorized:
            indices = _selected_key_indices_vectorized(
                mapping_values, mapping_offsets, row_indices,
                field_id, candidate_columns, column_bases)
            if field_id not in overflow_set or overflow_base is None:
                children.append(pc.take(value_pool, indices))
                continue
            selected_rows = indices.to_pylist()
        else:
            selected_rows = _selected_key_indices(
                mapping_rows, null_rows, num_columns,
                field_id, candidate_columns, column_bases)

        if field_id in overflow_set and overflow_base is not None:
            # Both paths use the same first-match overflow lookup. A physical
            # match already has an index, even when its value is null.
            if not null_rows:
                null_rows = column.is_null().to_pylist()
            for row, selected in enumerate(selected_rows):
                if selected is not None or null_rows[row] or overflow_nulls[row]:
                    continue
                for item_index in range(overflow_offsets[row], overflow_offsets[row + 1]):
                    if overflow_keys[item_index] == field_id:
                        selected_rows[row] = overflow_base + item_index
                        break
        children.append(pc.take(value_pool, pa.array(selected_rows, type=pa.int64())))

    fields = [pa.field(key, value_type) for key in selected_keys]
    mask = column.is_null() if column.null_count else None
    return pa.StructArray.from_arrays(children, fields=fields, mask=mask)


def _use_vectorized_selected_keys(value_type: pa.DataType, rows: int, candidate_count: int) -> bool:
    # Conservative limits from local benchmarks, not universal crossover points.
    # Count key/column pairs: shared columns still need a match for each key.
    if rows < _VECTORIZED_MIN_ROWS or candidate_count > _VECTORIZED_MAX_CANDIDATE_PAIRS:
        return False
    # Other types retain the original path pending performance validation.
    return (pa.types.is_string(value_type)
            or pa.types.is_binary(value_type)
            or pa.types.is_boolean(value_type)
            or pa.types.is_signed_integer(value_type)
            or pa.types.is_floating(value_type)
            or pa.types.is_decimal128(value_type)
            or pa.types.is_date32(value_type)
            or pa.types.is_time32(value_type)
            or pa.types.is_timestamp(value_type))


def _validate_selected_key_mapping(
        mapping: Union[pa.ListArray, pa.LargeListArray],
        valid_rows: pa.BooleanArray,
        num_columns: int) -> None:
    lengths = pc.list_value_length(mapping)
    invalid_lengths = pc.not_equal(lengths, pa.scalar(num_columns))
    invalid_rows = pc.and_(valid_rows, pc.fill_null(invalid_lengths, pa.scalar(True)))
    if pc.any(invalid_rows).as_py():
        raise ValueError("Shared-shredding field mapping length must equal {}".format(num_columns))


def _selected_key_indices(
        mapping_rows: _FieldMappings,
        null_rows: List[Optional[bool]],
        num_columns: int,
        field_id: int,
        candidate_columns: Sequence[int],
        column_bases: Dict[int, int]) -> List[Optional[int]]:
    """Find the first physical match in each row, as an index into the value pool."""
    indices: List[Optional[int]] = []
    for row, row_mapping in enumerate(mapping_rows):
        selected = None
        if not null_rows[row]:
            if row_mapping is None or len(row_mapping) != num_columns:
                raise ValueError(
                    "Shared-shredding field mapping length must equal {}".format(num_columns))
            for physical_index in candidate_columns:
                if row_mapping[physical_index] == field_id:
                    base = column_bases.get(physical_index)
                    if base is None:
                        raise ValueError(
                            "Missing shared-shredding physical column {}".format(physical_index))
                    selected = base + row
                    break
        indices.append(selected)
    return indices


def _selected_key_indices_vectorized(
        mapping_values: pa.Array,
        mapping_offsets: pa.Array,
        row_indices: pa.Array,
        field_id: int,
        candidate_columns: Sequence[int],
        column_bases: Dict[int, int]) -> pa.Array:
    """Find the same first-match indices using Arrow operations across rows."""
    indices = pa.nulls(len(row_indices), type=pa.int64())
    field_id_scalar = pa.scalar(field_id)
    false_scalar = pa.scalar(False)
    for physical_index in candidate_columns:
        mapped_ids = pc.take(mapping_values, pc.add(mapping_offsets, physical_index))
        matches = pc.equal(mapped_ids, field_id_scalar)
        # A null mapping entry is not a match; later candidates cannot replace
        # an index chosen by an earlier candidate.
        matches = pc.and_(pc.fill_null(matches, false_scalar), pc.is_null(indices))
        base = column_bases.get(physical_index)
        if base is None:
            if pc.any(matches).as_py():
                raise ValueError("Missing shared-shredding physical column {}".format(physical_index))
            continue
        indices = pc.if_else(matches, pc.add(row_indices, base), indices)
    return indices


def assemble_normal_map_selected_keys(
        column: pa.MapArray,
        selected_keys: List[str],
        value_type: pa.DataType) -> pa.StructArray:
    """Materialize selected values when an older file stores a normal MAP."""
    if not pa.types.is_map(column.type):
        raise TypeError("Selected-key MAP must be stored as a map or shared struct")
    offsets, start, end = _normalized_offsets(column)
    keys = column.keys.slice(start, end - start).to_pylist()
    values = _restore_orc_temporal_values(
        column.items.slice(start, end - start), value_type)
    null_rows = column.is_null().to_pylist()
    children = []
    for selected_key in selected_keys:
        indices = []
        for row in range(len(column)):
            selected = None
            if not null_rows[row]:
                for item_index in range(offsets[row], offsets[row + 1]):
                    if keys[item_index] == selected_key:
                        selected = item_index
                        break
            indices.append(selected)
        children.append(pc.take(
            values, pa.array(indices, type=pa.int64())))
    fields = [pa.field(key, value_type) for key in selected_keys]
    mask = column.is_null() if column.null_count else None
    return pa.StructArray.from_arrays(children, fields=fields, mask=mask)


def _restore_orc_temporal_values(column, logical_type):
    """Restore logical temporal types from their ORC representations."""
    if column.type == logical_type:
        return column
    if pa.types.is_time(logical_type) and pa.types.is_int32(column.type):
        return column.cast(logical_type)
    if (pa.types.is_timestamp(logical_type)
            and pa.types.is_timestamp(column.type)):
        return column.cast(logical_type)
    if pa.types.is_struct(logical_type) and pa.types.is_struct(column.type):
        if len(column.type) != len(logical_type):
            return column
        fields = list(logical_type)
        children = [
            _restore_orc_temporal_values(column.field(i), field.type)
            for i, field in enumerate(fields)
        ]
        mask = column.is_null() if column.null_count else None
        return pa.StructArray.from_arrays(children, fields=fields, mask=mask)
    if ((pa.types.is_list(logical_type) and pa.types.is_list(column.type))
            or (pa.types.is_large_list(logical_type)
                and pa.types.is_large_list(column.type))):
        offsets, start, end = _normalized_offsets(column)
        offsets = _nullable_offsets(column, offsets, logical_type)
        values = _restore_orc_temporal_values(
            column.values.slice(start, end - start), logical_type.value_type)
        result = (pa.LargeListArray.from_arrays(offsets, values)
                  if pa.types.is_large_list(logical_type)
                  else pa.ListArray.from_arrays(offsets, values))
        return pa.Array.from_buffers(
            logical_type,
            len(result),
            result.buffers()[:2],
            null_count=result.null_count,
            children=[values],
        )
    if pa.types.is_map(logical_type) and pa.types.is_map(column.type):
        offsets, start, end = _normalized_offsets(column)
        offsets = _nullable_offsets(column, offsets, logical_type)
        keys = _restore_orc_temporal_values(
            column.keys.slice(start, end - start), logical_type.key_type)
        items = _restore_orc_temporal_values(
            column.items.slice(start, end - start), logical_type.item_type)
        result = pa.MapArray.from_arrays(offsets, keys, items)
        entries = pa.StructArray.from_arrays(
            [keys, items],
            fields=[logical_type.key_field, logical_type.item_field],
        )
        return pa.Array.from_buffers(
            logical_type,
            len(result),
            result.buffers()[:2],
            null_count=result.null_count,
            children=[entries],
        )
    return column


def _nullable_offsets(column, offsets, logical_type):
    for index, is_null in enumerate(column.is_null().to_pylist()):
        if is_null:
            offsets[index] = None
    offset_type = (pa.int64() if pa.types.is_large_list(logical_type)
                   else pa.int32())
    return pa.array(offsets, type=offset_type)


def _append_entry(keys, entry_sources, entry_positions, selected_indices,
                  name, source, source_index):
    keys.append(name)
    entry_sources.append(source)
    entry_positions.append(len(selected_indices[source]))
    selected_indices[source].append(source_index)


def _normalized_offsets(column) -> Tuple[List[int], int, int]:
    offsets_array = getattr(column, "offsets", None)
    if offsets_array is None:
        offsets_array = pa.Array.from_buffers(
            pa.int32(),
            len(column) + 1,
            [None, column.buffers()[1]],
            offset=column.offset,
        )
    # Arrow list offsets are non-null integers, even for null parent rows.
    offsets = cast(List[int], offsets_array.to_pylist())
    start = offsets[0]
    normalized = [value - start for value in offsets]
    return normalized, start, offsets[-1]


def _decompress(data: bytes, original_size: int, compression: str) -> bytes:
    if original_size < 0:
        raise ValueError("Shared-shredding field dictionary size must not be negative")
    if compression == "none":
        result = data
    elif compression == "zstd":
        import zstandard as zstd
        result = zstd.ZstdDecompressor().decompress(
            data, max_output_size=original_size)
    elif compression == "lz4":
        if len(data) < 8:
            raise ValueError("Shared-shredding LZ4 dictionary is truncated")
        compressed_size, stored_size = struct.unpack_from("<ii", data)
        if compressed_size < 0 or stored_size != original_size:
            raise ValueError("Shared-shredding LZ4 dictionary header is invalid")
        payload = data[8:]
        if len(payload) != compressed_size:
            raise ValueError("Shared-shredding LZ4 dictionary is truncated")
        result = bytes(pa.Codec("lz4_raw").decompress(payload, original_size))
    else:
        raise ValueError(
            "Unsupported shared-shredding dictionary compression: {}".format(
                compression))
    if len(result) != original_size:
        raise ValueError("Shared-shredding field dictionary size is invalid")
    return result


def _compress(data: bytes, compression: str) -> bytes:
    if compression == "none":
        return data
    if compression == "zstd":
        import zstandard as zstd
        return zstd.ZstdCompressor(level=1).compress(data)
    if compression == "lz4":
        payload = bytes(pa.Codec("lz4_raw").compress(data))
        return struct.pack("<ii", len(payload), len(data)) + payload
    raise ValueError(
        "Unsupported shared-shredding dictionary compression: {}".format(
            compression))


def _required(metadata, key):
    try:
        return metadata[key]
    except KeyError:
        raise ValueError(
            "Missing shared-shredding metadata key: {}".format(
                key.decode("utf-8")))


def _required_int(metadata, key):
    try:
        return int(_required(metadata, key))
    except ValueError:
        raise ValueError(
            "Malformed shared-shredding metadata value for: {}".format(
                key.decode("utf-8")))
