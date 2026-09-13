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

"""Read support for Paimon's shared-shredding MAP storage layout."""

import json
import struct
from typing import Dict

import pyarrow as pa
import pyarrow.compute as pc


_STORAGE_LAYOUT = b"paimon.map.storage-layout"
_VERSION = b"paimon.map.shared-shredding.version"
_FIELD_DICT = b"paimon.map.shared-shredding.field-dict"
_FIELD_DICT_COMPRESSION = b"paimon.map.shared-shredding.field-dict-compression"
_FIELD_DICT_ORIGINAL_SIZE = b"paimon.map.shared-shredding.field-dict-original-size"
_NUM_COLUMNS = b"paimon.map.shared-shredding.num-columns"
_FIELD_MAPPING = "__field_mapping"
_OVERFLOW = "__overflow"
_PHYSICAL_COLUMN_PREFIX = "__col_"


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


def _normalized_offsets(column):
    offsets_array = getattr(column, "offsets", None)
    if offsets_array is None:
        offsets_array = pa.Array.from_buffers(
            pa.int32(),
            len(column) + 1,
            [None, column.buffers()[1]],
            offset=column.offset,
        )
    offsets = offsets_array.to_pylist()
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
