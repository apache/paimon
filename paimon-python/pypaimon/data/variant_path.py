# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Read and replace paths in Arrow VARIANT columns."""

import functools
import re
import struct
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

import pyarrow as pa

from pypaimon.data._variant_binary import (
    _ARRAY,
    _OBJECT,
    _U32_SIZE,
    _primitive_header,
    _read_unsigned,
)
from pypaimon.data.generic_variant import (
    GenericVariant,
    _DOUBLE,
    _FLOAT,
    _Type,
    _value_size,
    _variant_get_type,
)
from pypaimon.data.variant_shredding import (
    _build_array_value,
    _build_object_value,
    _encode_scalar_to_value_bytes,
)


_INDEX_PATTERN = re.compile(r"\[(\d+)]")
_KEY_PATTERN = re.compile(r"\.([^\.\[]+)|\['([^']+)']|\[\"([^\"]+)\"]")
_Path = Tuple[Tuple[str, object], ...]
_ObjectLayout = Tuple[int, int, int, int, int, int]
_ArrayLayout = Tuple[int, int, int, int]


@functools.lru_cache(maxsize=256)
def _parse_path(path: str) -> _Path:
    if not isinstance(path, str) or not path.startswith('$'):
        raise ValueError(f"Invalid VARIANT path: {path}")

    pos = 1
    segments = []
    while pos < len(path):
        match = _INDEX_PATTERN.match(path, pos)
        if match is not None:
            segments.append(('index', int(match.group(1))))
        else:
            match = _KEY_PATTERN.match(path, pos)
            if match is None:
                raise ValueError(f"Invalid VARIANT path: {path}")
            key = next(value for value in match.groups()
                       if value is not None)
            segments.append(('key', key))
        pos = match.end()
    return tuple(segments)


@functools.lru_cache(maxsize=256)
def _metadata_key_ids(metadata: bytes) -> Dict[str, int]:
    if not metadata:
        raise ValueError("MALFORMED_VARIANT: empty metadata")
    offset_size = ((metadata[0] >> 6) & 0x3) + 1
    size = _read_unsigned(metadata, 1, offset_size)
    offset_start = 1 + offset_size
    string_start = offset_start + (size + 1) * offset_size
    result = {}
    for key_id in range(size):
        start = _read_unsigned(
            metadata, offset_start + key_id * offset_size, offset_size)
        end = _read_unsigned(
            metadata, offset_start + (key_id + 1) * offset_size,
            offset_size)
        result[
            metadata[string_start + start:string_start + end].decode('utf-8')
        ] = key_id
    return result


def _object_layout(value: bytes, pos: int) -> _ObjectLayout:
    header = value[pos]
    if (header & 0x3) != _OBJECT:
        raise ValueError("VARIANT path expects an object")
    type_info = (header >> 2) & 0x3F
    size_bytes = _U32_SIZE if ((type_info >> 4) & 0x1) else 1
    size = _read_unsigned(value, pos + 1, size_bytes)
    id_size = ((type_info >> 2) & 0x3) + 1
    offset_size = (type_info & 0x3) + 1
    id_start = pos + 1 + size_bytes
    offset_start = id_start + size * id_size
    data_start = offset_start + (size + 1) * offset_size
    return size, id_size, offset_size, id_start, offset_start, data_start


def _array_layout(value: bytes, pos: int) -> _ArrayLayout:
    header = value[pos]
    if (header & 0x3) != _ARRAY:
        raise ValueError("VARIANT path expects an array")
    type_info = (header >> 2) & 0x3F
    size_bytes = _U32_SIZE if ((type_info >> 2) & 0x1) else 1
    size = _read_unsigned(value, pos + 1, size_bytes)
    offset_size = (type_info & 0x3) + 1
    offset_start = pos + 1 + size_bytes
    data_start = offset_start + (size + 1) * offset_size
    return size, offset_size, offset_start, data_start


@functools.lru_cache(maxsize=2048)
def _field_slot(id_table: bytes, id_size: int, key_id: int) -> Optional[int]:
    for slot in range(len(id_table) // id_size):
        if _read_unsigned(id_table, slot * id_size, id_size) == key_id:
            return slot
    return None


def _object_field_position(
        value: bytes, pos: int, key_id: int) -> Optional[int]:
    size, id_size, offset_size, id_start, offset_start, data_start = (
        _object_layout(value, pos))
    id_table = bytes(value[id_start:id_start + size * id_size])
    slot = _field_slot(id_table, id_size, key_id)
    if slot is None:
        return None
    offset = _read_unsigned(
        value, offset_start + slot * offset_size, offset_size)
    return data_start + offset


def _array_element_position(
        value: bytes, pos: int, index: int) -> Optional[int]:
    size, offset_size, offset_start, data_start = _array_layout(value, pos)
    if index >= size:
        return None
    offset = _read_unsigned(
        value, offset_start + index * offset_size, offset_size)
    return data_start + offset


@functools.lru_cache(maxsize=256)
def _compile_paths(paths: Tuple[_Path, ...]):
    nodes = [(None, None, None)]
    node_by_prefix = {(): 0}
    results = []
    for path in paths:
        for length in range(1, len(path) + 1):
            prefix = path[:length]
            if prefix not in node_by_prefix:
                node_by_prefix[prefix] = len(nodes)
                nodes.append((node_by_prefix[prefix[:-1]],) + prefix[-1])
        results.append(node_by_prefix[path])
    return tuple(nodes), tuple(results)


class _PositionPlan:

    def __init__(self, metadata, checks, positions):
        self.metadata = metadata
        self.checks = checks
        self.positions = positions

    def matches(self, value: bytes, metadata: bytes) -> bool:
        return metadata == self.metadata and all(
            value[pos:pos + len(expected)] == expected
            for pos, expected in self.checks
        )


def _position_plan(
        value: bytes,
        metadata: bytes,
        paths: Sequence[_Path],
) -> _PositionPlan:
    key_ids = _metadata_key_ids(metadata)
    nodes, result_nodes = _compile_paths(tuple(paths))
    positions = [0]
    checks = []
    checked = set()
    for parent_node, kind, segment in nodes[1:]:
        parent = positions[parent_node]
        if parent is None:
            positions.append(None)
            continue
        if parent not in checked:
            basic_type = value[parent] & 0x3
            if basic_type == _OBJECT:
                data_start = _object_layout(value, parent)[-1]
            elif basic_type == _ARRAY:
                data_start = _array_layout(value, parent)[-1]
            else:
                data_start = parent + 1
            checks.append((parent, value[parent:data_start]))
            checked.add(parent)
        if kind == 'key':
            key_id = key_ids.get(segment)
            if key_id is None or (value[parent] & 0x3) != _OBJECT:
                positions.append(None)
            else:
                positions.append(_object_field_position(
                    value, parent, key_id))
        elif (value[parent] & 0x3) != _ARRAY:
            positions.append(None)
        else:
            positions.append(_array_element_position(
                value, parent, segment))
    return _PositionPlan(
        metadata,
        tuple(checks),
        tuple(positions[node] for node in result_nodes),
    )


def _path_positions(
        value: bytes,
        metadata: bytes,
        paths: Sequence[_Path],
        plans: List[_PositionPlan],
) -> Sequence[Optional[int]]:
    for index, plan in enumerate(plans):
        if plan.matches(value, metadata):
            if index:
                plans.insert(0, plans.pop(index))
            return plan.positions
    plan = _position_plan(value, metadata, paths)
    plans.insert(0, plan)
    del plans[8:]
    return plan.positions


def _replace_path(
        value: bytes,
        metadata: bytes,
        pos: int,
        path: _Path,
        replacement: bytes,
) -> bytes:
    if not path:
        return replacement

    kind, segment = path[0]
    if kind == 'key':
        if (value[pos] & 0x3) != _OBJECT:
            raise ValueError("VARIANT path expects an object")
        key_id = _metadata_key_ids(metadata).get(segment)
        if key_id is None:
            raise ValueError(f"VARIANT path does not exist: {segment}")
        size, id_size, offset_size, id_start, offset_start, data_start = (
            _object_layout(value, pos))
        ids = [
            _read_unsigned(value, id_start + i * id_size, id_size)
            for i in range(size)
        ]
        try:
            slot = ids.index(key_id)
        except ValueError:
            raise ValueError(f"VARIANT path does not exist: {segment}")
        children = []
        for i in range(size):
            child_pos = data_start + _read_unsigned(
                value, offset_start + i * offset_size, offset_size)
            child = value[child_pos:child_pos + _value_size(value, child_pos)]
            if i == slot:
                child = _replace_path(
                    value, metadata, child_pos, path[1:], replacement)
            children.append(child)
        return _build_object_value(list(zip(ids, children)))

    if (value[pos] & 0x3) != _ARRAY:
        raise ValueError("VARIANT path expects an array")
    size, offset_size, offset_start, data_start = _array_layout(value, pos)
    if segment >= size:
        raise ValueError(f"VARIANT array index does not exist: {segment}")
    children = []
    for i in range(size):
        child_pos = data_start + _read_unsigned(
            value, offset_start + i * offset_size, offset_size)
        child = value[child_pos:child_pos + _value_size(value, child_pos)]
        if i == segment:
            child = _replace_path(
                value, metadata, child_pos, path[1:], replacement)
        children.append(child)
    return _build_array_value(children)


def _variant_chunks(column):
    if isinstance(column, pa.ChunkedArray):
        chunks, chunked, data_type = column.chunks, True, column.type
    elif isinstance(column, pa.Array):
        chunks, chunked, data_type = [column], False, column.type
    else:
        raise TypeError(
            "VARIANT input must be a PyArrow Array or ChunkedArray")
    if (not pa.types.is_struct(data_type)
            or [field.name for field in data_type]
            != ['value', 'metadata']):
        raise TypeError(
            "VARIANT input must contain value and metadata fields")
    if not (pa.types.is_binary(data_type[0].type)
            or pa.types.is_large_binary(data_type[0].type)):
        raise TypeError("VARIANT value field must be binary")
    return chunks, chunked, data_type


class _BinaryValues:

    def __init__(self, array: pa.Array):
        self.array = array
        if pa.types.is_binary(array.type):
            self.width, self.value_format = 4, '<i'
        elif pa.types.is_large_binary(array.type):
            self.width, self.value_format = 8, '<q'
        else:
            raise TypeError("VARIANT binary field has an unsupported type")
        self.offsets = array.buffers()[1]
        data_buffer = array.buffers()[2]
        self.data = (memoryview(data_buffer) if data_buffer is not None
                     else memoryview(b''))

    def bounds(self, row: int) -> Tuple[int, int]:
        index = self.array.offset + row
        return (
            struct.unpack_from(
                self.value_format, self.offsets, index * self.width)[0],
            struct.unpack_from(
                self.value_format, self.offsets,
                (index + 1) * self.width)[0],
        )

    def view(self, row: int) -> memoryview:
        start, end = self.bounds(row)
        return self.data[start:end]


def _decode_scalar(value, metadata: bytes, pos: int):
    value_type = _variant_get_type(value, pos)
    if value_type == _Type.DOUBLE:
        return struct.unpack_from('<d', value, pos + 1)[0]
    if value_type == _Type.FLOAT:
        return struct.unpack_from('<f', value, pos + 1)[0]
    return GenericVariant(bytes(value), metadata, pos).to_python()


def _patched_chunk(chunk: pa.StructArray, patches) -> pa.StructArray:
    values = chunk.field(0)
    data_buffer = values.buffers()[2]
    data = bytearray(data_buffer) if data_buffer is not None else bytearray()
    for absolute_pos, replacement in patches:
        data[absolute_pos:absolute_pos + len(replacement)] = replacement

    buffers = list(values.buffers())
    buffers[2] = pa.py_buffer(data)
    patched_values = pa.Array.from_buffers(
        values.type,
        len(values),
        buffers,
        null_count=values.null_count,
        offset=values.offset,
    )
    metadata = chunk.field(1)
    if chunk.offset == 0:
        return pa.Array.from_buffers(
            chunk.type,
            len(chunk),
            [chunk.buffers()[0]],
            children=[patched_values, metadata],
            null_count=chunk.null_count,
        )
    return pa.StructArray.from_arrays(
        [patched_values, metadata],
        fields=list(chunk.type),
        mask=chunk.is_null(),
    )


def _rebuilt_chunk(
        chunk: pa.StructArray,
        values: Sequence[bytes],
) -> pa.StructArray:
    return pa.StructArray.from_arrays(
        [pa.array(values, type=chunk.type[0].type), chunk.field(1)],
        fields=list(chunk.type),
        mask=chunk.is_null(),
    )


class _Replacement:

    def __init__(self, value, length: int):
        if isinstance(value, pa.Scalar):
            self._value = value
            self._array = None
            self.type = value.type
        elif isinstance(value, (pa.Array, pa.ChunkedArray)):
            if len(value) != length:
                raise ValueError(
                    "VARIANT replacement length must match the input column")
            self._value = None
            self._array = value
            self.type = value.type
        else:
            raise TypeError(
                "VARIANT replacement must be an Arrow Scalar or Array")
        if not _supported_replacement_type(self.type):
            raise TypeError(
                f"Unsupported VARIANT replacement type: {self.type}")
        if pa.types.is_float64(self.type):
            self._value_format = '<Bd'
            self._type_header = _primitive_header(_DOUBLE)
        elif pa.types.is_float32(self.type):
            self._value_format = '<Bf'
            self._type_header = _primitive_header(_FLOAT)
        else:
            self._value_format = None
            self._type_header = None

    def values(self, offset: int, length: int):
        if self._array is None:
            return [self._value.as_py()] * length
        return self._array.slice(offset, length).to_pylist()

    def encode(self, value) -> bytes:
        if value is not None and self._value_format is not None:
            return struct.pack(
                self._value_format, self._type_header, value)
        return _encode_scalar_to_value_bytes(value, self.type)


def _supported_replacement_type(data_type: pa.DataType) -> bool:
    return (
        pa.types.is_null(data_type)
        or pa.types.is_boolean(data_type)
        or pa.types.is_signed_integer(data_type)
        or pa.types.is_float32(data_type)
        or pa.types.is_float64(data_type)
        or pa.types.is_string(data_type)
        or pa.types.is_large_string(data_type)
        or pa.types.is_binary(data_type)
        or pa.types.is_large_binary(data_type)
        or pa.types.is_date32(data_type)
        or pa.types.is_timestamp(data_type)
        or pa.types.is_decimal128(data_type)
    )


def _variant_get(column, paths: Mapping[str, pa.DataType]):
    parsed = []
    for path, target_type in paths.items():
        if not isinstance(target_type, pa.DataType):
            raise TypeError("VARIANT target_type must be a PyArrow data type")
        parsed.append((path, _parse_path(path), target_type))
    parsed_paths = [parsed_path for _, parsed_path, _ in parsed]
    chunks, chunked, _ = _variant_chunks(column)
    plans = []
    result_chunks = {path: [] for path in paths}
    for chunk in chunks:
        values = _BinaryValues(chunk.field(0))
        metadata = chunk.field(1).to_pylist()
        valid = chunk.is_valid().to_pylist()
        results = {path: [] for path in paths}
        for row in range(len(chunk)):
            if not valid[row]:
                for path in paths:
                    results[path].append(None)
                continue
            value = values.view(row)
            positions = _path_positions(
                value,
                metadata[row],
                parsed_paths,
                plans,
            )
            for (path, _, _), pos in zip(parsed, positions):
                results[path].append(
                    None if pos is None
                    else _decode_scalar(value, metadata[row], pos)
                )
        for path, _, target_type in parsed:
            result_chunks[path].append(
                pa.array(results[path], type=target_type))
    if not chunked:
        return {path: chunks[0] for path, chunks in result_chunks.items()}
    return {
        path: pa.chunked_array(chunks, type=paths[path])
        for path, chunks in result_chunks.items()
    }


def variant_get(column, path, target_type=None):
    """Read one or more VARIANT paths into Arrow arrays."""
    if isinstance(path, Mapping):
        if target_type is not None:
            raise TypeError(
                "VARIANT target_type must be omitted for path mappings")
        return _variant_get(column, path)
    if target_type is None:
        raise TypeError("VARIANT target_type must be a PyArrow data type")
    return _variant_get(column, {path: target_type})[path]


def _paths_overlap(first: _Path, second: _Path) -> bool:
    limit = min(len(first), len(second))
    return first[:limit] == second[:limit]


def _validate_distinct_paths(parsed) -> None:
    for index, (_, first, _) in enumerate(parsed):
        for _, second, _ in parsed[index + 1:]:
            if _paths_overlap(first, second):
                raise ValueError(
                    "VARIANT replacement paths must not overlap")


def variant_replace(
        column,
        path,
        replacement=None,
        strict: bool = False,
):
    """Replace one or more existing VARIANT paths with Arrow values."""
    if not isinstance(strict, bool):
        raise TypeError("VARIANT strict must be a boolean")
    if isinstance(path, Mapping):
        if replacement is not None:
            raise TypeError(
                "VARIANT replacement must be omitted for path mappings")
        replacements = path
    else:
        replacements = {path: replacement}
    parsed = [
        (path, _parse_path(path), _Replacement(value, len(column)))
        for path, value in replacements.items()
    ]
    _validate_distinct_paths(parsed)
    if not parsed:
        return column
    parsed_paths = [parsed_path for _, parsed_path, _ in parsed]

    chunks, chunked, data_type = _variant_chunks(column)
    plans = []
    result_chunks = []
    global_row = 0
    for chunk in chunks:
        values = _BinaryValues(chunk.field(0))
        metadata = chunk.field(1).to_pylist()
        valid = chunk.is_valid().to_pylist()
        chunk_replacements = {
            path: provider.values(global_row, len(chunk))
            for path, _, provider in parsed
        }
        row_replacements = [[] for _ in range(len(chunk))]
        patches = []
        rebuild = False
        for row in range(len(chunk)):
            if not valid[row]:
                continue
            value = values.view(row)
            row_start, _ = values.bounds(row)
            positions = _path_positions(
                value,
                metadata[row],
                parsed_paths,
                plans,
            )
            for (path, parsed_path, provider), pos in zip(
                    parsed, positions):
                if pos is None:
                    if strict:
                        raise ValueError(
                            f"VARIANT path does not exist: {path}")
                    continue
                encoded = provider.encode(chunk_replacements[path][row])
                row_replacements[row].append((parsed_path, encoded))
                if len(encoded) == _value_size(value, pos):
                    patches.append((row_start + pos, encoded))
                else:
                    rebuild = True

        if rebuild:
            rebuilt_values = []
            for row, replacements_for_row in enumerate(row_replacements):
                value = bytes(values.view(row))
                for parsed_path, encoded in replacements_for_row:
                    value = _replace_path(
                        value, metadata[row], 0, parsed_path, encoded)
                rebuilt_values.append(value)
            result_chunks.append(_rebuilt_chunk(chunk, rebuilt_values))
        elif patches:
            result_chunks.append(_patched_chunk(chunk, patches))
        else:
            result_chunks.append(chunk)
        global_row += len(chunk)

    if not chunked:
        return result_chunks[0]
    return pa.chunked_array(result_chunks, type=data_type)
