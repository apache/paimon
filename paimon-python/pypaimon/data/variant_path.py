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

import numpy as np
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

    def numpy_offsets(self):
        return np.frombuffer(
            self.offsets,
            dtype=np.dtype(self.value_format),
            count=len(self.array) + 1,
            offset=self.array.offset * self.width,
        ).astype(np.int64, copy=False)

    def used_bounds(self) -> Tuple[int, int]:
        first = self.array.offset
        last = first + len(self.array)
        return (
            struct.unpack_from(
                self.value_format, self.offsets, first * self.width)[0],
            struct.unpack_from(
                self.value_format, self.offsets, last * self.width)[0],
        )

    def view(self, row: int) -> memoryview:
        start, end = self.bounds(row)
        return self.data[start:end]

    def row(self, row: int) -> Tuple[int, memoryview]:
        start, end = self.bounds(row)
        return start, self.data[start:end]

    def copy_used_data(self) -> Tuple[bytearray, int]:
        start, end = self.used_bounds()
        return bytearray(self.data[start:end]), start

    def array_from_data(self, data: bytearray, start: int) -> pa.Array:
        buffers = list(self.array.buffers())
        offset = self.array.offset
        if offset == 0 and start == 0:
            buffers[2] = pa.py_buffer(data)
        else:
            offsets = bytearray((len(self.array) + 1) * self.width)
            for index in range(len(self.array) + 1):
                value = struct.unpack_from(
                    self.value_format,
                    self.offsets,
                    (self.array.offset + index) * self.width,
                )[0]
                struct.pack_into(
                    self.value_format,
                    offsets,
                    index * self.width,
                    value - start,
                )
            buffers = [
                (None if self.array.null_count == 0
                 else self.array.is_valid().buffers()[1]),
                pa.py_buffer(offsets),
                pa.py_buffer(data),
            ]
            offset = 0
        return pa.Array.from_buffers(
            self.array.type,
            len(self.array),
            buffers,
            null_count=self.array.null_count,
            offset=offset,
        )


def _take_unsigned(data, positions, widths):
    result = np.empty(len(positions), dtype=np.int64)
    for width in range(1, 5):
        selected = widths == width
        if not np.any(selected):
            continue
        selected_positions = positions[selected]
        if (np.any(selected_positions < 0)
                or np.any(selected_positions + width > len(data))):
            raise ValueError("Invalid VARIANT offset")
        indices = selected_positions[:, None] + np.arange(width)
        values = data[indices].astype(np.int64, copy=False)
        result[selected] = np.sum(
            values << (np.arange(width, dtype=np.int64) * 8), axis=1)
    return result


def _all_binary_values_equal(values: _BinaryValues, expected: bytes) -> bool:
    offsets = values.numpy_offsets()
    lengths = offsets[1:] - offsets[:-1]
    if np.any(lengths != len(expected)):
        return False
    if not len(offsets) > 1 or not expected:
        return True

    data = np.frombuffer(values.data, dtype=np.uint8)
    expected_array = np.frombuffer(expected, dtype=np.uint8)
    rows_per_batch = max(1, (1024 * 1024) // len(expected))
    for row in range(0, len(lengths), rows_per_batch):
        end = min(row + rows_per_batch, len(lengths))
        starts = offsets[row:end]
        indices = starts[:, None] + np.arange(len(expected))
        if not np.all(data[indices] == expected_array):
            return False
    return True


def _vectorized_path_positions(
        values: _BinaryValues,
        metadata: pa.Array,
        valid_count: int,
        paths: Sequence[_Path],
):
    if (valid_count != len(values.array)
            or values.array.null_count
            or metadata.null_count
            or not len(values.array)):
        return None

    metadata_values = _BinaryValues(metadata)
    first_metadata = bytes(metadata_values.view(0))
    if not _all_binary_values_equal(metadata_values, first_metadata):
        return None

    key_ids = _metadata_key_ids(first_metadata)
    nodes, result_nodes = _compile_paths(tuple(paths))
    row_offsets = values.numpy_offsets()
    row_starts = row_offsets[:-1]
    row_ends = row_offsets[1:]
    data = np.frombuffer(values.data, dtype=np.uint8)
    first_value = values.view(0)
    positions = [np.zeros(len(values.array), dtype=np.int64)]

    try:
        for parent_node, kind, segment in nodes[1:]:
            parent = positions[parent_node]
            absolute_parent = row_starts + parent
            if (np.any(absolute_parent < row_starts)
                    or np.any(absolute_parent >= row_ends)):
                return None
            headers = data[absolute_parent]
            type_info = (headers >> 2).astype(np.int64, copy=False)

            if kind == 'key':
                if np.any((headers & 0x3) != _OBJECT):
                    return None
                key_id = key_ids.get(segment)
                if key_id is None:
                    return None
                first_layout = _object_layout(
                    first_value, int(parent[0]))
                size, id_size, _, id_start, _, _ = first_layout
                id_table = bytes(
                    first_value[id_start:id_start + size * id_size])
                slot = _field_slot(id_table, id_size, key_id)
                if slot is None:
                    return None

                size_widths = np.where(
                    ((type_info >> 4) & 0x1) != 0, _U32_SIZE, 1)
                sizes = _take_unsigned(
                    data, absolute_parent + 1, size_widths)
                id_widths = ((type_info >> 2) & 0x3) + 1
                offset_widths = (type_info & 0x3) + 1
                if np.any(sizes <= slot):
                    return None
                id_starts = absolute_parent + 1 + size_widths
                ids = _take_unsigned(
                    data, id_starts + slot * id_widths, id_widths)
                if np.any(ids != key_id):
                    return None
                offset_starts = id_starts + sizes * id_widths
                offsets = _take_unsigned(
                    data,
                    offset_starts + slot * offset_widths,
                    offset_widths,
                )
                data_starts = offset_starts + (sizes + 1) * offset_widths
                child = data_starts + offsets - row_starts
            else:
                if np.any((headers & 0x3) != _ARRAY):
                    return None
                size_widths = np.where(
                    ((type_info >> 2) & 0x1) != 0, _U32_SIZE, 1)
                sizes = _take_unsigned(
                    data, absolute_parent + 1, size_widths)
                if np.any(sizes <= segment):
                    return None
                offset_widths = (type_info & 0x3) + 1
                offset_starts = absolute_parent + 1 + size_widths
                offsets = _take_unsigned(
                    data,
                    offset_starts + segment * offset_widths,
                    offset_widths,
                )
                data_starts = offset_starts + (sizes + 1) * offset_widths
                child = data_starts + offsets - row_starts

            if np.any(child < 0) or np.any(row_starts + child >= row_ends):
                return None
            positions.append(child)
    except (IndexError, ValueError):
        return None

    return (
        row_starts,
        row_ends,
        data,
        tuple(positions[node] for node in result_nodes),
    )


def _vectorized_get_chunk(chunk, values, parsed_paths, target_types):
    planned = _vectorized_path_positions(
        values, chunk.field(1), len(chunk) - chunk.null_count,
        parsed_paths)
    if planned is None:
        return None
    row_starts, row_ends, data, positions = planned
    results = []
    for pos, target_type in zip(positions, target_types):
        if not (pa.types.is_float32(target_type)
                or pa.types.is_float64(target_type)):
            return None
        absolute = row_starts + pos
        headers = data[absolute]
        if np.all(headers == _primitive_header(_DOUBLE)):
            value_size, data_type = 8, np.dtype('<f8')
        elif np.all(headers == _primitive_header(_FLOAT)):
            value_size, data_type = 4, np.dtype('<f4')
        else:
            return None
        if np.any(absolute + 1 + value_size > row_ends):
            return None
        indices = absolute[:, None] + 1 + np.arange(value_size)
        raw = np.ascontiguousarray(data[indices])
        result = raw.view(data_type).reshape(-1)
        results.append(pa.array(result, type=target_type))
    return results


def _decode_scalar(value, metadata: bytes, pos: int):
    value_type = _variant_get_type(value, pos)
    if value_type == _Type.DOUBLE:
        return struct.unpack_from('<d', value, pos + 1)[0]
    if value_type == _Type.FLOAT:
        return struct.unpack_from('<f', value, pos + 1)[0]
    return GenericVariant(bytes(value), metadata, pos).to_python()


def _patched_chunk(
        chunk: pa.StructArray,
        values: _BinaryValues,
        data: bytearray,
        start: int,
) -> pa.StructArray:
    patched_values = values.array_from_data(data, start)
    metadata = chunk.field(1)
    if chunk.offset == 0 and patched_values.offset == 0:
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
        self._fixed_size = (
            struct.calcsize(self._value_format)
            if self._value_format is not None else None
        )

    def values(self, offset: int, length: int):
        if self._array is None:
            return self._value.as_py()
        return self._array.slice(offset, length).to_pylist()

    def value_at(self, values, row: int):
        return values if self._array is None else values[row]

    def fixed_size(self, value) -> Optional[int]:
        return self._fixed_size if value is not None else None

    def patch(self, data: bytearray, pos: int, value) -> None:
        struct.pack_into(
            self._value_format, data, pos, self._type_header, value)

    def numpy_values(self, offset: int, length: int):
        if self._value_format is None:
            return None
        data_type = (
            np.dtype('<f8') if pa.types.is_float64(self.type)
            else np.dtype('<f4')
        )
        if self._array is None:
            if not self._value.is_valid:
                return None
            return np.full(length, self._value.as_py(), dtype=data_type)

        values = self._array.slice(offset, length)
        if values.null_count:
            return None
        if isinstance(values, pa.ChunkedArray):
            values = values.combine_chunks()
        return np.asarray(
            values.to_numpy(zero_copy_only=False), dtype=data_type)

    def encode(self, value) -> bytes:
        if value is not None and self._value_format is not None:
            return struct.pack(
                self._value_format, self._type_header, value)
        return _encode_scalar_to_value_bytes(value, self.type)


def _vectorized_replace_chunk(
        chunk,
        values,
        parsed,
        parsed_paths,
        global_row,
):
    planned = _vectorized_path_positions(
        values,
        chunk.field(1),
        len(chunk) - chunk.null_count,
        parsed_paths,
    )
    if planned is None:
        return None
    row_starts, row_ends, input_data, positions = planned

    replacements = []
    for (_, _, provider), pos in zip(parsed, positions):
        replacement = provider.numpy_values(global_row, len(chunk))
        if replacement is None:
            return None
        absolute = row_starts + pos
        expected_header = provider._type_header
        if not np.all(input_data[absolute] == expected_header):
            return None
        if np.any(absolute + provider._fixed_size > row_ends):
            return None
        replacements.append((pos, provider, replacement))

    data, data_start = values.copy_used_data()
    output_data = np.frombuffer(data, dtype=np.uint8)
    relative_starts = row_starts - data_start
    for pos, provider, replacement in replacements:
        absolute = relative_starts + pos
        output_data[absolute] = provider._type_header
        value_size = provider._fixed_size - 1
        replacement_bytes = np.ascontiguousarray(
            replacement).view(np.uint8).reshape(len(chunk), value_size)
        indices = absolute[:, None] + 1 + np.arange(value_size)
        output_data[indices] = replacement_bytes

    return _patched_chunk(chunk, values, data, data_start)


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
        vectorized = _vectorized_get_chunk(
            chunk,
            values,
            parsed_paths,
            [target_type for _, _, target_type in parsed],
        )
        if vectorized is not None:
            for (path, _, _), result in zip(parsed, vectorized):
                result_chunks[path].append(result)
            continue
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
        vectorized = _vectorized_replace_chunk(
            chunk,
            values,
            parsed,
            parsed_paths,
            global_row,
        )
        if vectorized is not None:
            result_chunks.append(vectorized)
            global_row += len(chunk)
            continue
        metadata = chunk.field(1).to_pylist()
        valid = chunk.is_valid().to_pylist()
        chunk_replacements = {
            path: provider.values(global_row, len(chunk))
            for path, _, provider in parsed
        }
        patched_data = None
        data_start = 0
        rebuild = False
        for row in range(len(chunk)):
            if not valid[row]:
                continue
            row_start, value = values.row(row)
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
                replacement_value = provider.value_at(
                    chunk_replacements[path], row)
                new_size = provider.fixed_size(replacement_value)
                encoded = (
                    None if new_size is not None
                    else provider.encode(replacement_value)
                )
                if new_size is None:
                    new_size = len(encoded)
                if new_size != _value_size(value, pos):
                    rebuild = True
                    break
                if patched_data is None:
                    patched_data, data_start = values.copy_used_data()
                patch_pos = row_start - data_start + pos
                if encoded is None:
                    provider.patch(
                        patched_data, patch_pos, replacement_value)
                else:
                    patched_data[patch_pos:patch_pos + new_size] = encoded
            if rebuild:
                break

        if rebuild:
            rebuilt_values = []
            for row in range(len(chunk)):
                value = bytes(values.view(row))
                if not valid[row]:
                    rebuilt_values.append(value)
                    continue
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
                    replacement_value = provider.value_at(
                        chunk_replacements[path], row)
                    encoded = provider.encode(replacement_value)
                    value = _replace_path(
                        value, metadata[row], 0, parsed_path, encoded)
                rebuilt_values.append(value)
            result_chunks.append(_rebuilt_chunk(chunk, rebuilt_values))
        elif patched_data is not None:
            result_chunks.append(_patched_chunk(
                chunk, values, patched_data, data_start))
        else:
            result_chunks.append(chunk)
        global_row += len(chunk)

    if not chunked:
        return result_chunks[0]
    return pa.chunked_array(result_chunks, type=data_type)
