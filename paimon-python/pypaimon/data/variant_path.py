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

import base64
import datetime
import decimal
import functools
import json
import math
import re
import struct
from typing import Dict, Mapping, Optional, Sequence, Tuple

import numpy as np
import pyarrow as pa

from pypaimon.data._variant_binary import (
    _ARRAY,
    _OBJECT,
    _SHORT_STR,
    _U32_SIZE,
    _primitive_header,
    _read_unsigned,
)
from pypaimon.data.generic_variant import (
    _BINARY,
    GenericVariant,
    _DOUBLE,
    _FLOAT,
    _LONG_STR,
    _PRIMITIVE_FIXED_SIZES,
)
from pypaimon.data.variant_shredding import (
    _build_array_value,
    _build_object_value,
    _encode_scalar_to_value_bytes,
)


_INDEX_PATTERN = re.compile(r"\[(\d+)]")
_KEY_PATTERN = re.compile(r"\.([^\.\[]+)|\['([^']+)']|\[\"([^\"]+)\"]")
_Path = Tuple[Tuple[str, object], ...]


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


def _malformed(message):
    raise ValueError(f"MALFORMED_VARIANT: {message}")


def _require_range(pos, size, limit):
    if pos < 0 or size < 0 or pos + size > limit:
        _malformed("value is truncated")


def _checked_object_layout(value, pos, limit):
    _require_range(pos, 2, limit)
    type_info = (value[pos] >> 2) & 0x3F
    size_width = _U32_SIZE if ((type_info >> 4) & 0x1) else 1
    _require_range(pos + 1, size_width, limit)
    size = _read_unsigned(value, pos + 1, size_width)
    id_width = ((type_info >> 2) & 0x3) + 1
    offset_width = (type_info & 0x3) + 1
    id_start = pos + 1 + size_width
    offset_start = id_start + size * id_width
    data_start = offset_start + (size + 1) * offset_width
    _require_range(pos, data_start - pos, limit)
    offsets = []
    for index in range(size + 1):
        offset = _read_unsigned(
            value, offset_start + index * offset_width, offset_width)
        offsets.append(offset)
    sentinel = offsets[-1]
    if ((size and (min(offsets[:-1]) != 0
                   or len(set(offsets[:-1])) != size))
            or any(offset >= sentinel for offset in offsets[:-1])):
        _malformed("invalid object offsets")
    _require_range(data_start, sentinel, limit)
    return (
        size, id_width, id_start, data_start, offsets,
        data_start + offsets[-1],
    )


def _checked_array_layout(value, pos, limit):
    _require_range(pos, 2, limit)
    type_info = (value[pos] >> 2) & 0x3F
    size_width = _U32_SIZE if ((type_info >> 2) & 0x1) else 1
    _require_range(pos + 1, size_width, limit)
    size = _read_unsigned(value, pos + 1, size_width)
    offset_width = (type_info & 0x3) + 1
    offset_start = pos + 1 + size_width
    data_start = offset_start + (size + 1) * offset_width
    _require_range(pos, data_start - pos, limit)
    offsets = []
    previous = 0
    for index in range(size + 1):
        offset = _read_unsigned(
            value, offset_start + index * offset_width, offset_width)
        if (index == 0 and offset != 0) or offset < previous:
            _malformed("invalid array offsets")
        offsets.append(offset)
        previous = offset
    _require_range(data_start, offsets[-1], limit)
    return size, data_start, offsets, data_start + offsets[-1]


def _checked_value_size(value, pos, limit=None):
    limit = len(value) if limit is None else limit
    _require_range(pos, 1, limit)
    header = value[pos]
    basic_type = header & 0x3
    type_info = (header >> 2) & 0x3F
    if basic_type == _OBJECT:
        end = _checked_object_layout(value, pos, limit)[-1]
    elif basic_type == _ARRAY:
        end = _checked_array_layout(value, pos, limit)[-1]
    elif basic_type == _SHORT_STR:
        end = pos + 1 + type_info
    else:
        fixed_size = _PRIMITIVE_FIXED_SIZES.get(type_info)
        if fixed_size is not None:
            end = pos + fixed_size
        elif type_info in (_BINARY, _LONG_STR):
            _require_range(pos + 1, _U32_SIZE, limit)
            end = (
                pos + 1 + _U32_SIZE
                + _read_unsigned(value, pos + 1, _U32_SIZE)
            )
        else:
            _malformed(f"unknown primitive type {type_info}")
    _require_range(pos, end - pos, limit)
    return end - pos


@functools.lru_cache(maxsize=2048)
def _field_slot(id_table: bytes, id_size: int, key_id: int) -> Optional[int]:
    for slot in range(len(id_table) // id_size):
        if _read_unsigned(id_table, slot * id_size, id_size) == key_id:
            return slot
    return None


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


def _path_positions(
        value: bytes,
        metadata: bytes,
        paths: Sequence[_Path],
        plans,
) -> Sequence[Optional[int]]:
    del plans
    root_size = _checked_value_size(value, 0)
    if root_size != len(value):
        _malformed("trailing bytes after root value")
    key_ids = _metadata_key_ids(metadata)
    nodes, result_nodes = _compile_paths(tuple(paths))
    bounds = [(0, len(value))]
    for parent_node, kind, segment in nodes[1:]:
        parent = bounds[parent_node]
        if parent is None:
            bounds.append(None)
            continue
        parent_pos, parent_end = parent
        basic_type = value[parent_pos] & 0x3
        if kind == 'key':
            key_id = key_ids.get(segment)
            if key_id is None or basic_type != _OBJECT:
                bounds.append(None)
                continue
            size, id_width, id_start, data_start, offsets, _ = (
                _checked_object_layout(value, parent_pos, parent_end))
            id_table = bytes(value[id_start:id_start + size * id_width])
            slot = _field_slot(id_table, id_width, key_id)
            if slot is None:
                bounds.append(None)
                continue
            child_start = data_start + offsets[slot]
            child_size = _checked_value_size(
                value, child_start, data_start + offsets[-1])
            child_end = child_start + child_size
        else:
            if basic_type != _ARRAY:
                bounds.append(None)
                continue
            size, data_start, offsets, _ = _checked_array_layout(
                value, parent_pos, parent_end)
            if segment >= size:
                bounds.append(None)
                continue
            slot = segment
            child_start = data_start + offsets[slot]
            child_end = data_start + offsets[slot + 1]
        if _checked_value_size(value, child_start, child_end) != (
                child_end - child_start):
            _malformed("child size does not match container offsets")
        bounds.append((child_start, child_end))
    return tuple(
        None if bounds[node] is None else bounds[node][0]
        for node in result_nodes
    )


def _replace_path(
        value: bytes,
        metadata: bytes,
        pos: int,
        path: _Path,
        replacement: bytes,
        limit=None,
) -> bytes:
    limit = len(value) if limit is None else limit
    value_end = pos + _checked_value_size(value, pos, limit)
    if not path:
        return replacement

    kind, segment = path[0]
    if kind == 'key':
        if (value[pos] & 0x3) != _OBJECT:
            raise ValueError("VARIANT path expects an object")
        key_id = _metadata_key_ids(metadata).get(segment)
        if key_id is None:
            raise ValueError(f"VARIANT path does not exist: {segment}")
        size, id_size, id_start, data_start, offsets, container_end = (
            _checked_object_layout(value, pos, value_end))
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
            child_pos = data_start + offsets[i]
            child_end = child_pos + _checked_value_size(
                value, child_pos, container_end)
            child = value[child_pos:child_end]
            if i == slot:
                child = _replace_path(
                    value, metadata, child_pos, path[1:], replacement,
                    child_end)
            children.append(child)
        return _build_object_value(list(zip(ids, children)))

    if (value[pos] & 0x3) != _ARRAY:
        raise ValueError("VARIANT path expects an array")
    size, data_start, offsets, _ = _checked_array_layout(
        value, pos, value_end)
    if segment >= size:
        raise ValueError(f"VARIANT array index does not exist: {segment}")
    children = []
    for i in range(size):
        child_pos = data_start + offsets[i]
        child_end = data_start + offsets[i + 1]
        child = value[child_pos:child_end]
        if i == segment:
            child = _replace_path(
                value, metadata, child_pos, path[1:], replacement,
                child_end)
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
    if len(positions) and np.all(widths == widths[0]):
        width = int(widths[0])
        if (width < 1 or width > 4
                or np.any(positions < 0)
                or np.any(positions + width > len(data))):
            raise ValueError("Invalid VARIANT offset")
        if width == 1:
            return data[positions].astype(np.int64, copy=False)
        indices = positions[:, None] + np.arange(width)
        values = data[indices].astype(np.int64, copy=False)
        return np.sum(
            values << (np.arange(width, dtype=np.int64) * 8), axis=1)

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
    limits = [row_ends - row_starts]

    try:
        for parent_node, kind, segment in nodes[1:]:
            parent = positions[parent_node]
            parent_ends = row_starts + limits[parent_node]
            absolute_parent = row_starts + parent
            if (np.any(absolute_parent < row_starts)
                    or np.any(absolute_parent >= parent_ends)):
                return None
            headers = data[absolute_parent]
            type_info = (headers >> 2).astype(np.int64, copy=False)

            if kind == 'key':
                if np.any((headers & 0x3) != _OBJECT):
                    return None
                key_id = key_ids.get(segment)
                if key_id is None:
                    return None
                first_layout = _checked_object_layout(
                    first_value, int(parent[0]), int(limits[parent_node][0]))
                size, id_size, id_start, _, first_offsets, _ = first_layout
                id_table = bytes(
                    first_value[id_start:id_start + size * id_size])
                slot = _field_slot(id_table, id_size, key_id)
                if slot is None:
                    return None
                successor_slot = min(
                    (
                        index for index in range(size + 1)
                        if first_offsets[index] > first_offsets[slot]
                    ),
                    key=lambda index: first_offsets[index],
                )

                size_widths = np.where(
                    ((type_info >> 4) & 0x1) != 0, _U32_SIZE, 1)
                if np.any(absolute_parent + 1 + size_widths > parent_ends):
                    return None
                sizes = _take_unsigned(
                    data, absolute_parent + 1, size_widths)
                id_widths = ((type_info >> 2) & 0x3) + 1
                offset_widths = (type_info & 0x3) + 1
                if np.any(sizes != size):
                    return None
                id_starts = absolute_parent + 1 + size_widths
                offset_starts = id_starts + sizes * id_widths
                data_starts = offset_starts + (sizes + 1) * offset_widths
                if np.any(data_starts > parent_ends):
                    return None
                ids = _take_unsigned(
                    data, id_starts + slot * id_widths, id_widths)
                if np.any(ids != key_id):
                    return None
            else:
                if np.any((headers & 0x3) != _ARRAY):
                    return None
                size = _checked_array_layout(
                    first_value, int(parent[0]),
                    int(limits[parent_node][0]))[0]
                if segment >= size:
                    return None
                size_widths = np.where(
                    ((type_info >> 2) & 0x1) != 0, _U32_SIZE, 1)
                if np.any(absolute_parent + 1 + size_widths > parent_ends):
                    return None
                sizes = _take_unsigned(
                    data, absolute_parent + 1, size_widths)
                if np.any(sizes != size):
                    return None
                slot = segment
                successor_slot = slot + 1
                offset_widths = (type_info & 0x3) + 1
                offset_starts = absolute_parent + 1 + size_widths
                data_starts = offset_starts + (sizes + 1) * offset_widths
                if np.any(data_starts > parent_ends):
                    return None
            offsets = _take_unsigned(
                data,
                offset_starts + slot * offset_widths,
                offset_widths,
            )
            next_offsets = _take_unsigned(
                data,
                offset_starts + successor_slot * offset_widths,
                offset_widths,
            )
            final_offsets = _take_unsigned(
                data,
                offset_starts + sizes * offset_widths,
                offset_widths,
            )

            child = data_starts + offsets - row_starts
            child_ends = data_starts + next_offsets - row_starts
            if (np.any(offsets >= next_offsets)
                    or np.any(next_offsets > final_offsets)
                    or np.any(data_starts + final_offsets != parent_ends)
                    or np.any(child < 0)
                    or np.any(child >= child_ends)):
                return None
            positions.append(child)
            limits.append(child_ends)
    except (IndexError, ValueError):
        return None

    return (
        row_starts,
        data,
        tuple(positions[node] for node in result_nodes),
        tuple(limits[node] for node in result_nodes),
    )


def _vectorized_get_chunk(chunk, values, parsed_paths, target_types):
    planned = _vectorized_path_positions(
        values, chunk.field(1), len(chunk) - chunk.null_count,
        parsed_paths)
    if planned is None:
        return None
    row_starts, data, positions, limits = planned
    results = []
    for pos, limit, target_type in zip(positions, limits, target_types):
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
        if np.any(absolute + 1 + value_size != row_starts + limit):
            return None
        indices = absolute[:, None] + 1 + np.arange(value_size)
        raw = np.ascontiguousarray(data[indices])
        result = raw.view(data_type).reshape(-1)
        results.append(pa.array(result, type=target_type))
    return results


def _decimal_text(value):
    text = format(value, 'f')
    if '.' in text:
        text = text.rstrip('0').rstrip('.')
    return '0' if text in ('', '-0') else text


def _json_text(value):
    if value is None:
        return 'null'
    if isinstance(value, bool):
        return 'true' if value else 'false'
    if isinstance(value, decimal.Decimal):
        return _decimal_text(value)
    if isinstance(value, float):
        return str(value) if math.isfinite(value) else json.dumps(str(value))
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False, separators=(',', ':'))
    if isinstance(value, bytes):
        return json.dumps(base64.b64encode(value).decode('ascii'))
    if isinstance(value, (datetime.date, datetime.datetime)):
        return json.dumps(value.isoformat())
    if isinstance(value, dict):
        return '{' + ','.join(
            f'{json.dumps(key, ensure_ascii=False)}:{_json_text(child)}'
            for key, child in value.items()
        ) + '}'
    if isinstance(value, (list, tuple)):
        return '[' + ','.join(_json_text(child) for child in value) + ']'
    return json.dumps(str(value), ensure_ascii=False)


def _cast_decimal(value, target_type):
    if isinstance(value, bool):
        value = decimal.Decimal(1 if value else 0)
    elif not isinstance(value, decimal.Decimal):
        value = decimal.Decimal(str(value))
    quantum = decimal.Decimal((0, (1,), -target_type.scale))
    with decimal.localcontext() as context:
        context.prec = max(50, target_type.precision + abs(target_type.scale))
        result = value.quantize(quantum, rounding=decimal.ROUND_HALF_UP)
    digits = len(result.as_tuple().digits)
    if digits > target_type.precision:
        raise ValueError("decimal precision overflow")
    return result


def _cast_integer(value, target_type):
    number = int(value)
    bits = target_type.bit_width
    return ((number + (1 << (bits - 1))) % (1 << bits)) - (1 << (bits - 1))


def _cast_python(value, target_type):
    if value is None or pa.types.is_null(target_type):
        return None
    try:
        if pa.types.is_struct(target_type):
            if isinstance(value, str):
                value = json.loads(value)
            if not isinstance(value, dict):
                raise TypeError
            return {
                field.name: (
                    None if field.name not in value
                    else _cast_python(value[field.name], field.type)
                )
                for field in target_type
            }
        if (pa.types.is_list(target_type)
                or pa.types.is_large_list(target_type)
                or pa.types.is_fixed_size_list(target_type)):
            if isinstance(value, str):
                value = json.loads(value)
            if not isinstance(value, list):
                raise TypeError
            return [
                _cast_python(child, target_type.value_type)
                for child in value
            ]
        if pa.types.is_map(target_type):
            if isinstance(value, str):
                value = json.loads(value)
            if not isinstance(value, dict) or not (
                    pa.types.is_string(target_type.key_type)
                    or pa.types.is_large_string(target_type.key_type)):
                raise TypeError
            return [
                (key, _cast_python(child, target_type.item_type))
                for key, child in value.items()
            ]
        if pa.types.is_string(target_type) or pa.types.is_large_string(
                target_type):
            if isinstance(value, (dict, list)):
                return _json_text(value)
            if isinstance(value, bool):
                return str(value).lower()
            if isinstance(value, decimal.Decimal):
                return _decimal_text(value)
            if isinstance(value, (datetime.date, datetime.datetime)):
                return value.isoformat()
            return str(value)
        if pa.types.is_boolean(target_type):
            if isinstance(value, str):
                lowered = value.strip().lower()
                if lowered not in ('true', 'false'):
                    raise ValueError
                return lowered == 'true'
            if isinstance(value, (bool, int, decimal.Decimal)):
                return value != 0
            raise TypeError
        if pa.types.is_signed_integer(target_type):
            if not isinstance(value, (bool, int, float, decimal.Decimal, str)):
                raise TypeError
            return _cast_integer(value, target_type)
        if pa.types.is_floating(target_type):
            if not isinstance(value, (bool, int, float, decimal.Decimal, str)):
                raise TypeError
            return float(value)
        if pa.types.is_decimal(target_type):
            if not isinstance(value, (bool, int, float, decimal.Decimal, str)):
                raise TypeError
            return _cast_decimal(value, target_type)
        if pa.types.is_binary(target_type) or pa.types.is_large_binary(
                target_type):
            if not isinstance(value, str):
                raise TypeError
            return value.encode('utf-8')
        if pa.types.is_date32(target_type):
            if isinstance(value, datetime.datetime):
                return value.date()
            if isinstance(value, datetime.date):
                return value
            if isinstance(value, str):
                return datetime.date.fromisoformat(value)
            raise TypeError
        if pa.types.is_timestamp(target_type):
            if isinstance(value, datetime.datetime):
                return value
            if isinstance(value, datetime.date):
                return datetime.datetime.combine(value, datetime.time())
            if isinstance(value, str):
                return datetime.datetime.fromisoformat(value)
            if isinstance(value, (int, float)):
                return datetime.datetime.fromtimestamp(
                    value, tz=datetime.timezone.utc)
            raise TypeError
    except (ArithmeticError, TypeError, ValueError):
        pass
    raise ValueError(f"Invalid cast {value!r} to {target_type}")


def _decode_scalar(value, metadata: bytes, pos: int, target_type):
    size = _checked_value_size(value, pos)
    selected = bytes(value[pos:pos + size])
    decoded = GenericVariant(selected, metadata).to_python()
    return _cast_python(decoded, target_type)


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
    row_starts, input_data, positions, limits = planned

    replacements = []
    for (_, _, provider), pos, limit in zip(parsed, positions, limits):
        replacement = provider.numpy_values(global_row, len(chunk))
        if replacement is None:
            return None
        absolute = row_starts + pos
        expected_header = provider._type_header
        if not np.all(input_data[absolute] == expected_header):
            return None
        if np.any(
                absolute + provider._fixed_size != row_starts + limit):
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
            for (path, _, target_type), pos in zip(parsed, positions):
                results[path].append(
                    None if pos is None
                    else _decode_scalar(
                        value, metadata[row], pos, target_type)
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
                if new_size != _checked_value_size(value, pos):
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
