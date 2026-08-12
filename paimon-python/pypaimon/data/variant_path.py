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
from typing import Dict, Mapping, Optional, Sequence, Tuple

import numpy as np
import pyarrow as pa

from pypaimon.data._variant_binary import (
    _ARRAY,
    _OBJECT,
    _PRIMITIVE,
    _SHORT_STR,
    _U32_SIZE,
    _VERSION,
    _VERSION_MASK,
    _primitive_header,
    _read_unsigned,
)
from pypaimon.data.generic_variant import (
    _BINARY,
    _DECIMAL4,
    _DECIMAL8,
    _DECIMAL16,
    _DOUBLE,
    _FLOAT,
    _LONG_STR,
    _NULL,
    _MAX_DECIMAL4_PRECISION,
    _MAX_DECIMAL8_PRECISION,
    _MAX_DECIMAL16_PRECISION,
    _PRIMITIVE_FIXED_SIZES,
    GenericVariant,
    _Type,
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
_SLOW_PATH_ROWS = 64


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


def _metadata_key_ids(metadata: bytes) -> Dict[str, int]:
    if not metadata:
        _malformed("empty metadata")
    if (metadata[0] & _VERSION_MASK) != _VERSION:
        _malformed("invalid metadata version")
    offset_size = ((metadata[0] >> 6) & 0x3) + 1
    _require_range(1, offset_size, len(metadata))
    size = _read_unsigned(metadata, 1, offset_size)
    offset_start = 1 + offset_size
    string_start = offset_start + (size + 1) * offset_size
    _require_range(offset_start, (size + 1) * offset_size, len(metadata))
    string_size = len(metadata) - string_start
    result = {}
    previous = 0
    for key_id in range(size):
        start = _read_unsigned(
            metadata, offset_start + key_id * offset_size, offset_size)
        end = _read_unsigned(
            metadata, offset_start + (key_id + 1) * offset_size,
            offset_size)
        if start != previous or end < start or end > string_size:
            _malformed("invalid metadata offsets")
        try:
            key = metadata[string_start + start:string_start + end].decode(
                'utf-8')
        except UnicodeDecodeError:
            _malformed("invalid metadata string")
        if key in result:
            _malformed("duplicate metadata key")
        result[key] = key_id
        previous = end
    sentinel = _read_unsigned(
        metadata, offset_start + size * offset_size, offset_size)
    if sentinel != string_size or sentinel != previous:
        _malformed("invalid metadata offsets")
    return result


def _validate_metadata_version(metadata):
    if not metadata:
        _malformed("empty metadata")
    if (metadata[0] & _VERSION_MASK) != _VERSION:
        _malformed("invalid metadata version")


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


def _checked_object_child_bounds(
        value, data_start, offsets, slot, end_by_offset=None):
    child_offset = offsets[slot]
    next_offset = (
        min(offset for offset in offsets if offset > child_offset)
        if end_by_offset is None else end_by_offset[child_offset]
    )
    child_start = data_start + child_offset
    child_end = data_start + next_offset
    if _checked_value_size(value, child_start, child_end) != (
            child_end - child_start):
        _malformed("child size does not match container offsets")
    return child_start, child_end


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
            _require_range(pos, fixed_size, limit)
            decimal_limit = {
                _DECIMAL4: _MAX_DECIMAL4_PRECISION,
                _DECIMAL8: _MAX_DECIMAL8_PRECISION,
                _DECIMAL16: _MAX_DECIMAL16_PRECISION,
            }.get(type_info)
            if decimal_limit is not None:
                scale = value[pos + 1]
                unscaled = int.from_bytes(
                    value[pos + 2:end], 'little', signed=True)
                precision = len(str(abs(unscaled))) if unscaled else 1
                if scale > decimal_limit or precision > decimal_limit:
                    _malformed("invalid decimal precision or scale")
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
) -> Sequence[Optional[int]]:
    root_size = _checked_value_size(value, 0)
    if root_size != len(value):
        _malformed("trailing bytes after root value")
    nodes, result_nodes = _compile_paths(tuple(paths))
    _validate_metadata_version(metadata)
    key_ids = (
        _metadata_key_ids(metadata)
        if any(kind == 'key' for _, kind, _ in nodes[1:]) else {}
    )
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
            child_start, child_end = _checked_object_child_bounds(
                value, data_start, offsets, slot)
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
        key_ids=None,
) -> bytes:
    limit = len(value) if limit is None else limit
    value_end = pos + _checked_value_size(value, pos, limit)
    if not path:
        return replacement

    kind, segment = path[0]
    if kind == 'key':
        if (value[pos] & 0x3) != _OBJECT:
            raise ValueError("VARIANT path expects an object")
        if key_ids is None:
            key_ids = _metadata_key_ids(metadata)
        key_id = key_ids.get(segment)
        if key_id is None:
            raise ValueError(f"VARIANT path does not exist: {segment}")
        size, id_size, id_start, data_start, offsets, _ = (
            _checked_object_layout(value, pos, value_end))
        ids = [
            _read_unsigned(value, id_start + i * id_size, id_size)
            for i in range(size)
        ]
        try:
            slot = ids.index(key_id)
        except ValueError:
            raise ValueError(f"VARIANT path does not exist: {segment}")
        ordered_offsets = sorted(offsets)
        end_by_offset = dict(zip(ordered_offsets, ordered_offsets[1:]))
        children = []
        for i in range(size):
            child_pos, child_end = _checked_object_child_bounds(
                value, data_start, offsets, i, end_by_offset)
            child = value[child_pos:child_end]
            if i == slot:
                child = _replace_path(
                    value, metadata, child_pos, path[1:], replacement,
                    child_end, key_ids)
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
                child_end, key_ids)
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
    if not (pa.types.is_binary(data_type[1].type)
            or pa.types.is_large_binary(data_type[1].type)):
        raise TypeError("VARIANT metadata field must be binary")
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


def _all_binary_values_equal(
        values: _BinaryValues, expected: bytes, rows=None) -> bool:
    offsets = values.numpy_offsets()
    lengths = offsets[1:] - offsets[:-1]
    starts = offsets[:-1]
    if rows is not None and len(rows) != len(lengths):
        lengths = lengths[rows]
        starts = starts[rows]
    if np.any(lengths != len(expected)):
        return False
    if not len(lengths) or not expected:
        return True

    data = np.frombuffer(values.data, dtype=np.uint8)
    expected_array = np.frombuffer(expected, dtype=np.uint8)
    rows_per_batch = max(1, (1024 * 1024) // len(expected))
    for row in range(0, len(lengths), rows_per_batch):
        end = min(row + rows_per_batch, len(lengths))
        indices = starts[row:end, None] + np.arange(len(expected))
        if not np.all(data[indices] == expected_array):
            return False
    return True


def _valid_row_indices(chunk, values, metadata):
    if (chunk.null_count == 0
            and values.array.null_count == 0
            and metadata.null_count == 0):
        return np.arange(len(chunk), dtype=np.int64)
    valid = np.asarray(
        chunk.is_valid().to_numpy(zero_copy_only=False), dtype=bool)
    value_valid = np.asarray(
        values.array.is_valid().to_numpy(zero_copy_only=False), dtype=bool)
    metadata_valid = np.asarray(
        metadata.is_valid().to_numpy(zero_copy_only=False), dtype=bool)
    if np.any(valid & (~value_valid | ~metadata_valid)):
        _malformed("valid VARIANT row has a null child")
    return np.flatnonzero(valid)


def _vectorized_path_positions(
        values: _BinaryValues,
        metadata: pa.Array,
        valid_rows,
        paths: Sequence[_Path],
):
    if not len(valid_rows):
        return None

    metadata_values = _BinaryValues(metadata)
    first_row = int(valid_rows[0])
    first_metadata = bytes(metadata_values.view(first_row))
    if not _all_binary_values_equal(
            metadata_values, first_metadata, valid_rows):
        return None

    nodes, result_nodes = _compile_paths(tuple(paths))
    _validate_metadata_version(first_metadata)
    key_ids = (
        _metadata_key_ids(first_metadata)
        if any(kind == 'key' for _, kind, _ in nodes[1:]) else {}
    )
    row_offsets = values.numpy_offsets()
    if len(valid_rows) == len(values.array):
        row_starts = row_offsets[:-1]
        row_ends = row_offsets[1:]
    else:
        row_starts = row_offsets[:-1][valid_rows]
        row_ends = row_offsets[1:][valid_rows]
    data = np.frombuffer(values.data, dtype=np.uint8)
    first_value = values.view(first_row)
    positions = [np.zeros(len(valid_rows), dtype=np.int64)]
    limits = [row_ends - row_starts]

    try:
        for parent_node, kind, segment in nodes[1:]:
            parent = positions[parent_node]
            if parent is None:
                positions.append(None)
                limits.append(None)
                continue
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
                    positions.append(None)
                    limits.append(None)
                    continue
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
        valid_rows,
        row_starts,
        data,
        tuple(positions[node] for node in result_nodes),
        tuple(limits[node] for node in result_nodes),
    )


def _partition_path_plans(values, metadata, valid_rows, parsed_paths):
    planned = _vectorized_path_positions(
        values, metadata, valid_rows, parsed_paths)
    if planned is not None:
        return [planned], []
    if len(valid_rows) <= _SLOW_PATH_ROWS:
        return [], list(valid_rows)
    middle = len(valid_rows) // 2
    left_plans, left_rows = _partition_path_plans(
        values, metadata, valid_rows[:middle], parsed_paths)
    right_plans, right_rows = _partition_path_plans(
        values, metadata, valid_rows[middle:], parsed_paths)
    return left_plans + right_plans, left_rows + right_rows


def _vectorized_get_chunk(chunk, values, parsed_paths, target_types):
    if not all(pa.types.is_float32(target_type)
               or pa.types.is_float64(target_type)
               for target_type in target_types):
        return None
    valid_rows = _valid_row_indices(chunk, values, chunk.field(1))
    if not len(valid_rows):
        return [pa.nulls(len(chunk), type=target_type)
                for target_type in target_types]
    plans, slow_rows = _partition_path_plans(
        values, chunk.field(1), valid_rows, parsed_paths)
    if len(valid_rows) == len(chunk) and len(plans) == 1 and not slow_rows:
        _, row_starts, data, positions, limits = plans[0]
        results = []
        uniform = True
        for pos, limit, target_type in zip(
                positions, limits, target_types):
            if pos is None:
                results.append(pa.nulls(len(chunk), type=target_type))
                continue
            absolute = row_starts + pos
            headers = data[absolute]
            type_info = (
                _FLOAT if pa.types.is_float32(target_type) else _DOUBLE)
            value_size = 4 if type_info == _FLOAT else 8
            data_type = (
                np.dtype('<f4') if type_info == _FLOAT else np.dtype('<f8'))
            if not np.all(headers == _primitive_header(type_info)):
                uniform = False
                break
            if np.any(absolute + 1 + value_size != row_starts + limit):
                uniform = False
                break
            indices = absolute[:, None] + 1 + np.arange(value_size)
            raw = np.ascontiguousarray(data[indices])
            result = raw.view(data_type).reshape(-1)
            results.append(pa.array(result, type=target_type))
        if uniform:
            return results
    outputs = [
        np.empty(
            len(chunk),
            dtype=np.dtype('<f4') if pa.types.is_float32(target_type)
            else np.dtype('<f8'),
        )
        for target_type in target_types
    ]
    masks = [np.ones(len(chunk), dtype=bool) for _ in target_types]
    slow_by_path = [set(int(row) for row in slow_rows)
                    for _ in target_types]
    for planned in plans:
        rows, row_starts, data, positions, limits = planned
        for index, (pos, limit) in enumerate(zip(positions, limits)):
            if pos is None:
                continue
            absolute = row_starts + pos
            headers = data[absolute]
            target_type = target_types[index]
            type_info = (
                _FLOAT if pa.types.is_float32(target_type) else _DOUBLE)
            value_size = 4 if type_info == _FLOAT else 8
            data_type = (
                np.dtype('<f4') if type_info == _FLOAT else np.dtype('<f8'))
            handled = (
                (headers == _primitive_header(type_info))
                & (absolute + 1 + value_size == row_starts + limit)
            )
            if np.any(handled):
                selected_rows = rows[handled]
                selected_absolute = absolute[handled]
                indices = (
                    selected_absolute[:, None] + 1 + np.arange(value_size)
                )
                raw = np.ascontiguousarray(data[indices])
                outputs[index][selected_rows] = raw.view(
                    data_type).reshape(-1)
                masks[index][selected_rows] = False
            slow_by_path[index].update(
                int(row) for row in rows[~handled])

    metadata = _BinaryValues(chunk.field(1))
    for row in set().union(*slow_by_path):
        value = values.view(row)
        row_metadata = bytes(metadata.view(row))
        positions = _path_positions(value, row_metadata, parsed_paths)
        for index, (pos, target_type) in enumerate(
                zip(positions, target_types)):
            if row not in slow_by_path[index] or pos is None:
                continue
            decoded = _decode_floating(value, pos, target_type)
            if decoded is not None:
                outputs[index][row] = decoded
                masks[index][row] = False
    return [
        pa.array(output, mask=mask, type=target_type)
        for output, mask, target_type in zip(outputs, masks, target_types)
    ]


def _decode_floating(value, pos, target_type):
    size = _checked_value_size(value, pos)
    header = value[pos]
    if (header & 0x3) != _PRIMITIVE:
        raise TypeError("VARIANT path is not FLOAT or DOUBLE")
    type_info = (header >> 2) & 0x3F
    if type_info == _FLOAT and pa.types.is_float32(target_type):
        return struct.unpack_from('<f', value, pos + 1)[0]
    if type_info == _DOUBLE and pa.types.is_float64(target_type):
        return struct.unpack_from('<d', value, pos + 1)[0]
    if type_info == _NULL and size == 1:
        return None
    raise TypeError(
        f"VARIANT path type does not match {target_type}")


def _variant_object_children(value, metadata, pos, end):
    size, id_size, id_start, data_start, offsets, _ = (
        _checked_object_layout(value, pos, end))
    keys = {
        key_id: key for key, key_id in _metadata_key_ids(metadata).items()
    }
    children = {}
    for slot in range(size):
        key_id = _read_unsigned(value, id_start + slot * id_size, id_size)
        if key_id not in keys:
            _malformed("object key is missing from metadata")
        children[keys[key_id]] = _checked_object_child_bounds(
            value, data_start, offsets, slot)
    return children


def _variant_array_children(value, pos, end):
    size, data_start, offsets, _ = _checked_array_layout(
        value, pos, end)
    children = []
    for index in range(size):
        child_start = data_start + offsets[index]
        child_end = data_start + offsets[index + 1]
        if _checked_value_size(value, child_start, child_end) != (
                child_end - child_start):
            _malformed("child size does not match container offsets")
        children.append((child_start, child_end))
    return children


def _supports_exact_get(data_type):
    if (pa.types.is_boolean(data_type)
            or pa.types.is_signed_integer(data_type)
            or pa.types.is_float32(data_type)
            or pa.types.is_float64(data_type)
            or pa.types.is_string(data_type)
            or pa.types.is_large_string(data_type)
            or pa.types.is_binary(data_type)
            or pa.types.is_large_binary(data_type)
            or pa.types.is_date32(data_type)
            or pa.types.is_decimal128(data_type)):
        return True
    if pa.types.is_timestamp(data_type):
        return data_type.unit == 'us'
    if pa.types.is_struct(data_type):
        return all(_supports_exact_get(field.type) for field in data_type)
    if (pa.types.is_list(data_type)
            or pa.types.is_large_list(data_type)
            or pa.types.is_fixed_size_list(data_type)):
        return _supports_exact_get(data_type.value_type)
    if pa.types.is_map(data_type):
        return ((pa.types.is_string(data_type.key_type)
                 or pa.types.is_large_string(data_type.key_type))
                and _supports_exact_get(data_type.item_type))
    return False


def _validate_decimal_scale(data_type):
    if pa.types.is_decimal128(data_type) and data_type.scale < 0:
        raise ValueError("VARIANT decimal scale must be non-negative")
    if pa.types.is_struct(data_type):
        for field in data_type:
            _validate_decimal_scale(field.type)
    elif (pa.types.is_list(data_type)
          or pa.types.is_large_list(data_type)
          or pa.types.is_fixed_size_list(data_type)):
        _validate_decimal_scale(data_type.value_type)
    elif pa.types.is_map(data_type):
        _validate_decimal_scale(data_type.item_type)


def _exact_primitive_matches(value, pos, data_type):
    variant_type = _variant_get_type(value, pos)
    if variant_type == _Type.NULL:
        return True
    if pa.types.is_boolean(data_type):
        return variant_type == _Type.BOOLEAN
    if pa.types.is_signed_integer(data_type):
        return variant_type == _Type.LONG
    if pa.types.is_float32(data_type):
        return variant_type == _Type.FLOAT
    if pa.types.is_float64(data_type):
        return variant_type == _Type.DOUBLE
    if pa.types.is_string(data_type) or pa.types.is_large_string(data_type):
        return variant_type == _Type.STRING
    if pa.types.is_binary(data_type) or pa.types.is_large_binary(data_type):
        return variant_type == _Type.BINARY
    if pa.types.is_date32(data_type):
        return variant_type == _Type.DATE
    if pa.types.is_timestamp(data_type):
        expected = _Type.TIMESTAMP if data_type.tz else _Type.TIMESTAMP_NTZ
        return variant_type == expected
    if pa.types.is_decimal128(data_type):
        if variant_type != _Type.DECIMAL:
            return False
        scale = value[pos + 1]
        return scale == data_type.scale
    return False


def _decode_exact(value, metadata, pos, data_type):
    size = _checked_value_size(value, pos)
    end = pos + size
    variant_type = _variant_get_type(value, pos)
    if variant_type == _Type.NULL:
        return None
    if pa.types.is_struct(data_type):
        if variant_type != _Type.OBJECT:
            raise TypeError(f"VARIANT path type does not match {data_type}")
        children = _variant_object_children(value, metadata, pos, end)
        return {
            field.name: (
                None if field.name not in children
                else _decode_exact(
                    value, metadata, children[field.name][0], field.type)
            )
            for field in data_type
        }
    if (pa.types.is_list(data_type)
            or pa.types.is_large_list(data_type)
            or pa.types.is_fixed_size_list(data_type)):
        if variant_type != _Type.ARRAY:
            raise TypeError(f"VARIANT path type does not match {data_type}")
        children = _variant_array_children(value, pos, end)
        if (pa.types.is_fixed_size_list(data_type)
                and len(children) != data_type.list_size):
            raise TypeError(f"VARIANT path type does not match {data_type}")
        return [
            _decode_exact(value, metadata, child_pos, data_type.value_type)
            for child_pos, _ in children
        ]
    if pa.types.is_map(data_type):
        if variant_type != _Type.OBJECT:
            raise TypeError(f"VARIANT path type does not match {data_type}")
        return [
            (key, _decode_exact(
                value, metadata, child_pos, data_type.item_type))
            for key, (child_pos, _) in _variant_object_children(
                value, metadata, pos, end).items()
        ]
    if not _exact_primitive_matches(value, pos, data_type):
        raise TypeError(f"VARIANT path type does not match {data_type}")
    return GenericVariant(bytes(value[pos:end]), metadata).to_python()


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


def _rebuilt_offsets(lengths, value_format):
    total = sum(int(length) for length in lengths)
    maximum = np.iinfo(np.dtype(value_format)).max
    if total > maximum:
        kind = 'Binary' if value_format == '<i' else 'LargeBinary'
        suffix = '; use LargeBinary' if kind == 'Binary' else ''
        raise ValueError(
            f'Rebuilt VARIANT values exceed the {kind} offset limit{suffix}')
    offsets = np.empty(len(lengths) + 1, dtype=np.dtype(value_format))
    offsets[0] = 0
    np.cumsum(lengths, out=offsets[1:])
    return offsets


def _sparse_rebuilt_chunk(
        chunk, values, data, data_start, rebuilt_rows):
    old_offsets = values.numpy_offsets()
    lengths = old_offsets[1:] - old_offsets[:-1]
    for row, rebuilt in rebuilt_rows.items():
        lengths[row] = len(rebuilt)
    offsets = _rebuilt_offsets(lengths, values.value_format)
    output = bytearray(int(offsets[-1]))
    source_start = int(old_offsets[0]) - data_start
    target_start = 0
    for row, rebuilt in sorted(rebuilt_rows.items()):
        row_start = int(old_offsets[row]) - data_start
        row_end = int(old_offsets[row + 1]) - data_start
        prefix_size = row_start - source_start
        output[target_start:target_start + prefix_size] = data[
            source_start:row_start]
        target_start += prefix_size
        output[target_start:target_start + len(rebuilt)] = rebuilt
        target_start += len(rebuilt)
        source_start = row_end
    source_end = int(old_offsets[-1]) - data_start
    output[target_start:] = data[source_start:source_end]
    validity = (
        None if values.array.null_count == 0
        else values.array.is_valid().buffers()[1]
    )
    rebuilt_values = pa.Array.from_buffers(
        values.array.type,
        len(chunk),
        [validity, pa.py_buffer(offsets), pa.py_buffer(output)],
        null_count=values.array.null_count,
    )
    return pa.StructArray.from_arrays(
        [rebuilt_values, chunk.field(1)],
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
        _validate_decimal_scale(self.type)
        if not _supported_replacement_type(self.type):
            raise TypeError(
                f"Unsupported exact VARIANT replacement type: {self.type}")
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

    def scalar_at(self, row: int):
        if self._array is None:
            return self._value.as_py()
        return self._array[row].as_py()

    def numpy_values(self, offset: int, length: int, rows=None):
        if self._value_format is None:
            return None
        data_type = (
            np.dtype('<f8') if pa.types.is_float64(self.type)
            else np.dtype('<f4')
        )
        if self._array is None:
            size = length if rows is None else len(rows)
            if not self._value.is_valid:
                return np.empty(size, dtype=data_type), np.zeros(
                    size, dtype=bool)
            return (
                np.full(size, self._value.as_py(), dtype=data_type),
                np.ones(size, dtype=bool),
            )

        values = self._array.slice(offset, length)
        if isinstance(values, pa.ChunkedArray):
            values = values.combine_chunks()
        if rows is not None:
            values = values.take(pa.array(rows, type=pa.int64()))
        valid = np.asarray(
            values.is_valid().to_numpy(zero_copy_only=False), dtype=bool)
        return (
            np.asarray(values.to_numpy(zero_copy_only=False), dtype=data_type),
            valid,
        )

    def encode(self, value) -> bytes:
        if value is not None and self._value_format is not None:
            return struct.pack(
                self._value_format, self._type_header, value)
        return _encode_scalar_to_value_bytes(value, self.type)

    def validate_source(self, value, pos) -> None:
        if not _replacement_type_matches(value, pos, self.type):
            raise TypeError(
                f"VARIANT path type does not match {self.type}")


def _vectorized_replace_chunk(
        chunk,
        values,
        parsed,
        parsed_paths,
        global_row,
        strict,
):
    if not all(provider._fixed_size is not None
               for _, _, provider in parsed):
        return None
    valid_rows = _valid_row_indices(chunk, values, chunk.field(1))
    if not len(valid_rows):
        return chunk
    plans, slow_rows = _partition_path_plans(
        values, chunk.field(1), valid_rows, parsed_paths)
    slow_rows = set(int(row) for row in slow_rows)
    data = None
    data_start = 0
    output_data = None
    for planned in plans:
        rows, row_starts, source_data, positions, limits = planned
        replacements = []
        compatible = np.ones(len(rows), dtype=bool)
        has_replacement = False
        for (path, _, provider), pos, limit in zip(
                parsed, positions, limits):
            if pos is None:
                if strict:
                    raise ValueError(
                        f"VARIANT path does not exist: {path}")
                replacements.append(None)
                continue
            has_replacement = True
            replacement, replacement_valid = provider.numpy_values(
                global_row,
                len(chunk),
                None if len(rows) == len(chunk) else rows,
            )
            absolute = row_starts + pos
            compatible &= (
                replacement_valid
                & (source_data[absolute] == provider._type_header)
                & (absolute + provider._fixed_size == row_starts + limit)
            )
            replacements.append((pos, provider, replacement))
        if not has_replacement:
            continue
        slow_rows.update(int(row) for row in rows[~compatible])
        if not np.any(compatible):
            continue
        if data is None:
            data, data_start = values.copy_used_data()
            output_data = np.frombuffer(data, dtype=np.uint8)
        relative_starts = row_starts - data_start
        compatible_rows = rows[compatible]
        for item in replacements:
            if item is None:
                continue
            pos, provider, replacement = item
            absolute = (relative_starts + pos)[compatible]
            output_data[absolute] = provider._type_header
            value_size = provider._fixed_size - 1
            replacement_bytes = np.ascontiguousarray(
                replacement[compatible]).view(np.uint8).reshape(
                    len(compatible_rows), value_size)
            indices = absolute[:, None] + 1 + np.arange(value_size)
            output_data[indices] = replacement_bytes

    metadata = _BinaryValues(chunk.field(1))
    rebuilt_rows = {}
    for row in slow_rows:
        value = values.view(row)
        row_metadata = bytes(metadata.view(row))
        positions = _path_positions(value, row_metadata, parsed_paths)
        if not any(pos is not None for pos in positions):
            if strict:
                missing = next(
                    path for (path, _, _), pos in zip(parsed, positions)
                    if pos is None)
                raise ValueError(
                    f"VARIANT path does not exist: {missing}")
            continue
        original = bytes(value)
        value = original
        for (_, _, provider), pos in zip(parsed, positions):
            if pos is not None:
                provider.validate_source(value, pos)
        for (path, parsed_path, provider), pos in zip(parsed, positions):
            if pos is None:
                if strict:
                    raise ValueError(f"VARIANT path does not exist: {path}")
                continue
            replacement_value = provider.scalar_at(global_row + row)
            value = _replace_path(
                value,
                row_metadata,
                0,
                parsed_path,
                provider.encode(replacement_value),
            )
        if value != original:
            rebuilt_rows[row] = value

    if rebuilt_rows:
        if data is None:
            data, data_start = values.copy_used_data()
        return _sparse_rebuilt_chunk(
            chunk, values, data, data_start, rebuilt_rows)
    if data is None:
        return chunk
    return _patched_chunk(chunk, values, data, data_start)


def _supported_replacement_type(data_type: pa.DataType) -> bool:
    return (
        pa.types.is_boolean(data_type)
        or pa.types.is_signed_integer(data_type)
        or pa.types.is_float32(data_type)
        or pa.types.is_float64(data_type)
        or pa.types.is_string(data_type)
        or pa.types.is_large_string(data_type)
        or pa.types.is_binary(data_type)
        or pa.types.is_large_binary(data_type)
        or pa.types.is_date32(data_type)
        or (pa.types.is_timestamp(data_type) and data_type.unit == 'us')
        or pa.types.is_decimal128(data_type)
    )


def _replacement_type_matches(value, pos, data_type):
    variant_type = _variant_get_type(value, pos)
    if variant_type == _Type.NULL:
        return True
    if pa.types.is_signed_integer(data_type):
        return variant_type == _Type.LONG
    return _exact_primitive_matches(value, pos, data_type)


def _rowwise_replace_chunk(
        chunk, values, parsed, parsed_paths, global_row, strict):
    metadata = _BinaryValues(chunk.field(1))
    valid = chunk.is_valid().to_pylist()
    rebuilt_rows = {}
    for row in range(len(chunk)):
        if not valid[row]:
            continue
        original = values.view(row)
        row_metadata = bytes(metadata.view(row))
        positions = _path_positions(original, row_metadata, parsed_paths)
        for (path, _, provider), pos in zip(parsed, positions):
            if pos is None:
                if strict:
                    raise ValueError(
                        f"VARIANT path does not exist: {path}")
                continue
            provider.validate_source(original, pos)
        value = None
        for (path, parsed_path, provider), pos in zip(parsed, positions):
            if pos is None:
                continue
            if value is None:
                value = bytes(original)
            value = _replace_path(
                value,
                row_metadata,
                0,
                parsed_path,
                provider.encode(provider.scalar_at(global_row + row)),
            )
        if value is not None and value != original:
            rebuilt_rows[row] = value
    if not rebuilt_rows:
        return chunk
    data, data_start = values.copy_used_data()
    return _sparse_rebuilt_chunk(
        chunk, values, data, data_start, rebuilt_rows)


def _variant_get(column, paths: Mapping[str, pa.DataType]):
    parsed = []
    for path, target_type in paths.items():
        if not isinstance(target_type, pa.DataType):
            raise TypeError("VARIANT data_type must be a PyArrow data type")
        _validate_decimal_scale(target_type)
        if not _supports_exact_get(target_type):
            raise TypeError(
                f"Unsupported exact VARIANT data type: {target_type}")
        parsed.append((path, _parse_path(path), target_type))
    parsed_paths = [parsed_path for _, parsed_path, _ in parsed]
    chunks, chunked, _ = _variant_chunks(column)
    result_chunks = {path: [] for path in paths}
    for chunk in chunks:
        values = _BinaryValues(chunk.field(0))
        results = _vectorized_get_chunk(
            chunk,
            values,
            parsed_paths,
            [target_type for _, _, target_type in parsed],
        )
        if results is not None:
            for (path, _, _), result in zip(parsed, results):
                result_chunks[path].append(result)
            continue
        metadata = _BinaryValues(chunk.field(1))
        valid = chunk.is_valid().to_pylist()
        decoded = {path: [] for path in paths}
        for row in range(len(chunk)):
            if not valid[row]:
                for path in paths:
                    decoded[path].append(None)
                continue
            value = values.view(row)
            row_metadata = bytes(metadata.view(row))
            positions = _path_positions(
                value, row_metadata, parsed_paths)
            for (path, _, data_type), pos in zip(parsed, positions):
                decoded[path].append(
                    None if pos is None
                    else _decode_exact(
                        value, row_metadata, pos, data_type)
                )
        for path, _, data_type in parsed:
            result_chunks[path].append(
                pa.array(decoded[path], type=data_type))
    if not chunked:
        return {path: chunks[0] for path, chunks in result_chunks.items()}
    return {
        path: pa.chunked_array(chunks, type=paths[path])
        for path, chunks in result_chunks.items()
    }


def variant_get(column, path, data_type=None):
    """Read one or more VARIANT paths without implicit casts."""
    if isinstance(path, Mapping):
        if data_type is not None:
            raise TypeError(
                "VARIANT data_type must be omitted for path mappings")
        return _variant_get(column, path)
    if data_type is None:
        raise TypeError("VARIANT data_type must be a PyArrow data type")
    return _variant_get(column, {path: data_type})[path]


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
    """Replace one or more existing VARIANT paths without implicit casts."""
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
    result_chunks = []
    global_row = 0
    for chunk in chunks:
        values = _BinaryValues(chunk.field(0))
        result = _vectorized_replace_chunk(
            chunk,
            values,
            parsed,
            parsed_paths,
            global_row,
            strict,
        )
        if result is None:
            result = _rowwise_replace_chunk(
                chunk,
                values,
                parsed,
                parsed_paths,
                global_row,
                strict,
            )
        result_chunks.append(result)
        global_row += len(chunk)

    if not chunked:
        return result_chunks[0]
    return pa.chunked_array(result_chunks, type=data_type)
