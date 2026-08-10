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

"""Transform existing DOUBLE paths in Arrow VARIANT columns."""

import functools
import re
import struct
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

import pyarrow as pa

from pypaimon.data._variant_binary import (
    _ARRAY,
    _OBJECT,
    _U32_SIZE,
    _read_unsigned,
)
from pypaimon.data.generic_variant import _Type, _variant_get_type


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
    id_table = value[id_start:id_start + size * id_size]
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
    return chunks, chunked, data_type


def _variant_array(values, metadata, nulls, data_type):
    return pa.StructArray.from_arrays(
        [pa.array(values, type=data_type[0].type), metadata],
        fields=list(data_type),
        mask=pa.array(nulls, type=pa.bool_()),
    )


def variant_transform(column, transforms: Mapping[str, object]):
    """Transform existing DOUBLE paths without decoding the whole VARIANT."""
    parsed = [
        (path, _parse_path(path), transform)
        for path, transform in transforms.items()
    ]
    if len({path for _, path, _ in parsed}) != len(parsed):
        raise ValueError("VARIANT transform paths must be unique")
    for path, _, transform in parsed:
        if not callable(transform):
            raise TypeError(f"VARIANT transform for {path} must be callable")
    if not parsed:
        return column

    chunks, chunked, data_type = _variant_chunks(column)
    paths = [path for _, path, _ in parsed]
    plans = []
    result_chunks = []
    for chunk in chunks:
        input_values = chunk.field(0).to_pylist()
        metadata_array = chunk.field(1)
        input_metadatas = metadata_array.to_pylist()
        valid = chunk.is_valid().to_pylist()
        values, nulls = [], []
        for row in range(len(chunk)):
            if not valid[row]:
                values.append(b'')
                nulls.append(True)
                continue

            value = input_values[row]
            metadata = input_metadatas[row]
            result = bytearray(value)
            positions = _path_positions(value, metadata, paths, plans)
            for (path, _, transform), pos in zip(parsed, positions):
                if pos is None:
                    raise ValueError(f"VARIANT path does not exist: {path}")
                if _variant_get_type(value, pos) != _Type.DOUBLE:
                    raise TypeError(f"VARIANT path is not DOUBLE: {path}")
                current = struct.unpack_from('<d', value, pos + 1)[0]
                updated = transform(current)
                if not isinstance(updated, float):
                    raise TypeError(
                        f"VARIANT transform for {path} must return DOUBLE")
                try:
                    struct.pack_into('<d', result, pos + 1, updated)
                except (TypeError, struct.error, OverflowError) as error:
                    raise TypeError(
                        f"VARIANT transform for {path} must return DOUBLE"
                    ) from error
            values.append(bytes(result))
            nulls.append(False)
        result_chunks.append(
            _variant_array(values, metadata_array, nulls, data_type))

    if not chunked:
        return result_chunks[0]
    return pa.chunked_array(result_chunks, type=data_type)
