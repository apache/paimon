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

"""Arrow helpers for accessing VARIANT paths without decoding whole values."""

import functools
import re
import struct
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

import pyarrow as pa

from pypaimon.data._variant_binary import (
    _ARRAY,
    _OBJECT,
    _U8_MAX,
    _U32_SIZE,
    _array_header,
    _get_int_size,
    _object_header,
    _read_unsigned,
)
from pypaimon.data.generic_variant import (
    GenericVariant,
    _Type,
    _value_size,
    _variant_get_type,
)


_INDEX_PATTERN = re.compile(r"\[(\d+)]")
_KEY_PATTERN = re.compile(r"\.([^.\[]+)|\['([^']+)']|\[\"([^\"]+)\"]")

_Path = Tuple[Tuple[str, object], ...]
_ObjectLayout = Tuple[int, int, int, int, int, int]
_ArrayLayout = Tuple[int, int, int, int]
_DOUBLE_HEADER = 7 << 2


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
            pos = match.end()
            continue

        match = _KEY_PATTERN.match(path, pos)
        if match is not None:
            key = next(value for value in match.groups() if value is not None)
            segments.append(('key', key))
            pos = match.end()
            continue
        raise ValueError(f"Invalid VARIANT path: {path}")
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
            metadata, offset_start + (key_id + 1) * offset_size, offset_size)
        key = metadata[string_start + start:string_start + end].decode('utf-8')
        result[key] = key_id
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
        value: bytes,
        pos: int,
        key_id: int,
) -> Optional[int]:
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
        value: bytes,
        pos: int,
        index: int,
) -> Optional[int]:
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
                parent = node_by_prefix[prefix[:-1]]
                kind, segment = prefix[-1]
                node_by_prefix[prefix] = len(nodes)
                nodes.append((parent, kind, segment))
        results.append(node_by_prefix[path])
    return tuple(nodes), tuple(results)


class _PositionPlan:

    def __init__(self, metadata, checks, positions):
        self.metadata = metadata
        self.checks = checks
        self.positions = positions

    def matches(self, value: bytes, metadata: bytes) -> bool:
        if metadata != self.metadata:
            return False
        for pos, expected in self.checks:
            if value[pos:pos + len(expected)] != expected:
                return False
        return True


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


def _cached_path_positions(
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


def _append_unsigned(buf: bytearray, value: int, size: int) -> None:
    buf.extend(value.to_bytes(size, 'little'))


def _build_object(ids: Sequence[int], children: Sequence[bytes]) -> bytes:
    size = len(ids)
    data_size = sum(len(child) for child in children)
    size_bytes = _U32_SIZE if size > _U8_MAX else 1
    id_size = _get_int_size(max(ids)) if ids else 1
    offset_size = _get_int_size(data_size) if data_size else 1

    buf = bytearray([_object_header(size > _U8_MAX, id_size, offset_size)])
    _append_unsigned(buf, size, size_bytes)
    for key_id in ids:
        _append_unsigned(buf, key_id, id_size)
    offset = 0
    for child in children:
        _append_unsigned(buf, offset, offset_size)
        offset += len(child)
    _append_unsigned(buf, offset, offset_size)
    for child in children:
        buf.extend(child)
    return bytes(buf)


def _build_array(children: Sequence[bytes]) -> bytes:
    size = len(children)
    data_size = sum(len(child) for child in children)
    size_bytes = _U32_SIZE if size > _U8_MAX else 1
    offset_size = _get_int_size(data_size) if data_size else 1

    buf = bytearray([_array_header(size > _U8_MAX, offset_size)])
    _append_unsigned(buf, size, size_bytes)
    offset = 0
    for child in children:
        _append_unsigned(buf, offset, offset_size)
        offset += len(child)
    _append_unsigned(buf, offset, offset_size)
    for child in children:
        buf.extend(child)
    return bytes(buf)


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
        return _build_object(ids, children)

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
    return _build_array(children)


def _variant_chunks(column):
    if isinstance(column, pa.ChunkedArray):
        chunks = column.chunks
        chunked = True
        data_type = column.type
    elif isinstance(column, pa.Array):
        chunks = [column]
        chunked = False
        data_type = column.type
    else:
        raise TypeError(
            "VARIANT input must be a PyArrow Array or ChunkedArray")
    if not pa.types.is_struct(data_type):
        raise TypeError(
            "VARIANT input must use the canonical Arrow struct type")
    names = [field.name for field in data_type]
    if names != ['value', 'metadata']:
        raise TypeError("VARIANT input must contain value and metadata fields")
    return chunks, chunked, data_type


def _build_variant_array(values, metadatas, nulls, data_type):
    return pa.StructArray.from_arrays(
        [pa.array(values, type=data_type[0].type),
         pa.array(metadatas, type=data_type[1].type)],
        fields=list(data_type),
        mask=pa.array(nulls, type=pa.bool_()),
    )


def variant_get_many(column, paths: Mapping[str, object]):
    """Extract several VARIANT paths in one pass.

    ``paths`` maps each path to its requested PyArrow type. Use ``None`` to
    keep a result in the canonical VARIANT Arrow representation.
    """
    parsed_paths = [
        (path, _parse_path(path), target_type)
        for path, target_type in paths.items()
    ]
    chunks, chunked, data_type = _variant_chunks(column)
    result_chunks = {path: [] for path in paths}
    position_plans = []
    for chunk in chunks:
        input_values = chunk.field(0).to_pylist()
        input_metadatas = chunk.field(1).to_pylist()
        valid = chunk.is_valid().to_pylist()
        rows = {
            path: ([], [], []) if target_type is None else []
            for path, _, target_type in parsed_paths
        }
        for row in range(len(chunk)):
            if not valid[row]:
                for path, _, target_type in parsed_paths:
                    if target_type is None:
                        values, metadatas, nulls = rows[path]
                        values.append(b'')
                        metadatas.append(b'')
                        nulls.append(True)
                    else:
                        rows[path].append(None)
                continue
            value = input_values[row]
            metadata = input_metadatas[row]
            positions = _cached_path_positions(
                value, metadata,
                [parsed for _, parsed, _ in parsed_paths],
                position_plans)
            for (path, _, target_type), pos in zip(parsed_paths, positions):
                if pos is None:
                    if target_type is None:
                        values, metadatas, nulls = rows[path]
                        values.append(b'')
                        metadatas.append(b'')
                        nulls.append(True)
                    else:
                        rows[path].append(None)
                elif target_type is None:
                    values, metadatas, nulls = rows[path]
                    values.append(value[pos:pos + _value_size(value, pos)])
                    metadatas.append(metadata)
                    nulls.append(False)
                else:
                    rows[path].append(_decode_scalar(value, metadata, pos))

        for path, _, target_type in parsed_paths:
            if target_type is None:
                values, metadatas, nulls = rows[path]
                result = _build_variant_array(
                    values, metadatas, nulls, data_type)
            else:
                result = pa.array(rows[path], type=target_type)
            result_chunks[path].append(result)

    results = {}
    for path, _, target_type in parsed_paths:
        if not chunked:
            results[path] = result_chunks[path][0]
        else:
            result_type = data_type if target_type is None else target_type
            results[path] = pa.chunked_array(
                result_chunks[path], type=result_type)
    return results


def variant_get(column, path: str, target_type=None):
    """Extract one VARIANT path without decoding unrelated fields.

    If ``target_type`` is omitted, the result remains a VARIANT Arrow struct.
    Otherwise values are converted to the requested PyArrow type.
    Missing paths and SQL NULL inputs produce NULL outputs.
    """
    return variant_get_many(column, {path: target_type})[path]


class _Replacement:

    def __init__(self, value, length: int):
        if isinstance(value, (pa.Array, pa.ChunkedArray)):
            if len(value) != length:
                raise ValueError(
                    "VARIANT replacement length must match the input column")
            self._array = value
            self._value = None
        elif isinstance(value, pa.Scalar):
            self._array = None
            self._value = value.as_py()
        else:
            self._array = None
            self._value = value

    def values(self, offset: int, length: int):
        if self._array is None:
            return [self._value] * length
        return self._array.slice(offset, length).to_pylist()


def _encode_replacement(value) -> bytes:
    if isinstance(value, (dict, list, tuple)):
        raise TypeError(
            "variant_set only accepts scalar replacements; nested values "
            "may require new metadata keys")
    if isinstance(value, float):
        return bytes([_DOUBLE_HEADER]) + struct.pack('<d', value)
    return GenericVariant.from_python(value).value()


def _decode_scalar(value: bytes, metadata: bytes, pos: int):
    if _variant_get_type(value, pos) == _Type.DOUBLE:
        return struct.unpack_from('<d', value, pos + 1)[0]
    return GenericVariant(value, metadata, pos).to_python()


def _paths_overlap(first: _Path, second: _Path) -> bool:
    limit = min(len(first), len(second))
    return first[:limit] == second[:limit]


def _validate_distinct_paths(paths: Sequence[_Path]) -> None:
    for i, first in enumerate(paths):
        for second in paths[i + 1:]:
            if _paths_overlap(first, second):
                raise ValueError("VARIANT replacement paths must not overlap")


def _replace_encoded_values(
        value: bytes,
        metadata: bytes,
        paths: Sequence[_Path],
        path_names: Sequence[str],
        replacements: Sequence[bytes],
        position_plans: List[_PositionPlan],
        found_positions=None,
) -> bytes:
    if found_positions is None:
        found_positions = _cached_path_positions(
            value, metadata, paths, position_plans)
    positions = []
    fixed_size = True
    for path, replacement, pos in zip(
            path_names, replacements, found_positions):
        if pos is None:
            raise ValueError(f"VARIANT path does not exist: {path}")
        size = _value_size(value, pos)
        positions.append((pos, size, replacement))
        fixed_size = fixed_size and size == len(replacement)

    if fixed_size:
        result = bytearray(value)
        for pos, size, replacement in positions:
            result[pos:pos + size] = replacement
        return bytes(result)

    for path, replacement in zip(paths, replacements):
        value = _replace_path(value, metadata, 0, path, replacement)
    return value


def variant_set_many(column, replacements: Mapping[str, object]):
    """Replace existing scalar VARIANT paths without a full object round trip.

    ``replacements`` maps paths to PyArrow arrays or scalar values. Array
    replacements must have the same row count as ``column``. All paths are
    updated in one pass, so common fixed-width updates copy each VARIANT value
    only once. Paths must already exist; this function never changes metadata.
    """
    chunks, chunked, data_type = _variant_chunks(column)
    total_length = len(column)
    parsed_replacements = [
        (path, _parse_path(path), _Replacement(value, total_length))
        for path, value in replacements.items()
    ]
    paths = [parsed for _, parsed, _ in parsed_replacements]
    path_names = [path for path, _, _ in parsed_replacements]
    _validate_distinct_paths(paths)

    result_chunks = []
    position_plans = []
    global_row = 0
    for chunk in chunks:
        input_values = chunk.field(0).to_pylist()
        input_metadatas = chunk.field(1).to_pylist()
        valid = chunk.is_valid().to_pylist()
        replacement_values = [
            replacement.values(global_row, len(chunk))
            for _, _, replacement in parsed_replacements
        ]
        values = []
        metadatas = []
        nulls = []
        for local_row in range(len(chunk)):
            if not valid[local_row]:
                values.append(b'')
                metadatas.append(b'')
                nulls.append(True)
                global_row += 1
                continue
            value = input_values[local_row]
            metadata = input_metadatas[local_row]
            row_replacements = [
                _encode_replacement(replacement_values[i][local_row])
                for i in range(len(parsed_replacements))
            ]
            value = _replace_encoded_values(
                value, metadata, paths, path_names, row_replacements,
                position_plans)
            values.append(value)
            metadatas.append(metadata)
            nulls.append(False)
            global_row += 1
        result_chunks.append(
            _build_variant_array(values, metadatas, nulls, data_type))

    if not chunked:
        return result_chunks[0]
    return pa.chunked_array(result_chunks, type=data_type)


def variant_set(column, path: str, values):
    """Replace one existing scalar VARIANT path for every input row."""
    return variant_set_many(column, {path: values})


def variant_transform(column, transforms: Mapping[str, object]):
    """Transform existing scalar VARIANT paths in one pass.

    Each callable receives only the scalar at its path. Unrelated fields are
    neither decoded nor rebuilt. This is preferable to ``variant_get`` plus
    ``variant_set_many`` when the operation can be expressed as a Python
    scalar function and no intermediate Arrow arrays are needed.
    """
    chunks, chunked, data_type = _variant_chunks(column)
    parsed_transforms = [
        (path, _parse_path(path), transform)
        for path, transform in transforms.items()
    ]
    for path, _, transform in parsed_transforms:
        if not callable(transform):
            raise TypeError(f"VARIANT transform for {path} must be callable")
    paths = [parsed for _, parsed, _ in parsed_transforms]
    path_names = [path for path, _, _ in parsed_transforms]
    _validate_distinct_paths(paths)

    result_chunks = []
    position_plans = []
    for chunk in chunks:
        input_values = chunk.field(0).to_pylist()
        input_metadatas = chunk.field(1).to_pylist()
        valid = chunk.is_valid().to_pylist()
        values = []
        metadatas = []
        nulls = []
        for row in range(len(chunk)):
            if not valid[row]:
                values.append(b'')
                metadatas.append(b'')
                nulls.append(True)
                continue
            value = input_values[row]
            metadata = input_metadatas[row]
            positions = _cached_path_positions(
                value, metadata, paths, position_plans)
            replacements = []
            for (path, _, transform), pos in zip(
                    parsed_transforms, positions):
                if pos is None:
                    raise ValueError(f"VARIANT path does not exist: {path}")
                current = _decode_scalar(value, metadata, pos)
                replacements.append(_encode_replacement(transform(current)))
            value = _replace_encoded_values(
                value, metadata, paths, path_names, replacements,
                position_plans, positions)
            values.append(value)
            metadatas.append(metadata)
            nulls.append(False)
        result_chunks.append(
            _build_variant_array(values, metadatas, nulls, data_type))

    if not chunked:
        return result_chunks[0]
    return pa.chunked_array(result_chunks, type=data_type)
