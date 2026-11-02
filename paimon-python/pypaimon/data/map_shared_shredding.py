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

"""Shared MAP metadata, public selection fields and Arrow offset helpers."""

import json
import struct
from copy import copy
from typing import Dict, List, Set, Tuple, cast
import pyarrow as pa


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
_SelectedKeyMetadata = Tuple[Dict[int, str], Dict[int, List[int]], Set[int], int]


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
