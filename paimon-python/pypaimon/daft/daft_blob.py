################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

"""Utilities for deserializing Paimon BlobDescriptor bytes into FileReference arrays."""

from __future__ import annotations

import struct

import pyarrow as pa

from pypaimon.daft.daft_compat import file_range_position_field, file_range_size_field
from pypaimon.table.row.blob import BlobDescriptor


FILE_PHYSICAL_TYPE = pa.struct(
    [
        pa.field("url", pa.large_utf8()),
        pa.field("io_config", pa.large_binary()),
        pa.field(file_range_position_field(), pa.int64()),
        pa.field(file_range_size_field(), pa.int64()),
    ]
)


def _deserialize_one(data: bytes) -> tuple[str, int, int]:
    """Deserialize a single BlobDescriptor -> (url, offset, length)."""
    try:
        descriptor = BlobDescriptor.deserialize(data)
    except (struct.error, UnicodeDecodeError) as e:
        raise ValueError("Invalid BlobDescriptor data") from e
    return descriptor.uri, descriptor.offset, descriptor.length


def blob_column_to_file_array(column: pa.Array, io_config_bytes: bytes | None = None) -> pa.Array:
    """Convert a large_binary column of serialized BlobDescriptors to a File-compatible struct.

    ``io_config_bytes`` (serialized IOConfig) is embedded into each File so native File ops carry
    credentials; when None the io_config is left null and ops fall back to Daft's global IOConfig.
    """
    urls: list[str | None] = []
    offsets: list[int | None] = []
    lengths: list[int | None] = []

    for value in column:
        if value is None or not value.is_valid:
            urls.append(None)
            offsets.append(None)
            lengths.append(None)
        else:
            raw = value.as_py()
            uri, off, length = _deserialize_one(raw)
            urls.append(uri)
            offsets.append(off)
            lengths.append(length)

    n = len(urls)
    if io_config_bytes is None:
        io_configs: pa.Array = pa.nulls(n, type=pa.large_binary())
    else:
        # Only populate io_config for valid rows; keep null rows null.
        io_configs = pa.array(
            [io_config_bytes if u is not None else None for u in urls],
            type=pa.large_binary(),
        )
    return pa.StructArray.from_arrays(
        [
            pa.array(urls, type=pa.large_utf8()),
            io_configs,
            pa.array(offsets, type=pa.int64()),
            pa.array(lengths, type=pa.int64()),
        ],
        names=["url", "io_config", file_range_position_field(), file_range_size_field()],
    )


def blob_array_column_to_file_array(
    column: pa.Array,
    io_config_bytes: bytes | None = None,
) -> pa.Array:
    """Convert a list of serialized BlobDescriptors to a list of File structs."""
    if not (pa.types.is_list(column.type) or pa.types.is_large_list(column.type)):
        raise TypeError(f"Expected a list column, but got {column.type}.")

    rows = []
    for row in column:
        if row is None or not row.is_valid:
            rows.append(None)
            continue

        files = []
        for raw in row.as_py():
            if raw is None:
                files.append(None)
                continue
            uri, offset, length = _deserialize_one(raw)
            files.append({
                "url": uri,
                "io_config": io_config_bytes,
                file_range_position_field(): offset,
                file_range_size_field(): length,
            })
        rows.append(files)

    value_field = pa.field(
        column.type.value_field.name,
        FILE_PHYSICAL_TYPE,
        nullable=column.type.value_field.nullable,
        metadata=column.type.value_field.metadata,
    )
    target_type = (
        pa.large_list(value_field)
        if pa.types.is_large_list(column.type)
        else pa.list_(value_field)
    )
    return pa.array(rows, type=target_type)


def blob_map_column_to_file_array(
    column: pa.Array,
    io_config_bytes: bytes | None = None,
) -> pa.Array:
    """Convert a map of serialized BlobDescriptors to a map of File structs."""
    if not pa.types.is_map(column.type):
        raise TypeError(f"Expected a map column, but got {column.type}.")

    rows = []
    for row in column:
        if row is None or not row.is_valid:
            rows.append(None)
            continue

        files = []
        for key, raw in row.as_py():
            if raw is None:
                files.append((key, None))
                continue
            uri, offset, length = _deserialize_one(raw)
            files.append((
                key,
                {
                    "url": uri,
                    "io_config": io_config_bytes,
                    file_range_position_field(): offset,
                    file_range_size_field(): length,
                },
            ))
        rows.append(files)

    item_field = pa.field(
        column.type.item_field.name,
        FILE_PHYSICAL_TYPE,
        nullable=column.type.item_field.nullable,
        metadata=column.type.item_field.metadata,
    )
    target_type = pa.map_(
        column.type.key_field,
        item_field,
        keys_sorted=column.type.keys_sorted,
    )
    return pa.array(rows, type=target_type)
