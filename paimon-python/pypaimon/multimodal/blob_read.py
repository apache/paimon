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
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Shared helpers for materialising multimodal BLOB descriptor columns."""


def fetch_blob_bodies(
        file_io, data, blob_cols, parallelism, map_blob_cols=()):
    """Fetch scalar and MAP BLOB payload bytes.

    ``data`` is a ``dict`` mapping each BLOB column name to row-aligned cells.
    A cell may be serialized ``BlobDescriptor`` bytes, inline payload bytes,
    ``None``, or a MAP represented by key-value pairs. Returned values preserve
    row and MAP entry order and are grouped per column.
    """
    from pypaimon.table.row.blob import BlobDescriptor, BlobViewStruct

    ranges = []
    inline = {}
    targets = []
    bodies = {col: [] for col in blob_cols}
    scalar_offsets = {}
    map_blob_cols = set(map_blob_cols)

    def queue_blob_fetch(value):
        index = len(ranges)
        if value is None:
            ranges.append(None)
        else:
            raw = bytes(value)
            if BlobViewStruct.is_blob_view_struct(raw):
                raise ValueError(
                    "read_blobs does not support unresolved blob-view columns; "
                    "read such a column on its own, or enable blob-view resolution.")
            if BlobDescriptor.is_blob_descriptor(raw):
                descriptor = BlobDescriptor.deserialize(raw)
                ranges.append(
                    (descriptor.uri, descriptor.offset, descriptor.length)
                )
            else:
                ranges.append(None)
                inline[index] = raw
        return index

    for col in blob_cols:
        if col not in map_blob_cols:
            start = len(ranges)
            for value in data[col]:
                queue_blob_fetch(value)
            scalar_offsets[col] = (start, len(ranges))
            continue

        for value in data[col]:
            if value is None:
                bodies[col].append(None)
                continue

            entries = _map_entries(value)
            row_index = len(bodies[col])
            row = []
            bodies[col].append(row)
            for key, item in entries:
                entry_index = len(row)
                row.append((key, None))
                range_index = queue_blob_fetch(item)
                targets.append((col, row_index, entry_index, range_index))

    fetched = (
        file_io.read_ranges_coalesced(ranges, parallelism)
        if ranges
        else []
    )
    for index, raw in inline.items():
        fetched[index] = raw

    for col, (start, end) in scalar_offsets.items():
        bodies[col] = fetched[start:end]
    for col, row_index, entry_index, index in targets:
        key = bodies[col][row_index][entry_index][0]
        bodies[col][row_index][entry_index] = (key, fetched[index])
    return bodies


def _map_entries(value):
    if isinstance(value, dict):
        return list(value.items())
    if isinstance(value, (list, tuple)) and all(
            isinstance(entry, (list, tuple)) and len(entry) == 2
            for entry in value):
        return value
    return None
