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


def fetch_blob_bodies(file_io, data, blob_cols, parallelism):
    """Fetch BLOB payload bytes for descriptor/inline/null cells.

    ``data`` is a ``dict`` mapping each BLOB column name to row-aligned cells.
    Each cell may be serialized ``BlobDescriptor`` bytes, inline payload bytes,
    or ``None``. Returned values preserve row order and are grouped per column.
    """
    from pypaimon.table.row.blob import BlobDescriptor, BlobViewStruct

    ranges = []
    inline = {}
    index = 0
    for col in blob_cols:
        for value in data[col]:
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
                    ranges.append((descriptor.uri, descriptor.offset, descriptor.length))
                else:
                    ranges.append(None)
                    inline[index] = raw
            index += 1

    fetched = file_io.read_ranges_coalesced(ranges, parallelism)
    for index, raw in inline.items():
        fetched[index] = raw

    bodies = {}
    offset = 0
    for col in blob_cols:
        count = len(data[col])
        bodies[col] = fetched[offset:offset + count]
        offset += count
    return bodies
