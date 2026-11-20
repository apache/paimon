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

"""Global index metadata."""

from dataclasses import dataclass
from typing import List, Optional


_CENTROID_MAGIC = b"PAIMCENT"
_CENTROID_SCALAR_LAYOUT = _CENTROID_MAGIC + b"\x02"
_CENTROID_HEADER_SIZE = 22


@dataclass
class GlobalIndexMeta:
    """Schema for global index metadata."""

    row_range_start: int
    row_range_end: int
    index_field_id: int
    extra_field_ids: Optional[List[int]] = None
    index_meta: Optional[bytes] = None
    source_meta: Optional[bytes] = None

    def has_centroid_scalar_indexes(self) -> bool:
        return (self.index_meta is not None and len(self.index_meta) >= _CENTROID_HEADER_SIZE
                and self.index_meta.startswith(_CENTROID_SCALAR_LAYOUT))

    def is_centroid_scalar_router(self) -> bool:
        return self.has_centroid_scalar_indexes() and self.index_meta.startswith(_CENTROID_SCALAR_LAYOUT + b"\x01")

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, GlobalIndexMeta):
            return False
        return (
            self.row_range_start == other.row_range_start and
            self.row_range_end == other.row_range_end and
            self.index_field_id == other.index_field_id and
            self.extra_field_ids == other.extra_field_ids and
            self.index_meta == other.index_meta and
            self.source_meta == other.source_meta
        )

    def __hash__(self) -> int:
        extra_ids_tuple = tuple(self.extra_field_ids) if self.extra_field_ids else None
        return hash((
            self.row_range_start,
            self.row_range_end,
            self.index_field_id,
            extra_ids_tuple,
            self.index_meta,
            self.source_meta,
        ))


@dataclass
class GlobalIndexIOMeta:
    """I/O metadata for global index files."""

    file_name: str
    file_size: int
    metadata: Optional[bytes] = None
    # Optional full path, mirrors Java IndexFileMeta.externalPath(). When set,
    # readers should open this path directly instead of joining the caller's
    # index_path with file_name (Java FileStorePathFactory#toPath prefers
    # externalPath over indexPath/fileName).
    external_path: Optional[str] = None

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, GlobalIndexIOMeta):
            return False
        return (
            self.file_name == other.file_name and
            self.file_size == other.file_size and
            self.metadata == other.metadata and
            self.external_path == other.external_path
        )

    def __hash__(self) -> int:
        return hash((self.file_name, self.file_size, self.external_path))


def has_centroid_scalar_index(table, index_type, index_column, partition_filter) -> bool:
    from pypaimon.index.index_file_handler import IndexFileHandler

    field_id = table.field_dict[index_column].id
    return any(
        entry.kind == 0
        and entry.index_file.index_type == index_type
        and entry.index_file.global_index_meta is not None
        and entry.index_file.global_index_meta.index_field_id == field_id
        and entry.index_file.global_index_meta.has_centroid_scalar_indexes()
        and (partition_filter is None or partition_filter.test(entry.partition))
        for entry in IndexFileHandler(table).scan(None)
    )


def range_vector_index_file(io_metas: List[GlobalIndexIOMeta]) -> GlobalIndexIOMeta:
    # Paimon Core prefixes routed manifest metadata with this layout marker.
    if any(meta.metadata and meta.metadata.startswith(_CENTROID_MAGIC) for meta in io_metas):
        raise NotImplementedError("Centroid-sharded vector indexes require the Java reader.")
    assert len(io_metas) == 1, "Expected exactly one index file per shard"
    return io_metas[0]
