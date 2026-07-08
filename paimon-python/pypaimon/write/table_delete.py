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

import uuid
from bisect import bisect_right
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Set, Tuple

from pypaimon.deletionvectors.bitmap_deletion_vector import BitmapDeletionVector
from pypaimon.deletionvectors.deletion_vector import DeletionVector
from pypaimon.index.deletion_vector_meta import DeletionVectorMeta
from pypaimon.index.index_file_meta import IndexFileMeta
from pypaimon.manifest.index_manifest_entry import IndexManifestEntry
from pypaimon.manifest.index_manifest_file import IndexManifestFile
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.read.scanner.data_evolution_split_generator import (
    DataEvolutionSplitGenerator,
)
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.source.deletion_file import DeletionFile
from pypaimon.utils.data_evolution_utils import retrieve_anchor_file
from pypaimon.utils.file_store_path_factory import FileStorePathFactory
from pypaimon.utils.range import Range
from pypaimon.write.commit_message import CommitMessage


_ADD = 0
_DELETE = 1
_DV_INDEX_VERSION = 1


@dataclass(frozen=True)
class _AnchorRange:
    row_range: Range
    partition: GenericRow
    bucket: int
    anchor_file: DataFileMeta


@dataclass(frozen=True)
class _AnchorRangesInfo:
    snapshot_id: int
    anchors: List[_AnchorRange]


class TableDeleteByRowId:
    """Write data-evolution deletes as deletion-vector index updates."""

    def __init__(
            self,
            table,
            _precomputed_anchor_ranges: Optional[_AnchorRangesInfo] = None,
    ):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.file_io = table.file_io
        self._precomputed_anchor_ranges = _precomputed_anchor_ranges

    def _snapshot_anchor_ranges(self) -> _AnchorRangesInfo:
        snapshot_id, anchors = self._scan_anchor_ranges()
        return _AnchorRangesInfo(snapshot_id=snapshot_id, anchors=anchors)

    def delete(self, row_ids: Sequence[int]) -> List[CommitMessage]:
        self._validate_delete()

        deduplicated = self._deduplicate_row_ids(row_ids)
        if not deduplicated:
            return []

        snapshot_id, anchors = self._load_anchor_ranges()
        targets = self._group_delete_targets(deduplicated, anchors)

        commit_messages = []
        for (partition, bucket), file_positions in targets.items():
            message = self._build_commit_message(
                partition,
                bucket,
                file_positions,
                snapshot_id,
            )
            if message is not None:
                commit_messages.append(message)
        return commit_messages

    def _validate_delete(self):
        if not self.table.options.data_evolution_enabled():
            raise ValueError(
                "delete requires 'data-evolution.enabled' = 'true'."
            )
        if not self.table.options.row_tracking_enabled():
            raise ValueError(
                "delete requires 'row-tracking.enabled' = 'true'."
            )
        if not self.table.options.deletion_vectors_enabled(False):
            raise ValueError(
                "delete requires 'deletion-vectors.enabled' = 'true'."
            )

    @staticmethod
    def _deduplicate_row_ids(row_ids: Sequence[int]) -> List[int]:
        result = []
        seen = set()
        for row_id in row_ids:
            if row_id is None:
                raise ValueError("_ROW_ID value must not be null.")
            row_id = int(row_id)
            if row_id not in seen:
                result.append(row_id)
                seen.add(row_id)
        return result

    def _load_anchor_ranges(self) -> Tuple[int, List[_AnchorRange]]:
        if self._precomputed_anchor_ranges is not None:
            return (
                self._precomputed_anchor_ranges.snapshot_id,
                self._precomputed_anchor_ranges.anchors,
            )
        return self._scan_anchor_ranges()

    def _scan_anchor_ranges(self) -> Tuple[int, List[_AnchorRange]]:
        plan = self.table.new_read_builder().new_scan().plan()
        snapshot_id = plan.snapshot_id if plan.snapshot_id is not None else -1
        anchors = []

        for split in plan.splits():
            files = [
                file
                for file in split.files
                if file.row_id_range() is not None
            ]
            for group in DataEvolutionSplitGenerator._split_by_row_id(files):
                anchor = retrieve_anchor_file(group)
                anchors.append(
                    _AnchorRange(
                        row_range=anchor.row_id_range(),
                        partition=split.partition,
                        bucket=split.bucket,
                        anchor_file=anchor,
                    )
                )

        anchors.sort(key=lambda item: item.row_range.from_)
        return snapshot_id, anchors

    def _group_delete_targets(
            self,
            row_ids: Sequence[int],
            anchors: List[_AnchorRange],
    ) -> Dict[Tuple[Tuple, int], Dict[str, Set[int]]]:
        starts = [anchor.row_range.from_ for anchor in anchors]
        targets: Dict[Tuple[Tuple, int], Dict[str, Set[int]]] = {}

        for row_id in row_ids:
            index = bisect_right(starts, row_id) - 1
            if index < 0 or not anchors[index].row_range.contains(row_id):
                raise ValueError(
                    f"Row ID {row_id} does not belong to any valid range "
                    f"{[f'[{a.row_range.from_}, {a.row_range.to}]' for a in anchors]}"
                )

            anchor = anchors[index]
            partition_tuple = tuple(anchor.partition.values)
            key = (partition_tuple, anchor.bucket)
            file_positions = targets.setdefault(key, {})
            positions = file_positions.setdefault(anchor.anchor_file.file_name, set())
            positions.add(row_id - anchor.row_range.from_)

        return targets

    def _build_commit_message(
            self,
            partition: Tuple,
            bucket: int,
            file_positions: Dict[str, Set[int]],
            snapshot_id: int,
    ) -> Optional[CommitMessage]:
        partition_row = GenericRow(list(partition), self.table.partition_keys_fields)
        old_entries, deletion_vectors = self._read_existing_deletion_vectors(
            partition_row,
            bucket,
            snapshot_id,
        )

        changed = False
        for file_name, positions in file_positions.items():
            deletion_vector = deletion_vectors.setdefault(
                file_name,
                BitmapDeletionVector(),
            )
            for position in positions:
                changed = deletion_vector.checked_delete(position) or changed

        if not changed:
            return None

        new_entry = self._write_deletion_vector_index(
            partition_row,
            bucket,
            deletion_vectors,
        )
        delete_entries = [
            IndexManifestEntry(
                kind=_DELETE,
                partition=entry.partition,
                bucket=entry.bucket,
                index_file=entry.index_file,
            )
            for entry in old_entries
        ]
        return CommitMessage(
            partition=partition,
            bucket=bucket,
            new_files=[],
            check_from_snapshot=snapshot_id,
            index_adds=[new_entry],
            index_deletes=delete_entries,
        )

    def _read_existing_deletion_vectors(
            self,
            partition: GenericRow,
            bucket: int,
            snapshot_id: int,
    ) -> Tuple[List[IndexManifestEntry], Dict[str, DeletionVector]]:
        snapshot_manager = self.table.snapshot_manager()
        snapshot = (
            snapshot_manager.get_snapshot_by_id(snapshot_id)
            if snapshot_id >= 0 else snapshot_manager.get_latest_snapshot()
        )
        if snapshot is None or not snapshot.index_manifest:
            return [], {}

        index_manifest_file = IndexManifestFile(self.table)
        entries = []
        deletion_vectors: Dict[str, DeletionVector] = {}
        for entry in index_manifest_file.read(snapshot.index_manifest):
            if entry.kind != _ADD:
                continue
            if entry.index_file.index_type != IndexManifestFile.DELETION_VECTORS_INDEX:
                continue
            if entry.bucket != bucket or entry.partition != partition:
                continue

            entries.append(entry)
            if not entry.index_file.dv_ranges:
                continue

            dv_path = self._index_file_path(entry.index_file)
            for data_file_name, meta in entry.index_file.dv_ranges.items():
                deletion_file = DeletionFile(
                    dv_index_path=dv_path,
                    offset=meta.offset,
                    length=meta.length,
                    cardinality=meta.cardinality,
                )
                deletion_vectors[data_file_name] = DeletionVector.read(
                    self.file_io,
                    deletion_file,
                )

        return entries, deletion_vectors

    def _write_deletion_vector_index(
            self,
            partition: GenericRow,
            bucket: int,
            deletion_vectors: Dict[str, DeletionVector],
    ) -> IndexManifestEntry:
        file_name = f"{FileStorePathFactory.INDEX_PREFIX}{uuid.uuid4()}-1"
        path = f"{self.table.path_factory().index_path()}/{file_name}"

        position = 1
        dv_ranges = {}
        chunks = [bytes([_DV_INDEX_VERSION])]
        for data_file_name in sorted(deletion_vectors):
            deletion_vector = deletion_vectors[data_file_name]
            serialized = deletion_vector.serialize()
            bitmap_length = int.from_bytes(serialized[:4], byteorder="big")
            dv_ranges[data_file_name] = DeletionVectorMeta(
                data_file_name=data_file_name,
                offset=position,
                length=bitmap_length,
                cardinality=deletion_vector.get_cardinality(),
            )
            chunks.append(serialized)
            position += len(serialized)

        data = b"".join(chunks)
        try:
            with self.file_io.new_output_stream(path) as output_stream:
                output_stream.write(data)
        except Exception:
            self.file_io.delete_quietly(path)
            raise

        index_file = IndexFileMeta(
            index_type=IndexManifestFile.DELETION_VECTORS_INDEX,
            file_name=file_name,
            file_size=len(data),
            row_count=len(dv_ranges),
            dv_ranges=dv_ranges,
        )
        return IndexManifestEntry(
            kind=_ADD,
            partition=partition,
            bucket=bucket,
            index_file=index_file,
        )

    def _index_file_path(self, index_file: IndexFileMeta) -> str:
        if index_file.external_path:
            return index_file.external_path
        return f"{self.table.path_factory().index_path()}/{index_file.file_name}"
