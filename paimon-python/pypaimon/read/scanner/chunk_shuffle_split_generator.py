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

import random
from abc import abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Iterator, List, Optional, Tuple

from pypaimon.deletionvectors.deletion_vector import DeletionVector
from pypaimon.globalindex.indexed_split import IndexedSplit
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.read.scanner.split_generator import AbstractSplitGenerator
from pypaimon.read.sliced_split import SlicedSplit
from pypaimon.read.split import DataSplit, Split
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.source.deletion_file import DeletionFile
from pypaimon.utils.data_evolution_utils import retrieve_anchor_file
from pypaimon.utils.range import Range
from pypaimon.utils.range_helper import RangeHelper


def _null_safe_partition_key(partition_values) -> tuple:
    """Wrap each partition value with a None-aware tag so tuples that mix
    null and non-null partition values can be ordered without raising
    ``TypeError: '<' not supported between instances of 'NoneType' and 'str'``.
    Paimon supports null partition values; Python 3 refuses to compare
    None against str/int directly.
    """
    return tuple((v is None, v) for v in partition_values)


@dataclass
class _PhysicalRowSlice:
    """A half-open physical row slice containing visible rows."""

    start_inclusive: int
    end_exclusive: int
    live_row_count: int

    def to_closed_row_id_range(self, first_row_id: int) -> Range:
        return Range(
            first_row_id + self.start_inclusive,
            first_row_id + self.end_exclusive - 1,
        )


class _LiveRowRangeSlicer:
    """Map requested live-row counts to contiguous physical row ranges.

    Deleted positions must be sorted and unique. Each position is consumed
    once, so slicing costs O(number of output ranges + DV cardinality)
    instead of O(physical row count).
    """

    def __init__(
        self,
        physical_row_count: int,
        deleted_positions: Iterator[int],
    ):
        if physical_row_count < 0:
            raise ValueError(
                f"physical_row_count must be non-negative, got {physical_row_count}"
            )
        self._physical_row_count = physical_row_count
        self._deleted_positions = iter(deleted_positions)
        self._physical_position = 0
        self._last_deleted_position = None
        self._next_deleted_position = self._advance_deleted_position()

    def take(self, expected_live_rows: int) -> Optional[_PhysicalRowSlice]:
        if expected_live_rows <= 0:
            raise ValueError(
                f"expected_live_rows must be positive, got {expected_live_rows}"
            )
        if self._physical_position >= self._physical_row_count:
            return None

        start = self._physical_position
        live_rows = 0

        while self._physical_position < self._physical_row_count:
            if self._next_deleted_position is None:
                take = min(
                    expected_live_rows - live_rows,
                    self._physical_row_count - self._physical_position,
                )
                self._physical_position += take
                live_rows += take
            else:
                live_run = self._next_deleted_position - self._physical_position
                needed = expected_live_rows - live_rows
                if needed <= live_run:
                    self._physical_position += needed
                    live_rows += needed
                else:
                    self._physical_position += live_run
                    live_rows += live_run

            if live_rows == expected_live_rows:
                # Deleted rows have zero live-row weight. Attach a deletion run
                # immediately after the boundary to this range so the next
                # range starts at a live row (or EOF).
                self._skip_deleted_positions_at_cursor()
                return _PhysicalRowSlice(
                    start,
                    self._physical_position,
                    live_rows,
                )

            # The current live run was insufficient, so the cursor must be at
            # the next deleted position. Consume it and continue.
            self._skip_deleted_positions_at_cursor()

        if live_rows == 0:
            return None
        return _PhysicalRowSlice(start, self._physical_position, live_rows)

    def _skip_deleted_positions_at_cursor(self) -> None:
        while (
            self._next_deleted_position is not None
            and self._next_deleted_position == self._physical_position
        ):
            self._physical_position += 1
            self._next_deleted_position = self._advance_deleted_position()

    def _advance_deleted_position(self) -> Optional[int]:
        position = next(self._deleted_positions, None)
        if position is None:
            return None
        if position < 0 or position >= self._physical_row_count:
            raise ValueError(
                f"Deletion vector position {position} is outside physical row "
                f"range [0, {self._physical_row_count})."
            )
        if (
            self._last_deleted_position is not None
            and position <= self._last_deleted_position
        ):
            raise ValueError(
                "Deletion vector positions must be strictly increasing, but found "
                f"{position} after {self._last_deleted_position}."
            )
        self._last_deleted_position = position
        return position


@dataclass
class _Chunk:
    """A unit of work for one DataLoader read. ``segments`` carries
    subclass-specific payload (file segments for append, aligned-group
    segments for data evolution).
    """
    partition: GenericRow
    bucket: int
    segments: List[Any]


class ChunkShuffleSplitGeneratorBase(AbstractSplitGenerator):
    """Common scaffolding for deterministic chunk-shuffled split generation.

    Pipeline (template method, in :meth:`create_splits`):
      1. Stable-sort entries (key from :meth:`_sort_key`) so manifest-read
         parallelism cannot bleed into the output.
      2. Group by (partition, bucket); iterate groups in sorted-key order.
      3. Per group, call :meth:`_slice_group_into_chunks` to produce a list
         of segment lists — one segment list per chunk.
      4. Wrap each chunk with its (partition, bucket) into ``_Chunk``,
         concatenate across groups.
      5. ``random.Random(seed).shuffle`` all chunks.
      6. If sharded, take this worker's slice via balanced ``_compute_shard_range``.
      7. Map each chunk through :meth:`_chunk_to_split`.

    Subclasses implement the three abstract hooks. Chunks ride on existing
    reader wrappers (``SlicedSplit`` / ``IndexedSplit``).
    """

    def __init__(
        self,
        table,
        target_split_size: int,
        open_file_cost: int,
        deletion_files_map=None,
        seed: int = 0,
        chunk_size: int = 0,
    ):
        super().__init__(table, target_split_size, open_file_cost, deletion_files_map)
        self.seed = seed
        self.chunk_size = chunk_size

    def create_splits(self, file_entries: List[ManifestEntry]) -> List[Split]:
        """TODO: Lazily initialize DataSplits to avoid creating too many objects."""

        if not file_entries:
            return []

        sorted_entries = sorted(file_entries, key=self._sort_key)

        partitioned: "defaultdict[Tuple[tuple, int], List[ManifestEntry]]" = defaultdict(list)
        for entry in sorted_entries:
            partitioned[(tuple(entry.partition.values), entry.bucket)].append(entry)

        all_chunks: List[_Chunk] = []
        for key in sorted(
            partitioned.keys(),
            key=lambda k: (_null_safe_partition_key(k[0]), k[1]),
        ):
            entries_in_group = partitioned[key]
            partition_row = entries_in_group[0].partition
            bucket = entries_in_group[0].bucket
            # Materialize file_path once per unique file in this group.
            seen_paths: set = set()
            for entry in entries_in_group:
                f = entry.file
                if f.file_name in seen_paths:
                    continue
                seen_paths.add(f.file_name)
                f.set_file_path(
                    self.table.table_path,
                    partition_row,
                    bucket,
                    self.default_part_value,
                )
            for segments in self._slice_group_into_chunks(entries_in_group):
                all_chunks.append(_Chunk(partition_row, bucket, segments))

        rng = random.Random(self.seed)
        rng.shuffle(all_chunks)

        if self.idx_of_this_subtask is not None:
            start, end = self._compute_shard_range(len(all_chunks))
            all_chunks = all_chunks[start:end]

        return [self._chunk_to_split(c) for c in all_chunks]

    @abstractmethod
    def _sort_key(self, entry: ManifestEntry):
        """Return a comparable, deterministic key for stable sort."""

    @abstractmethod
    def _slice_group_into_chunks(self, entries: List[ManifestEntry]) -> List[List[Any]]:
        """Cut one (partition, bucket) group into chunks of segments.

        Each returned inner list represents one chunk; segment shape is
        subclass-defined.
        """

    @abstractmethod
    def _chunk_to_split(self, chunk: _Chunk) -> Split:
        """Wrap a chunk into a Split that the existing readers consume."""

    def _deletion_file(
        self,
        partition: GenericRow,
        bucket: int,
        file_name: str,
    ) -> Optional[DeletionFile]:
        partition_key = (tuple(partition.values), bucket)
        return self.deletion_files_map.get(partition_key, {}).get(file_name)

    def _live_row_slicer(
        self,
        physical_row_count: int,
        deletion_file: Optional[DeletionFile],
    ) -> Optional[_LiveRowRangeSlicer]:
        if deletion_file is None or deletion_file.cardinality == 0:
            return _LiveRowRangeSlicer(physical_row_count, iter(()))

        cardinality = deletion_file.cardinality
        if cardinality is not None:
            if cardinality < 0 or cardinality > physical_row_count:
                raise ValueError(
                    f"Deletion vector cardinality {cardinality} is outside valid "
                    f"range [0, {physical_row_count}]."
                )
            if cardinality == physical_row_count:
                return None

        deletion_vector = DeletionVector.read(self.table.file_io, deletion_file)

        actual_cardinality = deletion_vector.get_cardinality()
        if cardinality is not None and cardinality != actual_cardinality:
            raise ValueError(
                f"Deletion vector cardinality mismatch, metadata is {cardinality} "
                f"but bitmap contains {actual_cardinality} positions."
            )
        if actual_cardinality > physical_row_count:
            raise ValueError(
                f"Deletion vector cardinality {actual_cardinality} exceeds physical "
                f"row count {physical_row_count}."
            )
        if actual_cardinality == physical_row_count:
            return None
        return _LiveRowRangeSlicer(
            physical_row_count,
            iter(deletion_vector.bit_map()),
        )


# ---------------------------------------------------------------------------
# Append implementation
# ---------------------------------------------------------------------------


@dataclass
class _FileSegment:
    """A contiguous slice of a data file inside one chunk.

    start/end are half-open row offsets within the file when the chunk
    boundary falls inside the file; both are None when the chunk owns
    the full file (so SlicedSplit's shard_file_idx_map can skip it and
    treat the file as full — see sliced_split.py:73-78).
    """
    file: DataFileMeta
    start: Optional[int]
    end: Optional[int]
    live_row_count: int


class AppendChunkShuffleSplitGenerator(ChunkShuffleSplitGeneratorBase):
    """Chunk-shuffled splits for plain append tables (non-PK, non-DE)."""

    def _sort_key(self, entry: ManifestEntry):
        return (
            _null_safe_partition_key(entry.partition.values),
            entry.bucket,
            entry.file.file_name,
        )

    def _slice_group_into_chunks(
        self, entries: List[ManifestEntry]
    ) -> List[List[_FileSegment]]:
        """Cut a (partition, bucket) group into chunks of at most
        ``self.chunk_size`` live rows. ``chunk_size`` is a hard upper bound:
        the last chunk may be smaller, but no chunk exceeds it after DV
        filtering.
        """
        chunks: List[List[_FileSegment]] = []
        current: List[_FileSegment] = []
        current_rows = 0

        for entry in entries:
            file = entry.file
            deletion_file = self._deletion_file(
                entry.partition,
                entry.bucket,
                file.file_name,
            )
            slicer = self._live_row_slicer(file.row_count, deletion_file)
            if slicer is None:
                continue

            while True:
                avail = self.chunk_size - current_rows
                if avail <= 0:
                    chunks.append(current)
                    current = []
                    current_rows = 0
                    avail = self.chunk_size

                physical_slice = slicer.take(avail)
                if physical_slice is None:
                    break

                if (
                    physical_slice.start_inclusive == 0
                    and physical_slice.end_exclusive == file.row_count
                ):
                    current.append(
                        _FileSegment(
                            file,
                            None,
                            None,
                            physical_slice.live_row_count,
                        )
                    )
                else:
                    current.append(
                        _FileSegment(
                            file,
                            physical_slice.start_inclusive,
                            physical_slice.end_exclusive,
                            physical_slice.live_row_count,
                        )
                    )

                current_rows += physical_slice.live_row_count

        if current:
            chunks.append(current)

        return chunks

    def _chunk_to_split(self, chunk: _Chunk) -> Split:
        files: List[DataFileMeta] = []
        shard_file_idx_map = {}
        for seg in chunk.segments:
            files.append(seg.file)
            if seg.start is not None and seg.end is not None:
                shard_file_idx_map[seg.file.file_name] = (seg.start, seg.end)

        # set_file_path is already done once per unique file in
        # ChunkShuffleSplitGeneratorBase.create_splits.
        data_deletion_files = self._get_deletion_files_for_split(
            files,
            chunk.partition,
            chunk.bucket,
        )

        data_split = DataSplit(
            files=files,
            partition=chunk.partition,
            bucket=chunk.bucket,
            raw_convertible=True,
            data_deletion_files=data_deletion_files,
        )

        exact_merged_row_count = sum(
            seg.live_row_count for seg in chunk.segments
        )
        if (
            shard_file_idx_map
            or data_split.merged_row_count() != exact_merged_row_count
        ):
            return SlicedSplit(
                data_split,
                shard_file_idx_map,
                exact_merged_row_count=exact_merged_row_count,
            )
        return data_split


# ---------------------------------------------------------------------------
# Data Evolution implementation
# ---------------------------------------------------------------------------


@dataclass
class _AlignedGroupSegment:
    """A row_id sub-range over one row-id-aligned file group.

    ``files`` is the entire group (may include blob/vector siblings),
    so the reader sees every column file even when only a slice of the
    group's row_id range lands in this chunk. ``row_range`` is the
    inclusive global row_id range this segment owns.
    """
    files: List[DataFileMeta]
    row_range: Range
    live_row_count: int


class DataEvolutionChunkShuffleSplitGenerator(ChunkShuffleSplitGeneratorBase):
    """Chunk-shuffled splits for data-evolution append tables.

    The minimum cuttable unit is a row_id-aligned file group: cutting
    inside one group would orphan column files relative to the row_id
    range, so we keep groups intact and only slice along their row_id
    axis. Each chunk maps to an :class:`IndexedSplit` whose ``row_ranges``
    bound the readable slice for that chunk.
    """

    def _sort_key(self, entry: ManifestEntry):
        first_row_id = (
            entry.file.first_row_id
            if entry.file.first_row_id is not None
            else float('-inf')
        )
        is_special = 1 if (
            DataFileMeta.is_blob_file(entry.file.file_name)
            or DataFileMeta.is_vector_file(entry.file.file_name)
        ) else 0
        return (
            _null_safe_partition_key(entry.partition.values),
            entry.bucket,
            first_row_id,
            is_special,
            entry.file.file_name,
        )

    def _slice_group_into_chunks(
        self, entries: List[ManifestEntry]
    ) -> List[List[_AlignedGroupSegment]]:
        files = [e.file for e in entries]
        # (Range, [files]) pairs sorted by row_id — see helper docstring.
        aligned_groups = self._split_by_row_id_with_range(files)

        chunks: List[List[_AlignedGroupSegment]] = []
        current: List[_AlignedGroupSegment] = []
        current_rows = 0
        partition = entries[0].partition
        bucket = entries[0].bucket

        for group_range, group_files in aligned_groups:
            anchor = None
            deletion_file = None
            if self.deletion_files_map:
                anchor = retrieve_anchor_file(group_files)
                deletion_file = self._deletion_file(
                    partition,
                    bucket,
                    anchor.file_name,
                )

            physical_row_count = group_range.count()
            first_row_id = group_range.from_
            if deletion_file is not None:
                anchor_range = anchor.row_id_range()
                if (
                    anchor_range.from_ > group_range.from_
                    or anchor_range.to < group_range.to
                ):
                    raise ValueError(
                        f"Data evolution anchor range {anchor_range} does not contain "
                        f"aligned group range {group_range}."
                    )
                physical_row_count = anchor.row_count
                first_row_id = anchor_range.from_

            slicer = self._live_row_slicer(
                physical_row_count,
                deletion_file,
            )
            if slicer is None:
                continue

            while True:
                avail = self.chunk_size - current_rows
                if avail <= 0:
                    chunks.append(current)
                    current = []
                    current_rows = 0
                    avail = self.chunk_size

                physical_slice = slicer.take(avail)
                if physical_slice is None:
                    break
                seg_range = physical_slice.to_closed_row_id_range(first_row_id)
                current.append(
                    _AlignedGroupSegment(
                        group_files,
                        seg_range,
                        physical_slice.live_row_count,
                    )
                )
                current_rows += physical_slice.live_row_count

        if current:
            chunks.append(current)

        return chunks

    def _chunk_to_split(self, chunk: _Chunk) -> Split:
        segments = chunk.segments
        if len(segments) == 1:
            all_files = segments[0].files
            row_ranges = [segments[0].row_range]
        else:
            all_files = []
            row_ranges = []
            for seg in segments:
                all_files.extend(seg.files)
                row_ranges.append(seg.row_range)
            row_ranges.sort(key=lambda r: r.from_)

        data_deletion_files = self._get_deletion_files_for_split(
            all_files,
            chunk.partition,
            chunk.bucket,
        )
        data_split = DataSplit(
            files=all_files,
            partition=chunk.partition,
            bucket=chunk.bucket,
            raw_convertible=False,
            data_deletion_files=data_deletion_files,
        )
        return IndexedSplit(
            data_split,
            row_ranges,
            scores=None,
            exact_merged_row_count=sum(
                seg.live_row_count for seg in segments
            ),
        )

    @staticmethod
    def _split_by_row_id_with_range(
        files: List[DataFileMeta],
    ) -> List[Tuple[Range, List[DataFileMeta]]]:
        """Group files by overlapping row_id range, returning (range, files)
        pairs sorted by ``range.from_``.

        Mirrors :meth:`DataEvolutionSplitGenerator._split_by_row_id` but
        also returns the merged row_id range per group, which the chunk
        slicer needs to drive row-count accumulation.
        """
        for f in files:
            if f.row_id_range() is None:
                raise ValueError(
                    "chunk_shuffle for data evolution tables requires row tracking; "
                    f"file {f.file_name} is missing first_row_id"
                )
        groups = RangeHelper(lambda f: f.row_id_range()).merge_overlapping_ranges(files)
        result = []
        for group in groups:
            ranges = [f.row_id_range() for f in group]
            merged = Range(min(r.from_ for r in ranges), max(r.to for r in ranges))
            result.append((merged, group))
        return sorted(result, key=lambda kv: kv[0].from_)
