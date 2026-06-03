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
from typing import Any, List, Optional, Tuple

from pypaimon.globalindex.indexed_split import IndexedSplit
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.read.scanner.split_generator import AbstractSplitGenerator
from pypaimon.read.sliced_split import SlicedSplit
from pypaimon.read.split import DataSplit, Split
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.utils.range import Range


def _null_safe_partition_key(partition_values) -> tuple:
    """Wrap each partition value with a None-aware tag so tuples that mix
    null and non-null partition values can be ordered without raising
    ``TypeError: '<' not supported between instances of 'NoneType' and 'str'``.
    Paimon supports null partition values; Python 3 refuses to compare
    None against str/int directly.
    """
    return tuple((v is None, v) for v in partition_values)


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

    Subclasses implement the three abstract hooks. Reader paths
    (``RawFileSplitRead`` for append, ``DataEvolutionSplitRead`` for DE)
    are unchanged because chunks ride on existing wrappers
    (``SlicedSplit`` / ``IndexedSplit``).
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


# ---------------------------------------------------------------------------
# Append (non-DE, non-DV) implementation
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


class AppendChunkShuffleSplitGenerator(ChunkShuffleSplitGeneratorBase):
    """Chunk-shuffled splits for plain append tables (non-PK, non-DV, non-DE)."""

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
        ``self.chunk_size`` rows. ``chunk_size`` is a hard upper bound:
        the last chunk may be smaller, but no chunk exceeds it.
        """
        chunks: List[List[_FileSegment]] = []
        current: List[_FileSegment] = []
        current_rows = 0

        for entry in entries:
            file = entry.file
            offset = 0
            remaining = file.row_count
            while remaining > 0:
                avail = self.chunk_size - current_rows
                if avail <= 0:
                    chunks.append(current)
                    current = []
                    current_rows = 0
                    avail = self.chunk_size

                take = min(remaining, avail)

                if take == file.row_count and offset == 0:
                    current.append(_FileSegment(file, None, None))
                else:
                    current.append(_FileSegment(file, offset, offset + take))

                current_rows += take
                offset += take
                remaining -= take

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

        data_split = DataSplit(
            files=files,
            partition=chunk.partition,
            bucket=chunk.bucket,
            raw_convertible=True,
            data_deletion_files=None,
        )

        if shard_file_idx_map:
            return SlicedSplit(data_split, shard_file_idx_map)
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

        for group_range, group_files in aligned_groups:
            offset = 0
            group_rows = group_range.count()
            while offset < group_rows:
                avail = self.chunk_size - current_rows
                if avail <= 0:
                    chunks.append(current)
                    current = []
                    current_rows = 0
                    avail = self.chunk_size

                take = min(group_rows - offset, avail)
                seg_range = Range(
                    group_range.from_ + offset,
                    group_range.from_ + offset + take - 1,
                )
                current.append(_AlignedGroupSegment(group_files, seg_range))
                current_rows += take
                offset += take

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

        data_split = DataSplit(
            files=all_files,
            partition=chunk.partition,
            bucket=chunk.bucket,
            raw_convertible=False,
            data_deletion_files=None,
        )
        return IndexedSplit(data_split, row_ranges, scores=None)

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
        list_ranges = []
        for f in files:
            file_range = f.row_id_range()
            if file_range is None:
                raise ValueError(
                    "chunk_shuffle for data evolution tables requires row tracking; "
                    f"file {f.file_name} is missing first_row_id"
                )
            list_ranges.append(file_range)
        if not list_ranges:
            return []
        sorted_ranges = Range.sort_and_merge_overlap(list_ranges, True, False)

        range_to_files: "dict[Range, List[DataFileMeta]]" = {}
        for f in files:
            file_range = f.row_id_range()
            for r in sorted_ranges:
                if r.overlaps(file_range):
                    range_to_files.setdefault(r, []).append(f)
                    break

        return sorted(range_to_files.items(), key=lambda kv: kv[0].from_)
