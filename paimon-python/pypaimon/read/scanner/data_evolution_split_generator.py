"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
from collections import defaultdict
from typing import List, Optional, Dict, Tuple

from pypaimon.globalindex.indexed_split import IndexedSplit
from pypaimon.globalindex.range import Range
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.read.scanner.split_generator import AbstractSplitGenerator
from pypaimon.read.split import DataSplit, Split
from pypaimon.read.sliced_split import SlicedSplit


class DataEvolutionSplitGenerator(AbstractSplitGenerator):
    """
    Split generator for data evolution tables.
    """

    def __init__(
        self,
        table,
        target_split_size: int,
        open_file_cost: int,
        deletion_files_map=None,
        row_ranges: Optional[List] = None,
        score_getter=None
    ):
        super().__init__(table, target_split_size, open_file_cost, deletion_files_map)
        self.row_ranges = row_ranges
        self.score_getter = score_getter

    def create_splits(self, file_entries: List[ManifestEntry]) -> List[Split]:
        """
        Create splits for data evolution tables.
        """
        def sort_key(manifest_entry: ManifestEntry) -> tuple:
            first_row_id = (
                manifest_entry.file.first_row_id
                if manifest_entry.file.first_row_id is not None
                else float('-inf')
            )
            is_blob = 1 if self._is_blob_file(manifest_entry.file.file_name) else 0
            max_seq = manifest_entry.file.max_sequence_number
            return first_row_id, is_blob, -max_seq

        sorted_entries = sorted(file_entries, key=sort_key)

        partitioned_files = defaultdict(list)
        for entry in sorted_entries:
            partitioned_files[(tuple(entry.partition.values), entry.bucket)].append(entry)

        plan_start_pos = 0
        plan_end_pos = 0

        if self.start_pos_of_this_subtask is not None:
            # shard data range: [plan_start_pos, plan_end_pos)
            partitioned_files, plan_start_pos, plan_end_pos = \
                self._filter_by_row_range(
                    partitioned_files,
                    self.start_pos_of_this_subtask,
                    self.end_pos_of_this_subtask
                )
        elif self.idx_of_this_subtask is not None:
            # shard data range: [plan_start_pos, plan_end_pos)
            partitioned_files, plan_start_pos, plan_end_pos = self._filter_by_shard(partitioned_files)

        def weight_func(file_list: List[DataFileMeta]) -> int:
            return max(sum(f.file_size for f in file_list), self.open_file_cost)

        splits = []
        for key, sorted_entries_list in partitioned_files.items():
            if not sorted_entries_list:
                continue

            data_files: List[DataFileMeta] = [e.file for e in sorted_entries_list]

            # Split files by firstRowId for data evolution
            split_by_row_id = self._split_by_row_id(data_files)

            # Pack the split groups for optimal split sizes
            packed_files = self._pack_for_ordered(
                split_by_row_id, weight_func, self.target_split_size
            )

            # Flatten the packed files and build splits
            flatten_packed_files: List[List[DataFileMeta]] = [
                [file for sub_pack in pack for file in sub_pack]
                for pack in packed_files
            ]

            splits += self._build_split_from_pack_for_data_evolution(
                flatten_packed_files, packed_files, sorted_entries_list
            )

        if self.start_pos_of_this_subtask is not None or self.idx_of_this_subtask is not None:
            splits = self._wrap_to_sliced_splits(splits, plan_start_pos, plan_end_pos)

        # Wrap splits with IndexedSplit if row_ranges is provided
        if self.row_ranges:
            splits = self._wrap_to_indexed_splits(splits)

        return splits

    def _build_split_from_pack_for_data_evolution(
        self,
        flatten_packed_files: List[List[DataFileMeta]],
        packed_files: List[List[List[DataFileMeta]]],
        file_entries: List[ManifestEntry]
    ) -> List[Split]:
        """
        Build splits from packed files for data evolution tables.
        raw_convertible is True only when each range (pack) contains exactly one file.
        """
        splits = []
        for i, file_group in enumerate(flatten_packed_files):
            # In Java: rawConvertible = f.stream().allMatch(file -> file.size() == 1)
            # This means raw_convertible is True only when each range contains exactly one file
            pack = packed_files[i] if i < len(packed_files) else []
            raw_convertible = all(len(sub_pack) == 1 for sub_pack in pack)

            file_paths = []
            total_file_size = 0
            total_record_count = 0

            for data_file in file_group:
                data_file.set_file_path(
                    self.table.table_path,
                    file_entries[0].partition,
                    file_entries[0].bucket
                )
                file_paths.append(data_file.file_path)
                total_file_size += data_file.file_size
                total_record_count += data_file.row_count

            if file_paths:
                # Get deletion files for this split
                data_deletion_files = None
                if self.deletion_files_map:
                    data_deletion_files = self._get_deletion_files_for_split(
                        file_group,
                        file_entries[0].partition,
                        file_entries[0].bucket
                    )

                split = DataSplit(
                    files=file_group,
                    partition=file_entries[0].partition,
                    bucket=file_entries[0].bucket,
                    file_paths=file_paths,
                    row_count=total_record_count,
                    file_size=total_file_size,
                    raw_convertible=raw_convertible,
                    data_deletion_files=data_deletion_files
                )
                splits.append(split)
        return splits

    def _wrap_to_sliced_splits(self, splits: List[Split], plan_start_pos: int, plan_end_pos: int) -> List[Split]:
        """
        Wrap splits with SlicedSplit to add file-level slicing information.
        """
        sliced_splits = []
        file_end_pos = 0  # end row position of current file in all splits data

        for split in splits:
            # Compute file index map for both data and blob files
            # Blob files share the same row position tracking as data files
            shard_file_idx_map = self._compute_split_file_idx_map(
                plan_start_pos, plan_end_pos, split, file_end_pos
            )
            file_end_pos = shard_file_idx_map[self.NEXT_POS_KEY]
            del shard_file_idx_map[self.NEXT_POS_KEY]
            
            if shard_file_idx_map:
                sliced_splits.append(SlicedSplit(split, shard_file_idx_map))
            else:
                sliced_splits.append(split)

        return sliced_splits

    def _filter_by_row_range(
        self,
        partitioned_files: defaultdict,
        start_pos: int,
        end_pos: int
    ) -> tuple:
        """
        Filter file entries by row range for data evolution tables.
        """
        plan_start_pos = 0
        plan_end_pos = 0
        entry_end_pos = 0  # end row position of current file in all data
        splits_start_pos = 0
        filtered_partitioned_files = defaultdict(list)

        # Iterate through all file entries to find files that overlap with current shard range
        for key, file_entries in partitioned_files.items():
            filtered_entries = []
            blob_added = False  # If it is true, all blobs corresponding to this data file are added
            for entry in file_entries:
                if self._is_blob_file(entry.file.file_name):
                    if blob_added:
                        filtered_entries.append(entry)
                    continue
                blob_added = False
                entry_begin_pos = entry_end_pos  # Starting row position of current file in all data
                entry_end_pos += entry.file.row_count  # Update to row position after current file

                # If current file is completely after shard range, stop iteration
                if entry_begin_pos >= end_pos:
                    break
                # If current file is completely before shard range, skip it
                if entry_end_pos <= start_pos:
                    continue
                if entry_begin_pos <= start_pos < entry_end_pos:
                    splits_start_pos = entry_begin_pos
                    plan_start_pos = start_pos - entry_begin_pos
                # If shard end position is within current file, record relative end position
                if entry_begin_pos < end_pos <= entry_end_pos:
                    plan_end_pos = end_pos - splits_start_pos
                # Add files that overlap with shard range to result
                filtered_entries.append(entry)
                blob_added = True
            if filtered_entries:
                filtered_partitioned_files[key] = filtered_entries

        return filtered_partitioned_files, plan_start_pos, plan_end_pos

    def _filter_by_shard(self, partitioned_files: defaultdict) -> tuple:
        """
        Filter file entries by shard for data evolution tables.
        """
        # Calculate total rows (excluding blob files)
        total_row = sum(
            entry.file.row_count
            for file_entries in partitioned_files.values()
            for entry in file_entries
            if not self._is_blob_file(entry.file.file_name)
        )

        # Calculate shard range using shared helper
        start_pos, end_pos = self._compute_shard_range(total_row)

        return self._filter_by_row_range(partitioned_files, start_pos, end_pos)

    def _split_by_row_id(self, files: List[DataFileMeta]) -> List[List[DataFileMeta]]:
        """
        Split files by row ID for data evolution tables.
        """
        split_by_row_id = []

        # Filter blob files to only include those within the row ID range of non-blob files
        sorted_files = self._filter_blob(files)

        # Split files by firstRowId
        last_row_id = -1
        check_row_id_start = 0
        current_split = []

        for file in sorted_files:
            first_row_id = file.first_row_id
            if first_row_id is None:
                # Files without firstRowId are treated as individual splits
                split_by_row_id.append([file])
                continue

            if not self._is_blob_file(file.file_name) and first_row_id != last_row_id:
                if current_split:
                    split_by_row_id.append(current_split)

                # Validate that files don't overlap
                if first_row_id < check_row_id_start:
                    file_names = [f.file_name for f in sorted_files]
                    raise ValueError(
                        f"There are overlapping files in the split: {file_names}, "
                        f"the wrong file is: {file.file_name}"
                    )

                current_split = []
                last_row_id = first_row_id
                check_row_id_start = first_row_id + file.row_count

            current_split.append(file)

        if current_split:
            split_by_row_id.append(current_split)

        return split_by_row_id

    def _compute_split_file_idx_map(
        self,
        plan_start_pos: int,
        plan_end_pos: int,
        split: Split,
        file_end_pos: int
    ) -> Dict[str, Tuple[int, int]]:
        """
        Compute file index map for a split, determining which rows to read from each file.
        For data files, the range is calculated based on the file's position in the cumulative row space.
        For blob files (which may be rolled), the range is calculated based on each file's first_row_id.
        """
        shard_file_idx_map = {}
        
        # First pass: data files only. Compute range and apply directly to avoid second-pass lookup.
        current_pos = file_end_pos
        data_file_infos = []
        for file in split.files:
            if self._is_blob_file(file.file_name):
                continue
            file_begin_pos = current_pos
            current_pos += file.row_count
            data_file_range = self._compute_file_range(
                plan_start_pos, plan_end_pos, file_begin_pos, file.row_count
            )
            data_file_infos.append((file, data_file_range))
            shard_file_idx_map[file.file_name] = data_file_range

        if not data_file_infos:
            # No data file, skip this split
            shard_file_idx_map[self.NEXT_POS_KEY] = file_end_pos
            return shard_file_idx_map

        next_pos = current_pos

        # Second pass: only blob files (data files already in shard_file_idx_map from first pass)
        for file in split.files:
            if not self._is_blob_file(file.file_name):
                continue
            blob_first_row_id = file.first_row_id if file.first_row_id is not None else 0
            data_file = None
            data_file_range = None
            data_file_first_row_id = None
            for df, fr in data_file_infos:
                df_first = df.first_row_id if df.first_row_id is not None else 0
                if df_first <= blob_first_row_id < df_first + df.row_count:
                    data_file = df
                    data_file_range = fr
                    data_file_first_row_id = df_first
                    break
            if data_file is None or data_file_range is None or data_file_first_row_id is None:
                continue
            if data_file_range == (-1, -1):
                shard_file_idx_map[file.file_name] = (-1, -1)
                continue
            blob_rel_start = blob_first_row_id - data_file_first_row_id
            blob_rel_end = blob_rel_start + file.row_count
            shard_start, shard_end = data_file_range
            intersect_start = max(blob_rel_start, shard_start)
            intersect_end = min(blob_rel_end, shard_end)
            if intersect_start >= intersect_end:
                shard_file_idx_map[file.file_name] = (-1, -1)
            elif intersect_start == blob_rel_start and intersect_end == blob_rel_end:
                pass
            else:
                local_start = intersect_start - blob_rel_start
                local_end = intersect_end - blob_rel_start
                shard_file_idx_map[file.file_name] = (local_start, local_end)

        shard_file_idx_map[self.NEXT_POS_KEY] = next_pos
        return shard_file_idx_map

    def _wrap_to_indexed_splits(self, splits: List[Split]) -> List[Split]:
        """
        Wrap splits with IndexedSplit for row range filtering.
        """
        indexed_splits = []
        for split in splits:
            # Calculate file ranges for this split
            file_ranges = []
            for file in split.files:
                first_row_id = file.first_row_id
                if first_row_id is not None:
                    file_ranges.append(Range(
                        first_row_id,
                        first_row_id + file.row_count - 1
                    ))

            if not file_ranges:
                # No row IDs, keep original split
                indexed_splits.append(split)
                continue

            # Merge file ranges
            file_ranges = Range.merge_sorted_as_possible(file_ranges)

            # Intersect with row_ranges from global index
            expected = Range.and_(file_ranges, self.row_ranges)

            if not expected:
                # No intersection, skip this split
                continue

            # Create scores array if score_getter is provided
            scores = None
            if self.score_getter is not None:
                scores = []
                for r in expected:
                    for row_id in range(r.from_, r.to + 1):
                        score = self.score_getter(row_id)
                        scores.append(score if score is not None else 0.0)

            indexed_splits.append(IndexedSplit(split, expected, scores))

        return indexed_splits

    @staticmethod
    def _filter_blob(files: List[DataFileMeta]) -> List[DataFileMeta]:
        """
        Filter blob files to only include those within row ID range of non-blob files.
        """
        result = []
        row_id_start = -1
        row_id_end = -1

        for file in files:
            if not DataEvolutionSplitGenerator._is_blob_file(file.file_name):
                if file.first_row_id is not None:
                    row_id_start = file.first_row_id
                    row_id_end = file.first_row_id + file.row_count
                result.append(file)
            else:
                if file.first_row_id is not None and row_id_start != -1:
                    if row_id_start <= file.first_row_id < row_id_end:
                        result.append(file)

        return result
