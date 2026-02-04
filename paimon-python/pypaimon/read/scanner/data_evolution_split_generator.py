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

        slice_row_ranges = None  # Row ID ranges for slice-based filtering

        if self.start_pos_of_this_subtask is not None:
            # Calculate Row ID range for slice-based filtering
            slice_row_ranges = self._calculate_slice_row_ranges(partitioned_files)
            if slice_row_ranges:
                # Filter files by Row ID range
                partitioned_files = self._filter_files_by_row_ranges(partitioned_files, slice_row_ranges)
            else:
                partitioned_files = defaultdict(list)
        elif self.idx_of_this_subtask is not None:
            partitioned_files = self._filter_by_shard(
                partitioned_files, self.idx_of_this_subtask, self.number_of_para_subtasks
            )

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

        # merge slice_row_ranges and self.row_ranges
        if slice_row_ranges is None:
            slice_row_ranges = self.row_ranges
        elif self.row_ranges is not None:
            slice_row_ranges = Range.and_(slice_row_ranges, self.row_ranges)

        # Wrap splits with IndexedSplit for slice-based filtering or row_ranges
        if slice_row_ranges:
            splits = self._wrap_to_indexed_splits(splits, slice_row_ranges)

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

    def _calculate_slice_row_ranges(self, partitioned_files: defaultdict) -> List[Range]:
        """
        Calculate Row ID ranges for slice-based filtering based on start_pos and end_pos.
        """
        # Collect all Row ID ranges from files
        list_ranges = []
        for file_entries in partitioned_files.values():
            for entry in file_entries:
                first_row_id = entry.file.first_row_id
                # Range is inclusive [from_, to], so use row_count - 1
                list_ranges.append(Range(first_row_id, first_row_id + entry.file.row_count - 1))

        # Merge overlapping ranges
        sorted_ranges = Range.sort_and_merge_overlap(list_ranges, True, False)

        # Calculate the Row ID range for this slice
        start_range, end_range = self._divide_ranges_by_position(sorted_ranges)
        if start_range is None or end_range is None:
            return []

        # Return the range for this slice
        return [Range(start_range.from_, end_range.to)]

    def _divide_ranges_by_position(self, sorted_ranges: List[Range]) -> Tuple[Optional[Range], Optional[Range]]:
        """
        Divide ranges by position (start_pos, end_pos) to get the Row ID range for this slice.
        """
        if not sorted_ranges:
            return None, None

        total_row_count = sum(r.count() for r in sorted_ranges)
        start_pos = self.start_pos_of_this_subtask
        end_pos = self.end_pos_of_this_subtask

        if start_pos >= total_row_count:
            return None, None

        # Find the start Row ID
        current_pos = 0
        start_row_id = None
        end_row_id = None

        for r in sorted_ranges:
            range_end_pos = current_pos + r.count()

            # Find start Row ID
            if start_row_id is None and start_pos < range_end_pos:
                offset = start_pos - current_pos
                start_row_id = r.from_ + offset

            # Find end Row ID
            if end_pos <= range_end_pos:
                offset = end_pos - current_pos
                end_row_id = r.from_ + offset - 1  # -1 because end_pos is exclusive
                break

            current_pos = range_end_pos

        if start_row_id is None:
            return None, None
        if end_row_id is None:
            end_row_id = sorted_ranges[-1].to

        return Range(start_row_id, start_row_id), Range(end_row_id, end_row_id)

    @staticmethod
    def _filter_files_by_row_ranges(partitioned_files: defaultdict, row_ranges: List[Range]) -> defaultdict:
        """
        Filter files by Row ID ranges. Keep files that overlap with the given ranges.
        """
        filtered_partitioned_files = defaultdict(list)

        for key, file_entries in partitioned_files.items():
            filtered_entries = []

            for entry in file_entries:
                first_row_id = entry.file.first_row_id
                file_range = Range(first_row_id, first_row_id + entry.file.row_count - 1)

                # Check if file overlaps with any of the row ranges
                overlaps = False
                for r in row_ranges:
                    if r.overlaps(file_range):
                        overlaps = True
                        break

                if overlaps:
                    filtered_entries.append(entry)

            if filtered_entries:
                filtered_partitioned_files[key] = filtered_entries

        return filtered_partitioned_files

    def _filter_by_shard(self, partitioned_files: defaultdict, sub_task_id: int, total_tasks: int) -> defaultdict:
        list_ranges = []
        for file_entries in partitioned_files.values():
            for entry in file_entries:
                first_row_id = entry.file.first_row_id
                if first_row_id is None:
                    raise ValueError("Found None first row id in files")
                # Range is inclusive [from_, to], so use row_count - 1
                list_ranges.append(Range(first_row_id, first_row_id + entry.file.row_count - 1))

        sorted_ranges = Range.sort_and_merge_overlap(list_ranges, True, False)

        start_range, end_range = self._divide_ranges(sorted_ranges, sub_task_id, total_tasks)
        if start_range is None or end_range is None:
            return defaultdict(list)
        start_first_row_id = start_range.from_
        end_first_row_id = end_range.to

        filtered_partitioned_files = {
            k: [x for x in v if x.file.first_row_id >= start_first_row_id and x.file.first_row_id <= end_first_row_id]
            for k, v in partitioned_files.items()
        }

        filtered_partitioned_files = {k: v for k, v in filtered_partitioned_files.items() if v}
        return defaultdict(list, filtered_partitioned_files)

    @staticmethod
    def _divide_ranges(
        sorted_ranges: List[Range], sub_task_id: int, total_tasks: int
    ) -> Tuple[Optional[Range], Optional[Range]]:
        if not sorted_ranges:
            return None, None

        num_ranges = len(sorted_ranges)

        # If more tasks than ranges, some tasks get nothing
        if sub_task_id >= num_ranges:
            return None, None

        # Calculate balanced distribution of ranges across tasks
        base_ranges_per_task = num_ranges // total_tasks
        remainder = num_ranges % total_tasks

        # Each of the first 'remainder' tasks gets one extra range
        if sub_task_id < remainder:
            num_ranges_for_task = base_ranges_per_task + 1
            start_idx = sub_task_id * (base_ranges_per_task + 1)
        else:
            num_ranges_for_task = base_ranges_per_task
            start_idx = (
                remainder * (base_ranges_per_task + 1) +
                (sub_task_id - remainder) * base_ranges_per_task
            )
        end_idx = start_idx + num_ranges_for_task - 1
        return sorted_ranges[start_idx], sorted_ranges[end_idx]

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

    def _compute_slice_split_file_idx_map(
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
            if data_file_range is not None:
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
            data_file_range = None
            data_file_first_row_id = None
            for df, fr in data_file_infos:
                df_first = df.first_row_id if df.first_row_id is not None else 0
                if df_first <= blob_first_row_id < df_first + df.row_count:
                    data_file_range = fr
                    data_file_first_row_id = df_first
                    break
            if data_file_range is None:
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

    def _wrap_to_indexed_splits(self, splits: List[Split], row_ranges: List[Range]) -> List[Split]:
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
            expected = Range.and_(file_ranges, row_ranges)

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
