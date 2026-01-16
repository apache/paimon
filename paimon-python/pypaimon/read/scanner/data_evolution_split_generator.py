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
import random
from collections import defaultdict
from typing import List, Optional, Dict, Tuple

from pyroaring import BitMap

from pypaimon.globalindex.indexed_split import IndexedSplit
from pypaimon.globalindex.range import Range
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.read.sampled_split import SampledSplit
from pypaimon.read.scanner.split_generator import AbstractSplitGenerator
from pypaimon.read.split import Split
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
            row_ranges: Optional[List] = None,
            score_getter=None
    ):
        super().__init__(table, target_split_size, open_file_cost)
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

        partitioned_files = defaultdict(list)
        for entry in file_entries:
            partitioned_files[(tuple(entry.partition.values), entry.bucket)].append(entry)

        plan_start_pos = 0
        plan_end_pos = 0

        if self.start_pos_of_this_subtask is not None:
            # shard data range: [plan_start_pos, plan_end_pos)
            partitioned_files, plan_start_pos, plan_end_pos = \
                self._filter_by_slice(
                    partitioned_files,
                    self.start_pos_of_this_subtask,
                    self.end_pos_of_this_subtask
                )
        elif self.idx_of_this_subtask is not None:
            # shard data range: [plan_start_pos, plan_end_pos)
            partitioned_files, plan_start_pos, plan_end_pos = self._filter_by_shard(partitioned_files)
        elif self.sample_num_rows is not None:
            partitioned_files, file_positions = self._filter_by_sample(partitioned_files)

        def weight_func(file_list: List[DataFileMeta]) -> int:
            return max(sum(f.file_size for f in file_list), self.open_file_cost)

        splits = []
        for key, sorted_entries_list in partitioned_files.items():
            if not sorted_entries_list:
                continue
            sorted_entries_list = sorted(sorted_entries_list, key=sort_key)
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

            splits += self._build_split_from_pack(
                flatten_packed_files, sorted_entries_list, False
            )

        if self.start_pos_of_this_subtask is not None or self.idx_of_this_subtask is not None:
            splits = self._wrap_to_sliced_splits(splits, plan_start_pos, plan_end_pos)
        elif self.sample_num_rows is not None:
            splits = self._wrap_to_sampled_splits(splits, file_positions)

        # Wrap splits with IndexedSplit if row_ranges is provided
        if self.row_ranges:
            splits = self._wrap_to_indexed_splits(splits)

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

    def _wrap_to_sampled_splits(self, splits: List[Split], file_positions: Dict[str, BitMap]) -> List[Split]:
        # Set sample file positions for each split
        sampled_splits = []
        for split in splits:
            sampled_file_idx_map = {}
            for file in split.files:
                sampled_file_idx_map[file.file_name] = file_positions[file.file_name]
            sampled_splits.append(SampledSplit(split, sampled_file_idx_map))
        return sampled_splits

    def _filter_by_slice(
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

        return self._filter_by_slice(partitioned_files, start_pos, end_pos)

    def _filter_by_sample(self, partitioned_files) -> (defaultdict, Dict[str, List[int]]):
        """
        Randomly sample num_rows data from partitioned_files:
        1. First use random to generate num_rows indexes
        2. Iterate through partitioned_files, find the file entries where corresponding indexes are located,
           add them to filtered_partitioned_files, and for each entry, add indexes to the list
        """
        # Calculate total number of rows
        total_rows = 0
        for key, file_entries in partitioned_files.items():
            for entry in file_entries:
                if not self._is_blob_file(entry.file.file_name):
                    total_rows += entry.file.row_count
        # Generate random sample indexes
        sample_indexes = sorted(random.sample(range(total_rows), self.sample_num_rows))

        # Map each sample index to its corresponding file and local index
        filtered_partitioned_files = defaultdict(list)
        file_positions = {}  # {file_name: BitMap of local_indexes}
        self._compute_file_sample_idx_map(partitioned_files, filtered_partitioned_files, file_positions,
                                          sample_indexes, is_blob=False)
        self._compute_file_sample_idx_map(partitioned_files, filtered_partitioned_files, file_positions,
                                          sample_indexes, is_blob=True)

        return filtered_partitioned_files, file_positions

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

        # Find the first non-blob file to determine the row range for this split
        data_file = None
        for file in split.files:
            if not self._is_blob_file(file.file_name):
                data_file = file
                break

        if data_file is None:
            # No data file, skip this split
            shard_file_idx_map[self.NEXT_POS_KEY] = file_end_pos
            return shard_file_idx_map

        # Calculate the row range based on the data file position
        file_begin_pos = file_end_pos
        file_end_pos += data_file.row_count
        data_file_first_row_id = data_file.first_row_id if data_file.first_row_id is not None else 0

        # Determine the row range for the data file in this split using shared helper
        data_file_range = self._compute_file_range(
            plan_start_pos, plan_end_pos, file_begin_pos, data_file.row_count
        )

        # Apply ranges to each file in the split
        for file in split.files:
            if self._is_blob_file(file.file_name):
                # For blob files, calculate range based on their first_row_id
                if data_file_range is None:
                    # Data file is completely within shard, blob files should also be
                    continue
                elif data_file_range == (-1, -1):
                    # Data file is completely outside shard, blob files should be skipped
                    shard_file_idx_map[file.file_name] = (-1, -1)
                else:
                    # Calculate blob file's position relative to data file's first_row_id
                    blob_first_row_id = file.first_row_id if file.first_row_id is not None else 0
                    # Blob's position relative to data file start
                    blob_rel_start = blob_first_row_id - data_file_first_row_id
                    blob_rel_end = blob_rel_start + file.row_count

                    # Shard range relative to data file start
                    shard_start = data_file_range[0]
                    shard_end = data_file_range[1]

                    # Intersect blob's range with shard range
                    intersect_start = max(blob_rel_start, shard_start)
                    intersect_end = min(blob_rel_end, shard_end)

                    if intersect_start >= intersect_end:
                        # Blob file is completely outside shard range
                        shard_file_idx_map[file.file_name] = (-1, -1)
                    elif intersect_start == blob_rel_start and intersect_end == blob_rel_end:
                        # Blob file is completely within shard range, no slicing needed
                        pass
                    else:
                        # Convert to file-local indices
                        local_start = intersect_start - blob_rel_start
                        local_end = intersect_end - blob_rel_start
                        shard_file_idx_map[file.file_name] = (local_start, local_end)
            else:
                # Data file
                if data_file_range is not None:
                    shard_file_idx_map[file.file_name] = data_file_range

        shard_file_idx_map[self.NEXT_POS_KEY] = file_end_pos
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
