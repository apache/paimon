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
from typing import List, Dict, Tuple

from pyroaring import BitMap

from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.read.sampled_split import SampledSplit
from pypaimon.read.scanner.split_generator import AbstractSplitGenerator
from pypaimon.read.split import Split
from pypaimon.read.sliced_split import SlicedSplit


class AppendTableSplitGenerator(AbstractSplitGenerator):
    """
    Split generator for append-only tables.
    """

    def create_splits(self, file_entries: List[ManifestEntry]) -> List[Split]:
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
            partitioned_files, plan_start_pos, plan_end_pos = self._filter_by_shard(partitioned_files)
        elif self.sample_num_rows is not None:
            partitioned_files, file_positions = self._filter_by_sample(partitioned_files)

        def weight_func(f: DataFileMeta) -> int:
            return max(f.file_size, self.open_file_cost)

        splits = []
        for key, file_entries_list in partitioned_files.items():
            if not file_entries_list:
                continue

            data_files: List[DataFileMeta] = [e.file for e in file_entries_list]

            packed_files: List[List[DataFileMeta]] = self._pack_for_ordered(
                data_files, weight_func, self.target_split_size
            )
            splits += self._build_split_from_pack(
                packed_files, file_entries_list, False
            )

        if self.start_pos_of_this_subtask is not None or self.idx_of_this_subtask is not None:
            splits = self._wrap_to_sliced_splits(splits, plan_start_pos, plan_end_pos)
        elif self.sample_num_rows is not None:
            splits = self._wrap_to_sampled_splits(splits, file_positions)

        return splits

    def _wrap_to_sliced_splits(self, splits: List[Split], plan_start_pos: int, plan_end_pos: int) -> List[Split]:
        sliced_splits = []
        file_end_pos = 0  # end row position of current file in all splits data

        for split in splits:
            shard_file_idx_map = self._compute_split_shard_file_idx_map(
                plan_start_pos, plan_end_pos, split, file_end_pos
            )
            file_end_pos = shard_file_idx_map[self.NEXT_POS_KEY]
            del shard_file_idx_map[self.NEXT_POS_KEY]

            if shard_file_idx_map:
                sliced_splits.append(SlicedSplit(split, shard_file_idx_map))
            else:
                sliced_splits.append(split)

        return sliced_splits

    @staticmethod
    def _wrap_to_sampled_splits(splits: List[Split], file_positions: Dict[str, BitMap]) -> List[Split]:
        # Set sample file positions for each split
        sampled_splits = []
        for split in splits:
            sampled_file_idx_map = {}
            for file in split.files:
                sampled_file_idx_map[file.file_name] = file_positions[file.file_name]
            sampled_splits.append(SampledSplit(split, sampled_file_idx_map))
        return sampled_splits

    @staticmethod
    def _filter_by_slice(
            partitioned_files: defaultdict,
            start_pos: int,
            end_pos: int
    ) -> tuple:
        plan_start_pos = 0
        plan_end_pos = 0
        entry_end_pos = 0  # end row position of current file in all data
        splits_start_pos = 0
        filtered_partitioned_files = defaultdict(list)

        # Iterate through all file entries to find files that overlap with current shard range
        for key, file_entries in partitioned_files.items():
            filtered_entries = []
            for entry in file_entries:
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
            if filtered_entries:
                filtered_partitioned_files[key] = filtered_entries

        return filtered_partitioned_files, plan_start_pos, plan_end_pos

    def _filter_by_shard(self, partitioned_files: defaultdict) -> tuple:
        """
        Filter file entries by shard. Only keep the files within the range, which means
        that only the starting and ending files need to be further divided subsequently.
        """
        # Calculate total rows
        total_row = sum(
            entry.file.row_count
            for file_entries in partitioned_files.values()
            for entry in file_entries
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
                total_rows += entry.file.row_count

        # Generate random sample indexes
        sample_indexes = sorted(random.sample(range(total_rows), self.sample_num_rows))

        # Map each sample index to its corresponding file and local index
        filtered_partitioned_files = defaultdict(list)
        file_positions = {}  # {file_name: BitMap of local_indexes}
        self._compute_file_sample_idx_map(partitioned_files, filtered_partitioned_files,
                                          file_positions,
                                          sample_indexes, is_blob=False)
        return filtered_partitioned_files, file_positions

    @staticmethod
    def _compute_split_shard_file_idx_map(
            plan_start_pos: int,
            plan_end_pos: int,
            split: Split,
            file_end_pos: int
    ) -> Dict[str, Tuple[int, int]]:
        """
        Compute file index map for a split, determining which rows to read from each file.

        """
        shard_file_idx_map = {}

        for file in split.files:
            file_begin_pos = file_end_pos  # Starting row position of current file in all data
            file_end_pos += file.row_count  # Update to row position after current file

            # Use shared helper to compute file range
            file_range = AppendTableSplitGenerator._compute_file_range(
                plan_start_pos, plan_end_pos, file_begin_pos, file.row_count
            )

            if file_range is not None:
                shard_file_idx_map[file.file_name] = file_range

        shard_file_idx_map[AppendTableSplitGenerator.NEXT_POS_KEY] = file_end_pos
        return shard_file_idx_map
