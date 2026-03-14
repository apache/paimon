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
from typing import List, Optional, Tuple

from pypaimon.globalindex.indexed_split import IndexedSplit
from pypaimon.utils.range import Range
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
            is_blob = 1 if DataFileMeta.is_blob_file(manifest_entry.file.file_name) else 0
            max_seq = manifest_entry.file.max_sequence_number
            return first_row_id, is_blob, -max_seq

        sorted_entries = sorted(file_entries, key=sort_key)

        partitioned_files = defaultdict(list)
        for entry in sorted_entries:
            partitioned_files[(tuple(entry.partition.values), entry.bucket)].append(entry)

        slice_row_ranges = None  # Row ID ranges for slice-based filtering

        if self.start_pos_of_this_subtask is not None or self.idx_of_this_subtask is not None:
            # Calculate Row ID range for slice-based filtering
            slice_row_ranges = self._calculate_slice_row_ranges(partitioned_files)
            # Filter files by Row ID range
            partitioned_files = self._filter_files_by_row_ranges(partitioned_files, slice_row_ranges)

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

            for data_file in file_group:
                data_file.set_file_path(
                    self.table.table_path,
                    file_entries[0].partition,
                    file_entries[0].bucket
                )

            if file_group:
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
                list_ranges.append(entry.file.row_id_range())

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
        If idx_of_this_subtask exists, divide total rows by number_of_para_subtasks.
        """
        if not sorted_ranges:
            return None, None

        total_row_count = sum(r.count() for r in sorted_ranges)
        
        # If idx_of_this_subtask exists, calculate start_pos and end_pos based on number_of_para_subtasks
        if self.idx_of_this_subtask is not None:
            # Calculate shard boundaries based on total row count
            rows_per_task = total_row_count // self.number_of_para_subtasks
            remainder = total_row_count % self.number_of_para_subtasks
            
            start_pos = self.idx_of_this_subtask * rows_per_task
            # Distribute remainder rows across first 'remainder' tasks
            if self.idx_of_this_subtask < remainder:
                start_pos += self.idx_of_this_subtask
                end_pos = start_pos + rows_per_task + 1
            else:
                start_pos += remainder
                end_pos = start_pos + rows_per_task
        else:
            # Use existing start_pos and end_pos
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
        Blob files are only included if they overlap with non-blob files that match the ranges.
        """
        filtered_partitioned_files = defaultdict(list)

        for key, file_entries in partitioned_files.items():
            # Separate blob and non-blob files
            non_blob_entries = []
            blob_entries = []
            
            for entry in file_entries:
                if DataFileMeta.is_blob_file(entry.file.file_name):
                    blob_entries.append(entry)
                else:
                    non_blob_entries.append(entry)
            
            # First, filter non-blob files based on row ranges
            filtered_non_blob_entries = []
            non_blob_ranges = []
            for entry in non_blob_entries:
                file_range = entry.file.row_id_range()

                # Check if file overlaps with any of the row ranges
                overlaps = False
                for r in row_ranges:
                    if r.overlaps(file_range):
                        overlaps = True
                        break
                
                if overlaps:
                    filtered_non_blob_entries.append(entry)
                    non_blob_ranges.append(file_range)
            
            # Then, filter blob files based on row ID range of non-blob files
            filtered_blob_entries = []
            non_blob_ranges = Range.sort_and_merge_overlap(non_blob_ranges, True, True)
            # Only keep blob files that overlap with merged non-blob ranges
            for entry in blob_entries:
                blob_range = entry.file.row_id_range()
                # Check if blob file overlaps with any merged range
                for merged_range in non_blob_ranges:
                    if merged_range.overlaps(blob_range):
                        filtered_blob_entries.append(entry)
                        break
            
            # Combine filtered non-blob and blob files
            filtered_entries = filtered_non_blob_entries + filtered_blob_entries
            
            if filtered_entries:
                filtered_partitioned_files[key] = filtered_entries

        return filtered_partitioned_files

    @staticmethod
    def _split_by_row_id(files: List[DataFileMeta]) -> List[List[DataFileMeta]]:
        """
        Split files by row ID for data evolution tables.
        Files are grouped by their overlapping row ID ranges.
        """
        list_ranges = [file.row_id_range() for file in files]

        if not list_ranges:
            return []

        sorted_ranges = Range.sort_and_merge_overlap(list_ranges, True, False)

        range_to_files = {}
        for file in files:
            file_range = file.row_id_range()
            for r in sorted_ranges:
                if r.overlaps(file_range):
                    range_to_files.setdefault(r, []).append(file)
                    break

        return list(range_to_files.values())

    def _wrap_to_indexed_splits(self, splits: List[Split], row_ranges: List[Range]) -> List[Split]:
        """
        Wrap splits with IndexedSplit for row range filtering.
        """
        indexed_splits = []
        for split in splits:
            # Calculate file ranges for this split
            file_ranges = [file.row_id_range() for file in split.files]

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
