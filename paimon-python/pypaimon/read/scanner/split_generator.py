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
from abc import ABC, abstractmethod
from typing import Callable, List, Optional, Tuple

from pyroaring import BitMap

from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.read.split import Split
from pypaimon.read.split import DataSplit
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.source.deletion_file import DeletionFile


class AbstractSplitGenerator(ABC):
    """
    Abstract base class for generating splits.
    """

    # Special key for tracking file end position in split file index map
    NEXT_POS_KEY = '_next_pos'

    def __init__(
            self,
            table,
            target_split_size: int,
            open_file_cost: int,
    ):
        self.table = table
        self.target_split_size = target_split_size
        self.open_file_cost = open_file_cost
        self.deletion_files_map = {}

        # Shard configuration
        self.idx_of_this_subtask = None
        self.number_of_para_subtasks = None
        self.start_pos_of_this_subtask = None
        self.end_pos_of_this_subtask = None

        self.sample_num_rows = None

    def with_shard(self, idx_of_this_subtask: int, number_of_para_subtasks: int):
        """Configure sharding for parallel processing."""
        if idx_of_this_subtask >= number_of_para_subtasks:
            raise ValueError("idx_of_this_subtask must be less than number_of_para_subtasks")
        if self.start_pos_of_this_subtask is not None:
            raise ValueError("with_shard and with_slice cannot be used simultaneously")
        self.idx_of_this_subtask = idx_of_this_subtask
        self.number_of_para_subtasks = number_of_para_subtasks
        return self

    def with_slice(self, start_pos: int, end_pos: int):
        """Configure slice range for processing."""
        if start_pos >= end_pos:
            raise ValueError("start_pos must be less than end_pos")
        if self.idx_of_this_subtask is not None:
            raise ValueError("with_slice and with_shard cannot be used simultaneously")
        self.start_pos_of_this_subtask = start_pos
        self.end_pos_of_this_subtask = end_pos
        return self

    def with_sample(self, num_rows: int):
        if self.idx_of_this_subtask is not None:
            raise ValueError("with_sample and with_shard cannot be used simultaneously now")
        if self.start_pos_of_this_subtask is not None:
            raise ValueError("with_sample and with_slice cannot be used simultaneously now")
        self.sample_num_rows = num_rows
        return self

    @abstractmethod
    def create_splits(self, file_entries: List[ManifestEntry]) -> List[Split]:
        """
        Create splits from manifest entries.
        """
        pass

    def _build_split_from_pack(
            self,
            packed_files: List[List[DataFileMeta]],
            file_entries: List[ManifestEntry],
            for_primary_key_split: bool,
            use_optimized_path: bool = False
    ) -> List[Split]:
        """
        Build splits from packed files.
        """
        splits = []
        for file_group in packed_files:
            if use_optimized_path:
                raw_convertible = True
            elif for_primary_key_split:
                raw_convertible = len(file_group) == 1 and self._without_delete_row(file_group[0])
            else:
                raw_convertible = True

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

    def _get_deletion_files_for_split(
            self,
            data_files: List[DataFileMeta],
            partition: GenericRow,
            bucket: int
    ) -> Optional[List[DeletionFile]]:
        """Get deletion files for the given data files in a split."""
        if not self.deletion_files_map:
            return None

        partition_key = (tuple(partition.values), bucket)
        file_deletion_map = self.deletion_files_map.get(partition_key, {})

        if not file_deletion_map:
            return None

        deletion_files = []
        for data_file in data_files:
            deletion_file = file_deletion_map.get(data_file.file_name)
            if deletion_file:
                deletion_files.append(deletion_file)
            else:
                deletion_files.append(None)

        return deletion_files if any(df is not None for df in deletion_files) else None

    @staticmethod
    def _without_delete_row(data_file_meta: DataFileMeta) -> bool:
        """Check if a data file has no deleted rows."""
        if data_file_meta.delete_row_count is None:
            return True
        return data_file_meta.delete_row_count == 0

    @staticmethod
    def _pack_for_ordered(
            items: List,
            weight_func: Callable,
            target_weight: int
    ) -> List[List]:
        """Pack items into groups based on target weight."""
        packed = []
        bin_items = []
        bin_weight = 0

        for item in items:
            weight = weight_func(item)
            if bin_weight + weight > target_weight and len(bin_items) > 0:
                packed.append(list(bin_items))
                bin_items.clear()
                bin_weight = 0

            bin_weight += weight
            bin_items.append(item)

        if len(bin_items) > 0:
            packed.append(bin_items)

        return packed

    def _compute_shard_range(self, total_row: int) -> Tuple[int, int]:
        """
        Calculate start and end positions for this shard based on total rows.
        Uses balanced distribution to avoid last shard overload.
        """
        base_rows_per_shard = total_row // self.number_of_para_subtasks
        remainder = total_row % self.number_of_para_subtasks

        # Each of the first 'remainder' shards gets one extra row
        if self.idx_of_this_subtask < remainder:
            num_row = base_rows_per_shard + 1
            start_pos = self.idx_of_this_subtask * (base_rows_per_shard + 1)
        else:
            num_row = base_rows_per_shard
            start_pos = (
                    remainder * (base_rows_per_shard + 1) +
                    (self.idx_of_this_subtask - remainder) * base_rows_per_shard
            )

        end_pos = start_pos + num_row
        return start_pos, end_pos

    def _compute_file_sample_idx_map(self, partitioned_files, filtered_partitioned_files, file_positions,
                                     sample_indexes, is_blob):
        current_row = 0
        sample_idx = 0

        for key, file_entries in partitioned_files.items():
            filtered_entries = []
            for entry in file_entries:
                if not is_blob and self._is_blob_file(entry.file.file_name):
                    continue
                if is_blob and not self._is_blob_file(entry.file.file_name):
                    continue
                file_start_row = current_row
                file_end_row = current_row + entry.file.row_count

                # Find all sample indexes that fall within this file
                local_indexes = BitMap()
                while sample_idx < len(sample_indexes) and sample_indexes[sample_idx] < file_end_row:
                    if sample_indexes[sample_idx] >= file_start_row:
                        # Convert global index to local index within this file
                        local_index = sample_indexes[sample_idx] - file_start_row
                        local_indexes.add(local_index)
                    sample_idx += 1

                # If this file contains any sampled rows, include it
                if len(local_indexes) > 0:
                    filtered_entries.append(entry)
                    file_positions[entry.file.file_name] = local_indexes

                current_row = file_end_row

                # Early exit if we've processed all sample indexes
                if sample_idx >= len(sample_indexes):
                    break

            if filtered_entries:
                filtered_partitioned_files[key] = filtered_partitioned_files.get(key, []) + filtered_entries

                # Early exit if we've processed all sample indexes
            if sample_idx >= len(sample_indexes):
                break

    @staticmethod
    def _compute_file_range(
            plan_start_pos: int,
            plan_end_pos: int,
            file_begin_pos: int,
            file_row_count: int
    ) -> Optional[Tuple[int, int]]:
        """
        Compute the row range to read from a file given shard range and file position.
        Returns None if file is completely within shard range (no slicing needed).
        Returns (-1, -1) if file is completely outside shard range.
        """
        file_end_pos = file_begin_pos + file_row_count

        if file_begin_pos <= plan_start_pos < plan_end_pos <= file_end_pos:
            return plan_start_pos - file_begin_pos, plan_end_pos - file_begin_pos
        elif file_begin_pos < plan_start_pos < file_end_pos:
            return plan_start_pos - file_begin_pos, file_row_count
        elif file_begin_pos < plan_end_pos < file_end_pos:
            return 0, plan_end_pos - file_begin_pos
        elif file_end_pos <= plan_start_pos or file_begin_pos >= plan_end_pos:
            return -1, -1
        # File is completely within the shard range
        return None

    @staticmethod
    def _is_blob_file(file_name: str) -> bool:
        """Check if a file is a blob file."""
        return file_name.endswith('.blob')
