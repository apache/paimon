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
from typing import List

from pypaimon.common.options.core_options import MergeEngine
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.read.interval_partition import IntervalPartition
from pypaimon.read.scanner.split_generator import AbstractSplitGenerator
from pypaimon.read.split import Split


class PrimaryKeyTableSplitGenerator(AbstractSplitGenerator):
    """
    Split generator for primary key tables.
    """

    def __init__(
            self,
            table,
            target_split_size: int,
            open_file_cost: int,
            deletion_files_map=None
    ):
        super().__init__(table, target_split_size, open_file_cost, deletion_files_map)
        self.deletion_vectors_enabled = table.options.deletion_vectors_enabled()
        self.merge_engine = table.options.merge_engine()

    def with_slice(self, start_pos: int, end_pos: int):
        """Primary key tables do not support slice-based sharding."""
        raise NotImplementedError(
            "Primary key tables do not support with_slice(). "
            "Use with_shard() for bucket-based parallel processing instead."
        )

    def create_splits(self, file_entries: List[ManifestEntry]) -> List[Split]:
        """
        Create splits for primary key tables.
        """
        if self.idx_of_this_subtask is not None:
            file_entries = self._filter_by_shard(file_entries)

        partitioned_files = defaultdict(list)
        for entry in file_entries:
            partitioned_files[(tuple(entry.partition.values), entry.bucket)].append(entry)

        def single_weight_func(f: DataFileMeta) -> int:
            return max(f.file_size, self.open_file_cost)

        def weight_func(fl: List[DataFileMeta]) -> int:
            return max(sum(f.file_size for f in fl), self.open_file_cost)

        merge_engine_first_row = self.merge_engine == MergeEngine.FIRST_ROW

        splits = []
        for key, file_entries_list in partitioned_files.items():
            if not file_entries_list:
                continue

            data_files: List[DataFileMeta] = [e.file for e in file_entries_list]

            raw_convertible = all(
                f.level != 0 and self._without_delete_row(f)
                for f in data_files
            )

            levels = {f.level for f in data_files}
            one_level = len(levels) == 1

            use_optimized_path = raw_convertible and (
                self.deletion_vectors_enabled or merge_engine_first_row or one_level
            )

            if use_optimized_path:
                packed_files: List[List[DataFileMeta]] = self._pack_for_ordered(
                    data_files, single_weight_func, self.target_split_size
                )
                splits += self._build_split_from_pack(
                    packed_files, file_entries_list, True, use_optimized_path
                )
            else:
                partition_sort_runs = IntervalPartition(data_files).partition()
                sections: List[List[DataFileMeta]] = [
                    [file for s in sl for file in s.files]
                    for sl in partition_sort_runs
                ]

                packed_files = self._pack_for_ordered(
                    sections, weight_func, self.target_split_size
                )

                flatten_packed_files: List[List[DataFileMeta]] = [
                    [file for sub_pack in pack for file in sub_pack]
                    for pack in packed_files
                ]
                splits += self._build_split_from_pack(
                    flatten_packed_files, file_entries_list, True, False
                )

        return splits

    def _filter_by_shard(self, file_entries: List[ManifestEntry]) -> List[ManifestEntry]:
        """
        Filter file entries by bucket-based sharding.
        """
        filtered_entries = []
        for entry in file_entries:
            if entry.bucket % self.number_of_para_subtasks == self.idx_of_this_subtask:
                filtered_entries.append(entry)
        return filtered_entries
