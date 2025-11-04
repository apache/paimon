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
import os
from collections import defaultdict
from typing import Callable, List, Optional

from pypaimon.common.core_options import CoreOptions
from pypaimon.common.predicate import Predicate
from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.manifest.schema.manifest_file_meta import ManifestFileMeta
from pypaimon.read.interval_partition import IntervalPartition, SortedRun
from pypaimon.read.plan import Plan
from pypaimon.read.push_down_utils import (trim_and_transform_predicate)
from pypaimon.read.scanner.starting_scanner import StartingScanner
from pypaimon.read.split import Split
from pypaimon.snapshot.snapshot_manager import SnapshotManager
from pypaimon.table.bucket_mode import BucketMode
from pypaimon.manifest.simple_stats_evolutions import SimpleStatsEvolutions


class FullStartingScanner(StartingScanner):
    def __init__(self, table, predicate: Optional[Predicate], limit: Optional[int]):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.predicate = predicate
        self.limit = limit

        self.snapshot_manager = SnapshotManager(table)
        self.manifest_list_manager = ManifestListManager(table)
        self.manifest_file_manager = ManifestFileManager(table)

        self.primary_key_predicate = trim_and_transform_predicate(
            self.predicate, self.table.field_names, self.table.trimmed_primary_keys)

        self.partition_key_predicate = trim_and_transform_predicate(
            self.predicate, self.table.field_names, self.table.partition_keys)

        # Get split target size and open file cost from table options
        self.target_split_size = CoreOptions.get_split_target_size(self.table.options)
        self.open_file_cost = CoreOptions.get_split_open_file_cost(self.table.options)

        self.idx_of_this_subtask = None
        self.number_of_para_subtasks = None

        self.only_read_real_buckets = True if int(
            self.table.options.get('bucket', -1)) == BucketMode.POSTPONE_BUCKET.value else False
        self.data_evolution = self.table.options.get(CoreOptions.DATA_EVOLUTION_ENABLED, 'false').lower() == 'true'

        def schema_fields_func(schema_id: int):
            return self.table.schema_manager.get_schema(schema_id).fields

        self.simple_stats_evolutions = SimpleStatsEvolutions(
            schema_fields_func,
            self.table.table_schema.id
        )

    def scan(self) -> Plan:
        file_entries = self.plan_files()
        if not file_entries:
            return Plan([])
        if self.table.is_primary_key_table:
            splits = self._create_primary_key_splits(file_entries)
        elif self.data_evolution:
            splits = self._create_data_evolution_splits(file_entries)
        else:
            splits = self._create_append_only_splits(file_entries)

        splits = self._apply_push_down_limit(splits)
        return Plan(splits)

    def plan_files(self) -> List[ManifestEntry]:
        latest_snapshot = self.snapshot_manager.get_latest_snapshot()
        if not latest_snapshot:
            return []
        manifest_files = self.manifest_list_manager.read_all(latest_snapshot)
        return self.read_manifest_entries(manifest_files)

    def read_manifest_entries(self, manifest_files: List[ManifestFileMeta]) -> List[ManifestEntry]:
        max_workers = int(self.table.options.get(CoreOptions.SCAN_MANIFEST_PARALLELISM, os.cpu_count() or 8))
        if max_workers < 8:
            max_workers = 8
        manifest_files = [entry for entry in manifest_files if self._filter_manifest_file(entry)]
        return self.manifest_file_manager.read_entries_parallel(manifest_files,
                                                                self._filter_manifest_entry,
                                                                max_workers=max_workers)

    def with_shard(self, idx_of_this_subtask, number_of_para_subtasks) -> 'FullStartingScanner':
        if idx_of_this_subtask >= number_of_para_subtasks:
            raise Exception("idx_of_this_subtask must be less than number_of_para_subtasks")
        self.idx_of_this_subtask = idx_of_this_subtask
        self.number_of_para_subtasks = number_of_para_subtasks
        return self

    def _append_only_filter_by_shard(self, partitioned_files: defaultdict) -> (defaultdict, int, int):
        total_row = 0
        # Sort by file creation time to ensure consistent sharding
        for key, file_entries in partitioned_files.items():
            for entry in file_entries:
                total_row += entry.file.row_count

        # Calculate number of rows this shard should process
        # Last shard handles all remaining rows (handles non-divisible cases)
        if self.idx_of_this_subtask == self.number_of_para_subtasks - 1:
            num_row = total_row - total_row // self.number_of_para_subtasks * self.idx_of_this_subtask
        else:
            num_row = total_row // self.number_of_para_subtasks
        # Calculate start row and end row position for current shard in all data
        start_row = self.idx_of_this_subtask * (total_row // self.number_of_para_subtasks)
        end_row = start_row + num_row

        plan_start_row = 0
        plan_end_row = 0
        entry_end_row = 0  # end row position of current file in all data
        splits_start_row = 0
        filtered_partitioned_files = defaultdict(list)
        # Iterate through all file entries to find files that overlap with current shard range
        for key, file_entries in partitioned_files.items():
            filtered_entries = []
            for entry in file_entries:
                entry_begin_row = entry_end_row  # Starting row position of current file in all data
                entry_end_row += entry.file.row_count  # Update to row position after current file

                # If current file is completely after shard range, stop iteration
                if entry_begin_row >= end_row:
                    break
                # If current file is completely before shard range, skip it
                if entry_end_row <= start_row:
                    continue
                if entry_begin_row <= start_row < entry_end_row:
                    splits_start_row = entry_begin_row
                    plan_start_row = start_row - entry_begin_row
                # If shard end position is within current file, record relative end position
                if entry_begin_row < end_row <= entry_end_row:
                    plan_end_row = end_row - splits_start_row
                # Add files that overlap with shard range to result
                filtered_entries.append(entry)
            if filtered_entries:
                filtered_partitioned_files[key] = filtered_entries

        return filtered_partitioned_files, plan_start_row, plan_end_row

    def _data_evolution_filter_by_shard(self, partitioned_files: defaultdict) -> (defaultdict, int, int):
        total_row = 0
        first_row_id_set = set()
        # Sort by file creation time to ensure consistent sharding
        for key, file_entries in partitioned_files.items():
            for entry in file_entries:
                if entry.file.first_row_id is None:
                    total_row += entry.file.row_count
                elif entry.file.first_row_id not in first_row_id_set:
                    first_row_id_set.add(entry.file.first_row_id)
                    total_row += entry.file.row_count

        # Calculate number of rows this shard should process
        # Last shard handles all remaining rows (handles non-divisible cases)
        if self.idx_of_this_subtask == self.number_of_para_subtasks - 1:
            num_row = total_row - total_row // self.number_of_para_subtasks * self.idx_of_this_subtask
        else:
            num_row = total_row // self.number_of_para_subtasks
        # Calculate start row and end row position for current shard in all data
        start_row = self.idx_of_this_subtask * (total_row // self.number_of_para_subtasks)
        end_row = start_row + num_row

        plan_start_row = 0
        plan_end_row = 0
        entry_end_row = 0  # end row position of current file in all data
        splits_start_row = 0
        filtered_partitioned_files = defaultdict(list)
        # Iterate through all file entries to find files that overlap with current shard range
        for key, file_entries in partitioned_files.items():
            filtered_entries = []
            first_row_id_set = set()
            for entry in file_entries:
                if entry.file.first_row_id is not None:
                    if entry.file.first_row_id in first_row_id_set:
                        filtered_entries.append(entry)
                        continue
                    else:
                        first_row_id_set.add(entry.file.first_row_id)
                entry_begin_row = entry_end_row  # Starting row position of current file in all data
                entry_end_row += entry.file.row_count  # Update to row position after current file

                # If current file is completely after shard range, stop iteration
                if entry_begin_row >= end_row:
                    break
                # If current file is completely before shard range, skip it
                if entry_end_row <= start_row:
                    continue
                if entry_begin_row <= start_row < entry_end_row:
                    splits_start_row = entry_begin_row
                    plan_start_row = start_row - entry_begin_row
                # If shard end position is within current file, record relative end position
                if entry_begin_row < end_row <= entry_end_row:
                    plan_end_row = end_row - splits_start_row
                # Add files that overlap with shard range to result
                filtered_entries.append(entry)
            if filtered_entries:
                filtered_partitioned_files[key] = filtered_entries

        return filtered_partitioned_files, plan_start_row, plan_end_row

    def _compute_split_start_end_row(self, splits: List[Split], plan_start_row, plan_end_row):
        file_end_row = 0  # end row position of current file in all data
        for split in splits:
            files = split.files
            split_start_row = file_end_row
            # Iterate through all file entries to find files that overlap with current shard range
            for file in files:
                file_begin_row = file_end_row  # Starting row position of current file in all data
                file_end_row += file.row_count  # Update to row position after current file

                # If shard start position is within current file, record actual start position and relative offset
                if file_begin_row <= plan_start_row < file_end_row:
                    split.split_start_row = plan_start_row - file_begin_row

                # If shard end position is within current file, record relative end position
                if file_begin_row < plan_end_row <= file_end_row:
                    split.split_end_row = plan_end_row - split_start_row
            if split.split_start_row is None:
                split.split_start_row = 0
            if split.split_end_row is None:
                split.split_end_row = split.row_count

    def _primary_key_filter_by_shard(self, file_entries: List[ManifestEntry]) -> List[ManifestEntry]:
        filtered_entries = []
        for entry in file_entries:
            if entry.bucket % self.number_of_para_subtasks == self.idx_of_this_subtask:
                filtered_entries.append(entry)
        return filtered_entries

    def _apply_push_down_limit(self, splits: List[Split]) -> List[Split]:
        if self.limit is None:
            return splits
        scanned_row_count = 0
        limited_splits = []

        for split in splits:
            if split.raw_convertible:
                limited_splits.append(split)
                scanned_row_count += split.row_count
                if scanned_row_count >= self.limit:
                    return limited_splits

        return limited_splits

    def _filter_manifest_file(self, file: ManifestFileMeta) -> bool:
        if not self.partition_key_predicate:
            return True
        return self.partition_key_predicate.test_by_simple_stats(
            file.partition_stats,
            file.num_added_files + file.num_deleted_files)

    def _filter_manifest_entry(self, entry: ManifestEntry) -> bool:
        if self.only_read_real_buckets and entry.bucket < 0:
            return False
        if self.partition_key_predicate and not self.partition_key_predicate.test(entry.partition):
            return False

        # Get SimpleStatsEvolution for this schema
        evolution = self.simple_stats_evolutions.get_or_create(entry.file.schema_id)

        # Apply evolution to stats
        if self.table.is_primary_key_table:
            if not self.primary_key_predicate:
                return True
            return self.primary_key_predicate.test_by_simple_stats(
                entry.file.key_stats,
                entry.file.row_count
            )
        else:
            if not self.predicate:
                return True
            if entry.file.value_stats_cols is None and entry.file.write_cols is not None:
                stats_fields = entry.file.write_cols
            else:
                stats_fields = entry.file.value_stats_cols
            evolved_stats = evolution.evolution(
                entry.file.value_stats,
                entry.file.row_count,
                stats_fields
            )
            return self.predicate.test_by_simple_stats(
                evolved_stats,
                entry.file.row_count
            )

    def _create_append_only_splits(self, file_entries: List[ManifestEntry]) -> List['Split']:
        partitioned_files = defaultdict(list)
        for entry in file_entries:
            partitioned_files[(tuple(entry.partition.values), entry.bucket)].append(entry)

        if self.idx_of_this_subtask is not None:
            partitioned_files, plan_start_row, plan_end_row = self._append_only_filter_by_shard(partitioned_files)

        def weight_func(f: DataFileMeta) -> int:
            return max(f.file_size, self.open_file_cost)

        splits = []
        for key, file_entries in partitioned_files.items():
            if not file_entries:
                return []

            data_files: List[DataFileMeta] = [e.file for e in file_entries]

            packed_files: List[List[DataFileMeta]] = self._pack_for_ordered(data_files, weight_func,
                                                                            self.target_split_size)
            splits += self._build_split_from_pack(packed_files, file_entries, False)
        if self.idx_of_this_subtask is not None:
            self._compute_split_start_end_row(splits, plan_start_row, plan_end_row)
        return splits

    def _create_primary_key_splits(self, file_entries: List[ManifestEntry]) -> List['Split']:
        if self.idx_of_this_subtask is not None:
            file_entries = self._primary_key_filter_by_shard(file_entries)
        partitioned_files = defaultdict(list)
        for entry in file_entries:
            partitioned_files[(tuple(entry.partition.values), entry.bucket)].append(entry)

        def weight_func(fl: List[DataFileMeta]) -> int:
            return max(sum(f.file_size for f in fl), self.open_file_cost)

        splits = []
        for key, file_entries in partitioned_files.items():
            if not file_entries:
                return []

            data_files: List[DataFileMeta] = [e.file for e in file_entries]
            partition_sort_runs: List[List[SortedRun]] = IntervalPartition(data_files).partition()
            sections: List[List[DataFileMeta]] = [
                [file for s in sl for file in s.files]
                for sl in partition_sort_runs
            ]

            packed_files: List[List[List[DataFileMeta]]] = self._pack_for_ordered(sections, weight_func,
                                                                                  self.target_split_size)
            flatten_packed_files: List[List[DataFileMeta]] = [
                [file for sub_pack in pack for file in sub_pack]
                for pack in packed_files
            ]
            splits += self._build_split_from_pack(flatten_packed_files, file_entries, True)
        return splits

    def _build_split_from_pack(self, packed_files, file_entries, for_primary_key_split: bool) -> List['Split']:
        splits = []
        for file_group in packed_files:
            raw_convertible = True
            if for_primary_key_split:
                raw_convertible = len(file_group) == 1

            file_paths = []
            total_file_size = 0
            total_record_count = 0

            for data_file in file_group:
                data_file.set_file_path(self.table.table_path, file_entries[0].partition,
                                        file_entries[0].bucket)
                file_paths.append(data_file.file_path)
                total_file_size += data_file.file_size
                total_record_count += data_file.row_count

            if file_paths:
                split = Split(
                    files=file_group,
                    partition=file_entries[0].partition,
                    bucket=file_entries[0].bucket,
                    _file_paths=file_paths,
                    _row_count=total_record_count,
                    _file_size=total_file_size,
                    raw_convertible=raw_convertible
                )
                splits.append(split)
        return splits

    @staticmethod
    def _pack_for_ordered(items: List, weight_func: Callable, target_weight: int) -> List[List]:
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

    def _create_data_evolution_splits(self, file_entries: List[ManifestEntry]) -> List['Split']:
        partitioned_files = defaultdict(list)
        for entry in file_entries:
            partitioned_files[(tuple(entry.partition.values), entry.bucket)].append(entry)

        if self.idx_of_this_subtask is not None:
            partitioned_files, plan_start_row, plan_end_row = self._data_evolution_filter_by_shard(partitioned_files)

        def weight_func(file_list: List[DataFileMeta]) -> int:
            return max(sum(f.file_size for f in file_list), self.open_file_cost)

        splits = []
        for key, file_entries in partitioned_files.items():
            if not file_entries:
                continue

            data_files: List[DataFileMeta] = [e.file for e in file_entries]

            # Split files by firstRowId for data evolution
            split_by_row_id = self._split_by_row_id(data_files)

            # Pack the split groups for optimal split sizes
            packed_files: List[List[List[DataFileMeta]]] = self._pack_for_ordered(split_by_row_id, weight_func,
                                                                                  self.target_split_size)

            # Flatten the packed files and build splits
            flatten_packed_files: List[List[DataFileMeta]] = [
                [file for sub_pack in pack for file in sub_pack]
                for pack in packed_files
            ]

            splits += self._build_split_from_pack(flatten_packed_files, file_entries, False)

        if self.idx_of_this_subtask is not None:
            self._compute_split_start_end_row(splits, plan_start_row, plan_end_row)
        return splits

    def _split_by_row_id(self, files: List[DataFileMeta]) -> List[List[DataFileMeta]]:
        split_by_row_id = []

        def sort_key(file: DataFileMeta) -> tuple:
            first_row_id = file.first_row_id if file.first_row_id is not None else float('-inf')
            is_blob = 1 if self._is_blob_file(file.file_name) else 0
            # For files with same firstRowId, sort by maxSequenceNumber in descending order
            # (larger sequence number means more recent data)
            max_seq = file.max_sequence_number
            return (first_row_id, is_blob, -max_seq)

        sorted_files = sorted(files, key=sort_key)

        # Filter blob files to only include those within the row ID range of non-blob files
        sorted_files = self._filter_blob(sorted_files)

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

    @staticmethod
    def _is_blob_file(file_name: str) -> bool:
        return file_name.endswith('.blob')

    @staticmethod
    def _filter_blob(files: List[DataFileMeta]) -> List[DataFileMeta]:
        result = []
        row_id_start = -1
        row_id_end = -1

        for file in files:
            if not FullStartingScanner._is_blob_file(file.file_name):
                if file.first_row_id is not None:
                    row_id_start = file.first_row_id
                    row_id_end = file.first_row_id + file.row_count
                result.append(file)
            else:
                if file.first_row_id is not None and row_id_start != -1:
                    if row_id_start <= file.first_row_id < row_id_end:
                        result.append(file)

        return result
