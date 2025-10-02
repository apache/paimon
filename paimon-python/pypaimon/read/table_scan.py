################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

from collections import defaultdict
from typing import Callable, List, Optional

from pypaimon.common.predicate import Predicate
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.read.interval_partition import IntervalPartition, SortedRun
from pypaimon.read.plan import Plan
from pypaimon.read.push_down_utils import (extract_predicate_to_dict,
                                           extract_predicate_to_list)
from pypaimon.read.split import Split
from pypaimon.schema.data_types import DataField
from pypaimon.snapshot.snapshot_manager import SnapshotManager
from pypaimon.table.bucket_mode import BucketMode


class TableScan:
    """Implementation of TableScan for native Python reading."""

    def __init__(self, table, predicate: Optional[Predicate], limit: Optional[int],
                 read_type: List[DataField]):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.predicate = predicate
        self.limit = limit
        self.read_type = read_type

        self.snapshot_manager = SnapshotManager(table)
        self.manifest_list_manager = ManifestListManager(table)
        self.manifest_file_manager = ManifestFileManager(table)

        pk_conditions = []
        trimmed_pk = [field.name for field in self.table.table_schema.get_trimmed_primary_key_fields()]
        extract_predicate_to_list(pk_conditions, self.predicate, trimmed_pk)
        self.primary_key_predicate = PredicateBuilder(self.table.fields).and_predicates(pk_conditions)

        partition_conditions = defaultdict(list)
        extract_predicate_to_dict(partition_conditions, self.predicate, self.table.partition_keys)
        self.partition_key_predicate = partition_conditions

        self.target_split_size = 128 * 1024 * 1024
        self.open_file_cost = 4 * 1024 * 1024

        self.idx_of_this_subtask = None
        self.number_of_para_subtasks = None

        self.only_read_real_buckets = True if int(
            self.table.options.get('bucket', -1)) == BucketMode.POSTPONE_BUCKET.value else False

    def plan(self) -> Plan:
        file_entries = self.plan_files()
        if not file_entries:
            return Plan([])
        if self.table.is_primary_key_table:
            splits = self._create_primary_key_splits(file_entries)
        else:
            splits = self._create_append_only_splits(file_entries)

        splits = self._apply_push_down_limit(splits)
        return Plan(splits)

    def plan_files(self) -> List[ManifestEntry]:
        latest_snapshot = self.snapshot_manager.get_latest_snapshot()
        if not latest_snapshot:
            return []
        manifest_files = self.manifest_list_manager.read_all(latest_snapshot)

        deleted_entries = set()
        added_entries = []
        # TODO: filter manifest files by predicate
        for manifest_file in manifest_files:
            manifest_entries = self.manifest_file_manager.read(manifest_file.file_name,
                                                               lambda row: self._bucket_filter(row))
            for entry in manifest_entries:
                if entry.kind == 0:
                    added_entries.append(entry)
                else:
                    deleted_entries.add((tuple(entry.partition.values), entry.bucket, entry.file.file_name))

        file_entries = [
            entry for entry in added_entries
            if (tuple(entry.partition.values), entry.bucket, entry.file.file_name) not in deleted_entries
        ]
        if self.predicate:
            file_entries = self._filter_by_predicate(file_entries)
        return file_entries

    def with_shard(self, idx_of_this_subtask, number_of_para_subtasks) -> 'TableScan':
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

    def _bucket_filter(self, entry: Optional[ManifestEntry]) -> bool:
        bucket = entry.bucket
        if self.only_read_real_buckets and bucket < 0:
            return False
        return True

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

    def _filter_by_predicate(self, file_entries: List[ManifestEntry]) -> List[ManifestEntry]:
        if not self.predicate:
            return file_entries

        filtered_files = []
        for file_entry in file_entries:
            if self.partition_key_predicate and not self._filter_by_partition(file_entry):
                continue
            if not self._filter_by_stats(file_entry):
                continue
            filtered_files.append(file_entry)

        return filtered_files

    def _filter_by_partition(self, file_entry: ManifestEntry) -> bool:
        partition_dict = file_entry.partition.to_dict()
        for field_name, conditions in self.partition_key_predicate.items():
            partition_value = partition_dict[field_name]
            for predicate in conditions:
                if not predicate.test_by_value(partition_value):
                    return False
        return True

    def _filter_by_stats(self, file_entry: ManifestEntry) -> bool:
        if file_entry.kind != 0:
            return False
        if self.table.is_primary_key_table:
            predicate = self.primary_key_predicate
            stats = file_entry.file.key_stats
        else:
            predicate = self.predicate
            stats = file_entry.file.value_stats
        return predicate.test_by_stats({
            "min_values": stats.min_values.to_dict(),
            "max_values": stats.max_values.to_dict(),
            "null_counts": {
                stats.min_values.fields[i].name: stats.null_counts[i] for i in range(len(stats.min_values.fields))
            },
            "row_count": file_entry.file.row_count,
        })

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
