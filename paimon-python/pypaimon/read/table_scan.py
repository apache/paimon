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
from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.read.interval_partition import IntervalPartition, SortedRun
from pypaimon.read.plan import Plan
from pypaimon.read.split import Split
from pypaimon.schema.data_types import DataField
from pypaimon.snapshot.snapshot_manager import SnapshotManager


class TableScan:
    """Implementation of TableScan for native Python reading."""

    def __init__(self, table, predicate: Optional[Predicate], limit: Optional[int],
                 read_type: List[DataField]):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.predicate = predicate
        self.predicate = predicate
        self.limit = limit
        self.read_type = read_type

        self.snapshot_manager = SnapshotManager(table)
        self.manifest_list_manager = ManifestListManager(table)
        self.manifest_file_manager = ManifestFileManager(table)

        self.partition_conditions = self._extract_partition_conditions()
        self.target_split_size = 128 * 1024 * 1024
        self.open_file_cost = 4 * 1024 * 1024

    def plan(self) -> Plan:
        latest_snapshot = self.snapshot_manager.get_latest_snapshot()
        if not latest_snapshot:
            return Plan([])
        manifest_files = self.manifest_list_manager.read_all_manifest_files(latest_snapshot)

        file_entries = []
        for manifest_file_path in manifest_files:
            manifest_entries = self.manifest_file_manager.read(manifest_file_path)
            for entry in manifest_entries:
                if entry.kind == 0:
                    file_entries.append(entry)

        if self.predicate:
            file_entries = self._filter_by_predicate(file_entries)

        partitioned_split = defaultdict(list)
        for entry in file_entries:
            partitioned_split[(tuple(entry.partition.values), entry.bucket)].append(entry)

        splits = []
        for key, values in partitioned_split.items():
            if self.table.is_primary_key_table:
                splits += self._create_primary_key_splits(values)
            else:
                splits += self._create_append_only_splits(values)

        splits = self._apply_push_down_limit(splits)

        return Plan(splits)

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
            if self.partition_conditions and not self._filter_by_partition(file_entry):
                continue
            if not self._filter_by_stats(file_entry):
                continue
            filtered_files.append(file_entry)

        return filtered_files

    def _filter_by_partition(self, file_entry: ManifestEntry) -> bool:
        # TODO: refactor with a better solution
        partition_dict = file_entry.partition.to_dict()
        for field_name, condition in self.partition_conditions.items():
            partition_value = partition_dict[field_name]
            if condition['op'] == '=':
                if str(partition_value) != str(condition['value']):
                    return False
            elif condition['op'] == 'in':
                if str(partition_value) not in [str(v) for v in condition['values']]:
                    return False
            elif condition['op'] == 'notIn':
                if str(partition_value) in [str(v) for v in condition['values']]:
                    return False
            elif condition['op'] == '>':
                if partition_value <= condition['values']:
                    return False
            elif condition['op'] == '>=':
                if partition_value < condition['values']:
                    return False
            elif condition['op'] == '<':
                if partition_value >= condition['values']:
                    return False
            elif condition['op'] == '<=':
                if partition_value > condition['values']:
                    return False
        return True

    def _filter_by_stats(self, file_entry: ManifestEntry) -> bool:
        # TODO: real support for filtering by stat
        return True

    def _extract_partition_conditions(self) -> dict:
        if not self.predicate or not self.table.partition_keys:
            return {}

        conditions = {}
        self._extract_conditions_from_predicate(self.predicate, conditions, self.table.partition_keys)
        return conditions

    def _extract_conditions_from_predicate(self, predicate: 'Predicate', conditions: dict,
                                           partition_keys: List[str]):
        if predicate.method == 'and':
            for sub_predicate in predicate.literals:
                self._extract_conditions_from_predicate(sub_predicate, conditions, partition_keys)
            return
        elif predicate.method == 'or':
            all_partition_conditions = True
            for sub_predicate in predicate.literals:
                if sub_predicate.field not in partition_keys:
                    all_partition_conditions = False
                    break
            if all_partition_conditions:
                for sub_predicate in predicate.literals:
                    self._extract_conditions_from_predicate(sub_predicate, conditions, partition_keys)
            return

        if predicate.field in partition_keys:
            if predicate.method == 'equal':
                conditions[predicate.field] = {
                    'op': '=',
                    'value': predicate.literals[0] if predicate.literals else None
                }
            elif predicate.method == 'in':
                conditions[predicate.field] = {
                    'op': 'in',
                    'values': predicate.literals if predicate.literals else []
                }
            elif predicate.method == 'notIn':
                conditions[predicate.field] = {
                    'op': 'notIn',
                    'values': predicate.literals if predicate.literals else []
                }
            elif predicate.method == 'greaterThan':
                conditions[predicate.field] = {
                    'op': '>',
                    'value': predicate.literals[0] if predicate.literals else None
                }
            elif predicate.method == 'greaterOrEqual':
                conditions[predicate.field] = {
                    'op': '>=',
                    'value': predicate.literals[0] if predicate.literals else None
                }
            elif predicate.method == 'lessThan':
                conditions[predicate.field] = {
                    'op': '<',
                    'value': predicate.literals[0] if predicate.literals else None
                }
            elif predicate.method == 'lessOrEqual':
                conditions[predicate.field] = {
                    'op': '<=',
                    'value': predicate.literals[0] if predicate.literals else None
                }

    def _create_append_only_splits(self, file_entries: List[ManifestEntry]) -> List['Split']:
        if not file_entries:
            return []

        data_files: List[DataFileMeta] = [e.file for e in file_entries]

        def weight_func(f: DataFileMeta) -> int:
            return max(f.file_size, self.open_file_cost)

        packed_files: List[List[DataFileMeta]] = _pack_for_ordered(data_files, weight_func, self.target_split_size)
        return self._build_split_from_pack(packed_files, file_entries, False)

    def _create_primary_key_splits(self, file_entries: List[ManifestEntry]) -> List['Split']:
        if not file_entries:
            return []

        data_files: List[DataFileMeta] = [e.file for e in file_entries]
        partition_sort_runs: List[List[SortedRun]] = IntervalPartition(data_files).partition()
        sections: List[List[DataFileMeta]] = [
            [file for s in sl for file in s.files]
            for sl in partition_sort_runs
        ]

        def weight_func(fl: List[DataFileMeta]) -> int:
            return max(sum(f.file_size for f in fl), self.open_file_cost)

        packed_files: List[List[List[DataFileMeta]]] = _pack_for_ordered(sections, weight_func, self.target_split_size)
        flatten_packed_files: List[List[DataFileMeta]] = [
            [file for sub_pack in pack for file in sub_pack]
            for pack in packed_files
        ]
        return self._build_split_from_pack(flatten_packed_files, file_entries, True)

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


def _pack_for_ordered(items: List, weight_func: Callable, target_weight: int) -> List[List]:
    packed = []
    bin_items = []
    bin_weight = 0

    for item in items:
        weight = weight_func(item)
        if bin_weight + weight > target_weight and len(bin_items) > 0:
            packed.append(bin_items)
            bin_items.clear()
            bin_weight = 0

        bin_weight += weight
        bin_items.append(item)

    if len(bin_items) > 0:
        packed.append(bin_items)

    return packed
