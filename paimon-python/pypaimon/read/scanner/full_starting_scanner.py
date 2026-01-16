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
from typing import List, Optional, Dict, Set

from pypaimon.common.predicate import Predicate
from pypaimon.globalindex import VectorSearchGlobalIndexResult
from pypaimon.table.source.deletion_file import DeletionFile
from pypaimon.manifest.index_manifest_file import IndexManifestFile
from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.manifest.schema.manifest_file_meta import ManifestFileMeta
from pypaimon.read.plan import Plan
from pypaimon.read.push_down_utils import (trim_and_transform_predicate)
from pypaimon.read.scanner.append_table_split_generator import AppendTableSplitGenerator
from pypaimon.read.scanner.data_evolution_split_generator import DataEvolutionSplitGenerator
from pypaimon.read.scanner.primary_key_table_split_generator import PrimaryKeyTableSplitGenerator
from pypaimon.read.scanner.starting_scanner import StartingScanner
from pypaimon.read.split import DataSplit
from pypaimon.snapshot.snapshot_manager import SnapshotManager
from pypaimon.table.bucket_mode import BucketMode
from pypaimon.manifest.simple_stats_evolutions import SimpleStatsEvolutions


class FullStartingScanner(StartingScanner):
    def __init__(
        self,
        table,
        predicate: Optional[Predicate],
        limit: Optional[int],
        vector_search: Optional['VectorSearch'] = None
    ):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.predicate = predicate
        self.limit = limit
        self.vector_search = vector_search

        self.snapshot_manager = SnapshotManager(table)
        self.manifest_list_manager = ManifestListManager(table)
        self.manifest_file_manager = ManifestFileManager(table)

        self.primary_key_predicate = trim_and_transform_predicate(
            self.predicate, self.table.field_names, self.table.trimmed_primary_keys)

        self.partition_key_predicate = trim_and_transform_predicate(
            self.predicate, self.table.field_names, self.table.partition_keys)
        options = self.table.options
        # Get split target size and open file cost from table options
        self.target_split_size = options.source_split_target_size()
        self.open_file_cost = options.source_split_open_file_cost()

        self.only_read_real_buckets = options.bucket() == BucketMode.POSTPONE_BUCKET.value
        self.data_evolution = options.data_evolution_enabled()
        self.deletion_vectors_enabled = options.deletion_vectors_enabled()

        def schema_fields_func(schema_id: int):
            return self.table.schema_manager.get_schema(schema_id).fields

        self.simple_stats_evolutions = SimpleStatsEvolutions(
            schema_fields_func,
            self.table.table_schema.id
        )

        # Create appropriate split generator based on table type
        if self.table.is_primary_key_table:
            self.split_generator = PrimaryKeyTableSplitGenerator(
                self.table,
                self.target_split_size,
                self.open_file_cost,
            )
        elif self.data_evolution:
            global_index_result = self._eval_global_index()
            row_ranges = None
            score_getter = None
            if global_index_result is not None:
                row_ranges = global_index_result.results().to_range_list()
                if isinstance(global_index_result, VectorSearchGlobalIndexResult):
                    score_getter = global_index_result.score_getter()
            self.split_generator = DataEvolutionSplitGenerator(
                self.table,
                self.target_split_size,
                self.open_file_cost,
                row_ranges,
                score_getter
            )
        else:
            self.split_generator = AppendTableSplitGenerator(
                self.table,
                self.target_split_size,
                self.open_file_cost,
            )

    def scan(self) -> Plan:
        file_entries = self.plan_files()
        if not file_entries:
            return Plan([])
        # Get deletion files map if deletion vectors are enabled.
        # {partition-bucket -> {filename -> DeletionFile}}
        if self.deletion_vectors_enabled:
            latest_snapshot = self.snapshot_manager.get_latest_snapshot()
            # Extract unique partition-bucket pairs from file entries
            buckets = set()
            for entry in file_entries:
                buckets.add((tuple(entry.partition.values), entry.bucket))
            deletion_files_map = self._scan_dv_index(latest_snapshot, buckets)
            self.split_generator.deletion_files_map = deletion_files_map

        # Generate splits
        splits = self.split_generator.create_splits(file_entries)

        splits = self._apply_push_down_limit(splits)
        return Plan(splits)

    def plan_files(self) -> List[ManifestEntry]:
        latest_snapshot = self.snapshot_manager.get_latest_snapshot()
        if not latest_snapshot:
            return []
        manifest_files = self.manifest_list_manager.read_all(latest_snapshot)
        return self.read_manifest_entries(manifest_files)

    def _eval_global_index(self):
        from pypaimon.globalindex.global_index_result import GlobalIndexResult
        from pypaimon.globalindex.global_index_scan_builder import (
            GlobalIndexScanBuilder
        )
        from pypaimon.globalindex.range import Range

        # No filter and no vector search - nothing to evaluate
        if self.predicate is None and self.vector_search is None:
            return None

        # Check if global index is enabled
        if not self.table.options.global_index_enabled():
            return None

        # Get latest snapshot
        snapshot = self.snapshot_manager.get_latest_snapshot()
        if snapshot is None:
            return None

        # Check if table has store with global index scan builder
        index_scan_builder = self.table.new_global_index_scan_builder()
        if index_scan_builder is None:
            return None

        # Set partition predicate and snapshot
        index_scan_builder.with_partition_predicate(
            self.partition_key_predicate
        ).with_snapshot(snapshot)

        # Get indexed row ranges
        indexed_row_ranges = index_scan_builder.shard_list()
        if not indexed_row_ranges:
            return None

        # Get next row ID from snapshot
        next_row_id = snapshot.next_row_id
        if next_row_id is None:
            return None

        # Calculate non-indexed row ranges
        non_indexed_row_ranges = Range(0, next_row_id - 1).exclude(indexed_row_ranges)

        # Get thread number from options (can be None, meaning use default)
        thread_num = self.table.options.global_index_thread_num()

        # Scan global index in parallel
        result = GlobalIndexScanBuilder.parallel_scan(
            indexed_row_ranges,
            index_scan_builder,
            self.predicate,
            self.vector_search,
            thread_num
        )

        if result is None:
            return None

        for row_range in non_indexed_row_ranges:
            result = result.or_(GlobalIndexResult.from_range(row_range))

        return result

    def read_manifest_entries(self, manifest_files: List[ManifestFileMeta]) -> List[ManifestEntry]:
        max_workers = max(8, self.table.options.scan_manifest_parallelism(os.cpu_count() or 8))
        manifest_files = [entry for entry in manifest_files if self._filter_manifest_file(entry)]
        return self.manifest_file_manager.read_entries_parallel(
            manifest_files,
            self._filter_manifest_entry,
            max_workers=max_workers
        )

    def with_shard(self, idx_of_this_subtask: int, number_of_para_subtasks: int) -> 'FullStartingScanner':
        self.split_generator.with_shard(idx_of_this_subtask, number_of_para_subtasks)
        return self

    def with_slice(self, start_pos: int, end_pos: int) -> 'FullStartingScanner':
        self.split_generator.with_slice(start_pos, end_pos)
        return self

    def with_sample(self, num_rows: int) -> 'FullStartingScanner':
        self.split_generator.with_sample(num_rows)
        return self

    def _apply_push_down_limit(self, splits: List[DataSplit]) -> List[DataSplit]:
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
        if self.deletion_vectors_enabled and entry.file.level == 0:  # do not read level 0 file
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

    def _scan_dv_index(self, snapshot, buckets: Set[tuple]) -> Dict[tuple, Dict[str, DeletionFile]]:
        """
        Scan deletion vector index from snapshot.
        Returns a map of (partition, bucket) -> {filename -> DeletionFile}

        Reference: SnapshotReaderImpl.scanDvIndex() in Java
        """
        if not snapshot or not snapshot.index_manifest:
            return {}

        result = {}

        # Read index manifest file
        index_manifest_file = IndexManifestFile(self.table)
        index_entries = index_manifest_file.read(snapshot.index_manifest)

        # Filter by DELETION_VECTORS_INDEX type and requested buckets
        for entry in index_entries:
            if entry.index_file.index_type != IndexManifestFile.DELETION_VECTORS_INDEX:
                continue

            partition_bucket = (tuple(entry.partition.values), entry.bucket)
            if partition_bucket not in buckets:
                continue

            # Convert to deletion files
            deletion_files = self._to_deletion_files(entry)
            if deletion_files:
                result[partition_bucket] = deletion_files

        return result

    def _to_deletion_files(self, index_entry) -> Dict[str, DeletionFile]:
        """
        Convert index manifest entry to deletion files map.
        Returns {filename -> DeletionFile}
        """
        deletion_files = {}
        index_file = index_entry.index_file

        # Check if dv_ranges exists
        if not index_file.dv_ranges:
            return deletion_files

        # Build deletion file path
        # Format: manifest/index-manifest-{uuid}
        index_path = self.table.table_path.rstrip('/') + '/index'
        dv_file_path = f"{index_path}/{index_file.file_name}"

        # Convert each DeletionVectorMeta to DeletionFile
        for data_file_name, dv_meta in index_file.dv_ranges.items():
            deletion_file = DeletionFile(
                dv_index_path=dv_file_path,
                offset=dv_meta.offset,
                length=dv_meta.length,
                cardinality=dv_meta.cardinality
            )
            deletion_files[data_file_name] = deletion_file

        return deletion_files
