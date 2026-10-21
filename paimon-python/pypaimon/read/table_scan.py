# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from dataclasses import replace
from typing import List, Optional, Tuple, TYPE_CHECKING

from pypaimon.common.predicate import Predicate
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.read.auth_support import (
    authorize, latest_auth_fields, validate_auth_rules)
from pypaimon.read.plan import Plan
from pypaimon.read.scan_config import (
    _validate_scan_mode, _parse_incremental_timestamp_window, resolve_scan_snapshot_range)
from pypaimon.read.scan_request import ScanRequest
from pypaimon.read.scan_stats import ScanStats
from pypaimon.utils.range import Range


if TYPE_CHECKING:
    from pypaimon.table.file_store_table import FileStoreTable


class TableScan:
    """Public scan facade selecting the configured execution backend."""

    def __init__(
        self,
        table,
        predicate: Optional[Predicate],
        limit: Optional[int],
        partition_predicate: Optional[Predicate] = None,
    ):
        self.table: "FileStoreTable" = table
        self.predicate = predicate
        self.limit = limit
        self.partition_predicate = partition_predicate
        self._read_type = None
        self._query_auth_fn = self.table.catalog_environment.table_query_auth(
            self.table.options, self.table.identifier)
        self._file_scanner = None
        self._incremental_snapshot_range = resolve_scan_snapshot_range(self.table)
        empty_incremental = (
            self.table.options.options.contains(CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP)
            and self._incremental_snapshot_range is None)
        # Empty incremental scanners historically carry no row predicate or LIMIT.
        self._scan_configuration = ScanRequest.for_table(
            table, None if empty_incremental else predicate,
            None if empty_incremental else limit, partition_predicate)

    @property
    def file_scanner(self):
        """Create the Python scanner when callers explicitly request its API."""
        if self._file_scanner is None:
            scanner = self._create_file_scanner()
            request = self._scan_configuration
            if request.idx_of_this_subtask is not None:
                scanner.with_shard(request.idx_of_this_subtask, request.number_of_para_subtasks)
            if request.start_pos_of_this_subtask is not None:
                scanner.with_slice(request.start_pos_of_this_subtask, request.end_pos_of_this_subtask)
            if request.global_index_result is not None:
                scanner.with_global_index_result(request.global_index_result)
            if request.row_ranges is not None:
                scanner.with_row_ranges([Range(start, end) for start, end in request.row_ranges])
            if request.chunk_shuffle is not None:
                scanner.with_chunk_shuffle(*request.chunk_shuffle)
            self._file_scanner = scanner
        return self._file_scanner

    @file_scanner.setter
    def file_scanner(self, scanner):
        self._file_scanner = scanner

    def _scan_request(self) -> ScanRequest:
        """Capture current public scanner configuration for one preparation."""
        scanner = self._file_scanner
        if scanner is None:
            return replace(
                self._scan_configuration,
                read_predicate=self.predicate,
                read_limit=self.limit,
                partition_filter=self.partition_predicate,
                read_type=tuple(self._read_type) if self._read_type is not None else None,
                incremental_snapshot_range=self._incremental_snapshot_range,
            )
        row_ranges = getattr(scanner, '_row_ranges', None)
        chunk_shuffle = getattr(scanner, 'chunk_shuffle', None)
        return ScanRequest(
            is_primary_key_table=self.table.is_primary_key_table,
            partition_keys=tuple(self.table.partition_keys or ()),
            predicate=scanner.predicate,
            user_predicate=scanner._user_predicate,
            partition_key_predicate=scanner.partition_key_predicate,
            explicit_partition_filter=scanner._explicit_partition_filter,
            auth_has_non_partition_filter=scanner.auth_has_non_partition_filter,
            limit=scanner.limit,
            data_evolution=scanner.data_evolution,
            deletion_vectors_enabled=getattr(scanner, 'deletion_vectors_enabled', False),
            idx_of_this_subtask=scanner.idx_of_this_subtask,
            number_of_para_subtasks=scanner.number_of_para_subtasks,
            start_pos_of_this_subtask=scanner.start_pos_of_this_subtask,
            end_pos_of_this_subtask=scanner.end_pos_of_this_subtask,
            chunk_shuffle=tuple(chunk_shuffle) if chunk_shuffle is not None else None,
            global_index_result=scanner._global_index_result,
            row_ranges=(tuple((range_.from_, range_.to) for range_ in row_ranges)
                        if row_ranges is not None else None),
            read_predicate=self.predicate,
            read_limit=self.limit,
            partition_filter=self.partition_predicate,
            read_type=tuple(self._read_type) if self._read_type is not None else None,
            incremental_snapshot_range=getattr(self, '_incremental_snapshot_range', None),
        )

    def plan(self) -> Plan:
        from pypaimon.execution.read_dispatch import plan_scan
        return plan_scan(self)

    def plan_all_files(self) -> Plan:
        """Plan matching files including L0 and all data-evolution column files.

        Existing filters and limits still apply. Query authorization must be
        unrestricted, as required by writer restore, overwrite and indexing.
        """
        from pypaimon.execution.read_dispatch import plan_all_files
        return plan_all_files(self)

    def __auth_query(self, all_columns=False):
        return authorize(self.table, self._query_auth_fn, None if all_columns else self._read_type,
                         self.predicate, self.partition_predicate)

    def scan_with_stats(self) -> Tuple[Plan, Optional[ScanStats]]:
        """Run :meth:`plan` while recording manifest / pruning counters.

        Only used by :meth:`ReadBuilder.explain`; the regular read path
        keeps going through :meth:`plan`. Native scans always collect lightweight
        counters in the same core planning pass, without opening data files.
        """
        from pypaimon.execution.read_dispatch import scan_with_stats
        return scan_with_stats(self)

    def with_shard(self, idx_of_this_subtask, number_of_para_subtasks) -> 'TableScan':
        if self._file_scanner is None:
            self._scan_configuration = self._scan_configuration.with_shard(idx_of_this_subtask, number_of_para_subtasks)
        else:
            self._file_scanner.with_shard(idx_of_this_subtask, number_of_para_subtasks)
        return self

    def with_slice(self, start_pos, end_pos) -> 'TableScan':
        if self._file_scanner is None:
            self._scan_configuration = self._scan_configuration.with_slice(start_pos, end_pos)
        else:
            self._file_scanner.with_slice(start_pos, end_pos)
        return self

    def with_global_index_result(self, result) -> 'TableScan':
        if self._file_scanner is None:
            self._scan_configuration = self._scan_configuration.with_global_index_result(result)
        else:
            self._file_scanner.with_global_index_result(result)
        return self

    def with_row_ranges(self, row_ranges) -> 'TableScan':
        if self._file_scanner is None:
            self._scan_configuration = self._scan_configuration.with_row_ranges(row_ranges)
        else:
            self._file_scanner.with_row_ranges(row_ranges)
        return self

    def with_chunk_shuffle(self, seed: int, chunk_size: int) -> 'TableScan':
        if self._file_scanner is None:
            self._scan_configuration = self._scan_configuration.with_chunk_shuffle(seed, chunk_size)
        else:
            self._file_scanner.with_chunk_shuffle(seed, chunk_size)
        return self

    def _validate_scan_mode(self):
        _validate_scan_mode(self.table)

    def _create_file_scanner(self) -> "FileScanner":
        from pypaimon.execution.python.read.table_scan import PythonTableScanMixin
        return PythonTableScanMixin._create_file_scanner(self)

    def _native_requested(self) -> bool:
        from pypaimon.execution.read_dispatch import native_plan_requested
        return native_plan_requested(self)

    def _native_plan_supported(self) -> bool:
        # Any probe failure (e.g. a remote schema/metadata read) must fall back, not fail the scan.
        from pypaimon.execution.native.read.plan import NativeTableScanMixin
        return NativeTableScanMixin._native_plan_supported(self)

    def _native_plan_supported_impl(self) -> bool:
        """Fall back to the Python scanner for scans native can't carry:
        a primary-key table whose trimmed PK is empty (PK equals the partition
        key; Rust rejects this schema), unsupported time travel selectors,
        custom authorization, a missing pypaimon-rust, or a catalog / identifier Rust
        cannot reconstruct. Keep this capability gate in sync when adding scan
        features."""
        from pypaimon.execution.native.read.plan import NativeTableScanMixin
        return NativeTableScanMixin._native_plan_supported_impl(self)

    def _native_global_index_result_supported(self, request: Optional[ScanRequest] = None) -> bool:
        from pypaimon.execution.native.read.plan import NativeTableScanMixin
        return NativeTableScanMixin._native_global_index_result_supported(self, request)

    def _native_row_ranges(self) -> Optional[List[Tuple[int, int]]]:
        from pypaimon.execution.native.read.plan import NativeTableScanMixin
        return NativeTableScanMixin._native_row_ranges(self)

    def _try_native_plan(self, all_files=False) -> Optional[Plan]:
        """Plan via pypaimon_rust, then drop partitions the predicate rejects.

        Predicate and limit are pushed into Rust planning and are still enforced
        by the reader. Snapshot metadata is preserved even when pruning removes
        every split.
        """
        from pypaimon.execution.native.read.plan import NativeTableScanMixin
        return NativeTableScanMixin._try_native_plan(self, all_files)


__all__ = [
    "TableScan", "authorize", "latest_auth_fields", "validate_auth_rules",
    "_validate_scan_mode", "_parse_incremental_timestamp_window",
]
