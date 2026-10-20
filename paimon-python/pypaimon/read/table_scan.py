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

from typing import Optional, Tuple

from pypaimon.catalog.catalog_exception import TableNoPermissionException
from pypaimon.common.predicate import Predicate
from pypaimon.execution.native.read.plan import NativeTableScanMixin
from pypaimon.execution.python.read.table_scan import PythonTableScanMixin
from pypaimon.read.auth_support import (
    authorize, latest_auth_fields, prune_scanner_by_auth, validate_auth_rules)
from pypaimon.read.plan import Plan
from pypaimon.read.query_auth_split import wrap_plan_with_auth
from pypaimon.read.scan_config import _validate_scan_mode, _parse_incremental_timestamp_window
from pypaimon.read.scan_request import ScanRequest
from pypaimon.read.scan_stats import ScanStats


class TableScan(PythonTableScanMixin, NativeTableScanMixin):
    """Public scan facade selecting the configured execution backend."""

    def __init__(
        self,
        table,
        predicate: Optional[Predicate],
        limit: Optional[int],
        partition_predicate: Optional[Predicate] = None,
    ):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.predicate = predicate
        self.limit = limit
        self.partition_predicate = partition_predicate
        self._read_type = None
        self._query_auth_fn = self.table.catalog_environment.table_query_auth(
            self.table.options, self.table.identifier)
        self.file_scanner = self._create_file_scanner()

    def _scan_request(self) -> ScanRequest:
        """Capture current public scanner configuration for one preparation."""
        scanner = self.file_scanner
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
        native_auth = (self.table.options.query_auth_enabled
                       and self._native_requested() and self._native_plan_supported())
        if native_auth:
            # Rust asks the REST server and carries its complete response in
            # QueryAuthSplit. Both readers then enforce that captured policy.
            native = self._try_native_plan()
            if native is not None:
                return native
        auth_result = self.__auth_query()
        if self._query_auth_fn is not None:
            prune_scanner_by_auth(self.table, self.file_scanner, auth_result)
        # The native planner bypasses the auth-aware file scanner. Resolve auth
        # before selecting a planning backend.
        if (not native_auth and auth_result is None and self._native_requested()
                and self._native_plan_supported()):
            native = self._try_native_plan()
            if native is not None:
                return native
        plan = self.file_scanner.scan()
        return wrap_plan_with_auth(auth_result, plan)

    def plan_all_files(self) -> Plan:
        """Plan matching files including L0 and all data-evolution column files.

        Existing filters and limits still apply. Query authorization must be
        unrestricted, as required by writer restore, overwrite and indexing.
        """
        from pypaimon.execution.native.table import _native_query_auth_supported
        if self._native_requested() and _native_query_auth_supported(self.table) and self._native_plan_supported():
            plan = self._try_native_plan(all_files=True)
            if plan is not None:
                return plan
        if self.__auth_query(all_columns=True) is not None:
            raise TableNoPermissionException(self.table.identifier)
        if self._query_auth_fn is not None:
            prune_scanner_by_auth(self.table, self.file_scanner, None)
        # Writer restore/overwrite must see L0 even when batch reads hide it.
        skip_level0 = self.file_scanner.skip_level0
        try:
            self.file_scanner.skip_level0 = False
            return self.file_scanner.scan()
        finally:
            self.file_scanner.skip_level0 = skip_level0

    def __auth_query(self, all_columns=False):
        return authorize(self.table, self._query_auth_fn, None if all_columns else self._read_type,
                         self.predicate, self.partition_predicate)

    def scan_with_stats(self) -> Tuple[Plan, Optional[ScanStats]]:
        """Run :meth:`plan` while recording manifest / pruning counters.

        Only used by :meth:`ReadBuilder.explain`; the regular read path
        keeps going through :meth:`plan`. Native scans always collect lightweight
        counters in the same core planning pass, without opening data files.
        """
        native_auth = (self.table.options.query_auth_enabled
                       and self._native_requested() and self._native_plan_supported())
        if native_auth:
            native = self._try_native_plan()
            if native is not None:
                return native, native.stats()
        auth_result = self.__auth_query()
        if self._query_auth_fn is not None:
            prune_scanner_by_auth(self.table, self.file_scanner, auth_result)
        if (not native_auth and auth_result is None and self._native_requested()
                and self._native_plan_supported()):
            native = self._try_native_plan()
            if native is not None:
                return native, native.stats()
        plan, stats = self.file_scanner.scan_with_stats()
        return wrap_plan_with_auth(auth_result, plan), stats

    def with_shard(self, idx_of_this_subtask, number_of_para_subtasks) -> 'TableScan':
        self.file_scanner.with_shard(idx_of_this_subtask, number_of_para_subtasks)
        return self

    def with_slice(self, start_pos, end_pos) -> 'TableScan':
        self.file_scanner.with_slice(start_pos, end_pos)
        return self

    def with_global_index_result(self, result) -> 'TableScan':
        self.file_scanner.with_global_index_result(result)
        return self

    def with_row_ranges(self, row_ranges) -> 'TableScan':
        self.file_scanner.with_row_ranges(row_ranges)
        return self

    def with_chunk_shuffle(self, seed: int, chunk_size: int) -> 'TableScan':
        self.file_scanner.with_chunk_shuffle(seed, chunk_size)
        return self

    def _validate_scan_mode(self):
        _validate_scan_mode(self.table)

__all__ = [
    "TableScan", "authorize", "latest_auth_fields", "validate_auth_rules",
    "prune_scanner_by_auth", "_validate_scan_mode", "_parse_incremental_timestamp_window",
]
