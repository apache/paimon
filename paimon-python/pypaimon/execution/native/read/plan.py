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

"""Native batch and streaming planning behind the public scan facade."""

import logging
from dataclasses import replace
from typing import List, Optional, Tuple

from pypaimon.common.identifier import UNKNOWN_DATABASE
from pypaimon.common.options.core_options import CoreOptions, ChangelogProducer
from pypaimon.common.predicate import Predicate
from pypaimon.execution.native import runtime
from pypaimon.execution.native import table as native_table
from pypaimon.execution.native.read import builder as native_builder
from pypaimon.read.plan import Plan
from pypaimon.read.query_auth_split import QueryAuthSplit
from pypaimon.read.scan_stats import ScanStats
from pypaimon.read.scan_request import ScanRequest
from pypaimon.read import split_serializer

logger = logging.getLogger(__name__)

_NATIVE_TIME_TRAVEL_OPTIONS = frozenset({
    CoreOptions.SCAN_VERSION.key(), CoreOptions.SCAN_SNAPSHOT_ID.key(),
    CoreOptions.SCAN_TAG_NAME.key(), CoreOptions.SCAN_TIMESTAMP.key(),
    CoreOptions.SCAN_TIMESTAMP_MILLIS.key(), CoreOptions.SCAN_WATERMARK.key(),
})


def native_plan(
        table,
        predicate: Optional[Predicate] = None,
        limit: Optional[int] = None,
        projection: Optional[List[str]] = None,
        row_ranges: Optional[List[Tuple[int, int]]] = None,
        incremental_range: Optional[Tuple[int, int]] = None,
        incremental_mode: str = 'delta',
        row_position_slice: Optional[Tuple[int, int]] = None,
        row_position_shard: Optional[Tuple[int, int]] = None,
        chunk_shuffle: Optional[Tuple[int, int]] = None,
        shard: Optional[Tuple[int, int]] = None,
        partition_predicate: Optional[Predicate] = None,
        global_index_result=None,
        all_files: bool = False) -> Plan:
    """Plan with pypaimon_rust, preserving snapshot metadata.

    Native conversion or planning failures are handled by TableScan, which
    falls back to the Python planner.
    """
    if all_files and incremental_range is not None:
        raise ValueError('planning all files does not accept incremental ranges')
    if incremental_range is None and incremental_mode != 'delta':
        raise ValueError('incremental_mode requires incremental_range')
    if not runtime.native_runtime_available():
        raise RuntimeError(
            "scan.native-plan.enabled needs the pypaimon-rust extension")
    builder = native_builder._configure_native_read_builder(
        native_builder._native_read_builder(table), predicate, limit, projection)
    if partition_predicate is not None:
        builder = builder.with_partition_filter(native_builder._predicate_to_native(partition_predicate))
    if row_ranges is not None:
        builder = builder.with_row_ranges(row_ranges)
    from pypaimon.table.source.global_index_split_result import GlobalIndexSplitResult
    supplied_pk_splits = (isinstance(global_index_result, GlobalIndexSplitResult)
                          and not table.options.query_auth_enabled)
    if supplied_pk_splits or incremental_range is None:
        scan = builder.new_scan()
    elif incremental_mode == 'delta':
        scan = builder.new_incremental_scan(*incremental_range)
    else:
        scan = builder.new_incremental_scan(*incremental_range, incremental_mode)
    if global_index_result is not None and not table.options.query_auth_enabled:
        # A PK result already selected a snapshot and physical files. Java's
        # PrimaryKeyBatchScan returns those selections before ordinary planning.
        from pypaimon_rust.datafusion import GlobalIndexResult, Split as NativeSplit
        from pypaimon.globalindex.vector_search_result import ScoredGlobalIndexResult
        from pypaimon.table.source.primary_key_scored_result import PrimaryKeyScoredResult
        if isinstance(global_index_result, GlobalIndexSplitResult):
            # Searches and scans share the same core GlobalIndexResult.
            result = (getattr(global_index_result, '_native_result', None)
                      if type(global_index_result) is PrimaryKeyScoredResult else None)
            # Java's PK result uses snapshot 0 for an uncommitted table,
            # whereas the core search uses None. Keep the Python result's ID.
            if result is not None and result.snapshot_id() is None:
                result = None
            if result is None:
                result = GlobalIndexResult.from_splits(
                    global_index_result.snapshot_id,
                    [NativeSplit.deserialize(split_serializer.serialize_split_v1(split))
                     for split in global_index_result.splits])
        elif isinstance(global_index_result, ScoredGlobalIndexResult):
            score = global_index_result.score_getter()
            result = GlobalIndexResult.from_scored_row_ids({
                row_id: score(row_id) for row_id in global_index_result.results()})
        else:
            result = GlobalIndexResult.from_row_ranges([
                (range_.from_, range_.to) for range_ in global_index_result.results().to_range_list()])
        scan = scan.with_global_index_result(result)
    if row_position_slice is not None:
        scan = scan.with_row_position_slice(*row_position_slice)
    if row_position_shard is not None:
        scan = scan.with_row_position_shard(*row_position_shard)
    if chunk_shuffle is not None:
        seed, chunk_size = chunk_shuffle
        scan = scan.with_chunk_shuffle(str(seed), chunk_size)
    if shard is not None:
        scan = scan.with_shard(*shard)
    rust_plan = scan.plan_all_files() if all_files else scan.plan()
    return _from_native_plan(table, rust_plan)


def native_stream_scan(table, predicate=None, read_type=None, bucket_filter=None, consumer_id=None):
    """Create Rust's stateful StreamTableScan with the canonical reader type."""
    builder = native_builder._configure_native_read_builder(
        native_builder._native_read_builder(table), predicate, None, None, read_type=read_type)
    scan = builder.new_stream_scan()
    if bucket_filter is not None:
        scan.with_bucket_filter(bucket_filter)
    if consumer_id is not None:
        scan.with_consumer_id(consumer_id)
    return scan


def _from_native_plan(table, rust_plan) -> Plan:
    """Keep one split/metadata bridge for batch and streaming planning."""
    rust_splits = rust_plan.splits()
    pfields = native_table._partition_fields(table)
    # Trimmed primary keys decode per-file min/max keys (PK merge-on-read).
    kfields = table.trimmed_primary_keys_fields
    splits = [
        split_serializer.deserialize_split_v1(split.serialize(), pfields, kfields)
        for split in rust_splits
    ]
    if table.options.native_read_enabled():
        # Retain the opaque Rust split next to the Python metadata view so an
        # unchanged native plan can be read without reserializing each split.
        # A caller that needs different metadata (such as an endpoint DV)
        # passes a new Python split, which the native reader converts at read
        # time from its current fields.
        for split, rust_split in zip(splits, rust_splits):
            split._native_split = rust_split
    snapshot_id = rust_plan.snapshot_id()
    return Plan(splits, snapshot_id=snapshot_id, _native_stats=rust_plan.stats())


class NativeTableScan:
    """Adapt the public scan configuration to Core planning."""

    def __init__(self, scan):
        self._scan = scan

    def plan(self, auth_result=None):
        # Core requests and captures query authorization with its own plan.
        return self._plan()

    def plan_all_files(self):
        return self._plan(all_files=True)

    def scan_with_stats(self, auth_result=None):
        # Preserve explicit facade hooks while Core records counters as part
        # of the ordinary planning pass.
        plan = self._scan._try_native_plan()
        return None if plan is None else (plan, plan.stats())

    def _native_plan_supported(self) -> bool:
        # Any probe failure (e.g. a remote schema/metadata read) must fall back, not fail the scan.
        scan = self._scan
        try:
            return scan._native_plan_supported_impl()
        except Exception as e:
            logger.warning("Native-plan capability probe failed, falling back: %s", e)
            return False

    def _native_plan_supported_impl(self) -> bool:
        """Fall back to the Python scanner for scans native can't carry:
        a primary-key table whose trimmed PK is empty (PK equals the partition
        key; Rust rejects this schema), unsupported time travel selectors,
        custom authorization, a missing pypaimon-rust, or a catalog / identifier Rust
        cannot reconstruct. Keep this capability gate in sync when adding scan
        features."""
        scan = self._scan
        from pypaimon.execution.native.table import _resolved_schema_file_io_options
        if not runtime.native_runtime_available():
            return False
        request = scan._scan_request()
        if not scan._native_global_index_result_supported(request):
            return False
        if request.chunk_shuffle is not None:
            request.validate_chunk_shuffle_compat()
        if _resolved_schema_file_io_options(scan.table) is None:
            loader = getattr(
                getattr(scan.table, 'catalog_environment', None),
                'catalog_loader',
                None,
            )
            context_fn = getattr(loader, 'context', None)
            if not callable(context_fn):
                return False
            from pypaimon.execution.native.table import _catalog_metastore
            if _catalog_metastore(loader) is None:
                return False
            context = context_fn()
            catalog_options = getattr(context, 'options', None)
            if catalog_options is None:
                return False
            database_name = scan.table.identifier.get_database_name()
            if not database_name or database_name == UNKNOWN_DATABASE:
                return False
        if scan.table.options.query_auth_enabled:
            # The filesystem path has no catalog-issued authorization session.
            loader = getattr(getattr(scan.table, 'catalog_environment', None), 'catalog_loader', None)
            from pypaimon.execution.native.table import _catalog_metastore
            if loader is None or _catalog_metastore(loader) != 'rest':
                return False
            from pypaimon.execution.native.table import _native_query_auth_supported
            if not _native_query_auth_supported(scan.table):
                return False
        # Rust rejects schemas whose primary keys are all partition keys.
        if getattr(scan.table, 'is_primary_key_table', False) \
                and not scan.table.trimmed_primary_keys:
            return False
        options = scan.table.options.options
        from pypaimon.snapshot.time_travel_util import SCAN_KEYS
        unsupported_scan_keys = set(SCAN_KEYS) - _NATIVE_TIME_TRAVEL_OPTIONS
        return not any(options.contains_key(k) for k in unsupported_scan_keys)

    def _native_global_index_result_supported(self, request: Optional[ScanRequest] = None) -> bool:
        scan = self._scan
        request = scan._scan_request() if request is None else request
        result = request.global_index_result
        if result is None:
            return True
        if request.is_primary_key_table:
            from pypaimon.table.source.global_index_split_result import GlobalIndexSplitResult
            return isinstance(result, GlobalIndexSplitResult)
        if not request.data_evolution:
            return False
        from pypaimon.globalindex.global_index_result import GlobalIndexResult
        return isinstance(result, GlobalIndexResult)

    def _native_row_ranges(self) -> Optional[List[Tuple[int, int]]]:
        scan = self._scan
        row_ranges = scan._scan_request().row_ranges
        return list(row_ranges) if row_ranges is not None else None

    def _plan(self, all_files=False) -> Optional[Plan]:
        """Plan via pypaimon_rust, then drop partitions the predicate rejects.

        Predicate and limit are pushed into Rust planning and are still enforced
        by the reader. Snapshot metadata is preserved even when pruning removes
        every split.
        """

        scan = self._scan
        try:
            request = scan._scan_request()
            has_distribution = (request.idx_of_this_subtask is not None
                                or request.start_pos_of_this_subtask is not None)
            extra_options = {}
            if scan.table.options.options.contains(CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP):
                if request.incremental_snapshot_range is None:
                    return Plan([], _scan_stats=ScanStats.empty_native(request.read_limit))
                extra_options['incremental_range'] = request.incremental_snapshot_range
                if scan.table.options.changelog_producer() != ChangelogProducer.NONE:
                    extra_options['incremental_mode'] = 'changelog'
            chunk_shuffle = request.chunk_shuffle
            if chunk_shuffle is not None:
                extra_options['chunk_shuffle'] = chunk_shuffle
            if (request.idx_of_this_subtask is not None
                    and (chunk_shuffle is not None or request.is_primary_key_table)):
                extra_options['shard'] = (
                    request.idx_of_this_subtask, request.number_of_para_subtasks)
            if (has_distribution and not request.is_primary_key_table
                    and chunk_shuffle is None):
                if request.idx_of_this_subtask is not None:
                    extra_options['row_position_shard'] = (
                        request.idx_of_this_subtask, request.number_of_para_subtasks)
                else:
                    extra_options['row_position_slice'] = (
                        request.start_pos_of_this_subtask, request.end_pos_of_this_subtask)
            row_ranges = list(request.row_ranges) if request.row_ranges is not None else None
            if request.global_index_result is not None:
                extra_options['global_index_result'] = request.global_index_result
            if request.partition_filter is not None:
                # Explicit partition selectors are scan-only. Keep the ordinary
                # row predicate separate so core can apply Java's override and
                # masked-partition validation semantics.
                extra_options['partition_predicate'] = request.partition_key_predicate
            # Append positions still precede LIMIT. Rust owns PK shards and
            # source-backed sorted-index refinement before applying LIMIT.
            defer_limit = has_distribution and not request.is_primary_key_table
            if all_files:
                extra_options['all_files'] = True
            plan = native_plan(
                scan.table,
                predicate=request.read_predicate,
                limit=None if defer_limit else request.read_limit,
                projection=(
                    [field.name for field in request.read_type]
                    if request.read_type is not None and chunk_shuffle is None else None),
                row_ranges=row_ranges,
                **extra_options,
            )
            splits = plan.splits()
            partition_predicate = request.partition_key_predicate
            if (partition_predicate is not None and not (
                    request.is_primary_key_table and request.global_index_result is not None)
                    and not any(isinstance(split, QueryAuthSplit) for split in splits)):
                # Authorized user predicates observe masked partition values;
                # Rust has already applied only the safe raw partition tests.
                splits = [s for s in splits
                          if getattr(s, 'partition', None) is None
                          or partition_predicate.test(s.partition)]
            if chunk_shuffle is None and defer_limit:
                # A partial IndexedSplit plus a file-wide DV cardinality cannot
                # reveal how many deleted rows lie inside the selected range.
                # Keep every selected split and let the reader enforce LIMIT.
                if not (request.deletion_vectors_enabled
                        and not request.is_primary_key_table):
                    splits = request.apply_push_down_limit(splits)
            return replace(plan, _splits=splits, _stats_limit=request.read_limit if defer_limit else None)
        except Exception as e:
            runtime._raise_if_native_fork_safety_error(e)
            # Any native construction/planning/pruning failure -> fall back.
            logger.warning(
                "Native plan failed, falling back to the Python scanner: %s", e)
            return None
