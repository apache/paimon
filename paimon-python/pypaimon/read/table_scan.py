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

import json as _json
import logging
from typing import Optional, Tuple

from pypaimon.catalog.catalog_exception import TableNoPermissionException
from pypaimon.common.identifier import UNKNOWN_DATABASE
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.predicate import Predicate
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.read.plan import Plan
from pypaimon.read.query_auth_split import resolve_auth_result, wrap_plan_with_auth
from pypaimon.read.scan_stats import ScanStats
from pypaimon.read.scanner.file_scanner import FileScanner

logger = logging.getLogger(__name__)

# Options native forwards to Rust; any other copy() override is invisible to Rust.
_NATIVE_FORWARDED_OPTIONS = frozenset({
    CoreOptions.SCAN_NATIVE_PLAN_ENABLED.key(),
    CoreOptions.SOURCE_SPLIT_TARGET_SIZE.key(),
    CoreOptions.SOURCE_SPLIT_OPEN_FILE_COST.key(),
})


class TableScan:
    """Implementation of TableScan for native Python reading."""

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

    def plan(self) -> Plan:
        auth_result = self.__auth_query()
        # Native planning covers only a plain full-snapshot scan and bypasses the
        # auth-aware file scanner; fall back to the normal path otherwise.
        if (auth_result is None and self.table.options.native_plan_enabled()
                and self._native_plan_supported()):
            native = self._try_native_plan()
            if native is not None:
                return native
        if auth_result is not None:
            prune_scanner_by_auth(self.table, self.file_scanner, auth_result)
        plan = self.file_scanner.scan()
        return wrap_plan_with_auth(auth_result, plan)

    def _native_plan_supported(self) -> bool:
        """Fall back to the Python scanner for scans native can't carry:
        shard/slice, chunk-shuffle, global-index, first-row merge-engine (Rust
        drops L0), deletion vectors (Python drops L0), data evolution
        (dedicated split generator), postpone bucket (drops synthetic buckets),
        a primary-key table whose trimmed PK is empty (PK equals the partition
        key; native may mark splits raw-convertible and skip merge), dynamic
        bucket / cross-partition PK tables (unconfirmed Rust parity), copy()
        overrides Rust does not see (e.g. a removed scan.snapshot-id), query
        auth, non-main branch, time-travel, incremental, a missing/old
        pypaimon-rust, or a catalog / identifier Rust cannot reconstruct. Keep
        this capability gate in sync when adding scan features."""
        from pypaimon.read.native_plan import native_runtime_available
        if not native_runtime_available():
            return False
        fs = self.file_scanner
        if (getattr(fs, 'idx_of_this_subtask', None) is not None
                or getattr(fs, 'start_pos_of_this_subtask', None) is not None
                or getattr(fs, 'chunk_shuffle', None) is not None
                or getattr(fs, '_global_index_result', None) is not None
                or getattr(fs, 'deletion_vectors_enabled', False)
                or getattr(fs, 'data_evolution', False)
                or getattr(fs, 'only_read_real_buckets', False)):
            return False
        loader = getattr(
            getattr(self.table, 'catalog_environment', None),
            'catalog_loader',
            None,
        )
        context_fn = getattr(loader, 'context', None)
        if not callable(context_fn):
            return False
        from pypaimon.read.native_plan import _catalog_metastore
        if _catalog_metastore(loader) is None:
            return False
        context = context_fn()
        catalog_options = getattr(context, 'options', None)
        if catalog_options is None:
            return False
        if any(getattr(context, attr, None) is not None for attr in (
                'hadoop_conf', 'prefer_io_loader', 'fallback_io_loader')):
            return False
        database_name = self.table.identifier.get_database_name()
        if not database_name or database_name == UNKNOWN_DATABASE or '.' in database_name:
            return False
        if self.table.options.query_auth_enabled \
                or self.table.options.merge_engine() == 'first-row' \
                or self.table.current_branch() != 'main':
            return False
        # Empty trimmed PK (PK == partition key): native skips merge -> duplicate/stale rows.
        if getattr(self.table, 'is_primary_key_table', False) \
                and not self.table.trimmed_primary_keys:
            return False
        # Dynamic-bucket / cross-partition PK: Rust parity unconfirmed -> fall back.
        from pypaimon.table.bucket_mode import BucketMode
        if self.table.bucket_mode() in (BucketMode.HASH_DYNAMIC, BucketMode.CROSS_PARTITION):
            return False
        # copy() overrides Rust can't see (e.g. removed scan.snapshot-id) -> fall back.
        overrides = set(getattr(self.table, '_applied_dynamic_options', {}) or {})
        if overrides - _NATIVE_FORWARDED_OPTIONS:
            return False
        from pypaimon.snapshot.time_travel_util import SCAN_KEYS
        options = self.table.options.options
        if any(options.contains_key(k) for k in SCAN_KEYS):
            return False
        return not options.contains(CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP)

    def _try_native_plan(self) -> Optional[Plan]:
        """Plan via pypaimon_rust, then drop partitions the predicate rejects.

        The predicate is not pushed to the native planner, so this may read more
        files; the reader's row filter/limit still apply, so results match. Return
        None when Rust finds no splits so the caller can use the matching Python
        fallback (with scan stats when requested).
        """
        from pypaimon.read.native_plan import native_plan

        try:
            splits = native_plan(self.table)
        except Exception as e:
            # Any native construction/planning failure (e.g. unsupported scheme) -> fall back.
            logger.warning(
                "Native plan failed, falling back to the Python scanner: %s", e)
            return None
        if not splits:
            return None
        snapshot_id = splits[0].snapshot_id
        partition_predicate = self.file_scanner.partition_key_predicate
        if partition_predicate is not None:
            splits = [s for s in splits
                      if getattr(s, 'partition', None) is None
                      or partition_predicate.test(s.partition)]
        return Plan(splits, snapshot_id=snapshot_id)

    def plan_for_write(self) -> Plan:
        if self.__auth_query() is not None:
            raise TableNoPermissionException(self.table.identifier)
        return self.file_scanner.scan()

    def __auth_query(self):
        return resolve_auth_result(self._query_auth_fn, self._read_type)

    def scan_with_stats(self) -> Tuple[Plan, Optional[ScanStats]]:
        """Run :meth:`plan` while recording manifest / pruning counters.

        Only used by :meth:`ReadBuilder.explain`; the regular read path
        keeps going through :meth:`plan`. Native planning is not tracked, so
        stats is None on the native path -- explain reflects the real plan and
        marks the pruning funnel as untracked.
        """
        auth_result = self.__auth_query()
        if (auth_result is None and self.table.options.native_plan_enabled()
                and self._native_plan_supported()):
            native = self._try_native_plan()
            if native is not None:
                return native, None
        if auth_result is not None:
            prune_scanner_by_auth(self.table, self.file_scanner, auth_result)
        plan, stats = self.file_scanner.scan_with_stats()
        return wrap_plan_with_auth(auth_result, plan), stats

    def _create_file_scanner(self) -> FileScanner:
        options = self.table.options.options
        snapshot_manager = self.table.snapshot_manager()
        manifest_list_manager = ManifestListManager(self.table)

        self._validate_scan_mode()

        from pypaimon.snapshot.time_travel_util import TimeTravelUtil, SCAN_KEYS
        has_time_travel = any(options.contains_key(key) for key in SCAN_KEYS)
        has_incremental = options.contains(CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP)

        if has_incremental and has_time_travel:
            raise ValueError(
                "incremental-between-timestamp cannot be used together with "
                "point-in-time scan options: %s" % SCAN_KEYS
            )

        if has_incremental:
            ts = options.get(CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP).split(",")
            if len(ts) != 2:
                raise ValueError(
                    "The incremental-between-timestamp must specific start(exclusive) and end timestamp. But is: " +
                    options.get(CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP))
            earliest_snapshot = snapshot_manager.try_get_earliest_snapshot()
            latest_snapshot = snapshot_manager.get_latest_snapshot()
            if earliest_snapshot is None or latest_snapshot is None:
                return FileScanner(
                    self.table,
                    lambda: ([], None),
                    partition_predicate=self.partition_predicate,
                )
            start_timestamp = int(ts[0])
            end_timestamp = int(ts[1])
            if start_timestamp >= end_timestamp:
                raise ValueError(
                    "Ending timestamp %s should be >= starting timestamp %s." % (end_timestamp, start_timestamp))
            if (start_timestamp == end_timestamp or start_timestamp > latest_snapshot.time_millis
                    or end_timestamp < earliest_snapshot.time_millis):
                return FileScanner(
                    self.table,
                    lambda: ([], None),
                    partition_predicate=self.partition_predicate,
                )

            starting_snapshot = snapshot_manager.earlier_or_equal_time_mills(start_timestamp)
            earliest_snapshot = snapshot_manager.try_get_earliest_snapshot()

            # If earliest_snapshot.time_millis > start_timestamp we should include the earliest_snapshot
            if starting_snapshot is None or (earliest_snapshot and earliest_snapshot.time_millis > start_timestamp):
                start_id = earliest_snapshot.id - 1 if earliest_snapshot else -1
            else:
                start_id = starting_snapshot.id

            end_snapshot = snapshot_manager.earlier_or_equal_time_mills(end_timestamp)
            latest_snapshot = snapshot_manager.get_latest_snapshot()
            end_id = end_snapshot.id if end_snapshot else (latest_snapshot.id if latest_snapshot else -1)

            def incremental_manifest():
                snapshots_in_range = []
                end_snapshot = snapshot_manager.get_snapshot_by_id(end_id) if end_id >= 1 else None
                for snapshot_id in range(start_id + 1, end_id + 1):
                    snapshot = snapshot_manager.get_snapshot_by_id(snapshot_id)
                    end_snapshot = snapshot
                    if snapshot.commit_kind == "APPEND":
                        snapshots_in_range.append(snapshot)

                manifests = []

                for snapshot in snapshots_in_range:
                    manifest_files = manifest_list_manager.read_delta(snapshot)
                    manifests.extend(manifest_files)
                return manifests, end_snapshot

            return FileScanner(
                self.table,
                incremental_manifest,
                self.predicate,
                self.limit,
                partition_predicate=self.partition_predicate,
            )

        if has_time_travel:
            def time_travel_manifest_scanner():
                snapshot = TimeTravelUtil.try_travel_to_snapshot(
                    options, self.table.tag_manager(), snapshot_manager
                )
                if snapshot is None:
                    raise ValueError(
                        "Could not resolve time travel snapshot from scan options."
                    )
                return manifest_list_manager.read_all(snapshot), snapshot

            return FileScanner(
                self.table,
                time_travel_manifest_scanner,
                self.predicate,
                self.limit,
                partition_predicate=self.partition_predicate,
            )

        def all_manifests():
            snapshot = snapshot_manager.get_latest_snapshot()
            return manifest_list_manager.read_all(snapshot), snapshot

        return FileScanner(
            self.table,
            all_manifests,
            self.predicate,
            self.limit,
            partition_predicate=self.partition_predicate,
        )

    def with_shard(self, idx_of_this_subtask, number_of_para_subtasks) -> 'TableScan':
        self.file_scanner.with_shard(idx_of_this_subtask, number_of_para_subtasks)
        return self

    def with_slice(self, start_pos, end_pos) -> 'TableScan':
        self.file_scanner.with_slice(start_pos, end_pos)
        return self

    def with_global_index_result(self, result) -> 'TableScan':
        self.file_scanner.with_global_index_result(result)
        return self

    def with_chunk_shuffle(self, seed: int, chunk_size: int) -> 'TableScan':
        self.file_scanner.with_chunk_shuffle(seed, chunk_size)
        return self

    def _validate_scan_mode(self):
        """Validate scan.mode against companion options using a whitelist approach.

        Each StartupMode declares exactly which scan keys are allowed. Any
        scan key present but not in the whitelist for the resolved mode is
        rejected. This matches Java's SchemaValidation mutual-exclusion matrix.
        """
        from pypaimon.common.options.core_options import StartupMode

        core_options = self.table.options
        mode = core_options.startup_mode()
        options = core_options.options

        has_snapshot_id = options.contains(CoreOptions.SCAN_SNAPSHOT_ID)
        has_tag_name = options.contains(CoreOptions.SCAN_TAG_NAME)
        has_watermark = options.contains(CoreOptions.SCAN_WATERMARK)
        has_timestamp_millis = options.contains(CoreOptions.SCAN_TIMESTAMP_MILLIS)
        has_timestamp = options.contains(CoreOptions.SCAN_TIMESTAMP)
        has_incremental = options.contains(CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP)
        has_file_creation_time = options.contains(CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS)
        has_creation_time = options.contains(CoreOptions.SCAN_CREATION_TIME_MILLIS)

        present_keys = []
        if has_snapshot_id:
            present_keys.append(CoreOptions.SCAN_SNAPSHOT_ID.key())
        if has_tag_name:
            present_keys.append(CoreOptions.SCAN_TAG_NAME.key())
        if has_watermark:
            present_keys.append(CoreOptions.SCAN_WATERMARK.key())
        if has_timestamp_millis:
            present_keys.append(CoreOptions.SCAN_TIMESTAMP_MILLIS.key())
        if has_timestamp:
            present_keys.append(CoreOptions.SCAN_TIMESTAMP.key())
        if has_incremental:
            present_keys.append(CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP.key())
        if has_file_creation_time:
            present_keys.append(CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS.key())
        if has_creation_time:
            present_keys.append(CoreOptions.SCAN_CREATION_TIME_MILLIS.key())

        # scan.timestamp-millis and scan.timestamp are mutually exclusive
        if has_timestamp_millis and has_timestamp:
            raise ValueError(
                "scan.timestamp-millis and scan.timestamp cannot both be set."
            )

        # Define allowed companion keys per mode
        if mode == StartupMode.FROM_TIMESTAMP:
            allowed = {
                CoreOptions.SCAN_TIMESTAMP_MILLIS.key(),
                CoreOptions.SCAN_TIMESTAMP.key(),
            }
            if not (has_timestamp_millis or has_timestamp):
                raise ValueError(
                    "scan.mode is 'from-timestamp' but neither "
                    "scan.timestamp-millis nor scan.timestamp is set."
                )
        elif mode == StartupMode.FROM_SNAPSHOT_FULL:
            allowed = {CoreOptions.SCAN_SNAPSHOT_ID.key()}
            if not has_snapshot_id:
                raise ValueError(
                    "scan.mode is 'from-snapshot-full' but scan.snapshot-id is not set."
                )
        elif mode == StartupMode.FROM_SNAPSHOT:
            allowed = {
                CoreOptions.SCAN_SNAPSHOT_ID.key(),
                CoreOptions.SCAN_TAG_NAME.key(),
                CoreOptions.SCAN_WATERMARK.key(),
            }
            if not (has_snapshot_id or has_tag_name or has_watermark):
                raise ValueError(
                    "scan.mode is 'from-snapshot' but none of "
                    "scan.snapshot-id, scan.tag-name, or scan.watermark is set."
                )
        elif mode == StartupMode.INCREMENTAL:
            allowed = {CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP.key()}
            if not has_incremental:
                raise ValueError(
                    "scan.mode is 'incremental' but "
                    "incremental-between-timestamp is not set."
                )
        elif mode in (StartupMode.LATEST_FULL, StartupMode.LATEST):
            allowed = set()
        elif mode in (StartupMode.COMPACTED_FULL,
                      StartupMode.FROM_CREATION_TIMESTAMP,
                      StartupMode.FROM_FILE_CREATION_TIME):
            raise ValueError(
                f"scan.mode '{mode.value}' is not yet supported in pypaimon."
            )
        else:
            allowed = set()

        # Reject any scan key that's not in the whitelist for this mode
        disallowed = [k for k in present_keys if k not in allowed]
        if disallowed:
            raise ValueError(
                f"scan.mode '{mode.value}' conflicts with: {disallowed}. "
                f"Only {sorted(allowed) if allowed else 'no scan keys'} "
                f"are allowed for this mode."
            )


def prune_scanner_by_auth(table, scanner, auth_result):
    if not auth_result.filter:
        return
    partition_preds, has_non_partition = __split_auth_filter(table, auth_result)
    if partition_preds:
        combined = PredicateBuilder.and_predicates(partition_preds)
        scanner.auth_partition_predicate = combined
    if has_non_partition:
        scanner.auth_has_non_partition_filter = True


def __split_auth_filter(table, auth_result):
    partition_keys = list(table.partition_keys or [])
    if not partition_keys:
        return [], bool(auth_result.filter)

    partition_preds = []
    has_non_partition = False
    partition_key_set = set(partition_keys)
    partition_index_map = {name: i for i, name in enumerate(partition_keys)}

    for json_str in (auth_result.filter or []):
        pred = __try_parse_partition_predicate(table, json_str, partition_key_set, partition_index_map)
        if pred is not None:
            partition_preds.append(pred)
        else:
            has_non_partition = True
    return partition_preds, has_non_partition


def __try_parse_partition_predicate(table, json_str, partition_keys, partition_index_map):
    data = _json.loads(json_str)
    if data is None or data.get("kind") != "LEAF":
        return None
    transform = data.get("transform", {})
    if transform.get("name") != "FIELD_REF":
        return None
    field_name = transform.get("fieldRef", {}).get("name")
    if field_name is None or field_name not in partition_keys:
        return None
    field_index = partition_index_map.get(field_name)
    if field_index is None:
        return None

    partition_field_type = None
    for f in table.fields:
        if f.name == field_name:
            partition_field_type = getattr(f.type, 'type', '')
            break
    base_type = partition_field_type.split('(')[0] if partition_field_type else ''
    safe_types = {'INT', 'BIGINT', 'SMALLINT', 'TINYINT', 'STRING', 'VARCHAR', 'CHAR'}
    if base_type not in safe_types:
        return None

    function = data.get("function", "")
    literals = data.get("literals", [])
    method_map = {
        "EQUAL": "equal", "NOT_EQUAL": "notEqual",
        "LESS_THAN": "lessThan", "LESS_OR_EQUAL": "lessOrEqual",
        "GREATER_THAN": "greaterThan", "GREATER_OR_EQUAL": "greaterOrEqual",
        "IS_NULL": "isNull", "IS_NOT_NULL": "isNotNull",
        "IN": "in", "NOT_IN": "notIn",
    }
    method = method_map.get(function)
    if method is None:
        return None
    return Predicate(method=method, index=field_index, field=field_name, literals=literals)
