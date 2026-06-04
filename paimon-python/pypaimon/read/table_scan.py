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

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.predicate import Predicate

from pypaimon.read.plan import Plan
from pypaimon.read.scan_stats import ScanStats
from pypaimon.read.scanner.file_scanner import FileScanner
from pypaimon.manifest.manifest_list_manager import ManifestListManager


class TableScan:
    """Implementation of TableScan for native Python reading."""

    def __init__(
        self,
        table,
        predicate: Optional[Predicate],
        limit: Optional[int]
    ):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.predicate = predicate
        self.limit = limit
        self.file_scanner = self._create_file_scanner()

    def plan(self) -> Plan:
        return self.file_scanner.scan()

    def scan_with_stats(self) -> Tuple[Plan, ScanStats]:
        """Run :meth:`plan` while recording manifest / pruning counters.

        Only used by :meth:`ReadBuilder.explain`; the regular read path
        keeps going through :meth:`plan`.
        """
        return self.file_scanner.scan_with_stats()

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
                return FileScanner(self.table, lambda: ([], None))
            start_timestamp = int(ts[0])
            end_timestamp = int(ts[1])
            if start_timestamp >= end_timestamp:
                raise ValueError(
                    "Ending timestamp %s should be >= starting timestamp %s." % (end_timestamp, start_timestamp))
            if (start_timestamp == end_timestamp or start_timestamp > latest_snapshot.time_millis
                    or end_timestamp < earliest_snapshot.time_millis):
                return FileScanner(self.table, lambda: ([], None))

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

            return FileScanner(self.table, incremental_manifest, self.predicate, self.limit)

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
                self.limit
            )

        def all_manifests():
            snapshot = snapshot_manager.get_latest_snapshot()
            return manifest_list_manager.read_all(snapshot), snapshot

        return FileScanner(
            self.table,
            all_manifests,
            self.predicate,
            self.limit
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
