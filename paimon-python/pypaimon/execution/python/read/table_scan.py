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

"""Python file scanner construction for the public TableScan."""

from pypaimon.common.options.core_options import CoreOptions, ChangelogProducer
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.read.scan_config import _parse_incremental_timestamp_window
from pypaimon.execution.python.read.scanner.file_scanner import FileScanner


class PythonTableScanMixin:
    def _create_file_scanner(self) -> FileScanner:
        self._incremental_snapshot_range = None
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
            start_timestamp, end_timestamp = _parse_incremental_timestamp_window(
                options.get(CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP))
            latest_snapshot = snapshot_manager.get_latest_snapshot()
            earliest_snapshot = snapshot_manager.try_get_earliest_snapshot() if latest_snapshot is not None else None
            if earliest_snapshot is None or latest_snapshot is None:
                return FileScanner(
                    self.table,
                    lambda: ([], None),
                    partition_predicate=self.partition_predicate,
                )
            if start_timestamp > end_timestamp:
                raise ValueError(
                    "Ending timestamp %s should be >= starting timestamp %s."
                    % (end_timestamp, start_timestamp))
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
            self._incremental_snapshot_range = (start_id, end_id)
            # Java's AUTO mode uses physical changelogs when a producer is
            # configured, and APPEND deltas otherwise.
            changelog = self.table.options.changelog_producer() != ChangelogProducer.NONE

            def incremental_manifest():
                snapshots_in_range = []
                end_snapshot = snapshot_manager.get_snapshot_by_id(end_id) if end_id >= 1 else None
                for snapshot_id in range(start_id + 1, end_id + 1):
                    snapshot = snapshot_manager.get_snapshot_by_id(snapshot_id)
                    end_snapshot = snapshot
                    if (snapshot.commit_kind != "OVERWRITE" and snapshot.changelog_manifest_list
                            if changelog else snapshot.commit_kind == "APPEND"):
                        snapshots_in_range.append(snapshot)

                manifests = []

                for snapshot in snapshots_in_range:
                    manifest_files = (manifest_list_manager.read_changelog(snapshot) if changelog else
                                      manifest_list_manager.read_delta(snapshot))
                    manifests.extend(manifest_files)
                return manifests, end_snapshot

            return FileScanner(
                self.table,
                incremental_manifest,
                self.predicate,
                self.limit,
                partition_predicate=self.partition_predicate,
                skip_level0=False,
                is_streaming=True,
            )

        if has_time_travel:
            def time_travel_manifest_scanner():
                snapshot = TimeTravelUtil.resolve_snapshot(self.table)
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
                skip_level0=self.table.options.batch_scan_skip_level0(),
            )

        def all_manifests():
            snapshot = TimeTravelUtil.resolve_snapshot(self.table)
            return manifest_list_manager.read_all(snapshot), snapshot

        return FileScanner(
            self.table,
            all_manifests,
            self.predicate,
            self.limit,
            partition_predicate=self.partition_predicate,
            skip_level0=self.table.options.batch_scan_skip_level0(),
        )
