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
from pypaimon.execution.python.read.auth_support import prune_scanner_by_auth
from pypaimon.read.query_auth_split import wrap_plan_with_auth
from pypaimon.execution.python.read.scanner.file_scanner import FileScanner


class PythonTableScan:
    """Plan with Python's file scanner and the public scan configuration."""

    def __init__(self, scan):
        self._scan = scan

    def _create_file_scanner(self) -> FileScanner:
        scan = self._scan
        options = scan.table.options.options
        snapshot_manager = scan.table.snapshot_manager()
        manifest_list_manager = ManifestListManager(scan.table)

        from pypaimon.snapshot.time_travel_util import TimeTravelUtil, SCAN_KEYS
        has_time_travel = any(options.contains_key(key) for key in SCAN_KEYS)
        has_incremental = options.contains(CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP)

        if has_incremental:
            if scan._incremental_snapshot_range is None:
                return FileScanner(
                    scan.table,
                    lambda: ([], None),
                    partition_predicate=scan.partition_predicate,
                )
            start_id, end_id = scan._incremental_snapshot_range
            # Java's AUTO mode uses physical changelogs when a producer is
            # configured, and APPEND deltas otherwise.
            changelog = scan.table.options.changelog_producer() != ChangelogProducer.NONE

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
                scan.table,
                incremental_manifest,
                scan.predicate,
                scan.limit,
                partition_predicate=scan.partition_predicate,
                skip_level0=False,
                is_streaming=True,
            )

        if has_time_travel:
            def time_travel_manifest_scanner():
                snapshot = TimeTravelUtil.resolve_snapshot(scan.table)
                if snapshot is None:
                    raise ValueError(
                        "Could not resolve time travel snapshot from scan options."
                    )
                return manifest_list_manager.read_all(snapshot), snapshot

            return FileScanner(
                scan.table,
                time_travel_manifest_scanner,
                scan.predicate,
                scan.limit,
                partition_predicate=scan.partition_predicate,
                skip_level0=scan.table.options.batch_scan_skip_level0(),
            )

        def all_manifests():
            snapshot = TimeTravelUtil.resolve_snapshot(scan.table)
            return manifest_list_manager.read_all(snapshot), snapshot

        return FileScanner(
            scan.table,
            all_manifests,
            scan.predicate,
            scan.limit,
            partition_predicate=scan.partition_predicate,
            skip_level0=scan.table.options.batch_scan_skip_level0(),
        )

    def _scanner(self, auth_result):
        scan = self._scan
        scanner = scan.file_scanner
        if scan._query_auth_fn is not None:
            prune_scanner_by_auth(scan.table, scanner, auth_result)
        return scanner

    def plan(self, auth_result=None):
        return wrap_plan_with_auth(auth_result, self._scanner(auth_result).scan())

    def plan_all_files(self):
        scanner = self._scanner(None)
        # Writer restore/overwrite must see L0 even when batch reads hide it.
        skip_level0 = scanner.skip_level0
        try:
            scanner.skip_level0 = False
            return scanner.scan()
        finally:
            scanner.skip_level0 = skip_level0

    def scan_with_stats(self, auth_result=None):
        plan, stats = self._scanner(auth_result).scan_with_stats()
        return wrap_plan_with_auth(auth_result, plan), stats
