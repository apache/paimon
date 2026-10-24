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
from pypaimon.read.scan_config import validate_scan_options
from pypaimon.execution.python.read.scanner.file_scanner import FileScanner
from pypaimon.utils.range import Range


def _resolve_incremental_snapshot_range(table):
    """Resolve Python scan boundaries only after selecting this backend."""
    window = validate_scan_options(table)
    if window is None:
        return None
    start_timestamp, end_timestamp = window
    snapshot_manager = table.snapshot_manager()
    latest_snapshot = snapshot_manager.get_latest_snapshot()
    earliest_snapshot = snapshot_manager.try_get_earliest_snapshot() if latest_snapshot is not None else None
    if earliest_snapshot is None or latest_snapshot is None:
        return None
    if start_timestamp > end_timestamp:
        raise ValueError(
            "Ending timestamp %s should be >= starting timestamp %s."
            % (end_timestamp, start_timestamp))
    if (start_timestamp == end_timestamp or start_timestamp > latest_snapshot.time_millis
            or end_timestamp < earliest_snapshot.time_millis):
        return None

    starting_snapshot = snapshot_manager.earlier_or_equal_time_mills(start_timestamp)
    earliest_snapshot = snapshot_manager.try_get_earliest_snapshot()
    if starting_snapshot is None or (earliest_snapshot and earliest_snapshot.time_millis > start_timestamp):
        start_id = earliest_snapshot.id - 1 if earliest_snapshot else -1
    else:
        start_id = starting_snapshot.id
    end_snapshot = snapshot_manager.earlier_or_equal_time_mills(end_timestamp)
    latest_snapshot = snapshot_manager.get_latest_snapshot()
    end_id = end_snapshot.id if end_snapshot else (latest_snapshot.id if latest_snapshot else -1)
    return start_id, end_id


class PythonTableScan:
    """Plan with Python's file scanner and the public scan configuration."""

    _is_native = False

    def __init__(self, scan):
        self._scan = scan
        self._file_scanner_cache = None

    def _create_file_scanner(self) -> FileScanner:
        scan = self._scan
        options = scan.table.options.options
        snapshot_manager = scan.table.snapshot_manager()
        manifest_list_manager = ManifestListManager(scan.table)

        from pypaimon.snapshot.time_travel_util import TimeTravelUtil, SCAN_KEYS
        has_time_travel = any(options.contains_key(key) for key in SCAN_KEYS)
        has_incremental = options.contains(CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP)

        if has_incremental:
            snapshot_range = _resolve_incremental_snapshot_range(scan.table)
            if snapshot_range is None:
                return FileScanner(
                    scan.table,
                    lambda: ([], None),
                    partition_predicate=scan.partition_predicate,
                )
            start_id, end_id = snapshot_range
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

    def _file_scanner(self):
        """Return the private Python scanner without entering public dispatch."""
        scan = self._scan
        scanner = self._file_scanner_cache
        if scanner is None:
            public_scanner = getattr(scan, '_file_scanner', None)
            from pypaimon.read.file_scanner import FileScanner as PublicFileScanner
            if public_scanner is not None and not isinstance(public_scanner, PublicFileScanner):
                self._file_scanner_cache = public_scanner
                return public_scanner
            scanner = self._create_file_scanner()
            request = scan._scan_request()
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
            if public_scanner is not None:
                self._configure_file_scanner(scanner, public_scanner)
            self._file_scanner_cache = scanner
        return scanner

    @staticmethod
    def _configure_file_scanner(scanner, configuration):
        from pypaimon.read.file_scanner import FileScanner as PublicFileScanner
        scanner._user_predicate = configuration._user_predicate
        scanner._partition_predicate = configuration._partition_predicate
        scanner._explicit_partition_filter = configuration._explicit_partition_filter
        scanner._set_predicate(configuration.predicate)
        scanner.partition_key_predicate = configuration.partition_key_predicate
        for name in ('limit', 'skip_level0', 'only_read_real_buckets', 'is_streaming'):
            setattr(scanner, name, getattr(configuration, name))
        for name in ('scan', 'scan_with_stats', 'plan_files'):
            if (name in vars(configuration)
                    or getattr(type(configuration), name) is not getattr(PublicFileScanner, name)):
                setattr(scanner, name, getattr(configuration, name))

    def _scanner(self, auth_result):
        scan = self._scan
        scanner = self._file_scanner()
        if scan._query_auth_fn is not None:
            prune_scanner_by_auth(scan.table, scanner, auth_result)
        return scanner

    def plan(self, auth_result=None):
        return wrap_plan_with_auth(auth_result, self._scanner(auth_result).scan())

    def plan_all(self):
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
