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

import re

from pypaimon.common.options.core_options import CoreOptions, ChangelogProducer, IncrementalBetweenScanMode, StartupMode
from pypaimon.catalog.catalog_exception import TableNoPermissionException
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.execution.python.read.auth_support import prune_scanner_by_auth
from pypaimon.read.query_auth_split import wrap_plan_with_auth
from pypaimon.read.auth_support import authorization_columns
from pypaimon.read.push_down_utils import predicate_field_names
from pypaimon.read.scan_config import validate_scan_options
from pypaimon.execution.python.read.scanner.file_scanner import FileScanner
from pypaimon.utils.range import Range


def _resolve_incremental_snapshot_range(table):
    """Resolve Python scan boundaries only after selecting this backend."""
    window = validate_scan_options(table)
    if table.options.options.contains(CoreOptions.INCREMENTAL_BETWEEN):
        return _resolve_incremental_ids(table)
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


def _resolve_incremental_ids(table):
    start, end = table.options.incremental_between()
    tag_manager = table.tag_manager()
    start_tag, end_tag = tag_manager.get(start), tag_manager.get(end)
    if start_tag is not None and end_tag is not None:
        start_id, end_id = start_tag.id, end_tag.id
        if not table.options.incremental_between_tag_to_snapshot():
            if end_id < start_id:
                raise ValueError(
                    "Tag end %s with snapshot id %s should be >= tag start %s with snapshot id %s"
                    % (end, end_id, start, start_id))
            if start_id == end_id:
                return None
            raise NotImplementedError(
                "Incremental diff between tags is not supported in pypaimon. "
                "Set incremental-between-tag-to-snapshot=true to scan snapshot deltas or changelogs.")
        if table.options.incremental_between_scan_mode() == IncrementalBetweenScanMode.DIFF:
            raise NotImplementedError("Unsupported incremental scan mode DIFF")
    else:
        try:
            if any(re.fullmatch(r'[+-]?[0-9]+', value) is None for value in (start, end)):
                raise ValueError()
            start_id, end_id = int(start), int(end)
            if not all(-(1 << 63) <= value < (1 << 63) for value in (start_id, end_id)):
                raise ValueError()
        except ValueError:
            raise ValueError(
                "Didn't find two tags for start '%s' and end '%s', and they are not two snapshot Ids. "
                "Please set two tags or two snapshot Ids." % (start, end))
        if end_id < start_id:
            raise ValueError("Ending snapshotId should >= starting snapshotId %s. [%s]" % (end_id, start_id))
        if table.snapshot_manager().get_latest_snapshot() is None or start_id == end_id:
            return None

    snapshot_manager = table.snapshot_manager()
    latest_id = snapshot_manager.get_latest_snapshot().id
    earliest_id = snapshot_manager.try_get_earliest_snapshot(latest_id).id
    if start_id < earliest_id - 1 or end_id > latest_id:
        raise ValueError(
            "The specified scan snapshotId range [%s, %s] is out of available snapshotId range [%s, %s]."
            % (start_id, end_id, earliest_id, latest_id))
    return start_id, end_id


class PythonTableScan:
    """Plan with Python's file scanner and the public scan configuration."""

    _is_native = False

    def __init__(self, scan):
        self._scan = scan
        self._file_scanner_cache = None
        self._configuration = None

    def _create_file_scanner(self) -> FileScanner:
        scan = self._scan
        options = scan.table.options.options
        snapshot_manager = scan.table.snapshot_manager()
        manifest_list_manager = ManifestListManager(scan.table)

        from pypaimon.snapshot.time_travel_util import TimeTravelUtil, SCAN_KEYS
        has_time_travel = any(options.contains_key(key) for key in SCAN_KEYS)
        has_incremental = scan.table.options.startup_mode() == StartupMode.INCREMENTAL

        if has_incremental:
            snapshot_range = _resolve_incremental_snapshot_range(scan.table)
            if snapshot_range is None:
                return FileScanner(
                    scan.table,
                    lambda: ([], None),
                    partition_predicate=scan.partition_predicate,
                )
            start_id, end_id = snapshot_range
            mode = scan.table.options.incremental_between_scan_mode()
            if mode == IncrementalBetweenScanMode.DIFF:
                raise NotImplementedError("incremental-between-scan-mode 'diff' is not supported in pypaimon.")
            changelog = (mode == IncrementalBetweenScanMode.CHANGELOG
                         or (mode == IncrementalBetweenScanMode.AUTO
                             and scan.table.options.changelog_producer() != ChangelogProducer.NONE))

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
        public_scanner = getattr(scan, '_file_scanner', None)
        from pypaimon.read.file_scanner import FileScanner as PublicFileScanner
        if public_scanner is not None and not isinstance(public_scanner, PublicFileScanner):
            self._file_scanner_cache = public_scanner
            return public_scanner
        request = scan._scan_request()
        scanner = self._file_scanner_cache
        # Mutable public range/configuration containers can change without a
        # facade setter. Refresh this backend's scanner from the captured values
        # while retaining Python selection and applying authorization afterward.
        if scanner is None or self._configuration != request:
            scanner = self._create_file_scanner()
            self._apply_file_scanner_request(scanner, request, public_scanner)
            self._file_scanner_cache = scanner
            self._configuration = request
        return scanner

    @staticmethod
    def _apply_file_scanner_request(scanner, request, configuration):
        scanner.idx_of_this_subtask = scanner.number_of_para_subtasks = None
        scanner.start_pos_of_this_subtask = scanner.end_pos_of_this_subtask = None
        scanner._global_index_result = scanner._row_ranges = scanner.chunk_shuffle = None
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
        if configuration is not None:
            PythonTableScan._configure_file_scanner(scanner, configuration)

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
                setattr(scanner, name, PythonTableScan._file_scanner_hook(configuration, name))
            else:
                vars(scanner).pop(name, None)

    @staticmethod
    def _file_scanner_hook(configuration, name):
        from pypaimon.read.read_dispatch import file_scanner_hook
        hook = getattr(configuration, name)

        def call():
            with file_scanner_hook(configuration, name):
                return hook()
        return call

    def _scanner(self, auth_result):
        scan = self._scan
        scanner = self._file_scanner()
        if scan._query_auth_fn is not None:
            prune_scanner_by_auth(scan.table, scanner, auth_result)
        return scanner

    def _run_file_scan(self, method, auth_result, *, base=False, all_files=False):
        from pypaimon.read.read_dispatch import file_scanner_execution
        scan = self._scan
        scanner = self._scanner(auth_result)
        configuration = scan._file_scanner
        from pypaimon.read.file_scanner import FileScanner as PublicFileScanner
        captured_configuration = configuration is None or isinstance(configuration, PublicFileScanner)
        unrestricted = all_files or method == 'plan_files'

        def auth_scope(request):
            if scan._query_auth_fn is None or unrestricted or not captured_configuration:
                return None
            columns = authorization_columns(scan.table, request.read_type,
                                            request.read_predicate, request.partition_filter)
            return (tuple(columns) if columns is not None else None,
                    frozenset(predicate_field_names(request.partition_filter))
                    if request.partition_filter is not None else None)

        authorized_scope = auth_scope(scan._scan_request() if captured_configuration else None)

        def run(method, base=False):
            nonlocal auth_result, authorized_scope
            request = scan._scan_request() if captured_configuration else None
            scope = auth_scope(request)
            if scope != authorized_scope:
                auth_result = scan._TableScan__auth_query(all_columns=unrestricted)
                authorized_scope = scope
                if unrestricted and auth_result is not None:
                    raise TableNoPermissionException(scan.table.identifier)
            if captured_configuration and configuration is not None and self._configuration != request:
                # Nested hooks share the scanner already executing scan()/stats;
                # replacing it would lose its snapshot and active counters.
                self._apply_file_scanner_request(scanner, request, configuration)
                self._configuration = request
            if scan._query_auth_fn is not None:
                prune_scanner_by_auth(scan.table, scanner, auth_result)
            skip_level0 = scanner.skip_level0 if all_files else None
            try:
                if all_files:
                    scanner.skip_level0 = False
                with file_scanner_execution(configuration, lambda method: run(method, base=True)):
                    result = (getattr(FileScanner, method)(scanner) if base else getattr(scanner, method)())
                if configuration is not None and method == 'plan_files':
                    configuration._scanned_snapshot = scanner._scanned_snapshot
                    configuration._scanned_snapshot_id = scanner._scanned_snapshot_id
                return result
            finally:
                if all_files:
                    scanner.skip_level0 = skip_level0

        result = run(method, base)
        if method == 'scan':
            return wrap_plan_with_auth(auth_result, result)
        if method == 'scan_with_stats':
            plan, stats = result
            return wrap_plan_with_auth(auth_result, plan), stats
        return result

    def plan(self, auth_result=None):
        return self._run_file_scan('scan', auth_result)

    def plan_all(self):
        return self._run_file_scan('scan', None, all_files=True)

    def scan_with_stats(self, auth_result=None):
        return self._run_file_scan('scan_with_stats', auth_result)
