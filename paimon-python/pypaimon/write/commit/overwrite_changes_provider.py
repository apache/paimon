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

from typing import List, Optional

from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.read.scanner.file_scanner import FileScanner
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.table.row.generic_row import GenericRow


class OverwriteChangesProvider:
    """Builds the commit entries (DELETE existing + ADD new) for an OVERWRITE,
    caching the existing files of the target partitions across commit retries
    to avoid repeated full scans.

    On retry, if the latest snapshot advanced, the cache is reused only when the
    snapshots in between are all APPEND and have not touched the target
    partitions; otherwise it is rebuilt by a full scan. A whole-table overwrite
    (``partition_filter is None``) always rebuilds. Mirrors Java
    ``OverwriteChangesProvider`` (#7894).
    """

    def __init__(self, table, manifest_list_manager, snapshot_manager,
                 partition_filter, commit_messages):
        self.table = table
        self.manifest_list_manager = manifest_list_manager
        self.snapshot_manager = snapshot_manager
        self.partition_filter = partition_filter
        self.commit_messages = commit_messages

        self._cached_snapshot: Optional[Snapshot] = None
        self._cached_entries: List[ManifestEntry] = []

        # Counters for tests / observability (mirrors Java @VisibleForTesting).
        self.full_scan_count = 0
        self.delta_probe_count = 0

    def provide(self, latest_snapshot: Optional[Snapshot]) -> List[ManifestEntry]:
        if latest_snapshot is None:
            # Empty table: nothing existing to delete, just add the new files.
            return self._build_result([])

        if self._cached_snapshot is None:
            self._cached_entries = self._full_scan(latest_snapshot)
            self._cached_snapshot = latest_snapshot
        elif self._cached_snapshot.id > latest_snapshot.id:
            raise RuntimeError(
                f"Cached snapshot id {self._cached_snapshot.id} is greater than "
                f"latest snapshot id {latest_snapshot.id}")
        elif self._cached_snapshot.id < latest_snapshot.id:
            if not self._can_use_cache(latest_snapshot):
                self._cached_entries = self._full_scan(latest_snapshot)
            self._cached_snapshot = latest_snapshot
        # cached_snapshot.id == latest_snapshot.id -> reuse cache as-is

        return self._build_result(self._cached_entries)

    def _full_scan(self, latest_snapshot: Snapshot) -> List[ManifestEntry]:
        self.full_scan_count += 1
        return (FileScanner(self.table, lambda: ([], None),
                            partition_predicate=self.partition_filter)
                .read_manifest_entries(self.manifest_list_manager.read_all(latest_snapshot)))

    def _can_use_cache(self, latest_snapshot: Snapshot) -> bool:
        if self.partition_filter is None:
            # Whole-table overwrite: any concurrent commit touches the target,
            # so skip the delta probe and force a full scan.
            return False
        for snapshot_id in range(self._cached_snapshot.id + 1, latest_snapshot.id + 1):
            self.delta_probe_count += 1
            try:
                snapshot = self.snapshot_manager.get_snapshot_by_id(snapshot_id)
                if snapshot is None:
                    return False
                if snapshot.commit_kind != "APPEND":
                    # Only APPEND snapshots produce a reliable DELTA manifest for
                    # probing; other kinds may rewrite/reorganize manifests.
                    return False
                if self._delta_touches_target(snapshot):
                    return False
            except Exception:
                # e.g. the snapshot is being expired; a full scan is always safe.
                return False
        return True

    def _delta_touches_target(self, snapshot: Snapshot) -> bool:
        delta_manifests = self.manifest_list_manager.read_delta(snapshot)
        if not delta_manifests:
            return False
        mfm = ManifestFileManager(self.table)
        for mf in delta_manifests:
            for entry in mfm.read(mf.file_name):
                # _can_use_cache already returned False for a null filter, so
                # partition_filter is always set here.
                if self.partition_filter.test(entry.partition):
                    return True
        return False

    def _build_result(self, existing_entries: List[ManifestEntry]) -> List[ManifestEntry]:
        entries = []
        # Existing files of the target partitions become DELETE entries. Build
        # fresh entries so the cached (kind=0) entries are never mutated.
        for entry in existing_entries:
            entries.append(ManifestEntry(
                kind=1,
                partition=entry.partition,
                bucket=entry.bucket,
                total_buckets=entry.total_buckets,
                file=entry.file,
            ))
        # New files being written by this overwrite.
        for msg in self.commit_messages:
            partition = GenericRow(list(msg.partition), self.table.partition_keys_fields)
            for file in msg.new_files:
                entries.append(ManifestEntry(
                    kind=0,
                    partition=partition,
                    bucket=msg.bucket,
                    total_buckets=self.table.total_buckets,
                    file=file,
                ))
        return entries
