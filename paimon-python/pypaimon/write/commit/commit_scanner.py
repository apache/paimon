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

"""
Manifest entries scanner for commit operations.
"""
from typing import Optional, List

from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.manifest.index_manifest_file import IndexManifestFile
from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.read.scanner.file_scanner import FileScanner
from pypaimon.snapshot.snapshot import Snapshot


class CommitScanner:
    """Manifest entries scanner for commit operations.

    This class provides methods to scan manifest entries for commit operations
    """

    def __init__(self, table, manifest_list_manager: ManifestListManager):
        """Initialize CommitScanner.

        Args:
            table: The FileStoreTable instance.
            manifest_list_manager: Manager for reading manifest lists.
        """
        self.table = table
        self.manifest_list_manager = manifest_list_manager

    def read_all_entries_from_changed_partitions(self,
                                                 latest_snapshot: Optional[Snapshot],
                                                 commit_entries: List[ManifestEntry],
                                                 index_entries=None):
        """Read all entries from the latest snapshot for partitions that are changed.

        Builds a partition predicate from delta entries and passes it to FileScanner,
        so that manifest files and entries are filtered during reading rather than
        after a full scan.

        Args:
            latest_snapshot: The latest snapshot to read entries from.
            commit_entries: The delta entries being committed, used to determine
                which partitions have changed.

        Returns:
            List of ManifestEntry from the latest snapshot for changed partitions.
        """
        if latest_snapshot is None:
            return []

        partition_filter = self._build_partition_filter_from_changes(
            commit_entries, index_entries)

        all_manifests = self.manifest_list_manager.read_all(latest_snapshot)
        return FileScanner(
            self.table, lambda: ([], None), partition_predicate=partition_filter
        ).read_manifest_entries(all_manifests)

    def read_incremental_entries_from_changed_partitions(self,
                                                         snapshot: Snapshot,
                                                         commit_entries: List[ManifestEntry],
                                                         index_entries=None):
        """Read incremental manifest entries from a snapshot's delta manifest list.

        Builds a partition predicate from delta entries and passes it to FileScanner,
        so that manifest files and entries are filtered during reading rather than
        after a full scan.

        Args:
            snapshot: The snapshot to read incremental entries from.
            commit_entries: The delta entries being committed, used to determine
                which partitions have changed.

        Returns:
            List of ManifestEntry matching the partition filter.
        """
        delta_manifests = self.manifest_list_manager.read_delta(snapshot)
        if not delta_manifests:
            return []

        partition_filter = self._build_partition_filter_from_changes(
            commit_entries, index_entries)

        return FileScanner(
            self.table, lambda: ([], None), partition_predicate=partition_filter
        ).read_manifest_entries(delta_manifests)

    def read_incremental_raw_entries_from_changed_partitions(self, snapshot: Snapshot,
                                                             commit_entries: List[ManifestEntry],
                                                             partition_filter=None,
                                                             index_entries=None):
        """Like ``read_incremental_entries_from_changed_partitions`` but preserves
        DELETE entries (kind=1). ``partition_filter`` may be passed to avoid
        rebuilding it per call.
        """
        delta_manifests = self.manifest_list_manager.read_delta(snapshot)
        if not delta_manifests:
            return []

        if partition_filter is None:
            partition_filter = self._build_partition_filter_from_changes(
                commit_entries, index_entries)
        mfm = ManifestFileManager(self.table)
        entries = []
        for mf in delta_manifests:
            for entry in mfm.read(mf.file_name):
                if partition_filter is not None and not partition_filter.test(entry.partition):
                    continue
                entries.append(entry)
        return entries

    def read_incremental_changes(self,
                                 from_snapshot: Snapshot,
                                 to_snapshot: Snapshot,
                                 commit_entries: List[ManifestEntry],
                                 index_entries=None) -> Optional[List[ManifestEntry]]:
        """Delta entries (incl. DELETEs) in ``(from_snapshot, to_snapshot]``,
        changed-partition filtered, so a retry can reuse the prior base and read
        only the changes since. Returns None on a missing snapshot (caller then
        full-scans). Mirrors Java ``CommitScanner#readIncrementalChanges``.
        """
        snapshot_manager = self.table.snapshot_manager()
        partition_filter = self._build_partition_filter_from_changes(
            commit_entries, index_entries)
        entries = []
        for snapshot_id in range(from_snapshot.id + 1, to_snapshot.id + 1):
            snapshot = snapshot_manager.get_snapshot_by_id(snapshot_id)
            if snapshot is None:
                return None
            entries.extend(
                self.read_incremental_raw_entries_from_changed_partitions(
                    snapshot, commit_entries, partition_filter))
        return entries

    def _build_partition_filter_from_entries(self, entries: List[ManifestEntry]):
        return self._build_partition_filter_from_changes(entries)

    def _build_partition_filter_from_changes(self, entries, index_entries=None):
        """Build a partition predicate that matches all partitions present in the given entries.

        Args:
            entries: List of ManifestEntry whose partitions should be matched.
            index_entries: Optional index manifest entries whose partitions
                should be matched.

        Returns:
            A Predicate matching any of the changed partitions, or None if
            partition keys are empty.
        """
        partition_keys = self.table.partition_keys
        if not partition_keys:
            return None

        changed_partitions = set()
        for entry in entries or []:
            changed_partitions.add(tuple(entry.partition.values))
        for entry in index_entries or []:
            if self._index_entry_changes_partition(entry):
                changed_partitions.add(tuple(entry.partition.values))

        if not changed_partitions:
            return None

        predicate_builder = PredicateBuilder(self.table.partition_keys_fields)
        partition_predicates = []
        for partition_values in changed_partitions:
            sub_predicates = []
            for i, key in enumerate(partition_keys):
                if partition_values[i] is None:
                    sub_predicates.append(predicate_builder.is_null(key))
                else:
                    sub_predicates.append(predicate_builder.equal(key, partition_values[i]))
            partition_predicates.append(predicate_builder.and_predicates(sub_predicates))

        return predicate_builder.or_predicates(partition_predicates)

    @staticmethod
    def _index_entry_changes_partition(entry):
        return (entry.index_file.index_type == IndexManifestFile.DELETION_VECTORS_INDEX
                or entry.index_file.global_index_meta is not None)
