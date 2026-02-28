#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

"""Conflict detection for commit operations.

Follows the design of Java's org.apache.paimon.operation.commit.ConflictDetection.
"""

import logging

from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.file_entry import FileEntry
from pypaimon.read.scanner.file_scanner import FileScanner
from pypaimon.utils.range import Range
from pypaimon.utils.range_helper import RangeHelper

logger = logging.getLogger(__name__)


class ConflictDetection:
    """Detects conflicts between base and delta files during commit.

    Follows the design of Java's ConflictDetection class, providing
    row ID range conflict checks and row ID from snapshot conflict checks
    for Data Evolution tables.
    """

    def __init__(self, data_evolution_enabled, snapshot_manager,
                 manifest_list_manager, table):
        """Initialize ConflictDetection.

        Args:
            data_evolution_enabled: Whether data evolution feature is enabled.
            snapshot_manager: Manager for reading snapshot metadata.
            manifest_list_manager: Manager for reading manifest lists.
            table: The FileStoreTable instance.
        """
        self.data_evolution_enabled = data_evolution_enabled
        self.snapshot_manager = snapshot_manager
        self.manifest_list_manager = manifest_list_manager
        self.table = table
        self._row_id_check_from_snapshot = None

    def should_be_overwrite_commit(self):
        """Check if the commit should be treated as an overwrite commit.

        returns True if rowIdCheckFromSnapshot is set.

        Returns:
            True if the commit should be treated as OVERWRITE.
        """
        return self._row_id_check_from_snapshot is not None

    def check_conflicts(self, latest_snapshot, base_entries, delta_entries, commit_kind):
        """Run all conflict checks and return the first detected conflict.

        merges base_entries and delta_entries, then runs conflict checks
        on the merged result.

        Args:
            latest_snapshot: The latest snapshot at commit time.
            base_entries: All entries read from the latest snapshot.
            delta_entries: The delta entries being committed.
            commit_kind: The kind of commit (e.g. "APPEND", "COMPACT", "OVERWRITE").

        Returns:
            A RuntimeError if a conflict is detected, otherwise None.
        """
        all_entries = list(base_entries) + list(delta_entries)

        try:
            merged_entries = FileEntry.merge_entries(all_entries)
        except Exception as e:
            return RuntimeError(
                "File deletion conflicts detected! Give up committing. " + str(e))

        conflict = self.check_row_id_range_conflicts(commit_kind, merged_entries)
        if conflict is not None:
            return conflict

        return self.check_for_row_id_from_snapshot(latest_snapshot, delta_entries)

    def check_row_id_range_conflicts(self, commit_kind, commit_entries):
        """Check for row ID range conflicts among merged entries.

        only enabled when data evolution is active, and checks that
        overlapping row ID ranges in non-blob data files are identical.

        Args:
            commit_kind: The kind of commit (e.g. "APPEND", "COMPACT").
            commit_entries: The entries being committed.

        Returns:
            A RuntimeError if conflict is detected, otherwise None.
        """
        if not self.data_evolution_enabled:
            return None
        if self._row_id_check_from_snapshot is None and commit_kind != "COMPACT":
            return None

        entries_with_row_id = [
            entry for entry in commit_entries
            if entry.file.first_row_id is not None
        ]

        if not entries_with_row_id:
            return None

        range_helper = RangeHelper(lambda entry: entry.file.row_id_range())
        merged_groups = range_helper.merge_overlapping_ranges(entries_with_row_id)

        for group in merged_groups:
            data_files = [
                entry for entry in group
                if not DataFileMeta.is_blob_file(entry.file.file_name)
            ]
            if not range_helper.are_all_ranges_same(data_files):
                file_descriptions = [
                    "{name}(rowId={row_id}, count={count})".format(
                        name=entry.file.file_name,
                        row_id=entry.file.first_row_id,
                        count=entry.file.row_count,
                    )
                    for entry in data_files
                ]
                return RuntimeError(
                    "For Data Evolution table, multiple 'MERGE INTO' and 'COMPACT' "
                    "operations have encountered conflicts, data files: "
                    + str(file_descriptions))

        return None

    def check_for_row_id_from_snapshot(self, latest_snapshot, commit_entries):
        """Check for row ID conflicts from a specific snapshot onwards.

        collects row ID ranges from delta entries, then checks if any
        incremental changes between the check snapshot and latest snapshot
        have overlapping row ID ranges.

        Args:
            latest_snapshot: The latest snapshot at commit time.
            commit_entries: The delta entries being committed.

        Returns:
            A RuntimeError if conflict is detected, otherwise None.
        """
        if not self.data_evolution_enabled:
            return None
        if self._row_id_check_from_snapshot is None:
            return None

        changed_partitions = set()
        for entry in commit_entries:
            partition_key = tuple(entry.partition.values)
            changed_partitions.add(partition_key)

        history_id_ranges = []
        for entry in commit_entries:
            first_row_id = entry.file.first_row_id
            row_count = entry.file.row_count
            if first_row_id is not None:
                history_id_ranges.append(
                    Range(first_row_id, first_row_id + row_count - 1))

        check_snapshot = self.snapshot_manager.get_snapshot_by_id(
            self._row_id_check_from_snapshot)
        if check_snapshot is None or check_snapshot.next_row_id is None:
            raise RuntimeError(
                "Next row id cannot be null for snapshot "
                "{snapshot}.".format(snapshot=self._row_id_check_from_snapshot))
        check_next_row_id = check_snapshot.next_row_id

        for snapshot_id in range(
                self._row_id_check_from_snapshot + 1,
                latest_snapshot.id + 1):
            snapshot = self.snapshot_manager.get_snapshot_by_id(snapshot_id)
            if snapshot is None:
                continue
            if snapshot.commit_kind == "COMPACT":
                continue

            incremental_entries = self._read_incremental_entries(
                snapshot, changed_partitions)
            for entry in incremental_entries:
                file_range = entry.file.row_id_range()
                if file_range is None:
                    continue
                if file_range.from_ < check_next_row_id:
                    for history_range in history_id_ranges:
                        if history_range.overlaps(file_range):
                            print("conflict2")
                            return RuntimeError(
                                "For Data Evolution table, multiple 'MERGE INTO' "
                                "operations have encountered conflicts, updating "
                                "the same file, which can render some updates "
                                "ineffective.")

        return None

    def _read_incremental_entries(self, snapshot, partition_filter):
        """Read incremental manifest entries from a snapshot's delta manifest list.

        reads the delta manifest list and filters entries by partition.

        Args:
            snapshot: The snapshot to read incremental entries from.
            partition_filter: Set of partition tuples to filter by.

        Returns:
            List of ManifestEntry matching the partition filter.
        """
        delta_manifests = self.manifest_list_manager.read_delta(snapshot)
        if not delta_manifests:
            return []

        all_entries = FileScanner(
            self.table, lambda: [], None
        ).read_manifest_entries(delta_manifests)

        if not partition_filter:
            return all_entries

        return [
            entry for entry in all_entries
            if tuple(entry.partition.values) in partition_filter
        ]
