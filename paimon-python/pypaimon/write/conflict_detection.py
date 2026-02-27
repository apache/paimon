################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

"""Conflict detection for commit operations.

Follows the design of Java's org.apache.paimon.operation.commit.ConflictDetection.
"""

import logging
from typing import List, Optional, Set

from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.read.scanner.file_scanner import FileScanner
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.utils.range import Range

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

    def set_row_id_check_from_snapshot(self, row_id_check_from_snapshot):
        """Set the snapshot ID from which to check row ID conflicts."""
        self._row_id_check_from_snapshot = row_id_check_from_snapshot

    def should_be_overwrite_commit(self, commit_entries):
        """Check if the commit should be treated as an overwrite commit.

        Follows Java ConflictDetection.shouldBeOverwriteCommit logic:
        returns True if any entry is a DELETE (kind=1), or if
        rowIdCheckFromSnapshot is set.

        Args:
            commit_entries: The entries being committed.

        Returns:
            True if the commit should be treated as OVERWRITE.
        """
        for entry in commit_entries:
            if entry.kind == 1:
                return True
        print(self._row_id_check_from_snapshot)
        return self._row_id_check_from_snapshot is not None

    def check_conflicts(self, latest_snapshot, base_entries, delta_entries, commit_kind):
        """Run all conflict checks and return the first detected conflict.

        Follows Java ConflictDetection.checkConflicts logic:
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
            merged_entries = merge_entries(all_entries)
        except Exception as e:
            return RuntimeError(
                "File deletion conflicts detected! Give up committing. " + str(e))
        print("hello1")
        conflict = self.check_row_id_range_conflicts(commit_kind, merged_entries)
        if conflict is not None:
            return conflict

        return self.check_for_row_id_from_snapshot(latest_snapshot, delta_entries)

    def check_row_id_range_conflicts(self, commit_kind, commit_entries):
        """Check for row ID range conflicts among merged entries.

        Follows Java ConflictDetection.checkRowIdRangeConflicts logic:
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

        merged_groups = _merge_overlapping_row_id_ranges(entries_with_row_id)

        for group in merged_groups:
            data_files = [
                entry for entry in group
                if not DataFileMeta.is_blob_file(entry.file.file_name)
            ]
            if not _are_all_row_id_ranges_same(data_files):
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

        Follows Java ConflictDetection.checkForRowIdFromSnapshot logic:
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

        changed_parts = changed_partitions(commit_entries)

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
                snapshot, changed_parts)
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

        Follows Java CommitScanner.readIncrementalEntries logic:
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


def changed_partitions(commit_entries):
    """Extract unique changed partitions from commit entries.

    Follows Java ManifestEntryChanges.changedPartitions logic.

    Args:
        commit_entries: List of ManifestEntry to extract partitions from.

    Returns:
        Set of partition tuples.
    """
    partitions = set()
    for entry in commit_entries:
        partition_key = tuple(entry.partition.values)
        partitions.add(partition_key)
    return partitions


def _merge_overlapping_row_id_ranges(entries):
    """Merge entries with overlapping row ID ranges into groups.

    Follows Java RangeHelper.mergeOverlappingRanges logic:
    sorts entries by row ID range start, then merges overlapping groups.

    Args:
        entries: List of ManifestEntry with non-null first_row_id.

    Returns:
        List of groups, where each group is a list of entries
        with overlapping row ID ranges.
    """
    if not entries:
        return []

    indexed = []
    for i, entry in enumerate(entries):
        row_range = entry.file.row_id_range()
        if row_range is not None:
            indexed.append((entry, row_range, i))

    if not indexed:
        return []

    indexed.sort(key=lambda item: (item[1].from_, item[1].to))

    groups = []
    current_group = [indexed[0]]
    current_end = indexed[0][1].to

    for i in range(1, len(indexed)):
        entry, row_range, original_index = indexed[i]
        if row_range.from_ <= current_end:
            current_group.append(indexed[i])
            if row_range.to > current_end:
                current_end = row_range.to
        else:
            groups.append(current_group)
            current_group = [indexed[i]]
            current_end = row_range.to

    groups.append(current_group)

    result = []
    for group in groups:
        group.sort(key=lambda item: item[2])
        result.append([item[0] for item in group])

    return result


def _are_all_row_id_ranges_same(entries):
    """Check if all entries have the same row ID range.

    Follows Java RangeHelper.areAllRangesSame logic.

    Args:
        entries: List of ManifestEntry to check.

    Returns:
        True if all entries have the same row ID range, False otherwise.
    """
    if not entries:
        return True

    first_range = entries[0].file.row_id_range()
    if first_range is None:
        return False

    for entry in entries[1:]:
        entry_range = entry.file.row_id_range()
        if entry_range is None:
            return False
        if entry_range.from_ != first_range.from_ or entry_range.to != first_range.to:
            return False

    return True


def _entry_identifier(entry):
    """Build a unique identifier tuple for a ManifestEntry.

    Follows Java FileEntry.Identifier logic: uses partition, bucket, level,
    fileName, extraFiles, embeddedIndex and externalPath to identify a file.

    Args:
        entry: A ManifestEntry instance.

    Returns:
        A hashable identifier tuple.
    """
    partition_key = tuple(entry.partition.values)
    extra_files = tuple(entry.file.extra_files) if entry.file.extra_files else ()
    embedded_index = (bytes(entry.file.embedded_index)
                      if entry.file.embedded_index is not None else None)
    return (
        partition_key,
        entry.bucket,
        entry.file.level,
        entry.file.file_name,
        extra_files,
        embedded_index,
        entry.file.external_path,
    )


def merge_entries(entries):
    """Merge manifest entries: ADD and DELETE of the same file cancel each other.

    Follows Java FileEntry.mergeEntries logic:
    - ADD: if identifier already in map, raise error; otherwise add to map
    - DELETE: if identifier already in map, remove both (cancel); otherwise add to map

    Args:
        entries: Iterable of ManifestEntry.

    Returns:
        List of merged ManifestEntry values.

    Raises:
        RuntimeError: If trying to add a file that is already in the map.
    """
    entry_map = {}
    insertion_order = []

    for entry in entries:
        identifier = _entry_identifier(entry)
        if entry.kind == 0:  # ADD
            if identifier in entry_map:
                raise RuntimeError(
                    "Trying to add file {} which is already added.".format(
                        entry.file.file_name))
            entry_map[identifier] = entry
            insertion_order.append(identifier)
        elif entry.kind == 1:  # DELETE
            if identifier in entry_map:
                del entry_map[identifier]
            else:
                entry_map[identifier] = entry
                insertion_order.append(identifier)
        else:
            raise RuntimeError("Unknown entry kind: {}".format(entry.kind))

    return [entry_map[key] for key in insertion_order if key in entry_map]
