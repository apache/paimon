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

"""
Manifest entries scanner for commit operations.
"""
from typing import Optional, List

from pypaimon.common.predicate_builder import PredicateBuilder
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

    def read_all_entries_from_changed_partitions(self, latest_snapshot: Optional[Snapshot],
                                                 commit_entries: List[ManifestEntry]):
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

        partition_filter = self._build_partition_filter_from_entries(commit_entries)

        all_manifests = self.manifest_list_manager.read_all(latest_snapshot)
        return FileScanner(
            self.table, lambda: ([], None), partition_predicate=partition_filter
        ).read_manifest_entries(all_manifests)

    def read_incremental_entries_from_changed_partitions(self, snapshot: Snapshot,
                                                         commit_entries: List[ManifestEntry]):
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

        partition_filter = self._build_partition_filter_from_entries(commit_entries)

        return FileScanner(
            self.table, lambda: ([], None), partition_predicate=partition_filter
        ).read_manifest_entries(delta_manifests)

    def _build_partition_filter_from_entries(self, entries: List[ManifestEntry]):
        """Build a partition predicate that matches all partitions present in the given entries.

        Args:
            entries: List of ManifestEntry whose partitions should be matched.

        Returns:
            A Predicate matching any of the changed partitions, or None if
            partition keys are empty.
        """
        partition_keys = self.table.partition_keys
        if not partition_keys:
            return None

        changed_partitions = set()
        for entry in entries:
            changed_partitions.add(tuple(entry.partition.values))

        if not changed_partitions:
            return None

        predicate_builder = PredicateBuilder(self.table.partition_keys_fields)
        partition_predicates = []
        for partition_values in changed_partitions:
            sub_predicates = []
            for i, key in enumerate(partition_keys):
                sub_predicates.append(predicate_builder.equal(key, partition_values[i]))
            partition_predicates.append(predicate_builder.and_predicates(sub_predicates))

        return predicate_builder.or_predicates(partition_predicates)
