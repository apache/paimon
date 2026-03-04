"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
"""
Incremental diff scanner for efficient catch-up reads.

Uses diff approach: Read 2 base_manifest_lists (start and end) and compute
file-level diff. More efficient than reading N delta_manifest_lists when
there are many snapshots between start and end.
"""
import os
from typing import List, Set, Tuple

from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.read.plan import Plan
from pypaimon.read.scanner.append_table_split_generator import \
    AppendTableSplitGenerator
from pypaimon.snapshot.snapshot import Snapshot


class IncrementalDiffScanner:
    """
    Efficiently scan files added between two snapshots using diff.

    Instead of reading N delta_manifest_lists (one per intermediate snapshot),
    this scanner reads only 2 base_manifest_lists (start and end snapshots)
    and computes the file-level diff.

    This is more efficient when:
    - There are many snapshots between start and end (e.g., > 10)
    - Many intermediate snapshots are COMPACT commits (no new data)

    Algorithm:
        added_files = end_files - start_files
        (keyed by partition, bucket, filename)
    """

    def __init__(self, table):
        """
        Initialize the scanner.

        Args:
            table: The FileStoreTable to scan
        """
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.manifest_list_manager = ManifestListManager(table)
        self.manifest_file_manager = ManifestFileManager(table)

        # Get split configuration from table options
        options = self.table.options
        self.target_split_size = options.source_split_target_size()
        self.open_file_cost = options.source_split_open_file_cost()

    def scan(self, start_snapshot: Snapshot, end_snapshot: Snapshot) -> Plan:
        """
        Scan files added between start and end snapshots.

        Args:
            start_snapshot: Starting snapshot (exclusive - files here are NOT included)
            end_snapshot: Ending snapshot (inclusive - files here ARE included)

        Returns:
            Plan with splits for all added files
        """
        # Compute diff to get added entries
        added_entries = self.compute_diff(start_snapshot, end_snapshot)

        if not added_entries:
            return Plan([])

        # Create splits from added entries
        split_generator = AppendTableSplitGenerator(
            self.table,
            self.target_split_size,
            self.open_file_cost,
            {}  # No deletion files for incremental diff
        )

        splits = split_generator.create_splits(added_entries)
        return Plan(splits)

    def compute_diff(
        self,
        start_snapshot: Snapshot,
        end_snapshot: Snapshot
    ) -> List[ManifestEntry]:
        """
        Compute files added between two snapshots.

        Files in end_snapshot but not in start_snapshot are considered "added".
        Files in start_snapshot but not in end_snapshot are "deleted" (ignored).

        Args:
            start_snapshot: Starting snapshot
            end_snapshot: Ending snapshot

        Returns:
            List of ManifestEntry for files added between the snapshots
        """
        # Read ALL manifests (base + delta) for both snapshots to get full state
        # base_manifest_list contains state BEFORE the snapshot's changes
        # delta_manifest_list contains the changes IN the snapshot
        # So we need both to get the complete state AT the snapshot
        start_manifest_files = self.manifest_list_manager.read_all(start_snapshot)
        end_manifest_files = self.manifest_list_manager.read_all(end_snapshot)

        # Read all entries from manifest files
        max_workers = max(8, self.table.options.scan_manifest_parallelism(os.cpu_count() or 8))

        start_entries = self.manifest_file_manager.read_entries_parallel(
            start_manifest_files,
            max_workers=max_workers
        )
        end_entries = self.manifest_file_manager.read_entries_parallel(
            end_manifest_files,
            max_workers=max_workers
        )

        # Compute diff: files in end but not in start
        start_keys: Set[Tuple] = {self._entry_key(e) for e in start_entries}

        added_entries = [
            entry for entry in end_entries
            if self._entry_key(entry) not in start_keys
        ]

        return added_entries

    def _entry_key(self, entry: ManifestEntry) -> Tuple:
        """
        Create unique key for a manifest entry.

        The key uniquely identifies a file by (partition, bucket, filename).
        Same file in different partitions/buckets will have different keys.

        Args:
            entry: ManifestEntry to create key for

        Returns:
            Tuple of (partition_values, bucket, filename)
        """
        return (
            tuple(entry.partition.values),
            entry.bucket,
            entry.file.file_name
        )
