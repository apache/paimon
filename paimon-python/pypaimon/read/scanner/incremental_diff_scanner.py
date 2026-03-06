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
    Scan files added between two snapshots via set-diff.

    More efficient than reading N delta_manifest_lists when many intermediate
    snapshots are compaction-only.
    """

    def __init__(self, table):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.manifest_list_manager = ManifestListManager(table)
        self.manifest_file_manager = ManifestFileManager(table)

        options = self.table.options
        self.target_split_size = options.source_split_target_size()
        self.open_file_cost = options.source_split_open_file_cost()

    def scan(self, start_snapshot: Snapshot, end_snapshot: Snapshot) -> Plan:
        """Scan files added between start (exclusive) and end (inclusive) snapshots."""
        added_entries = self.compute_diff(start_snapshot, end_snapshot)

        if not added_entries:
            return Plan([])

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
        """Return files present in end_snapshot but absent from start_snapshot."""
        start_manifest_files = self.manifest_list_manager.read_all(start_snapshot)
        end_manifest_files = self.manifest_list_manager.read_all(end_snapshot)

        max_workers = self.table.options.scan_manifest_parallelism(os.cpu_count() or 8)

        start_entries = self.manifest_file_manager.read_entries_parallel(
            start_manifest_files,
            max_workers=max_workers
        )
        end_entries = self.manifest_file_manager.read_entries_parallel(
            end_manifest_files,
            max_workers=max_workers
        )

        start_keys: Set[Tuple] = {self._entry_key(e) for e in start_entries}

        added_entries = [
            entry for entry in end_entries
            if self._entry_key(entry) not in start_keys
        ]

        return added_entries

    def _entry_key(self, entry: ManifestEntry) -> Tuple:
        return (
            tuple(entry.partition.values),
            entry.bucket,
            entry.file.file_name
        )
