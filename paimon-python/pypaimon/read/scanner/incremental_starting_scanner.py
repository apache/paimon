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
from typing import List, Optional

from pypaimon.common.predicate import Predicate
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.read.scanner.full_starting_scanner import FullStartingScanner
from pypaimon.schema.data_types import DataField
from pypaimon.snapshot.snapshot_manager import SnapshotManager


class IncrementalStartingScanner(FullStartingScanner):
    def __init__(self, table, predicate: Optional[Predicate], limit: Optional[int],
                 read_type: List[DataField], start: int, end: int):
        super().__init__(table, predicate, limit, read_type)
        self.startingSnapshotId = start
        self.endingSnapshotId = end

    def plan_files(self) -> List[ManifestEntry]:
        snapshots_in_range = []
        for snapshot_id in range(self.startingSnapshotId + 1, self.endingSnapshotId + 1):
            snapshot = self.snapshot_manager.get_snapshot_by_id(snapshot_id)
            if snapshot.commit_kind == "APPEND":
                snapshots_in_range.append(snapshot)

        # Collect all file entries from all snapshots in range
        file_entries = []

        for snapshot in snapshots_in_range:
            # Get manifest files for this snapshot
            manifest_files = self.manifest_list_manager.read_delta(snapshot)

            # Read all entries from manifest files
            for manifest_file in manifest_files:
                entries = self.manifest_file_manager.read(manifest_file.file_name)
                file_entries.extend(entries)
        if self.predicate:
            file_entries = self._filter_by_predicate(file_entries)
        return file_entries

    @staticmethod
    def between_timestamps(table, predicate: Optional[Predicate], limit: Optional[int],
                           read_type: List[DataField], start_timestamp: int,
                           end_timestamp: int) -> 'IncrementalStartingScanner':
        """
        Create an IncrementalStartingScanner for snapshots between two timestamps.
        """
        snapshot_manager = SnapshotManager(table)
        starting_snapshot = snapshot_manager.earlier_or_equal_time_mills(start_timestamp)
        earliest_snapshot = snapshot_manager.try_get_earliest_snapshot()

        # If earliest_snapshot.time_millis > start_timestamp we should include the earliest_snapshot
        if starting_snapshot is None or (earliest_snapshot and earliest_snapshot.time_millis > start_timestamp):
            start_id = earliest_snapshot.id - 1 if earliest_snapshot else -1
        else:
            start_id = starting_snapshot.id

        end_snapshot = snapshot_manager.earlier_or_equal_time_mills(end_timestamp)
        latest_snapshot = snapshot_manager.get_latest_snapshot()
        end_id = end_snapshot.id if end_snapshot else (latest_snapshot.id if latest_snapshot else -1)

        return IncrementalStartingScanner(table, predicate, limit, read_type, start_id, end_id)
