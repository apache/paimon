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

from typing import Optional, TYPE_CHECKING

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.predicate import Predicate

from pypaimon.read.plan import Plan
from pypaimon.read.scanner.file_scanner import FileScanner
from pypaimon.snapshot.snapshot_manager import SnapshotManager
from pypaimon.manifest.manifest_list_manager import ManifestListManager

if TYPE_CHECKING:
    from pypaimon.globalindex.vector_search import VectorSearch


class TableScan:
    """Implementation of TableScan for native Python reading."""

    def __init__(
        self,
        table,
        predicate: Optional[Predicate],
        limit: Optional[int],
        vector_search: Optional['VectorSearch'] = None
    ):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.predicate = predicate
        self.limit = limit
        self.vector_search = vector_search
        self.file_scanner = self._create_file_scanner()

    def plan(self) -> Plan:
        return self.file_scanner.scan()

    def _create_file_scanner(self) -> FileScanner:
        options = self.table.options.options
        snapshot_manager = SnapshotManager(self.table)
        manifest_list_manager = ManifestListManager(self.table)
        if options.contains(CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP):
            ts = options.get(CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP).split(",")
            if len(ts) != 2:
                raise ValueError(
                    "The incremental-between-timestamp must specific start(exclusive) and end timestamp. But is: " +
                    options.get(CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP))
            earliest_snapshot = snapshot_manager.try_get_earliest_snapshot()
            latest_snapshot = snapshot_manager.get_latest_snapshot()
            if earliest_snapshot is None or latest_snapshot is None:
                return FileScanner(self.table, lambda: [])
            start_timestamp = int(ts[0])
            end_timestamp = int(ts[1])
            if start_timestamp >= end_timestamp:
                raise ValueError(
                    "Ending timestamp %s should be >= starting timestamp %s." % (end_timestamp, start_timestamp))
            if (start_timestamp == end_timestamp or start_timestamp > latest_snapshot.time_millis
                    or end_timestamp < earliest_snapshot.time_millis):
                return FileScanner(self.table, lambda: [])

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

            def incremental_manifest():
                snapshots_in_range = []
                for snapshot_id in range(start_id + 1, end_id + 1):
                    snapshot = snapshot_manager.get_snapshot_by_id(snapshot_id)
                    if snapshot.commit_kind == "APPEND":
                        snapshots_in_range.append(snapshot)

                manifests = []

                for snapshot in snapshots_in_range:
                    manifest_files = manifest_list_manager.read_delta(snapshot)
                    manifests.extend(manifest_files)
                return manifests

            return FileScanner(self.table, incremental_manifest, self.predicate, self.limit)
        elif options.contains(CoreOptions.SCAN_TAG_NAME):  # Handle tag-based reading
            tag_name = options.get(CoreOptions.SCAN_TAG_NAME)

            def tag_manifest_scanner():
                tag_manager = self.table.tag_manager()
                tag = tag_manager.get_or_throw(tag_name)
                snapshot = tag.trim_to_snapshot()
                return manifest_list_manager.read_all(snapshot)

            return FileScanner(
                self.table,
                tag_manifest_scanner,
                self.predicate,
                self.limit,
                vector_search=self.vector_search
            )

        def all_manifests():
            snapshot = snapshot_manager.get_latest_snapshot()
            return manifest_list_manager.read_all(snapshot)

        return FileScanner(
            self.table,
            all_manifests,
            self.predicate,
            self.limit,
            vector_search=self.vector_search
        )

    def with_shard(self, idx_of_this_subtask, number_of_para_subtasks) -> 'TableScan':
        self.file_scanner.with_shard(idx_of_this_subtask, number_of_para_subtasks)
        return self

    def with_slice(self, start_pos, end_pos) -> 'TableScan':
        self.file_scanner.with_slice(start_pos, end_pos)
        return self
