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
from typing import Optional

from pypaimon.common.file_io import FileIO
from pypaimon.common.json_util import JSON
from pypaimon.snapshot.snapshot import Snapshot


class SnapshotManager:
    """Manager for snapshot files using unified FileIO."""

    def __init__(self, table):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.file_io: FileIO = self.table.file_io
        snapshot_path = self.table.table_path.rstrip('/')
        self.snapshot_dir = f"{snapshot_path}/snapshot"
        self.latest_file = f"{self.snapshot_dir}/LATEST"

    def get_latest_snapshot(self) -> Optional[Snapshot]:
        if not self.file_io.exists(self.latest_file):
            return None

        latest_content = self.file_io.read_file_utf8(self.latest_file)
        latest_snapshot_id = int(latest_content.strip())

        snapshot_file = f"{self.snapshot_dir}/snapshot-{latest_snapshot_id}"
        if not self.file_io.exists(snapshot_file):
            return None

        snapshot_content = self.file_io.read_file_utf8(snapshot_file)
        return JSON.from_json(snapshot_content, Snapshot)

    def get_snapshot_path(self, snapshot_id: int) -> str:
        """
        Get the path for a snapshot file.

        Args:
            snapshot_id: The snapshot ID

        Returns:
            Path to the snapshot file
        """
        return f"{self.snapshot_dir}/snapshot-{snapshot_id}"

    def try_get_earliest_snapshot(self) -> Optional[Snapshot]:
        earliest_file = f"{self.snapshot_dir}/EARLIEST"
        if self.file_io.exists(earliest_file):
            earliest_content = self.file_io.read_file_utf8(earliest_file)
            earliest_snapshot_id = int(earliest_content.strip())
            return self.get_snapshot_by_id(earliest_snapshot_id)
        else:
            return self.get_snapshot_by_id(1)

    def earlier_or_equal_time_mills(self, timestamp: int) -> Optional[Snapshot]:
        """
        Find the latest snapshot with time_millis <= the given timestamp.

        Args:
            timestamp: The timestamp to compare against

        Returns:
            The latest snapshot with time_millis <= timestamp, or None if no such snapshot exists
        """
        earliest = 1
        latest = self.get_latest_snapshot().id
        final_snapshot = None

        while earliest <= latest:
            mid = earliest + (latest - earliest) // 2
            snapshot = self.get_snapshot_by_id(mid)
            commit_time = snapshot.time_millis

            if commit_time > timestamp:
                latest = mid - 1
            elif commit_time < timestamp:
                earliest = mid + 1
                final_snapshot = snapshot
            else:
                final_snapshot = snapshot
                break

        return final_snapshot

    def get_snapshot_by_id(self, snapshot_id: int) -> Optional[Snapshot]:
        """
        Get a snapshot by its ID.

        Args:
            snapshot_id: The snapshot ID

        Returns:
            The snapshot with the specified ID, or None if not found
        """
        snapshot_file = self.get_snapshot_path(snapshot_id)
        if not self.file_io.exists(snapshot_file):
            return None

        snapshot_content = self.file_io.read_file_utf8(snapshot_file)
        return JSON.from_json(snapshot_content, Snapshot)
