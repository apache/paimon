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
from pathlib import Path
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
        self.snapshot_dir = self.table.table_path / "snapshot"
        self.latest_file = self.snapshot_dir / "LATEST"

    def get_latest_snapshot(self) -> Optional[Snapshot]:
        if not self.file_io.exists(self.latest_file):
            return None

        latest_content = self.file_io.read_file_utf8(self.latest_file)
        latest_snapshot_id = int(latest_content.strip())

        snapshot_file = self.snapshot_dir / f"snapshot-{latest_snapshot_id}"
        if not self.file_io.exists(snapshot_file):
            return None

        snapshot_content = self.file_io.read_file_utf8(snapshot_file)
        return JSON.from_json(snapshot_content, Snapshot)

    def get_snapshot_path(self, snapshot_id: int) -> Path:
        """
        Get the path for a snapshot file.

        Args:
            snapshot_id: The snapshot ID

        Returns:
            Path to the snapshot file
        """
        return self.snapshot_dir / f"snapshot-{snapshot_id}"
