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

import time
from pathlib import Path
from typing import List

from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.snapshot.snapshot_manager import SnapshotManager
from pypaimon.write.commit_message import CommitMessage


class FileStoreCommit:
    """Core commit logic for file store operations."""

    def __init__(self, table, commit_user: str):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.commit_user = commit_user

        self.snapshot_manager = SnapshotManager(table)
        self.manifest_file_manager = ManifestFileManager(table)
        self.manifest_list_manager = ManifestListManager(table)

        self.manifest_target_size = 8 * 1024 * 1024
        self.manifest_merge_min_count = 30

    def commit(self, commit_messages: List[CommitMessage], commit_identifier: int):
        """Commit the given commit messages in normal append mode."""
        if not commit_messages:
            return

        new_manifest_files = self.manifest_file_manager.write(commit_messages)
        if not new_manifest_files:
            return
        latest_snapshot = self.snapshot_manager.get_latest_snapshot()
        existing_manifest_files = []
        if latest_snapshot:
            existing_manifest_files = self.manifest_list_manager.read_all_manifest_files(latest_snapshot)
        new_manifest_files.extend(existing_manifest_files)
        manifest_list = self.manifest_list_manager.write(new_manifest_files)

        new_snapshot_id = self._generate_snapshot_id()
        snapshot_data = Snapshot(
            version=3,
            id=new_snapshot_id,
            schema_id=0,
            base_manifest_list=manifest_list,
            delta_manifest_list=manifest_list,
            commit_user=self.commit_user,
            commit_identifier=commit_identifier,
            commit_kind="APPEND",
            time_millis=int(time.time() * 1000),
            log_offsets={},
        )
        self.snapshot_manager.commit_snapshot(new_snapshot_id, snapshot_data)

    def overwrite(self, partition, commit_messages: List[CommitMessage], commit_identifier: int):
        if not commit_messages:
            return

        new_manifest_files = self.manifest_file_manager.write(commit_messages)
        if not new_manifest_files:
            return

        # In overwrite mode, we don't merge with existing manifests
        manifest_list = self.manifest_list_manager.write(new_manifest_files)

        new_snapshot_id = self._generate_snapshot_id()
        snapshot_data = Snapshot(
            version=3,
            id=new_snapshot_id,
            schema_id=0,
            base_manifest_list=manifest_list,
            delta_manifest_list=manifest_list,
            commit_user=self.commit_user,
            commit_identifier=commit_identifier,
            commit_kind="OVERWRITE",
            time_millis=int(time.time() * 1000),
            log_offsets={},
        )
        self.snapshot_manager.commit_snapshot(new_snapshot_id, snapshot_data)

    def abort(self, commit_messages: List[CommitMessage]):
        for message in commit_messages:
            for file in message.new_files():
                try:
                    file_path_obj = Path(file.file_path)
                    if file_path_obj.exists():
                        file_path_obj.unlink()
                except Exception as e:
                    print(f"Warning: Failed to clean up file {file.file_path}: {e}")

    def close(self):
        pass

    def _generate_snapshot_id(self) -> int:
        latest_snapshot = self.snapshot_manager.get_latest_snapshot()
        if latest_snapshot:
            return latest_snapshot.id + 1
        else:
            return 1
