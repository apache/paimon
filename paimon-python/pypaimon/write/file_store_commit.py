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

from pypaimon.catalog.snapshot_commit import PartitionStatistics, SnapshotCommit
from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.snapshot.snapshot_manager import SnapshotManager
from pypaimon.write.commit_message import CommitMessage


class FileStoreCommit:
    """
    Core commit logic for file store operations.

    This class provides atomic commit functionality similar to
    org.apache.paimon.operation.FileStoreCommitImpl in Java.
    """

    def __init__(self, snapshot_commit: SnapshotCommit, table, commit_user: str):
        from pypaimon.table.file_store_table import FileStoreTable

        self.snapshot_commit = snapshot_commit
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

        # Generate partition statistics for the commit
        statistics = self._generate_partition_statistics(commit_messages)

        # Use SnapshotCommit for atomic commit
        with self.snapshot_commit:
            success = self.snapshot_commit.commit(snapshot_data, self.table.current_branch(), statistics)
            if not success:
                raise RuntimeError(f"Failed to commit snapshot {new_snapshot_id}")

    def overwrite(self, partition, commit_messages: List[CommitMessage], commit_identifier: int):
        """Commit the given commit messages in overwrite mode."""
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

        # Generate partition statistics for the commit
        statistics = self._generate_partition_statistics(commit_messages)

        # Use SnapshotCommit for atomic commit
        with self.snapshot_commit:
            success = self.snapshot_commit.commit(snapshot_data, self.table.current_branch(), statistics)
            if not success:
                raise RuntimeError(f"Failed to commit snapshot {new_snapshot_id}")

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
        """Close the FileStoreCommit and release resources."""
        if hasattr(self.snapshot_commit, 'close'):
            self.snapshot_commit.close()

    def _generate_snapshot_id(self) -> int:
        """Generate the next snapshot ID."""
        latest_snapshot = self.snapshot_manager.get_latest_snapshot()
        if latest_snapshot:
            return latest_snapshot.id + 1
        else:
            return 1

    def _generate_partition_statistics(self, commit_messages: List[CommitMessage]) -> List[PartitionStatistics]:
        """
        Generate partition statistics from commit messages.

        Args:
            commit_messages: List of commit messages to analyze

        Returns:
            List of PartitionStatistics for each unique partition
        """
        partition_stats = {}

        for message in commit_messages:
            # Convert partition tuple to dictionary for PartitionStatistics
            partition_value = message.partition()  # Call the method to get partition value
            if partition_value:
                # Assuming partition is a tuple and we need to convert it to a dict
                # This may need adjustment based on actual partition format
                if isinstance(partition_value, tuple):
                    # Create partition spec from partition tuple and table partition keys
                    partition_spec = {}
                    if len(partition_value) == len(self.table.partition_keys):
                        for i, key in enumerate(self.table.partition_keys):
                            partition_spec[key] = str(partition_value[i])
                    else:
                        # Fallback: use indices as keys
                        for i, value in enumerate(partition_value):
                            partition_spec[f"partition_{i}"] = str(value)
                else:
                    # If partition is already a dict or other format
                    partition_spec = dict(partition_value) if partition_value else {}
            else:
                # Default partition for unpartitioned tables
                partition_spec = {}

            partition_key = tuple(sorted(partition_spec.items()))

            if partition_key not in partition_stats:
                partition_stats[partition_key] = {
                    'partition_spec': partition_spec,
                    'record_count': 0,
                    'file_count': 0
                }

            # Count files and estimate records
            new_files = message.new_files()
            partition_stats[partition_key]['file_count'] += len(new_files)

            # Estimate record count (this is a simplification)
            # In a real implementation, this should come from file metadata
            for file_entry in new_files:
                # Estimate records per file (this could be improved with actual metadata)
                estimated_records = getattr(file_entry, 'record_count', 1000)  # Default estimate
                partition_stats[partition_key]['record_count'] += estimated_records

        # Convert to PartitionStatistics objects
        return [
            PartitionStatistics.create(
                partition_spec=stats['partition_spec'],
                record_count=stats['record_count'],
                file_count=stats['file_count'],
                file_size_in_bytes=stats.get('file_size_in_bytes', 0),
                last_file_creation_time=int(time.time() * 1000)
            )
            for stats in partition_stats.values()
        ]
