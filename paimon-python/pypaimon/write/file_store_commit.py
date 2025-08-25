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
        record_count_add = self._generate_record_count_add(commit_messages)
        total_record_count = record_count_add

        if latest_snapshot:
            existing_manifest_files = self.manifest_list_manager.read_all_manifest_files(latest_snapshot)
            total_record_count += latest_snapshot.total_record_count

        new_manifest_files.extend(existing_manifest_files)
        manifest_list = self.manifest_list_manager.write(new_manifest_files)

        new_snapshot_id = self._generate_snapshot_id()
        snapshot_data = Snapshot(
            version=3,
            id=new_snapshot_id,
            schema_id=0,
            base_manifest_list=manifest_list,
            delta_manifest_list=manifest_list,
            total_record_count=total_record_count,
            delta_record_count=record_count_add,
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

        record_count_add = self._generate_record_count_add(commit_messages)

        new_snapshot_id = self._generate_snapshot_id()
        snapshot_data = Snapshot(
            version=3,
            id=new_snapshot_id,
            schema_id=0,
            base_manifest_list=manifest_list,
            delta_manifest_list=manifest_list,
            total_record_count=record_count_add,
            delta_record_count=record_count_add,
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

        This method follows the Java implementation pattern from
        org.apache.paimon.manifest.PartitionEntry.fromManifestEntry() and
        PartitionEntry.merge() methods.

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
                    'file_count': 0,
                    'file_size_in_bytes': 0,
                    'last_file_creation_time': 0
                }

            # Process each file in the commit message
            # Following Java implementation: PartitionEntry.fromDataFile()
            new_files = message.new_files()
            for file_meta in new_files:
                # Extract actual file metadata (following Java DataFileMeta pattern)
                record_count = file_meta.row_count
                file_size_in_bytes = file_meta.file_size
                file_count = 1

                # Convert creation_time to milliseconds (Java uses epoch millis)
                if file_meta.creation_time:
                    file_creation_time = int(file_meta.creation_time.timestamp() * 1000)
                else:
                    file_creation_time = int(time.time() * 1000)

                # Accumulate statistics (following Java PartitionEntry.merge() logic)
                partition_stats[partition_key]['record_count'] += record_count
                partition_stats[partition_key]['file_size_in_bytes'] += file_size_in_bytes
                partition_stats[partition_key]['file_count'] += file_count

                # Keep the latest creation time
                partition_stats[partition_key]['last_file_creation_time'] = max(
                    partition_stats[partition_key]['last_file_creation_time'],
                    file_creation_time
                )

        # Convert to PartitionStatistics objects
        # Following Java PartitionEntry.toPartitionStatistics() pattern
        return [
            PartitionStatistics.create(
                partition_spec=stats['partition_spec'],
                record_count=stats['record_count'],
                file_count=stats['file_count'],
                file_size_in_bytes=stats['file_size_in_bytes'],
                last_file_creation_time=stats['last_file_creation_time']
            )
            for stats in partition_stats.values()
        ]

    def _generate_record_count_add(self, commit_messages: List[CommitMessage]) -> int:
        """
        Generate record count add from commit messages.

        This method follows the Java implementation pattern from
        org.apache.paimon.manifest.ManifestEntry.recordCountAdd().

        Args:
            commit_messages: List of commit messages to analyze

        Returns:
            Count of add record
        """
        record_count = 0

        for message in commit_messages:
            new_files = message.new_files()
            for file_meta in new_files:
                record_count += file_meta.row_count

        return record_count
