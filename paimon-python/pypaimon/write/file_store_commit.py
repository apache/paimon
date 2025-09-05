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
import uuid
from pathlib import Path
from typing import List

from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.manifest.schema.manifest_file_meta import ManifestFileMeta
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.read.table_scan import TableScan
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.snapshot.snapshot_commit import (PartitionStatistics,
                                               SnapshotCommit)
from pypaimon.snapshot.snapshot_manager import SnapshotManager
from pypaimon.table.row.binary_row import BinaryRow
from pypaimon.table.row.offset_row import OffsetRow
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

        commit_entries = []
        for msg in commit_messages:
            partition = BinaryRow(list(msg.partition), self.table.table_schema.get_partition_key_fields())
            for file in msg.new_files:
                commit_entries.append(ManifestEntry(
                    kind=0,
                    partition=partition,
                    bucket=msg.bucket,
                    total_buckets=self.table.total_buckets,
                    file=file
                ))

        self._try_commit(commit_kind="APPEND",
                         commit_entries=commit_entries,
                         commit_identifier=commit_identifier)

    def overwrite(self, overwrite_partition, commit_messages: List[CommitMessage], commit_identifier: int):
        """Commit the given commit messages in overwrite mode."""
        if not commit_messages:
            return

        partition_filter = None
        # sanity check, all changes must be done within the given partition, meanwhile build a partition filter
        if len(overwrite_partition) > 0:
            predicate_builder = PredicateBuilder(self.table.table_schema.get_partition_key_fields())
            sub_predicates = []
            for key, value in overwrite_partition.items():
                sub_predicates.append(predicate_builder.equal(key, value))
            partition_filter = predicate_builder.and_predicates(sub_predicates)

            for msg in commit_messages:
                row = OffsetRow(msg.partition, 0, len(msg.partition))
                if not partition_filter.test(row):
                    raise RuntimeError(f"Trying to overwrite partition {overwrite_partition}, but the changes "
                                       f"in {msg.partition} does not belong to this partition")

        commit_entries = []
        current_entries = TableScan(self.table, partition_filter, None, []).plan().files()
        for entry in current_entries:
            entry.kind = 1
            commit_entries.append(entry)
        for msg in commit_messages:
            partition = BinaryRow(list(msg.partition), self.table.table_schema.get_partition_key_fields())
            for file in msg.new_files:
                commit_entries.append(ManifestEntry(
                    kind=0,
                    partition=partition,
                    bucket=msg.bucket,
                    total_buckets=self.table.total_buckets,
                    file=file
                ))

        self._try_commit(commit_kind="OVERWRITE",
                         commit_entries=commit_entries,
                         commit_identifier=commit_identifier)

    def _try_commit(self, commit_kind, commit_entries, commit_identifier):
        unique_id = uuid.uuid4()
        base_manifest_list = f"manifest-list-{unique_id}-0"
        delta_manifest_list = f"manifest-list-{unique_id}-1"

        # process new_manifest
        new_manifest_file = f"manifest-{str(uuid.uuid4())}-0"
        added_file_count = 0
        deleted_file_count = 0
        delta_record_count = 0
        for entry in commit_entries:
            if entry.kind == 0:
                added_file_count += 1
                delta_record_count += entry.file.row_count
            else:
                deleted_file_count += 1
                delta_record_count -= entry.file.row_count
        self.manifest_file_manager.write(new_manifest_file, commit_entries)
        # TODO: implement noConflictsOrFail logic

        partition_columns = list(zip(*(entry.partition.values for entry in commit_entries)))
        partition_min_stats = [min(col) for col in partition_columns]
        partition_max_stats = [max(col) for col in partition_columns]
        partition_null_counts = [sum(value == 0 for value in col) for col in partition_columns]
        if not all(count == 0 for count in partition_null_counts):
            raise RuntimeError("Partition value should not be null")

        new_manifest_list = ManifestFileMeta(
            file_name=new_manifest_file,
            file_size=self.table.file_io.get_file_size(self.manifest_file_manager.manifest_path / new_manifest_file),
            num_added_files=added_file_count,
            num_deleted_files=deleted_file_count,
            partition_stats=SimpleStats(
                min_values=BinaryRow(
                    values=partition_min_stats,
                    fields=self.table.table_schema.get_partition_key_fields(),
                ),
                max_values=BinaryRow(
                    values=partition_max_stats,
                    fields=self.table.table_schema.get_partition_key_fields(),
                ),
                null_counts=partition_null_counts,
            ),
            schema_id=self.table.table_schema.id,
        )
        self.manifest_list_manager.write(delta_manifest_list, [new_manifest_list])

        # process existing_manifest
        latest_snapshot = self.snapshot_manager.get_latest_snapshot()
        total_record_count = 0
        if latest_snapshot:
            existing_manifest_files = self.manifest_list_manager.read_all(latest_snapshot)
            previous_record_count = latest_snapshot.total_record_count
            if previous_record_count:
                total_record_count += previous_record_count
        else:
            existing_manifest_files = []
        self.manifest_list_manager.write(base_manifest_list, existing_manifest_files)

        # process snapshot
        new_snapshot_id = self._generate_snapshot_id()
        total_record_count += delta_record_count
        snapshot_data = Snapshot(
            version=1,
            id=new_snapshot_id,
            schema_id=self.table.table_schema.id,
            base_manifest_list=base_manifest_list,
            delta_manifest_list=delta_manifest_list,
            total_record_count=total_record_count,
            delta_record_count=delta_record_count,
            commit_user=self.commit_user,
            commit_identifier=commit_identifier,
            commit_kind=commit_kind,
            time_millis=int(time.time() * 1000),
        )

        # Generate partition statistics for the commit
        statistics = self._generate_partition_statistics(commit_entries)

        # Use SnapshotCommit for atomic commit
        with self.snapshot_commit:
            success = self.snapshot_commit.commit(snapshot_data, self.table.current_branch(), statistics)
            if not success:
                raise RuntimeError(f"Failed to commit snapshot {new_snapshot_id}")

    def abort(self, commit_messages: List[CommitMessage]):
        for message in commit_messages:
            for file in message.new_files:
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

    def _generate_partition_statistics(self, commit_entries: List[ManifestEntry]) -> List[PartitionStatistics]:
        """
        Generate partition statistics from commit entries.

        This method follows the Java implementation pattern from
        org.apache.paimon.manifest.PartitionEntry.fromManifestEntry() and
        PartitionEntry.merge() methods.

        Args:
            commit_entries: List of commit entries to analyze

        Returns:
            List of PartitionStatistics for each unique partition
        """
        partition_stats = {}

        for entry in commit_entries:
            # Convert partition tuple to dictionary for PartitionStatistics
            partition_value = tuple(entry.partition.values)  # Call the method to get partition value
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

            # Following Java implementation: PartitionEntry.fromDataFile()
            file_meta = entry.file
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
