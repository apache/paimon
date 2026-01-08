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

import logging
import random
import time
import uuid
from typing import List, Optional

from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.manifest.schema.manifest_file_meta import ManifestFileMeta
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.read.scanner.full_starting_scanner import FullStartingScanner
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.snapshot.snapshot_commit import (PartitionStatistics,
                                               SnapshotCommit)
from pypaimon.snapshot.snapshot_manager import SnapshotManager
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.row.offset_row import OffsetRow
from pypaimon.write.commit_message import CommitMessage

logger = logging.getLogger(__name__)


class CommitResult:
    """Base class for commit results."""

    def is_success(self) -> bool:
        """Returns True if commit was successful."""
        raise NotImplementedError


class SuccessResult(CommitResult):
    """Result indicating successful commit."""

    def is_success(self) -> bool:
        return True


class RetryResult(CommitResult):

    def __init__(self, latest_snapshot, exception: Optional[Exception] = None):
        self.latest_snapshot = latest_snapshot
        self.exception = exception

    def is_success(self) -> bool:
        return False

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

        self.commit_max_retries = table.options.commit_max_retries()
        self.commit_timeout = table.options.commit_timeout()
        self.commit_min_retry_wait = table.options.commit_min_retry_wait()
        self.commit_max_retry_wait = table.options.commit_max_retry_wait()

    def commit(self, commit_messages: List[CommitMessage], commit_identifier: int):
        """Commit the given commit messages in normal append mode."""
        if not commit_messages:
            return

        commit_entries = []
        for msg in commit_messages:
            partition = GenericRow(list(msg.partition), self.table.partition_keys_fields)
            for file in msg.new_files:
                commit_entries.append(ManifestEntry(
                    kind=0,
                    partition=partition,
                    bucket=msg.bucket,
                    total_buckets=self.table.total_buckets,
                    file=file
                ))

        self._try_commit(commit_kind="APPEND",
                         commit_identifier=commit_identifier,
                         commit_entries_plan=lambda snapshot: commit_entries)

    def overwrite(self, overwrite_partition, commit_messages: List[CommitMessage], commit_identifier: int):
        """Commit the given commit messages in overwrite mode."""
        if not commit_messages:
            return

        partition_filter = None
        # sanity check, all changes must be done within the given partition, meanwhile build a partition filter
        if len(overwrite_partition) > 0:
            predicate_builder = PredicateBuilder(self.table.partition_keys_fields)
            sub_predicates = []
            for key, value in overwrite_partition.items():
                sub_predicates.append(predicate_builder.equal(key, value))
            partition_filter = predicate_builder.and_predicates(sub_predicates)

            for msg in commit_messages:
                row = OffsetRow(msg.partition, 0, len(msg.partition))
                if not partition_filter.test(row):
                    raise RuntimeError(f"Trying to overwrite partition {overwrite_partition}, but the changes "
                                       f"in {msg.partition} does not belong to this partition")

        self._overwrite_partition_filter = partition_filter
        self._overwrite_commit_messages = commit_messages

        self._try_commit(
            commit_kind="OVERWRITE",
            commit_identifier=commit_identifier,
            commit_entries_plan=lambda snapshot: self._generate_overwrite_entries(snapshot)
        )

    def _try_commit(self, commit_kind, commit_identifier, commit_entries_plan):
        import threading

        retry_count = 0
        retry_result = None
        start_time_ms = int(time.time() * 1000)
        thread_id = threading.current_thread().name
        while True:
            latest_snapshot = self.snapshot_manager.get_latest_snapshot()
            commit_entries = commit_entries_plan(latest_snapshot)

            result = self._try_commit_once(
                retry_result=retry_result,
                commit_kind=commit_kind,
                commit_entries=commit_entries,
                commit_identifier=commit_identifier,
                latest_snapshot=latest_snapshot
            )

            if result.is_success():
                logger.info(
                    f"Thread {thread_id}: commit success {latest_snapshot.id + 1 if latest_snapshot else 1} "
                    f"after {retry_count} retries"
                )
                break

            retry_result = result

            elapsed_ms = int(time.time() * 1000) - start_time_ms
            if elapsed_ms > self.commit_timeout or retry_count >= self.commit_max_retries:
                error_msg = (
                    f"Commit failed {latest_snapshot.id + 1 if latest_snapshot else 1} "
                    f"after {elapsed_ms} millis with {retry_count} retries, "
                    f"there maybe exist commit conflicts between multiple jobs."
                )
                if retry_result.exception:
                    raise RuntimeError(error_msg) from retry_result.exception
                else:
                    raise RuntimeError(error_msg)

            self._commit_retry_wait(retry_count)
            retry_count += 1

    def _try_commit_once(self, retry_result: Optional[RetryResult], commit_kind: str,
                         commit_entries: List[ManifestEntry], commit_identifier: int,
                         latest_snapshot: Optional[Snapshot]) -> CommitResult:
        if retry_result is not None and latest_snapshot is not None:
            start_check_snapshot_id = 1  # Snapshot.FIRST_SNAPSHOT_ID
            if retry_result.latest_snapshot is not None:
                start_check_snapshot_id = retry_result.latest_snapshot.id + 1

            for snapshot_id in range(start_check_snapshot_id, latest_snapshot.id + 2):
                snapshot = self.snapshot_manager.get_snapshot_by_id(snapshot_id)
                if (snapshot and snapshot.commit_user == self.commit_user and
                        snapshot.commit_identifier == commit_identifier and
                        snapshot.commit_kind == commit_kind):
                    logger.info(
                        f"Commit already completed (snapshot {snapshot_id}), "
                        f"user: {self.commit_user}, identifier: {commit_identifier}"
                    )
                    return SuccessResult()

        unique_id = uuid.uuid4()
        base_manifest_list = f"manifest-list-{unique_id}-0"
        delta_manifest_list = f"manifest-list-{unique_id}-1"

        # process new_manifest
        new_manifest_file = f"manifest-{str(uuid.uuid4())}-0"
        added_file_count = 0
        deleted_file_count = 0
        delta_record_count = 0
        # process snapshot
        new_snapshot_id = latest_snapshot.id + 1 if latest_snapshot else 1

        # Check if row tracking is enabled
        row_tracking_enabled = self.table.options.row_tracking_enabled()

        # Apply row tracking logic if enabled
        next_row_id = None
        if row_tracking_enabled:
            # Assign snapshot ID to delta files
            commit_entries = self._assign_snapshot_id(new_snapshot_id, commit_entries)

            # Get the next row ID start from the latest snapshot
            first_row_id_start = self._get_next_row_id_start(latest_snapshot)

            # Assign row IDs to new files and get the next row ID for the snapshot
            commit_entries, next_row_id = self._assign_row_tracking_meta(first_row_id_start, commit_entries)

        for entry in commit_entries:
            if entry.kind == 0:
                added_file_count += 1
                delta_record_count += entry.file.row_count
            else:
                deleted_file_count += 1
                delta_record_count -= entry.file.row_count
        try:
            self.manifest_file_manager.write(new_manifest_file, commit_entries)
            # TODO: implement noConflictsOrFail logic
            partition_columns = list(zip(*(entry.partition.values for entry in commit_entries)))
            partition_min_stats = [min(col) for col in partition_columns]
            partition_max_stats = [max(col) for col in partition_columns]
            partition_null_counts = [sum(value == 0 for value in col) for col in partition_columns]
            if not all(count == 0 for count in partition_null_counts):
                raise RuntimeError("Partition value should not be null")
            manifest_file_path = f"{self.manifest_file_manager.manifest_path}/{new_manifest_file}"
            new_manifest_file_meta = ManifestFileMeta(
                file_name=new_manifest_file,
                file_size=self.table.file_io.get_file_size(manifest_file_path),
                num_added_files=added_file_count,
                num_deleted_files=deleted_file_count,
                partition_stats=SimpleStats(
                    min_values=GenericRow(
                        values=partition_min_stats,
                        fields=self.table.partition_keys_fields
                    ),
                    max_values=GenericRow(
                        values=partition_max_stats,
                        fields=self.table.partition_keys_fields
                    ),
                    null_counts=partition_null_counts,
                ),
                schema_id=self.table.table_schema.id,
            )
            self.manifest_list_manager.write(delta_manifest_list, [new_manifest_file_meta])

            # process existing_manifest
            total_record_count = 0
            if latest_snapshot:
                existing_manifest_files = self.manifest_list_manager.read_all(latest_snapshot)
                previous_record_count = latest_snapshot.total_record_count
                if previous_record_count:
                    total_record_count += previous_record_count
            else:
                existing_manifest_files = []
            self.manifest_list_manager.write(base_manifest_list, existing_manifest_files)

            total_record_count += delta_record_count
            snapshot_data = Snapshot(
                version=3,
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
                next_row_id=next_row_id,
            )
            # Generate partition statistics for the commit
            statistics = self._generate_partition_statistics(commit_entries)
        except Exception as e:
            self._cleanup_preparation_failure(delta_manifest_list, base_manifest_list)
            logger.warning(f"Exception occurs when preparing snapshot: {e}", exc_info=True)
            raise RuntimeError(f"Failed to prepare snapshot: {e}")

        # Use SnapshotCommit for atomic commit
        try:
            with self.snapshot_commit:
                success = self.snapshot_commit.commit(snapshot_data, self.table.current_branch(), statistics)
                if not success:
                    logger.warning(f"Atomic commit failed for snapshot #{new_snapshot_id} failed")
                    self._cleanup_preparation_failure(delta_manifest_list, base_manifest_list)
                    return RetryResult(latest_snapshot, None)
        except Exception as e:
            # Commit exception, not sure about the situation and should not clean up the files
            logger.warning("Retry commit for exception")
            return RetryResult(latest_snapshot, e)

        logger.warning(
            f"Successfully commit snapshot {new_snapshot_id} to table {self.table.identifier} "
            f"for snapshot-{new_snapshot_id} by user {self.commit_user} "
            + f"with identifier {commit_identifier} and kind {commit_kind}."
        )
        return SuccessResult()

    def _generate_overwrite_entries(self, latestSnapshot):
        """Generate commit entries for OVERWRITE mode based on latest snapshot."""
        entries = []
        current_entries = [] if latestSnapshot is None \
            else (FullStartingScanner(self.table, self._overwrite_partition_filter, None).
                  read_manifest_entries(self.manifest_list_manager.read_all(latestSnapshot)))
        for entry in current_entries:
            entry.kind = 1  # DELETE
            entries.append(entry)
        for msg in self._overwrite_commit_messages:
            partition = GenericRow(list(msg.partition), self.table.partition_keys_fields)
            for file in msg.new_files:
                entries.append(ManifestEntry(
                    kind=0,  # ADD
                    partition=partition,
                    bucket=msg.bucket,
                    total_buckets=self.table.total_buckets,
                    file=file
                ))
        return entries

    def _commit_retry_wait(self, retry_count: int):
        import threading
        thread_id = threading.get_ident()

        retry_wait_ms = min(
            self.commit_min_retry_wait * (2 ** retry_count),
            self.commit_max_retry_wait
        )

        jitter_ms = random.randint(0, max(1, int(retry_wait_ms * 0.2)))
        total_wait_ms = retry_wait_ms + jitter_ms

        logger.debug(
            f"Thread {thread_id}: Waiting {total_wait_ms}ms before retry (base: {retry_wait_ms}ms, "
            f"jitter: {jitter_ms}ms)"
        )
        time.sleep(total_wait_ms / 1000.0)

    def _cleanup_preparation_failure(self,
                                     delta_manifest_list: Optional[str],
                                     base_manifest_list: Optional[str]):
        try:
            manifest_path = self.manifest_list_manager.manifest_path

            if delta_manifest_list:
                manifest_files = self.manifest_list_manager.read(delta_manifest_list)
                for manifest_meta in manifest_files:
                    manifest_file_path = f"{self.manifest_file_manager.manifest_path}/{manifest_meta.file_name}"
                    self.table.file_io.delete_quietly(manifest_file_path)
                delta_path = f"{manifest_path}/{delta_manifest_list}"
                self.table.file_io.delete_quietly(delta_path)

            if base_manifest_list:
                base_path = f"{manifest_path}/{base_manifest_list}"
                self.table.file_io.delete_quietly(base_path)
        except Exception as e:
            logger.warning(f"Failed to clean up temporary files during preparation failure: {e}", exc_info=True)

    def abort(self, commit_messages: List[CommitMessage]):
        """Abort commit and delete files. Uses external_path if available to ensure proper scheme handling."""
        for message in commit_messages:
            for file in message.new_files:
                try:
                    path_to_delete = file.external_path if file.external_path else file.file_path
                    if path_to_delete:
                        path_str = str(path_to_delete)
                        self.table.file_io.delete_quietly(path_str)
                except Exception as e:
                    import logging
                    logger = logging.getLogger(__name__)
                    path_to_delete = file.external_path if file.external_path else file.file_path
                    logger.warning(f"Failed to clean up file {path_to_delete} during abort: {e}")

    def close(self):
        """Close the FileStoreCommit and release resources."""
        if hasattr(self.snapshot_commit, 'close'):
            self.snapshot_commit.close()

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
            record_count = file_meta.row_count if entry.kind == 0 else file_meta.row_count * -1
            file_size_in_bytes = file_meta.file_size if entry.kind == 0 else file_meta.file_size * -1
            file_count = 1 if entry.kind == 0 else -1

            # Use epoch millis
            if file_meta.creation_time:
                file_creation_time = file_meta.creation_time_epoch_millis()
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

    def _assign_snapshot_id(self, snapshot_id: int, commit_entries: List[ManifestEntry]) -> List[ManifestEntry]:
        """Assign snapshot ID to all commit entries."""
        return [entry.assign_sequence_number(snapshot_id, snapshot_id) for entry in commit_entries]

    def _get_next_row_id_start(self, latest_snapshot) -> int:
        """Get the next row ID start from the latest snapshot."""
        if latest_snapshot and hasattr(latest_snapshot, 'next_row_id') and latest_snapshot.next_row_id is not None:
            return latest_snapshot.next_row_id
        return 0

    def _assign_row_tracking_meta(self, first_row_id_start: int, commit_entries: List[ManifestEntry]):
        """
        Assign row tracking metadata (first_row_id) to new files.
        This follows the Java implementation logic from FileStoreCommitImpl.assignRowTrackingMeta.
        """
        if not commit_entries:
            return commit_entries, first_row_id_start

        row_id_assigned = []
        start = first_row_id_start
        blob_start = first_row_id_start

        for entry in commit_entries:
            # Check if this is an append file that needs row ID assignment
            if (entry.kind == 0 and  # ADD kind
                    entry.file.file_source == 0 and  # APPEND file source
                    entry.file.first_row_id is None):  # No existing first_row_id

                if self._is_blob_file(entry.file.file_name):
                    # Handle blob files specially
                    if blob_start >= start:
                        raise RuntimeError(
                            f"This is a bug, blobStart {blob_start} should be less than start {start} "
                            f"when assigning a blob entry file."
                        )
                    row_count = entry.file.row_count
                    row_id_assigned.append(entry.assign_first_row_id(blob_start))
                    blob_start += row_count
                else:
                    # Handle regular files
                    row_count = entry.file.row_count
                    row_id_assigned.append(entry.assign_first_row_id(start))
                    blob_start = start
                    start += row_count
            else:
                # For compact files or files that already have first_row_id, don't assign
                row_id_assigned.append(entry)

        return row_id_assigned, start

    @staticmethod
    def _is_blob_file(file_name: str) -> bool:
        """Check if a file is a blob file based on its extension."""
        return file_name.endswith('.blob')
