# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import logging
import random
import time
import uuid
from typing import Dict, List, Optional

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.manifest_file_merger import ManifestFileMerger
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.file_entry import FileEntry
from pypaimon.manifest.schema.manifest_entry import ManifestEntry

from pypaimon.manifest.schema.manifest_file_meta import ManifestFileMeta
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.snapshot.snapshot_commit import (PartitionStatistics,
                                               SnapshotCommit)
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.row.offset_row import OffsetRow
from pypaimon.write.commit.commit_rollback import CommitRollback
from pypaimon.write.commit.commit_scanner import CommitScanner
from pypaimon.write.commit.conflict_detection import ConflictDetection
from pypaimon.write.commit.overwrite_changes_provider import OverwriteChangesProvider
from pypaimon.table.special_fields import SpecialFields
from pypaimon.write.commit_callback import CommitCallback, CommitCallbackContext
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

    def __init__(self, latest_snapshot, exception: Optional[Exception] = None,
                 base_data_files: Optional[List[ManifestEntry]] = None):
        self.latest_snapshot = latest_snapshot
        self.exception = exception
        # Base entries as of latest_snapshot, carried so the next attempt reuses
        # them and reads only the incremental changes.
        self.base_data_files = base_data_files

    def is_success(self) -> bool:
        return False


class FileStoreCommit:
    """
    Core commit logic for file store operations.

    This class provides atomic commit functionality similar to
    org.apache.paimon.operation.FileStoreCommitImpl in Java.
    """

    def __init__(self, snapshot_commit: SnapshotCommit, table, commit_user: str,
                 commit_callbacks: Optional[List[CommitCallback]] = None):
        from pypaimon.table.file_store_table import FileStoreTable

        self.snapshot_commit = snapshot_commit
        self.table: FileStoreTable = table
        self.commit_user = commit_user
        self.commit_callbacks: List[CommitCallback] = commit_callbacks if commit_callbacks is not None else []

        self.snapshot_manager = table.snapshot_manager()
        self.manifest_file_manager = ManifestFileManager(table)
        self.manifest_list_manager = ManifestListManager(table)

        self.manifest_target_size = table.options.manifest_target_size()
        self.manifest_merge_min_count = table.options.manifest_merge_min_count()
        self.manifest_file_merger = ManifestFileMerger(
            self.manifest_file_manager,
            self.manifest_target_size,
            self.manifest_merge_min_count,
        )

        self.commit_max_retries = table.options.commit_max_retries()
        self.commit_timeout = table.options.commit_timeout()
        self.commit_min_retry_wait = table.options.commit_min_retry_wait()
        self.commit_max_retry_wait = table.options.commit_max_retry_wait()

        self.commit_scanner = CommitScanner(table, self.manifest_list_manager)

        self.conflict_detection = ConflictDetection(
            data_evolution_enabled=table.options.data_evolution_enabled(),
            snapshot_manager=self.snapshot_manager,
            manifest_list_manager=self.manifest_list_manager,
            table=table,
            commit_scanner=self.commit_scanner
        )

        table_rollback = table.catalog_environment.catalog_table_rollback()
        self.rollback = CommitRollback(table_rollback) if table_rollback is not None else None

    def commit(self, commit_messages: List[CommitMessage], commit_identifier: int):
        """Commit the given commit messages in normal append mode."""
        if not commit_messages:
            return

        # Extract the minimum check_from_snapshot from commit messages
        valid_snapshots = [msg.check_from_snapshot for msg in commit_messages
                           if msg.check_from_snapshot != -1]
        if valid_snapshots:
            self.conflict_detection._row_id_check_from_snapshot = min(valid_snapshots)

        logger.info(
            "Ready to commit to table %s, number of commit messages: %d",
            self.table.identifier,
            len(commit_messages),
        )
        commit_entries = self._collect_manifest_entries(commit_messages)
        changelog_entries = self._collect_changelog_entries(commit_messages)

        logger.info("Finished collecting changes, including: %d entries, %d changelog entries",
                    len(commit_entries), len(changelog_entries))

        index_deletes = []
        index_adds = []
        for msg in commit_messages:
            index_deletes.extend(msg.index_deletes)
            index_adds.extend(msg.index_adds)

        if not index_deletes:
            from pypaimon.write.global_index_update_checker import (
                apply_global_index_update_action,
            )
            updated_cols = set()
            written_partitions = set()
            for msg in commit_messages:
                if msg.check_from_snapshot == -1:
                    continue
                for f in msg.new_files:
                    if f.write_cols:
                        updated_cols.update(f.write_cols)
                        written_partitions.add(msg.partition)
            if updated_cols:
                snapshot = self.snapshot_manager.get_latest_snapshot()
                index_msgs = apply_global_index_update_action(
                    self.table, snapshot, list(updated_cols), written_partitions,
                )
                for m in index_msgs:
                    index_deletes.extend(m.index_deletes)

        commit_kind = "APPEND"
        detect_conflicts = False
        allow_rollback = False
        if self.conflict_detection.should_be_overwrite_commit(
                commit_entries, index_adds + index_deletes):
            commit_kind = "OVERWRITE"
            detect_conflicts = True
            allow_rollback = True
        if self.conflict_detection.has_row_id_check_from_snapshot():
            detect_conflicts = True
            allow_rollback = True
        if self.conflict_detection.has_global_index_additions(index_adds):
            detect_conflicts = True

        self._try_commit(commit_kind=commit_kind,
                         commit_identifier=commit_identifier,
                         commit_entries_plan=lambda snapshot: commit_entries,
                         changelog_entries=changelog_entries,
                         detect_conflicts=detect_conflicts,
                         allow_rollback=allow_rollback,
                         index_deletes=index_deletes,
                         index_adds=index_adds)

    def overwrite(self, overwrite_partition, commit_messages: List[CommitMessage], commit_identifier: int):
        """Commit the given commit messages in overwrite mode."""
        logger.info(
            "Ready to overwrite to table %s, number of commit messages: %d",
            self.table.identifier,
            len(commit_messages),
        )
        skip_overwrite = False
        partition_filter = None

        # Partition filter is built from dynamic or static partition according to options.
        if len(self.table.partition_keys) > 0 and self.table.options.dynamic_partition_overwrite():
            if not commit_messages:
                # In dynamic mode, if there are no changes to commit, no data will be deleted
                skip_overwrite = True
            else:
                partition_filter = self._create_dynamic_partition_filter(commit_messages)
        else:
            partition_filter = self._create_static_partition_filter(overwrite_partition, commit_messages)

        changelog_entries = self._collect_changelog_entries(commit_messages)

        if not skip_overwrite:
            provider = self._overwrite_changes_provider(partition_filter, commit_messages)
            self._try_commit(
                commit_kind="OVERWRITE",
                commit_identifier=commit_identifier,
                commit_entries_plan=provider.provide,
                changelog_entries=changelog_entries,
                detect_conflicts=True,
                allow_rollback=False,
            )

    def drop_partitions(self, partitions: List[Dict[str, str]], commit_identifier: int) -> None:
        if not partitions:
            raise ValueError("Partitions list cannot be empty.")

        partition_keys_set = set(self.table.partition_keys)
        for part in partitions:
            for key in part:
                if key not in partition_keys_set:
                    raise ValueError(
                        f"Partition spec key '{key}' is not a partition column. "
                        f"Partition keys are: {list(self.table.partition_keys)}."
                    )

        predicate_builder = PredicateBuilder(self.table.partition_keys_fields)
        default_part_value = self.table.options.options.get(
            CoreOptions.PARTITION_DEFAULT_NAME, "__DEFAULT_PARTITION__")
        partition_predicates = []
        for part in partitions:
            sub_predicates = []
            for key, value in part.items():
                if value is None or (isinstance(value, str) and value == default_part_value):
                    sub_predicates.append(predicate_builder.is_null(key))
                else:
                    sub_predicates.append(predicate_builder.equal(key, value))
            if sub_predicates:
                pred = predicate_builder.and_predicates(sub_predicates)
                if pred is not None:
                    partition_predicates.append(pred)
        if not partition_predicates:
            raise RuntimeError("Failed to build partition filter for drop_partitions.")

        partition_filter = predicate_builder.or_predicates(partition_predicates)

        self.drop_by_partition_filter(partition_filter, commit_identifier)

    def drop_by_partition_filter(self, partition_filter, commit_identifier: int) -> None:
        if partition_filter is None:
            raise RuntimeError("Failed to build partition filter.")

        provider = self._overwrite_changes_provider(partition_filter, [])
        self._try_commit(
            commit_kind="OVERWRITE",
            commit_identifier=commit_identifier,
            commit_entries_plan=provider.provide,
            detect_conflicts=True,
            allow_rollback=False,
        )

    def truncate_table(self, commit_identifier: int) -> None:
        """Truncate the entire table, deleting all data."""
        provider = self._overwrite_changes_provider(None, [])
        self._try_commit(
            commit_kind="OVERWRITE",
            commit_identifier=commit_identifier,
            commit_entries_plan=provider.provide,
            detect_conflicts=True,
            allow_rollback=False,
        )

    def _try_commit(self, commit_kind, commit_identifier, commit_entries_plan,
                    detect_conflicts=False, allow_rollback=False, index_deletes=None,
                    index_adds=None, changelog_entries=None):

        retry_count = 0
        retry_result = None
        start_time_ms = int(time.time() * 1000)
        while True:
            latest_snapshot = self.snapshot_manager.get_latest_snapshot()
            commit_entries = commit_entries_plan(latest_snapshot)

            # No entries to commit (e.g. drop_partitions with no matching data): skip commit
            # to avoid creating manifest/snapshot with empty partition_stats (causes read errors).
            if not commit_entries and not index_deletes and not index_adds:
                break

            result = self._try_commit_once(
                retry_result=retry_result,
                commit_kind=commit_kind,
                commit_entries=commit_entries,
                changelog_entries=changelog_entries or [],
                commit_identifier=commit_identifier,
                latest_snapshot=latest_snapshot,
                detect_conflicts=detect_conflicts,
                allow_rollback=allow_rollback,
                index_deletes=index_deletes,
                index_adds=index_adds,
            )

            if result.is_success():
                commit_duration_ms = int(time.time() * 1000) - start_time_ms
                if commit_kind == "OVERWRITE":
                    logger.info(
                        "Finished overwrite to table %s, duration %d ms",
                        self.table.identifier,
                        commit_duration_ms,
                    )
                else:
                    logger.info(
                        "Finished commit to table %s, duration %d ms",
                        self.table.identifier,
                        commit_duration_ms,
                    )
                break

            retry_result = result

            elapsed_ms = int(time.time() * 1000) - start_time_ms
            if elapsed_ms > self.commit_timeout or retry_count >= self.commit_max_retries:
                if commit_kind == "OVERWRITE":
                    logger.info(
                        "Finished (Uncertain of success) overwrite to table %s, duration %d ms",
                        self.table.identifier,
                        elapsed_ms,
                    )
                else:
                    logger.info(
                        "Finished (Uncertain of success) commit to table %s, duration %d ms",
                        self.table.identifier,
                        elapsed_ms,
                    )
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
                         commit_entries: List[ManifestEntry],
                         changelog_entries: List[ManifestEntry],
                         commit_identifier: int,
                         latest_snapshot: Optional[Snapshot],
                         detect_conflicts: bool = False,
                         allow_rollback: bool = False,
                         index_deletes=None,
                         index_adds=None) -> CommitResult:
        start_millis = int(time.time() * 1000)
        if self._is_duplicate_commit(retry_result, latest_snapshot, commit_identifier, commit_kind):
            return SuccessResult()

        unique_id = uuid.uuid4()
        base_manifest_list = f"manifest-list-{unique_id}-0"
        delta_manifest_list = f"manifest-list-{unique_id}-1"

        # process new_manifest
        new_manifest_file = f"manifest-{str(uuid.uuid4())}"
        new_index_manifest = None
        # process snapshot
        new_snapshot_id = latest_snapshot.id + 1 if latest_snapshot else 1
        index_entries = (index_deletes or []) + (index_adds or [])

        # Base entries for conflict detection. On retry, reuse the previous
        # attempt's base + read only the incremental changes (mirrors Java).
        base_data_files = None
        if detect_conflicts and latest_snapshot is not None:
            incremental = None
            if (retry_result is not None
                    and retry_result.latest_snapshot is not None
                    and retry_result.base_data_files is not None):
                incremental = self.commit_scanner.read_incremental_changes(
                    retry_result.latest_snapshot,
                    latest_snapshot,
                    commit_entries,
                    index_entries)
            if incremental is not None:
                base_data_files = list(retry_result.base_data_files)
                if incremental:
                    base_data_files.extend(incremental)
                    base_data_files = FileEntry.merge_entries(base_data_files)
            else:
                # First attempt, or incremental could not be built (missing
                # snapshot): scan the changed partitions in full.
                base_data_files = self.commit_scanner.read_all_entries_from_changed_partitions(
                    latest_snapshot, commit_entries, index_entries)

            conflict_exception = self.conflict_detection.check_conflicts(
                latest_snapshot,
                base_data_files,
                commit_entries,
                commit_kind,
                index_entries,
            )

            if conflict_exception is not None:
                if allow_rollback and self.rollback is not None:
                    if self.rollback.try_to_rollback(latest_snapshot):
                        # Rolled back: base/snapshot no longer valid; next attempt
                        # re-scans from scratch (matches Java RollbackRetryResult).
                        return RetryResult(None, conflict_exception)
                raise conflict_exception

        # Apply row tracking logic after conflict detection (matches Java ordering)
        row_tracking_enabled = self.table.options.row_tracking_enabled()
        next_row_id = None
        if row_tracking_enabled:
            commit_entries = self._assign_snapshot_id(new_snapshot_id, commit_entries)
            first_row_id_start = self._get_next_row_id_start(latest_snapshot)
            commit_entries, next_row_id = self._assign_row_tracking_meta(first_row_id_start, commit_entries)

        changelog_manifest_list_name = None
        changelog_manifest_list_size = None
        changelog_record_count = None
        merge_new_files = []
        try:
            new_manifest_file_metas = self._write_manifest_files(commit_entries, new_manifest_file)
            self.manifest_list_manager.write(delta_manifest_list, new_manifest_file_metas)

            # Write changelog manifest if changelog entries exist
            if changelog_entries:
                changelog_manifest_file = f"manifest-{str(uuid.uuid4())}-changelog"
                changelog_manifest_file_metas = self._write_manifest_files(
                    changelog_entries, changelog_manifest_file)
                changelog_manifest_list_name = f"manifest-list-{unique_id}-changelog"
                self.manifest_list_manager.write(
                    changelog_manifest_list_name, changelog_manifest_file_metas)
                manifest_path = self.manifest_list_manager.manifest_path
                changelog_manifest_list_size = self.table.file_io.get_file_size(
                    f"{manifest_path}/{changelog_manifest_list_name}")
                # kind==0 means ADD; pypaimon producers only support additions currently
                changelog_record_count = sum(
                    entry.file.row_count for entry in changelog_entries if entry.kind == 0)

            # process existing_manifest
            total_record_count = 0
            if latest_snapshot:
                existing_manifest_files = self.manifest_list_manager.read_all(latest_snapshot)
                previous_record_count = latest_snapshot.total_record_count
                if previous_record_count:
                    total_record_count += previous_record_count
            else:
                existing_manifest_files = []
            merged_manifest_files, merge_new_files = self.manifest_file_merger.merge(
                existing_manifest_files)
            self.manifest_list_manager.write(base_manifest_list, merged_manifest_files)

            delta_record_count = 0
            for entry in commit_entries:
                if entry.kind == 0:
                    delta_record_count += entry.file.row_count
                else:
                    delta_record_count -= entry.file.row_count

            total_record_count += delta_record_count
            index_manifest = latest_snapshot.index_manifest if latest_snapshot else None
            if index_deletes or index_adds:
                from pypaimon.manifest.index_manifest_file import IndexManifestFile
                previous_index_manifest = index_manifest
                index_manifest = IndexManifestFile(self.table).combine_changes(
                    previous_index_manifest, index_adds or [], index_deletes or [])
                if index_manifest != previous_index_manifest:
                    new_index_manifest = index_manifest

            snapshot_data = Snapshot(
                version=3,
                id=new_snapshot_id,
                schema_id=self.table.table_schema.id,
                base_manifest_list=base_manifest_list,
                delta_manifest_list=delta_manifest_list,
                changelog_manifest_list=changelog_manifest_list_name,
                changelog_manifest_list_size=changelog_manifest_list_size,
                changelog_record_count=changelog_record_count,
                total_record_count=total_record_count,
                delta_record_count=delta_record_count,
                commit_user=self.commit_user,
                commit_identifier=commit_identifier,
                commit_kind=commit_kind,
                time_millis=int(time.time() * 1000),
                next_row_id=next_row_id,
                index_manifest=index_manifest,
            )
            # Generate partition statistics for the commit
            statistics = self._generate_partition_statistics(commit_entries)
        except Exception as e:
            try:
                self._clean_up_reuse_tmp_manifests(
                    delta_manifest_list, changelog_manifest_list_name, new_index_manifest)
                self._clean_up_no_reuse_tmp_manifests(
                    base_manifest_list, merge_new_files)
            except Exception as cleanup_err:
                logger.warning(f"Failed to clean up temporary files: {cleanup_err}",
                               exc_info=True)
            logger.warning(f"Exception occurs when preparing snapshot: {e}", exc_info=True)
            raise RuntimeError(f"Failed to prepare snapshot: {e}")

        # Use SnapshotCommit for atomic commit
        try:
            with self.snapshot_commit:
                success = self.snapshot_commit.commit(snapshot_data, statistics)
                if not success:
                    commit_time_s = (int(time.time() * 1000) - start_millis) / 1000
                    logger.warning(
                        "Atomic commit failed for snapshot #%d by user %s "
                        "with identifier %s and kind %s after %.0f seconds. Try again.",
                        new_snapshot_id,
                        self.commit_user,
                        commit_identifier,
                        commit_kind,
                        commit_time_s,
                    )
                    return RetryResult(latest_snapshot, None, base_data_files=base_data_files)
        except Exception as e:
            # Commit exception, not sure about the situation and should not clean up the files
            logger.warning("Retry commit for exception.", exc_info=True)
            return RetryResult(latest_snapshot, e, base_data_files=base_data_files)

        logger.info(
            "Successfully commit snapshot %d to table %s by user %s "
            "with identifier %s and kind %s.",
            new_snapshot_id,
            self.table.identifier,
            self.commit_user,
            commit_identifier,
            commit_kind,
        )

        if self.commit_callbacks:
            context = CommitCallbackContext(
                snapshot=snapshot_data,
                commit_entries=commit_entries,
                identifier=commit_identifier,
            )
            for callback in self.commit_callbacks:
                callback.call(context)

        return SuccessResult()

    def _write_manifest_files(self, commit_entries, base_name):
        return self.manifest_file_manager.rolling_write(
            commit_entries, self.manifest_target_size, base_name)

    def _is_duplicate_commit(self, retry_result, latest_snapshot, commit_identifier, commit_kind) -> bool:
        if retry_result is not None and latest_snapshot is not None:
            start_check_snapshot_id = 1  # Snapshot.FIRST_SNAPSHOT_ID
            if retry_result.latest_snapshot is not None:
                start_check_snapshot_id = retry_result.latest_snapshot.id + 1

            for snapshot_id in range(start_check_snapshot_id, latest_snapshot.id + 1):
                snapshot = self.snapshot_manager.get_snapshot_by_id(snapshot_id)
                if (snapshot and snapshot.commit_user == self.commit_user and
                        snapshot.commit_identifier == commit_identifier and
                        snapshot.commit_kind == commit_kind):
                    logger.info(
                        f"Commit already completed (snapshot {snapshot_id}), "
                        f"user: {self.commit_user}, identifier: {commit_identifier}"
                    )
                    return True
        return False

    def _create_dynamic_partition_filter(self, commit_messages: List[CommitMessage]):
        """Build a partition filter from the unique partitions present in commit_messages."""
        predicate_builder = PredicateBuilder(self.table.partition_keys_fields)
        predicates = []
        seen_partitions = set()
        for msg in commit_messages:
            partition_values = tuple(msg.partition)
            if partition_values not in seen_partitions:
                seen_partitions.add(partition_values)
                equalities = []
                for name, value in zip(self.table.partition_keys, msg.partition):
                    if value is None:
                        equalities.append(predicate_builder.is_null(name))
                    else:
                        equalities.append(predicate_builder.equal(name, value))
                predicates.append(predicate_builder.and_predicates(equalities))
        return predicate_builder.or_predicates(predicates)

    def _create_static_partition_filter(self, overwrite_partition, commit_messages: List[CommitMessage]):
        """Build a partition filter from the explicit overwrite_partition spec."""
        if not overwrite_partition:
            return None
        predicate_builder = PredicateBuilder(self.table.partition_keys_fields)
        default_part_value = self.table.options.options.get(
            CoreOptions.PARTITION_DEFAULT_NAME, "__DEFAULT_PARTITION__")
        equalities = []
        for key, value in overwrite_partition.items():
            if value is None or (isinstance(value, str) and value == default_part_value):
                equalities.append(predicate_builder.is_null(key))
            else:
                equalities.append(predicate_builder.equal(key, value))
        partition_filter = predicate_builder.and_predicates(equalities)
        for msg in commit_messages:
            row = OffsetRow(msg.partition, 0, len(msg.partition))
            if not partition_filter.test(row):
                raise RuntimeError(f"Trying to overwrite partition {overwrite_partition}, but the changes "
                                   f"in {msg.partition} does not belong to this partition")
        return partition_filter

    def _overwrite_changes_provider(self, partition_filter, commit_messages):
        """Build a stateful provider of OVERWRITE commit entries that caches the
        existing files of the target partitions across retries (see
        OverwriteChangesProvider). One instance per overwrite operation."""
        return OverwriteChangesProvider(
            self.table,
            self.manifest_list_manager,
            self.snapshot_manager,
            partition_filter,
            commit_messages,
        )

    def _commit_retry_wait(self, retry_count: int):

        retry_wait_ms = min(
            self.commit_min_retry_wait * (2 ** retry_count),
            self.commit_max_retry_wait
        )

        jitter_ms = random.randint(0, max(1, int(retry_wait_ms * 0.2)))
        total_wait_ms = retry_wait_ms + jitter_ms

        time.sleep(total_wait_ms / 1000.0)

    def _collect_changelog_entries(self, commit_messages: List[CommitMessage]) -> List[ManifestEntry]:
        changelog_entries = []
        for msg in commit_messages:
            partition = GenericRow(list(msg.partition), self.table.partition_keys_fields)
            for file in msg.changelog_files:
                changelog_entries.append(ManifestEntry(
                    kind=0,
                    partition=partition,
                    bucket=msg.bucket,
                    total_buckets=self.table.total_buckets,
                    file=file
                ))
        return changelog_entries

    def _collect_manifest_entries(self, commit_messages: List[CommitMessage]) -> List[ManifestEntry]:
        commit_entries = []
        for msg in commit_messages:
            partition = GenericRow(list(msg.partition), self.table.partition_keys_fields)
            for file in msg.new_files:
                commit_entries.append(ManifestEntry(
                    kind=0,
                    partition=partition,
                    bucket=msg.bucket,
                    total_buckets=self.table.total_buckets,
                    file=file,
                ))
            for file in msg.deleted_files:
                commit_entries.append(ManifestEntry(
                    kind=1,
                    partition=partition,
                    bucket=msg.bucket,
                    total_buckets=self.table.total_buckets,
                    file=file,
                ))
        return commit_entries

    def _clean_up_reuse_tmp_manifests(
            self,
            delta_manifest_list: Optional[str],
            changelog_manifest_list: Optional[str],
            index_manifest: Optional[str] = None):
        """Clean up delta/changelog manifests and index manifest.

        Mirrors Java CommitCleaner.cleanUpReuseTmpManifests.
        """
        manifest_path = self.manifest_list_manager.manifest_path
        for ml_name in (delta_manifest_list, changelog_manifest_list):
            if ml_name:
                try:
                    for meta in self.manifest_list_manager.read(ml_name):
                        self.table.file_io.delete_quietly(
                            f"{self.manifest_file_manager.manifest_path}/{meta.file_name}")
                except Exception:
                    pass
                self.table.file_io.delete_quietly(f"{manifest_path}/{ml_name}")
        if index_manifest:
            self.table.file_io.delete_quietly(f"{manifest_path}/{index_manifest}")

    def _clean_up_no_reuse_tmp_manifests(
            self,
            base_manifest_list: Optional[str],
            merge_new_files: List[ManifestFileMeta]):
        """Clean up base manifest list and newly created merge manifests.

        Mirrors Java CommitCleaner.cleanUpNoReuseTmpManifests.
        """
        manifest_path = self.manifest_list_manager.manifest_path
        if base_manifest_list:
            self.table.file_io.delete_quietly(f"{manifest_path}/{base_manifest_list}")
        for meta in merge_new_files:
            self.table.file_io.delete_quietly(
                f"{self.manifest_file_manager.manifest_path}/{meta.file_name}")

    def abort(self, commit_messages: List[CommitMessage]):
        """Abort commit and delete files. Uses external_path if available to ensure proper scheme handling."""
        for message in commit_messages:
            for file in list(message.new_files) + list(message.changelog_files):
                try:
                    path_to_delete = file.external_path if file.external_path else file.file_path
                    if path_to_delete:
                        path_str = str(path_to_delete)
                        self.table.file_io.delete_quietly(path_str)
                except Exception as e:
                    path_to_delete = file.external_path if file.external_path else file.file_path
                    logger.warning(f"Failed to clean up file {path_to_delete} during abort: {e}")
            for entry in message.index_adds:
                try:
                    file_name = entry.index_file.file_name
                    index_path = (
                        entry.index_file.external_path
                        or self.table.path_factory()
                        .global_index_path_factory()
                        .to_path(file_name)
                    )
                    self.table.file_io.delete_quietly(index_path)
                except Exception as e:
                    logger.warning(
                        f"Failed to clean up index file {entry.index_file.file_name} during abort: {e}")

    def close(self):
        """Close the FileStoreCommit and release resources."""
        for callback in self.commit_callbacks:
            try:
                callback.close()
            except Exception:
                pass
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
                    'last_file_creation_time': 0,
                    'total_buckets': entry.total_buckets
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
                last_file_creation_time=stats['last_file_creation_time'],
                total_buckets=stats['total_buckets']
            )
            for stats in partition_stats.values()
        ]

    def _assign_snapshot_id(self, snapshot_id: int, commit_entries: List[ManifestEntry]) -> List[ManifestEntry]:
        """Assign snapshot ID to delta entries whose minSequenceNumber is 0."""
        result = []
        for entry in commit_entries:
            if entry.file.min_sequence_number == 0:
                result.append(entry.assign_sequence_number(snapshot_id, snapshot_id))
            else:
                result.append(entry)
        return result

    def _get_next_row_id_start(self, latest_snapshot) -> int:
        """Get the next row ID start from the latest snapshot."""
        if latest_snapshot and hasattr(latest_snapshot, 'next_row_id') and latest_snapshot.next_row_id is not None:
            return latest_snapshot.next_row_id
        return 0

    def _assign_row_tracking_meta(self, first_row_id_start: int, commit_entries: List[ManifestEntry]):
        """Assign row tracking metadata (first_row_id) to new files.

        Aligned with Java RowTrackingCommitUtils.assignRowTrackingMeta.
        """
        if not commit_entries:
            return commit_entries, first_row_id_start

        row_id_assigned = []
        start = first_row_id_start
        blob_start_default = first_row_id_start
        blob_starts = {}
        vector_store_start = first_row_id_start

        for entry in commit_entries:
            assert entry.file.file_source is not None, \
                f"file_source must be present for row-tracking table, file={entry.file.file_name}"

            write_cols = entry.file.write_cols
            contains_row_id = (
                write_cols is not None
                and SpecialFields.ROW_ID.name in write_cols
            )

            if (entry.file.file_source == 0
                    and entry.file.first_row_id is None
                    and not contains_row_id):
                row_count = entry.file.row_count

                if DataFileMeta.is_blob_file(entry.file.file_name):
                    blob_field_name = entry.file.write_cols[0]
                    blob_start = blob_starts.get(blob_field_name, blob_start_default)
                    if blob_start >= start:
                        raise RuntimeError(
                            f"This is a bug, blobStart {blob_start} should be less than "
                            f"start {start} when assigning a blob entry file."
                        )
                    row_id_assigned.append(entry.assign_first_row_id(blob_start))
                    blob_starts[blob_field_name] = blob_start + row_count

                elif DataFileMeta.is_vector_file(entry.file.file_name):
                    if vector_store_start >= start:
                        raise RuntimeError(
                            f"This is a bug, vectorStoreStart {vector_store_start} should be "
                            f"less than start {start} when assigning a vector-store entry file."
                        )
                    row_id_assigned.append(entry.assign_first_row_id(vector_store_start))
                    vector_store_start += row_count

                else:
                    row_id_assigned.append(entry.assign_first_row_id(start))
                    blob_start_default = start
                    blob_starts.clear()
                    start += row_count
            else:
                row_id_assigned.append(entry)

        return row_id_assigned, start
