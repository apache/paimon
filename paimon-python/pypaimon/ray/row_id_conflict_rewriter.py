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
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
################################################################################

"""Rebase Ray self-merge updates after concurrent compaction."""

import logging
import random
import time
from dataclasses import dataclass, replace
from typing import Dict, List, Optional, Sequence, Tuple

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.read.split import DataSplit
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.special_fields import SpecialFields
from pypaimon.utils.range import Range
from pypaimon.write.commit.conflict_detection import (
    RowIdLineageConflict,
    RowIdRebaseConflict,
)
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.file_store_commit import CommitResultUncertainError

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _StagedFile:
    message_index: int
    message: CommitMessage
    file: DataFileMeta


@dataclass(frozen=True)
class _RewriteResult:
    update_messages: List[CommitMessage]
    superseded_messages: List[CommitMessage]
    rewritten_file_count: int


def commit_self_merge_with_compaction_retry(
        table,
        update_messages: Sequence[CommitMessage],
        other_messages: Sequence[CommitMessage],
        *,
        num_partitions: int,
        ray_remote_args=None,
        base_snapshot_uuid=None,
) -> None:
    """Commit self-merge messages, rebasing stale updates when compaction wins."""
    current_updates = list(update_messages)
    other_messages = list(other_messages)
    superseded_messages = []
    current_base_snapshot_uuid = base_snapshot_uuid
    retry_count = 0
    start_millis = int(time.time() * 1000)

    def abort_known_uncommitted(include_current):
        messages = list(superseded_messages)
        superseded_messages[:] = []
        if include_current:
            messages.extend(current_updates)
            messages.extend(other_messages)
        if messages:
            from pypaimon.write.file_store_commit import (
                _abort_commit_messages,
            )
            _abort_commit_messages(table, messages)

    try:
        # Ray performs the rebase below without the local driver's size limit.
        commit_table = table.copy_without_time_travel({
            CoreOptions.DATA_EVOLUTION_ROW_ID_CONFLICT_REWRITE_MAX_SIZE.key():
                "0 B",
        })
        base_snapshot_ids = _base_snapshot_ids(current_updates)
        latest_snapshot = table.snapshot_manager().get_latest_snapshot()
    except Exception:
        abort_known_uncommitted(True)
        raise
    if (
            len(base_snapshot_ids) == 1
            and latest_snapshot is not None
            and latest_snapshot.id != next(iter(base_snapshot_ids))
    ):
        try:
            result = _rewrite_updates(
                table,
                current_updates,
                latest_snapshot,
                num_partitions=num_partitions,
                ray_remote_args=ray_remote_args,
                expected_base_snapshot_uuid=current_base_snapshot_uuid,
            )
            if result is not None:
                current_updates = result.update_messages
                superseded_messages.extend(result.superseded_messages)
                current_base_snapshot_uuid = latest_snapshot.uuid
                logger.info(
                    "Rewrote %d stale self-merge file(s) against snapshot %d "
                    "before committing to table %s.",
                    result.rewritten_file_count,
                    latest_snapshot.id,
                    table.identifier,
                )
        except Exception:
            abort_known_uncommitted(True)
            raise

    while True:
        commit = None
        commit_attempted = False
        conflict = None
        try:
            commit = commit_table.new_batch_write_builder().new_commit()
            # This layer owns stale row-id layout recovery. Do not enter the
            # legacy compaction-rollback loop before the distributed rebase;
            # ordinary FileStoreCommit retries remain enabled.
            commit.file_store_commit.rollback = None
            conflict_detection = commit.file_store_commit.conflict_detection
            conflict_detection.set_row_id_check_from_snapshot_uuid(
                current_base_snapshot_uuid
            )
            commit_attempted = True
            commit.commit(current_updates + other_messages)
        except CommitResultUncertainError:
            # An unobservable snapshot may reference these files.
            abort_known_uncommitted(False)
            raise
        except Exception as error:
            lineage_conflict = _find_error(error, RowIdLineageConflict)
            if lineage_conflict is not None:
                abort_known_uncommitted(True)
                if lineage_conflict is error:
                    raise
                raise lineage_conflict from error
            conflict = _find_row_id_conflict(error)
            if conflict is None:
                abort_known_uncommitted(not commit_attempted)
                raise
        finally:
            if commit is not None:
                try:
                    commit.close()
                except Exception as close_error:
                    logger.warning(
                        "Failed to close self-merge commit: %s",
                        close_error,
                        exc_info=close_error,
                    )

        if conflict is None:
            abort_known_uncommitted(False)
            return

        elapsed = int(time.time() * 1000) - start_millis
        if (
                elapsed > table.options.commit_timeout()
                or retry_count >= table.options.commit_max_retries()
        ):
            abort_known_uncommitted(True)
            raise conflict

        latest_snapshot = table.snapshot_manager().get_latest_snapshot()
        if latest_snapshot is None:
            abort_known_uncommitted(True)
            raise conflict

        try:
            result = _rewrite_updates(
                table,
                current_updates,
                latest_snapshot,
                num_partitions=num_partitions,
                ray_remote_args=ray_remote_args,
                expected_base_snapshot_uuid=current_base_snapshot_uuid,
            )
        except Exception as rewrite_error:
            abort_known_uncommitted(True)
            raise RuntimeError(
                "{} {}".format(conflict, rewrite_error)
            )
        if result is None:
            abort_known_uncommitted(True)
            raise conflict

        current_updates = result.update_messages
        superseded_messages.extend(result.superseded_messages)
        current_base_snapshot_uuid = latest_snapshot.uuid
        elapsed = int(time.time() * 1000) - start_millis
        if elapsed > table.options.commit_timeout():
            abort_known_uncommitted(True)
            raise conflict

        logger.info(
            "Rewrote %d stale self-merge file(s) against snapshot %d "
            "before retrying commit to table %s.",
            result.rewritten_file_count,
            latest_snapshot.id,
            table.identifier,
        )
        try:
            _retry_wait(table, retry_count)
        except Exception:
            abort_known_uncommitted(True)
            raise
        retry_count += 1


def _rewrite_updates(
        table,
        update_messages: List[CommitMessage],
        latest_snapshot,
        *,
        num_partitions: int,
        ray_remote_args=None,
        expected_base_snapshot_uuid=None,
) -> Optional[_RewriteResult]:
    if table.options.deletion_vectors_enabled(False):
        return None
    if latest_snapshot.next_row_id is None:
        return None
    if any(
            message.deleted_files or message.changelog_files
            for message in update_messages
    ):
        return None

    base_snapshot_ids = _base_snapshot_ids(update_messages)
    if len(base_snapshot_ids) != 1:
        return None
    base_snapshot = table.snapshot_manager().get_snapshot_by_id(
        next(iter(base_snapshot_ids))
    )
    if (
            expected_base_snapshot_uuid is not None
            and (
                base_snapshot is None
                or base_snapshot.uuid != expected_base_snapshot_uuid
            )
    ):
        raise RowIdLineageConflict(
            "Row-id snapshot lineage conflict: snapshot {} no longer has "
            "the staged UUID {}.".format(
                next(iter(base_snapshot_ids)),
                expected_base_snapshot_uuid,
            )
        )
    if base_snapshot is not None and latest_snapshot.id < base_snapshot.id:
        raise RowIdLineageConflict(
            "Latest snapshot {} is older than self-merge base snapshot {}."
            .format(latest_snapshot.id, base_snapshot.id)
        )
    if (
            base_snapshot is None
            or base_snapshot.schema_id != latest_snapshot.schema_id
    ):
        return None

    # The failed commit already checked for logical conflicts up to the
    # snapshot it observed. Repeat that check against the snapshot selected
    # for this rewrite to close the race between those two reads of latest.
    _validate_no_logical_conflict(
        table,
        update_messages,
        latest_snapshot,
        min(base_snapshot_ids),
    )

    scan_table = table.copy_without_time_travel({
        CoreOptions.SCAN_SNAPSHOT_ID.key(): str(latest_snapshot.id),
    })
    scan_plan = scan_table.new_read_builder().new_scan().plan_for_write()
    if scan_plan.snapshot_id != latest_snapshot.id:
        return None
    current_splits = list(scan_plan.splits())
    current_files = [
        (split, file)
        for split in current_splits
        for file in split.files
        if _is_normal_row_id_file(file)
    ]
    current_exact_ranges = {
        _range_key(tuple(split.partition.values), split.bucket, file)
        for split, file in current_files
    }

    staged = [
        _StagedFile(index, message, file)
        for index, message in enumerate(update_messages)
        for file in message.new_files
    ]
    if any(
            _is_dedicated_file(item.file)
            and item.file.first_row_id is not None
            and item.file.first_row_id < latest_snapshot.next_row_id
            for item in staged
    ):
        return None

    candidates = [
        item for item in staged
        if _is_rewrite_candidate(
            item,
            current_exact_ranges,
            latest_snapshot.next_row_id,
        )
    ]
    if not candidates:
        # The physical files changed but their row-id boundaries did not.
        # Logical history was validated above, so advancing the baseline is
        # sufficient and avoids rejecting another same-boundary compaction.
        return _RewriteResult(
            update_messages=[
                replace(
                    message,
                    check_from_snapshot=latest_snapshot.id,
                )
                for message in update_messages
            ],
            superseded_messages=[],
            rewritten_file_count=0,
        )
    if not _ranges_are_still_covered(current_files, candidates):
        return None

    candidates_by_message: Dict[int, List[DataFileMeta]] = {}
    for candidate in candidates:
        candidates_by_message.setdefault(candidate.message_index, []).append(
            candidate.file
        )
    candidate_ids = {id(candidate.file) for candidate in candidates}

    remaining_messages = []
    superseded_messages = []
    for index, message in enumerate(update_messages):
        kept_files = [
            file for file in message.new_files
            if id(file) not in candidate_ids
        ]
        remaining = replace(
            message,
            new_files=kept_files,
            check_from_snapshot=latest_snapshot.id,
        )
        if not remaining.is_empty():
            remaining_messages.append(remaining)

        replaced_files = candidates_by_message.get(index, [])
        if replaced_files:
            superseded_messages.append(CommitMessage(
                partition=message.partition,
                bucket=message.bucket,
                new_files=replaced_files,
                total_buckets=message.total_buckets,
            ))

    rewritten_messages = []
    groups: Dict[Tuple[str, ...], List[_StagedFile]] = {}
    for candidate in candidates:
        groups.setdefault(tuple(candidate.file.write_cols), []).append(
            candidate
        )
    try:
        for columns, files in groups.items():
            messages, rewritten_rows, _ = _rewrite_group(
                table,
                list(columns),
                files,
                latest_snapshot.id,
                num_partitions=num_partitions,
                ray_remote_args=ray_remote_args,
            )
            expected_rows = sum(item.file.row_count for item in files)
            if rewritten_rows != expected_rows:
                from pypaimon.write.file_store_commit import (
                    _abort_commit_messages,
                )
                _abort_commit_messages(table, messages)
                raise RuntimeError(
                    "Distributed row-id conflict rewrite read {} rows from "
                    "staged files, expected {}.".format(
                        rewritten_rows,
                        expected_rows,
                    )
                )
            rewritten_messages.extend(messages)
    except Exception:
        if rewritten_messages:
            from pypaimon.write.file_store_commit import _abort_commit_messages
            _abort_commit_messages(table, rewritten_messages)
        raise

    return _RewriteResult(
        update_messages=remaining_messages + rewritten_messages,
        superseded_messages=superseded_messages,
        rewritten_file_count=len(candidates),
    )


def _base_snapshot_ids(update_messages: Sequence[CommitMessage]):
    return {
        message.check_from_snapshot
        for message in update_messages
        if message.check_from_snapshot is not None
        and message.check_from_snapshot >= 0
    }


def _rewrite_group(
        table,
        columns: List[str],
        candidates: List[_StagedFile],
        snapshot_id: int,
        *,
        num_partitions: int,
        ray_remote_args=None,
):
    import ray

    from pypaimon.ray.data_evolution_merge_join import (
        distributed_update_apply,
    )
    from pypaimon.read.datasource.ray_datasource import RayDatasource
    from pypaimon.read.datasource.split_provider import (
        PreResolvedSplitProvider,
    )

    read_type = [table.field_dict[name] for name in columns]
    read_type.append(SpecialFields.ROW_ID)
    splits = [
        DataSplit(
            files=[candidate.file],
            partition=GenericRow(
                list(candidate.message.partition),
                table.partition_keys_fields,
            ),
            bucket=candidate.message.bucket,
            raw_convertible=True,
        )
        for candidate in candidates
    ]
    provider = PreResolvedSplitProvider(
        table,
        splits,
        read_type,
    )
    parallelism = max(1, min(num_partitions, len(splits)))
    staged_updates = ray.data.read_datasource(
        RayDatasource(provider),
        ray_remote_args=ray_remote_args,
        concurrency=parallelism,
        override_num_blocks=parallelism,
    )
    return distributed_update_apply(
        staged_updates,
        table,
        columns,
        num_partitions=num_partitions,
        ray_remote_args=ray_remote_args,
        base_snapshot_id=snapshot_id,
    )


def _validate_no_logical_conflict(
        table,
        update_messages: List[CommitMessage],
        latest_snapshot,
        base_snapshot_id: int,
) -> None:
    """Reject non-compaction changes before advancing the rewrite baseline."""
    commit = table.new_batch_write_builder().new_commit()
    try:
        file_store_commit = commit.file_store_commit
        file_store_commit.conflict_detection._row_id_check_from_snapshot = (
            base_snapshot_id
        )
        entries = file_store_commit._collect_manifest_entries(update_messages)
        conflict = file_store_commit.conflict_detection.check_row_id_from_snapshot(
            latest_snapshot,
            entries,
            check_compaction=False,
        )
        if conflict is not None:
            raise conflict
    finally:
        commit.close()


def _find_row_id_conflict(error) -> Optional[RowIdRebaseConflict]:
    return _find_error(error, RowIdRebaseConflict)


def _find_error(error, error_type):
    seen = set()
    current = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, error_type):
            return current
        current = (
            getattr(current, "cause", None)
            or getattr(current, "__cause__", None)
        )
    return None


def _is_rewrite_candidate(
        item: _StagedFile,
        current_exact_ranges,
        next_row_id: int,
) -> bool:
    file = item.file
    return (
        _is_normal_row_id_file(file)
        and file.first_row_id < next_row_id
        and bool(file.write_cols)
        and not any(
            SpecialFields.is_system_field(name)
            for name in file.write_cols
        )
        and _range_key(item.message.partition, item.message.bucket, file)
        not in current_exact_ranges
    )


def _ranges_are_still_covered(current_files, candidates) -> bool:
    current_ranges = {}
    for split, file in current_files:
        key = (tuple(split.partition.values), split.bucket)
        current_ranges.setdefault(key, []).append(file.row_id_range())
    current_ranges = {
        key: Range.sort_and_merge_overlap(ranges, True, True)
        for key, ranges in current_ranges.items()
    }
    for candidate in candidates:
        key = (tuple(candidate.message.partition), candidate.message.bucket)
        if candidate.file.row_id_range().exclude(
                current_ranges.get(key, [])):
            return False
    return True


def _range_key(partition, bucket, file):
    return (
        tuple(partition),
        bucket,
        file.first_row_id,
        file.row_count,
    )


def _is_normal_row_id_file(file) -> bool:
    return file.first_row_id is not None and not _is_dedicated_file(file)


def _is_dedicated_file(file) -> bool:
    return (
        DataFileMeta.is_blob_file(file.file_name)
        or DataFileMeta.is_vector_file(file.file_name)
    )


def _retry_wait(table, retry_count: int) -> None:
    wait_millis = min(
        table.options.commit_min_retry_wait() * (2 ** retry_count),
        table.options.commit_max_retry_wait(),
    )
    jitter = random.randint(0, max(1, int(wait_millis * 0.2)))
    time.sleep((wait_millis + jitter) / 1000.0)
