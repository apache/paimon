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

"""Rebase staged Ray row-id updates after concurrent compaction."""

import logging
import random
import sys
import time
from dataclasses import dataclass, replace
from typing import Dict, List, Optional, Sequence, Tuple

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.read.split import DataSplit
from pypaimon.schema.data_types import RowType, collect_field_ids
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.special_fields import SpecialFields
from pypaimon.utils.range import Range
from pypaimon.write.row_id_conflict import (
    RowIdExistenceConflict, check_row_id_conflicts, validate_row_id_update_fields,
)
from pypaimon.write.commit_message import CommitMessage

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _StagedFile:
    message_index: int
    message: CommitMessage
    file: DataFileMeta


@dataclass(frozen=True)
class _RewriteResult:
    update_messages: List[CommitMessage]
    rewritten_file_count: int


def commit_updates_with_compaction_retry(
        table,
        update_messages: Sequence[CommitMessage],
        other_messages: Sequence[CommitMessage],
        *,
        num_partitions: Optional[int],
        ray_remote_args=None,
) -> None:
    """Commit row-id update messages, rebasing stale files with Ray workers."""
    current_updates = list(update_messages)
    other_messages = list(other_messages)
    retry_count = 0
    timeout_millis = table.options.commit_timeout()
    deadline = time.monotonic() + timeout_millis / 1000.0
    rewritten = False
    rewrite_enabled = table.options.data_evolution_row_id_conflict_rewrite_max_size() > 0

    # Ray performs the rebase below without the local driver's size limit.
    commit_table = table.copy_without_time_travel({
        CoreOptions.DATA_EVOLUTION_ROW_ID_CONFLICT_REWRITE_MAX_SIZE.key():
            "0 B",
    })
    base_snapshot_ids = _base_snapshot_ids(current_updates)
    latest_snapshot = table.snapshot_manager().get_latest_snapshot()
    if (
            rewrite_enabled
            and len(base_snapshot_ids) == 1
            and latest_snapshot is not None
            and latest_snapshot.id != next(iter(base_snapshot_ids))
    ):
        result = _rewrite_updates(
            table,
            current_updates,
            latest_snapshot,
            num_partitions=num_partitions,
            ray_remote_args=ray_remote_args,
            deadline=deadline,
        )
        if result is not None:
            rewritten = True
            current_updates = result.update_messages
            logger.info(
                "Rewrote %d stale row-id update file(s) against snapshot %d "
                "before committing to table %s.",
                result.rewritten_file_count,
                latest_snapshot.id,
                table.identifier,
            )

    # Every outer rebase attempt creates a fresh commit. Preserve the commit
    # user even when a schema refresh requires replacing the write builder.
    write_builder = commit_table.new_batch_write_builder()
    while True:
        commit = None
        conflict = None
        try:
            # DDL need not publish a data snapshot. Validate every staged
            # update, including unchanged ranges and explicitly disabled
            # recovery, against the schema used by this commit attempt.
            current_table = _with_latest_schema(commit_table)
            _validate_update_fields(current_table, current_updates)
            if rewritten:
                remaining_millis = _check_commit_deadline(deadline)
                if timeout_millis < sys.maxsize:
                    # Share the budget without reverting the refreshed write
                    # schema to the schema of a historical scan snapshot.
                    current_table = current_table.copy_without_time_travel({
                        CoreOptions.COMMIT_TIMEOUT.key(): '{} ms'.format(remaining_millis)})
            if current_table is not commit_table:
                commit_table = current_table
                write_builder = commit_table.new_batch_write_builder().with_commit_user(
                    write_builder.commit_user)
            if rewritten:
                _check_commit_deadline(deadline)
            commit = write_builder.new_commit()
            if rewritten:
                _check_commit_deadline(deadline)
            commit.commit(current_updates + other_messages)
        except Exception as error:
            conflict = _find_row_id_conflict(error)
            if conflict is None:
                raise
        finally:
            if commit is not None:
                try:
                    commit.close()
                except Exception as close_error:
                    logger.warning(
                        "Failed to close row-id update commit: %s",
                        close_error,
                        exc_info=close_error,
                    )

        if conflict is None:
            return
        if not rewrite_enabled:
            raise conflict

        if (
                time.monotonic() > deadline
                or retry_count >= table.options.commit_max_retries()
        ):
            raise conflict

        latest_snapshot = table.snapshot_manager().get_latest_snapshot()
        if latest_snapshot is None:
            raise conflict

        try:
            result = _rewrite_updates(
                table,
                current_updates,
                latest_snapshot,
                num_partitions=num_partitions,
                ray_remote_args=ray_remote_args,
                deadline=deadline,
            )
        except TimeoutError:
            raise
        except Exception as rewrite_error:
            raise RuntimeError(
                "{} {}".format(conflict, rewrite_error)
            ) from conflict
        if result is None:
            raise conflict

        current_updates = result.update_messages
        rewritten = True
        _check_commit_deadline(deadline)

        logger.info(
            "Rewrote %d stale row-id update file(s) against snapshot %d "
            "before retrying commit to table %s.",
            result.rewritten_file_count,
            latest_snapshot.id,
            table.identifier,
        )
        _retry_wait(table, retry_count)
        retry_count += 1


def _rewrite_updates(
        table,
        update_messages: List[CommitMessage],
        latest_snapshot,
        *,
        num_partitions: Optional[int],
        ray_remote_args=None,
        deadline=None,
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
    if base_snapshot is None:
        return None

    table = _with_latest_schema(table)
    _validate_update_fields(table, update_messages)
    scan_table = table.copy_without_time_travel({
        CoreOptions.SCAN_SNAPSHOT_ID.key(): str(latest_snapshot.id),
    })
    scan_plan = (scan_table.new_read_builder()
                 .with_partition_filter(_update_partition_filter(table, update_messages))
                 .new_scan().plan_all())
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
            table,
            item,
            current_exact_ranges,
            latest_snapshot.next_row_id,
        )
    ]
    if not candidates:
        return None
    if not _ranges_are_still_covered(current_files, candidates):
        return None

    # An append can advance the snapshot without changing update file ranges.
    # Ordinary commits retain their own validation; only actual rewrites need
    # this extra check before advancing the conflict baseline.
    _validate_no_logical_conflict(
        table,
        update_messages,
        latest_snapshot,
        base_snapshot,
    )

    candidate_ids = {id(candidate.file) for candidate in candidates}

    remaining_messages = []
    for message in update_messages:
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

    rewritten_messages = []
    groups: Dict[Tuple[str, ...], List[_StagedFile]] = {}
    for candidate in candidates:
        groups.setdefault(tuple(_partial_file_write_cols(
            table, candidate.file)), []).append(
            candidate
        )
    for columns, files in groups.items():
        _check_commit_deadline(deadline)
        partitions = {(tuple(item.message.partition), item.message.bucket) for item in files}
        # Keep complete splits, including concurrent partial-column files and
        # dedicated files, from the same snapshot used by the conflict check.
        routing_plan = scan_plan._with_splits([
            split for split in current_splits
            if (tuple(split.partition.values), split.bucket) in partitions
        ])
        messages, rewritten_rows, _ = _rewrite_group(
            table,
            list(columns),
            files,
            latest_snapshot.id,
            num_partitions=num_partitions,
            ray_remote_args=ray_remote_args,
            scan_plan=routing_plan,
        )
        _check_commit_deadline(deadline)
        expected_rows = sum(item.file.row_count for item in files)
        if rewritten_rows != expected_rows:
            raise RuntimeError(
                "Distributed row-id conflict rewrite read {} rows from "
                "staged files, expected {}.".format(
                    rewritten_rows,
                    expected_rows,
                )
            )
        rewritten_messages.extend(messages)

    return _RewriteResult(
        update_messages=remaining_messages + rewritten_messages,
        rewritten_file_count=len(candidates),
    )


def _base_snapshot_ids(update_messages: Sequence[CommitMessage]):
    return {
        message.check_from_snapshot
        for message in update_messages
        if message.check_from_snapshot is not None
        and message.check_from_snapshot >= 0
    }


def _check_commit_deadline(deadline):
    if deadline is not None:
        remaining = deadline - time.monotonic()
        if remaining < 0:
            raise TimeoutError('Row-id update commit timed out before snapshot publication')
        return int(remaining * 1000)


def _update_partition_filter(table, update_messages):
    if not table.partition_keys:
        return None
    builder = PredicateBuilder(table.partition_keys_fields)
    partitions = {tuple(message.partition) for message in update_messages if message.new_files}
    return builder.or_predicates([
        builder.and_predicates([
            builder.is_null(key) if value is None else builder.equal(key, value)
            for key, value in zip(table.partition_keys, partition)
        ])
        for partition in partitions
    ])


def _validate_update_fields(table, update_messages):
    validate_row_id_update_fields(
        table.table_schema, table.schema_manager,
        (file for message in update_messages for file in message.new_files))


def _rewrite_group(
        table,
        columns: List[str],
        candidates: List[_StagedFile],
        snapshot_id: int,
        *,
        num_partitions: Optional[int],
        scan_plan,
        ray_remote_args=None,
):
    import ray

    from pypaimon.ray.data_evolution_merge_join import (
        distributed_update_apply,
    )
    from pypaimon.ray.partitioning import _resolve_num_partitions
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
    estimated_size_bytes = sum(candidate.file.file_size for candidate in candidates)
    estimated_num_rows = sum(candidate.file.row_count for candidate in candidates)
    parallelism = max(1, min(
        _resolve_num_partitions(num_partitions, estimated_size_bytes), len(splits)))
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
        estimated_size_bytes=estimated_size_bytes,
        estimated_num_rows=estimated_num_rows,
        scan_plan=scan_plan,
    )


def _validate_no_logical_conflict(
        table,
        update_messages: List[CommitMessage],
        latest_snapshot,
        base_snapshot,
) -> None:
    """Reject non-compaction changes before advancing the rewrite baseline."""
    # The native checker requires identical snapshot schema IDs. Choose the
    # Python read-only check for this unsupported case before dispatch, while
    # leaving the actual commit backend unchanged. Native check failures must
    # still propagate without retrying the check through another backend.
    if (table.options.native_commit_enabled()
            and base_snapshot.schema_id != latest_snapshot.schema_id):
        table = table.copy({CoreOptions.COMMIT_NATIVE_ENABLED.key(): 'false'})
    check_row_id_conflicts(table, update_messages, latest_snapshot.id, base_snapshot.id)


def _find_row_id_conflict(error) -> Optional[RowIdExistenceConflict]:
    seen = set()
    current = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, RowIdExistenceConflict):
            return current
        current = (
            getattr(current, "cause", None)
            or getattr(current, "__cause__", None)
        )
    return None


def _is_rewrite_candidate(
        table,
        item: _StagedFile,
        current_exact_ranges,
        next_row_id: int,
) -> bool:
    file = item.file
    if (not _is_normal_row_id_file(file)
            or file.first_row_id >= next_row_id
            or _range_key(item.message.partition, item.message.bucket, file) in current_exact_ranges):
        return False
    write_cols = _partial_file_write_cols(table, file)
    return (
        bool(write_cols)
        and not any(
            SpecialFields.is_system_field(name)
            for name in write_cols
        )
    )


def _partial_file_write_cols(table, file: DataFileMeta):
    schema = (
        table.table_schema
        if file.schema_id == table.table_schema.id
        else table.schema_manager.get_schema(file.schema_id)
    )
    if schema is None:
        raise RuntimeError(f"Schema {file.schema_id} not found")
    columns = schema.partial_file_write_cols(file.write_cols)
    if columns is not None and any(SpecialFields.is_system_field(name) for name in columns):
        return columns
    current_fields = {field.id: field for field in table.table_schema.fields}
    file_fields = {field.name: field for field in schema.fields}
    fields = []
    for name in columns if columns is not None else file_fields:
        if name not in file_fields:
            raise RuntimeError("Column '{}' not found in schema {}".format(name, file.schema_id))
        fields.append(file_fields[name])
    validate_row_id_update_fields(table.table_schema, table.schema_manager, [file])
    if columns is not None:
        for field in fields:
            if isinstance(field.type, RowType):
                written_ids, current_ids = set(), set()
                collect_field_ids(field.type, written_ids)
                collect_field_ids(current_fields[field.id].type, current_ids)
                if not current_ids.issubset(written_ids):
                    # Reading the old file with the current ROW type fills added
                    # children with nulls. A whole-ROW rewrite would publish those
                    # nulls over concurrent partial writes to the new children.
                    raise RuntimeError(
                        "Row-id conflict rewrite cannot expand ROW field '{}' with added nested fields; "
                        "restage the update against the current schema".format(field.name))
    # Resolve renamed fields by identity, including when an old name has been
    # reused. TableRead applies the normal schema-evolution casts to the data.
    return None if columns is None else [current_fields[field.id].name for field in fields]


def _with_latest_schema(table):
    from pypaimon.table.file_store_table import FileStoreTable

    schema = table.schema_manager.latest()
    if schema is None:
        raise RuntimeError('Cannot load the current schema for row-id conflict rewrite')
    if schema.id == table.table_schema.id:
        return table
    return FileStoreTable(
        table.file_io, table.identifier, table.table_path,
        schema.copy(new_options=table.options.options.to_map()), table.catalog_environment)


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
