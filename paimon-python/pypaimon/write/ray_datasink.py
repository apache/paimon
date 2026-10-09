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

"""
Module to write a Paimon table from a Ray Dataset, by using the Ray Datasink API.
"""

import logging
import time
import traceback
import uuid
from dataclasses import dataclass
from typing import (TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Set)

import ray
from ray.data.datasource.datasink import Datasink

from ray.util.annotations import DeveloperAPI
from ray.data.block import BlockAccessor, Block
from ray.data._internal.execution.interfaces import TaskContext
import pyarrow as pa

from pypaimon.write.commit_callback import CommitCallback

if TYPE_CHECKING:
    from pypaimon.table.table import Table
    from pypaimon.write.write_builder import WriteBuilder
    from pypaimon.write.commit_message import CommitMessage

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PaimonWriteResult:
    """Metadata reported by the exact coordinator commit."""

    row_count: int
    snapshot_id: int


class _SnapshotIdRecorder(CommitCallback):

    def __init__(self):
        self.snapshot_id = None

    def call(self, context):
        self.snapshot_id = context.snapshot.id


class _TaskCommitMessages(list):
    """Commit messages plus the logical rows written by one Ray task."""

    def __init__(self, messages=(), row_count=0):
        super().__init__(messages)
        self.row_count = row_count


def _cast_binary_to_table_schema(table: pa.Table, target_schema: pa.Schema) -> pa.Table:
    """Restore binary layouts using the table's BYTES/BLOB contract.

    Python dict inference loses BLOB's large_binary layout, while Ray block
    promotion may widen BYTES. Neither transformation changes the table type.
    """
    cast_indices = []
    for i, field in enumerate(table.schema):
        target_field = target_schema.field(field.name) if field.name in target_schema.names else None
        if (target_field and field.type != target_field.type
                and all(pa.types.is_binary(t) or pa.types.is_large_binary(t)
                        for t in (field.type, target_field.type))):
            cast_indices.append(i)

    if not cast_indices:
        return table

    columns = table.columns
    for i in cast_indices:
        columns[i] = columns[i].cast(target_schema.field(table.schema[i].name).type, safe=True)
    fields = [target_schema.field(f.name) if i in cast_indices else f
              for i, f in enumerate(table.schema)]
    return pa.table(columns, schema=pa.schema(fields, metadata=table.schema.metadata))


def _current_job_id() -> Optional[str]:
    """Best-effort Ray job id, or None when the runtime context is
    unavailable (e.g. a no-Ray unit test)."""
    try:
        job_id = ray.get_runtime_context().get_job_id()
        return str(job_id) if job_id else None
    except Exception:
        return None


def _build_commit_tag(ctx: TaskContext) -> str:
    """Build a commit message tag identifying the Ray worker/task."""
    parts = []
    if ctx is not None and hasattr(ctx, "task_idx"):
        parts.append(f"worker={ctx.task_idx}")
    job_id = _current_job_id()
    if job_id is not None:
        parts.append(f"job={job_id}")
    return " ".join(parts) if parts else "ray-write"


# Re-reads of the dedup bound when the snapshot it named expires mid-read (see
# PaimonCommitterActor.dedup_base). Runs once per write before any task.
DEDUP_ANCHOR_READ_ATTEMPTS = 3
DEDUP_ANCHOR_READ_WAIT_SECONDS = 0.2


def _is_first_actor_incarnation() -> bool:
    """Whether the calling actor has never been restarted.

    Only a first incarnation can claim to have seen every commit its write made,
    which is what lets an absent commit_user be trusted as "not a replay".

    Only an explicit False counts. The property is not guaranteed to return a
    bool: the GCS actor lookup behind it yields ``{}`` when it finds no record,
    and ``actor_info and ...`` passes that straight through -- plausibly
    falsy-but-not-False, and ``not {}`` is True. Treated as a bool that would
    read as "never restarted" for an actor whose restart history is precisely
    what could not be read, and a restarted actor has an empty
    ``_attempted_users``, so a replay would skip the scan and commit twice.

    So every other answer -- True, {}, None, an exception, no actor context --
    resolves to False. That costs a scan the caller would otherwise have
    skipped, and nothing else.
    """
    try:
        return ray.get_runtime_context().was_current_actor_reconstructed is False
    except Exception:
        return False


# Python 3.8 / Ray 2.10: Datasink is not subscriptable at runtime
try:
    _DatasinkBase = Datasink[List["CommitMessage"]]
except TypeError:
    _DatasinkBase = Datasink


@DeveloperAPI
class PaimonDatasink(_DatasinkBase):
    def __init__(
        self,
        table: "Table",
        overwrite: bool = False,
        static_partition: Optional[Dict[str, Any]] = None,
        postpone_bucket_plan=None,
        options: Optional[Dict[str, str]] = None,
    ):
        self.table = table
        self.overwrite = overwrite
        self.static_partition = static_partition
        self._postpone_bucket_plan = postpone_bucket_plan
        self._options = dict(options) if options else None
        self._table_name = table.identifier.get_full_name()
        self._writer_builder: Optional["WriteBuilder"] = None
        self.commit_result: Optional[PaimonWriteResult] = None

    def _is_overwrite(self) -> bool:
        return self.overwrite or self.static_partition is not None

    def __getstate__(self) -> dict:
        state = self.__dict__.copy()
        return state

    def __setstate__(self, state: dict) -> None:
        self.__dict__.update(state)
        writer_builder = getattr(self, '_writer_builder', None)
        if writer_builder is not None and not hasattr(writer_builder, 'table'):
            self._writer_builder = None
        if not hasattr(self, '_table_name'):
            self._table_name = self.table.identifier.get_full_name()
        if not hasattr(self, 'static_partition'):
            self.static_partition = None
        if not hasattr(self, '_postpone_bucket_plan'):
            self._postpone_bucket_plan = None
        if not hasattr(self, '_options'):
            self._options = None

    def on_write_start(self, schema=None) -> None:
        logger.info(f"Starting write job for table {self._table_name}")

        self._writer_builder = self.table.new_batch_write_builder()
        if self._options:
            self._writer_builder = self._writer_builder.with_options(self._options)
        if self._is_overwrite():
            self._writer_builder = self._writer_builder.overwrite(self.static_partition)

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> List["CommitMessage"]:
        commit_messages_list: List["CommitMessage"] = []
        row_count = 0
        table_write = None

        try:
            writer_builder = (
                self.table.new_postpone_fixed_bucket_write_builder()
                if self._postpone_bucket_plan is not None
                else self.table.new_batch_write_builder()
            )
            if self._options:
                writer_builder = writer_builder.with_options(self._options)
            if self._is_overwrite():
                writer_builder = writer_builder.overwrite(self.static_partition)

            if self._postpone_bucket_plan is not None:
                writer_builder.with_bucket_plan(self._postpone_bucket_plan)
            table_write = writer_builder.new_write()

            table_schema = self.table.table_schema
            from pypaimon.schema.data_types import PyarrowFieldParser
            target_pa_schema = PyarrowFieldParser.from_paimon_schema(table_schema.fields)

            for block in blocks:
                block_arrow: pa.Table = BlockAccessor.for_block(block).to_arrow()

                if block_arrow.num_rows == 0:
                    continue
                row_count += block_arrow.num_rows

                block_arrow = _cast_binary_to_table_schema(block_arrow, target_pa_schema)

                table_write.write_arrow(block_arrow)

            commit_messages = table_write.prepare_commit()
            commit_messages_list.extend(commit_messages)

            table_write.close()
            table_write = None
            return _TaskCommitMessages(commit_messages_list, row_count)
        except Exception:
            if table_write is not None:
                try:
                    table_write.abort()
                except Exception as abort_error:
                    logger.warning(
                        f"Error aborting worker-side table_write: {abort_error}",
                        exc_info=abort_error
                    )
            # prepare_commit has handed these files off, but a failed close
            # prevents this worker from returning them to the coordinator.
            # Never delete files from CommitMessage on failure.
            # A commit can succeed even when its response raises an exception.
            raise

    @staticmethod
    def _extract_write_returns(write_result: Any):
        """Normalize WriteResult.write_returns (Ray 2.44+) vs list of returns
        (older Ray) into a list of per-task commit-message lists."""
        if hasattr(write_result, "write_returns"):
            return write_result.write_returns
        if isinstance(write_result, list):
            return write_result
        raise TypeError(
            f"Unexpected write_result type {type(write_result).__name__}: "
            "expected object with .write_returns or list of commit message "
            "lists. Refusing to proceed to avoid silent data loss."
        )

    @staticmethod
    def _extract_row_count(write_result: Any, write_returns) -> int:
        if hasattr(write_result, "num_rows"):
            return int(write_result.num_rows)
        return sum(
            int(getattr(messages, "row_count", 0))
            for messages in write_returns
        )

    def on_write_complete(
        self, write_result: Any
    ):
        table_commit = None
        try:
            write_returns = self._extract_write_returns(write_result)
            all_commit_messages = [
                commit_message
                for commit_messages in write_returns
                for commit_message in commit_messages
            ]

            non_empty_messages = [
                msg for msg in all_commit_messages if not msg.is_empty()
            ]

            if not non_empty_messages and not self._is_overwrite():
                logger.info("No data to commit (all commit messages are empty)")
                return

            # Ray does not call on_write_start when the input has no blocks.
            if self._writer_builder is None:
                self.on_write_start()

            logger.info(
                f"Committing {len(non_empty_messages)} commit messages "
                f"for table {self._table_name}"
            )

            table_commit = self._writer_builder.new_commit()
            recorder = _SnapshotIdRecorder()
            table_commit.add_commit_callback(recorder)
            table_commit.commit(non_empty_messages)
            if recorder.snapshot_id is not None:
                self.commit_result = PaimonWriteResult(
                    row_count=self._extract_row_count(
                        write_result, write_returns),
                    snapshot_id=recorder.snapshot_id,
                )

            logger.info(f"Successfully committed write job for table {self._table_name}")
        except Exception as e:
            logger.error(
                f"Error committing write job for table {self._table_name}: {e}",
                exc_info=e
            )
            raise
        finally:
            if table_commit is not None:
                try:
                    table_commit.close()
                except Exception as e:
                    logger.warning(
                        f"Error closing table_commit: {e}",
                        exc_info=e
                    )

    def on_write_failed(self, error: Exception) -> None:
        logger.error(
            f"Write job failed for table {self._table_name}. Error: {error}",
            exc_info=error
        )
        # Do not abort files returned by completed write tasks. Ray or an outer
        # scheduler may replay those commit messages in another attempt.


def write_paimon_dataset(
    dataset,
    table,
    *,
    overwrite: bool = False,
    static_partition: Optional[Dict[str, Any]] = None,
    concurrency: Optional[int] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    hash_fixed_precluster: str = "auto",
    postpone_bucket_planner=None,
    commit_mode: str = "two_phase",
    options: Optional[Dict[str, str]] = None,
) -> Optional[PaimonWriteResult]:
    """Write a Ray Dataset through the safe path for the table's bucket mode.

    Args:
        dataset: Ray Dataset to write.
        table: Paimon table to write to.
        overwrite: Whether to overwrite existing data. Only supported with
            commit_mode='two_phase'.
        static_partition: Static partition spec for overwrite. Only supported
            with commit_mode='two_phase'.
        concurrency: Number of concurrent write tasks.
        ray_remote_args: Ray remote arguments for write tasks.
        hash_fixed_precluster: Pre-clustering strategy for HASH_FIXED tables.
        postpone_bucket_planner: Postpone bucket planner instance.
        commit_mode: Commit mode - 'two_phase' (default, coordinator commits
            after all tasks finish) or 'actor' (each task commits via a
            singleton actor for serialized commits without lock contention).
        options: Optional dynamic table options to override defaults at write
            time, e.g. {"target-file-size": "256mb"}.

    Returns:
        PaimonWriteResult with row count and snapshot id, or None if no data
        was written.
    """
    from pypaimon.ray.shuffle import (
        HASH_FIXED_PRECLUSTER_MAP_GROUPS,
        HASH_FIXED_PRECLUSTER_MODES,
        HASH_FIXED_PRECLUSTER_OFF,
        maybe_apply_repartition,
    )
    from pypaimon.table.bucket_mode import BucketMode

    if commit_mode not in ("two_phase", "actor"):
        raise ValueError(
            f"Unknown commit_mode={commit_mode!r}; "
            "expected 'two_phase' or 'actor'."
        )

    if hash_fixed_precluster not in HASH_FIXED_PRECLUSTER_MODES:
        raise ValueError(
            "hash_fixed_precluster must be one of {}, got {!r}".format(
                sorted(HASH_FIXED_PRECLUSTER_MODES),
                hash_fixed_precluster,
            )
        )

    # Actor mode does not support overwrite or static_partition
    if commit_mode == "actor" and (overwrite or static_partition is not None):
        raise ValueError(
            "commit_mode='actor' does not support overwrite=True or "
            "static_partition; atomic overwrite requires a single coordinated "
            "commit. Use commit_mode='two_phase' for overwrite."
        )

    # Actor mode conflicts with map_groups shuffle (barrier defeats incremental visibility)
    if commit_mode == "actor" and hash_fixed_precluster == HASH_FIXED_PRECLUSTER_MAP_GROUPS:
        raise ValueError(
            "commit_mode='actor' does not support hash_fixed_precluster='map_groups'. "
            "The map_groups barrier conflicts with incremental commit visibility. "
            "Use commit_mode='two_phase' or hash_fixed_precluster='auto'/'off'."
        )

    # POSTPONE tables are routed to a coordinator-committed path below, which
    # would silently downgrade an actor request to two-phase semantics. Reject
    # instead of pretending the request was honoured.
    if commit_mode == "actor" and table.bucket_mode() == BucketMode.POSTPONE_MODE:
        raise ValueError(
            "commit_mode='actor' does not support POSTPONE bucket tables; "
            "their bucket plan is resolved on the driver and committed by a "
            "coordinator. Use commit_mode='two_phase'."
        )

    if (
        table.bucket_mode() == BucketMode.POSTPONE_MODE
        and (
            postpone_bucket_planner is not None
            or (
                table.options.postpone_batch_write_fixed_bucket()
                and hash_fixed_precluster != HASH_FIXED_PRECLUSTER_OFF
            )
        )
    ):
        from pypaimon.write.postpone_bucket import (
            PostponeBucketPlanner,
        )
        from pypaimon.write.row_key_extractor import (
            PostponeFixedBucketRowKeyExtractor,
        )

        planner = (
            postpone_bucket_planner
            if postpone_bucket_planner is not None
            else PostponeBucketPlanner(
                table, known_num_buckets={}
                if overwrite or static_partition is not None else None)
        )
        plan = planner.current_plan()
        if table.partition_keys or not plan.contains(()):
            dataset, partition_stats = _collect_partition_stats(
                dataset, planner
            )
            plan = planner.plan(
                partition_stats,
                include_postpone_rows=not (
                    overwrite or static_partition is not None
                ),
            )
        return _write_primary_key_groups(
            dataset,
            table,
            overwrite=overwrite,
            static_partition=static_partition,
            concurrency=concurrency,
            ray_remote_args=ray_remote_args,
            bucket_extractor=PostponeFixedBucketRowKeyExtractor(table, plan),
            postpone_bucket_plan=plan,
            options=options,
        )

    if (
        hash_fixed_precluster == HASH_FIXED_PRECLUSTER_MAP_GROUPS
        and table.bucket_mode() == BucketMode.HASH_FIXED
        and getattr(table, "is_primary_key_table", False)
    ):
        return _write_primary_key_groups(
            dataset,
            table,
            overwrite=overwrite,
            static_partition=static_partition,
            concurrency=concurrency,
            ray_remote_args=ray_remote_args,
            options=options,
        )

    dataset = maybe_apply_repartition(dataset, table, hash_fixed_precluster)

    if commit_mode == "actor":
        # Actor mode: serialize commits through a singleton actor
        catalog_context = table.catalog_environment.catalog_context()
        if catalog_context is None:
            raise ValueError(
                "commit_mode='actor' requires a table with catalog_context. "
                "This table may have been created without a catalog."
            )
        catalog_options = catalog_context.options.to_map()
        table_identifier = table.identifier.get_full_name()

        datasink = PaimonActorDatasink(
            table,
            catalog_options=catalog_options,
            table_identifier=table_identifier,
            overwrite=overwrite,
            static_partition=static_partition,
            postpone_bucket_plan=None,
            options=options,
        )
    else:
        # Two-phase mode: coordinator commits after all tasks finish
        datasink = PaimonDatasink(
            table,
            overwrite=overwrite,
            static_partition=static_partition,
            options=options,
        )

    dataset.write_datasink(
        datasink,
        concurrency=concurrency,
        ray_remote_args=ray_remote_args,
    )
    return datasink.commit_result


def _ray_map_kwargs(method, concurrency, ray_remote_args, **kwargs):
    import inspect

    if concurrency is not None:
        concurrency_param = inspect.signature(
            method
        ).parameters.get("concurrency")
        if (
            concurrency_param is not None
            and concurrency_param.kind != inspect.Parameter.VAR_KEYWORD
        ):
            kwargs["concurrency"] = concurrency
        else:
            from ray.data._internal.compute import TaskPoolStrategy
            kwargs["compute"] = TaskPoolStrategy(size=concurrency)
    if ray_remote_args:
        kwargs.update(ray_remote_args)
    return kwargs


def _consume_write_results(
    results,
    coordinator,
    message_col,
    error_col=None,
) -> Optional[PaimonWriteResult]:
    import pickle

    write_returns = []
    errors = []
    try:
        for batch in results.iter_batches(batch_format="pyarrow"):
            messages = batch.column(message_col).to_pylist()
            batch_errors = (
                batch.column(error_col).to_pylist()
                if error_col is not None else [None] * len(messages)
            )
            for blob, error in zip(messages, batch_errors):
                commit_messages = pickle.loads(blob)
                write_returns.append(commit_messages)
                if error is not None:
                    errors.append(error)
        if errors:
            raise RuntimeError(
                "One or more Ray write tasks failed:\n{}".format(
                    "\n".join(errors)
                )
            )
        coordinator.on_write_complete(write_returns)
        return coordinator.commit_result
    except Exception as error:
        coordinator.on_write_failed(error)
        raise


def _collect_partition_stats(dataset, planner):
    import pickle

    partition_col = "__paimon_partition__"
    rows_col = "__paimon_rows__"
    size_col = "__paimon_size__"

    def _stats(batch: pa.Table) -> pa.Table:
        stats = planner.input_partition_stats(batch)
        items = list(stats.items())
        return pa.table({
            partition_col: pa.array(
                [pickle.dumps(partition) for partition, _ in items],
                type=pa.binary(),
            ),
            rows_col: pa.array(
                [value[0] for _, value in items], type=pa.int64()
            ),
            size_col: pa.array(
                [value[1] for _, value in items], type=pa.int64()
            ),
        })

    materialized = dataset.materialize()
    stats_dataset = materialized.map_batches(
        _stats, batch_format="pyarrow", zero_copy_batch=True
    )
    combined = {}
    for batch in stats_dataset.iter_batches(batch_format="pyarrow"):
        for partition, rows, size in zip(
            batch.column(partition_col).to_pylist(),
            batch.column(rows_col).to_pylist(),
            batch.column(size_col).to_pylist(),
        ):
            key = pickle.loads(partition)
            previous_rows, previous_size = combined.get(key, (0, 0))
            combined[key] = (
                previous_rows + rows,
                previous_size + size,
            )
    return materialized, combined


def _write_primary_key_groups(
    dataset,
    table,
    *,
    overwrite: bool,
    static_partition: Optional[Dict[str, Any]],
    concurrency: Optional[int],
    ray_remote_args: Optional[Dict[str, Any]],
    bucket_extractor=None,
    postpone_bucket_plan=None,
    options: Optional[Dict[str, str]] = None,
) -> Optional[PaimonWriteResult]:
    import pickle

    from pypaimon.ray.shuffle import (
        _group_by_partition_bucket,
    )

    grouped, bucket_col = _group_by_partition_bucket(
        dataset, table, extractor=bucket_extractor
    )
    message_col = "__paimon_commit_messages__"
    error_col = "__paimon_write_error__"
    captured_table = table

    # Keep the writer inside the group UDF. Ray may split the UDF output
    # into multiple blocks, so only serialized commit messages leave it.
    def _write_group(group: pa.Table) -> pa.Table:
        if group.num_rows == 0:
            return pa.table({
                message_col: pa.array([], type=pa.binary()),
                error_col: pa.array([], type=pa.string()),
            })

        rows = group.drop_columns([bucket_col])
        worker_sink = PaimonDatasink(
            captured_table,
            overwrite=overwrite,
            static_partition=static_partition,
            postpone_bucket_plan=postpone_bucket_plan,
            options=options,
        )
        try:
            commit_messages = worker_sink.write([rows], None)
            error = None
        except Exception:
            commit_messages = []
            error = traceback.format_exc()
        return pa.table({
            message_col: pa.array(
                [pickle.dumps(commit_messages)], type=pa.binary()
            ),
            error_col: pa.array([error], type=pa.string()),
        })

    map_kwargs = _ray_map_kwargs(
        grouped.map_groups,
        concurrency,
        ray_remote_args,
        batch_format="pyarrow",
    )

    messages = grouped.map_groups(_write_group, **map_kwargs)
    coordinator = PaimonDatasink(
        table,
        overwrite=overwrite,
        static_partition=static_partition,
        options=options,
    )
    coordinator.on_write_start()
    return _consume_write_results(
        messages, coordinator, message_col, error_col
    )


class PaimonCommitterActor:
    """Per-write-job singleton that serializes all Paimon commits.

    Created by :meth:`PaimonActorDatasink.on_write_start` on the driver with
    ``num_cpus=0``. Write tasks RPC their commit messages here and block on
    the ack, so commits execute one at a time (Ray actors run methods
    serially) and intra-job optimistic-lock (CAS) contention is eliminated.

    The table is built once at actor init; a fresh builder and TableCommit
    are created per commit request. ``BatchTableCommit`` is strictly one-shot,
    so reuse would fail outright.

    **Within-job commit dedup via a stable commit_user.** The write task
    computes a deterministic ``commit_user``
    (``{job_id}-{write_uuid}-{task_idx}``) and sends it, with the
    driver-pinned ``base_snapshot_id``, in the RPC args. If the actor dies
    mid-commit, Ray re-issues the *same* actor task with the *same* args, so
    the replayed commit carries the same ``commit_user``; the injected
    ``commit.dedup-on-first-attempt`` scan then finds the already-landed
    snapshot (same ``commit_user`` + ``commit_identifier`` + ``commit_kind``)
    and skips it instead of committing the same files twice. Without this the
    fresh builder's random commit_user would make every replay a duplicate.
    This does not extend across jobs -- see :class:`PaimonActorDatasink`.

    **The scan is skipped where it provably has nothing to find.** That scan
    reads every snapshot from the write's fixed bound up to the current latest,
    so as the write commits, each commit scans one more snapshot than the last:
    over a write of T commits it costs O(T^2) snapshot reads, all serialized
    through this one actor. But every commit this write makes goes through here,
    so an instance alive since before the first of them knows, without looking,
    that a commit_user it has never handled has nothing on the table to be a
    replay of. Only that direction is taken from memory. The reverse -- "I
    remember committing this, so skip it" -- is deliberately *not*, because
    memory is not an authority on the table: a rollback can remove a snapshot
    this instance remembers writing, so what memory holds and what the table
    holds can differ, and only one of the two decides. So an already-attempted
    commit_user keeps going through the filesystem dedup path unchanged. Memory
    only ever withholds work the filesystem would have found nothing in.
    """

    def __init__(
        self,
        catalog_options: Dict[str, str],
        table_identifier: str,
        options: Optional[Dict[str, str]] = None,
    ):
        from pypaimon.catalog.catalog_factory import CatalogFactory

        catalog = CatalogFactory.create(catalog_options)
        self._table = catalog.get_table(table_identifier)
        self._options = options
        self._table_identifier = table_identifier
        # commit_users whose commit this instance has started. Only ever used to
        # withhold the claim below -- never to answer "already committed", which
        # would be a memory of the table that a rollback can silently outdate.
        # Recording an *attempt* is safe against that: it only ever sends work
        # back to the filesystem.
        self._attempted_users: Set[str] = set()
        # Whether this instance has been alive since before this write's first
        # commit, and so has seen every commit the write made. A restarted
        # instance has not, and must ask the filesystem instead.
        self._sees_every_commit = _is_first_actor_incarnation()

    def _target_table(self):
        """The table the commits actually go to.

        ``with_options`` applies the dynamic options through ``table.copy()``,
        and options like ``branch`` change which snapshot directory that table
        resolves to. Anything derived for the commit -- above all the dedup
        bound -- has to come from this table, not the un-copied one, or the
        bound is an id on a different branch than the scan walks.
        """
        return self._table.copy(self._options) if self._options else self._table

    def dedup_base(self) -> Dict[str, Optional[int]]:
        """Fixed lower bound for this write's duplicate-commit scan.

        The branch's latest snapshot id, with the filesystem mtime of that
        snapshot's file. Read on the table the commits actually go to, so the
        bound is an id on the same branch the scan will walk.

        Taken once per write and shipped to every task, so every attempt of
        every task scans from the same place: a bound re-read per attempt moves
        forward and lets a retry step over its own earlier commit.

        The id and the mtime are two reads, and the snapshot can be expired out
        from under the second one (another writer commits past it, retention
        drops it). Both halves have to describe the same, still-present
        snapshot, so an incomplete pair is re-read rather than frozen: a bound
        whose mtime is missing can no longer tell a normal id line from a
        rolled-back one, which degrades the scan to the whole history and makes
        the expired ids below the bound look like missing evidence -- refusing
        a first commit that has not even been attempted yet.
        """
        manager = self._target_table().snapshot_manager()
        for attempt in range(DEDUP_ANCHOR_READ_ATTEMPTS + 1):
            if attempt:
                time.sleep(DEDUP_ANCHOR_READ_WAIT_SECONDS)
            # get_latest_snapshot relocates past a candidate expired mid-read
            # itself, so a snapshot vanishing between the id and the file does
            # not surface here. What is left for this loop is the pair below.
            latest = manager.get_latest_snapshot()
            if latest is None:
                # No snapshot to bound against. 0 rather than None: it says "the
                # branch started empty", which makes every snapshot a candidate
                # and nothing missing from below -- as opposed to "no bound was
                # taken".
                return {"snapshot_id": 0, "file_millis": None}
            file_millis = manager.snapshot_file_millis(latest.id)
            if file_millis is not None:
                return {"snapshot_id": latest.id, "file_millis": file_millis}
            # The snapshot that read as latest is already gone. Re-reading gets
            # the one that replaced it, which is newer and therefore still a
            # valid lower bound: this write has not committed anything yet, so
            # no snapshot that could be ours exists at or below it.
            logger.info(
                f"Dedup bound snapshot {latest.id} of {self._table_identifier} "
                f"disappeared while being read (attempt {attempt + 1}); re-reading"
            )
        raise RuntimeError(
            f"Cannot establish a duplicate-commit bound for {self._table_identifier}: "
            f"the branch's latest snapshot was expired out from under every one of "
            f"{DEDUP_ANCHOR_READ_ATTEMPTS + 1} reads. Committing with an incomplete "
            f"bound would make the dedup scan unable to tell a replay from a first "
            f"commit. Retry when the table is not being expired this aggressively."
        )

    def commit(
        self,
        commit_messages: List["CommitMessage"],
        message: Optional[str] = None,
        commit_user: Optional[str] = None,
        dedup_base: Optional[Dict[str, Optional[int]]] = None,
    ) -> None:
        non_empty_messages = [m for m in commit_messages if not m.is_empty()]
        if not non_empty_messages:
            return

        options = dict(self._options) if self._options else {}
        # Enable the first-attempt duplicate scan so a replayed commit (same
        # stable commit_user, resent after a mid-commit actor crash) is
        # recognised and skipped rather than committed a second time.
        #
        # Unless this instance can already account for the commit_user: it has
        # handled every commit of this write, and has never started one for this
        # commit_user, so no snapshot on the table can be a replay of it and
        # there is nothing for the scan to find. Skipping it is what keeps the
        # write's total dedup cost linear in its number of commits rather than
        # quadratic. Note this gate only governs the *first* attempt -- an
        # optimistic-lock retry inside the commit still dedups against the
        # snapshot its failed attempt saw, whatever this is set to.
        can_vouch = (
            self._sees_every_commit
            and commit_user is not None
            and commit_user not in self._attempted_users
        )
        options['commit.dedup-on-first-attempt'] = 'false' if can_vouch else 'true'
        if dedup_base is not None:
            base_id = dedup_base.get("snapshot_id")
            if base_id is not None:
                options['commit.dedup-base-snapshot'] = str(base_id)
            file_millis = dedup_base.get("file_millis")
            if file_millis is not None:
                options['commit.dedup-base-file-millis'] = str(file_millis)

        builder = self._table.new_batch_write_builder()
        # with_options returns a new builder; discarding it silently drops the
        # options (commit.metadata.* and the dedup options would never land).
        builder = builder.with_options(options)
        if commit_user is not None:
            # with_options rebuilt the builder (regenerating a random
            # commit_user), so the stable per-task commit_user must be applied
            # after it. This is what lets _is_duplicate_commit match a replay.
            builder.commit_user = commit_user

        # Create commit and pass message via snapshot_properties if provided
        table_commit = builder.new_commit()
        snapshot_properties = {}
        if message:
            snapshot_properties['commit.message'] = message

        try:
            if commit_user is not None:
                # Recorded before the attempt, not after: a commit that raises
                # may still have landed -- the retry loop gives up with
                # "uncertain of success" after the snapshot is durable -- so a
                # later request for this commit_user must go to the filesystem
                # rather than be claimed as never attempted.
                self._attempted_users.add(commit_user)
            table_commit.commit(non_empty_messages, snapshot_properties=snapshot_properties)
            logger.info(
                f"Actor committed {len(non_empty_messages)} commit messages "
                f"for table {self._table_identifier}"
            )
        finally:
            try:
                table_commit.close()
            except Exception as e:
                logger.warning(f"Error closing table_commit: {e}", exc_info=e)


@DeveloperAPI
class PaimonActorDatasink(_DatasinkBase):
    """Ray Datasink that serializes commits through a per-job singleton actor.

    The write path is identical to the two-phase :class:`PaimonDatasink` (write
    files + prepare_commit inside each task), but instead of collecting commit
    messages at the driver the task sends its CommitMessages to a
    :class:`PaimonCommitterActor` over RPC and synchronously waits for the
    commit ack. Commits therefore happen one at a time -- no intra-job
    optimistic-lock contention -- while each task still returns only after its
    own data is committed, so progress/checkpoint mechanisms keyed off
    ``write()`` returning stay ordered after the commit, and data remains
    incrementally visible.

    Trade-offs (intentional):

    * **Append-only tables only.** Primary-key tables are rejected by
      ``write_paimon_dataset``. Their sequence numbers are derived from a
      baseline each writer reads when it is *created* and are baked into the
      data files before ``prepare_commit``, so two parallel Ray tasks writing
      the same bucket can assign overlapping sequence numbers regardless of
      how the commits are ordered. Serializing commits does not address this;
      use ``hash_fixed_precluster='map_groups'`` with
      ``commit_mode='two_phase'``, which keeps each bucket in one task.
    * **No overwrite support.** Constructing with ``overwrite=True`` raises
      ``ValueError``.
    * **No abort / rollback on failure.** Snapshots committed before a job
      failure stay visible; ``on_write_failed`` only logs.
    * **Within-job commit dedup via a stable per-task commit_user.** Each
      write task derives ``commit_user = {job_id}-{write_uuid}-{task_idx}``
      and sends it with its CommitMessages, alongside the ``base_snapshot_id``
      pinned by ``on_write_start``. Two retry scopes are covered: if the actor
      dies mid-commit Ray replays the same actor task with the same args, and
      if a whole write task is retried it regenerates its data files but keeps
      the same commit_user. In both cases the committer's first-attempt dedup
      scan finds the already-landed snapshot and skips it, so at most one
      file-set per task is committed; files from the discarded attempt are
      unreferenced orphans.
    * **No cross-job exactly-once.** ``commit_user`` embeds the Ray job id, so
      re-running a failed job writes and commits again. Recovering from a job
      failure is the caller's responsibility.
    """

    def __init__(
        self,
        table: "Table",
        catalog_options: Dict[str, str],
        table_identifier: str,
        overwrite: bool = False,
        static_partition: Optional[Dict[str, Any]] = None,
        postpone_bucket_plan=None,
        options: Optional[Dict[str, str]] = None,
    ):
        if overwrite or static_partition is not None:
            raise ValueError(
                "PaimonActorDatasink does not support overwrite=True or "
                "static_partition; atomic overwrite requires a single "
                "coordinated commit. Use the default PaimonDatasink "
                "(two-phase mode) for overwrite."
            )
        self.table = table
        self._catalog_options = catalog_options
        self._table_identifier = table_identifier
        self._options = options
        self._postpone_bucket_plan = postpone_bucket_plan
        self._table_name = table.identifier.get_full_name()
        # Unique per datasink instance: two write_paimon calls in one job
        # must not share an actor (they may carry different options). The
        # per-job anonymous namespace already isolates concurrent jobs.
        self._actor_name = (
            f"paimon-committer-{table_identifier}-{uuid.uuid4().hex[:8]}"
        )
        # Unique per datasink instance (per write_paimon call): combined with
        # job_id and task_idx it makes each write task's commit_user unique, so
        # two write_paimon calls or two jobs never share a commit_user (which
        # would let the dedup scan mistake one task's commit for another's).
        self._write_uuid = uuid.uuid4().hex
        self._actor = None
        # Dedup scan lower bound, pinned once by on_write_start (see there for
        # why it must not be recomputed per task attempt).
        self._dedup_base = None

    def __getstate__(self) -> dict:
        return self.__dict__.copy()

    def __setstate__(self, state: dict) -> None:
        self.__dict__.update(state)
        if not hasattr(self, '_table_name'):
            self._table_name = self.table.identifier.get_full_name()
        if not hasattr(self, '_actor'):
            self._actor = None
        if not hasattr(self, '_write_uuid'):
            self._write_uuid = uuid.uuid4().hex
        if not hasattr(self, 'static_partition'):
            self.static_partition = None
        if not hasattr(self, '_postpone_bucket_plan'):
            self._postpone_bucket_plan = None
        if not hasattr(self, '_dedup_base'):
            self._dedup_base = None

    def _target_table(self):
        """The table the writers and committer actually operate on.

        ``options`` is applied via ``table.copy()`` in every write/commit
        builder, and options like ``branch`` change which snapshot directory
        the result resolves to. Driver-side reads about the written data have
        to go through here, not ``self.table``, or they report on a different
        branch than the one being written.
        """
        return self.table.copy(self._options) if self._options else self.table

    def on_write_start(self, schema=None) -> None:
        # Created on the driver BEFORE the write closure is serialized (the
        # closure is ray.put lazily at first task submission, after on_start),
        # so the handle stored on self reaches every write task. write() also
        # falls back to ray.get_actor for paths that serialize earlier (e.g.
        # ActorPoolStrategy with initial_size >= 1).
        self._actor = ray.remote(PaimonCommitterActor).options(
            name=self._actor_name,
            num_cpus=0,
            max_restarts=3,
            # If the actor dies mid-commit, Ray replays the in-flight commit on
            # the restarted actor with the SAME args (same stable commit_user).
            # The first-attempt dedup scan then skips the already-landed
            # snapshot, so retrying is safe; 0 would instead fail the blocked
            # write task and (Ray Data does not retry it) fail the whole job.
            max_task_retries=3,
        ).remote(
            catalog_options=self._catalog_options,
            table_identifier=self._table_identifier,
            options=self._options,
        )

        # Pin the dedup scan's lower bound ONCE, before any write task runs. It
        # must not be read inside write(): a whole-task retry would then observe
        # its own earlier commit as the latest snapshot and set the bound above
        # it, so the scan range (bound, latest] would exclude that commit, the
        # duplicate would go undetected and the task's files would be committed
        # twice. Pinned here, every attempt of every task scans from the same
        # pre-write snapshot and finds the replay.
        #
        # Read inside the actor, which holds the options-applied table: the
        # bound has to be an id on the branch the commits go to, and the scan
        # also needs the snapshot file's mtime to tell that id line apart from
        # one that was rolled back and re-used the same ids.
        self._dedup_base = ray.get(self._actor.dedup_base.remote())

        logger.info(
            f"Starting actor-commit write job for table {self._table_name} "
            f"(committer actor: {self._actor_name})"
        )

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> List[Any]:
        table_write = None
        writer_closed = False
        try:
            writer_builder = (
                self.table.new_postpone_fixed_bucket_write_builder()
                if self._postpone_bucket_plan is not None
                else self.table.new_batch_write_builder()
            )
            if self._options:
                writer_builder = writer_builder.with_options(self._options)

            if self._postpone_bucket_plan is not None:
                writer_builder.with_bucket_plan(self._postpone_bucket_plan)
            table_write = writer_builder.new_write()

            table_schema = self.table.table_schema
            from pypaimon.schema.data_types import PyarrowFieldParser
            target_pa_schema = PyarrowFieldParser.from_paimon_schema(table_schema.fields)

            total_rows = 0
            for block in blocks:
                block_arrow: pa.Table = BlockAccessor.for_block(block).to_arrow()

                if block_arrow.num_rows == 0:
                    continue

                total_rows += block_arrow.num_rows
                block_arrow = _cast_binary_to_table_schema(block_arrow, target_pa_schema)

                table_write.write_arrow(block_arrow)

            commit_messages = table_write.prepare_commit()
            non_empty_messages = [m for m in commit_messages if not m.is_empty()]

            # Close the writer BEFORE publishing the commit. A failing close
            # runs the writer's abort path, which deletes the very data files
            # these commit messages reference. After a successful commit that
            # would leave a snapshot pointing at deleted files while the task
            # still reported success (reads then fail with FileNotFoundError).
            # Closing first turns the same failure into a plain task failure
            # with nothing committed, which Ray can safely retry.
            table_write.close()
            writer_closed = True

            if not non_empty_messages:
                logger.info(
                    f"Worker has no data to commit for table {self._table_name}"
                )
                return []

            commit_tag = _build_commit_tag(ctx)
            # Stable per-task commit_user: unique across tasks/jobs, but
            # identical when Ray replays this actor commit (same RPC args), so
            # the committer's dedup scan can recognise a re-issued commit.
            commit_user = f"{_current_job_id() or 'nojob'}-{self._write_uuid}-{ctx.task_idx}"
            # Pinned by on_write_start. Deliberately NOT read here: see
            # on_write_start for why a per-attempt read breaks dedup on a
            # whole-task retry. None means on_write_start never ran, so leave
            # it unset and let the committer scan from the first snapshot --
            # slower, but still correct, since commit_user is unique to this
            # task.
            dedup_base = self._dedup_base
            actor = self._actor
            if actor is None:
                actor = ray.get_actor(self._actor_name)
            # Synchronous: write() returns only after this task's data is
            # committed, keeping downstream checkpoint recording correctly
            # ordered after the commit.
            ray.get(actor.commit.remote(
                non_empty_messages,
                message=f"[{commit_tag}]",
                commit_user=commit_user,
                dedup_base=dedup_base,
            ))

            logger.info(
                f"Worker {ctx.task_idx} committed {len(non_empty_messages)} "
                f"commit messages ({total_rows} rows) for table {self._table_name} via actor"
            )
            return [{"row_count": total_rows}]
        finally:
            # Only reached with the writer still open when we failed before
            # the commit. Abort-on-close may delete this task's files, which
            # is correct here because nothing was committed; swallow so the
            # original failure is the one that propagates.
            if table_write is not None and not writer_closed:
                try:
                    table_write.close()
                except Exception as e:
                    logger.warning(
                        f"Error closing table_write: {e}", exc_info=e
                    )

    def _shutdown_actor(self) -> None:
        if self._actor is None:
            return
        try:
            ray.kill(self._actor)
        except Exception as e:
            logger.warning(
                f"Error killing committer actor {self._actor_name}: {e}",
                exc_info=e,
            )
        self._actor = None

    def on_write_complete(self, write_result: Any) -> None:
        # All commits already happened via the actor during the write tasks.
        self._shutdown_actor()
        if hasattr(write_result, "write_returns"):
            write_returns = write_result.write_returns
        elif isinstance(write_result, list):
            write_returns = write_result
        else:
            write_returns = []

        # Set commit_result for compatibility with write_paimon_dataset return value
        # Actor mode commits incrementally, so we report the final state.
        # Read through _target_table: with options like branch, self.table
        # resolves to a different snapshot directory than the one just written,
        # and the reported snapshot id would be some other branch's.
        latest_snapshot = self._target_table().snapshot_manager().get_latest_snapshot()

        if latest_snapshot:
            # Extract row_count from write_returns
            # write_returns is a list of lists, where each inner list contains dicts
            total_rows = 0
            for task_result in write_returns:
                if isinstance(task_result, list):
                    # Each task returns a list with a single dict
                    for r in task_result:
                        if isinstance(r, dict):
                            total_rows += r.get('row_count', 0)
                        else:
                            total_rows += getattr(r, 'row_count', 0)
                elif isinstance(task_result, dict):
                    total_rows += task_result.get('row_count', 0)
                else:
                    total_rows += getattr(task_result, 'row_count', 0)

            self.commit_result = PaimonWriteResult(
                row_count=total_rows,
                snapshot_id=latest_snapshot.id
            )
        else:
            self.commit_result = None

        logger.info(
            f"Actor-commit write job complete for table {self._table_name}: "
            f"{len(write_returns)} worker tasks finished"
        )

    def on_write_failed(self, error: Exception) -> None:
        # Same contract as current behavior: already-committed snapshots stay
        # visible and are NOT rolled back.
        self._shutdown_actor()
        logger.error(
            f"Actor-commit write job failed for table {self._table_name}: "
            f"{error}. Note: any data already committed remains visible and "
            f"is NOT rolled back. Manual cleanup may be required.",
            exc_info=error,
        )
