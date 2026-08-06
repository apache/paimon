#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

"""Distributed transform updates on Ray for data-evolution tables."""

import logging
import uuid
from typing import Any, Callable, Dict, List, Optional, Union

from pypaimon.ray.data_evolution_merge_into import (
    _reraise_inner,
    _require_ray_join,
    _resolve_num_partitions,
)
from pypaimon.ray.data_evolution_merge_join import (
    GroupApplyError,
    distributed_update_apply,
)
from pypaimon.ray.data_evolution_merge_transform import build_update_schema
from pypaimon.ray.update_by_row_id import (
    _abort_pending_update_messages,
    _blob_col_names,
)

__all__ = ["update_by_transform"]

logger = logging.getLogger(__name__)


def _positive_int(name, value):
    if (isinstance(value, bool)
            or not isinstance(value, int)
            or value <= 0):
        raise ValueError("{} must be a positive integer.".format(name))


def _prepare_transform_update(
        target,
        catalog_options,
        read_projection,
        transform,
        update_cols,
        rows_per_commit,
        transform_filter,
        transform_batch_size):
    from pypaimon.catalog.catalog_factory import CatalogFactory
    from pypaimon.common.where_parser import (
        extract_fields_from_where,
        parse_where_clause,
    )
    from pypaimon.schema.data_types import PyarrowFieldParser
    from pypaimon.table.special_fields import SpecialFields

    _positive_int("rows_per_commit", rows_per_commit)
    _positive_int("transform_batch_size", transform_batch_size)
    if not callable(transform):
        raise ValueError("transform must be callable.")
    if not read_projection:
        raise ValueError("read_projection must be non-empty.")
    if not update_cols:
        raise ValueError("update_cols must be non-empty.")
    if (transform_filter is not None
            and not isinstance(transform_filter, str)
            and not callable(transform_filter)):
        raise ValueError("filter must be a SQL expression string or callable.")

    update_cols = list(dict.fromkeys(update_cols))
    read_cols = list(dict.fromkeys(read_projection))
    table = CatalogFactory.create(catalog_options).get_table(target)
    if table.is_primary_key_table:
        raise ValueError(
            "update_by_transform requires a non-primary-key table.")
    if not table.options.data_evolution_enabled():
        raise ValueError(
            f"update_by_transform requires 'data-evolution.enabled'='true' "
            f"on '{target}'.")
    if not table.options.row_tracking_enabled():
        raise ValueError(
            f"update_by_transform requires 'row-tracking.enabled'='true' "
            f"on '{target}'.")
    if table.options.deletion_vectors_enabled():
        raise ValueError(
            "update_by_transform does not support deletion-vectors-enabled "
            f"tables yet: '{target}'.")

    rid = SpecialFields.ROW_ID.name
    if rid in read_cols:
        raise ValueError(
            "update_by_transform keeps _ROW_ID internal; remove it from "
            "read_projection.")
    unknown = [col for col in read_cols if col not in table.field_names]
    if unknown:
        raise ValueError(
            f"read column {unknown[0]!r} is not in target '{target}'.")
    blob_cols = _blob_col_names(table)
    partition_keys = set(table.partition_keys or [])
    for col in update_cols:
        if col not in table.field_names:
            raise ValueError(
                f"update column {col!r} is not in target '{target}'.")
        if col in blob_cols:
            raise ValueError(
                f"update_by_transform cannot update blob column {col!r}.")
        if col in partition_keys:
            raise ValueError(
                f"update_by_transform cannot update partition column {col!r}.")

    target_pa = PyarrowFieldParser.from_paimon_schema(
        table.table_schema.fields)
    update_schema = build_update_schema(target_pa, update_cols, rid)
    predicate = (
        parse_where_clause(transform_filter, table.table_schema.fields)
        if isinstance(transform_filter, str) else None
    )
    filter_cols = (
        extract_fields_from_where(
            transform_filter, set(table.field_names))
        if isinstance(transform_filter, str) else set()
    )
    protected_cols = list(dict.fromkeys(
        read_cols + [col for col in table.field_names if col in filter_cols]
    ))
    filter_fn = transform_filter if callable(transform_filter) else None
    return (table, read_cols, update_cols, update_schema, predicate,
            filter_fn, protected_cols)


def update_by_transform(
    target: str,
    catalog_options: Dict[str, str],
    *,
    read_projection: List[str],
    transform: Callable,
    update_cols: List[str],
    rows_per_commit: int,
    filter: Optional[Union[str, Callable]] = None,
    num_partitions: Optional[int] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    transform_batch_size: int = 1024,
) -> Dict[str, int]:
    """Transform matching rows and commit continuous row-id ranges.

    The transform receives ``read_projection`` and returns ``update_cols``
    with the same row count and order. ``filter`` defaults to the full target.
    Row ids remain internal to this operation.

    Returns ``{"num_updated": <rows>}``.
    """
    _require_ray_join()
    (table, read_cols, update_cols, update_schema,
     predicate, filter_fn, protected_cols) = _prepare_transform_update(
        target,
        catalog_options,
        read_projection,
        transform,
        update_cols,
        rows_per_commit,
        filter,
        transform_batch_size,
    )
    num_partitions = _resolve_num_partitions(num_partitions)
    base = table.snapshot_manager().get_latest_snapshot()
    if base is None or base.total_record_count == 0:
        return {"num_updated": 0}

    retention_tag = "pypaimon-transform-update-{}".format(uuid.uuid4().hex)
    table.create_tag(retention_tag, snapshot_id=base.id, time_retained="30d")
    committer = _IncrementalUpdateCommitter(
        table, base, table.table_schema.id, protected_cols)
    try:
        _, num_updated, _ = distributed_update_apply(
            None,
            table,
            update_cols,
            num_partitions=num_partitions,
            ray_remote_args=ray_remote_args,
            base_snapshot_id=base.id,
            on_group_result=committer.add_range,
            rows_per_range=rows_per_commit,
            read_projection=read_cols,
            transform=transform,
            transform_filter=filter_fn,
            transform_predicate=predicate,
            transform_update_schema=update_schema,
            transform_batch_size=transform_batch_size,
        )
        committer.finish()
    except GroupApplyError:
        committer.finish()
        raise
    except Exception as error:
        if committer.failed:
            raise
        _reraise_inner(error)
        raise
    finally:
        committer.close()
        try:
            table.delete_tag(retention_tag)
        except Exception as error:
            logger.warning(
                "Failed to delete transform retention tag %s: %s",
                retention_tag,
                error,
                exc_info=error,
            )
    return {"num_updated": num_updated}


class _IncrementalUpdateCommitter:

    def __init__(self, table, base_snapshot=None, planned_schema_id=None,
                 protected_columns=None):
        self._table = table
        self._table_commit = None
        self._snapshot_callback = None
        self._commit_user = None
        self._checkpoint_snapshot = base_snapshot
        self._planned_schema_id = planned_schema_id
        self._protected_columns = protected_columns
        self._next_commit_identifier = 1
        self._deferred_commit_error = None

    @property
    def failed(self) -> bool:
        return self._deferred_commit_error is not None

    def add_range(self, commit_messages, _num_updated, _row_ids) -> None:
        if self.failed:
            _abort_pending_update_messages(self._table, commit_messages)
            return
        try:
            self._commit(commit_messages)
        except Exception as error:
            self._deferred_commit_error = error

    def finish(self) -> None:
        if self.failed:
            raise self._deferred_commit_error

    def _commit(self, messages) -> None:
        if not messages:
            return
        if self._table_commit is None:
            builder = self._table.new_stream_write_builder()
            if self._checkpoint_snapshot is not None:
                self._commit_user = builder.commit_user
            self._table_commit = builder.new_commit()
            self._snapshot_callback = _SnapshotCallback()
            self._table_commit.add_commit_callback(self._snapshot_callback)

        identifier = self._next_commit_identifier
        if self._checkpoint_snapshot is not None:
            self._table_commit.protect_from_external_rewrites(
                self._checkpoint_snapshot,
                self._commit_user,
                self._planned_schema_id,
                self._protected_columns,
            )
        self._table_commit.commit(messages, identifier)
        if self._checkpoint_snapshot is None:
            self._next_commit_identifier += 1
            return
        committed = self._snapshot_callback.pop(identifier)
        if committed is None:
            raise RuntimeError(
                "Committed transform update snapshot cannot be found.")
        latest_schema = self._table.schema_manager.latest()
        if (committed.schema_id != self._planned_schema_id
                or latest_schema is None
                or latest_schema.id != self._planned_schema_id):
            from pypaimon.write.commit.conflict_detection import (
                CommitConflictError,
            )
            raise CommitConflictError(
                "Target schema changed during update_by_transform.")
        self._checkpoint_snapshot = committed
        self._next_commit_identifier += 1

    def close(self) -> None:
        if self._table_commit is None:
            return
        try:
            self._table_commit.close()
        except Exception as error:
            logger.warning(
                "Failed to close update_by_transform commit: %s",
                error,
                exc_info=error,
            )


class _SnapshotCallback:

    def __init__(self):
        self._snapshots = {}

    def call(self, context):
        self._snapshots[context.identifier] = context.snapshot

    def pop(self, identifier):
        return self._snapshots.pop(identifier, None)

    def close(self):
        pass
