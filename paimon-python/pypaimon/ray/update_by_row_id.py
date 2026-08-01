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

"""Distributed row-id update on Ray for data-evolution tables.

Update columns of a data-evolution table straight from a Ray Dataset that already
carries ``_ROW_ID`` and the new values -- no full-target read and no big-table
shuffle join (unlike ``merge_into(on=["_ROW_ID"])``, which reads and joins the whole
target). Pairs with ``bucket_join``, which produces the row ids.
"""

import hashlib
import json
import logging
import time
from typing import Any, Dict, List, Optional

import pyarrow as pa

from pypaimon.ray.data_evolution_merge_into import (
    _normalize_source,
    _reraise_inner,
    _require_ray_join,
    _resolve_num_partitions,
)
from pypaimon.ray.data_evolution_merge_join import (
    GroupApplyError,
    distributed_update_apply,
)
from pypaimon.ray.data_evolution_merge_transform import build_update_schema
from pypaimon.schema.data_types import is_blob_file_field

__all__ = [
    "update_by_row_id",
    "delete_update_by_row_id_checkpoint",
]

logger = logging.getLogger(__name__)

_CHECKPOINT_PROPERTY = "data-evolution.update-by-row-id.checkpoint"
_CHECKPOINT_TAG_PREFIX = "_pypaimon_ray_update_"
_OFFSET_CHECKPOINT_VERSION = 2
_OFFSET_CHECKPOINT_MODE = "source-offset"
_CHECKPOINT_TAG_UPDATE_ATTEMPTS = 3
_ROUTE_KIND = "_ROUTE_KIND"
_ROUTE_SOURCE_UNIT = "_SOURCE_UNIT"
_ROUTE_TARGET_GROUP = "_TARGET_GROUP"
_ROUTE_DATA = "data"
_ROUTE_MARKER = "marker"


def _blob_col_names(table: "FileStoreTable") -> set:
    return {f.name for f in table.table_schema.fields
            if is_blob_file_field(f)}


def update_by_row_id(
    target: str,
    source: Any,
    catalog_options: Dict[str, str],
    *,
    update_cols: List[str],
    num_partitions: Optional[int] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    commit_mode: str = "atomic",
    max_groups_per_commit: Optional[int] = None,
    operation_id: Optional[str] = None,
) -> Dict[str, int]:
    """Update ``update_cols`` of a data-evolution table by ``_ROW_ID``.

    ``source`` (a ``ray.data.Dataset`` / ``pyarrow.Table`` / ``pandas.DataFrame``)
    must already carry the target ``_ROW_ID`` and the new values. Each row is
    routed to the data file owning its row id and only those files are rewritten --
    the target is never fully read and there is no join against it. Requires
    ``ray >= 2.50`` and a target with ``data-evolution.enabled`` + ``row-tracking.enabled``.

    A :class:`PaimonOffsetSource` pins its source snapshot and commits one unit
    window at a time. :class:`PaimonCoBucketedJoinOffsetSource` first routes joined
    row ids by target file group, then checkpoints target-group windows. Its
    transform receives only the routed row-id column.

    By default, all file groups are committed atomically. Set
    ``commit_mode="incremental"`` together with ``max_groups_per_commit`` to
    commit completed groups in smaller windows. Incremental mode is not atomic:
    commits made before a later failure remain visible. Deterministic duplicate
    ``_ROW_ID`` errors are returned as group results so completed groups can be
    flushed first. Other application errors propagate to Ray and remain subject
    to the retry options in ``ray_remote_args``. A ``PaimonOffsetSource`` also
    requires a stable ``operation_id`` so a later invocation can resume from its
    durable source offset.

    Returns ``{"num_updated": <rows>}``.
    """
    from pypaimon.catalog.catalog_factory import CatalogFactory
    from pypaimon.ray.offset_source import PaimonOffsetSource
    from pypaimon.schema.data_types import PyarrowFieldParser
    from pypaimon.table.special_fields import SpecialFields

    _require_ray_join()
    offset_source = isinstance(source, PaimonOffsetSource)
    if not update_cols:
        raise ValueError("update_cols must be non-empty.")
    if commit_mode not in ("atomic", "incremental"):
        raise ValueError("commit_mode must be 'atomic' or 'incremental'.")
    if commit_mode == "atomic" and max_groups_per_commit is not None:
        raise ValueError(
            "max_groups_per_commit requires commit_mode='incremental'.")
    if (commit_mode == "incremental"
            and max_groups_per_commit is None
            and not offset_source):
        raise ValueError(
            "commit_mode='incremental' requires max_groups_per_commit.")
    if offset_source and operation_id is None:
        raise ValueError("PaimonOffsetSource requires operation_id.")
    if operation_id is not None and not offset_source:
        raise ValueError("operation_id requires a PaimonOffsetSource.")
    if operation_id is not None:
        if not isinstance(operation_id, str) or not operation_id.strip():
            raise ValueError("operation_id must be a non-empty string.")
        if len(operation_id) > 256:
            raise ValueError("operation_id must contain at most 256 characters.")
    if max_groups_per_commit is not None:
        if (isinstance(max_groups_per_commit, bool)
                or not isinstance(max_groups_per_commit, int)
                or max_groups_per_commit <= 0):
            raise ValueError("max_groups_per_commit must be a positive integer.")
    if offset_source:
        if commit_mode != "incremental":
            raise ValueError(
                "PaimonOffsetSource requires commit_mode='incremental'.")
        if max_groups_per_commit is not None:
            raise ValueError(
                "PaimonOffsetSource uses units_per_checkpoint instead of "
                "max_groups_per_commit.")
    update_cols = list(dict.fromkeys(update_cols))  # de-dup, keep order
    num_partitions = _resolve_num_partitions(num_partitions)

    catalog = CatalogFactory.create(catalog_options)
    table = catalog.get_table(target)
    if not table.options.data_evolution_enabled():
        raise ValueError(
            f"update_by_row_id requires 'data-evolution.enabled'='true' on '{target}'.")
    if not table.options.row_tracking_enabled():
        raise ValueError(
            f"update_by_row_id requires 'row-tracking.enabled'='true' on '{target}'.")
    if table.options.deletion_vectors_enabled():
        # A DV-deleted row still lives in its data file, so row-id ranges can't tell it
        # apart without reading the target; refuse rather than update a deleted row.
        raise ValueError(
            f"update_by_row_id does not support deletion-vectors-enabled tables yet: "
            f"'{target}'.")

    rid = SpecialFields.ROW_ID.name
    blob_cols = _blob_col_names(table)
    partition_keys = set(table.partition_keys or [])
    for col in update_cols:
        if col not in table.field_names:
            raise ValueError(f"update column {col!r} is not in target '{target}'.")
        if col in blob_cols:
            # Update writes plain data files; blob deltas are a separate path.
            raise ValueError(f"update_by_row_id cannot update blob column {col!r}.")
        if col in partition_keys:
            # In-place rewrite can't move a row across partitions.
            raise ValueError(
                f"update_by_row_id cannot update partition column {col!r}; "
                "cross-partition row movement is not supported.")

    if offset_source:
        return _update_from_offset_source(
            target,
            source,
            catalog,
            catalog_options,
            table,
            update_cols,
            num_partitions,
            ray_remote_args,
            operation_id,
        )

    if isinstance(source, str):
        # A table's system _ROW_ID is its own, independent of the target's, so a
        # table-name source can't address target rows. Require in-memory data that
        # already carries the target row ids (e.g. produced by bucket_join).
        raise ValueError(
            "update_by_row_id does not accept a table-name source; pass a ray.data."
            f"Dataset / pyarrow.Table / pandas.DataFrame carrying the target {rid}.")
    source_ds = _normalize_source(source, catalog_options)
    src_cols = set(source_ds.schema().names)
    missing = [c for c in [rid] + update_cols if c not in src_cols]
    if missing:
        raise ValueError(
            f"source is missing columns {missing}; it must carry {rid} and {update_cols}.")

    # Cast to the on-disk schema (int64 _ROW_ID + target column types) so the writer
    # gets exactly the target types regardless of the source's arrow types.
    target_pa = PyarrowFieldParser.from_paimon_schema(table.table_schema.fields)
    update_schema = build_update_schema(target_pa, update_cols, rid)

    def _project_cast(batch: pa.Table) -> pa.Table:
        return batch.select([rid] + update_cols).cast(update_schema)

    update_ds = source_ds.map_batches(_project_cast, batch_format="pyarrow")

    base = table.snapshot_manager().get_latest_snapshot()
    incremental_committer = (
        _IncrementalUpdateCommitter(table, max_groups_per_commit)
        if commit_mode == "incremental" else None
    )
    # Without deletion vectors (rejected above), total_record_count is the live row
    # count, so 0 means the target is empty (never written, or emptied by overwrite).
    if base is None or base.total_record_count == 0:
        # Every source row id is foreign; don't silently no-op non-empty input.
        try:
            if update_ds.limit(1).count() > 0:
                raise ValueError(
                    f"target '{target}' has no rows; every _ROW_ID in the "
                    "source is foreign.")
            return {"num_updated": 0}
        finally:
            if incremental_committer is not None:
                incremental_committer.close()
    try:
        apply_kwargs = {
            "num_partitions": num_partitions,
            "ray_remote_args": ray_remote_args,
            "base_snapshot_id": base.id,
        }
        if incremental_committer is not None:
            apply_kwargs["on_group_result"] = incremental_committer.add_group
        msgs, num_updated, _ = distributed_update_apply(
            update_ds, table, update_cols, **apply_kwargs
        )
        if incremental_committer is not None:
            incremental_committer.finish()
    except GroupApplyError:
        if incremental_committer is not None:
            try:
                incremental_committer.finish()
            except Exception:
                incremental_committer.abort_pending()
                raise
        raise
    except Exception as e:
        if incremental_committer is not None:
            incremental_committer.abort_pending()
            if incremental_committer._commit_failed:
                raise
        _reraise_inner(e)
        raise  # _reraise_inner always raises; keeps msgs/num_updated defined for linters
    finally:
        if incremental_committer is not None:
            incremental_committer.close()

    if incremental_committer is None and msgs:
        _commit_update_messages(table, msgs)
    return {"num_updated": num_updated}


def _update_from_offset_source(
        target,
        source,
        catalog,
        catalog_options,
        table,
        update_cols,
        num_partitions,
        ray_remote_args,
        operation_id):
    from pypaimon.schema.data_types import PyarrowFieldParser
    from pypaimon.table.special_fields import SpecialFields

    initial_snapshot = table.snapshot_manager().get_latest_snapshot()
    if initial_snapshot is None or initial_snapshot.total_record_count == 0:
        raise ValueError(
            "PaimonOffsetSource requires a non-empty target table.")

    commit_user = _operation_commit_user(operation_id)
    checkpoint_tags = _operation_checkpoint_tags(operation_id)
    loaded = _load_offset_operation_checkpoint(
        catalog,
        target,
        table,
        operation_id,
        update_cols,
        commit_user,
        checkpoint_tags,
    )
    saved_plan = (
        loaded[1]["source"]
        if loaded is not None and loaded[1] is not None
        else None)
    retention_tags = {}
    retained_snapshot_ids = {}
    for role, table_identifier in source._retention_tables().items():
        tag_name = _operation_source_tag(
            operation_id, target, role)
        retention_tags[role] = tag_name
        retained = _get_checkpoint_tag(
            catalog, table_identifier, tag_name)
        if retained is not None:
            retained_snapshot_ids[role] = retained.snapshot.id
    bind_kwargs = {
        "checkpoint_plan": saved_plan,
        "retention_tags": {
            role: retention_tags[role]
            for role in retained_snapshot_ids
        },
        "retained_snapshot_ids": retained_snapshot_ids,
        "catalog_options": catalog_options,
    }
    target_read_snapshot_id = None
    if getattr(source, "_needs_target_read_plan", False):
        planner_tag = _operation_planner_tag(operation_id)
        retained_target = _get_checkpoint_tag(
            catalog, target, planner_tag)
        target_read_snapshot_id = (
            retained_target.snapshot.id
            if retained_target is not None
            else (saved_plan or {}).get(
                "target_snapshot_id", initial_snapshot.id)
        )
        _ensure_source_tag(
            catalog, target, planner_tag, target_read_snapshot_id)
        bind_kwargs["target"] = target
        bind_kwargs["operation_id"] = operation_id
        bind_kwargs["target_snapshot_id"] = target_read_snapshot_id
    bound_source = source._bind(
        catalog,
        **bind_kwargs,
    )
    for retention in bound_source.plan["retentions"]:
        role = retention["role"]
        _ensure_source_tag(
            catalog,
            retention["table"],
            retention_tags[role],
            retention["snapshot_id"],
        )
    committer = _OffsetUpdateCommitter(
        table,
        catalog,
        target,
        operation_id,
        update_cols,
        bound_source.plan,
        initial_snapshot,
        loaded,
    )
    if committer.next_offset > bound_source.num_units:
        committer.close()
        raise RuntimeError(
            "Offset checkpoint is beyond the source unit count.")

    rid = SpecialFields.ROW_ID.name
    target_pa = PyarrowFieldParser.from_paimon_schema(
        table.table_schema.fields)
    update_schema = build_update_schema(target_pa, update_cols, rid)
    target_read_plan = None
    target_write_plan = None

    try:
        if getattr(source, "_needs_target_read_plan", False):
            target_read_plan = _OffsetTargetFilesPlan(
                _load_row_id_files_info(
                    table, target_read_snapshot_id))
            _prepare_bucket_join_route(
                catalog,
                bound_source,
                target_read_plan.base_info,
                operation_id,
                target,
            )
            if not bound_source.route_splits_by_bucket:
                if committer.next_offset < bound_source.num_units:
                    committer.commit_window(
                        [], 0, bound_source.num_units, set())
                committer.finish()
                return {"num_updated": committer.num_updated}
            latest = table.snapshot_manager().get_latest_snapshot()
            if latest is None:
                raise RuntimeError(
                    "Target has no snapshot before offset update.")
            # Only untouched groups remain; signatures guard concurrent rewrites.
            target_write_plan = target_read_plan
            target_write_plan.snapshot_id = latest.id

        for start, end in bound_source.windows(committer.next_offset):
            latest = table.snapshot_manager().get_latest_snapshot()
            if latest is None:
                raise RuntimeError(
                    "Target has no snapshot before offset update.")
            if (target_write_plan is not None
                    and target_write_plan.snapshot_id != latest.id):
                target_write_plan = None
            if (getattr(source, "_needs_target_read_plan", False)
                    and target_write_plan is None):
                target_write_plan = _OffsetTargetFilesPlan(
                    _load_row_id_files_info(table, latest.id))

            if target_read_plan is None:
                source_ds = bound_source.read_window(
                    committer.next_offset, end)
            else:
                from pypaimon.ray.read_by_row_id import (
                    _use_offset_read_plan,
                )

                with _use_offset_read_plan(
                        target,
                        target_read_plan.base_info.snapshot_id,
                        target_read_plan.base_info,
                        target_read_plan.base_ref):
                    source_ds = bound_source.read_window(
                        committer.next_offset, end)

            def _project_cast(batch: pa.Table) -> pa.Table:
                missing = [
                    col for col in [rid] + update_cols
                    if col not in batch.column_names
                ]
                if missing:
                    raise ValueError(
                        "source is missing columns {}; it must carry {} and {}."
                        .format(missing, rid, update_cols))
                return batch.select(
                    [rid] + update_cols).cast(update_schema)

            update_ds = source_ds.map_batches(
                _project_cast, batch_format="pyarrow")

            messages = []
            planned_file_signatures = set()

            def _collect_group(
                    group_messages,
                    _group_num_updated,
                    _row_ids,
                    group_file_signatures):
                messages.extend(group_messages)
                planned_file_signatures.update(group_file_signatures)

            try:
                _, num_updated, _ = distributed_update_apply(
                    update_ds,
                    table,
                    update_cols,
                    num_partitions=num_partitions,
                    ray_remote_args=ray_remote_args,
                    base_snapshot_id=latest.id,
                    on_group_result=_collect_group,
                    precomputed_files_info=(
                        target_write_plan.base_info
                        if target_write_plan is not None
                        and target_write_plan.snapshot_id == latest.id
                        else None),
                    precomputed_info_ref=(
                        target_write_plan.base_ref
                        if target_write_plan is not None
                        and target_write_plan.snapshot_id == latest.id
                        else None),
                )
            except Exception as error:
                _abort_pending_update_messages(table, messages)
                _reraise_inner(error)
            try:
                _validate_routed_target_groups(
                    bound_source,
                    planned_file_signatures,
                    start,
                    end,
                )
            except Exception:
                _abort_pending_update_messages(table, messages)
                raise
            committed_snapshot = committer.commit_window(
                messages,
                num_updated,
                end,
                planned_file_signatures,
            )
            if (target_write_plan is not None
                    and committed_snapshot.id == latest.id + 1):
                target_write_plan.snapshot_id = committed_snapshot.id
            else:
                target_write_plan = None
        committer.finish()
        return {"num_updated": committer.num_updated}
    finally:
        committer.close()


def _prepare_bucket_join_route(
        catalog,
        bound_source,
        target_files_info,
        operation_id,
        target):
    route_table = _ensure_route_table(
        catalog, bound_source, operation_id, target)
    route_tag = bound_source.plan["route_tag"]
    tagged = _get_checkpoint_tag(
        catalog, bound_source.plan["route_table"], route_tag)
    if tagged is None:
        from pypaimon.write.ray_datasink import write_paimon_dataset

        completed = _route_completed_units(route_table)
        if (bound_source.route_num_units == 0
                and route_table.snapshot_manager().get_latest_snapshot()
                is None):
            write_paimon_dataset(
                _route_markers(bound_source.source.row_id_col, [-1]),
                route_table,
            )
        for unit_indexes in bound_source.route_windows(completed):
            joined = bound_source.read_join_units(unit_indexes)
            routed = _route_joined_rows(
                joined,
                bound_source.source.row_id_col,
                target_files_info,
                unit_indexes,
            )
            write_paimon_dataset(routed, route_table)
            completed.update(unit_indexes)
        expected = set(range(bound_source.route_num_units))
        completed = _route_completed_units(route_table)
        if completed != expected:
            missing = sorted(expected - completed)
            raise RuntimeError(
                "Row-id routing is incomplete; missing source units {}."
                .format(missing[:10]))
        latest = route_table.snapshot_manager().get_latest_snapshot()
        if latest is None:
            raise RuntimeError("Row-id routing produced no snapshot.")
        try:
            catalog.create_tag(
                bound_source.plan["route_table"],
                route_tag,
                snapshot_id=latest.id,
            )
        except Exception:
            tagged = _get_checkpoint_tag(
                catalog, bound_source.plan["route_table"], route_tag)
            if tagged is None or tagged.snapshot.id != latest.id:
                raise
        tagged = _get_checkpoint_tag(
            catalog, bound_source.plan["route_table"], route_tag)
    route_table, splits_by_bucket, read_type, nested_name_paths = (
        _plan_route_buckets(
            catalog,
            bound_source.plan["route_table"],
            route_tag,
            bound_source.source.row_id_col,
            bound_source.source.routing_buckets,
        )
    )
    bound_source.configure_route(
        route_table, splits_by_bucket, read_type, nested_name_paths)


def _validate_routed_target_groups(
        bound_source, planned_file_signatures, start, end):
    if not getattr(bound_source.source, "_needs_target_read_plan", False):
        return
    extractor = bound_source.route_table.create_row_key_extractor()
    for signature in planned_file_signatures:
        first_row_id = signature[7]
        _, bucket = extractor.extract_partition_bucket_row({
            _ROUTE_KIND: _ROUTE_DATA,
            _ROUTE_TARGET_GROUP: first_row_id,
        })
        if bucket < start or bucket >= end:
            raise ValueError(
                "PaimonCoBucketedJoinOffsetSource transform changed a row id "
                "outside its checkpoint window.")


def _ensure_route_table(catalog, bound_source, operation_id, target):
    from pypaimon.schema.data_types import PyarrowFieldParser
    from pypaimon.schema.schema import Schema

    source = bound_source.source
    route_schema = pa.schema([
        pa.field(source.row_id_col, pa.int64()),
        pa.field(_ROUTE_TARGET_GROUP, pa.int64(), nullable=False),
        pa.field(_ROUTE_SOURCE_UNIT, pa.int64()),
        pa.field(_ROUTE_KIND, pa.string(), nullable=False),
    ])
    options = {
        "bucket": str(source.routing_buckets),
        "bucket-key": _ROUTE_TARGET_GROUP,
        "pypaimon.ray.route.operation-id": operation_id,
        "pypaimon.ray.route.target": target,
        "pypaimon.ray.route.fingerprint": bound_source.plan["fingerprint"],
    }
    identifier = bound_source.plan["route_table"]
    catalog.create_table(
        identifier,
        Schema.from_pyarrow_schema(
            route_schema,
            partition_keys=[_ROUTE_KIND],
            options=options,
        ),
        True,
    )
    table = catalog.get_table(identifier)
    actual_options = table.table_schema.options
    for key, value in options.items():
        if actual_options.get(key) != value:
            raise RuntimeError(
                "Routing table {!r} belongs to another operation."
                .format(identifier))
    actual_schema = PyarrowFieldParser.from_paimon_schema(
        table.table_schema.fields)
    if (not actual_schema.equals(route_schema)
            or table.partition_keys != [_ROUTE_KIND]):
        raise RuntimeError(
            "Routing table {!r} has an incompatible schema."
            .format(identifier))
    return table


def _route_completed_units(route_table):
    from pypaimon.common.predicate_builder import PredicateBuilder

    latest = route_table.snapshot_manager().get_latest_snapshot()
    if latest is None:
        return set()
    predicate = PredicateBuilder(route_table.fields).equal(
        _ROUTE_KIND, _ROUTE_MARKER)
    builder = (
        route_table.new_read_builder()
        .with_partition_filter(predicate)
        .with_projection([_ROUTE_SOURCE_UNIT])
    )
    splits = builder.new_scan().plan().splits()
    if not splits:
        return set()
    result = builder.new_read().to_arrow(splits)
    if result is None:
        return set()
    values = result.column(_ROUTE_SOURCE_UNIT).to_pylist()
    if any(value is None for value in values):
        raise RuntimeError("Routing checkpoint contains a null source unit.")
    return {value for value in values if value >= 0}


def _route_joined_rows(joined, row_id_col, files_info, unit_indexes):
    import numpy as np

    from pypaimon.ray.data_evolution_merge_join import (
        _sorted_range_membership,
    )

    sorted_first_row_ids = np.asarray(
        files_info.first_row_ids, dtype=np.int64)
    valid_ranges = files_info.valid_row_id_ranges
    range_starts = np.asarray(
        [row_range.from_ for row_range in valid_ranges], dtype=np.int64)
    range_ends = np.asarray(
        [row_range.to for row_range in valid_ranges], dtype=np.int64)
    route_schema = pa.schema([
        pa.field(row_id_col, pa.int64()),
        pa.field(_ROUTE_TARGET_GROUP, pa.int64(), nullable=False),
        pa.field(_ROUTE_SOURCE_UNIT, pa.int64()),
        pa.field(_ROUTE_KIND, pa.string(), nullable=False),
    ])

    def _route_batch(batch):
        if row_id_col not in batch.column_names:
            raise ValueError(
                "bucket join is missing row_id_col {!r}.".format(row_id_col))
        row_ids = batch.column(row_id_col)
        if row_ids.null_count:
            raise ValueError("bucket join produced a null row id.")
        row_ids = row_ids.cast(pa.int64())
        values = row_ids.to_numpy(zero_copy_only=False)
        in_range = _sorted_range_membership(
            values, range_starts, range_ends)
        if not in_range.all():
            bad = values[~in_range][0]
            raise ValueError(
                "_ROW_ID {} does not belong to the target snapshot."
                .format(bad))
        indexes = np.searchsorted(
            sorted_first_row_ids, values, side="right") - 1
        groups = sorted_first_row_ids[indexes]
        return pa.Table.from_arrays([
            row_ids,
            pa.array(groups, type=pa.int64()),
            pa.nulls(batch.num_rows, type=pa.int64()),
            pa.array([_ROUTE_DATA] * batch.num_rows, type=pa.string()),
        ], schema=route_schema)

    routed = joined.map_batches(_route_batch, batch_format="pyarrow")
    return routed.union(_route_markers(row_id_col, unit_indexes))


def _route_markers(row_id_col, unit_indexes):
    import ray

    route_schema = pa.schema([
        pa.field(row_id_col, pa.int64()),
        pa.field(_ROUTE_TARGET_GROUP, pa.int64(), nullable=False),
        pa.field(_ROUTE_SOURCE_UNIT, pa.int64()),
        pa.field(_ROUTE_KIND, pa.string(), nullable=False),
    ])
    markers = pa.Table.from_arrays([
        pa.nulls(len(unit_indexes), type=pa.int64()),
        pa.array([0] * len(unit_indexes), type=pa.int64()),
        pa.array(unit_indexes, type=pa.int64()),
        pa.array([_ROUTE_MARKER] * len(unit_indexes), type=pa.string()),
    ], schema=route_schema)
    return ray.data.from_arrow(markers)


def _plan_route_buckets(
        catalog,
        identifier,
        tag_name,
        row_id_col,
        expected_buckets):
    from pypaimon.common.options.core_options import CoreOptions
    from pypaimon.common.predicate_builder import PredicateBuilder

    table = catalog.get_table(identifier).copy({
        CoreOptions.SCAN_TAG_NAME.key(): tag_name,
    })
    predicate = PredicateBuilder(table.fields).equal(
        _ROUTE_KIND, _ROUTE_DATA)
    builder = (
        table.new_read_builder()
        .with_partition_filter(predicate)
        .with_projection([row_id_col])
    )
    scan = builder.new_scan()
    entries = scan.file_scanner.plan_files()
    stale = {
        entry.total_buckets
        for entry in entries
        if entry.total_buckets != expected_buckets
    }
    if stale:
        raise RuntimeError(
            "Routing table contains unexpected bucket counts {}."
            .format(sorted(stale)))
    scan.file_scanner.plan_files = lambda: entries
    splits_by_bucket = {}
    for split in scan.plan().splits():
        splits_by_bucket.setdefault(split.bucket, []).append(split)
    return (
        table,
        splits_by_bucket,
        builder.read_type(),
        builder._nested_name_paths(),
    )


def _load_row_id_files_info(table, snapshot_id):
    from pypaimon.common.options.core_options import CoreOptions
    from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
    from pypaimon.write.table_update_by_row_id import TableUpdateByRowId

    scan_table = table.copy({
        CoreOptions.SCAN_SNAPSHOT_ID.key(): str(snapshot_id),
    })
    return TableUpdateByRowId(
        scan_table,
        "_offset_target_planner_",
        BATCH_COMMIT_IDENTIFIER,
    )._snapshot_files_info()


class _OffsetTargetFilesPlan:

    def __init__(self, base_info):
        import ray

        self.base_info = base_info
        self.base_ref = ray.put(base_info)
        self.snapshot_id = base_info.snapshot_id


class _OffsetUpdateCommitter:

    def __init__(
            self,
            table,
            catalog,
            target,
            operation_id,
            update_cols,
            source_plan,
            initial_snapshot,
            checkpoint):
        self._table = table
        self._catalog = catalog
        self._target = target
        self._operation_id = operation_id
        self._update_cols = list(update_cols)
        self._source_plan = dict(source_plan)
        self._commit_user = _operation_commit_user(operation_id)
        self._checkpoint_tags = _operation_checkpoint_tags(operation_id)
        self._table_commit = None

        initialize = checkpoint is None
        if checkpoint is not None:
            snapshot, state = checkpoint
            self._checkpoint_snapshot = snapshot
            if state is None:
                initialize = True
                self._next_offset = 0
                self._num_updated = 0
                self._complete = False
                self._next_commit_identifier = 1
            else:
                if state["source"] != self._source_plan:
                    raise ValueError(
                        "PaimonOffsetSource does not match the saved checkpoint.")
                self._next_offset = state["next_offset"]
                self._num_updated = state["num_updated"]
                self._complete = state["complete"]
                self._next_commit_identifier = (
                    snapshot.commit_identifier + 1)
        else:
            self._checkpoint_snapshot = initial_snapshot
            self._next_offset = 0
            self._num_updated = 0
            self._complete = False
            self._next_commit_identifier = 1

        try:
            builder = table.new_stream_write_builder()
            builder.commit_user = self._commit_user
            self._table_commit = builder.new_commit()
            if initialize:
                self._commit_initial_checkpoint()
        except Exception:
            self.close()
            raise

    @property
    def next_offset(self):
        return self._next_offset

    @property
    def num_updated(self):
        return self._num_updated

    def _commit_initial_checkpoint(self):
        self._commit_checkpoint(
            [],
            0,
            0,
            set(),
            complete=False,
        )

    def commit_window(
            self,
            messages,
            num_updated,
            next_offset,
            planned_file_signatures):
        if next_offset <= self._next_offset:
            raise RuntimeError("Source checkpoint offset did not advance.")
        self._commit_checkpoint(
            messages,
            next_offset,
            self._num_updated + num_updated,
            planned_file_signatures,
            complete=False,
        )
        self._next_offset = next_offset
        self._num_updated += num_updated
        logger.info(
            "Committed source offsets through %d for operation %s.",
            next_offset,
            self._operation_id,
        )
        return self._checkpoint_snapshot

    def finish(self):
        if self._complete:
            return
        self._commit_checkpoint(
            [],
            self._next_offset,
            self._num_updated,
            set(),
            complete=True,
        )
        self._complete = True

    def _commit_checkpoint(
            self,
            messages,
            next_offset,
            num_updated,
            planned_file_signatures,
            complete):
        properties = _offset_checkpoint_properties(
            self._operation_id,
            self._table.table_schema.id,
            self._update_cols,
            self._source_plan,
            next_offset,
            num_updated,
            complete,
        )
        commit_identifier = self._next_commit_identifier
        ranges = _row_id_ranges_from_messages(messages)
        self._table_commit.protect_from_external_rewrites(
            self._checkpoint_snapshot, self._commit_user)
        self._table_commit.protect_planned_row_id_files(
            ranges, planned_file_signatures)
        self._table_commit.with_snapshot_properties(properties)
        if messages:
            self._table_commit.commit(messages, commit_identifier)
        else:
            self._table_commit.commit_metadata(commit_identifier)
        snapshot = _find_offset_operation_snapshot(
            self._table,
            self._commit_user,
            commit_identifier,
            self._checkpoint_snapshot.id,
        )
        if snapshot is None:
            raise RuntimeError(
                "Committed offset checkpoint snapshot cannot be found.")
        _store_operation_checkpoint(
            self._catalog,
            self._target,
            self._checkpoint_tags,
            snapshot,
        )

        self._checkpoint_snapshot = snapshot
        self._next_commit_identifier += 1

    def close(self):
        if self._table_commit is None:
            return
        try:
            self._table_commit.close()
        except Exception as error:
            logger.warning(
                "Failed to close offset update commit: %s",
                error,
                exc_info=error,
            )
        self._table_commit = None


class _IncrementalUpdateCommitter:

    def __init__(self, table, max_groups_per_commit: int):
        self._table = table
        self._max_groups_per_commit = max_groups_per_commit
        self._pending_messages: list = []
        self._pending_groups = 0
        self._table_commit = None
        self._next_commit_identifier = 1
        self._commit_failed = False
        self._deferred_commit_error = None

    def add_group(
            self,
            commit_messages,
            _num_updated,
            _row_ids,
            _planned_file_signatures=()) -> None:
        self._pending_messages.extend(commit_messages)
        self._pending_groups += 1
        if (self._deferred_commit_error is None
                and self._pending_groups >= self._max_groups_per_commit):
            try:
                self._commit_pending()
            except Exception as error:
                # Keep draining materialized Ray results so their staged files
                # are known to the driver and can be aborted by the caller.
                self._deferred_commit_error = error

    def finish(self) -> None:
        if self._deferred_commit_error is not None:
            raise self._deferred_commit_error
        self._commit_pending()

    def _commit_pending(self) -> None:
        if self._pending_groups == 0:
            return
        if not self._pending_messages:
            self._pending_groups = 0
            return

        try:
            if self._table_commit is None:
                self._table_commit = (
                    self._table.new_stream_write_builder().new_commit()
                )

            messages = self._pending_messages
            self._pending_messages = []
            self._pending_groups = 0
            commit_identifier = self._next_commit_identifier
            self._table_commit.commit(messages, commit_identifier)
        except Exception:
            self._commit_failed = True
            raise
        self._next_commit_identifier += 1

    def abort_pending(self) -> None:
        if not self._pending_messages:
            return
        messages = self._pending_messages
        self._pending_messages = []
        self._pending_groups = 0
        _abort_pending_update_messages(self._table, messages)

    def close(self) -> None:
        if self._table_commit is None:
            return
        try:
            self._table_commit.close()
        except Exception as close_error:
            logger.warning(
                "Failed to close incremental update_by_row_id commit: %s",
                close_error,
                exc_info=close_error,
            )


def delete_update_by_row_id_checkpoint(
        target: str,
        catalog_options: Dict[str, str],
        operation_id: str) -> bool:
    """Delete a completed incremental update's durable resume checkpoint."""
    from pypaimon.catalog.catalog_factory import CatalogFactory

    if not isinstance(operation_id, str) or not operation_id.strip():
        raise ValueError("operation_id must be a non-empty string.")
    catalog = CatalogFactory.create(catalog_options)
    deleted = False
    checkpoint_tags = _operation_checkpoint_tags(operation_id)
    source_retentions = []
    route_table = None
    offset_snapshots = []
    for checkpoint_tag in checkpoint_tags:
        tagged = _get_checkpoint_tag(catalog, target, checkpoint_tag)
        if (tagged is not None
                and _has_offset_checkpoint_state(tagged.snapshot)):
            offset_snapshots.append(tagged.snapshot)
    if not offset_snapshots:
        table = catalog.get_table(target)
        snapshot_manager = table.snapshot_manager()
        latest = snapshot_manager.get_latest_snapshot()
        if latest is not None:
            earliest = snapshot_manager.try_get_earliest_snapshot(latest.id)
            commit_user = _operation_commit_user(operation_id)
            for snapshot_id in range(
                    latest.id, earliest.id - 1, -1):
                snapshot = snapshot_manager.get_snapshot_by_id(snapshot_id)
                if (snapshot is not None
                        and snapshot.commit_user == commit_user
                        and _has_offset_checkpoint_state(snapshot)):
                    offset_snapshots.append(snapshot)
                    break
    if offset_snapshots:
        offset_snapshot = max(
            offset_snapshots, key=lambda snapshot: snapshot.id)
        source_plan = _offset_checkpoint_state(
            offset_snapshot)["source"]
        route_table = source_plan.get("route_table")
        source_retentions = source_plan.get("retentions") or [{
            "role": "source",
            "table": source_plan.get("table"),
        }]

    if route_table is not None:
        from pypaimon.catalog.catalog_exception import (
            TableNotExistException,
        )

        try:
            catalog.get_table(route_table)
        except TableNotExistException:
            pass
        except Exception:
            logger.warning(
                "Failed to inspect update_by_row_id routing table %s.",
                route_table,
                exc_info=True,
            )
            raise
        else:
            catalog.drop_table(route_table, False)
            deleted = True
    operation_tags = checkpoint_tags + (_operation_planner_tag(operation_id),)
    for checkpoint_tag in operation_tags:
        deleted = (
            _delete_checkpoint_tag(
                catalog, target, checkpoint_tag, ignore_missing=True)
            or deleted
        )
    for retention in source_retentions:
        if retention.get("table"):
            deleted = (
                _delete_checkpoint_tag(
                    catalog,
                    retention["table"],
                    _operation_source_tag(
                        operation_id, target, retention["role"]),
                    ignore_missing=True,
                )
                or deleted
            )
    return deleted


def _operation_digest(operation_id):
    return hashlib.sha256(operation_id.encode("utf-8")).hexdigest()[:32]


def _operation_commit_user(operation_id):
    return "pypaimon-ray-update-" + _operation_digest(operation_id)


def _operation_checkpoint_tags(operation_id):
    base = _CHECKPOINT_TAG_PREFIX + _operation_digest(operation_id)
    return base + "_0", base + "_1"


def _operation_planner_tag(operation_id):
    return _CHECKPOINT_TAG_PREFIX + _operation_digest(operation_id) + "_plan"


def _operation_source_tag(operation_id, target, role="source"):
    identity = target + "\0" + operation_id
    if role != "source":
        identity += "\0" + role
    return (
        _CHECKPOINT_TAG_PREFIX
        + _operation_digest(identity)
        + "_source"
    )


def _offset_checkpoint_properties(
        operation_id,
        schema_id,
        update_cols,
        source_plan,
        next_offset,
        num_updated,
        complete):
    state = {
        "version": _OFFSET_CHECKPOINT_VERSION,
        "mode": _OFFSET_CHECKPOINT_MODE,
        "operation_id": operation_id,
        "schema_id": schema_id,
        "update_cols": list(update_cols),
        "source": dict(source_plan),
        "next_offset": next_offset,
        "num_updated": num_updated,
        "complete": complete,
    }
    return {
        _CHECKPOINT_PROPERTY: json.dumps(
            state, sort_keys=True, separators=(",", ":"))
    }


def _offset_checkpoint_state(snapshot):
    encoded = (snapshot.properties or {}).get(_CHECKPOINT_PROPERTY)
    if encoded is None:
        return None
    try:
        state = json.loads(encoded)
    except Exception as error:
        raise RuntimeError(
            "Invalid source-offset checkpoint JSON.") from error
    if (state.get("version") != _OFFSET_CHECKPOINT_VERSION
            or state.get("mode") != _OFFSET_CHECKPOINT_MODE):
        raise RuntimeError(
            "operation_id belongs to a different checkpoint mode.")
    required = {
        "operation_id",
        "schema_id",
        "update_cols",
        "source",
        "next_offset",
        "num_updated",
        "complete",
    }
    if not required.issubset(state):
        raise RuntimeError("Incomplete source-offset checkpoint.")
    if not isinstance(state["source"], dict):
        raise RuntimeError("Invalid source-offset checkpoint source.")
    required_source = {
        "kind",
        "table",
        "snapshot_id",
        "fingerprint",
        "num_units",
        "units_per_checkpoint",
    }
    if not required_source.issubset(state["source"]):
        raise RuntimeError("Incomplete source-offset checkpoint source.")
    source = state["source"]
    if (source["kind"] not in (
            "paimon-units-v1", "paimon-bucket-join-v1")
            or not isinstance(source["table"], str)
            or not isinstance(source["fingerprint"], str)
            or isinstance(source["num_units"], bool)
            or not isinstance(source["num_units"], int)
            or source["num_units"] < 0
            or isinstance(source["units_per_checkpoint"], bool)
            or not isinstance(source["units_per_checkpoint"], int)
            or source["units_per_checkpoint"] <= 0
            or (source["snapshot_id"] is not None
                and (isinstance(source["snapshot_id"], bool)
                     or not isinstance(source["snapshot_id"], int)
                     or source["snapshot_id"] <= 0))):
        raise RuntimeError("Invalid source-offset checkpoint source.")
    if source["kind"] == "paimon-units-v1" and (
            "rows_per_unit" not in source
            or isinstance(source["rows_per_unit"], bool)
            or not isinstance(source["rows_per_unit"], int)
            or source["rows_per_unit"] <= 0):
        raise RuntimeError("Invalid source-offset checkpoint source.")
    if source["kind"] == "paimon-bucket-join-v1" and (
            not isinstance(source.get("right_table"), str)
            or isinstance(source.get("right_snapshot_id"), bool)
            or not isinstance(source.get("right_snapshot_id"), int)
            or source["right_snapshot_id"] <= 0
            or isinstance(source.get("route_num_units"), bool)
            or not isinstance(source.get("route_num_units"), int)
            or source["route_num_units"] < 0
            or isinstance(source.get("routing_buckets"), bool)
            or not isinstance(source.get("routing_buckets"), int)
            or source["routing_buckets"] <= 0
            or source["num_units"] != source["routing_buckets"]
            or isinstance(source.get("route_units_per_commit"), bool)
            or not isinstance(source.get("route_units_per_commit"), int)
            or source["route_units_per_commit"] <= 0
            or not isinstance(source.get("row_id_col"), str)
            or not source["row_id_col"]
            or not isinstance(source.get("route_table"), str)
            or not source["route_table"]
            or not isinstance(source.get("route_tag"), str)
            or not source["route_tag"]
            or isinstance(source.get("target_snapshot_id"), bool)
            or not isinstance(source.get("target_snapshot_id"), int)
            or source["target_snapshot_id"] <= 0):
        raise RuntimeError("Invalid source-offset checkpoint source.")
    retentions = source.get("retentions")
    if retentions is not None:
        roles = set()
        for retention in retentions:
            if (not isinstance(retention, dict)
                    or not isinstance(retention.get("role"), str)
                    or not isinstance(retention.get("table"), str)
                    or isinstance(retention.get("snapshot_id"), bool)
                    or not isinstance(retention.get("snapshot_id"), int)
                    or retention["snapshot_id"] <= 0
                    or retention["role"] in roles):
                raise RuntimeError(
                    "Invalid source-offset checkpoint retention.")
            roles.add(retention["role"])
    for name in ("next_offset", "num_updated"):
        value = state[name]
        if (isinstance(value, bool)
                or not isinstance(value, int)
                or value < 0):
            raise RuntimeError(
                "Invalid source-offset checkpoint {}.".format(name))
    if not isinstance(state["complete"], bool):
        raise RuntimeError(
            "Invalid source-offset checkpoint completion state.")
    if (state["next_offset"] > source["num_units"]
            or (state["complete"]
                and state["next_offset"] != source["num_units"])):
        raise RuntimeError(
            "Invalid source-offset checkpoint progress.")
    return state


def _validate_offset_checkpoint_state(
        snapshot,
        operation_id,
        update_cols,
        schema_id,
        state):
    if state["operation_id"] != operation_id:
        raise RuntimeError(
            "Source-offset update operation id hash collision.")
    if state["schema_id"] != schema_id:
        raise RuntimeError(
            "Target schema changed since source-offset operation "
            "{!r} started.".format(operation_id))
    if state["update_cols"] != list(update_cols):
        raise ValueError(
            "operation_id {!r} was already used with update_cols {}; "
            "got {}.".format(
                operation_id, state["update_cols"], list(update_cols)))
    return state


def _load_offset_operation_checkpoint(
        catalog,
        target,
        table,
        operation_id,
        update_cols,
        commit_user,
        checkpoint_tags):
    snapshot_manager = table.snapshot_manager()
    latest = snapshot_manager.get_latest_snapshot()
    tagged_snapshots = []
    for checkpoint_tag in checkpoint_tags:
        tagged = _get_checkpoint_tag(catalog, target, checkpoint_tag)
        if tagged is not None:
            tagged_snapshots.append(tagged.snapshot)

    checkpoint_snapshot = (
        max(tagged_snapshots, key=lambda snapshot: snapshot.id)
        if tagged_snapshots else None)
    if latest is not None:
        if checkpoint_snapshot is not None:
            lower_bound = checkpoint_snapshot.id + 1
        else:
            earliest = snapshot_manager.try_get_earliest_snapshot(latest.id)
            lower_bound = earliest.id
        for snapshot_id in range(latest.id, lower_bound - 1, -1):
            snapshot = snapshot_manager.get_snapshot_by_id(snapshot_id)
            if (snapshot is not None
                    and snapshot.commit_user == commit_user
                    and _has_offset_checkpoint_state(snapshot)):
                checkpoint_snapshot = snapshot
                break

    if checkpoint_snapshot is None:
        return None
    state = _offset_checkpoint_state(checkpoint_snapshot)
    if state is None:
        return checkpoint_snapshot, None
    if checkpoint_snapshot.commit_user != commit_user:
        raise RuntimeError(
            "Source-offset checkpoint tag belongs to another writer.")
    state = _validate_offset_checkpoint_state(
        checkpoint_snapshot,
        operation_id,
        update_cols,
        table.table_schema.id,
        state,
    )
    _store_operation_checkpoint(
        catalog, target, checkpoint_tags, checkpoint_snapshot)
    return checkpoint_snapshot, state


def _find_offset_operation_snapshot(
        table,
        commit_user,
        commit_identifier,
        after_snapshot_id):
    snapshot_manager = table.snapshot_manager()
    latest = snapshot_manager.get_latest_snapshot()
    if latest is None or latest.id <= after_snapshot_id:
        return None
    for snapshot_id in range(
            latest.id, after_snapshot_id, -1):
        snapshot = snapshot_manager.get_snapshot_by_id(snapshot_id)
        if (snapshot is not None
                and snapshot.commit_user == commit_user
                and snapshot.commit_identifier == commit_identifier
                and _has_offset_checkpoint_state(snapshot)):
            return snapshot
    return None


def _store_operation_checkpoint(
        catalog, target, checkpoint_tags, snapshot):
    for attempt in range(_CHECKPOINT_TAG_UPDATE_ATTEMPTS):
        try:
            _store_operation_checkpoint_once(
                catalog, target, checkpoint_tags, snapshot)
            return
        except Exception:
            if attempt + 1 == _CHECKPOINT_TAG_UPDATE_ATTEMPTS:
                raise
            logger.warning(
                "Retry resumable update checkpoint tag.",
                exc_info=True,
            )
            time.sleep(0.1 * (2 ** attempt))


def _store_operation_checkpoint_once(
        catalog, target, checkpoint_tags, snapshot):
    slot = snapshot.commit_identifier % 2
    checkpoint_tag = checkpoint_tags[slot]
    previous = _get_checkpoint_tag(catalog, target, checkpoint_tag)
    if previous is None or previous.snapshot.id != snapshot.id:
        if previous is not None:
            _delete_checkpoint_tag(catalog, target, checkpoint_tag)
        catalog.create_tag(
            target, checkpoint_tag, snapshot_id=snapshot.id)
    _delete_checkpoint_tag(
        catalog, target, checkpoint_tags[1 - slot], ignore_missing=True)


def _get_checkpoint_tag(catalog, target, checkpoint_tag):
    from pypaimon.catalog.catalog_exception import TagNotExistException

    try:
        return catalog.get_tag(target, checkpoint_tag)
    except TagNotExistException:
        return None


def _ensure_source_tag(
        catalog, source_table, source_tag, snapshot_id):
    previous = _get_checkpoint_tag(catalog, source_table, source_tag)
    if previous is not None:
        if previous.snapshot.id != snapshot_id:
            raise ValueError(
                "operation_id is already bound to another source snapshot.")
        return
    catalog.create_tag(
        source_table, source_tag, snapshot_id=snapshot_id)


def _delete_checkpoint_tag(
        catalog, target, checkpoint_tag, ignore_missing=False):
    from pypaimon.catalog.catalog_exception import TagNotExistException

    try:
        catalog.delete_tag(target, checkpoint_tag)
        return True
    except TagNotExistException:
        if not ignore_missing:
            raise
        return False


def _has_offset_checkpoint_state(snapshot):
    if _CHECKPOINT_PROPERTY not in (snapshot.properties or {}):
        return False
    try:
        state = json.loads(
            snapshot.properties[_CHECKPOINT_PROPERTY])
    except Exception:
        return False
    return (
        state.get("version") == _OFFSET_CHECKPOINT_VERSION
        and state.get("mode") == _OFFSET_CHECKPOINT_MODE
    )


def _merge_row_id_ranges(*range_groups):
    from pypaimon.utils.range import Range

    return Range.sort_and_merge_overlap(
        [
            row_range
            for ranges in range_groups
            for row_range in ranges
        ],
        True,
        True,
    )


def _row_id_ranges_from_messages(messages):
    ranges = []
    for message in messages:
        for data_file in message.new_files:
            row_range = data_file.row_id_range()
            if row_range is not None:
                ranges.append(row_range)
    merged = _merge_row_id_ranges(ranges)
    if messages and not merged:
        raise RuntimeError(
            "Cannot checkpoint update messages without row-id ranges.")
    return merged


def _commit_update_messages(table, commit_messages) -> None:
    pending_msgs: list = list(commit_messages)
    commit_started = False

    try:
        table_commit = None
        try:
            table_commit = table.new_batch_write_builder().new_commit()
            commit_started = True
            table_commit.commit(pending_msgs)
        finally:
            if table_commit is not None:
                try:
                    table_commit.close()
                except Exception as close_error:
                    logger.warning(
                        "Failed to close update_by_row_id commit: %s",
                        close_error,
                        exc_info=close_error,
                    )
    except Exception as e:
        if not commit_started:
            _abort_pending_update_messages(table, pending_msgs)
        _reraise_inner(e)


def _abort_pending_update_messages(table, commit_messages) -> None:
    if not commit_messages:
        return

    table_commit = None
    try:
        table_commit = table.new_batch_write_builder().new_commit()
        table_commit.abort(commit_messages)
    except Exception as abort_error:
        logger.warning(
            "Failed to abort pending update_by_row_id commit messages: %s",
            abort_error,
            exc_info=abort_error,
        )
    finally:
        if table_commit is not None:
            try:
                table_commit.close()
            except Exception as close_error:
                logger.warning(
                    "Failed to close update_by_row_id abort commit: %s",
                    close_error,
                    exc_info=close_error,
                )
