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
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import pyarrow as pa

from pypaimon.ray.data_evolution_merge_transform import (
    SourceColumnRef,
    _NormalizedClause,
    build_delete_schema,
    build_update_schema,
    cast_to_schema,
    vectorized_delete_transform,
    vectorized_insert_transform,
    vectorized_matched_transform,
)


logger = logging.getLogger(__name__)


class GroupApplyError(RuntimeError):
    """A distributed update group failed."""


def _group_error_text(error: BaseException) -> str:
    import traceback

    return "distributed update group failed: {}: {}\n{}".format(
        type(error).__name__,
        str(error),
        "".join(traceback.format_exception(
            type(error), error, error.__traceback__)),
    )


def _group_result(messages, num_updated, row_ids=(), error=None):
    import pickle

    return pa.table({
        "msgs_blob": pa.array([pickle.dumps(messages)], type=pa.binary()),
        "n_updated": pa.array([num_updated], type=pa.int64()),
        "row_ids_blob": pa.array([pickle.dumps(row_ids)], type=pa.binary()),
        "error": pa.array([error], type=pa.string()),
    })


def _is_transform_factory(transform):
    import functools

    return (isinstance(transform, type)
            or (isinstance(transform, functools.partial)
                and isinstance(transform.func, type)))


def _as_arrow_table(value):
    if isinstance(value, pa.Table):
        return value
    if isinstance(value, pa.RecordBatch):
        return pa.Table.from_batches([value])
    if isinstance(value, dict):
        return pa.table(value)
    try:
        import pandas as pd
    except ImportError:
        pd = None
    if pd is not None and isinstance(value, pd.DataFrame):
        return pa.Table.from_pandas(value, preserve_index=False)
    raise ValueError(
        "transform must return a pyarrow.Table, RecordBatch, pandas "
        "DataFrame, or column mapping.")


class _RangeTransformUpdateWorker:

    def __init__(
            self,
            table,
            read_table,
            files_info_ref,
            read_projection,
            update_cols,
            update_schema,
            transform,
            transform_filter,
            transform_predicate,
            transform_batch_size,
            max_application_retries,
            retry_exceptions):
        self._table = table
        self._read_table = read_table
        self._init_error = None
        self._max_application_retries = max_application_retries
        self._retry_exceptions = retry_exceptions
        attempts = 0
        while True:
            try:
                import ray

                self._files_info = ray.get(files_info_ref)
                self._read_projection = list(read_projection)
                self._update_cols = list(update_cols)
                self._update_schema = update_schema
                self._transform = (
                    transform()
                    if _is_transform_factory(transform) else transform)
                self._transform_filter = transform_filter
                self._transform_predicate = transform_predicate
                self._transform_batch_size = transform_batch_size
                break
            except Exception as error:
                if self._should_retry(error, attempts):
                    attempts += 1
                    continue
                self._init_error = _group_error_text(error)
                break

    def _should_retry(self, error, attempts):
        retry = self._retry_exceptions
        if retry is True:
            retryable = True
        elif isinstance(retry, (list, tuple)):
            try:
                retryable = isinstance(error, tuple(retry))
            except TypeError:
                retryable = False
        else:
            retryable = False
        return retryable and (
            self._max_application_retries == -1
            or attempts < self._max_application_retries
        )

    def _file_groups(self, group):
        return [
            (first_row_id, None)
            for first_row_id in dict.fromkeys(
                group.column("_FIRST_ROW_ID").to_pylist())
        ]

    def _read_batches(self, first_row_id, row_ids):
        from pypaimon.globalindex.indexed_split import IndexedSplit
        from pypaimon.read.split import DataSplit
        from pypaimon.table.special_fields import SpecialFields
        from pypaimon.utils.range import Range

        row_id_col = SpecialFields.ROW_ID.name
        owning_split, target_files = (
            self._files_info.first_row_id_index[first_row_id])
        split = DataSplit(
            files=target_files,
            partition=owning_split.partition,
            bucket=owning_split.bucket,
            raw_convertible=True,
        )
        if row_ids is not None:
            split = IndexedSplit(split, Range.to_ranges(list(row_ids)))

        projection = list(self._read_projection)
        if row_id_col not in projection:
            projection.append(row_id_col)
        read_builder = self._read_table.new_read_builder()
        if self._transform_predicate is not None:
            read_builder = read_builder.with_filter(
                self._transform_predicate)
        reader = (
            read_builder.with_projection(projection)
            .new_read().to_arrow_batch_reader([split])
        )
        try:
            for record_batch in reader:
                table = pa.Table.from_batches([record_batch])
                for batch in table.to_batches(
                        max_chunksize=self._transform_batch_size):
                    yield pa.Table.from_batches([batch])
        finally:
            reader.close()

    def _build_updates(self, first_row_id, selected_row_ids):
        from pypaimon.table.special_fields import SpecialFields

        row_id_col = SpecialFields.ROW_ID.name
        updates = []
        for batch in self._read_batches(first_row_id, selected_row_ids):
            if self._transform_filter is not None:
                mask = self._filter_mask(
                    self._transform_filter(
                        batch.select(self._read_projection)),
                    batch.num_rows,
                )
                batch = batch.filter(mask)
            if batch.num_rows == 0:
                continue
            result = _as_arrow_table(
                self._transform(batch.select(self._read_projection)))
            missing = [
                col for col in self._update_cols
                if col not in result.column_names
            ]
            if missing:
                raise ValueError(
                    "transform output is missing columns {}.".format(missing))
            if result.num_rows != batch.num_rows:
                raise ValueError(
                    "transform output must preserve input row count.")
            try:
                projected = pa.Table.from_arrays(
                    [batch.column(row_id_col)] + [
                        result.column(col) for col in self._update_cols
                    ],
                    names=[row_id_col] + self._update_cols,
                ).cast(self._update_schema)
            except Exception as error:
                raise ValueError(
                    "transform output does not match the update schema.") from error
            if projected.num_rows:
                updates.append(projected)
        if not updates:
            return self._update_schema.empty_table()
        return pa.concat_tables(updates)

    @staticmethod
    def _filter_mask(value, expected_rows):
        try:
            mask = value if isinstance(
                value, (pa.Array, pa.ChunkedArray)) else pa.array(value)
        except Exception as error:
            raise ValueError(
                "callable filter must return a boolean mask.") from error
        if not pa.types.is_boolean(mask.type) or len(mask) != expected_rows:
            raise ValueError(
                "callable filter must return one boolean per input row.")
        if mask.null_count:
            import pyarrow.compute as pc
            mask = pc.fill_null(mask, False)
        return mask

    def __call__(self, group):
        from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
        from pypaimon.write.table_update_by_row_id import TableUpdateByRowId

        if self._init_error is not None:
            return _group_result([], 0, error=self._init_error)

        attempts = 0
        while True:
            writer = None
            try:
                writer = TableUpdateByRowId(
                    self._table,
                    "_ray_update_worker_",
                    BATCH_COMMIT_IDENTIFIER,
                    _precomputed_files_info=self._files_info,
                )
                num_updated = 0
                for first_row_id, row_ids in self._file_groups(group):
                    updates = self._build_updates(first_row_id, row_ids)
                    if updates.num_rows:
                        writer.update_columns(updates, self._update_cols)
                        num_updated += updates.num_rows
                return _group_result(writer.commit_messages, num_updated)
            except Exception as error:
                if self._should_retry(error, attempts):
                    _abort_failed_writer(self._table, writer)
                    attempts += 1
                    continue
                return _failed_group_result(self._table, writer, error)


def _apply_range_transform(group, **worker_options):
    return _RangeTransformUpdateWorker(**worker_options)(group)


def _map_kwargs(
    ray_remote_args: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Build kwargs for map_batches/map_groups; spread ray_remote_args because
    those APIs take remote options as **kwargs, not under a 'ray_remote_args'
    key."""
    kwargs: Dict[str, Any] = {"batch_format": "pyarrow"}
    if ray_remote_args:
        kwargs.update(ray_remote_args)
    return kwargs


def _resolve_source_projection(
    clauses: List[_NormalizedClause],
    source_on: Sequence[str],
    source_field_names: Sequence[str],
) -> list:
    needed = set(source_on)
    source_set = set(source_field_names)

    for clause in clauses:
        for value in clause.spec.values():
            if isinstance(value, SourceColumnRef):
                needed.add(value.column)
        if clause.condition is not None:
            from pypaimon.ray.merge_condition import extract_columns
            for ref in extract_columns(clause.condition):
                prefix, col = ref.split(".", 1)
                if prefix == "s" and col in source_set:
                    needed.add(col)

    return [c for c in source_field_names if c in needed]


def _build_matched_transform(
    clauses: List[_NormalizedClause],
    on_map: Dict[str, str],
    on_pairs: List[Tuple[str, str]],
    update_cols: List[str],
    row_id_name: str,
    update_schema: pa.Schema,
):
    prepared_clauses = []
    for clause in clauses:
        rewritten = None
        if clause.condition is not None:
            from pypaimon.ray.merge_condition import (
                remap_source_on_keys, rewrite_condition,
            )
            rewritten = remap_source_on_keys(
                rewrite_condition(clause.condition), on_map,
            )
        prepared_clauses.append((clause.spec, rewritten, clause.delete))

    _filter_batch = None
    if any(r is not None for _, r, _ in prepared_clauses):
        from pypaimon.ray.merge_condition import filter_batch as _filter_batch

    def _transform(batch: pa.Table) -> pa.Table:
        remaining = batch
        parts = []
        for spec, rewritten, is_delete in prepared_clauses:
            if remaining.num_rows == 0:
                break
            if rewritten is not None:
                matched = _filter_batch(
                    remaining, rewritten, _pre_rewritten=True,
                )
            else:
                matched = remaining
            if matched.num_rows == 0:
                continue
            if not is_delete:
                parts.append(vectorized_matched_transform(
                    matched, spec, on_pairs,
                    update_cols, row_id_name,
                    update_schema,
                ))
            if rewritten is not None and matched.num_rows < remaining.num_rows:
                not_cond = f"COALESCE(NOT ({rewritten}), TRUE)"
                remaining = _filter_batch(
                    remaining, not_cond, _pre_rewritten=True,
                )
            else:
                remaining = remaining.slice(0, 0)
        if not parts:
            return update_schema.empty_table()
        return pa.concat_tables(parts)

    return _transform


def _build_matched_delete_transform(
    clauses: List[_NormalizedClause],
    on_map: Dict[str, str],
    row_id_name: str,
    delete_schema: pa.Schema,
):
    prepared_clauses = []
    for clause in clauses:
        rewritten = None
        if clause.condition is not None:
            from pypaimon.ray.merge_condition import (
                remap_source_on_keys, rewrite_condition,
            )
            rewritten = remap_source_on_keys(
                rewrite_condition(clause.condition), on_map,
            )
        prepared_clauses.append((rewritten, clause.delete))

    _filter_batch = None
    if any(r is not None for r, _ in prepared_clauses):
        from pypaimon.ray.merge_condition import filter_batch as _filter_batch

    def _transform(batch: pa.Table) -> pa.Table:
        remaining = batch
        parts = []
        for rewritten, is_delete in prepared_clauses:
            if remaining.num_rows == 0:
                break
            if rewritten is not None:
                matched = _filter_batch(
                    remaining, rewritten, _pre_rewritten=True,
                )
            else:
                matched = remaining
            if matched.num_rows > 0 and is_delete:
                parts.append(
                    vectorized_delete_transform(
                        matched, row_id_name, delete_schema,
                    )
                )
            if rewritten is not None and matched.num_rows < remaining.num_rows:
                not_cond = f"COALESCE(NOT ({rewritten}), TRUE)"
                remaining = _filter_batch(
                    remaining, not_cond, _pre_rewritten=True,
                )
            else:
                remaining = remaining.slice(0, 0)
        if not parts:
            return delete_schema.empty_table()
        return pa.concat_tables(parts)

    return _transform


def build_self_merge_update_ds(
    *,
    target_identifier: str,
    clauses: List[_NormalizedClause],
    target_field_names: Sequence[str],
    target_pa_schema: pa.Schema,
    update_cols: Sequence[str],
    catalog_options: Dict[str, str],
    resolve_target_projection,
    snapshot_id: Optional[int] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
) -> Tuple:
    from pypaimon.ray.ray_paimon import read_paimon
    from pypaimon.table.special_fields import SpecialFields

    row_id_name = SpecialFields.ROW_ID.name
    needed_cols = set(resolve_target_projection(
        clauses, [row_id_name], update_cols, target_field_names,
    ))
    for clause in clauses:
        for value in clause.spec.values():
            if isinstance(value, SourceColumnRef):
                needed_cols.add(value.column)
    target_set = set(target_field_names)
    for clause in clauses:
        if clause.condition is not None:
            from pypaimon.ray.merge_condition import extract_columns
            for ref in extract_columns(clause.condition):
                prefix, col = ref.split(".", 1)
                if prefix == "s" and col in target_set:
                    needed_cols.add(col)
    projection = [row_id_name] + [
        c for c in target_field_names if c in needed_cols
    ]

    target_ds = read_paimon(
        target_identifier, catalog_options,
        projection=projection, snapshot_id=snapshot_id,
    )
    update_schema = build_update_schema(target_pa_schema, update_cols, row_id_name)

    orig_names = target_ds.schema().names
    target_renamed = target_ds.rename_columns(
        {c: f"t.{c}" for c in orig_names}
    )

    def _add_source_aliases(batch: pa.Table) -> pa.Table:
        columns = list(batch.columns)
        names = list(batch.schema.names)
        for orig in orig_names:
            if orig == row_id_name:
                continue
            t_col_name = f"t.{orig}"
            if t_col_name in names:
                idx = names.index(t_col_name)
                columns.append(columns[idx])
                names.append(f"s.{orig}")
        return pa.table(columns, names=names)

    aliased = target_renamed.map_batches(
        _add_source_aliases, **_map_kwargs(ray_remote_args),
    )

    _transform = _build_matched_transform(
        clauses,
        on_map={row_id_name: row_id_name},
        on_pairs=[(row_id_name, row_id_name)],
        update_cols=list(update_cols),
        row_id_name=row_id_name,
        update_schema=update_schema,
    )
    return aliased.map_batches(_transform, **_map_kwargs(ray_remote_args))


def build_self_merge_delete_ds(
    *,
    target_identifier: str,
    clauses: List[_NormalizedClause],
    target_field_names: Sequence[str],
    catalog_options: Dict[str, str],
    resolve_target_projection,
    snapshot_id: Optional[int] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
) -> Tuple:
    from pypaimon.ray.ray_paimon import read_paimon
    from pypaimon.table.special_fields import SpecialFields

    row_id_name = SpecialFields.ROW_ID.name
    needed_cols = set(resolve_target_projection(
        clauses, [row_id_name], [], target_field_names,
    ))
    target_set = set(target_field_names)
    for clause in clauses:
        if clause.condition is not None:
            from pypaimon.ray.merge_condition import extract_columns
            for ref in extract_columns(clause.condition):
                prefix, col = ref.split(".", 1)
                if prefix == "s" and col in target_set:
                    needed_cols.add(col)
    projection = [row_id_name] + [
        c for c in target_field_names if c in needed_cols
    ]

    target_ds = read_paimon(
        target_identifier, catalog_options,
        projection=projection, snapshot_id=snapshot_id,
    )
    delete_schema = build_delete_schema(row_id_name)

    orig_names = target_ds.schema().names
    target_renamed = target_ds.rename_columns(
        {c: f"t.{c}" for c in orig_names}
    )

    def _add_source_aliases(batch: pa.Table) -> pa.Table:
        columns = list(batch.columns)
        names = list(batch.schema.names)
        for orig in orig_names:
            if orig == row_id_name:
                continue
            t_col_name = f"t.{orig}"
            if t_col_name in names:
                idx = names.index(t_col_name)
                columns.append(columns[idx])
                names.append(f"s.{orig}")
        return pa.table(columns, names=names)

    aliased = target_renamed.map_batches(
        _add_source_aliases, **_map_kwargs(ray_remote_args),
    )

    _transform = _build_matched_delete_transform(
        clauses,
        on_map={row_id_name: row_id_name},
        row_id_name=row_id_name,
        delete_schema=delete_schema,
    )
    return aliased.map_batches(_transform, **_map_kwargs(ray_remote_args))


def build_matched_update_ds(
    *,
    target_identifier: str,
    source_ds,
    target_on: Sequence[str],
    source_on: Sequence[str],
    clauses: List[_NormalizedClause],
    target_field_names: Sequence[str],
    target_pa_schema: pa.Schema,
    update_cols: Sequence[str],
    catalog_options: Dict[str, str],
    num_partitions: int,
    resolve_target_projection,
    snapshot_id: Optional[int] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
) -> Tuple:
    from pypaimon.ray.ray_paimon import read_paimon
    from pypaimon.table.special_fields import SpecialFields

    row_id_name = SpecialFields.ROW_ID.name
    needed_cols = resolve_target_projection(
        clauses, target_on, update_cols, target_field_names,
    )
    projection = [row_id_name] + [c for c in needed_cols if c != row_id_name]

    target_ds = read_paimon(
        target_identifier, catalog_options,
        projection=projection, snapshot_id=snapshot_id,
    )
    update_schema = build_update_schema(target_pa_schema, update_cols, row_id_name)

    target_renamed = target_ds.rename_columns(
        {c: f"t.{c}" for c in target_ds.schema().names}
    )
    source_cols = _resolve_source_projection(
        clauses, source_on, source_ds.schema().names,
    )
    source_ds = source_ds.select_columns(source_cols)
    source_renamed = source_ds.rename_columns(
        {c: f"s.{c}" for c in source_cols}
    )

    joined = target_renamed.join(
        source_renamed,
        join_type="inner",
        num_partitions=num_partitions,
        on=tuple(f"t.{c}" for c in target_on),
        right_on=tuple(f"s.{c}" for c in source_on),
    )

    _transform = _build_matched_transform(
        clauses,
        on_map=dict(zip(source_on, target_on)),
        on_pairs=list(zip(source_on, target_on)),
        update_cols=list(update_cols),
        row_id_name=row_id_name,
        update_schema=update_schema,
    )
    return joined.map_batches(_transform, **_map_kwargs(ray_remote_args))


def build_matched_delete_ds(
    *,
    target_identifier: str,
    source_ds,
    target_on: Sequence[str],
    source_on: Sequence[str],
    clauses: List[_NormalizedClause],
    target_field_names: Sequence[str],
    catalog_options: Dict[str, str],
    num_partitions: int,
    resolve_target_projection,
    snapshot_id: Optional[int] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
) -> Tuple:
    from pypaimon.ray.ray_paimon import read_paimon
    from pypaimon.table.special_fields import SpecialFields

    row_id_name = SpecialFields.ROW_ID.name
    needed_cols = resolve_target_projection(
        clauses,
        target_on,
        [],
        target_field_names,
    )
    projection = [row_id_name] + [c for c in needed_cols if c != row_id_name]

    target_ds = read_paimon(
        target_identifier, catalog_options,
        projection=projection, snapshot_id=snapshot_id,
    )
    delete_schema = build_delete_schema(row_id_name)

    target_renamed = target_ds.rename_columns(
        {c: f"t.{c}" for c in target_ds.schema().names}
    )
    source_cols = list(source_ds.schema().names)
    source_renamed = source_ds.rename_columns(
        {c: f"s.{c}" for c in source_cols}
    )

    joined = target_renamed.join(
        source_renamed,
        join_type="inner",
        num_partitions=num_partitions,
        on=tuple(f"t.{c}" for c in target_on),
        right_on=tuple(f"s.{c}" for c in source_on),
    )

    _transform = _build_matched_delete_transform(
        clauses,
        on_map=dict(zip(source_on, target_on)),
        row_id_name=row_id_name,
        delete_schema=delete_schema,
    )
    return joined.map_batches(_transform, **_map_kwargs(ray_remote_args))


def distributed_update_apply(
    update_ds,
    table,
    write_update_cols: Sequence[str],
    *,
    num_partitions: int,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    base_snapshot_id: Optional[int] = None,
    collect_row_ids: bool = False,
    on_group_result: Optional[Callable[[list, int, list], None]] = None,
    rows_per_range: Optional[int] = None,
    read_projection: Optional[Sequence[str]] = None,
    transform: Optional[Callable] = None,
    transform_filter: Optional[Callable] = None,
    transform_predicate=None,
    transform_update_schema: Optional[pa.Schema] = None,
    transform_batch_size: int = 1024,
) -> Tuple[list, int, list]:
    import numpy as np
    import pickle
    import uuid

    import pyarrow.compute as pc
    import ray

    from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
    from pypaimon.table.special_fields import SpecialFields
    from pypaimon.write.table_update_by_row_id import TableUpdateByRowId

    row_id_name = SpecialFields.ROW_ID.name
    cols = list(write_update_cols)

    for col in cols:
        if col not in table.field_names:
            raise ValueError(
                f"Column '{col}' is not in target table schema."
            )

    # Pin the planner to the caller's base snapshot so row-id routing and the
    # commit-time conflict check agree even if a concurrent commit lands (mirrors
    # the delete path).
    from pypaimon.common.options.core_options import CoreOptions
    scan_options = {}
    if base_snapshot_id is not None:
        scan_options[CoreOptions.SCAN_SNAPSHOT_ID.key()] = str(base_snapshot_id)
    scan_table = (
        table.copy_without_time_travel(scan_options)
        if scan_options else table
    )
    planner = TableUpdateByRowId(
        scan_table,
        "_merge_into_planner_" + uuid.uuid4().hex[:8],
        BATCH_COMMIT_IDENTIFIER,
    )
    sorted_first_row_ids = list(planner.first_row_ids)
    if not sorted_first_row_ids:
        return [], 0, []

    # Pin commit-time conflict check to the snapshot the join was built on,
    # so concurrent commits between read and planner are detected.
    check_from_snapshot = (
        base_snapshot_id if base_snapshot_id is not None
        else planner.snapshot_id
    )

    # Put file metadata into Ray's object store and pass a single ref to
    # workers. Avoids per-task manifest re-scans (Jingsong review #6) and
    # avoids serializing the metadata into every task's closure. Override
    # snapshot_id with the join's base snapshot so commit-time conflict
    # detection covers the read→planner window.
    from dataclasses import replace
    files_info = replace(
        planner._snapshot_files_info(),
        snapshot_id=check_from_snapshot,
    )
    precomputed_info_ref = ray.put(files_info)

    frid_col = "_FIRST_ROW_ID"
    range_col = "_ROW_ID_RANGE_START"
    captured_sorted = sorted_first_row_ids
    captured_sorted_arr = np.asarray(captured_sorted, dtype=np.int64)
    valid_ranges = planner.valid_row_id_ranges
    range_starts = np.asarray([r.from_ for r in valid_ranges], dtype=np.int64)
    range_ends = np.asarray([r.to for r in valid_ranges], dtype=np.int64)
    row_id_range_starts = (
        _row_id_range_starts(files_info, rows_per_range)
        if rows_per_range is not None else None
    )

    def _assign_frid(batch: pa.Table) -> pa.Table:
        if batch.num_rows == 0:
            result = batch.append_column(
                frid_col, pa.array([], type=pa.int64()))
            if row_id_range_starts is None:
                return result
            return result.append_column(
                range_col, pa.array([], type=pa.int64()))
        rid_col = batch.column(row_id_name)
        if rid_col.null_count:
            raise ValueError(
                "_ROW_ID is null; planner snapshot is stale "
                "or matched rows come from a different table."
            )
        rids = rid_col.to_numpy(zero_copy_only=False)
        in_range = np.zeros(len(rids), dtype=bool)
        for start, end in zip(range_starts, range_ends):
            in_range |= (rids >= start) & (rids <= end)
        if not in_range.all():
            bad = rids[~in_range][0]
            raise ValueError(
                f"_ROW_ID {bad} does not belong to any valid range "
                f"{[f'[{r.from_}, {r.to}]' for r in valid_ranges]}; "
                "planner snapshot is stale or row ids come from another table."
            )
        indexes = np.searchsorted(
            captured_sorted_arr, rids, side="right") - 1
        result = batch.append_column(
            frid_col,
            pa.array(captured_sorted_arr[indexes], type=pa.int64()))
        if row_id_range_starts is None:
            return result
        return result.append_column(
            range_col,
            pa.array(row_id_range_starts[indexes], type=pa.int64()),
        )

    worker_map_kwargs = _map_kwargs(ray_remote_args)
    if transform is not None:
        with_frid = ray.data.from_arrow(pa.table({
            frid_col: pa.array(captured_sorted, type=pa.int64()),
            range_col: pa.array(row_id_range_starts, type=pa.int64()),
        }))
    else:
        with_frid = update_ds.map_batches(
            _assign_frid, **worker_map_kwargs)

    captured_table = table
    captured_cols = cols
    capture_group_errors = on_group_result is not None

    def _apply_group(group: pa.Table) -> pa.Table:
        worker = None
        try:
            if group.num_rows == 0:
                return pa.Table.from_pydict({
                    "msgs_blob": pa.array([], type=pa.binary()),
                    "n_updated": pa.array([], type=pa.int64()),
                    "row_ids_blob": pa.array([], type=pa.binary()),
                    "error": pa.array([], type=pa.string()),
                })

            if (pc.count_distinct(group.column(row_id_name)).as_py()
                    != group.num_rows):
                raise ValueError(
                    "MERGE matched multiple source rows to the same "
                    "target _ROW_ID. Deduplicate the source before merging.")

            routing_columns = [frid_col]
            if row_id_range_starts is not None:
                routing_columns.append(range_col)
            for_update = group.drop_columns(routing_columns)
            row_ids = (
                for_update.column(row_id_name).to_pylist()
                if collect_row_ids else []
            )
            worker = TableUpdateByRowId(
                captured_table,
                "_merge_into_shard_" + uuid.uuid4().hex[:8],
                BATCH_COMMIT_IDENTIFIER,
                _precomputed_files_info=ray.get(precomputed_info_ref),
            )
            if capture_group_errors:
                return _write_group_result(
                    captured_table, worker, for_update, captured_cols, row_ids)
            messages = worker.update_columns(for_update, captured_cols)
            return _group_result(messages, for_update.num_rows, row_ids)
        except Exception as error:
            if capture_group_errors:
                return _failed_group_result(captured_table, worker, error)
            raise

    group_col = frid_col
    num_groups = len(captured_sorted)
    if row_id_range_starts is not None:
        group_col = range_col
        num_groups = len(set(row_id_range_starts.tolist()))
    group_partitions = max(1, min(num_groups, num_partitions))
    grouped = with_frid.groupby(
        group_col, num_partitions=group_partitions)
    if transform is None:
        msgs_ds = grouped.map_groups(_apply_group, **worker_map_kwargs)
    else:
        retry_options = ray_remote_args or {}
        worker_options = {
            "table": table,
            "read_table": scan_table,
            "files_info_ref": precomputed_info_ref,
            "read_projection": list(read_projection or []),
            "update_cols": list(captured_cols),
            "update_schema": transform_update_schema,
            "transform": transform,
            "transform_filter": transform_filter,
            "transform_predicate": transform_predicate,
            "transform_batch_size": transform_batch_size,
            "max_application_retries": retry_options.get("max_retries", 3),
            "retry_exceptions": retry_options.get(
                "retry_exceptions", False),
        }
        if _is_transform_factory(transform):
            actor_map_kwargs = dict(worker_map_kwargs)
            actor_map_kwargs.pop("retry_exceptions", None)
            actor_map_kwargs.pop("max_retries", None)
            msgs_ds = grouped.map_groups(
                _RangeTransformUpdateWorker,
                fn_constructor_kwargs=worker_options,
                **actor_map_kwargs,
            )
        else:
            msgs_ds = grouped.map_groups(
                _apply_range_transform,
                fn_kwargs=worker_options,
                **worker_map_kwargs,
            )

    all_msgs: list = []
    num_updated = 0
    action_row_ids = []
    group_error = None
    for batch in msgs_ds.iter_batches(batch_format="pyarrow"):
        for result in batch.to_pylist():
            error = result["error"]
            if error is not None:
                if group_error is None:
                    group_error = error
                continue
            group_msgs = pickle.loads(result["msgs_blob"])
            group_row_ids = (
                pickle.loads(result["row_ids_blob"])
                if collect_row_ids else []
            )
            n = result["n_updated"]
            if on_group_result is None:
                all_msgs.extend(group_msgs)
            else:
                on_group_result(group_msgs, n, group_row_ids)
            num_updated += n
            action_row_ids.extend(group_row_ids)
    if group_error is not None:
        if on_group_result is None:
            if all_msgs:
                try:
                    _abort_group_messages(captured_table, all_msgs)
                except Exception:
                    logger.warning(
                        "Failed to abort completed update groups.",
                        exc_info=True)
            raise ValueError(group_error)
        raise GroupApplyError(group_error)
    return all_msgs, num_updated, action_row_ids


def _abort_group_messages(table, messages):
    commit = table.new_batch_write_builder().new_commit()
    try:
        commit.abort(messages)
    finally:
        commit.close()


def _write_group_result(table, writer, updates, update_cols, row_ids=()):
    try:
        messages = writer.update_columns(updates, list(update_cols))
        return _group_result(messages, updates.num_rows, row_ids)
    except Exception as error:
        return _failed_group_result(table, writer, error)


def _failed_group_result(table, writer, error):
    _abort_failed_writer(table, writer)
    return _group_result([], 0, error=_group_error_text(error))


def _abort_failed_writer(table, writer):
    if writer is not None and writer.commit_messages:
        try:
            _abort_group_messages(table, writer.commit_messages)
        except Exception:
            logger.warning(
                "Failed to abort row-id range files.", exc_info=True)


def _row_id_range_starts(files_info, target_rows):
    """Pack adjacent file groups into continuous target-sized ranges."""
    import numpy as np

    starts = []
    current_start = None
    current_rows = 0
    for first_row_id in files_info.first_row_ids:
        if current_start is None:
            current_start = first_row_id
        starts.append(current_start)

        files = files_info.first_row_id_index[first_row_id][1]
        group_ends = [
            data_file.row_id_range().to
            for data_file in files
            if (data_file.first_row_id == first_row_id
                and data_file.row_id_range() is not None)
        ]
        if not group_ends:
            raise RuntimeError(
                "Cannot determine row count for file group {}.".format(
                    first_row_id))
        current_rows += max(group_ends) - first_row_id + 1
        if current_rows >= target_rows:
            current_start = None
            current_rows = 0
    return np.asarray(starts, dtype=np.int64)


def _read_output_schema(table, read_cols: Sequence[str]) -> "pa.Schema":
    """Result schema: each projected column's type plus int64 ``_ROW_ID``, in
    ``read_cols`` order. Shared by the empty-result paths so they can't drift."""
    from pypaimon.schema.data_types import PyarrowFieldParser
    from pypaimon.table.special_fields import SpecialFields

    rid = SpecialFields.ROW_ID.name
    full = PyarrowFieldParser.from_paimon_schema(table.table_schema.fields)
    # Keep each field's nullability so an empty result matches a non-empty read.
    return pa.schema([
        pa.field(rid, pa.int64(), nullable=False) if col == rid else full.field(col)
        for col in read_cols
    ])


def distributed_read_by_row_id(
    row_ids_ds,
    table,
    projection: Sequence[str],
    *,
    num_partitions: int,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    base_snapshot_id: Optional[int] = None,
):
    """Read ``projection`` for the ``_ROW_ID``s in ``row_ids_ds``, routing each to its
    owning file and reading only the matched rows via ``IndexedSplit`` slicing (blob
    resolved). Returns a ``ray.data.Dataset`` of ``(*projection, _ROW_ID)``, or ``None``
    if the target is empty. Read-side mirror of ``distributed_update_apply``.
    """
    import numpy as np
    import uuid

    import ray

    from pypaimon.common.options.core_options import CoreOptions
    from pypaimon.globalindex.indexed_split import IndexedSplit
    from pypaimon.read.split import DataSplit
    from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
    from pypaimon.table.special_fields import SpecialFields
    from pypaimon.utils.range import Range
    from pypaimon.write.table_update_by_row_id import TableUpdateByRowId

    row_id_name = SpecialFields.ROW_ID.name
    read_cols = list(projection)
    if row_id_name not in read_cols:
        read_cols.append(row_id_name)

    # Typed empty block so all output blocks share one schema.
    empty_out = _read_output_schema(table, read_cols).empty_table()

    # Read-only planner (only scans the manifest); pinned to the base snapshot for stable routing.
    scan_table = (
        table.copy({CoreOptions.SCAN_SNAPSHOT_ID.key(): str(base_snapshot_id)})
        if base_snapshot_id is not None else table
    )
    planner = TableUpdateByRowId(
        scan_table,
        "_read_by_row_id_planner_" + uuid.uuid4().hex[:8],
        BATCH_COMMIT_IDENTIFIER,
    )
    sorted_first_row_ids = list(planner.first_row_ids)
    if not sorted_first_row_ids:
        return None

    precomputed_info_ref = ray.put(planner._snapshot_files_info())
    frid_col = "_FIRST_ROW_ID"
    sorted_arr = np.asarray(sorted_first_row_ids, dtype=np.int64)
    valid_ranges = planner.valid_row_id_ranges
    range_starts = np.asarray([r.from_ for r in valid_ranges], dtype=np.int64)
    range_ends = np.asarray([r.to for r in valid_ranges], dtype=np.int64)

    def _assign_frid(batch: pa.Table) -> pa.Table:
        if batch.num_rows == 0:
            return batch.append_column(frid_col, pa.array([], type=pa.int64()))
        rid_col = batch.column(row_id_name)
        if rid_col.null_count:
            raise ValueError(
                "_ROW_ID is null; the planner snapshot is stale or the row ids "
                "come from a different table."
            )
        rids = rid_col.to_numpy(zero_copy_only=False)
        # Foreign-id check: valid_ranges are sorted+merged, so one searchsorted finds
        # the candidate range (O(rows log ranges), like distributed_delete_apply).
        ridx = np.searchsorted(range_starts, rids, side="right") - 1
        safe = np.clip(ridx, 0, len(range_starts) - 1)
        in_range = (
            (ridx >= 0)
            & (rids >= range_starts[safe])
            & (rids <= range_ends[safe])
        )
        if not in_range.all():
            bad = rids[~in_range][0]
            raise ValueError(
                f"_ROW_ID {bad} does not belong to any valid range "
                f"{[f'[{r.from_}, {r.to}]' for r in valid_ranges]}; the planner "
                f"snapshot is stale or the row ids come from a different table."
            )
        idx = np.searchsorted(sorted_arr, rids, side="right") - 1
        return batch.append_column(
            frid_col, pa.array(sorted_arr[idx], type=pa.int64())
        )

    captured_table = scan_table  # read at the same pinned snapshot the planner routed on
    captured_read_cols = read_cols
    captured_empty = empty_out

    def _read_group(group: pa.Table) -> pa.Table:
        if group.num_rows == 0:
            return captured_empty
        frid = int(group.column(frid_col)[0].as_py())
        info = ray.get(precomputed_info_ref)
        owning_split, target_files = info.first_row_id_index[frid]
        origin_split = DataSplit(
            files=target_files,
            partition=owning_split.partition,
            bucket=owning_split.bucket,
            raw_convertible=True,
        )
        # Only matched rows (deduped, contiguous ids -> ranges); blob gets row-index pushdown.
        wanted = set(group.column(row_id_name).to_pylist())
        indexed = IndexedSplit(origin_split, Range.to_ranges(list(wanted)))
        read = captured_table.new_read_builder().with_projection(
            captured_read_cols
        ).new_read()
        return read.to_arrow([indexed])

    map_kwargs = _map_kwargs(ray_remote_args)
    with_frid = row_ids_ds.map_batches(_assign_frid, **map_kwargs)
    group_partitions = max(1, min(len(sorted_first_row_ids), num_partitions))
    return with_frid.groupby(frid_col, num_partitions=group_partitions).map_groups(
        _read_group, **map_kwargs
    )


def distributed_delete_apply(
    delete_ds,
    table,
    *,
    num_partitions: int,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    base_snapshot_id: Optional[int] = None,
    collect_row_ids: bool = False,
) -> Tuple[list, int, list]:
    import base64
    import numpy as np
    import pickle

    import pyarrow.compute as pc
    import ray

    from pypaimon.common.options.core_options import CoreOptions
    from pypaimon.table.special_fields import SpecialFields
    from pypaimon.write.table_delete import TableDeleteByRowId

    row_id_name = SpecialFields.ROW_ID.name
    scan_table = (
        table.copy({CoreOptions.SCAN_SNAPSHOT_ID.key(): str(base_snapshot_id)})
        if base_snapshot_id is not None else table
    )

    planner = TableDeleteByRowId(scan_table)
    anchor_info = planner._snapshot_anchor_ranges()
    if not anchor_info.anchors:
        return [], 0, []

    precomputed_info_ref = ray.put(anchor_info)

    starts = np.asarray(
        [a.row_range.from_ for a in anchor_info.anchors], dtype=np.int64
    )
    ends = np.asarray(
        [a.row_range.to for a in anchor_info.anchors], dtype=np.int64
    )

    def _group_key(anchor) -> str:
        partition_blob = base64.b64encode(
            pickle.dumps(tuple(anchor.partition.values))
        ).decode("ascii")
        return f"{anchor.bucket}:{partition_blob}"

    group_keys = [_group_key(a) for a in anchor_info.anchors]
    unique_group_count = len(set(group_keys))
    group_col = "_DELETE_GROUP_KEY"
    valid_ranges = [
        f"[{a.row_range.from_}, {a.row_range.to}]"
        for a in anchor_info.anchors
    ]

    def _assign_group(batch: pa.Table) -> pa.Table:
        if batch.num_rows == 0:
            return batch.append_column(
                group_col, pa.array([], type=pa.string())
            )
        rid_col = batch.column(row_id_name)
        if rid_col.null_count:
            raise ValueError(
                "_ROW_ID is null; planner snapshot is stale "
                "or matched rows come from a different table."
            )
        rids = rid_col.to_numpy(zero_copy_only=False)
        idx = np.searchsorted(starts, rids, side="right") - 1
        safe_idx = np.clip(idx, 0, len(starts) - 1)
        in_range = (
            (idx >= 0)
            & (idx < len(starts))
            & (rids >= starts[safe_idx])
            & (rids <= ends[safe_idx])
        )
        if not in_range.all():
            bad = rids[~in_range][0]
            raise ValueError(
                f"_ROW_ID {bad} does not belong to any valid range "
                f"{valid_ranges}; planner snapshot is stale or matched "
                f"rows come from a different table."
            )
        return batch.append_column(
            group_col,
            pa.array([group_keys[i] for i in safe_idx], type=pa.string()),
        )

    map_kwargs = _map_kwargs(ray_remote_args)
    with_group = delete_ds.map_batches(_assign_group, **map_kwargs)
    captured_table = scan_table

    def _apply_group(group: pa.Table) -> pa.Table:
        if group.num_rows == 0:
            return pa.Table.from_pydict({
                "msgs_blob": pa.array([], type=pa.binary()),
                "n_deleted": pa.array([], type=pa.int64()),
                "row_ids_blob": pa.array([], type=pa.binary()),
            })

        if (
            pc.count_distinct(group.column(row_id_name)).as_py()
            != group.num_rows
        ):
            raise ValueError(
                "MERGE matched multiple source rows to the same "
                "target _ROW_ID. Deduplicate the source before "
                "merging."
            )

        row_ids = group.column(row_id_name).to_pylist()
        worker = TableDeleteByRowId(
            captured_table,
            _precomputed_anchor_ranges=ray.get(precomputed_info_ref),
        )
        msgs = worker.delete(row_ids)
        return pa.Table.from_pydict({
            "msgs_blob": pa.array([pickle.dumps(msgs)], type=pa.binary()),
            "n_deleted": pa.array([len(row_ids)], type=pa.int64()),
            "row_ids_blob": pa.array(
                [pickle.dumps(row_ids if collect_row_ids else [])],
                type=pa.binary(),
            ),
        })

    group_partitions = max(1, min(unique_group_count, num_partitions))
    msgs_ds = with_group.groupby(
        group_col, num_partitions=group_partitions
    ).map_groups(_apply_group, **map_kwargs)

    all_msgs: list = []
    num_deleted = 0
    action_row_ids = []
    for batch in msgs_ds.iter_batches(batch_format="pyarrow"):
        for blob in batch.column("msgs_blob").to_pylist():
            all_msgs.extend(pickle.loads(blob))
        for n in batch.column("n_deleted").to_pylist():
            num_deleted += n
        if collect_row_ids:
            for blob in batch.column("row_ids_blob").to_pylist():
                action_row_ids.extend(pickle.loads(blob))
    return all_msgs, num_deleted, action_row_ids


def build_not_matched_insert_ds(
    *,
    target_identifier: str,
    source_ds,
    target_on: Sequence[str],
    source_on: Sequence[str],
    clauses: List[_NormalizedClause],
    target_field_names: Sequence[str],
    target_pa_schema: pa.Schema,
    catalog_options: Dict[str, str],
    num_partitions: int,
    target_empty: bool = False,
    snapshot_id: Optional[int] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
):
    from pypaimon.ray.ray_paimon import read_paimon

    captured_field_names = list(target_field_names)
    out_schema = target_pa_schema

    source_cols = _resolve_source_projection(
        clauses, source_on, source_ds.schema().names,
    )
    source_ds = source_ds.select_columns(source_cols)
    source_renamed = source_ds.rename_columns(
        {c: f"s.{c}" for c in source_cols}
    )

    if target_empty:
        unmatched = source_renamed
    else:
        target_ds = read_paimon(
            target_identifier, catalog_options,
            projection=list(target_on), snapshot_id=snapshot_id,
        )
        target_renamed = target_ds.rename_columns(
            {c: f"t.{c}" for c in target_on}
        )
        unmatched = source_renamed.join(
            target_renamed,
            join_type="left_anti",
            num_partitions=num_partitions,
            on=tuple(f"s.{c}" for c in source_on),
            right_on=tuple(f"t.{c}" for c in target_on),
        )

    prepared_clauses = []
    for clause in clauses:
        rewritten = None
        if clause.condition is not None:
            from pypaimon.ray.merge_condition import rewrite_condition
            rewritten = rewrite_condition(clause.condition)
        prepared_clauses.append((clause.spec, rewritten))

    _filter_batch_nm = None
    if any(r is not None for _, r in prepared_clauses):
        from pypaimon.ray.merge_condition import filter_batch as _filter_batch_nm

    def _transform(batch: pa.Table) -> pa.Table:
        remaining = batch
        parts = []
        for spec, rewritten in prepared_clauses:
            if remaining.num_rows == 0:
                break
            if rewritten is not None:
                matched = _filter_batch_nm(
                    remaining, rewritten, _pre_rewritten=True,
                )
                if matched.num_rows > 0:
                    parts.append(vectorized_insert_transform(
                        matched, spec, captured_field_names, out_schema
                    ))
                if matched.num_rows < remaining.num_rows:
                    not_cond = f"COALESCE(NOT ({rewritten}), TRUE)"
                    remaining = _filter_batch_nm(
                        remaining, not_cond, _pre_rewritten=True,
                    )
                else:
                    remaining = remaining.slice(0, 0)
            else:
                parts.append(vectorized_insert_transform(
                    remaining, spec, captured_field_names, out_schema
                ))
                remaining = remaining.slice(0, 0)
        if not parts:
            return out_schema.empty_table()
        return cast_to_schema(pa.concat_tables(parts), out_schema)

    return unmatched.map_batches(
        _transform, **_map_kwargs(ray_remote_args)
    )


def distributed_write_collect_msgs(
    insert_ds,
    table,
    *,
    ray_remote_args: Optional[Dict[str, Any]],
    concurrency: Optional[int],
) -> list:
    from pypaimon.write.ray_datasink import PaimonDatasink

    class _CollectingDatasink(PaimonDatasink):
        def __init__(self, t):
            super().__init__(t, overwrite=False)
            self.collected: list = []

        def on_write_complete(self, write_result):
            self.collected = [
                m
                for batch in self._extract_write_returns(write_result)
                for m in batch
                if not m.is_empty()
            ]

    sink = _CollectingDatasink(table)
    write_kwargs: Dict[str, Any] = {}
    if ray_remote_args is not None:
        write_kwargs["ray_remote_args"] = ray_remote_args
    if concurrency is not None:
        write_kwargs["concurrency"] = concurrency
    insert_ds.write_datasink(sink, **write_kwargs)
    return sink.collected
