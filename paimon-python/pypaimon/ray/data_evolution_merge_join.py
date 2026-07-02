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

from typing import Any, Dict, List, Optional, Sequence, Tuple

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

    planner = TableUpdateByRowId(
        table,
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
    captured_sorted = sorted_first_row_ids
    captured_sorted_arr = np.asarray(captured_sorted, dtype=np.int64)
    valid_ranges = planner.valid_row_id_ranges
    range_starts = np.asarray([r.from_ for r in valid_ranges], dtype=np.int64)
    range_ends = np.asarray([r.to for r in valid_ranges], dtype=np.int64)

    def _assign_frid(batch: pa.Table) -> pa.Table:
        if batch.num_rows == 0:
            return batch.append_column(
                frid_col, pa.array([], type=pa.int64())
            )
        rid_col = batch.column(row_id_name)
        if rid_col.null_count:
            raise ValueError(
                "_ROW_ID is null; planner snapshot is stale "
                "or matched rows come from a different table."
            )
        rids = rid_col.to_numpy(zero_copy_only=False)
        # Check each row_id belongs to a valid range (vectorized).
        in_range = np.zeros(len(rids), dtype=bool)
        for s, e in zip(range_starts, range_ends):
            in_range |= (rids >= s) & (rids <= e)
        if not in_range.all():
            bad = rids[~in_range][0]
            raise ValueError(
                f"_ROW_ID {bad} does not belong to any valid range "
                f"{[f'[{r.from_}, {r.to}]' for r in valid_ranges]}; "
                f"planner snapshot is stale or matched rows come "
                f"from a different table."
            )
        idx = np.searchsorted(
            captured_sorted_arr, rids, side="right"
        ) - 1
        frids = captured_sorted_arr[idx]
        return batch.append_column(
            frid_col, pa.array(frids, type=pa.int64())
        )

    map_kwargs = _map_kwargs(ray_remote_args)
    with_frid = update_ds.map_batches(_assign_frid, **map_kwargs)

    captured_table = table
    captured_cols = cols

    def _apply_group(group: pa.Table) -> pa.Table:
        if group.num_rows == 0:
            return pa.Table.from_pydict({
                "msgs_blob": pa.array([], type=pa.binary()),
                "n_updated": pa.array([], type=pa.int64()),
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

        for_update = group.drop_columns([frid_col])
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
        msgs = worker.update_columns(for_update, list(captured_cols))
        return pa.Table.from_pydict({
            "msgs_blob": [pickle.dumps(msgs)],
            "n_updated": pa.array(
                [for_update.num_rows], type=pa.int64()
            ),
            "row_ids_blob": pa.array(
                [pickle.dumps(row_ids)], type=pa.binary()
            ),
        })

    # One group per target data file; bounded by file count and num_partitions.
    group_partitions = max(
        1, min(len(captured_sorted), num_partitions)
    )
    msgs_ds = with_frid.groupby(
        frid_col, num_partitions=group_partitions
    ).map_groups(_apply_group, **map_kwargs)

    all_msgs: list = []
    num_updated = 0
    action_row_ids = []
    for batch in msgs_ds.iter_batches(batch_format="pyarrow"):
        for blob in batch.column("msgs_blob").to_pylist():
            all_msgs.extend(pickle.loads(blob))
        for n in batch.column("n_updated").to_pylist():
            num_updated += n
        if collect_row_ids:
            for blob in batch.column("row_ids_blob").to_pylist():
                action_row_ids.extend(pickle.loads(blob))
    return all_msgs, num_updated, action_row_ids


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

    source_cols = list(source_ds.schema().names)
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
