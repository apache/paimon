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
    _NormalizedClause,
    build_update_schema,
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

    # MVP supports a single matched clause; future fan-out (conditions, multi-
    # clause fall-through) must thread every clause's spec through the
    # transform — guard so silent first-only behaviour can't sneak in.
    assert len(clauses) == 1, (
        f"build_matched_update_ds expected 1 clause, got {len(clauses)}"
    )
    spec = clauses[0].spec
    condition = clauses[0].condition
    captured_update_cols = list(update_cols)
    captured_row_id_name = row_id_name
    captured_on_pairs = list(zip(source_on, target_on))
    captured_schema = update_schema

    captured_apply = None
    captured_rewritten = None
    if condition is not None:
        from pypaimon.ray.merge_condition import (
            apply_condition, remap_source_on_keys, rewrite_condition,
        )
        on_map = dict(zip(source_on, target_on))
        captured_rewritten = remap_source_on_keys(
            rewrite_condition(condition), on_map,
        )
        captured_apply = apply_condition

    def _transform(batch: pa.Table) -> pa.Table:
        if captured_apply is not None:
            batch = captured_apply(
                batch, captured_rewritten, captured_schema,
            )
            if batch.num_rows == 0:
                return batch
        return vectorized_matched_transform(
            batch, spec, captured_on_pairs,
            captured_update_cols, captured_row_id_name,
            captured_schema,
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
) -> Tuple[list, int]:
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
        return [], 0

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
    for batch in msgs_ds.iter_batches(batch_format="pyarrow"):
        for blob in batch.column("msgs_blob").to_pylist():
            all_msgs.extend(pickle.loads(blob))
        for n in batch.column("n_updated").to_pylist():
            num_updated += n
    return all_msgs, num_updated


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
    from pypaimon.ray.shuffle import _coerce_large_string_types

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

    # MVP supports a single not-matched clause; see build_matched_update_ds
    # for why we assert instead of silently dropping the rest.
    assert len(clauses) == 1, (
        f"build_not_matched_insert_ds expected 1 clause, got {len(clauses)}"
    )
    spec = clauses[0].spec
    condition = clauses[0].condition
    captured_apply = None
    captured_rewritten = None
    if condition is not None:
        from pypaimon.ray.merge_condition import apply_condition, rewrite_condition
        captured_rewritten = rewrite_condition(condition)
        captured_apply = apply_condition

    def _transform(batch: pa.Table) -> pa.Table:
        if captured_apply is not None:
            batch = captured_apply(
                batch, captured_rewritten, out_schema,
            )
            if batch.num_rows == 0:
                return _coerce_large_string_types(batch)
        return _coerce_large_string_types(
            vectorized_insert_transform(
                batch, spec, captured_field_names, out_schema
            )
        )

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
