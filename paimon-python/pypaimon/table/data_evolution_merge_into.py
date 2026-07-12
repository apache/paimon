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

"""Single-process MERGE INTO for Paimon data-evolution tables."""

from dataclasses import dataclass
from typing import Any, List, Optional, Sequence

import pyarrow as pa
import pyarrow.compute as pc

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.ray.data_evolution_merge_into import (
    _normalize_on,
    _normalize_set_spec,
    _resolve_target_projection,
    _union_update_cols,
    _validate_source_has_target_cols,
)
from pypaimon.ray.data_evolution_merge_join import (
    _build_matched_delete_transform,
    _build_matched_transform,
)
from pypaimon.ray.data_evolution_merge_transform import (
    OnSpec,
    SourceColumnRef,
    WhenMatched,
    WhenNotMatched,
    _NormalizedClause,
    build_delete_schema,
    build_update_schema,
    cast_to_schema,
    lit,
    source_col,
    target_col,
    vectorized_insert_transform,
)
from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
from pypaimon.table.special_fields import SpecialFields
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.table_write import BatchTableWrite, StreamTableWrite
from pypaimon.write.table_delete import TableDeleteByRowId
from pypaimon.write.table_update_by_row_id import TableUpdateByRowId

__all__ = [
    "merge_into",
    "WhenMatched",
    "WhenNotMatched",
    "source_col",
    "target_col",
    "lit",
]


@dataclass(frozen=True)
class _PrepareCtx:
    target_on_cols: List[str]
    source_on_cols: List[str]
    settable_field_names: List[str]
    full_target_field_names: List[str]
    update_pa_schema: pa.Schema
    full_pa_schema: pa.Schema
    is_self_merge: bool = False


def merge_into(
    target_table,
    source: Any,
    *,
    on: OnSpec,
    when_matched: Sequence[WhenMatched] = (),
    when_not_matched: Sequence[WhenNotMatched] = (),
    commit_user: str,
    commit_identifier: int = BATCH_COMMIT_IDENTIFIER,
) -> List[CommitMessage]:
    """Prepare MERGE INTO commit messages in the current Python process.

    The returned messages must be committed by the caller's matching
    ``TableCommit``.
    """
    base_snapshot = target_table.snapshot_manager().get_latest_snapshot()
    source_table, matched_specs, not_matched_specs, ctx = _prepare(
        target_table,
        source,
        list(when_matched),
        list(when_not_matched),
        on,
    )

    update_table, delete_table, insert_table, update_cols_union = _build_tables(
        target_table,
        source_table,
        matched_specs,
        not_matched_specs,
        ctx,
        base_snapshot,
    )

    return _prepare_commit_messages(
        target_table,
        update_table,
        delete_table,
        insert_table,
        update_cols_union,
        base_snapshot,
        commit_user,
        commit_identifier,
    )


def _prepare(
    target_table,
    source,
    when_matched,
    when_not_matched,
    on,
):
    if not when_matched and not when_not_matched:
        raise ValueError(
            "At least one of when_matched or when_not_matched must be non-empty."
        )
    for label, clauses in [("when_matched", when_matched),
                           ("when_not_matched", when_not_matched)]:
        for i, clause in enumerate(clauses[:-1]):
            if clause.condition is None:
                raise ValueError(
                    "Only the last {} clause may omit its condition. "
                    "Clause at index {} has no condition, making subsequent "
                    "clauses unreachable.".format(label, i)
                )

    target_on_cols, source_on_cols = _normalize_on(on)
    if not target_table.options.data_evolution_enabled():
        raise ValueError(
            "merge_into requires 'data-evolution.enabled' = 'true' on target table."
        )
    if not target_table.options.row_tracking_enabled():
        raise ValueError(
            "merge_into requires 'row-tracking.enabled' = 'true' on target table."
        )
    if any(c.delete for c in when_matched) and not (
        target_table.options.deletion_vectors_enabled(False)
    ):
        raise ValueError(
            "merge_into DELETE requires 'deletion-vectors.enabled' = "
            "'true' on target table."
        )

    full_target_field_names = list(target_table.field_names)
    settable_field_names = list(full_target_field_names)
    on_map = dict(zip(target_on_cols, source_on_cols))

    matched_specs = []
    for c in when_matched:
        spec = {}
        if not c.delete:
            spec = _normalize_set_spec(
                c.update,
                settable_field_names,
                on_map,
            )
        matched_specs.append(
            _NormalizedClause(
                spec=spec,
                condition=c.condition,
                delete=c.delete,
            )
        )
    if matched_specs and target_table.partition_keys:
        partition_set = set(target_table.partition_keys)
        for clause in matched_specs:
            modified_partition_cols = partition_set & set(clause.spec.keys())
            if modified_partition_cols:
                raise ValueError(
                    "merge_into does not support updating partition columns "
                    "{}; cross-partition row movement is not implemented."
                    .format(sorted(modified_partition_cols))
                )

    has_condition = any(
        c.condition is not None
        for c in list(when_matched) + list(when_not_matched)
    )
    if has_condition:
        from pypaimon.ray.merge_condition import (
            _load_datafusion,
            extract_target_columns,
        )

        _load_datafusion()
        for c in when_not_matched:
            if c.condition is not None:
                t_refs = extract_target_columns(c.condition)
                if t_refs:
                    raise ValueError(
                        "WhenNotMatched condition must not reference "
                        "target columns (t.*), but found: {}".format(
                            sorted(t_refs)
                        )
                    )

    not_matched_specs = []
    for c in when_not_matched:
        spec = _normalize_set_spec(
            c.insert,
            settable_field_names,
            on_map,
            allow_target_refs=False,
        )
        for tk, sk in on_map.items():
            if tk in settable_field_names and tk not in spec:
                spec[tk] = SourceColumnRef(sk)
        not_matched_specs.append(
            _NormalizedClause(spec=spec, condition=c.condition)
        )

    is_self_merge = _is_self_merge(
        target_table, source, target_on_cols, source_on_cols
    )
    if is_self_merge and not_matched_specs:
        raise ValueError(
            "Self-merge (source == target with ON _ROW_ID) does not "
            "support WHEN NOT MATCHED clauses."
        )

    if is_self_merge:
        source_table = None
        source_col_names = set(full_target_field_names) | set(source_on_cols)
    else:
        source_table = _normalize_source(source)
        _validate_source_on_cols(source_table, source_on_cols)
        source_col_names = set(source_table.schema.names)

    _validate_source_has_target_cols(
        source_col_names, matched_specs + not_matched_specs
    )

    if has_condition:
        from pypaimon.ray.merge_condition import extract_columns

        target_names = set(full_target_field_names)
        if is_self_merge:
            target_names |= set(target_on_cols)
        for c in list(when_matched) + list(when_not_matched):
            if c.condition is not None:
                for ref in extract_columns(c.condition):
                    prefix, col = ref.split(".", 1)
                    if prefix == "s" and col not in source_col_names:
                        raise ValueError(
                            "condition references unknown source column '{}'"
                            .format(col)
                        )
                    if prefix == "t" and col not in target_names:
                        raise ValueError(
                            "condition references unknown target column '{}'"
                            .format(col)
                        )

    from pypaimon.schema.data_types import PyarrowFieldParser

    full_pa_schema = PyarrowFieldParser.from_paimon_schema(
        target_table.table_schema.fields
    )
    update_pa_schema = pa.schema(
        [full_pa_schema.field(c) for c in settable_field_names]
    )
    ctx = _PrepareCtx(
        target_on_cols=target_on_cols,
        source_on_cols=source_on_cols,
        settable_field_names=settable_field_names,
        full_target_field_names=full_target_field_names,
        update_pa_schema=update_pa_schema,
        full_pa_schema=full_pa_schema,
        is_self_merge=is_self_merge,
    )
    return source_table, matched_specs, not_matched_specs, ctx


def _build_tables(
    target_table,
    source_table: Optional[pa.Table],
    matched_specs: List[_NormalizedClause],
    not_matched_specs: List[_NormalizedClause],
    ctx: _PrepareCtx,
    base_snapshot,
):
    base_snapshot_id = base_snapshot.id if base_snapshot is not None else None
    update_table = None
    delete_table = None
    insert_table = None
    update_cols_union: List[str] = []

    if ctx.is_self_merge:
        if matched_specs and base_snapshot is not None:
            update_cols_union = _union_update_cols(matched_specs)
            if update_cols_union:
                update_table = _build_self_merge_update_table(
                    target_table,
                    matched_specs,
                    ctx,
                    update_cols_union,
                    base_snapshot_id,
                )
            if any(c.delete for c in matched_specs):
                delete_table = _build_self_merge_delete_table(
                    target_table,
                    matched_specs,
                    ctx,
                    base_snapshot_id,
                )
        return update_table, delete_table, insert_table, update_cols_union

    if matched_specs and base_snapshot is not None:
        update_cols_union = _union_update_cols(matched_specs)
        if update_cols_union:
            update_table = _build_matched_update_table(
                target_table,
                source_table,
                matched_specs,
                ctx,
                update_cols_union,
                base_snapshot_id,
            )
        if any(c.delete for c in matched_specs):
            delete_table = _build_matched_delete_table(
                target_table,
                source_table,
                matched_specs,
                ctx,
                base_snapshot_id,
            )

    if not_matched_specs:
        insert_table = _build_not_matched_insert_table(
            target_table,
            source_table,
            not_matched_specs,
            ctx,
            base_snapshot_id,
            target_empty=base_snapshot is None,
        )

    return update_table, delete_table, insert_table, update_cols_union


def _build_self_merge_update_table(
    target_table,
    clauses: List[_NormalizedClause],
    ctx: _PrepareCtx,
    update_cols: Sequence[str],
    snapshot_id: Optional[int],
) -> pa.Table:
    row_id_name = SpecialFields.ROW_ID.name
    needed_cols = set(
        _resolve_target_projection(
            clauses,
            [row_id_name],
            update_cols,
            ctx.full_target_field_names,
        )
    )
    for clause in clauses:
        for value in clause.spec.values():
            if isinstance(value, SourceColumnRef):
                needed_cols.add(value.column)
    target_set = set(ctx.full_target_field_names)
    for clause in clauses:
        if clause.condition is not None:
            from pypaimon.ray.merge_condition import extract_columns

            for ref in extract_columns(clause.condition):
                prefix, col = ref.split(".", 1)
                if prefix == "s" and col in target_set:
                    needed_cols.add(col)
    projection = [row_id_name] + [
        c for c in ctx.full_target_field_names if c in needed_cols
    ]

    target = _read_table(target_table, projection=projection, snapshot_id=snapshot_id)
    update_schema = build_update_schema(
        ctx.update_pa_schema, update_cols, row_id_name
    )
    if target.num_rows == 0:
        return update_schema.empty_table()

    orig_names = list(target.schema.names)
    target_renamed = _rename_with_prefix(target, "t.")
    aliased = _add_self_merge_source_aliases(
        target_renamed, orig_names, row_id_name
    )
    transform = _build_matched_transform(
        clauses,
        on_map={row_id_name: row_id_name},
        on_pairs=[(row_id_name, row_id_name)],
        update_cols=list(update_cols),
        row_id_name=row_id_name,
        update_schema=update_schema,
    )
    return transform(aliased)


def _build_self_merge_delete_table(
    target_table,
    clauses: List[_NormalizedClause],
    ctx: _PrepareCtx,
    snapshot_id: Optional[int],
) -> pa.Table:
    row_id_name = SpecialFields.ROW_ID.name
    needed_cols = set(
        _resolve_target_projection(
            clauses,
            [row_id_name],
            [],
            ctx.full_target_field_names,
        )
    )
    target_set = set(ctx.full_target_field_names)
    for clause in clauses:
        if clause.condition is not None:
            from pypaimon.ray.merge_condition import extract_columns

            for ref in extract_columns(clause.condition):
                prefix, col = ref.split(".", 1)
                if prefix == "s" and col in target_set:
                    needed_cols.add(col)
    projection = [row_id_name] + [
        c for c in ctx.full_target_field_names if c in needed_cols
    ]

    target = _read_table(target_table, projection=projection, snapshot_id=snapshot_id)
    delete_schema = build_delete_schema(row_id_name)
    if target.num_rows == 0:
        return delete_schema.empty_table()

    orig_names = list(target.schema.names)
    target_renamed = _rename_with_prefix(target, "t.")
    aliased = _add_self_merge_source_aliases(
        target_renamed, orig_names, row_id_name
    )
    transform = _build_matched_delete_transform(
        clauses,
        on_map={row_id_name: row_id_name},
        row_id_name=row_id_name,
        delete_schema=delete_schema,
    )
    return transform(aliased)


def _build_matched_update_table(
    target_table,
    source_table: pa.Table,
    clauses: List[_NormalizedClause],
    ctx: _PrepareCtx,
    update_cols: Sequence[str],
    snapshot_id: Optional[int],
) -> pa.Table:
    row_id_name = SpecialFields.ROW_ID.name
    needed_cols = _resolve_target_projection(
        clauses,
        ctx.target_on_cols,
        update_cols,
        ctx.settable_field_names,
    )
    projection = [row_id_name] + [c for c in needed_cols if c != row_id_name]
    target = _read_table(target_table, projection=projection, snapshot_id=snapshot_id)
    update_schema = build_update_schema(
        ctx.update_pa_schema, update_cols, row_id_name
    )
    if target.num_rows == 0 or source_table.num_rows == 0:
        return update_schema.empty_table()

    target_renamed = _rename_with_prefix(target, "t.")
    source_renamed = _rename_with_prefix(source_table, "s.")
    joined = target_renamed.join(
        source_renamed,
        keys=["t.{}".format(c) for c in ctx.target_on_cols],
        right_keys=["s.{}".format(c) for c in ctx.source_on_cols],
        join_type="inner",
    )
    transform = _build_matched_transform(
        clauses,
        on_map=dict(zip(ctx.source_on_cols, ctx.target_on_cols)),
        on_pairs=list(zip(ctx.source_on_cols, ctx.target_on_cols)),
        update_cols=list(update_cols),
        row_id_name=row_id_name,
        update_schema=update_schema,
    )
    return transform(joined)


def _build_matched_delete_table(
    target_table,
    source_table: pa.Table,
    clauses: List[_NormalizedClause],
    ctx: _PrepareCtx,
    snapshot_id: Optional[int],
) -> pa.Table:
    row_id_name = SpecialFields.ROW_ID.name
    needed_cols = _resolve_target_projection(
        clauses,
        ctx.target_on_cols,
        [],
        ctx.settable_field_names,
    )
    projection = [row_id_name] + [c for c in needed_cols if c != row_id_name]
    target = _read_table(target_table, projection=projection, snapshot_id=snapshot_id)
    delete_schema = build_delete_schema(row_id_name)
    if target.num_rows == 0 or source_table.num_rows == 0:
        return delete_schema.empty_table()

    target_renamed = _rename_with_prefix(target, "t.")
    source_renamed = _rename_with_prefix(source_table, "s.")
    joined = target_renamed.join(
        source_renamed,
        keys=["t.{}".format(c) for c in ctx.target_on_cols],
        right_keys=["s.{}".format(c) for c in ctx.source_on_cols],
        join_type="inner",
    )
    transform = _build_matched_delete_transform(
        clauses,
        on_map=dict(zip(ctx.source_on_cols, ctx.target_on_cols)),
        row_id_name=row_id_name,
        delete_schema=delete_schema,
    )
    return transform(joined)


def _build_not_matched_insert_table(
    target_table,
    source_table: pa.Table,
    clauses: List[_NormalizedClause],
    ctx: _PrepareCtx,
    snapshot_id: Optional[int],
    target_empty: bool = False,
) -> pa.Table:
    source_renamed = _rename_with_prefix(source_table, "s.")
    if source_renamed.num_rows == 0:
        return ctx.full_pa_schema.empty_table()

    if target_empty:
        unmatched = source_renamed
    else:
        target = _read_table(
            target_table,
            projection=list(ctx.target_on_cols),
            snapshot_id=snapshot_id,
        )
        if target.num_rows == 0:
            unmatched = source_renamed
        else:
            target_renamed = _rename_with_prefix(target, "t.")
            unmatched = source_renamed.join(
                target_renamed,
                keys=["s.{}".format(c) for c in ctx.source_on_cols],
                right_keys=["t.{}".format(c) for c in ctx.target_on_cols],
                join_type="left anti",
            )

    transform = _build_insert_transform(
        clauses, ctx.full_target_field_names, ctx.full_pa_schema
    )
    return transform(unmatched)


def _prepare_commit_messages(
    table,
    update_table: Optional[pa.Table],
    delete_table: Optional[pa.Table],
    insert_table: Optional[pa.Table],
    update_cols_union: Sequence[str],
    base_snapshot,
    commit_user: str,
    commit_identifier: int,
) -> List[CommitMessage]:
    all_msgs: list = []
    _validate_unique_action_row_ids(update_table, delete_table)

    if update_table is not None and update_table.num_rows > 0:
        update_snapshot_table = _copy_at_snapshot(
            table, base_snapshot.id if base_snapshot is not None else None
        )
        updater = TableUpdateByRowId(
            update_snapshot_table,
            commit_user,
            commit_identifier,
        )
        update_msgs = updater.update_columns(
            update_table, list(update_cols_union)
        )
        all_msgs.extend(update_msgs)

    if delete_table is not None and delete_table.num_rows > 0:
        delete_snapshot_table = _copy_at_snapshot(
            table, base_snapshot.id if base_snapshot is not None else None
        )
        deleter = TableDeleteByRowId(delete_snapshot_table)
        row_id_name = SpecialFields.ROW_ID.name
        delete_msgs = deleter.delete(
            delete_table.column(row_id_name).to_pylist()
        )
        all_msgs.extend(delete_msgs)

    if insert_table is not None and insert_table.num_rows > 0:
        writer = _new_table_write(table, commit_user, commit_identifier)
        try:
            writer.write_arrow(insert_table)
            if commit_identifier == BATCH_COMMIT_IDENTIFIER:
                insert_msgs = writer.prepare_commit()
            else:
                insert_msgs = writer.prepare_commit(commit_identifier)
        finally:
            writer.close()
        all_msgs.extend(insert_msgs)

    return all_msgs


def _new_table_write(table, commit_user: str, commit_identifier: int):
    if commit_identifier == BATCH_COMMIT_IDENTIFIER:
        return BatchTableWrite(table, commit_user)
    return StreamTableWrite(table, commit_user)


def _build_insert_transform(
    clauses: List[_NormalizedClause],
    target_field_names: Sequence[str],
    target_pa_schema: pa.Schema,
):
    prepared_clauses = []
    for clause in clauses:
        rewritten = None
        if clause.condition is not None:
            from pypaimon.ray.merge_condition import rewrite_condition

            rewritten = rewrite_condition(clause.condition)
        prepared_clauses.append((clause.spec, rewritten))

    _filter_batch = None
    if any(r is not None for _, r in prepared_clauses):
        from pypaimon.ray.merge_condition import filter_batch as _filter_batch

    def _transform(batch: pa.Table) -> pa.Table:
        remaining = batch
        parts = []
        for spec, rewritten in prepared_clauses:
            if remaining.num_rows == 0:
                break
            if rewritten is not None:
                matched = _filter_batch(
                    remaining, rewritten, _pre_rewritten=True
                )
                if matched.num_rows > 0:
                    parts.append(
                        vectorized_insert_transform(
                            matched,
                            spec,
                            target_field_names,
                            target_pa_schema,
                        )
                    )
                if matched.num_rows < remaining.num_rows:
                    not_cond = "COALESCE(NOT ({}), TRUE)".format(rewritten)
                    remaining = _filter_batch(
                        remaining, not_cond, _pre_rewritten=True
                    )
                else:
                    remaining = remaining.slice(0, 0)
            else:
                parts.append(
                    vectorized_insert_transform(
                        remaining,
                        spec,
                        target_field_names,
                        target_pa_schema,
                    )
                )
                remaining = remaining.slice(0, 0)
        if not parts:
            return target_pa_schema.empty_table()
        return cast_to_schema(pa.concat_tables(parts), target_pa_schema)

    return _transform


def _normalize_source(source: Any) -> pa.Table:
    if isinstance(source, pa.Table):
        return source
    if _is_table_like(source):
        snapshot = source.snapshot_manager().get_latest_snapshot()
        snapshot_id = snapshot.id if snapshot is not None else None
        return _read_table(source, snapshot_id=snapshot_id)
    try:
        import pandas as pd
    except ImportError:
        pd = None
    if pd is not None and isinstance(source, pd.DataFrame):
        return pa.Table.from_pandas(source, preserve_index=False)
    raise TypeError(
        "source must be a pyarrow.Table, a pandas.DataFrame, or a "
        "Paimon table; got {}.".format(type(source).__name__)
    )


def _read_table(table, projection: Optional[List[str]] = None,
                snapshot_id: Optional[int] = None) -> pa.Table:
    read_table = _copy_at_snapshot(table, snapshot_id)
    read_builder = read_table.new_read_builder()
    if projection is not None:
        read_builder.with_projection(projection)
    scan = read_builder.new_scan()
    return read_builder.new_read().to_arrow(scan.plan().splits())


def _copy_at_snapshot(table, snapshot_id: Optional[int]):
    if snapshot_id is None:
        return table
    return table.copy({CoreOptions.SCAN_SNAPSHOT_ID.key(): str(snapshot_id)})


def _validate_source_on_cols(source_table: pa.Table, on: Sequence[str]) -> None:
    names = set(source_table.schema.names)
    missing = [c for c in on if c not in names]
    if missing:
        raise ValueError(
            "'on' columns {} missing from source schema {}.".format(
                missing, list(names)
            )
        )


def _validate_unique_action_row_ids(
    update_table: Optional[pa.Table],
    delete_table: Optional[pa.Table],
) -> None:
    chunks = []
    total_rows = 0
    row_id_name = SpecialFields.ROW_ID.name
    for table in (update_table, delete_table):
        if table is None or table.num_rows == 0:
            continue
        total_rows += table.num_rows
        chunks.extend(table.column(row_id_name).chunks)
    if not chunks:
        return
    row_ids = pa.chunked_array(chunks)
    if pc.count_distinct(row_ids).as_py() != total_rows:
        raise ValueError(
            "MERGE matched multiple source rows to the same target _ROW_ID. "
            "Deduplicate the source before merging."
        )


def _is_self_merge(target_table, source, target_on, source_on) -> bool:
    row_id_name = SpecialFields.ROW_ID.name
    return (
        _is_table_like(source)
        and _same_table(target_table, source)
        and target_on == [row_id_name]
        and source_on == [row_id_name]
    )


def _is_table_like(obj) -> bool:
    return hasattr(obj, "new_read_builder") and hasattr(obj, "snapshot_manager")


def _same_table(left, right) -> bool:
    if left is right:
        return True
    return (
        getattr(left, "table_path", None) == getattr(right, "table_path", None)
        and str(getattr(left, "identifier", "")) == str(
            getattr(right, "identifier", "")
        )
        and _current_branch(left) == _current_branch(right)
    )


def _current_branch(table):
    current_branch = getattr(table, "current_branch", None)
    return current_branch() if current_branch is not None else None


def _rename_with_prefix(table: pa.Table, prefix: str) -> pa.Table:
    return table.rename_columns(
        ["{}{}".format(prefix, name) for name in table.schema.names]
    )


def _add_self_merge_source_aliases(
    target_renamed: pa.Table, orig_names: Sequence[str], row_id_name: str
) -> pa.Table:
    columns = list(target_renamed.columns)
    names = list(target_renamed.schema.names)
    for orig in orig_names:
        if orig == row_id_name:
            continue
        target_name = "t.{}".format(orig)
        if target_name in names:
            idx = names.index(target_name)
            columns.append(columns[idx])
            names.append("s.{}".format(orig))
    return pa.table(columns, names=names)
