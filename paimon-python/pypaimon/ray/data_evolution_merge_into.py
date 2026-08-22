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

"""MERGE INTO ... USING ... for Paimon data-evolution tables via Ray Datasets."""

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import pyarrow as pa

from pypaimon.common.predicate import Predicate
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.ray.data_evolution_merge_join import (
    _resolve_source_projection,
    build_matched_delete_ds,
    build_matched_update_ds,
    build_not_matched_insert_ds,
    build_self_merge_delete_ds,
    _SelfMergeUpdatePlan,
    build_self_merge_update_plan,
    distributed_delete_apply,
    distributed_self_merge_update_apply,
    distributed_update_apply,
    distributed_write_collect_msgs,
)
from pypaimon.ray.data_evolution_merge_transform import (
    LiteralValue,
    OnSpec,
    SetSpec,
    SourceColumnRef,
    TargetColumnRef,
    WhenMatched,
    WhenNotMatched,
    _NormalizedClause,
)
from pypaimon.ray.partitioning import (
    _default_hash_shuffle_parallelism,
    _estimate_dataset_size_bytes,
    _resolve_num_partitions,
)

__all__ = ["merge_into", "WhenMatched", "WhenNotMatched"]

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _PrepareCtx:
    """Bag of values _prepare hands to _build_datasets."""
    target_on_cols: List[str]
    source_on_cols: List[str]
    settable_field_names: List[str]
    full_target_field_names: List[str]
    update_pa_schema: pa.Schema
    full_pa_schema: pa.Schema
    catalog_options: Dict[str, str]
    is_self_merge: bool = False
    self_merge_scan_predicate: Optional[Predicate] = None
    read_columns: Tuple[str, ...] = ()


def merge_into(
    target: str,
    source: Any,
    catalog_options: Dict[str, str],
    *,
    on: OnSpec,
    when_matched: Sequence[WhenMatched] = (),
    when_not_matched: Sequence[WhenNotMatched] = (),
    num_partitions: Optional[int] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    concurrency: Optional[int] = None,
    read_columns: Optional[Sequence[str]] = None,
) -> Dict[str, int]:
    _require_ray_join()

    table, source_ds, matched_specs, not_matched_specs, ctx = _prepare(
        target, source, catalog_options,
        list(when_matched), list(when_not_matched), on,
        read_columns,
    )
    base_snapshot = table.snapshot_manager().get_latest_snapshot()
    target_empty = _is_target_empty(base_snapshot)
    estimated_size_bytes = None
    if num_partitions is None:
        estimated_size_bytes = _estimate_merge_input_size_bytes(
            source_ds, ctx,
        )
    min_partitions = 1
    unknown_num_partitions = None
    if num_partitions is None and not ctx.is_self_merge:
        unknown_num_partitions = _default_hash_shuffle_parallelism()
        if not target_empty:
            min_partitions = unknown_num_partitions
    num_partitions = _resolve_num_partitions(
        num_partitions,
        estimated_size_bytes,
        min_partitions=min_partitions,
        unknown_num_partitions=unknown_num_partitions,
    )

    update_ds, delete_ds, insert_ds, update_cols_union = _build_datasets(
        table, target, source_ds, matched_specs, not_matched_specs,
        ctx, base_snapshot, num_partitions, ray_remote_args,
    )

    return _execute_and_commit(
        table, update_ds, delete_ds, insert_ds, update_cols_union,
        base_snapshot, num_partitions,
        ray_remote_args, concurrency,
    )


def _prepare(
    target, source, catalog_options, when_matched, when_not_matched, on,
    read_columns=None,
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
                    f"Only the last {label} clause may omit its condition. "
                    f"Clause at index {i} has no condition, making subsequent "
                    f"clauses unreachable."
                )
    target_on_cols, source_on_cols = _normalize_on(on)

    from pypaimon.catalog.catalog_factory import CatalogFactory

    catalog = CatalogFactory.create(catalog_options)
    table = catalog.get_table(target)
    if not table.options.data_evolution_enabled():
        raise ValueError(
            f"merge_into requires 'data-evolution.enabled' = 'true' on '{target}'."
        )
    if not table.options.row_tracking_enabled():
        raise ValueError(
            f"merge_into requires 'row-tracking.enabled' = 'true' on '{target}'."
        )
    if any(c.delete for c in when_matched) and not (
        table.options.deletion_vectors_enabled(False)
    ):
        raise ValueError(
            f"merge_into DELETE requires 'deletion-vectors.enabled' = "
            f"'true' on '{target}'."
        )

    full_target_field_names = list(table.field_names)
    settable_field_names = list(full_target_field_names)
    on_map = dict(zip(target_on_cols, source_on_cols))
    is_self_merge = _is_self_merge(
        target, source, target_on_cols, source_on_cols
    )
    matched_specs = []
    for c in when_matched:
        spec = {}
        if not c.delete:
            spec = _normalize_set_spec(
                c.update,
                settable_field_names,
                on_map,
                allow_callables=is_self_merge,
            )
        matched_specs.append(
            _NormalizedClause(
                spec=spec,
                condition=c.condition,
                delete=c.delete,
            )
        )
    if matched_specs and table.partition_keys:
        partition_set = set(table.partition_keys)
        for clause in matched_specs:
            modified_partition_cols = partition_set & set(clause.spec.keys())
            if modified_partition_cols:
                raise ValueError(
                    f"merge_into does not support updating partition columns "
                    f"{sorted(modified_partition_cols)}; cross-partition row "
                    f"movement is not implemented."
                )
    has_condition = any(
        c.condition is not None
        for c in list(when_matched) + list(when_not_matched)
    )
    if has_condition:
        from pypaimon.ray.merge_condition import (
            _load_datafusion, extract_target_columns,
        )
        _load_datafusion()
        for c in when_not_matched:
            if c.condition is not None:
                t_refs = extract_target_columns(c.condition)
                if t_refs:
                    raise ValueError(
                        f"WhenNotMatched condition must not reference "
                        f"target columns (t.*), but found: {sorted(t_refs)}"
                    )
    not_matched_specs = []
    for c in when_not_matched:
        spec = _normalize_set_spec(
            c.insert, settable_field_names, on_map,
            allow_target_refs=False,
        )
        for tk, sk in on_map.items():
            if tk in settable_field_names and tk not in spec:
                spec[tk] = SourceColumnRef(sk)
        not_matched_specs.append(
            _NormalizedClause(spec=spec, condition=c.condition)
        )

    if is_self_merge and not_matched_specs:
        raise ValueError(
            "Self-merge (source == target with ON _ROW_ID) does not "
            "support WHEN NOT MATCHED clauses."
        )

    read_columns = tuple(dict.fromkeys(read_columns or ()))
    has_callable = any(
        callable(value) and not isinstance(value, type)
        for clause in matched_specs
        for value in clause.spec.values()
    )
    if read_columns and not has_callable:
        raise ValueError("read_columns requires a callable SET value.")
    if has_callable:
        if not read_columns:
            raise ValueError("Callable SET values require read_columns.")
        for col in read_columns:
            if col not in full_target_field_names:
                raise ValueError(
                    f"Read column {col!r} is not in target '{target}'."
                )

    if is_self_merge:
        source_ds = None
        source_col_names = set(full_target_field_names) | set(source_on_cols)
    else:
        source_snapshot_id = None
        source_read_projection = None
        if isinstance(source, str):
            source_table = catalog.get_table(source)
            source_read_projection = _resolve_source_projection(
                matched_specs + not_matched_specs,
                source_on_cols,
                source_table.field_names,
            )
            source_snapshot = source_table.snapshot_manager().get_latest_snapshot()
            if source_snapshot is not None:
                source_snapshot_id = source_snapshot.id
        source_ds = _normalize_source(
            source, catalog_options, source_snapshot_id=source_snapshot_id,
            projection=source_read_projection,
        )
        _validate_source_on_cols(source_ds, source_on_cols)
        source_col_names = set(_source_schema_or_raise(source_ds).names)
    _validate_source_has_target_cols(
        source_col_names, matched_specs + not_matched_specs,
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
                            f"condition references unknown source "
                            f"column '{col}'"
                        )
                    if prefix == "t" and col not in target_names:
                        raise ValueError(
                            f"condition references unknown target "
                            f"column '{col}'"
                        )

    from pypaimon.schema.data_types import PyarrowFieldParser
    full_pa_schema = PyarrowFieldParser.from_paimon_schema(
        table.table_schema.fields
    )
    update_pa_schema = pa.schema(
        [full_pa_schema.field(c) for c in settable_field_names]
    )
    self_merge_scan_predicate = None
    if (is_self_merge and matched_specs
            and all(c.condition is not None for c in matched_specs)):
        from pypaimon.common.predicate_builder import PredicateBuilder
        from pypaimon.ray.merge_condition import (
            try_parse_self_merge_predicate,
        )
        predicates = [
            try_parse_self_merge_predicate(
                c.condition, table.table_schema.fields,
            )
            for c in matched_specs
        ]
        if all(predicate is not None for predicate in predicates):
            self_merge_scan_predicate = PredicateBuilder.or_predicates(
                predicates
            )
    ctx = _PrepareCtx(
        target_on_cols=target_on_cols,
        source_on_cols=source_on_cols,
        settable_field_names=settable_field_names,
        full_target_field_names=full_target_field_names,
        update_pa_schema=update_pa_schema,
        full_pa_schema=full_pa_schema,
        catalog_options=catalog_options,
        is_self_merge=is_self_merge,
        self_merge_scan_predicate=self_merge_scan_predicate,
        read_columns=read_columns,
    )
    return table, source_ds, matched_specs, not_matched_specs, ctx


def _is_self_merge(target, source, target_on_cols, source_on_cols) -> bool:
    from pypaimon.table.special_fields import SpecialFields
    row_id_name = SpecialFields.ROW_ID.name
    return (isinstance(source, str)
            and source == target
            and target_on_cols == [row_id_name]
            and source_on_cols == [row_id_name])


def _build_datasets(
    table, target, source_ds, matched_specs, not_matched_specs,
    ctx: "_PrepareCtx", base_snapshot, num_partitions, ray_remote_args,
):
    # Pin every target read to base_snapshot so all branches see the same
    # snapshot the caller observed; otherwise concurrent commits in between
    # would mix data from different snapshots.
    base_snapshot_id = base_snapshot.id if base_snapshot is not None else None

    update_ds = None
    delete_ds = None
    insert_ds = None
    update_cols_union: List[str] = []
    target_empty = _is_target_empty(base_snapshot)

    if ctx.is_self_merge:
        if matched_specs and not target_empty:
            update_cols_union = _union_update_cols(matched_specs)
            if update_cols_union:
                update_ds = build_self_merge_update_plan(
                    table=table,
                    clauses=matched_specs,
                    target_field_names=ctx.full_target_field_names,
                    target_pa_schema=ctx.update_pa_schema,
                    update_cols=update_cols_union,
                    resolve_target_projection=_resolve_target_projection,
                    snapshot_id=base_snapshot_id,
                    scan_predicate=ctx.self_merge_scan_predicate,
                    read_columns=ctx.read_columns,
                )
            if any(c.delete for c in matched_specs):
                delete_ds = build_self_merge_delete_ds(
                    target_identifier=target,
                    clauses=matched_specs,
                    target_field_names=ctx.full_target_field_names,
                    catalog_options=ctx.catalog_options,
                    resolve_target_projection=_resolve_target_projection,
                    snapshot_id=base_snapshot_id,
                    scan_predicate=ctx.self_merge_scan_predicate,
                    ray_remote_args=ray_remote_args,
                )
        return update_ds, delete_ds, insert_ds, update_cols_union

    # Mirror Spark: matched/not-matched run as two independent joins
    # (inner / left_anti). One unified left_outer join would force
    # joined.materialize() to feed both branches, which can OOM on large merges.
    if matched_specs and not target_empty:
        update_cols_union = _union_update_cols(matched_specs)
        if update_cols_union:
            update_ds = build_matched_update_ds(
                target_identifier=target,
                source_ds=source_ds,
                target_on=ctx.target_on_cols,
                source_on=ctx.source_on_cols,
                clauses=matched_specs,
                target_field_names=ctx.settable_field_names,
                target_pa_schema=ctx.update_pa_schema,
                update_cols=update_cols_union,
                catalog_options=ctx.catalog_options,
                num_partitions=num_partitions,
                resolve_target_projection=_resolve_target_projection,
                snapshot_id=base_snapshot_id,
                ray_remote_args=ray_remote_args,
            )
        if any(c.delete for c in matched_specs):
            delete_ds = build_matched_delete_ds(
                target_identifier=target,
                source_ds=source_ds,
                target_on=ctx.target_on_cols,
                source_on=ctx.source_on_cols,
                clauses=matched_specs,
                target_field_names=ctx.settable_field_names,
                catalog_options=ctx.catalog_options,
                num_partitions=num_partitions,
                resolve_target_projection=_resolve_target_projection,
                snapshot_id=base_snapshot_id,
                ray_remote_args=ray_remote_args,
            )

    if not_matched_specs:
        insert_ds = build_not_matched_insert_ds(
            target_identifier=target,
            source_ds=source_ds,
            target_on=ctx.target_on_cols,
            source_on=ctx.source_on_cols,
            clauses=not_matched_specs,
            target_field_names=ctx.full_target_field_names,
            target_pa_schema=ctx.full_pa_schema,
            catalog_options=ctx.catalog_options,
            num_partitions=num_partitions,
            snapshot_id=base_snapshot_id,
            target_empty=target_empty,
            ray_remote_args=ray_remote_args,
        )

    return update_ds, delete_ds, insert_ds, update_cols_union


def _execute_and_commit(
    table, update_ds, delete_ds, insert_ds, update_cols_union,
    base_snapshot, num_partitions,
    ray_remote_args, concurrency,
):
    collect_action_row_ids = update_ds is not None and delete_ds is not None
    commit_messages: list = []

    update_msgs: list = []
    num_updated = 0
    update_row_ids = []
    delete_msgs: list = []
    num_deleted = 0
    delete_row_ids = []
    num_inserted = 0

    try:
        if update_ds is not None:
            if isinstance(update_ds, _SelfMergeUpdatePlan):
                update_msgs, num_updated, update_row_ids = (
                    distributed_self_merge_update_apply(
                        update_ds,
                        num_partitions=num_partitions,
                        ray_remote_args=ray_remote_args,
                        collect_row_ids=collect_action_row_ids,
                    )
                )
            else:
                update_msgs, num_updated, update_row_ids = (
                    distributed_update_apply(
                        update_ds, table, update_cols_union,
                        num_partitions=num_partitions,
                        ray_remote_args=ray_remote_args,
                        base_snapshot_id=(
                            base_snapshot.id
                            if base_snapshot is not None else None
                        ),
                        collect_row_ids=collect_action_row_ids,
                    )
                )
            commit_messages.extend(update_msgs)

        if delete_ds is not None:
            delete_msgs, num_deleted, delete_row_ids = distributed_delete_apply(
                delete_ds, table,
                num_partitions=num_partitions,
                ray_remote_args=ray_remote_args,
                base_snapshot_id=(
                    base_snapshot.id
                    if base_snapshot is not None else None
                ),
                collect_row_ids=collect_action_row_ids,
            )
            commit_messages.extend(delete_msgs)

        if collect_action_row_ids:
            _validate_disjoint_action_row_ids(update_row_ids, delete_row_ids)

        if insert_ds is not None:
            insert_msgs = distributed_write_collect_msgs(
                insert_ds, table,
                ray_remote_args=ray_remote_args, concurrency=concurrency,
            )
            commit_messages.extend(insert_msgs)
            num_inserted = sum(
                f.row_count
                for m in insert_msgs
                for f in m.new_files
                if not DataFileMeta.is_blob_file(f.file_name)
            )

        all_msgs: list = list(commit_messages)
        if all_msgs:
            table_commit = None
            try:
                table_commit = table.new_batch_write_builder().new_commit()
                table_commit.commit(all_msgs)
            finally:
                if table_commit is not None:
                    try:
                        table_commit.close()
                    except Exception as close_error:
                        logger.warning(
                            "Failed to close merge_into commit: %s",
                            close_error,
                            exc_info=close_error,
                        )
    except Exception as e:
        _reraise_inner(e)

    # num_matched = rows that passed a matched condition and changed
    return {
        "num_matched": num_updated + num_deleted,
        "num_inserted": num_inserted,
        "num_unchanged": 0,
    }


def _normalize_on(on: OnSpec) -> Tuple[List[str], List[str]]:
    if isinstance(on, Mapping):
        target_cols = list(on.keys())
        source_cols = list(on.values())
    else:
        target_cols = list(on)
        source_cols = list(on)
    if not target_cols:
        raise ValueError("'on' must be non-empty.")
    return target_cols, source_cols


def _estimate_merge_input_size_bytes(
    source_ds,
    ctx: "_PrepareCtx",
) -> Optional[int]:
    if ctx.is_self_merge:
        return None
    return _estimate_dataset_size_bytes(source_ds)


def _is_target_empty(snapshot) -> bool:
    return snapshot is None or snapshot.total_record_count == 0


def _require_ray_join() -> None:
    import ray
    from packaging.version import parse

    if parse(ray.__version__) < parse("2.50.0"):
        raise RuntimeError(
            f"this Ray operation requires ray>=2.50; "
            f"installed ray is {ray.__version__}."
        )


def _reraise_inner(err: BaseException) -> None:
    """Unwrap Ray's RayTaskError so callers see the worker-side exception."""
    inner = err
    cause = getattr(err, "cause", None) or getattr(err, "__cause__", None)
    while cause is not None:
        inner = cause
        cause = getattr(inner, "cause", None) or getattr(inner, "__cause__", None)
    if inner is err:
        raise err
    raise inner from err


def _validate_disjoint_action_row_ids(update_row_ids, delete_row_ids) -> None:
    seen = set()
    for row_id in list(update_row_ids) + list(delete_row_ids):
        if row_id in seen:
            raise ValueError(
                "MERGE matched multiple source rows to the same target "
                "_ROW_ID. Deduplicate the source before merging."
            )
        seen.add(row_id)


def _union_update_cols(clauses: List[_NormalizedClause]) -> List[str]:
    seen: List[str] = []
    seen_set: set = set()
    for clause in clauses:
        for col in clause.spec.keys():
            if col not in seen_set:
                seen.append(col)
                seen_set.add(col)
    return seen


def _needed_target_cols(
    clauses: List[_NormalizedClause],
    on: Sequence[str],
    update_cols: Sequence[str],
    all_target_cols: Sequence[str],
) -> list:
    # Target needs only: join keys, t.col refs, and cols that may fall back
    # (not set by every clause). Cols all clauses set from source aren't read.
    needed = set(on)
    set_by_all = set(update_cols)
    for clause in clauses:
        for value in clause.spec.values():
            if isinstance(value, TargetColumnRef):
                needed.add(value.column)
        set_by_all &= set(clause.spec.keys())
    needed |= set(update_cols) - set_by_all
    return [c for c in all_target_cols if c in needed]


def _resolve_target_projection(
    clauses: List[_NormalizedClause],
    target_on: Sequence[str],
    update_cols: Sequence[str],
    target_field_names: Sequence[str],
) -> list:
    needed = set(_needed_target_cols(
        clauses, target_on, update_cols, target_field_names,
    ))
    if any(c.condition is not None for c in clauses):
        from pypaimon.ray.merge_condition import extract_target_columns
        target_set = set(target_field_names)
        for clause in clauses:
            if clause.condition is not None:
                needed |= extract_target_columns(clause.condition) & target_set
    return [c for c in target_field_names if c in needed]


def _normalize_set_spec(
    spec: SetSpec,
    target_field_names: Sequence[str],
    on_map: Optional[Mapping[str, str]] = None,
    allow_target_refs: bool = True,
    allow_callables: bool = False,
) -> Dict[str, Any]:
    on_map = on_map or {}
    if spec == "*":
        return {
            col: SourceColumnRef(on_map.get(col, col))
            for col in target_field_names
        }
    if not isinstance(spec, Mapping):
        raise TypeError(
            f"SET spec must be '*' or a mapping, got {type(spec).__name__}"
        )
    if not spec:
        raise ValueError("SET spec must not be empty")
    target_set = set(target_field_names)
    for key in spec:
        if key not in target_set:
            raise ValueError(
                f"SET spec references unknown target column '{key}'"
            )
    result: Dict[str, Any] = {}
    for key, val in spec.items():
        if callable(val) and not isinstance(val, type):
            if allow_callables:
                result[key] = val
                continue
            raise TypeError(
                "SET values must be source_col(), target_col(), "
                "lit(), or literals; callables are only supported "
                "for self-merge"
            )
        if isinstance(val, SourceColumnRef):
            result[key] = val
        elif isinstance(val, TargetColumnRef):
            if not allow_target_refs:
                raise ValueError(
                    "INSERT spec must not reference target columns "
                    f"(t.*), but found: 't.{val.column}'"
                )
            if val.column not in target_set:
                raise ValueError(
                    f"SET spec references unknown target column "
                    f"'{val.column}'"
                )
            if val.column == key:
                continue
            result[key] = val
        elif isinstance(val, LiteralValue):
            result[key] = val
        elif isinstance(val, str) and val.startswith("s."):
            result[key] = SourceColumnRef(val[2:])
        elif isinstance(val, str) and val.startswith("t."):
            if not allow_target_refs:
                raise ValueError(
                    "INSERT spec must not reference target columns "
                    f"(t.*), but found: '{val}'"
                )
            ref = val[2:]
            if ref not in target_set:
                raise ValueError(
                    f"SET spec references unknown target column '{ref}'"
                )
            if ref == key:
                continue
            result[key] = TargetColumnRef(ref)
        else:
            result[key] = LiteralValue(val)
    return result


def _normalize_source(
    source: Any,
    catalog_options: Dict[str, str],
    source_snapshot_id: Optional[int] = None,
    projection: Optional[List[str]] = None,
):
    import ray.data

    if isinstance(source, ray.data.Dataset):
        return source
    if isinstance(source, str):
        from pypaimon.ray.ray_paimon import read_paimon
        read_kwargs = {}
        if source_snapshot_id is not None:
            read_kwargs["snapshot_id"] = source_snapshot_id
        if projection is not None:
            read_kwargs["projection"] = projection
        return read_paimon(source, catalog_options, **read_kwargs)
    if isinstance(source, pa.Table):
        return ray.data.from_arrow(source)
    try:
        import pandas as pd
    except ImportError:
        pd = None
    if pd is not None and isinstance(source, pd.DataFrame):
        return ray.data.from_pandas(source)
    raise TypeError(
        "source must be a ray.data.Dataset, a Paimon table identifier string, "
        f"a pyarrow.Table, or a pandas.DataFrame; got {type(source).__name__}."
    )


def _source_schema_or_raise(source_ds):
    """Get source schema; refuse to proceed if Ray can't tell us the columns."""
    schema = source_ds.schema()
    if schema is None:
        raise ValueError(
            "merge_into could not infer the source schema; pass a "
            "ray.data.Dataset that has been materialized (e.g. via "
            ".materialize()) or constructed from pyarrow/pandas."
        )
    return schema


def _validate_source_on_cols(source_ds, on: Sequence[str]) -> None:
    names = set(_source_schema_or_raise(source_ds).names)
    missing = [c for c in on if c not in names]
    if missing:
        raise ValueError(
            f"'on' columns {missing} missing from source schema {list(names)}."
        )


def _validate_source_has_target_cols(
    source_col_names: set,
    specs: List[_NormalizedClause],
) -> None:
    needed = set()
    for clause in specs:
        for val in clause.spec.values():
            if isinstance(val, SourceColumnRef):
                needed.add(val.column)
    missing = sorted(needed - source_col_names)
    if missing:
        raise ValueError(
            f"source is missing columns {missing} referenced by SET spec"
        )
