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

from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import pyarrow as pa

from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.ray.data_evolution_merge_join import (
    build_matched_delete_ds,
    build_matched_update_ds,
    build_not_matched_insert_ds,
    build_self_merge_delete_ds,
    build_self_merge_update_ds,
    distributed_delete_apply,
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

__all__ = ["merge_into", "WhenMatched", "WhenNotMatched"]


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
) -> Dict[str, int]:
    _require_ray_join()
    num_partitions = _resolve_num_partitions(num_partitions)

    table, source_ds, matched_specs, not_matched_specs, ctx = _prepare(
        target, source, catalog_options,
        list(when_matched), list(when_not_matched), on,
    )
    base_snapshot = table.snapshot_manager().get_latest_snapshot()

    update_ds, delete_ds, insert_ds, update_cols_union = _build_datasets(
        target, source_ds, matched_specs, not_matched_specs,
        ctx, base_snapshot, num_partitions, ray_remote_args,
    )

    return _execute_and_commit(
        table, update_ds, delete_ds, insert_ds, update_cols_union,
        base_snapshot, num_partitions,
        ray_remote_args, concurrency,
    )


def _prepare(target, source, catalog_options, when_matched, when_not_matched, on):
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
    matched_specs = []
    for c in when_matched:
        spec = {}
        if not c.delete:
            spec = _normalize_set_spec(
                c.update, settable_field_names, on_map,
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

    is_self_merge = _is_self_merge(target, source, target_on_cols, source_on_cols)
    if is_self_merge and not_matched_specs:
        raise ValueError(
            "Self-merge (source == target with ON _ROW_ID) does not "
            "support WHEN NOT MATCHED clauses."
        )

    if is_self_merge:
        source_ds = None
        source_col_names = set(full_target_field_names) | set(source_on_cols)
    else:
        source_snapshot_id = None
        if isinstance(source, str):
            source_snapshot = (
                catalog.get_table(source)
                .snapshot_manager()
                .get_latest_snapshot()
            )
            if source_snapshot is not None:
                source_snapshot_id = source_snapshot.id
        source_ds = _normalize_source(
            source, catalog_options, source_snapshot_id=source_snapshot_id,
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
    ctx = _PrepareCtx(
        target_on_cols=target_on_cols,
        source_on_cols=source_on_cols,
        settable_field_names=settable_field_names,
        full_target_field_names=full_target_field_names,
        update_pa_schema=update_pa_schema,
        full_pa_schema=full_pa_schema,
        catalog_options=catalog_options,
        is_self_merge=is_self_merge,
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
    target, source_ds, matched_specs, not_matched_specs,
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

    if ctx.is_self_merge:
        if matched_specs and base_snapshot is not None:
            update_cols_union = _union_update_cols(matched_specs)
            if update_cols_union:
                update_ds = build_self_merge_update_ds(
                    target_identifier=target,
                    clauses=matched_specs,
                    target_field_names=ctx.full_target_field_names,
                    target_pa_schema=ctx.update_pa_schema,
                    update_cols=update_cols_union,
                    catalog_options=ctx.catalog_options,
                    resolve_target_projection=_resolve_target_projection,
                    snapshot_id=base_snapshot_id,
                    ray_remote_args=ray_remote_args,
                )
            if any(c.delete for c in matched_specs):
                delete_ds = build_self_merge_delete_ds(
                    target_identifier=target,
                    clauses=matched_specs,
                    target_field_names=ctx.full_target_field_names,
                    catalog_options=ctx.catalog_options,
                    resolve_target_projection=_resolve_target_projection,
                    snapshot_id=base_snapshot_id,
                    ray_remote_args=ray_remote_args,
                )
        return update_ds, delete_ds, insert_ds, update_cols_union

    # Mirror Spark: matched/not-matched run as two independent joins
    # (inner / left_anti). One unified left_outer join would force
    # joined.materialize() to feed both branches, which can OOM on large merges.
    if matched_specs and base_snapshot is not None:
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
            target_empty=base_snapshot is None,
            ray_remote_args=ray_remote_args,
        )

    return update_ds, delete_ds, insert_ds, update_cols_union


def _execute_and_commit(
    table, update_ds, delete_ds, insert_ds, update_cols_union,
    base_snapshot, num_partitions,
    ray_remote_args, concurrency,
):
    collect_action_row_ids = update_ds is not None and delete_ds is not None

    update_msgs: list = []
    num_updated = 0
    update_row_ids = []
    if update_ds is not None:
        try:
            update_msgs, num_updated, update_row_ids = distributed_update_apply(
                update_ds, table, update_cols_union,
                num_partitions=num_partitions,
                ray_remote_args=ray_remote_args,
                base_snapshot_id=(
                    base_snapshot.id
                    if base_snapshot is not None else None
                ),
                collect_row_ids=collect_action_row_ids,
            )
        except Exception as e:
            _reraise_inner(e)

    delete_msgs: list = []
    num_deleted = 0
    delete_row_ids = []
    if delete_ds is not None:
        try:
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
        except Exception as e:
            _reraise_inner(e)

    if collect_action_row_ids:
        _validate_disjoint_action_row_ids(update_row_ids, delete_row_ids)

    all_msgs: list = list(update_msgs) + list(delete_msgs)
    num_inserted = 0
    if insert_ds is not None:
        try:
            insert_msgs = distributed_write_collect_msgs(
                insert_ds, table,
                ray_remote_args=ray_remote_args, concurrency=concurrency,
            )
        except Exception as e:
            _reraise_inner(e)
        num_inserted = sum(
            f.row_count
            for m in insert_msgs
            for f in m.new_files
            if not DataFileMeta.is_blob_file(f.file_name)
        )
        all_msgs.extend(insert_msgs)
    if all_msgs:
        wb = table.new_batch_write_builder()
        tc = wb.new_commit()
        tc.commit(all_msgs)
        tc.close()

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


def _resolve_num_partitions(num_partitions: Optional[int]) -> int:
    if num_partitions is not None:
        return num_partitions
    try:
        import ray

        cpus = int(ray.cluster_resources().get("CPU", 4))
        return max(1, cpus * 2)
    except Exception:
        return 4


def _require_ray_join() -> None:
    import ray
    from packaging.version import parse

    if parse(ray.__version__) < parse("2.50.0"):
        raise RuntimeError(
            f"merge_into requires ray>=2.50; "
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
            raise TypeError(
                "SET values must be source_col(), target_col(), "
                "lit(), or literals, not callables"
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
):
    import ray.data

    if isinstance(source, ray.data.Dataset):
        return source
    if isinstance(source, str):
        from pypaimon.ray.ray_paimon import read_paimon
        read_kwargs = {}
        if source_snapshot_id is not None:
            read_kwargs["snapshot_id"] = source_snapshot_id
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
