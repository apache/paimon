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

from pypaimon.ray.data_evolution_merge_join import (
    build_matched_update_ds,
    build_not_matched_insert_ds,
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

    update_ds, insert_ds, update_cols_union = _build_datasets(
        target, source_ds, matched_specs, not_matched_specs,
        ctx, base_snapshot, num_partitions, ray_remote_args,
    )

    return _execute_and_commit(
        table, update_ds, insert_ds, update_cols_union,
        base_snapshot, num_partitions,
        ray_remote_args, concurrency,
    )


def _prepare(target, source, catalog_options, when_matched, when_not_matched, on):
    if not when_matched and not when_not_matched:
        raise ValueError(
            "At least one of when_matched or when_not_matched must be non-empty."
        )
    if len(when_matched) > 1 or len(when_not_matched) > 1:
        raise NotImplementedError(
            "merge_into currently supports a single WhenMatched and a single "
            "WhenNotMatched clause; multi-clause fall-through will be added "
            "in a follow-up PR."
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

    blob_cols = _blob_col_names(table)
    full_target_field_names = list(table.field_names)
    # SET specs only cover non-blob columns: update can't rewrite blob files
    # (data evolution puts them in dedicated .blob files), and insert leaves
    # blob columns null since the source can't carry them through SET="*".
    settable_field_names = [
        c for c in full_target_field_names if c not in blob_cols
    ]
    on_map = dict(zip(target_on_cols, source_on_cols))
    if when_matched and table.partition_keys:
        raise ValueError(
            "merge_into does not support matched clauses on partitioned "
            "tables; cross-partition row movement is not implemented."
        )
    matched_specs = [
        _NormalizedClause(
            spec=_normalize_set_spec(
                c.update, settable_field_names, on_map,
            ),
            condition=c.condition,
        )
        for c in when_matched
    ]
    has_condition = any(
        c.condition is not None
        for c in list(when_matched) + list(when_not_matched)
    )
    if has_condition:
        from pypaimon.ray.merge_condition import (
            _require_datafusion, extract_target_columns,
        )
        _require_datafusion()
        for c in when_not_matched:
            if c.condition is not None:
                t_refs = extract_target_columns(c.condition)
                if t_refs:
                    raise ValueError(
                        f"WhenNotMatched condition must not reference "
                        f"target columns (t.*), but found: {sorted(t_refs)}"
                    )
        for c in list(when_matched) + list(when_not_matched):
            if c.condition is not None:
                blob_refs = extract_target_columns(c.condition) & blob_cols
                if blob_refs:
                    raise ValueError(
                        f"condition must not reference blob columns, "
                        f"but found: {sorted(blob_refs)}"
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
    _validate_source_has_target_cols(
        source_ds, matched_specs + not_matched_specs,
    )

    if has_condition:
        from pypaimon.ray.merge_condition import extract_columns
        source_names = set(_source_schema_or_raise(source_ds).names)
        target_names = set(full_target_field_names)
        for c in list(when_matched) + list(when_not_matched):
            if c.condition is not None:
                for ref in extract_columns(c.condition):
                    prefix, col = ref.split(".", 1)
                    if prefix == "s" and col not in source_names:
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
    # update_pa_schema strips blob (only non-blob cols are written by the
    # update path); insert_pa_schema is the full table schema so the writer
    # gets every column (blob columns end up null).
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
    )
    return table, source_ds, matched_specs, not_matched_specs, ctx


def _build_datasets(
    target, source_ds, matched_specs, not_matched_specs,
    ctx: "_PrepareCtx", base_snapshot, num_partitions, ray_remote_args,
):
    # Pin every target read to base_snapshot so all branches see the same
    # snapshot the caller observed; otherwise concurrent commits in between
    # would mix data from different snapshots.
    base_snapshot_id = base_snapshot.id if base_snapshot is not None else None

    update_ds = None
    insert_ds = None
    update_cols_union: List[str] = []

    # Mirror Spark: matched/not-matched run as two independent joins
    # (inner / left_anti). One unified left_outer join would force
    # joined.materialize() to feed both branches, which can OOM on large merges.
    if matched_specs and base_snapshot is not None:
        update_cols_union = _union_update_cols(matched_specs)
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

    if not_matched_specs:
        # Insert writes the full target schema; SET spec only covers
        # settable cols, so blob columns fall through to null.
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

    return update_ds, insert_ds, update_cols_union


def _execute_and_commit(
    table, update_ds, insert_ds, update_cols_union,
    base_snapshot, num_partitions,
    ray_remote_args, concurrency,
):
    update_msgs: list = []
    num_updated = 0
    if update_ds is not None:
        try:
            update_msgs, num_updated = distributed_update_apply(
                update_ds, table, update_cols_union,
                num_partitions=num_partitions,
                ray_remote_args=ray_remote_args,
                base_snapshot_id=(
                    base_snapshot.id
                    if base_snapshot is not None else None
                ),
            )
        except Exception as e:
            _reraise_inner(e)

    all_msgs: list = list(update_msgs)
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
            f.row_count for m in insert_msgs for f in m.new_files
        )
        all_msgs.extend(insert_msgs)
    if all_msgs:
        wb = table.new_batch_write_builder()
        tc = wb.new_commit()
        tc.commit(all_msgs)
        tc.close()

    # num_matched = rows that passed the condition and were updated
    return {
        "num_matched": num_updated,
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


def _blob_col_names(table) -> set:
    return {
        f.name
        for f in table.table_schema.fields
        if getattr(f.type, "type", None) == "BLOB"
    }


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
    source_ds,
    specs: List[_NormalizedClause],
) -> None:
    names = set(_source_schema_or_raise(source_ds).names)
    needed = set()
    for clause in specs:
        for val in clause.spec.values():
            if isinstance(val, SourceColumnRef):
                needed.add(val.column)
    missing = sorted(needed - names)
    if missing:
        raise ValueError(
            f"source is missing columns {missing} referenced by SET spec"
        )
