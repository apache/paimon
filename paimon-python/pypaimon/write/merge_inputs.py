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

"""Shared MERGE input validation and normalization without table execution."""

from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import pyarrow as pa

from pypaimon.table.special_fields import SpecialFields
from pypaimon.write.merge_types import (
    LiteralValue,
    OnSpec,
    SetSpec,
    SourceColumnRef,
    TargetColumnRef,
    _NormalizedClause,
)


@dataclass(frozen=True)
class _PrepareCtx:
    target_on_cols: List[str]
    source_on_cols: List[str]
    settable_field_names: List[str]
    full_target_field_names: List[str]
    update_pa_schema: pa.Schema
    full_pa_schema: pa.Schema
    is_self_merge: bool = False


def _prepare(
    target_table,
    source,
    when_matched,
    when_not_matched,
    on,
    *,
    source_schema=None,
    source_loader=None,
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
                allow_callables=True,
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
        from pypaimon.ray.merge_condition import extract_target_columns
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
            allow_callables=True,
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
        # Native table input needs schema validation without Python I/O.
        source_table = (source_schema.empty_table() if source_schema is not None
                        else (source_loader(source, source_on_cols)
                              if source_loader is not None
                              else _normalize_memory_source(source)))
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
    callable_columns: Optional[Sequence[str]] = None,
) -> list:
    needed = set(_needed_target_cols(
        clauses, target_on, update_cols, target_field_names,
    ))
    if any(callable(value) and not isinstance(value, type)
           for clause in clauses for value in clause.spec.values()):
        needed.update(target_field_names if callable_columns is None else callable_columns)
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


def _validate_source_on_cols(source_table: pa.Table, on: Sequence[str]) -> None:
    names = set(source_table.schema.names)
    missing = [c for c in on if c not in names]
    if missing:
        raise ValueError(
            "'on' columns {} missing from source schema {}.".format(
                missing, list(names)
            )
        )


def _is_self_merge(target_table, source, target_on, source_on) -> bool:
    row_id_name = SpecialFields.ROW_ID.name
    return (
        _is_table_like(source)
        and _same_table(target_table, source)
        and (source is target_table or not (
            _has_selected_read_view(target_table) or _has_selected_read_view(source)))
        and target_on == [row_id_name]
        and source_on == [row_id_name]
    )


def _has_selected_read_view(table):
    from pypaimon.snapshot.time_travel_util import SCAN_KEYS
    return ('_read_snapshot' in vars(table)
            or any(key in table.table_schema.options for key in SCAN_KEYS))


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


def _merge_callable_input(batch: pa.Table, on_pairs):
    # Arrow joins coalesce ON keys; callbacks still expose both SQL aliases.
    for source, target in on_pairs:
        source_name, target_name = "s." + source, "t." + target
        if target_name in batch.column_names:
            if source_name in batch.column_names:
                batch = batch.set_column(batch.schema.get_field_index(source_name),
                                         source_name, batch.column(target_name))
            else:
                batch = batch.append_column(source_name, batch.column(target_name))
    return batch.select(sorted(name for name in batch.column_names
                               if name.startswith(("t.", "s."))))


def _normalize_memory_source(source: Any) -> pa.Table:
    if isinstance(source, pa.Table):
        return source
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
