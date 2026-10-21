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

from pypaimon.write.merge_types import (
    LiteralValue as LiteralValue,
    OnSpec as OnSpec,
    SetSpec as SetSpec,
    SourceColumnRef as SourceColumnRef,
    TargetColumnRef as TargetColumnRef,
    WhenMatched as WhenMatched,
    WhenNotMatched as WhenNotMatched,
    _NormalizedClause as _NormalizedClause,
    lit as lit,
    source_col as source_col,
    target_col as target_col,
)


__all__ = [
    'LiteralValue', 'OnSpec', 'SetSpec', 'SourceColumnRef', 'TargetColumnRef',
    'WhenMatched', 'WhenNotMatched', '_NormalizedClause', 'lit', 'source_col', 'target_col',
    'vectorized_matched_transform', 'vectorized_delete_transform', 'vectorized_insert_transform',
]


def vectorized_matched_transform(
    batch: pa.Table,
    spec: Dict[str, Any],
    on_pairs: Sequence[Tuple[str, str]],
    update_cols: Sequence[str],
    row_id_name: str,
    update_schema: pa.Schema,
    callable_input: Optional[pa.Table] = None,
) -> pa.Table:
    available = set(batch.schema.names)
    arrays: list = [batch.column(f"t.{row_id_name}")]
    for col in update_cols:
        out_type = update_schema.field(col).type
        if col in spec:
            arrays.append(
                _resolve_spec_array(
                    spec[col], batch, available, on_pairs, out_type,
                    callable_input=callable_input,
                )
            )
        else:
            arrays.append(batch.column(f"t.{col}"))
    return pa.Table.from_arrays(arrays, schema=update_schema)


def vectorized_delete_transform(
    batch: pa.Table,
    row_id_name: str,
    delete_schema: pa.Schema,
) -> pa.Table:
    return pa.Table.from_arrays(
        [batch.column(f"t.{row_id_name}")], schema=delete_schema
    )


def vectorized_insert_transform(
    batch: pa.Table,
    spec: Dict[str, Any],
    target_field_names: Sequence[str],
    target_pa_schema: pa.Schema,
) -> pa.Table:
    available = set(batch.schema.names)
    arrays: list = []
    for col in target_field_names:
        out_type = target_pa_schema.field(col).type
        if col in spec:
            arrays.append(
                _resolve_spec_array(
                    spec[col], batch, available, (), out_type
                )
            )
        else:
            arrays.append(pa.nulls(batch.num_rows, type=out_type))
    return pa.Table.from_arrays(arrays, schema=target_pa_schema)


def cast_to_schema(batch: pa.Table, schema: pa.Schema) -> pa.Table:
    return batch if batch.schema == schema else batch.cast(schema)


def build_update_schema(
    target_pa_schema: pa.Schema,
    update_cols: Sequence[str],
    row_id_name: str,
) -> pa.Schema:
    return pa.schema(
        [pa.field(row_id_name, pa.int64(), nullable=False)]
        + [target_pa_schema.field(col) for col in update_cols]
    )


def build_delete_schema(row_id_name: str) -> pa.Schema:
    return pa.schema([pa.field(row_id_name, pa.int64(), nullable=False)])


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


def _resolve_spec_array(
    val: Any,
    batch: pa.Table,
    available: set,
    on_pairs: Sequence[Tuple[str, str]],
    out_type: pa.DataType,
    callable_input: Optional[pa.Table] = None,
):
    if callable(val) and not isinstance(val, type):
        # Public MERGE callbacks see the joined t./s. columns. The internal
        # predicate-update adapter supplies its existing unqualified read view.
        result = val(_merge_callable_input(batch, on_pairs) if callable_input is None else callable_input)
        if not isinstance(result, (pa.Array, pa.ChunkedArray)):
            raise ValueError(
                "Callable SET values must return a pyarrow.Array or "
                "pyarrow.ChunkedArray."
            )
        if len(result) != batch.num_rows:
            raise ValueError(
                "Callable SET result length must match matched row count: "
                f"{len(result)} != {batch.num_rows}."
            )
        if result.type != out_type:
            if isinstance(result, pa.ChunkedArray):
                result = pa.chunked_array(
                    [chunk.cast(out_type) for chunk in result.chunks],
                    type=out_type,
                )
            else:
                result = result.cast(out_type)
        return result
    if isinstance(val, LiteralValue):
        return pa.array([val.value] * batch.num_rows, type=out_type)
    if isinstance(val, SourceColumnRef):
        ref = val.column
        if f"s.{ref}" in available:
            return batch.column(f"s.{ref}")
        for sk, tk in on_pairs:
            if sk == ref and f"t.{tk}" in available:
                return batch.column(f"t.{tk}")
        return pa.nulls(batch.num_rows, type=out_type)
    if isinstance(val, TargetColumnRef):
        col_name = f"t.{val.column}"
        return batch.column(col_name) if col_name in available else pa.nulls(
            batch.num_rows, type=out_type
        )
    raise TypeError(f"unexpected spec value type: {type(val).__name__}")


def _build_matched_transform(
    clauses: List[_NormalizedClause],
    on_map: Dict[str, str],
    on_pairs: List[Tuple[str, str]],
    update_cols: List[str],
    row_id_name: str,
    update_schema: pa.Schema,
    callable_input_columns: Optional[Sequence[str]] = None,
):
    prepared_clauses = []
    for clause in clauses:
        rewritten = None
        if clause.condition is not None:
            from pypaimon.write.merge_condition import (
                remap_source_on_keys, rewrite_condition,
            )
            rewritten = remap_source_on_keys(
                rewrite_condition(clause.condition), on_map,
            )
        prepared_clauses.append((clause.spec, rewritten, clause.delete))

    _filter_batch = None
    if any(r is not None for _, r, _ in prepared_clauses):
        from pypaimon.write.merge_condition import filter_batch as _filter_batch

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
                callable_input = None
                if (callable_input_columns is not None
                        and any(callable(value) and not isinstance(value, type)
                                for value in spec.values())):
                    callable_input = pa.Table.from_arrays(
                        [matched.column(f"t.{col}")
                         for col in callable_input_columns],
                        names=list(callable_input_columns),
                    )
                parts.append(vectorized_matched_transform(
                    matched, spec, on_pairs,
                    update_cols, row_id_name,
                    update_schema,
                    callable_input=callable_input,
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
            from pypaimon.write.merge_condition import (
                remap_source_on_keys, rewrite_condition,
            )
            rewritten = remap_source_on_keys(
                rewrite_condition(clause.condition), on_map,
            )
        prepared_clauses.append((rewritten, clause.delete))

    _filter_batch = None
    if any(r is not None for r, _ in prepared_clauses):
        from pypaimon.write.merge_condition import filter_batch as _filter_batch

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
