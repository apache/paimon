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

"""Select the first matching MERGE action for each Python input row."""

from typing import Dict, List, Optional, Sequence, Tuple

import pyarrow as pa

from pypaimon.write.merge_condition import _load_datafusion
from pypaimon.write.merge_types import _NormalizedClause
from pypaimon.write.merge_transform import (
    cast_to_schema,
    vectorized_delete_transform,
    vectorized_insert_transform,
    vectorized_matched_transform,
)


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
        _load_datafusion()
        from pypaimon.execution.python.write.merge_condition import filter_batch as _filter_batch

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
        _load_datafusion()
        from pypaimon.execution.python.write.merge_condition import filter_batch as _filter_batch

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


def _build_insert_transform(
    clauses: List[_NormalizedClause],
    target_field_names: Sequence[str],
    target_pa_schema: pa.Schema,
):
    prepared_clauses = []
    for clause in clauses:
        rewritten = None
        if clause.condition is not None:
            from pypaimon.write.merge_condition import rewrite_condition

            rewritten = rewrite_condition(clause.condition)
        prepared_clauses.append((clause.spec, rewritten))

    _filter_batch = None
    if any(r is not None for _, r in prepared_clauses):
        _load_datafusion()
        from pypaimon.execution.python.write.merge_condition import filter_batch as _filter_batch

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
