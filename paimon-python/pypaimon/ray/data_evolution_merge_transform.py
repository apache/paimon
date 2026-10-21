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

from typing import Any, Dict, Optional, Sequence, Tuple

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
    from pypaimon.execution.merge_transform import vectorized_matched_transform as execute
    return execute(batch, spec, on_pairs, update_cols, row_id_name, update_schema, callable_input)


def vectorized_delete_transform(
    batch: pa.Table,
    row_id_name: str,
    delete_schema: pa.Schema,
) -> pa.Table:
    from pypaimon.execution.merge_transform import vectorized_delete_transform as execute
    return execute(batch, row_id_name, delete_schema)


def vectorized_insert_transform(
    batch: pa.Table,
    spec: Dict[str, Any],
    target_field_names: Sequence[str],
    target_pa_schema: pa.Schema,
) -> pa.Table:
    from pypaimon.execution.merge_transform import vectorized_insert_transform as execute
    return execute(batch, spec, target_field_names, target_pa_schema)


def cast_to_schema(batch: pa.Table, schema: pa.Schema) -> pa.Table:
    from pypaimon.execution.merge_transform import cast_to_schema as execute
    return execute(batch, schema)


def build_update_schema(
    target_pa_schema: pa.Schema,
    update_cols: Sequence[str],
    row_id_name: str,
) -> pa.Schema:
    from pypaimon.execution.merge_transform import build_update_schema as execute
    return execute(target_pa_schema, update_cols, row_id_name)


def build_delete_schema(row_id_name: str) -> pa.Schema:
    from pypaimon.execution.merge_transform import build_delete_schema as execute
    return execute(row_id_name)


def _merge_callable_input(batch: pa.Table, on_pairs):
    # Arrow joins coalesce ON keys; callbacks still expose both SQL aliases.
    from pypaimon.execution.merge_transform import _merge_callable_input as execute
    return execute(batch, on_pairs)


def _resolve_spec_array(
    val: Any,
    batch: pa.Table,
    available: set,
    on_pairs: Sequence[Tuple[str, str]],
    out_type: pa.DataType,
    callable_input: Optional[pa.Table] = None,
):
    from pypaimon.execution.merge_transform import _resolve_spec_array as execute
    return execute(val, batch, available, on_pairs, out_type, callable_input)
