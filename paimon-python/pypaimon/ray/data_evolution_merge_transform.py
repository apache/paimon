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

from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple, Union

import pyarrow as pa

SetSpec = Union[str, Mapping[str, Any]]
OnSpec = Union[Sequence[str], Mapping[str, str]]


@dataclass(frozen=True)
class SourceColumnRef:
    column: str


@dataclass(frozen=True)
class TargetColumnRef:
    column: str


@dataclass(frozen=True)
class LiteralValue:
    value: Any


def source_col(name: str) -> SourceColumnRef:
    return SourceColumnRef(name)


def target_col(name: str) -> TargetColumnRef:
    return TargetColumnRef(name)


def lit(value: Any) -> LiteralValue:
    return LiteralValue(value)


@dataclass
class WhenMatched:
    update: SetSpec
    condition: Optional[str] = None


@dataclass
class WhenNotMatched:
    insert: SetSpec
    condition: Optional[str] = None


@dataclass
class _NormalizedClause:
    spec: Dict[str, Any]
    condition: Optional[str] = None


def vectorized_matched_transform(
    batch: pa.Table,
    spec: Dict[str, Any],
    on_pairs: Sequence[Tuple[str, str]],
    update_cols: Sequence[str],
    row_id_name: str,
    update_schema: pa.Schema,
) -> pa.Table:
    available = set(batch.schema.names)
    arrays: list = [batch.column(f"t.{row_id_name}")]
    for col in update_cols:
        out_type = update_schema.field(col).type
        if col in spec:
            arrays.append(
                _resolve_spec_array(
                    spec[col], batch, available, on_pairs, out_type
                )
            )
        else:
            arrays.append(batch.column(f"t.{col}"))
    return pa.Table.from_arrays(arrays, schema=update_schema)


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


def build_update_schema(
    target_pa_schema: pa.Schema,
    update_cols: Sequence[str],
    row_id_name: str,
) -> pa.Schema:
    return pa.schema(
        [pa.field(row_id_name, pa.int64(), nullable=False)]
        + [target_pa_schema.field(col) for col in update_cols]
    )


def _resolve_spec_array(
    val: Any,
    batch: pa.Table,
    available: set,
    on_pairs: Sequence[Tuple[str, str]],
    out_type: pa.DataType,
):
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
