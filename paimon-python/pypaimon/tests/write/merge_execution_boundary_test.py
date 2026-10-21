# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Local and distributed MERGE use the same Python action execution."""

import importlib

import pyarrow as pa
import pytest

from pypaimon.execution.python.write.merge_transform import _build_insert_transform
from pypaimon.write.merge_transform import build_delete_schema, build_update_schema
from pypaimon.write.merge_types import LiteralValue, SourceColumnRef, _NormalizedClause

pytestmark = [pytest.mark.python_plan, pytest.mark.python_read,
              pytest.mark.python_write, pytest.mark.python_commit]


@pytest.mark.parametrize('module', [
    'pypaimon.execution.python.write.merge_transform',
    'pypaimon.ray.data_evolution_merge_join',
])
def test_matched_actions_consume_first_match_and_preserve_null_fallthrough(module):
    pytest.importorskip('datafusion')
    if '.ray.' in module:
        pytest.importorskip('ray')
    execution = importlib.import_module(module)
    batch = pa.table({
        't._ROW_ID': pa.array([0, 1, 2, 3], type=pa.int64()),
        't.value': pa.array([90, 91, 92, 93], type=pa.int32()),
        's.value': pa.array([None, 5, 15, 25], type=pa.int32()),
    })
    calls = []

    def fallback(rows):
        calls.append(rows.column('t._ROW_ID').to_pylist())
        return pa.array([7] * rows.num_rows, type=pa.int32())

    clauses = [
        _NormalizedClause(spec={}, condition='s.value > 20', delete=True),
        _NormalizedClause(spec={'value': SourceColumnRef('value')}, condition='s.value > 10'),
        _NormalizedClause(spec={'value': fallback}),
    ]
    update_schema = build_update_schema(pa.schema([('value', pa.int32())]), ['value'], '_ROW_ID')
    update = execution._build_matched_transform(
        clauses, {}, [], ['value'], '_ROW_ID', update_schema)(batch)
    delete = execution._build_matched_delete_transform(
        clauses, {}, '_ROW_ID', build_delete_schema('_ROW_ID'))(batch)

    assert update.to_pylist() == [
        {'_ROW_ID': 2, 'value': 15}, {'_ROW_ID': 0, 'value': 7}, {'_ROW_ID': 1, 'value': 7},
    ]
    assert delete.to_pylist() == [{'_ROW_ID': 3}]
    assert calls == [[0, 1]]


def test_insert_actions_choose_first_match_across_arrow_chunks():
    pytest.importorskip('datafusion')
    batch = pa.table({'s.value': pa.chunked_array([
        pa.array([None, 5], type=pa.int32()), pa.array([15, 25], type=pa.int32()),
    ])})
    schema = pa.schema([('value', pa.int32())])
    transform = _build_insert_transform([
        _NormalizedClause(spec={'value': LiteralValue(20)}, condition='s.value >= 20'),
        _NormalizedClause(spec={'value': LiteralValue(10)}, condition='s.value >= 10'),
        _NormalizedClause(spec={'value': SourceColumnRef('value')}),
    ], ['value'], schema)

    assert transform(batch).to_pylist() == [
        {'value': 20}, {'value': 10}, {'value': None}, {'value': 5},
    ]
