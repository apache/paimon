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
from unittest.mock import patch

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


@pytest.mark.parametrize('action', ['update', 'delete'])
@pytest.mark.parametrize('stream', [False, True])
def test_selected_python_merge_keeps_actions_in_python(tmp_path, action, stream):
    from pypaimon import CatalogFactory, Schema
    from pypaimon.write.merge_types import WhenMatched, WhenNotMatched

    schema = pa.schema([('id', pa.int64()), ('value', pa.int32())])
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.actions', Schema.from_pyarrow_schema(schema, options={
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'deletion-vectors.enabled': 'true',
        'write.native.enabled': 'false', 'commit.native.enabled': 'false',
        'scan.native-plan.enabled': 'false', 'read.native.enabled': 'false',
    }), False)
    table = catalog.get_table('default.actions')
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.table({'id': [1, 2], 'value': [10, 20]}, schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()

    # Native may be unavailable for the whole MERGE request. Its chosen Python
    # executor must not attempt another Native selection for individual actions.
    table = table.copy({'write.native.enabled': 'true'})
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    commit = builder.new_commit()
    source = pa.table({'id': [1, 3], 'value': [11, 30]}, schema=schema)
    matched = WhenMatched.update({'value': SourceColumnRef('value')}) if action == 'update' else WhenMatched.delete()
    with patch('pypaimon.write.backend_dispatch.prepare_native_merge_into', return_value=None) as select, \
            patch('pypaimon.write.backend_dispatch.prepare_native_write_operation',
                  side_effect=AssertionError('Python MERGE tried to select Native update')) as select_update, \
            patch('pypaimon.execution.native.write.native_update.create_native_row_id_delete',
                  side_effect=AssertionError('Python MERGE tried to select Native delete')) as select_delete, \
            patch('pypaimon.execution.native.write.native_write.create_native_write',
                  side_effect=AssertionError('Python MERGE tried to select Native insert')) as select_insert:
        try:
            update = builder.new_update()
            arguments = dict(on=['id'], when_matched=[matched], when_not_matched=[WhenNotMatched('*')])
            if stream:
                messages = update.merge_into(source, commit_identifier=7, **arguments)
                commit.commit(messages, 7)
            else:
                messages = update.merge_into(source, **arguments)
                commit.commit(messages)
        finally:
            commit.close()
        select.assert_called_once()
        select_update.assert_not_called()
        select_delete.assert_not_called()
        select_insert.assert_not_called()

    read = table.new_read_builder()
    actual = read.new_read().to_arrow(read.new_scan().plan().splits()).sort_by('id').to_pylist()
    expected = ([{'id': 1, 'value': 11}] if action == 'update' else []) + [
        {'id': 2, 'value': 20}, {'id': 3, 'value': 30}]
    assert actual == expected
