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

"""Real SDK coverage of the joined-batch MERGE boundary used by Ray workers."""

import pyarrow as pa
import pytest

from pypaimon.write.backend_dispatch import create_merge_batch_transform
from pypaimon.write.merge_types import LiteralValue, SourceColumnRef, _NormalizedClause

pytestmark = pytest.mark.native_plan


def test_native_matched_actions_preserve_clause_order_and_callable_view():
    batch = pa.table({
        't._ROW_ID': pa.chunked_array([[0, 1], [2, 3]], type=pa.int64()),
        't.id': [10, 11, 12, 13],
        't.value': pa.array([90, 91, 92, 93], type=pa.int32()),
        't.other': ['a', 'b', 'c', 'd'],
        's.value': pa.array([None, 5, 15, 25], type=pa.int32()),
    })
    calls = []

    def fallback(rows):
        calls.append(rows.to_pydict())
        assert rows.column_names == ['s.key', 's.value', 't._ROW_ID', 't.id', 't.other', 't.value']
        return pa.chunked_array([[str(value)] for value in rows.column('s.key').to_pylist()])

    clauses = [
        _NormalizedClause({}, condition='s.value > 20', delete=True),
        _NormalizedClause({'value': SourceColumnRef('value')}, condition='s.value > 10'),
        _NormalizedClause({'other': fallback}),
    ]
    # Output order can differ from the first appearance of fields in clauses.
    schema = pa.schema([pa.field('_ROW_ID', pa.int64(), nullable=False),
                        ('other', pa.string()), ('value', pa.int32())])
    transform = create_merge_batch_transform(
        'update', clauses, {'key': 'id'}, [('key', 'id')], ['other', 'value'],
        '_ROW_ID', schema, native_enabled=True)
    actual = transform(batch)
    assert actual.schema == schema
    assert actual.to_pylist() == [
        {'_ROW_ID': 2, 'other': 'c', 'value': 15},
        {'_ROW_ID': 0, 'other': '10', 'value': 90},
        {'_ROW_ID': 1, 'other': '11', 'value': 91},
    ]
    assert len(calls) == 1
    assert calls[0]['t._ROW_ID'] == [0, 1]
    delete_schema = pa.schema([pa.field('_ROW_ID', pa.int64(), nullable=False)])
    deletes = create_merge_batch_transform(
        'delete', clauses, {'key': 'id'}, '_ROW_ID', delete_schema, native_enabled=True)
    assert deletes(batch).to_pylist() == [{'_ROW_ID': 3}]
    assert len(calls) == 1  # UPDATE assignments are irrelevant to the delete route.


def test_native_insert_actions_fill_typed_nulls_and_keep_chunk_order():
    schema = pa.schema([('value', pa.int32()), ('optional', pa.string())])
    clauses = [
        _NormalizedClause({'value': LiteralValue(20)}, condition='s.value >= (SELECT 20)'),
        _NormalizedClause({'value': LiteralValue(10)}, condition='s.value >= 10'),
        _NormalizedClause({'value': SourceColumnRef('value')}),
    ]
    batch = pa.table({'s.value': pa.chunked_array([
        pa.array([None, 5], type=pa.int32()), pa.array([15, 25], type=pa.int32()),
    ])})
    transform = create_merge_batch_transform(
        'insert', clauses, schema.names, schema, native_enabled=True)
    actual = transform(batch)
    assert actual.schema == schema
    assert actual.to_pylist() == [
        {'value': 20, 'optional': None}, {'value': 10, 'optional': None},
        {'value': None, 'optional': None}, {'value': 5, 'optional': None},
    ]


def test_native_worker_transform_is_serializable_after_compilation():
    import cloudpickle

    schema = pa.schema([pa.field('_ROW_ID', pa.int64(), nullable=False), ('value', pa.int32())])
    transform = create_merge_batch_transform(
        'update', [_NormalizedClause({'value': SourceColumnRef('key')})],
        {'key': 'id'}, [('key', 'id')], ['value'], '_ROW_ID', schema, native_enabled=True)
    batch = pa.table({'t._ROW_ID': [7], 't.id': pa.array([42], type=pa.int32())})
    assert transform(batch).to_pylist() == [{'_ROW_ID': 7, 'value': 42}]
    restored = cloudpickle.loads(cloudpickle.dumps(transform))
    assert restored(batch).to_pylist() == [{'_ROW_ID': 7, 'value': 42}]


def test_native_self_update_callable_keeps_explicit_unqualified_projection():
    calls = []

    def assignment(rows):
        calls.append(rows.column_names)
        assert rows.schema == pa.schema([('value', pa.int32()), ('id', pa.int64())])
        return rows.column('value')

    schema = pa.schema([pa.field('_ROW_ID', pa.int64(), nullable=False), ('value', pa.int32())])
    transform = create_merge_batch_transform(
        'update', [_NormalizedClause({'value': assignment}, condition='s.id > 1')],
        {'id': 'id'}, [('id', 'id')], ['value'], '_ROW_ID', schema,
        callable_input_columns=['value', 'id'], native_enabled=True)
    batch = pa.table({'t._ROW_ID': [0, 1], 't.id': [1, 2],
                      't.value': pa.array([3, 4], type=pa.int32())})
    assert transform(batch).to_pylist() == [{'_ROW_ID': 1, 'value': 4}]
    assert calls == [['value', 'id']]
