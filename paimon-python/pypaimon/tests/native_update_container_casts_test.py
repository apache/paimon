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

"""Equivalent Arrow container inputs must work on the existing update APIs."""

from contextlib import ExitStack
from decimal import Decimal
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.table.data_evolution_merge_into import WhenMatched, source_col
from pypaimon.write.table_update import TableUpdate
from pypaimon.write.table_update_by_row_id import TableUpdateByRowId
from pypaimon.write.table_upsert_by_key import TableUpsertByKey


pytestmark = pytest.mark.native_plan

_CASES = [
    pytest.param(pa.int32(), pa.int64(), [1, None, 4], [1, None, 4], id='integer'),
    pytest.param(pa.string(), pa.int32(), ['1', None, '4'], [1, None, 4], id='text'),
    pytest.param(pa.decimal128(8, 3), pa.decimal128(10, 2),
                 [Decimal('1.250'), None, Decimal('-2.500')],
                 [Decimal('1.25'), None, Decimal('-2.50')], id='decimal'),
    pytest.param(pa.large_binary(), pa.binary(), [b'\xff\0a', None, b''], [b'\xff\0a', None, b''], id='binary'),
]
_OPERATIONS = ['row_id', 'incremental', 'predicate_array', 'predicate_callable', 'upsert', 'merge']


def _layout(source_value_type, target_value_type, source_values, expected_values, shape):
    source_type = pa.map_(pa.field('source_key', pa.string(), nullable=False),
                          pa.field('source_value', source_value_type))
    target_type = pa.list_(pa.struct([('label', pa.string()), ('amount', target_value_type)]))
    values = [[('a', source_values[0]), ('b', source_values[1])], None, [], [('z', source_values[2])]]
    expected = [[{'label': 'a', 'amount': expected_values[0]},
                 {'label': 'b', 'amount': expected_values[1]}], None, [],
                [{'label': 'z', 'amount': expected_values[2]}]]
    if shape == 'row':
        source_type = pa.struct([('items', source_type), ('extra', pa.int32())])
        target_type = pa.struct([('items', target_type), ('extra', pa.int32())])
        values = [{'items': value, 'extra': i} if i != 1 else None for i, value in enumerate(values)]
        expected = [{'items': value, 'extra': i} if i != 1 else None for i, value in enumerate(expected)]
    elif shape == 'array':
        source_type, target_type = pa.list_(source_type), pa.list_(target_type)
        values = [[values[0], None, []], None, [], [values[3]]]
        expected = [[expected[0], None, []], None, [], [expected[3]]]
    elif shape == 'map':
        source_type, target_type = pa.map_(pa.string(), source_type), pa.map_(pa.string(), target_type)
        values = [[('items', values[0]), ('null', None)], None, [], [('last', values[3])]]
        expected = [[('items', expected[0]), ('null', None)], None, [], [('last', expected[3])]]
    # The slice retains nonzero MAP offsets at one or more nesting levels.
    source = pa.array([values[0]] + values + [values[3]], type=source_type).slice(1, 4)
    return source, target_type, expected


def _table(tmp_path, value_type, native):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('db', True)
    schema = pa.schema([('id', pa.int32()), ('value', value_type), ('keep', pa.int32())])
    catalog.create_table('db.t', Schema.from_pyarrow_schema(schema, options={
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'write.native.enabled': str(native).lower(),
    }), False)
    table = catalog.get_table('db.t')
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.table({'id': list(range(5)), 'value': [None] * 5,
                                    'keep': list(range(10, 15))}, schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    return table


def _native_only(native):
    stack = ExitStack()
    if native:
        for cls, method in [(TableUpdateByRowId, '_load_existing_files_info'),
                            (TableUpdate, '_build_predicate_update_table'),
                            (TableUpsertByKey, '_upsert_partition')]:
            stack.enter_context(patch.object(cls, method, side_effect=AssertionError('Python update fallback')))
        stack.enter_context(patch('pypaimon.table.data_evolution_merge_into._build_tables',
                                  side_effect=AssertionError('Python MERGE fallback')))
    return stack


def _apply(table, source, native, stream, operation):
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    update = builder.new_update().with_update_type(['value'])
    # Include empty chunks and boundaries that differ from the target files.
    chunks = pa.chunked_array([source.slice(0, 0), source.slice(0, 1), source.slice(1, 2),
                               source.slice(3, 0), source.slice(3)])
    ids = pa.array(range(len(source)), type=pa.int32())
    data = pa.table({'value': chunks, '_ROW_ID': pa.array(range(len(source)), type=pa.int64())})
    with _native_only(native):
        if operation == 'row_id':
            messages = (update.update_by_arrow_with_row_id(data, 31) if stream
                        else update.update_by_arrow_with_row_id(data))
        elif operation == 'incremental':
            writer = update.new_update_by_row_id(31) if stream else update.new_update_by_row_id()
            messages = writer.update_columns(data, ['value'])
        elif operation.startswith('predicate'):
            kwargs = {}
            if operation == 'predicate_callable':
                def assignment(rows):
                    assert rows.column_names == ['id', '_ROW_ID']
                    # Return a different chunk layout from the selected batches.
                    return chunks.take(rows['id'])
                kwargs['read_columns'] = ['id']
            else:
                assignment = chunks
            predicate = PredicateBuilder(table.fields).less_than('id', len(source))
            messages = (update.update_by_predicate(predicate, {'value': assignment}, 31, **kwargs) if stream
                        else update.update_by_predicate(predicate, {'value': assignment}, **kwargs))
        elif operation == 'upsert':
            data = pa.table({'id': ids, 'value': chunks})
            messages = update.upsert_by_arrow_with_key(data, ['id'], 31) if stream else \
                update.upsert_by_arrow_with_key(data, ['id'])
        else:
            data = pa.table({'id': ids, 'value': chunks})
            kwargs = {'commit_identifier': 31} if stream else {}
            messages = update.merge_into(data, on=['id'], when_matched=[
                WhenMatched.update({'value': source_col('value')})], **kwargs)
    commit = builder.new_commit()
    try:
        commit.commit(messages, 31) if stream else commit.commit(messages)
    finally:
        commit.close()


def _check(table, expected):
    for native in (False, True):
        view = table.copy({'scan.native-plan.enabled': str(native).lower(),
                           'read.native.enabled': str(native).lower()})
        builder = view.new_read_builder().with_projection(['id', 'value', 'keep', '_ROW_ID'])
        reader = builder.new_read()
        with ExitStack() as stack:
            if native:
                stack.enter_context(patch.object(reader, '_create_split_read',
                                                 side_effect=AssertionError('Python read fallback')))
            rows = reader.to_arrow(builder.new_scan().plan().splits()).sort_by('id')
        assert rows['value'].to_pylist() == expected + [None] * (5 - len(expected))
        assert rows['keep'].to_pylist() == list(range(10, 15))
        assert rows['_ROW_ID'].to_pylist() == list(range(5))


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
@pytest.mark.parametrize('operation', _OPERATIONS)
@pytest.mark.parametrize('shape', ['root', 'row', 'array', 'map'])
@pytest.mark.parametrize('source_type,target_type,values,expected', _CASES)
def test_map_inputs_update_array_rows(tmp_path, native, stream, operation, shape,
                                      source_type, target_type, values, expected):
    source, target_type, expected = _layout(source_type, target_type, values, expected, shape)
    table = _table(tmp_path, target_type, native)
    _apply(table, source, native, stream, operation)
    _check(table, expected)


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('operation', _OPERATIONS)
@pytest.mark.parametrize('shape', ['root', 'row', 'array'])
def test_invalid_map_child_cannot_change_existing_rows(tmp_path, native, operation, shape):
    source, target_type, _ = _layout(pa.int64(), pa.int32(), [1, None, 2 ** 40], [1, None, 0], shape)
    table = _table(tmp_path, target_type, native)
    before = table.snapshot_manager().get_latest_snapshot().id
    with pytest.raises((ValueError, TypeError, OverflowError)):
        _apply(table, source, native, False, operation)
    assert table.snapshot_manager().get_latest_snapshot().id == before
    _check(table, [None] * 4)
