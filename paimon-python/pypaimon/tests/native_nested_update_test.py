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

"""Nested Native update dispatch and parity with the Python implementation."""

from contextlib import ExitStack
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.write.table_update import TableUpdate
from pypaimon.write.table_update_by_row_id import TableUpdateByRowId
from pypaimon.write.table_upsert_by_key import TableUpsertByKey


pytestmark = pytest.mark.native_plan

CASES = [
    (pa.list_(pa.struct([('name', pa.string()), ('amount', pa.int32())])),
     [[{'name': 'a', 'amount': 1}, None], [], None, [{'name': None, 'amount': 4}]]),
    (pa.map_(pa.field('source_key', pa.string(), nullable=False),
             pa.field('source_value', pa.list_(pa.int32()))),
     [[('a', [1, None]), ('b', [])], [], None, [('c', None)]]),
    (pa.struct([('items', pa.list_(pa.int32())), ('raw', pa.binary())]),
     [{'items': [1, None], 'raw': b'abc'}, {'items': [], 'raw': None}, None,
      {'items': None, 'raw': b''}]),
]


def _table(tmp_path, data_type, native):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([('id', pa.int32()), ('value', data_type), ('untouched', pa.int32())])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, options={
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'write.native.enabled': str(native).lower(),
    }), False)
    table = catalog.get_table('default.t')
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.table({'id': [0, 1, 2, 3], 'value': [None] * 4,
                                    'untouched': [10, 11, 12, 13]}, schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    return table, schema


def _native_only(native):
    stack = ExitStack()
    if native:
        for cls, method in [(TableUpdateByRowId, '_load_existing_files_info'),
                            (TableUpdate, '_build_predicate_update_table'),
                            (TableUpsertByKey, '_upsert_partition'),
                            (TableUpsertByKey, '_upsert_row_partition')]:
            stack.enter_context(patch.object(cls, method, side_effect=AssertionError('Python fallback')))
    return stack


def _commit(builder, messages, stream):
    commit = builder.new_commit()
    try:
        commit.commit(messages, 7) if stream else commit.commit(messages)
    finally:
        commit.close()


def _check_rows(table, values):
    for native in (False, True):
        copy = table.copy({'scan.native-plan.enabled': str(native).lower(),
                           'read.native.enabled': str(native).lower()})
        builder = copy.new_read_builder()
        read = builder.new_read()
        with ExitStack() as stack:
            if native:
                stack.enter_context(patch.object(read, '_create_split_read',
                                                 side_effect=AssertionError('Python read fallback')))
            actual = read.to_arrow(builder.new_scan().plan().splits()).sort_by('id')
        assert actual['value'].to_pylist() == values
        assert actual['untouched'].to_pylist() == list(range(10, 10 + len(values)))


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('stream', [False, True])
@pytest.mark.parametrize('data_type,values', CASES)
@pytest.mark.parametrize('operation', [
    'row_id', 'incremental', 'predicate_array', 'predicate_callable', 'predicate_scalar',
])
def test_nested_updates_use_core(tmp_path, native, stream, data_type, values, operation):
    table, _ = _table(tmp_path, data_type, native)
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    update = builder.new_update().with_update_type(['value'])
    # Slices retain nonzero offsets in both the top level and nested buffers.
    array = pa.array([values[0]] + values, type=data_type).slice(1)
    data = pa.table({'_ROW_ID': [0, 1, 2, 3], 'value': array})
    with _native_only(native):
        if operation == 'row_id':
            messages = (update.update_by_arrow_with_row_id(data, 7) if stream
                        else update.update_by_arrow_with_row_id(data))
        elif operation == 'incremental':
            writer = update.new_update_by_row_id(7) if stream else update.new_update_by_row_id()
            messages = writer.update_columns(data, ['value'])
        else:
            predicate = PredicateBuilder(table.fields).greater_or_equal('id', 0)
            if operation == 'predicate_callable':
                def assignment(matched):
                    assert matched.column_names == ['id', '_ROW_ID']
                    return pa.array([values[i] for i in matched['id'].to_pylist()], type=data_type)
            elif operation == 'predicate_scalar':
                assignment = values[0]
            else:
                assignment = pa.chunked_array([array.slice(0, 1), array.slice(1)])
            kwargs = {'read_columns': ['id']} if operation == 'predicate_callable' else {}
            if stream:
                messages = update.update_by_predicate(predicate, {'value': assignment}, 7, **kwargs)
            else:
                messages = update.update_by_predicate(predicate, {'value': assignment}, **kwargs)
    _commit(builder, messages, stream)
    _check_rows(table, [values[0]] * 4 if operation == 'predicate_scalar' else values)


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('stream', [False, True])
@pytest.mark.parametrize('data_type,values', CASES)
@pytest.mark.parametrize('row_input', [False, True])
def test_nested_upsert_updates_and_appends(tmp_path, native, stream, data_type, values, row_input):
    table, schema = _table(tmp_path, data_type, native)
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    update = builder.new_update().with_update_type(['value'])
    ids = [0, 0, 1, 2, 3, 4]
    source_values = [None] + values + [values[0]]
    data = pa.table({'id': ids, 'value': source_values,
                     'untouched': [99, 99, 99, 99, 99, 14]}, schema=schema)
    with _native_only(native):
        if row_input:
            rows = [GenericRow([row[name] for name in schema.names], table.fields)
                    for row in data.to_pylist()]
            messages = (update.upsert_by_key(rows, ['id'], 7) if stream
                        else update.upsert_by_key(rows, ['id']))
        else:
            # Column order is unrelated to the schema/updated-column order.
            data = data.select(['untouched', 'value', 'id'])
            messages = (update.upsert_by_arrow_with_key(data, ['id'], 7) if stream
                        else update.upsert_by_arrow_with_key(data, ['id']))
    _commit(builder, messages, stream)
    _check_rows(table, values + [values[0]])


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('input_values,target', [
    (pa.array([{'a': 1.5, 'b': 2.0}, None]), pa.map_(pa.string(), pa.int32())),
    (pa.array([{'a': 1, 'b': None}, None]), pa.map_(pa.string(), pa.int32())),
    (pa.array([{}, None]), pa.map_(pa.string(), pa.int32())),
    (pa.array([[[1.0, 2.5], [3.0, 4.0]], None, []]), pa.map_(pa.int32(), pa.int32())),
    (pa.array([[[1, 2, 3]], None]), pa.map_(pa.int32(), pa.int32())),
    (pa.array([[1.5, None], [], None]), pa.list_(pa.int32())),
    (pa.array([{'a': 1, 'b': 1.5}]), pa.struct([('a', pa.string()), ('b', pa.int32())])),
    (pa.array([{'m': {'a': 1, 'b': None}}, None]),
     pa.struct([('m', pa.map_(pa.string(), pa.int32()))])),
    (pa.array([{'m': [[1, 2]]}]), pa.struct([('m', pa.map_(pa.int32(), pa.int32()))])),
    (pa.array([[{'a': 1, 'b': None}], None]), pa.list_(pa.map_(pa.string(), pa.int32()))),
    (pa.array([[[[1, 2]]]]), pa.list_(pa.map_(pa.int32(), pa.int32()))),
    (pa.array([[('a', 1), ('b', 2)], None], type=pa.map_(pa.string(), pa.int32())),
     pa.struct([('a', pa.int32()), ('b', pa.int32())])),
    (pa.array([[('a', 1.5), ('b', 2.5), ('ignored', float('nan'))], [('a', 3.5)]],
              type=pa.map_(pa.string(), pa.float64())),
     pa.struct([('a', pa.int32()), ('b', pa.int32())])),
    (pa.array([[('b', 1), ('a', 2)]], type=pa.map_(pa.string(), pa.int32())),
     pa.struct([('a', pa.int32()), ('b', pa.int32())])),
    (pa.StructArray.from_arrays([
        pa.DictionaryArray.from_arrays(pa.array([0, 1], type=pa.int8()),
                                       pa.array([1, None], type=pa.int32()))], names=['a']),
     pa.map_(pa.string(), pa.int32())),
])
def test_nested_row_id_coercion_matches_python(tmp_path, native, input_values, target):
    table, _ = _table(tmp_path, target, native)
    builder = table.new_batch_write_builder()
    update = builder.new_update().with_update_type(['value'])
    data = pa.table({'_ROW_ID': list(range(len(input_values))), 'value': input_values})
    before = set(tmp_path.rglob('*.parquet'))
    try:
        expected = TableUpdateByRowId._coerce_column(input_values, target).to_pylist()
    except (ValueError, TypeError, pa.ArrowException):
        with _native_only(native), pytest.raises((ValueError, TypeError, pa.ArrowException)):
            update.update_by_arrow_with_row_id(data)
        assert set(tmp_path.rglob('*.parquet')) == before
        expected = [None] * len(input_values)
    else:
        with _native_only(native):
            messages = update.update_by_arrow_with_row_id(data)
        _commit(builder, messages, False)
    _check_rows(table, expected + [None] * (4 - len(input_values)))


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('operation', ['row_id', 'predicate'])
def test_nested_struct_reordering_uses_operation_cast_rules(tmp_path, native, operation):
    target = pa.struct([('b', pa.int32()), ('a', pa.int32())])
    values = pa.array([{'a': 1, 'b': 2}])
    table, _ = _table(tmp_path, target, native)
    builder = table.new_batch_write_builder()
    update = builder.new_update().with_update_type(['value'])
    predicate = PredicateBuilder(table.fields).equal('id', 0)
    before = set(tmp_path.rglob('*.parquet'))
    with _native_only(native):
        if operation == 'predicate':
            # Safe assignment casts reject reordering; row-ID coercion retries
            # the whole input as dictionaries and can match ROW fields by name.
            with pytest.raises((ValueError, pa.ArrowException)):
                update.update_by_predicate(predicate, {'value': values})
            assert set(tmp_path.rglob('*.parquet')) == before
            expected = [None] * 4
        else:
            messages = update.update_by_arrow_with_row_id(pa.table({'_ROW_ID': [0], 'value': values}))
            _commit(builder, messages, False)
            expected = [{'b': 2, 'a': 1}, None, None, None]
    _check_rows(table, expected)


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('layout', ['list_view', 'large_list_view'])
def test_view_pairs_update_map(tmp_path, native, layout):
    if not hasattr(pa, layout):
        pytest.skip('List view arrays require a newer PyArrow version')
    target = pa.map_(pa.int32(), pa.int32())
    table, _ = _table(tmp_path, target, native)
    values = pa.array([[[99, 99, 99]], [[1, 2], [3, None]], None, []],
                      type=getattr(pa, layout)(pa.list_(pa.int64()))).slice(1)
    builder = table.new_batch_write_builder()
    with _native_only(native):
        messages = builder.new_update().update_by_arrow_with_row_id(
            pa.table({'_ROW_ID': [0, 1, 2], 'value': values}))
    _commit(builder, messages, False)
    _check_rows(table, [[(1, 2), (3, None)], None, [], None])


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('data_type,values', CASES)
@pytest.mark.parametrize('overlap', [False, True])
def test_nested_grouped_updates_abort_overlap(tmp_path, native, data_type, values, overlap):
    table, _ = _table(tmp_path, data_type, native)
    builder = table.new_batch_write_builder()
    update = builder.new_update().with_update_type(['value'])
    data = pa.table({'_ROW_ID': [0, 1, 2, 3], 'value': pa.array(values, type=data_type)})
    before = set(tmp_path.rglob('*.parquet'))
    with _native_only(native):
        if overlap:
            with pytest.raises(ValueError):
                update.update_by_arrow_batches_with_row_id(iter([data, data]))
            assert set(tmp_path.rglob('*.parquet')) == before
        else:
            messages = update.update_by_arrow_batches_with_row_id(iter([data.slice(0, 0), data]))
            _commit(builder, messages, False)
    _check_rows(table, [None] * 4 if overlap else values)


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('key_type', ['string', 'large_string', 'string_view', 'binary',
                                      'large_binary', 'binary_view', 'fixed_binary'])
def test_map_field_name_encodings(tmp_path, native, key_type):
    if key_type == 'fixed_binary':
        key_type = pa.binary(1)
    elif hasattr(pa, key_type):
        key_type = getattr(pa, key_type)()
    else:
        pytest.skip('View arrays require a newer PyArrow version')
    target = pa.struct([('a', pa.int32()), ('b', pa.int32())])
    table, _ = _table(tmp_path, target, native)
    values = pa.array([[('a', 1.5), ('b', 2.5), ('x', float('nan'))], [('a', 3.5)]],
                      type=pa.map_(key_type, pa.float64()))
    if not native:
        try:
            values.take(pa.array([0], type=pa.int32()))
        except pa.ArrowNotImplementedError:
            pytest.skip('This PyArrow version cannot filter MAPs with view keys')
    builder = table.new_batch_write_builder()
    with _native_only(native):
        messages = builder.new_update().update_by_arrow_with_row_id(
            pa.table({'_ROW_ID': [0, 1], 'value': values}))
    _commit(builder, messages, False)
    _check_rows(table, [{'a': 1, 'b': 2}, {'a': 3, 'b': None}, None, None])
