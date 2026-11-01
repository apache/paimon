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

from contextlib import ExitStack
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.catalog.table_query_auth import TableQueryAuthResult
from pypaimon.execution.native.read.table_read import NativeTableRead
from pypaimon.execution.python.read.table_read import PythonTableRead
from pypaimon.schema.arrow_schema import read_arrow_schema
from pypaimon.schema.data_types import AtomicType, DataField, RowType
from pypaimon.table.row.internal_row import InternalRow
from pypaimon.table.row.row_kind import RowKind


pytestmark = pytest.mark.python_read


@pytest.fixture(params=['append', 'pk'])
def projected_table(tmp_path, request):
    schema = pa.schema([
        pa.field('id', pa.int64(), nullable=False),
        ('v', pa.int64()),
        ('profile', pa.struct([('city', pa.string())])),
        ('attributes', pa.map_(pa.string(), pa.string())),
    ])
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('db', True)
    catalog.create_table('db.t', Schema.from_pyarrow_schema(
        schema, primary_keys=['id'] if request.param == 'pk' else None,
        options={'bucket': '1' if request.param == 'pk' else '-1',
                 'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
                 'write.native.enabled': 'false', 'read.batch-size': '2'}), False)
    table = catalog.get_table('db.t')
    rows = [
        {'id': 1, 'v': 10, 'profile': {'city': 'Hangzhou'},
         'attributes': [('source', 'web'), ('source', 'mobile')]},
        {'id': 2, 'v': 20, 'profile': None,
         'attributes': [('source', None), ('source', 'later')]},
        {'id': 3, 'v': 30, 'profile': {'city': None}, 'attributes': [('other', 'value')]},
        {'id': 4, 'v': 40, 'profile': {'city': 'Shenzhen'}, 'attributes': None},
        {'id': 5, 'v': 50, 'profile': {'city': 'Beijing'}, 'attributes': []},
    ]
    writes = [rows]
    expected = [(1, 'Hangzhou', 'web', 10), (2, None, None, 20),
                (3, None, None, 30), (4, 'Shenzhen', None, 40), (5, 'Beijing', None, 50)]
    if request.param == 'pk':
        writes.append([dict(rows[0], v=11, profile={'city': 'Suzhou'},
                            attributes=[('source', 'api'), ('source', 'later')])])
        expected[0] = (1, 'Suzhou', 'api', 11)
    for values in writes:
        builder = table.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        try:
            writer.write_arrow(pa.Table.from_pylist(values, schema=schema))
            commit.commit(writer.prepare_commit())
        finally:
            writer.close()
            commit.close()
    return table, expected


def _rows(read, splits):
    iterator = read.to_iterator(splits)
    try:
        result = []
        for row in iterator:
            assert isinstance(row, InternalRow)
            assert row.get_row_kind() == RowKind.INSERT
            result.append(tuple(row.get_field(i) for i in range(len(row))))
        return result
    finally:
        iterator.close()


@pytest.mark.parametrize('projection,positions', [
    ({'user_id': 'id'}, [0]),
    ({'city': 'profile.city'}, [1]),
    ({'source': "attributes['source']"}, [2]),
    ({'user_id': 'id', 'city': 'profile.city', 'source': "attributes['source']"}, [0, 1, 2]),
    ({'city': 'profile.city', 'source': "attributes['source']", 'user_id': 'id',
      'city_again': 'profile.city', 'id_again': 'id', 'source_again': "attributes['source']"},
     [1, 2, 0, 1, 0, 2]),
])
def test_named_iterator_matches_arrow(projected_table, projection, positions):
    table, values = projected_table
    builder = table.new_read_builder().with_projection(projection)
    read, splits = builder.new_read(), builder.new_scan().plan().splits()
    expected = [tuple(row[i] for i in positions) for row in values]
    actual = _rows(read, splits)
    assert sorted(actual, key=repr) == sorted(expected, key=repr)
    batches = read.to_arrow_batch_reader(splits, parallelism=1)
    try:
        streaming = batches.read_all()
    finally:
        batches.close()
    for arrow in (read.to_arrow(splits, parallelism=1), streaming):
        assert arrow.column_names == list(projection)
        assert sorted(zip(*(column.to_pylist() for column in arrow.columns)), key=repr) == sorted(actual, key=repr)


def test_named_iterator_keeps_filter_source_binding(projected_table):
    table, _ = projected_table
    predicate = table.new_read_builder().new_predicate_builder().greater_than('v', 20)
    builder = table.new_read_builder().with_projection({
        'v': 'id', 'id': 'v', 'city': 'profile.city',
    }).with_filter(predicate)
    assert sorted(_rows(builder.new_read(), builder.new_scan().plan().splits())) == [
        (3, 30, None), (4, 40, 'Shenzhen'), (5, 50, 'Beijing')]


@pytest.mark.parametrize('limit', [0, 1, 3, None])
def test_named_iterator_limit_and_empty_input(projected_table, limit):
    table, values = projected_table
    builder = table.new_read_builder().with_projection({
        'user_id': 'id', 'city': 'profile.city', 'source': "attributes['source']",
    })
    if limit is not None:
        builder.with_limit(limit)
    read, splits = builder.new_read(), builder.new_scan().plan().splits()
    actual = _rows(read, splits)
    assert len(actual) == (len(values) if limit is None else limit)
    assert all(row in [value[:3] for value in values] for row in actual)
    assert _rows(read, []) == []


@pytest.mark.parametrize('consume', [False, True])
def test_named_iterator_is_lazy_and_closes_reader(projected_table, consume):
    table, _ = projected_table
    builder = table.new_read_builder().with_projection({'city': 'profile.city'})
    read, splits = builder.new_read(), builder.new_scan().plan().splits()
    create = PythonTableRead._create_split_read
    readers = []

    def create_split_read(*args, **kwargs):
        split_read = create(*args, **kwargs)
        create_reader = split_read.create_reader

        def tracked_reader():
            reader = create_reader()
            readers.append(stack.enter_context(patch.object(reader, 'close', wraps=reader.close)))
            return reader

        split_read.create_reader = tracked_reader
        return split_read

    with ExitStack() as stack, patch.object(
            PythonTableRead, '_create_split_read', autospec=True, side_effect=create_split_read):
        iterator = read.to_iterator(splits)
        assert readers == []
        try:
            if consume:
                assert isinstance(next(iterator), InternalRow)
                assert len(readers) == 1
                readers[0].assert_not_called()
        finally:
            iterator.close()
        if consume:
            readers[0].assert_called_once_with()
        else:
            assert readers == []


@pytest.mark.parametrize('stop', ['exhaust', 'close', 'limit', 'zero_limit'])
def test_named_iterator_adapts_native_batches(projected_table, stop):
    table, _ = projected_table
    builder = table.copy({'read.native.enabled': 'true'}).new_read_builder().with_projection({
        'city': 'profile.city', 'key': 'id', 'source': "attributes['source']",
        'copy': 'profile.city', 'rowkind': 'id',
    })
    if stop in ('limit', 'zero_limit'):
        builder.with_limit(1 if stop == 'limit' else 0)
    read, splits = builder.new_read(), builder.new_scan().plan().splits()
    schema = pa.schema([pa.field('rowkind', pa.string())] + list(read_arrow_schema(read.read_type)))
    batch = pa.RecordBatch.from_pylist([
        {'rowkind': '+I', 'id': 1, 'profile': {'city': 'Hangzhou'}, 'attributes': {'source': 'web'}},
        {'rowkind': '-U', 'id': 2, 'profile': None, 'attributes': {'source': None}},
        {'rowkind': '+U', 'id': 3, 'profile': {'city': None}, 'attributes': None},
        {'rowkind': '-D', 'id': 4, 'profile': {'city': 'Beijing'}, 'attributes': {'source': 'api'}},
    ], schema=schema)
    closed = []

    def batches():
        try:
            # Core applies LIMIT before returning batches to Python.
            yield batch.slice(0, 1) if stop == 'limit' else batch
        finally:
            closed.append(True)

    def converted(native_read, splits, output_schema, **kwargs):
        assert native_read._read.include_row_kind
        assert kwargs == {'parallelism': 1, 'ordered': True}
        return native_read.convert_batches(batches(), output_schema)

    with patch.object(NativeTableRead, 'read_batches', autospec=True, side_effect=converted) as native, \
            patch.object(PythonTableRead, '_create_split_read', side_effect=AssertionError('Python fallback')):
        iterator = read.to_iterator(splits)
        native.assert_not_called()
        try:
            if stop == 'close':
                row = next(iterator)
                actual = [(tuple(row.get_field(i) for i in range(len(row))), row.get_row_kind())]
                assert closed == []
            else:
                actual = [(tuple(row.get_field(i) for i in range(len(row))), row.get_row_kind())
                          for row in iterator]
        finally:
            iterator.close()
    expected = [
        (('Hangzhou', 1, 'web', 'Hangzhou', 1), RowKind.INSERT),
        ((None, 2, None, None, 2), RowKind.UPDATE_BEFORE),
        ((None, 3, None, None, 3), RowKind.UPDATE_AFTER),
        (('Beijing', 4, 'api', 'Beijing', 4), RowKind.DELETE),
    ]
    assert actual == ([] if stop == 'zero_limit' else expected if stop == 'exhaust' else expected[:1])
    assert closed == ([] if stop == 'zero_limit' else [True])
    assert read.include_row_kind is False


def test_named_iterator_keeps_map_key_authorization_boundary(projected_table):
    table, _ = projected_table
    auth = TableQueryAuthResult(filter=None, column_masking={'attributes': '{"name":"NULL"}'})
    table.catalog_environment.table_query_auth = lambda options, identifier: lambda select: auth
    builder = table.new_read_builder().with_projection({'source': "attributes['source']"})
    splits = builder.new_scan().plan().splits()
    with pytest.raises(NotImplementedError, match='with query authorization'):
        _rows(builder.new_read(), splits)


@pytest.mark.parametrize('projection', [
    {'user_id': 'id'}, {'city': 'profile.city'}, {'source': "attributes['source']"},
])
def test_named_projection_keeps_non_streaming_torch_row_boundary(projected_table, projection):
    table, _ = projected_table
    read = table.new_read_builder().with_projection(projection).new_read()
    with pytest.raises(RuntimeError, match='not supported by Torch row format'):
        read.to_torch([], streaming=False, batch_format='row')


@pytest.mark.parametrize('function', ['variant_get', 'try_variant_get'])
@pytest.mark.parametrize('nested', [False, True])
@pytest.mark.parametrize('mixed', [False, True])
def test_variant_iterator_and_torch_rows_require_native(tmp_path, function, nested, mixed):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('db', True)
    payload = DataField(2 if nested else 1, 'payload', AtomicType('VARIANT'))
    fields = [DataField(0, 'id', AtomicType('BIGINT')),
              DataField(1, 'profile', RowType(True, [payload])) if nested else payload]
    catalog.create_table('db.t', Schema(fields, options={
        'bucket': '-1', 'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false'}), False)
    source = 'profile.payload' if nested else 'payload'
    projection = {'x': "%s(%s, '$.x', 'float')" % (function, source)}
    if mixed:
        projection['user_id'] = 'id'
    read = catalog.get_table('db.t').new_read_builder().with_projection(projection).new_read()
    with patch.object(NativeTableRead, 'read_rows') as native, \
            patch.object(PythonTableRead, 'read_rows', side_effect=AssertionError('Python Variant execution')):
        with pytest.raises(RuntimeError, match='Variant extraction requires a compatible native reader'):
            list(read.to_iterator([]))
        with pytest.raises(RuntimeError, match='not supported by Torch row format'):
            read.to_torch([], streaming=False, batch_format='row')
        native.assert_not_called()

        pytest.importorskip('torch')
        dataset = read.to_torch([], streaming=True, batch_format='row')
        with pytest.raises(RuntimeError, match='Variant extraction requires a compatible native reader'):
            list(dataset)
        native.assert_not_called()
