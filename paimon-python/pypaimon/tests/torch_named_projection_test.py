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

"""Map-style Torch datasets preserve named top-level column projections."""

import json
import pickle
from contextlib import contextmanager, ExitStack
from unittest.mock import patch

import pyarrow as pa
import pytest

pytest.importorskip('torch')

from torch.utils.data import DataLoader

from pypaimon import CatalogFactory, Schema
from pypaimon.api.api_response import AuthTableQueryResponse
from pypaimon.catalog.table_query_auth import TableQueryAuthResult
from pypaimon.execution.native.runtime import native_runtime_available
from pypaimon.execution.python.read.table_read import PythonTableRead
from pypaimon.globalindex.indexed_split import IndexedSplit
from pypaimon.read.datasource.torch_dataset import TorchDataset, row_ranges_for_split
from pypaimon.read.table_read import TableRead
from pypaimon.tests import native_plan_rest_test
from pypaimon.utils.range import Range


rest_catalog = native_plan_rest_test.rest_catalog


@pytest.fixture(params=['python', 'native'])
def read_backend(request):
    if request.param == 'native' and not native_runtime_available():
        pytest.skip('pypaimon-rust not installed')
    return request.param


@contextmanager
def _read_with_backend(table, backend):
    with ExitStack() as stack:
        if backend == 'native':
            for method in ('_materialize', 'read_batches', 'read_rows'):
                stack.enter_context(patch.object(
                    PythonTableRead, method, side_effect=AssertionError('Python reader fallback')))
        yield table.copy({'read.native.enabled': str(backend == 'native').lower()})


@pytest.fixture(params=['append', 'pk', 'row_tracking', 'data_evolution'])
def projected_table(tmp_path, request, read_backend):
    table = _create_projected_table(tmp_path, request.param)
    with _read_with_backend(table, read_backend) as table:
        yield table, request.param


def _create_projected_table(tmp_path, mode):
    schema = pa.schema([
        pa.field('id', pa.int64(), nullable=False),
        ('v', pa.int64()),
        ('part', pa.string()),
    ])
    options = {
        'bucket': '1' if mode == 'pk' else '-1',
        'read.native.enabled': 'false',
        'scan.native-plan.enabled': 'false',
        'write.native.enabled': 'false',
        'commit.native.enabled': 'false',
        'read.batch-size': '2',
        'read.parallelism': '1',
    }
    if mode in ('row_tracking', 'data_evolution'):
        options['row-tracking.enabled'] = 'true'
    if mode == 'data_evolution':
        options['data-evolution.enabled'] = 'true'
        options['deletion-vectors.enabled'] = 'true'
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('db', True)
    catalog.create_table('db.t', Schema.from_pyarrow_schema(
        schema, primary_keys=['id', 'part'] if mode == 'pk' else None,
        partition_keys=['part'], options=options), False)
    table = catalog.get_table('db.t')
    rows = [{'id': 101 + i, 'v': 10 * i if i != 2 else None,
             'part': 'a' if i < 3 else 'b'} for i in range(6)]
    writes = [rows]
    if mode == 'pk':
        writes.append([dict(rows[0], v=99)])
    for values in writes:
        builder = table.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        try:
            writer.write_arrow(pa.Table.from_pylist(values, schema=schema))
            commit.commit(writer.prepare_commit())
        finally:
            writer.close()
            commit.close()
    return table


@pytest.mark.parametrize('projection', [
    {'user_id': 'id', 'value': 'v', 'id_copy': 'id'},
    {'value': 'v', 'user_id': 'id', 'id_copy': 'id'},
    {'v': 'id', 'id': 'v', 'id_copy': 'id'},
    {'_ROW_ID': 'id', 'value': 'v', 'id_copy': 'id'},
])
def test_map_style_named_columns(projected_table, projection):
    table, mode = projected_table
    builder = table.new_read_builder().with_projection(projection)
    read, splits = builder.new_read(), builder.new_scan().plan().splits()
    expected = read.to_arrow(splits, parallelism=1).to_pylist()

    dataset = read.to_torch(splits, streaming=False, batch_format='row')

    assert (dataset._data is None) == (mode == 'data_evolution')
    assert len(dataset) == len(expected) == 6
    assert dataset[0] == expected[0]
    assert dataset[-1] == expected[-1]
    assert dataset[-len(dataset)] == expected[0]
    indices = [3, 1, 3, -1, -len(dataset)]
    assert dataset.__getitems__(indices) == [expected[i] for i in indices]
    assert dataset[1:5:2] == expected[1:5:2]
    assert dataset[::-1] == expected[::-1]
    assert dataset[2:2] == []
    assert dataset.__getitems__([]) == []
    assert dataset[:] == expected
    assert all(list(row) == list(projection) for row in dataset[:])
    assert pickle.loads(pickle.dumps(dataset)).__getitems__(indices) == [expected[i] for i in indices]
    for index in (len(dataset), -len(dataset) - 1):
        with pytest.raises(IndexError, match='TorchDataset index out of range'):
            dataset[index]


@pytest.mark.parametrize('projected_table', ['data_evolution'], indirect=True)
def test_lazy_named_columns_preserve_output_projection(projected_table):
    table, _ = projected_table
    builder = table.new_read_builder().with_projection({
        'user_id': 'id', 'value': 'v', 'id_copy': 'id',
    })
    read, splits = builder.new_read(), builder.new_scan().plan().splits()
    expected = read.to_arrow(splits, parallelism=1).to_pylist()
    with patch.object(TableRead, 'to_arrow', side_effect=AssertionError('unexpected materialization')):
        dataset = TorchDataset(read, splits)

    assert dataset._data is None
    with patch.object(TableRead, 'to_arrow', autospec=True, side_effect=TableRead.to_arrow) as fetch:
        assert dataset.__getitems__([3, 1, 3]) == [expected[i] for i in [3, 1, 3]]
    fetch.assert_called_once()
    selected = fetch.call_args[0][1]
    assert all(isinstance(split, IndexedSplit) for split in selected)
    assert sum(row_range.count() for split in selected for row_range in split.row_ranges()) == 2


@pytest.mark.parametrize('projection', [
    {'v': 'id', 'id': 'v', 'id_copy': 'id'},
    {'_ROW_ID': 'id', 'id_copy': 'id'},
])
@pytest.mark.parametrize('limit', [None, 0, 1, 3])
@pytest.mark.parametrize('plan_before_limit', [False, True])
def test_named_columns_filter_and_limit(projected_table, projection, limit, plan_before_limit):
    table, mode = projected_table
    predicate = table.new_read_builder().new_predicate_builder().greater_than('v', 20)
    builder = table.new_read_builder().with_projection(projection).with_filter(predicate)
    splits = builder.new_scan().plan().splits() if plan_before_limit else None
    if limit is not None:
        builder.with_limit(limit)
    if splits is None:
        splits = builder.new_scan().plan().splits()
    read = builder.new_read()
    expected = read.to_arrow(splits, parallelism=1).to_pylist()

    dataset = read.to_torch(splits, streaming=False)

    assert (dataset._data is None) == (mode == 'data_evolution')
    count = 4 if mode == 'pk' else 3
    assert len(dataset) == (count if limit is None else min(count, limit))
    assert dataset.__getitems__(range(len(dataset))) == expected
    assert all(list(row) == list(projection) for row in expected)
    if expected:
        assert dataset.__getitems__([-1, 0, -1]) == [expected[-1], expected[0], expected[-1]]


@pytest.mark.parametrize('limit', [0, 1, 4, 10])
@pytest.mark.parametrize('plan_before_limit', [False, True])
def test_named_columns_limit_without_filter(projected_table, limit, plan_before_limit):
    table, mode = projected_table
    builder = table.new_read_builder().with_projection({'_ROW_ID': 'id', 'id_copy': 'id'})
    splits = builder.new_scan().plan().splits() if plan_before_limit else None
    builder.with_limit(limit)
    if splits is None:
        splits = builder.new_scan().plan().splits()
    read = builder.new_read()
    expected = read.to_arrow(splits, parallelism=1).to_pylist()

    with patch.object(TableRead, 'to_arrow', autospec=True, side_effect=TableRead.to_arrow) as fetch:
        dataset = read.to_torch(splits, streaming=False)
    if mode == 'data_evolution':
        fetch.assert_not_called()
        assert dataset._data is None
    else:
        fetch.assert_called_once()
    assert len(dataset) == min(6, limit)
    assert dataset.__getitems__(range(len(dataset))) == expected


@pytest.mark.parametrize('projected_table', ['row_tracking', 'data_evolution'], indirect=True)
@pytest.mark.parametrize('projection', [
    {'record_id': '_ROW_ID', 'user_id': 'id', 'record_id_copy': '_ROW_ID'},
    {'_ROW_ID': 'id', 'id': '_ROW_ID', 'record_id_copy': '_ROW_ID'},
    {'_ROW_ID': '_ROW_ID', 'user_id': 'id', 'record_id_copy': '_ROW_ID'},
])
def test_named_columns_keep_explicit_row_id(projected_table, projection):
    table, _ = projected_table
    builder = table.new_read_builder().with_projection(projection)
    read, splits = builder.new_read(), builder.new_scan().plan().splits()
    expected = read.to_arrow(splits, parallelism=1).to_pylist()

    dataset = read.to_torch(splits, streaming=False)

    assert dataset[::-1] == expected[::-1]
    assert all(list(row) == list(projection) for row in dataset[:])


def test_named_columns_empty_input(projected_table):
    table, _ = projected_table
    read = table.new_read_builder().with_projection({'user_id': 'id', 'id_copy': 'id'}).new_read()
    dataset = read.to_torch([], streaming=False)

    assert len(dataset) == 0
    assert dataset[0] is None
    assert dataset.__getitems__([]) == []


@pytest.mark.parametrize('projected_table', ['data_evolution'], indirect=True)
def test_lazy_named_columns_respect_deletion_vectors(projected_table):
    table, _ = projected_table
    builder = table.new_batch_write_builder()
    messages = builder.new_update().delete_by_row_id([1])
    commit = builder.new_commit()
    try:
        commit.commit(messages)
    finally:
        commit.close()
    builder = table.new_read_builder().with_projection({'_ROW_ID': 'id', 'id_copy': 'id'})
    read, splits = builder.new_read(), builder.new_scan().plan().splits()
    expected = read.to_arrow(splits, parallelism=1).to_pylist()

    dataset = read.to_torch(splits, streaming=False)

    assert dataset._data is None
    assert isinstance(dataset._row_ids, pa.Array)
    assert len(dataset) == 5
    assert dataset[::-1] == expected[::-1]


@pytest.mark.parametrize('projected_table', ['data_evolution'], indirect=True)
@pytest.mark.parametrize('read_backend', ['python'], indirect=True)
@pytest.mark.parametrize('masked_column', ['id', '_ROW_ID'])
def test_named_columns_respect_masking(projected_table, masked_column):
    table, _ = projected_table
    auth = TableQueryAuthResult(filter=None, column_masking={masked_column: '{"name":"NULL"}'})
    table.catalog_environment.table_query_auth = lambda options, identifier: lambda select: auth
    builder = table.new_read_builder().with_projection({'_ROW_ID': 'id', 'id_copy': 'id'})
    read, splits = builder.new_read(), builder.new_scan().plan().splits()
    expected = read.to_arrow(splits, parallelism=1).to_pylist()

    dataset = read.to_torch(splits, streaming=False)

    assert (dataset._data is None) == (masked_column != '_ROW_ID')
    assert dataset[:] == expected
    if masked_column == 'id':
        assert expected == [{'_ROW_ID': None, 'id_copy': None}] * 6


@pytest.mark.parametrize('projected_table', ['data_evolution'], indirect=True)
@pytest.mark.parametrize('read_backend', ['python'], indirect=True)
def test_lazy_named_columns_with_dataloader_workers(projected_table):
    table, _ = projected_table
    builder = table.new_read_builder().with_projection({'_ROW_ID': 'id', 'id_copy': 'id'})
    splits = builder.new_scan().plan().splits()
    dataset = builder.new_read().to_torch(splits, streaming=False)
    expected = dataset.__getitems__([3, 1, 3])
    loader = DataLoader(dataset, batch_size=3, sampler=[3, 1, 3], num_workers=2,
                        multiprocessing_context='spawn')

    batches = list(loader)

    assert len(batches) == 1
    assert list(batches[0]) == ['_ROW_ID', 'id_copy']
    assert batches[0]['_ROW_ID'].tolist() == [row['_ROW_ID'] for row in expected]
    assert batches[0]['id_copy'].tolist() == [row['id_copy'] for row in expected]


@pytest.mark.parametrize('filtered', [False, True])
@pytest.mark.parametrize('limit', [None, 1, 3])
def test_lazy_named_columns_after_column_update(tmp_path, read_backend, filtered, limit):
    table = _create_projected_table(tmp_path, 'data_evolution')
    builder = table.new_batch_write_builder()
    messages = builder.new_update().with_update_type(['v']).update_by_arrow_with_row_id(
        pa.table({'_ROW_ID': [1, 4], 'v': [111, 444]}))
    commit = builder.new_commit()
    try:
        commit.commit(messages)
    finally:
        commit.close()

    with _read_with_backend(table, read_backend) as table:
        projection = {'_ROW_ID': 'id', 'position': '_ROW_ID', 'value': 'v', 'value_copy': 'v'}
        builder = table.new_read_builder().with_projection(projection)
        if filtered:
            predicate = table.new_read_builder().new_predicate_builder().greater_than('v', 100)
            builder.with_filter(predicate)
        splits = builder.new_scan().plan().splits()
        if limit is not None:
            builder.with_limit(limit)
        read = builder.new_read()
        values = [0, 111, None, 30, 444, 50]
        expected = [dict(zip(projection, [101 + i, i, value, value])) for i, value in enumerate(values)
                    if not filtered or (value is not None and value > 100)][:limit]

        dataset = read.to_torch(splits, streaming=False)

        assert dataset._data is None
        assert dataset[:] == read.to_arrow(splits).to_pylist() == expected
        assert all(list(row) == list(projection) for row in dataset[:])
        assert dataset.__getitems__([-1, 0, -1]) == [expected[-1], expected[0], expected[-1]]


@pytest.mark.parametrize('limit', [None, 0, 1, 3])
@pytest.mark.parametrize('plan_before_limit', [False, True])
@pytest.mark.parametrize('user_filter', [False, True])
@pytest.mark.parametrize('read_backend,mode', [
    # Native authorization rejects the internal row-ID reads needed by lazy datasets.
    ('python', 'append'), ('native', 'append'), ('python', 'data_evolution'),
], indirect=['read_backend'])
def test_named_columns_rest_filter_and_limit(rest_catalog, read_backend, mode, limit, plan_before_limit, user_filter):
    catalog, server = rest_catalog
    schema = pa.schema([('id', pa.int64()), ('v', pa.int64())])
    options = {
        'query-auth.enabled': 'true',
        'read.native.enabled': 'false',
        'scan.native-plan.enabled': 'false', 'write.native.enabled': 'false', 'commit.native.enabled': 'false',
        'read.parallelism': '1', 'source.split.target-size': '1b',
    }
    if mode == 'data_evolution':
        options.update({'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true'})
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, options=options), False)
    table = catalog.get_table('default.t')
    for offset in (0, 3):
        builder = table.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        try:
            writer.write_arrow(pa.Table.from_pylist([
                {'id': 101 + i, 'v': (i + 1) * 10} for i in range(offset, offset + 3)], schema=schema))
            commit.commit(writer.prepare_commit())
        finally:
            writer.close()
            commit.close()
    policy = json.dumps({
        'kind': 'LEAF', 'function': 'GREATER_THAN', 'literals': [20],
        'transform': {'name': 'FIELD_REF', 'fieldRef': {'name': 'v', 'type': 'BIGINT', 'index': 1}},
    })
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(filter=[policy])
    with _read_with_backend(table, read_backend) as table:
        projection = {'_ROW_ID': 'id', 'id_copy': 'id'}
        builder = table.new_read_builder().with_projection(projection)
        if user_filter:
            predicate = table.new_read_builder().new_predicate_builder().greater_than('id', 103)
            builder.with_filter(predicate)
        splits = builder.new_scan().plan().splits() if plan_before_limit else None
        if limit is not None:
            builder.with_limit(limit)
        if splits is None:
            splits = builder.new_scan().plan().splits()
        expected = [{'_ROW_ID': i, 'id_copy': i} for i in range(104 if user_filter else 103, 107)][:limit]
        read = builder.new_read()

        dataset = read.to_torch(splits, streaming=False)

        assert (dataset._data is None) == (mode == 'data_evolution')
        if mode == 'data_evolution' and splits:
            assert isinstance(dataset._row_ids, pa.Array)
        assert len(dataset) == len(expected)
        assert dataset.__getitems__(range(len(dataset))) == read.to_arrow(splits).to_pylist() == expected
        if expected:
            assert dataset.__getitems__([-1, 0, -1]) == [expected[-1], expected[0], expected[-1]]


@pytest.mark.parametrize('projected_table', ['data_evolution'], indirect=True)
@pytest.mark.parametrize('parallelism', [1, 2])
@pytest.mark.parametrize('limit', [None, 1, 3])
def test_lazy_named_columns_row_id_filter(projected_table, parallelism, limit):
    table, _ = projected_table
    table = table.copy({'read.parallelism': str(parallelism)})
    predicate = table.new_read_builder().with_projection(['_ROW_ID']).new_predicate_builder().greater_than('_ROW_ID', 1)
    builder = table.new_read_builder().with_projection({
        '_ROW_ID': 'id', 'position': '_ROW_ID', 'value': 'v', 'value_copy': 'v',
    }).with_filter(predicate)
    if limit is not None:
        builder.with_limit(limit)
    splits = builder.new_scan().plan().splits()

    dataset = builder.new_read().to_torch(splits, streaming=False)

    assert dataset._data is None
    assert len(dataset) == (4 if limit is None else limit)
    rows = dataset[:]
    assert all(row['position'] > 1 and row['_ROW_ID'] == row['position'] + 101 for row in rows)
    assert all(row['value'] == row['value_copy'] for row in rows)
    assert dataset.__getitems__([-1, 0, -1]) == [rows[-1], rows[0], rows[-1]]


@pytest.mark.parametrize('projected_table', ['data_evolution'], indirect=True)
def test_lazy_named_columns_keep_input_indexed_ranges(projected_table):
    table, _ = projected_table
    builder = table.new_read_builder().with_projection({'_ROW_ID': 'id', 'id_copy': 'id'})
    splits = []
    for split in builder.new_scan().plan().splits():
        allowed = Range.and_([Range(1, 1), Range(4, 4)], row_ranges_for_split(split))
        if allowed:
            splits.append(IndexedSplit(split, allowed))

    dataset = builder.new_read().to_torch(splits, streaming=False)

    assert dataset._data is None
    assert len(dataset) == 2
    assert dataset.__getitems__([1, 0, 1]) == [
        {'_ROW_ID': 105, 'id_copy': 105}, {'_ROW_ID': 102, 'id_copy': 102}, {'_ROW_ID': 105, 'id_copy': 105},
    ]
