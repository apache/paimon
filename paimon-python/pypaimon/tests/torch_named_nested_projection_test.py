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

"""Map-style named paths retain Arrow values and materialized indexing."""

from unittest.mock import patch

import pyarrow as pa
import pytest

pytest.importorskip('torch')

from torch.utils.data import DataLoader

from pypaimon import CatalogFactory, Schema
from pypaimon.catalog.table_query_auth import TableQueryAuthResult
from pypaimon.read.datasource.torch_dataset import TorchDataset
from pypaimon.read.table_read import TableRead
from pypaimon.tests import torch_named_projection_test


read_backend = torch_named_projection_test.read_backend


def _create_table(tmp_path, mode, file_format='parquet', single_child=False):
    children = [('city', pa.string())]
    if not single_child:
        children.append(('unused', pa.string()))
    schema = pa.schema([
        pa.field('id', pa.int64(), nullable=False),
        ('profile', pa.struct(children)),
        ('attributes', pa.map_(pa.string(), pa.string())),
    ])
    options = {
        'bucket': '1' if mode == 'pk' else '-1', 'file.format': file_format,
        'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
        'write.native.enabled': 'false', 'commit.native.enabled': 'false',
        'read.parallelism': '1', 'read.batch-size': '2',
        'source.split.target-size': '1b',
    }
    if mode == 'data_evolution':
        options.update({'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true'})
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('db', True)
    catalog.create_table('db.t', Schema.from_pyarrow_schema(
        schema, primary_keys=['id'] if mode == 'pk' else None, options=options), False)
    table = catalog.get_table('db.t')
    rows = [
        {'id': 1, 'profile': {'city': 'Hangzhou'},
         'attributes': [('source', 'web'), ('source', 'mobile'), ('a;b', 'literal')]},
        {'id': 2, 'profile': None, 'attributes': [('source', None), ('source', 'later')]},
        {'id': 3, 'profile': {'city': None}, 'attributes': [('other', 'value')]},
        {'id': 4, 'profile': {'city': 'Shenzhen'}, 'attributes': None},
        {'id': 5, 'profile': {'city': 'Beijing'}, 'attributes': []},
    ]
    writes = [rows[:2], rows[2:]]
    if mode == 'pk':
        writes.append([dict(rows[0], profile={'city': 'Suzhou'},
                            attributes=[('source', 'api'), ('source', 'later')])])
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


@pytest.fixture(params=[('append', 'parquet'), ('append', 'orc'), ('pk', 'parquet')])
def projected_table(tmp_path, request, read_backend):
    mode, file_format = request.param
    table = _create_table(tmp_path, mode, file_format)
    with torch_named_projection_test._read_with_backend(table, read_backend) as table:
        yield table, mode


def _assert_materialized_indexing(read, splits):
    expected = read.to_arrow(splits, parallelism=1).to_pylist()
    with patch.object(TableRead, 'to_arrow', autospec=True, side_effect=TableRead.to_arrow) as fetch:
        dataset = read.to_torch(splits, streaming=False, batch_format='row')
    fetch.assert_called_once()
    assert dataset._data is not None and dataset._row_ids is None
    assert len(dataset) == len(expected) == 5
    assert all(list(row) == [alias for alias, _ in read.output_projection.columns] for row in expected)
    indices = [3, 1, 3, -1, -len(dataset)]
    with patch.object(TableRead, 'to_arrow', side_effect=AssertionError('Read after materialization')):
        assert dataset[0] == expected[0]
        assert dataset[-1] == expected[-1]
        assert dataset[1:5:2] == expected[1:5:2]
        assert dataset[::-1] == expected[::-1]
        assert dataset[2:2] == []
        assert dataset.__getitems__([]) == []
        assert dataset.__getitems__(indices) == [expected[i] for i in indices]
        assert list(DataLoader(dataset, batch_sampler=[indices], collate_fn=list)) == [
            [expected[i] for i in indices]]
    for index in (len(dataset), -len(dataset) - 1):
        with pytest.raises(IndexError, match='TorchDataset index out of range'):
            dataset[index]
    return expected


@pytest.mark.parametrize('projection,positions', [
    ({'city': 'profile.city'}, [1]),
    ({'source': "attributes['source']"}, [2]),
    ({'user_id': 'id', 'city': 'profile.city', 'source': "attributes['source']"}, [0, 1, 2]),
    ({'source': "attributes['source']", 'city': 'profile.city', 'user_id': 'id',
      'city_again': 'profile.city', 'source_again': "attributes['source']", 'id_again': 'id'},
     [2, 1, 0, 1, 2, 0]),
    ({'id': 'profile.city', '_ROW_ID': 'id', 'profile': "attributes['source']"}, [1, 0, 2]),
])
def test_named_paths_match_arrow(projected_table, projection, positions):
    table, mode = projected_table
    builder = table.new_read_builder().with_projection(projection)
    actual = _assert_materialized_indexing(builder.new_read(), builder.new_scan().plan().splits())
    values = [(1, 'Suzhou', 'api') if mode == 'pk' else (1, 'Hangzhou', 'web'),
              (2, None, None), (3, None, None), (4, 'Shenzhen', None), (5, 'Beijing', None)]
    expected = [dict(zip(projection, (row[i] for i in positions))) for row in values]
    assert sorted(actual, key=repr) == sorted(expected, key=repr)


@pytest.mark.parametrize('projection,single_child', [
    ({'source': "attributes['source']", 'user_id': 'id'}, False),
    ({'city': 'profile.city', 'city_again': 'profile.city'}, True),
    ({'whole': 'profile', 'city': 'profile.city', 'user_id': 'id'}, False),
    ({'whole': 'attributes', 'source': "attributes['source']"}, False),
    ({'value': "attributes['a;b']", 'user_id': 'id'}, False),
])
def test_data_evolution_named_paths_materialize(tmp_path, read_backend, projection, single_child):
    table = _create_table(tmp_path, 'data_evolution', single_child=single_child)
    with torch_named_projection_test._read_with_backend(table, read_backend) as table:
        builder = table.new_read_builder().with_projection(projection)
        read, splits = builder.new_read(), builder.new_scan().plan().splits()
        _assert_materialized_indexing(read, splits)
        # Exercise routing independently of the public entry point's validation.
        dataset = TorchDataset(read, splits)
        assert dataset._data is not None and dataset._row_ids is None


def test_data_evolution_partial_row_keeps_arrow_boundary(tmp_path, read_backend):
    table = _create_table(tmp_path, 'data_evolution')
    with torch_named_projection_test._read_with_backend(table, read_backend) as table:
        builder = table.new_read_builder().with_projection({
            'user_id': 'id', 'city': 'profile.city', 'source': "attributes['source']",
        })
        read, splits = builder.new_read(), builder.new_scan().plan().splits()
        if read_backend == 'python':
            message = 'ROW nested-field projection on data-evolution tables is not yet supported'
            with pytest.raises(NotImplementedError, match=message):
                read.to_arrow(splits)
            with pytest.raises(NotImplementedError, match=message):
                read.to_torch(splits, streaming=False)
        else:
            _assert_materialized_indexing(read, splits)


@pytest.mark.parametrize('limit', [None, 0, 1, 3])
@pytest.mark.parametrize('plan_before_limit', [False, True])
def test_named_paths_filter_and_limit(projected_table, limit, plan_before_limit):
    table, _ = projected_table
    predicate = table.new_read_builder().new_predicate_builder().greater_than('id', 2)
    builder = table.new_read_builder().with_projection({
        'id': 'profile.city', 'source': "attributes['source']",
    }).with_filter(predicate)
    splits = builder.new_scan().plan().splits() if plan_before_limit else None
    if limit is not None:
        builder.with_limit(limit)
    if splits is None:
        splits = builder.new_scan().plan().splits()
    read = builder.new_read()
    expected = read.to_arrow(splits, parallelism=1).to_pylist()
    dataset = read.to_torch(splits, streaming=False)
    assert dataset._data is not None and dataset._row_ids is None
    assert len(dataset) == (3 if limit is None else limit)
    assert dataset.__getitems__(range(len(dataset))) == expected
    if expected:
        assert dataset.__getitems__([-1, 0, -1]) == [expected[-1], expected[0], expected[-1]]


@pytest.mark.parametrize('read_backend', ['python'], indirect=True)
def test_named_map_keeps_authorization_boundary(projected_table):
    table, _ = projected_table
    auth = TableQueryAuthResult(filter=None, column_masking={'attributes': '{"name":"NULL"}'})
    with patch.object(table.catalog_environment, 'table_query_auth', return_value=lambda select: auth):
        builder = table.new_read_builder().with_projection({'source': "attributes['source']"})
        read, splits = builder.new_read(), builder.new_scan().plan().splits()
        with pytest.raises(NotImplementedError, match='with query authorization'):
            read.to_arrow(splits)
        with pytest.raises(NotImplementedError, match='with query authorization'):
            read.to_torch(splits, streaming=False)
