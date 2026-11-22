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

"""Torch row datasets retain Native Variant projection semantics."""

from types import SimpleNamespace
from unittest.mock import patch

import pyarrow as pa
import pytest

pytest.importorskip('torch')

from torch.utils.data import DataLoader

from pypaimon.execution.native.read.table_read import native_read
from pypaimon.execution.native.runtime import native_runtime_available
from pypaimon.read.datasource.torch_dataset import TorchDataset
from pypaimon.read.table_read import TableRead
from pypaimon.tests import native_variant_iterator_test


pytestmark = pytest.mark.skipif(not native_runtime_available(), reason='pypaimon-rust not installed')
variant_table = native_variant_iterator_test.variant_table
variant_builder = native_variant_iterator_test.variant_builder


@pytest.fixture(params=[
    {}, {'prefetch_concurrency': 2},
    {'shuffle': True, 'seed': 17, 'buffer_size': 1, 'max_buffer_input_splits': 1},
    {'shuffle': True, 'seed': 17, 'buffer_size': 1, 'max_buffer_input_splits': 3},
], ids=['serial', 'prefetch', 'shuffle_ordered', 'shuffle_interleaved'])
def row_options(request):
    return request.param


def _expected_rows():
    names = ['score', 'identifier', 'score_copy', 'good', 'identifier_copy']
    values = [(1.25, 1, 1.25, 2.5, 1), (None, 2, None, 3.5, 2)]
    values += [(None, value, None, None, value) for value in range(3, 7)]
    return [dict(zip(names, row)) for row in values]


def _assert_rows(actual, expected):
    assert sorted(actual, key=lambda row: row['identifier']) == expected
    assert all(list(row) == list(_expected_rows()[0]) for row in actual)
    assert all(row['score'] is None or isinstance(row['score'], float) for row in actual)


def test_streaming_variant_rows(variant_builder, row_options):
    builder, _ = variant_builder
    splits = builder.new_scan().plan().splits()
    assert len(splits) > 1
    dataset = builder.new_read().to_torch(splits, streaming=True, **row_options)
    _assert_rows(list(DataLoader(dataset, batch_size=None, num_workers=0)), _expected_rows())
    if row_options.get('shuffle'):
        assert list(dataset) == list(dataset)


def test_streaming_variant_only_projection(variant_builder, row_options):
    builder, source = variant_builder
    builder.with_projection({'score': "try_variant_get(%s, '$.score', 'float')" % source})
    splits = builder.new_scan().plan().splits()
    actual = list(builder.new_read().to_torch(splits, streaming=True, **row_options))
    expected = [{'score': row['score']} for row in _expected_rows()]
    assert sorted(actual, key=repr) == sorted(expected, key=repr)


@pytest.mark.parametrize('limit', [0, 1, 3, 6])
@pytest.mark.parametrize('plan_before_limit', [False, True])
def test_streaming_variant_limit(variant_builder, row_options, limit, plan_before_limit):
    builder, _ = variant_builder
    splits = builder.new_scan().plan().splits() if plan_before_limit else None
    builder.with_limit(limit)
    if splits is None:
        splits = builder.new_scan().plan().splits()
    dataset = builder.new_read().to_torch(splits, streaming=True, **row_options)
    _assert_rows(list(dataset), _expected_rows()[:limit])


@pytest.mark.parametrize('limit', [None, 2])
def test_streaming_variant_filter(variant_builder, row_options, limit):
    builder, _ = variant_builder
    predicate = builder.table.new_read_builder().new_predicate_builder().greater_than('id', 2)
    builder.with_filter(predicate)
    if limit is not None:
        builder.with_limit(limit)
    splits = builder.new_scan().plan().splits()
    dataset = builder.new_read().to_torch(splits, streaming=True, **row_options)
    _assert_rows(list(dataset), _expected_rows()[2:][:limit])


def test_streaming_variant_empty_input(variant_builder, row_options):
    builder, _ = variant_builder
    assert list(builder.new_read().to_torch([], streaming=True, **row_options)) == []


def test_streaming_variant_strict_cast_errors(variant_builder, row_options):
    builder, source = variant_builder
    builder.with_projection({'score': "variant_get(%s, '$.score', 'float')" % source})
    splits = builder.new_scan().plan().splits()
    dataset = builder.new_read().to_torch(splits, streaming=True, **row_options)
    with pytest.raises(Exception, match='Cannot cast Variant'):
        list(dataset)


@pytest.mark.parametrize('failure', ['disabled', 'missing_runtime', 'setup', 'unsupported_split'])
def test_streaming_variant_requires_native(variant_builder, row_options, failure):
    builder, _ = variant_builder
    splits = builder.new_scan().plan().splits()
    target = 'pypaimon.execution.native.read.table_read.native_read'
    if failure == 'disabled':
        builder.table = builder.table.copy({'read.native.enabled': 'false'})
        options = {'wraps': native_read}
    elif failure == 'missing_runtime':
        target = 'pypaimon.execution.native.runtime.native_runtime_available'
        options = {'return_value': False}
    elif failure == 'setup':
        options = {'side_effect': RuntimeError('Native setup failed')}
    else:
        target = 'pypaimon.execution.native.read.table_read.NativeTableRead._split_files_supported'
        options = {'return_value': False}
    dataset = builder.new_read().to_torch(splits, streaming=True, **row_options)
    with patch(target, **options):
        with pytest.raises(RuntimeError, match='Variant extraction requires a compatible native reader'):
            list(dataset)


@pytest.mark.parametrize('limit', [None, 0, 1, 6])
def test_streaming_variant_rank_worker_sharding(variant_builder, row_options, limit):
    builder, _ = variant_builder
    if limit is not None:
        builder.with_limit(limit)
    read, splits = builder.new_read(), builder.new_scan().plan().splits()
    actual = []
    for rank in range(2):
        dataset = read.to_torch(splits, streaming=True, sharding_rank=rank, sharding_world_size=2,
                                **row_options)
        for worker in range(2):
            with patch('torch.utils.data.get_worker_info', return_value=SimpleNamespace(id=worker, num_workers=2)):
                if limit == 1:
                    with pytest.raises(ValueError, match='limit is not supported with distributed Torch sharding'):
                        list(dataset)
                else:
                    actual.extend(dataset)
    if limit != 1:
        _assert_rows(actual, _expected_rows()[:limit])


def test_streaming_variant_dataloader_workers(variant_builder):
    builder, _ = variant_builder
    splits = builder.new_scan().plan().splits()
    dataset = builder.new_read().to_torch(splits, streaming=True)
    loader = DataLoader(dataset, batch_size=None, num_workers=2, multiprocessing_context='spawn')
    _assert_rows(list(loader), _expected_rows())


@pytest.mark.parametrize('stop', ['unstarted', 'close', 'exhaust', 'failure'])
@pytest.mark.parametrize('limit', [None, 3])
def test_streaming_variant_closes_native_streams(variant_builder, row_options, stop, limit):
    builder, _ = variant_builder
    if limit is not None:
        builder.with_limit(limit)
    splits = builder.new_scan().plan().splits()
    opened, closed = [], []
    failure = OSError('Native stream failed')

    def tracked_native(*args, **kwargs):
        batches = native_read(*args, **kwargs)
        token = object()
        opened.append(token)

        def tracked_batches():
            try:
                for batch in batches:
                    yield batch
                    if stop == 'failure':
                        raise failure
            finally:
                batches.close()
                closed.append(token)
        return tracked_batches()

    with patch('pypaimon.execution.native.read.table_read.native_read', side_effect=tracked_native):
        dataset = builder.new_read().to_torch(splits, streaming=True, **row_options)
        rows = iter(dataset)
        try:
            if stop == 'failure':
                with pytest.raises(OSError) as error:
                    list(rows)
                assert error.value is failure
            elif stop == 'exhaust':
                _assert_rows(list(rows), _expected_rows()[:limit])
            elif stop == 'close':
                assert next(rows) in _expected_rows()[:limit]
        finally:
            rows.close()
    assert set(opened) == set(closed)
    assert len(opened) == len(closed)
    assert bool(opened) == (stop != 'unstarted')


def _assert_map_indexing(read, splits):
    arrow = read.to_arrow(splits, parallelism=1)
    expected = arrow.to_pylist()
    with patch.object(TableRead, 'to_arrow', autospec=True, side_effect=TableRead.to_arrow) as fetch, \
            patch.object(TorchDataset, '_compact_row_id_index', side_effect=AssertionError('Lazy row-ID read')):
        dataset = read.to_torch(splits, streaming=False, batch_format='row')
    fetch.assert_called_once_with(read, splits)
    assert dataset._data is not None and dataset._row_ids is None
    assert len(dataset) == len(expected)
    assert dataset._data.column_names == [alias for alias, _ in read.output_projection.columns]
    assert dataset._data.schema == arrow.schema
    with patch.object(TableRead, 'to_arrow', side_effect=AssertionError('Read after materialization')):
        assert dataset.__getitems__([]) == []
        if expected:
            indices = [len(dataset) - 1, 0, -1, 0, -len(dataset)]
            assert dataset[0] == expected[0]
            assert dataset[-1] == expected[-1]
            assert list(dataset[0]) == list(expected[0])
            assert dataset[1::2] == expected[1::2]
            assert dataset[::-1] == expected[::-1]
            assert dataset[2:2] == []
            assert dataset.__getitems__(indices) == [expected[i] for i in indices]
            assert list(DataLoader(dataset, batch_sampler=[indices], collate_fn=list)) == [
                [expected[i] for i in indices]]
            for index in (len(dataset), -len(dataset) - 1):
                with pytest.raises(IndexError, match='TorchDataset index out of range'):
                    dataset[index]
                with pytest.raises(IndexError, match='TorchDataset index out of range'):
                    dataset.__getitems__([0, index, 0])
        else:
            assert dataset[0] is None
            with pytest.raises(IndexError, match='TorchDataset index out of range'):
                dataset.__getitems__([0])
    return expected


def test_map_variant_rows(variant_builder):
    builder, _ = variant_builder
    builder.table = builder.table.copy({'read.parallelism': '1'})
    read, splits = builder.new_read(), builder.new_scan().plan().splits()
    assert read._output_arrow_schema().field('score').type == pa.float32()
    _assert_rows(_assert_map_indexing(read, splits), _expected_rows())


@pytest.mark.parametrize('aliases', [False, True])
def test_map_variant_only_and_aliases(variant_builder, aliases):
    builder, source = variant_builder
    expression = "try_variant_get(%s, '$.score', 'float')" % source
    projection = ({'id': expression, '_ROW_ID': 'id', 'payload': 'id', 'again': expression}
                  if aliases else {'score': expression})
    builder.with_projection(projection)
    builder.table = builder.table.copy({'read.parallelism': '1'})
    read, splits = builder.new_read(), builder.new_scan().plan().splits()
    actual = _assert_map_indexing(read, splits)
    expected = [dict(zip(projection, [row['score'], row['identifier'], row['identifier'], row['score']]))
                if aliases else {'score': row['score']} for row in _expected_rows()]
    assert sorted(actual, key=repr) == sorted(expected, key=repr)


def test_map_variant_mixed_nested_paths(variant_builder):
    builder, source = variant_builder
    projection = {
        'city': 'profile.city', 'source': "attributes['source']",
        'score': "try_variant_get(%s, '$.score', 'float')" % source,
        'user_id': 'id', 'score_copy': "try_variant_get(%s, '$.score', 'float')" % source,
    }
    builder.with_projection(projection)
    builder.table = builder.table.copy({'read.parallelism': '1'})
    read, splits = builder.new_read(), builder.new_scan().plan().splits()
    actual = _assert_map_indexing(read, splits)
    cities = ['Hangzhou', 'Beijing', None, 'Shenzhen', 'Suzhou', None]
    sources = ['web', None, None, None, None, 'app']
    expected = [dict(zip(projection, [city, value, row['score'], row['identifier'], row['score']]))
                for city, value, row in zip(cities, sources, _expected_rows())]
    assert sorted(actual, key=lambda row: row['user_id']) == expected


@pytest.mark.parametrize('limit', [0, 1, 3])
@pytest.mark.parametrize('filtered', [False, True])
def test_map_variant_filter_and_limit(variant_builder, limit, filtered):
    builder, _ = variant_builder
    builder.table = builder.table.copy({'read.parallelism': '1'})
    if filtered:
        builder.with_filter(builder.table.new_read_builder().new_predicate_builder().greater_than('id', 2))
    splits = builder.new_scan().plan().splits()
    builder.with_limit(limit)
    actual = _assert_map_indexing(builder.new_read(), splits)
    _assert_rows(actual, (_expected_rows()[2:] if filtered else _expected_rows())[:limit])


def test_map_variant_empty_input(variant_builder):
    builder, _ = variant_builder
    assert _assert_map_indexing(builder.new_read(), []) == []


def test_map_variant_strict_cast_errors(variant_builder):
    builder, source = variant_builder
    builder.with_projection({'score': "variant_get(%s, '$.score', 'float')" % source})
    read, splits = builder.new_read(), builder.new_scan().plan().splits()
    with pytest.raises(Exception, match='Cannot cast Variant'):
        read.to_arrow(splits, parallelism=1)
    with pytest.raises(Exception, match='Cannot cast Variant'):
        read.to_torch(splits, streaming=False, batch_format='row')


@pytest.mark.parametrize('failure', ['disabled', 'missing_runtime', 'setup', 'unsupported_split'])
def test_map_variant_requires_native(variant_builder, failure):
    builder, _ = variant_builder
    splits = builder.new_scan().plan().splits()
    target = 'pypaimon.execution.native.read.table_read.native_read'
    if failure == 'disabled':
        builder.table = builder.table.copy({'read.native.enabled': 'false'})
        options = {'wraps': native_read}
    elif failure == 'missing_runtime':
        target = 'pypaimon.execution.native.runtime.native_runtime_available'
        options = {'return_value': False}
    elif failure == 'setup':
        options = {'side_effect': RuntimeError('Native setup failed')}
    else:
        target = 'pypaimon.execution.native.read.table_read.NativeTableRead._split_files_supported'
        options = {'return_value': False}
    with patch(target, **options):
        with pytest.raises(RuntimeError, match='Variant extraction requires a compatible native reader'):
            builder.new_read().to_torch(splits, streaming=False, batch_format='row')


@pytest.mark.parametrize('batch_format', ['pyarrow', 'torch'])
def test_map_variant_batch_formats_remain_unsupported(variant_builder, batch_format):
    builder, _ = variant_builder
    with pytest.raises(ValueError, match='requires streaming=True'):
        builder.new_read().to_torch([], streaming=False, batch_format=batch_format)
