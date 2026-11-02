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

"""Streaming Torch rows retain Native Variant projection semantics."""

from types import SimpleNamespace
from unittest.mock import patch

import pytest

pytest.importorskip('torch')

from torch.utils.data import DataLoader

from pypaimon.execution.native.read.table_read import native_read
from pypaimon.execution.native.runtime import native_runtime_available
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


def test_non_streaming_variant_rows_remain_unsupported(variant_builder):
    builder, _ = variant_builder
    splits = builder.new_scan().plan().splits()
    with pytest.raises(RuntimeError, match='VARIANT expressions are not supported by Torch row format'):
        builder.new_read().to_torch(splits)
