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

"""Native input execution is chosen before data and remains native on workers."""

import pickle
import sys
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.execution.native.write.input_routing import NativeInputRouter
from pypaimon.execution.native.write.postpone_bucket import NativePostponeBucketPlanner
from pypaimon.write.backend_dispatch import create_input_router, create_postpone_bucket_planner
from pypaimon.write.bucket_plan import PostponeBucketPlan
from pypaimon.write.bucket_planning import PostponeBucketPlanning
from pypaimon.write.input_routing import FixedBucketInputRouter


def _table(tmp_path, bucket='4', partition='p', native=True):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        pa.schema([('id', pa.int64()), (partition, pa.string()), ('value', pa.string())]),
        primary_keys=['id', partition], partition_keys=[partition], options={
            'bucket': bucket, 'write.native.enabled': str(native).lower(),
            'postpone.target-row-num-per-bucket': '2',
            'postpone.batch-write-fixed-bucket.max-parallelism': '8',
        }), False)
    return catalog.get_table('default.t')


@pytest.fixture
def sdk(monkeypatch):
    module = SimpleNamespace(BucketRouter=Mock(), PostponeBucketPlanner=Mock())
    monkeypatch.setitem(sys.modules, 'pypaimon_rust.datafusion', module)
    return module


@pytest.mark.parametrize('native', [False, True])
def test_bucket_router_selection_finishes_before_input(tmp_path, native):
    table = _table(tmp_path, native=native)
    with patch('pypaimon.execution.native.write.input_routing.create_native_input_router',
               side_effect=RuntimeError('unsupported table environment')) as prepare:
        router = create_input_router(table)
    assert isinstance(router, FixedBucketInputRouter)
    assert prepare.call_count == int(native)


@pytest.mark.parametrize('bucket', ['4', '-2'])
@pytest.mark.parametrize('chunked', [False, True])
def test_python_router_returns_arrow_buckets_for_the_shared_route_interface(tmp_path, bucket, chunked):
    table = _table(tmp_path, bucket=bucket, native=False)
    plan = PostponeBucketPlan({('west',): 3, ('east',): 2}) if bucket == '-2' else None
    router = create_input_router(table, plan)
    batch = pa.record_batch([['west', 'east', 'west'], [1, 2, 3]], names=['p', 'id'])
    expected = router.extract_partition_bucket_batch(batch)[1]
    data = pa.Table.from_batches([batch.slice(0, 1), batch.slice(1)]) if chunked else batch
    buckets = router.route(data)
    assert isinstance(buckets, pa.Int32Array)
    assert buckets.to_pylist() == expected
    assert buckets.null_count == 0
    empty = router.route(data.slice(0, 0))
    assert isinstance(empty, pa.Int32Array)
    assert len(empty) == 0


def test_native_router_passes_arrow_without_python_row_hashing(tmp_path, sdk):
    table = _table(tmp_path)
    batch = pa.record_batch([['west', 'east'], [1, 2]], names=['p', 'id'])
    buckets = pa.array([2, 1], type=pa.int32())
    sdk.BucketRouter.return_value.route.return_value = buckets
    native_table = Mock()
    with patch('pypaimon.execution.native.write.input_routing.native_runtime_available', return_value=True), \
            patch('pypaimon.execution.native.write.input_routing.create_native_write_table',
                  return_value=native_table), \
            patch.object(FixedBucketInputRouter, '_binary_row_hash_code',
                         side_effect=AssertionError('Python input hashing')):
        router = create_input_router(table)
        assert isinstance(router, NativeInputRouter)
        assert router.route(batch) is buckets
    sdk.BucketRouter.assert_called_once_with(native_table.schema.return_value)
    sdk.BucketRouter.return_value.route.assert_called_once_with(batch)


def test_native_router_failure_propagates_without_selecting_python(tmp_path, sdk):
    table = _table(tmp_path)
    router = NativeInputRouter(table)
    failure = ValueError('Input must contain a routing column')
    router._router = Mock()
    router._router.route.side_effect = failure
    with patch.object(FixedBucketInputRouter, '__init__', side_effect=AssertionError('Python replay')):
        with pytest.raises(ValueError) as raised:
            router.route(pa.record_batch([[1]], names=['value']))
    assert raised.value is failure


def test_native_router_pickle_discards_sdk_and_restores_bucket_plan(tmp_path, sdk):
    table = _table(tmp_path, bucket='-2')
    plan = PostponeBucketPlan({('west',): 3})
    router = NativeInputRouter(table, plan)
    # An SDK object is deliberately not serializable.
    router._router = lambda: None
    restored = pickle.loads(pickle.dumps(router))
    assert restored._router is None
    native = sdk.BucketRouter.return_value
    native.with_bucket_plan.return_value = native
    native.route.return_value = pa.array([1], type=pa.int32())
    with patch('pypaimon.execution.native.write.input_routing.create_native_write_table', return_value=Mock()):
        assert restored.route(pa.record_batch([[7], ['west']], names=['id', 'p'])).to_pylist() == [1]
    assert native.with_bucket_plan.call_args[0][0].equals(plan.to_arrow(table))


def test_native_planner_transports_group_statistics_positionally(tmp_path, sdk):
    table = _table(tmp_path, bucket='-2', partition='row_count')
    native_table = Mock()
    core = sdk.PostponeBucketPlanner.return_value
    core.input_statistics.return_value = pa.record_batch(
        [['west'], pa.array([3], type=pa.int64()), pa.array([80], type=pa.int64())],
        names=['row_count', 'row_count', 'data_size'])
    expected = PostponeBucketPlan({('west',): 2})
    core.plan.return_value = expected.to_arrow(table)
    with patch('pypaimon.execution.native.write.postpone_bucket.native_runtime_available', return_value=True), \
            patch('pypaimon.execution.native.write.postpone_bucket.create_native_write_table',
                  return_value=native_table), \
            patch.object(PostponeBucketPlanning, 'input_partition_stats',
                         side_effect=AssertionError('Python input statistics')), \
            patch.object(PostponeBucketPlanning, 'plan', side_effect=AssertionError('Python bucket inference')):
        planner = create_postpone_bucket_planner(table, known_num_buckets={})
        assert isinstance(planner, NativePostponeBucketPlanner)
        assert not isinstance(planner, PostponeBucketPlanning)
        batch = pa.table({'id': [1, 2, 3], 'row_count': ['west'] * 3, 'value': ['data'] * 3})
        stats = planner.input_partition_stats(batch)
        assert stats == {('west',): (3, 80)}
        assert planner.plan(stats).as_dict() == expected.as_dict()
    native_table.new_postpone_fixed_bucket_write_builder.assert_not_called()
    transported = core.input_statistics.call_args[0][0]
    assert isinstance(transported, pa.RecordBatch) and transported.num_rows == 3
    grouped = core.plan.call_args[0][0]
    assert grouped.schema.names == ['row_count', 'row_count', 'data_size']
    assert [column.to_pylist() for column in grouped.columns] == [['west'], [3], [80]]
    assert grouped.column(1).type == pa.int64() and grouped.column(2).type == pa.int64()


def test_native_planner_pickle_keeps_resolved_counts_and_worker_failures(tmp_path, sdk):
    table = _table(tmp_path, bucket='-2')
    core = sdk.PostponeBucketPlanner.return_value
    expected = PostponeBucketPlan({('west',): 7})
    core.plan.return_value = expected.to_arrow(table)
    planner = NativePostponeBucketPlanner(table, {}, Mock())
    planner.plan({('west',): (13, 0)})
    planner._planner = lambda: None
    restored = pickle.loads(pickle.dumps(planner))
    assert restored._planner is None
    sdk.PostponeBucketPlanner.reset_mock()
    failure = RuntimeError('worker statistics failed')
    core.input_statistics.side_effect = failure
    with patch('pypaimon.execution.native.write.postpone_bucket.create_native_write_table', return_value=Mock()), \
            patch.object(PostponeBucketPlanning, 'input_partition_stats', side_effect=AssertionError('Python replay')):
        with pytest.raises(RuntimeError) as raised:
            restored.input_partition_stats(pa.table({'id': [1], 'p': ['west'], 'value': ['x']}))
    assert raised.value is failure
    assert sdk.PostponeBucketPlanner.call_args[0][1].equals(expected.to_arrow(table))
