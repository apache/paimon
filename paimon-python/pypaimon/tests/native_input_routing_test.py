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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Distributed input hashes and postpone statistics execute in the real SDK."""

from decimal import Decimal
from datetime import datetime, timezone
import pickle
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.execution.native.write.input_routing import NativeInputRouter
from pypaimon.execution.native.write.postpone_bucket import NativePostponeBucketPlanner
from pypaimon.ray.shuffle import _make_bucket_udf
from pypaimon.write.backend_dispatch import create_input_router, create_postpone_bucket_planner
from pypaimon.write.bucket_planning import PostponeBucketPlanning
from pypaimon.execution.python.write.input_routing import FixedBucketInputRouter, PostponeFixedBucketInputRouter

pytestmark = pytest.mark.native_plan


def _table(catalog, schema, options, partition='p'):
    table_options = {
        'file.format': 'parquet', 'write.native.enabled': 'true',
        'postpone.target-row-num-per-bucket': '2',
        'postpone.batch-write-fixed-bucket.max-parallelism': '8',
    }
    table_options.update(options)
    table_options = {key: value for key, value in table_options.items() if value is not None}
    catalog.create_table('default.input_routing', Schema.from_pyarrow_schema(
        schema, primary_keys=['id', partition], partition_keys=[partition], options=table_options), False)
    return catalog.get_table('default.input_routing')


@pytest.mark.parametrize('bucket_function', ['default', 'mod'])
def test_native_ray_bucket_hashes_match_writer_for_reordered_subset(native_rest_catalog, bucket_function):
    schema = pa.schema([('id', pa.int64()), ('p', pa.string()), ('value', pa.string())])
    table = _table(native_rest_catalog, schema, {
        'bucket': '7', 'bucket-function.type': bucket_function})
    batch = pa.record_batch([['west', 'east', 'west', 'east'], [-9, 0, 11, -(1 << 63)]], names=['p', 'id'])
    expected = FixedBucketInputRouter(table.table_schema).extract_partition_bucket_batch(batch)[1]
    with patch.object(FixedBucketInputRouter, '_extract_partitions_batch',
                      side_effect=AssertionError('Python partitions')), \
            patch.object(FixedBucketInputRouter, '_binary_row_hash_code', side_effect=AssertionError('Python hashes')):
        router = create_input_router(table)
        assert isinstance(router, NativeInputRouter)
        restored = pickle.loads(pickle.dumps(router))
        chunked = pa.concat_tables([pa.Table.from_batches([batch.slice(0, 2)]),
                                    pa.Table.from_batches([batch.slice(2, 2)])])
        result = _make_bucket_udf(restored, '__bucket__')(chunked)
        assert result.column('__bucket__').to_pylist() == expected
        assert result.column('__bucket__').type == pa.int32()
        assert result.drop_columns(['__bucket__']).equals(chunked)
        with pytest.raises(Exception, match='id|routing|column|field'):
            restored.route(pa.record_batch([['west']], names=['p']))


def test_native_postpone_statistics_inference_and_routing_skip_python(native_rest_catalog):
    schema = pa.schema([
        ('id', pa.int64()), ('row_count', pa.string()), ('value', pa.string()),
        ('items', pa.list_(pa.int64())), ('attrs', pa.map_(pa.string(), pa.int64())),
        ('nested', pa.struct([('label', pa.string()), ('count', pa.int64())])),
        ('amount', pa.decimal128(24, 3)),
    ])
    table = _table(native_rest_catalog, schema, {
        'bucket': '-2', 'postpone.target-size-per-bucket': '64',
        'postpone.target-row-num-per-bucket': None}, partition='row_count')
    rows = [dict(id=i, row_count='known' if i == 0 else 'west' if i < 4 else 'east',
                 value='abcdefghi' * i, items=[1, None, i], attrs=[('key', i)],
                 nested={'label': 'nested-value', 'count': i}, amount=Decimal('12345678901234567890.123'))
            for i in range(6)]
    batch = pa.RecordBatch.from_pylist(rows, schema=schema)
    reference = PostponeBucketPlanning(table, {('known',): 3})
    expected_stats = reference.input_partition_stats(batch)
    expected = reference.plan(expected_stats)
    with patch.object(PostponeBucketPlanning, 'input_partition_stats',
                      side_effect=AssertionError('Python row sizes')), \
            patch.object(PostponeBucketPlanning, 'plan', side_effect=AssertionError('Python bucket inference')):
        planner = create_postpone_bucket_planner(table, {('known',): 3})
        assert isinstance(planner, NativePostponeBucketPlanner)
        restored = pickle.loads(pickle.dumps(planner))
        stats = restored.input_partition_stats(pa.Table.from_batches([batch.slice(0, 2), batch.slice(2)]))
        assert stats == expected_stats
        plan = restored.plan(stats)
        assert plan.as_dict() == expected.as_dict()
        assert restored.current_plan().as_dict() == expected.as_dict()
        assert restored.input_partition_stats(batch) == {}
    reference_router = PostponeFixedBucketInputRouter(table, expected)
    expected_buckets = reference_router.extract_partition_bucket_batch(batch)[1]
    with patch.object(PostponeFixedBucketInputRouter, 'extract_partition_bucket_batch',
                      side_effect=AssertionError('Python postpone hashes')):
        router = create_input_router(table, plan)
        assert isinstance(router, NativeInputRouter)
        assert router.route(batch).to_pylist() == expected_buckets
        # A worker reconstructs from the immutable plan without SDK pickling.
        assert pickle.loads(pickle.dumps(router)).route(batch).to_pylist() == expected_buckets


def test_native_postpone_known_empty_skips_metadata_and_stats_failure_is_terminal(native_rest_catalog):
    table = _table(native_rest_catalog, pa.schema([('id', pa.int64()), ('p', pa.string())]), {'bucket': '-2'})
    with patch.object(NativePostponeBucketPlanner, '_load_bucket_metadata', side_effect=AssertionError('metadata')):
        planner = create_postpone_bucket_planner(table, {})
        assert isinstance(planner, NativePostponeBucketPlanner)
        assert planner.current_plan().as_dict() == {}
        assert planner.plan({('west',): (9, 0)}).as_dict() == {('west',): 8}
    with patch.object(PostponeBucketPlanning, 'input_partition_stats', side_effect=AssertionError('Python replay')):
        with pytest.raises(Exception, match='p|partition|column|field'):
            planner.input_partition_stats(pa.record_batch([[1]], names=['id']))


@pytest.mark.parametrize('key_type,value,null_bucket', [
    (pa.decimal128(19, 2), Decimal('123.40'), 1),
    (pa.timestamp('us'), datetime(2024, 1, 2, 3, 4, 5, 123456), 28),
    (pa.timestamp('us', tz='UTC'), datetime(2024, 1, 2, 3, 4, 5, 123456, tzinfo=timezone.utc), 28),
])
def test_native_nullable_noncompact_bucket_keys_match_java(native_rest_catalog, key_type, value, null_bucket):
    schema = pa.schema([('id', key_type), ('label', pa.string()), ('p', pa.string())])
    native_rest_catalog.create_table('default.null_bucket', Schema.from_pyarrow_schema(
        schema, primary_keys=['id', 'label', 'p'], partition_keys=['p'], options={
            'bucket': '100', 'primary-key.nullable': 'true', 'write.native.enabled': 'true',
        }), False)
    table = native_rest_catalog.get_table('default.null_bucket')
    batch = pa.RecordBatch.from_arrays([
        pa.array([None, value, None], type=key_type),
        pa.array(['abcdefghijk'] * 3), pa.array(['west'] * 3),
    ], schema=schema)
    expected = FixedBucketInputRouter(table.table_schema).extract_partition_bucket_batch(batch)[1]
    # Current Java hashes [NULL, "abcdefghijk"] as 1558048601 for DECIMAL
    # and -684602128 for noncompact TIMESTAMP/LTZ; Java abs(hash % 100) follows.
    assert expected[0] == expected[2] == null_bucket
    with patch.object(FixedBucketInputRouter, '_binary_row_hash_code', side_effect=AssertionError('Python hashes')):
        router = create_input_router(table)
        assert isinstance(router, NativeInputRouter)
        assert router.route(batch).to_pylist() == expected


def test_native_postpone_fixed_binary_partition_slices_match_python(native_rest_catalog):
    schema = pa.schema([('id', pa.int64()), ('p', pa.binary(4)), ('value', pa.string())])
    table = _table(native_rest_catalog, schema, {
        'bucket': '-2', 'partition.legacy-name': 'false', 'primary-key.nullable': 'true',
        'postpone.target-row-num-per-bucket': None, 'postpone.target-size-per-bucket': '128',
    })
    assert table.field_dict['p'].type.type == 'BINARY(4)'
    payloads = ['payload-' * 4] * 9
    payloads[3] = None
    source = pa.RecordBatch.from_arrays([
        pa.array(range(9), type=pa.int64()),
        pa.array([b'pad!', b'abcd', None, b'abcd', b'abcd', None, b'efgh', b'efgh', b'end!'], type=pa.binary(4)),
        pa.array(payloads),
    ], schema=schema)
    batch = source.slice(1, 7)
    assert batch.column(1).offset == 1
    chunked = pa.Table.from_batches([batch.slice(0, 2), batch.slice(2)])
    reference = PostponeBucketPlanning(table, {})
    expected_stats = reference.input_partition_stats(batch)
    assert expected_stats == {(b'abcd',): (3, 160), (None,): (2, 128), (b'efgh',): (2, 128)}
    expected_plan = reference.plan(expected_stats)
    assert expected_plan.as_dict() == {(b'abcd',): 2, (None,): 1, (b'efgh',): 1}
    expected_buckets = PostponeFixedBucketInputRouter(table, expected_plan).extract_partition_bucket_batch(batch)[1]

    with patch.object(PostponeBucketPlanning, 'input_partition_stats',
                      side_effect=AssertionError('Python statistics')), \
            patch.object(PostponeBucketPlanning, 'plan', side_effect=AssertionError('Python bucket inference')):
        planner = create_postpone_bucket_planner(table, {})
        assert isinstance(planner, NativePostponeBucketPlanner)
        assert planner.input_partition_stats(batch) == expected_stats
        assert planner.input_partition_stats(chunked) == expected_stats
        plan = planner.plan(expected_stats)
        assert plan.as_dict() == expected_plan.as_dict()
        assert planner.input_partition_stats(chunked) == {}
    with patch.object(PostponeFixedBucketInputRouter, 'extract_partition_bucket_batch',
                      side_effect=AssertionError('Python bucket hashing')):
        router = create_input_router(table, plan)
        assert isinstance(router, NativeInputRouter)
        assert router.route(batch).to_pylist() == expected_buckets
        assert router.route(chunked).to_pylist() == expected_buckets
