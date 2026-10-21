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

"""Distributed input routing must never construct a Python file executor."""

from contextlib import ExitStack
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.index.dynamic_bucket import HashBucketAssigner
from pypaimon.read.plan import Plan
from pypaimon.read.split import DataSplit
from pypaimon.read.split_serializer import deserialize_split_v1, serialize_split_v1
from pypaimon.write.input_routing import DynamicBucketInputRouter, FixedBucketInputRouter


def _table(tmp_path, bucket):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        pa.schema([('id', pa.int64()), ('value', pa.string()), ('p', pa.string())]),
        partition_keys=['p'], primary_keys=['id', 'p'], options={
            'bucket': bucket, 'dynamic-bucket.target-row-num': '1',
            'write.native.enabled': 'true',
        }), False)
    return catalog.get_table('default.t')


@pytest.fixture
def forbid_python_execution():
    with ExitStack() as stack:
        for target in [
            'execution.python.write.row_key_extractor.FixedBucketRowKeyExtractor.__init__',
            'execution.python.write.row_key_extractor.DynamicBucketRowKeyExtractor.__init__',
            'execution.python.write.row_key_extractor.SimpleHashBucketAssigner.__init__',
            'execution.python.read.scanner.file_scanner.FileScanner.__init__',
            'execution.python.write.table_write.PythonTableWrite.__init__',
            'execution.python.write.dynamic_bucket.DynamicBucketIndexMaintainer.__init__',
        ]:
            stack.enter_context(patch('pypaimon.' + target, side_effect=AssertionError(target)))
        yield


def test_ray_fixed_grouping_executes_input_router_without_python_extractor(tmp_path, forbid_python_execution):
    from pypaimon.ray.shuffle import _group_by_partition_bucket

    table = _table(tmp_path, '4')
    dataset = Mock()
    _group_by_partition_bucket(dataset, table)
    udf = dataset.map_batches.call_args[0][0]
    rows = pa.table({'id': [1, 2, 1], 'value': ['a', 'b', 'c'], 'p': ['p', 'p', 'q']})
    actual = udf(rows)
    assert actual.column_names[:3] == rows.column_names
    assert actual.column(3).to_pylist() == FixedBucketInputRouter(table.table_schema).extract_partition_bucket_batch(
        rows.to_batches()[0])[1]
    assert dataset.map_batches.return_value.groupby.call_args[0][0] == ['p', actual.column_names[3]]


def test_daft_fixed_and_dynamic_routing_do_not_construct_python_extractors(tmp_path, forbid_python_execution):
    daft = pytest.importorskip('daft')
    from pypaimon.daft.daft_datasink import make_dynamic_routing_udf, make_fixed_bucket_udf
    from pypaimon.index.dynamic_bucket import compute_assigner, to_signed_int32

    table = _table(tmp_path, '4')
    rows = {'id': [1, 2, 1], 'value': ['a', 'b', 'c'], 'p': ['p', 'p', 'q']}
    columns = [daft.col(name) for name in table.field_names]
    fixed = daft.from_pydict(rows).with_column('bucket', make_fixed_bucket_udf(table)(*columns)).to_pydict()
    batch = pa.table(rows).to_batches()[0]
    assert fixed['bucket'] == FixedBucketInputRouter(table.table_schema).extract_partition_bucket_batch(batch)[1]
    # Only input hashing belongs to this stage; it must not construct even a
    # transient SimpleHashBucketAssigner or persistent HASH-index maintainer.
    dynamic = _table(tmp_path / 'dynamic', '-1')
    router = DynamicBucketInputRouter(dynamic.table_schema)
    _, partition_hashes, key_hashes = router.extract_hashes_batch(batch)
    routed = daft.from_pydict(rows).select(
        *columns, make_dynamic_routing_udf(dynamic, 2, 2, 'assigner', 'hash')(*columns)).to_pydict()
    assert routed['hash'] == [to_signed_int32(value) for value in key_hashes]
    assert routed['assigner'] == [compute_assigner(p, k, 2, 2) for p, k in zip(partition_hashes, key_hashes)]


def _seed_snapshot(table):
    # Seed real data and complete HASH indexes before guarding every Pure
    # executor constructor, so an empty-table shortcut cannot hide the leak.
    pure = table.copy({'write.native.enabled': 'false', 'commit.native.enabled': 'false'})
    builder = pure.new_batch_write_builder()
    writer = builder.new_write()
    writer.write_arrow(pa.table({'id': [1], 'value': ['old'], 'p': ['p']}))
    commit = builder.new_commit()
    commit.commit(writer.prepare_commit())
    writer.close()
    commit.close()
    snapshot = table.snapshot_manager().get_latest_snapshot()
    splits = pure.new_read_builder().new_scan().plan_all_files().splits()
    plan = Plan([deserialize_split_v1(
        serialize_split_v1(split), table.partition_keys_fields,
        table.trimmed_primary_keys_fields) for split in splits])
    assert plan.splits()[0].partition.values == ['p']
    return snapshot, plan


@pytest.mark.parametrize('ignore_existing', [False, True])
def test_daft_dynamic_assignment_restores_pinned_native_metadata_without_python_executor(tmp_path, ignore_existing):
    daft = pytest.importorskip('daft')
    from pypaimon.daft.daft_datasink import make_dynamic_bucket_assignment_udf

    table = _table(tmp_path, '-1')
    snapshot, plan = _seed_snapshot(table)
    rows = {'id': [2, 1], 'value': ['other', 'new'], 'p': ['p', 'p']}
    router = DynamicBucketInputRouter(table.table_schema)
    _, _, hashes = router.extract_hashes_batch(pa.table(rows).to_batches()[0])
    rows.update(assigner=[0, 0], hash=hashes)
    with ExitStack() as stack:
        for target in [
            'execution.python.write.row_key_extractor.DynamicBucketRowKeyExtractor.__init__',
            'execution.python.write.row_key_extractor.SimpleHashBucketAssigner.__init__',
            'execution.python.read.scanner.file_scanner.FileScanner.__init__',
            'execution.python.write.dynamic_bucket.DynamicBucketIndexMaintainer.__init__',
        ]:
            stack.enter_context(patch('pypaimon.' + target, side_effect=AssertionError(target)))
        stack.enter_context(patch('pypaimon.daft.daft_datasink._load_worker_table', return_value=table))
        native_plan = stack.enter_context(patch(
            'pypaimon.execution.native.read.plan.native_plan', return_value=plan))
        udf = make_dynamic_bucket_assignment_udf(
            table, 1, 1, 'bucket', 'hash', 'base_snapshot',
            ignore_existing=ignore_existing, base_snapshot_id=snapshot.id)
        result = daft.from_pydict(rows).select(udf(*[daft.col(name) for name in rows])).to_pydict()
    assert result['bucket'] == ([0, 1] if ignore_existing else [1, 0])
    assert result['base_snapshot'] == [snapshot.id, snapshot.id]
    if ignore_existing:
        native_plan.assert_not_called()
    else:
        native_plan.assert_called_once()
        assert native_plan.call_args[0][0].options.scan_snapshot_id() == snapshot.id
        assert native_plan.call_args[1]['all_files'] is True
        predicate = native_plan.call_args[1]['predicate']
        assert predicate.field == 'p' and predicate.literals == ['p']


def test_native_dynamic_metadata_preserves_missing_hash_index_validation(tmp_path):
    table = _table(tmp_path, '-1')
    snapshot, plan = _seed_snapshot(table)
    split = plan.splits()[0]
    plan = Plan([DataSplit(files=split.files, partition=split.partition, bucket=7)])
    with patch('pypaimon.execution.native.read.plan.native_plan', return_value=plan), \
            patch('pypaimon.execution.python.read.scanner.file_scanner.FileScanner.__init__',
                  side_effect=AssertionError('Pure scanner')):
        assigner = HashBucketAssigner(table, 1, 1, 0, 1, -1, snapshot=snapshot)
        with pytest.raises(RuntimeError, match='no complete HASH index.*7'):
            assigner.assign(('p',), 0, 123)


def test_dynamic_metadata_preparation_fallback_happens_before_assignment(tmp_path):
    table = _table(tmp_path, '-1')
    snapshot, _ = _seed_snapshot(table)
    from pypaimon.write.backend_dispatch import dynamic_bucket_data_buckets
    with patch('pypaimon.execution.native.read.plan.native_plan',
               side_effect=RuntimeError('unrestricted grant needed')), \
            patch('pypaimon.execution.python.write.bucket_metadata.dynamic_bucket_data_buckets',
                  return_value={3}) as pure:
        assert dynamic_bucket_data_buckets(table, ('p',), snapshot) == {3}
        pure.assert_called_once_with(table, ('p',), snapshot)
