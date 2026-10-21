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

"""Ray input grouping must not allocate a Python file execution backend."""

from pathlib import Path
import subprocess
import sys
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.execution.write_dispatch import NativePostponeFixedBucketTableWrite
from pypaimon.read.plan import Plan
from pypaimon.read.split import DataSplit
from pypaimon.read.split_serializer import deserialize_split_v1, serialize_split_v1
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.write.bucket_plan import PostponeBucketPlan


def test_public_postpone_input_planning_does_not_load_either_backend(tmp_path):
    source = r'''
import importlib.abc
import sys

class BlockExecution(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.startswith(('pypaimon.execution.python', 'pypaimon.execution.native', 'pypaimon_rust')):
            raise AssertionError('Input planning loaded an execution backend: ' + fullname)

sys.meta_path.insert(0, BlockExecution())
import pyarrow as pa
from pypaimon import CatalogFactory, Schema
from pypaimon.write.postpone_bucket import PostponeBucketPlan, PostponeBucketPlanner

catalog = CatalogFactory.create({'warehouse': sys.argv[1]})
catalog.create_database('default', True)
catalog.create_table('default.data', Schema.from_pyarrow_schema(
    pa.schema([('id', pa.int32()), ('p', pa.string()), ('value', pa.int64())]),
    primary_keys=['id', 'p'], partition_keys=['p'], options={
        'bucket': '-2', 'postpone.target-row-num-per-bucket': '2',
        'postpone.batch-write-fixed-bucket.max-parallelism': '8',
    }), False)
table = catalog.get_table('default.data')
data = pa.record_batch([
    pa.array(range(7), pa.int32()), ['first'] * 5 + ['second'] * 2,
    pa.array(range(7), pa.int64()),
], names=['id', 'p', 'value'])
planner = PostponeBucketPlanner(table, known_num_buckets={})
assert planner.current_plan().as_dict() == {}
stats = planner.input_partition_stats(data)
assert stats == {('first',): (5, 0), ('second',): (2, 0)}
plan = planner.plan(stats)
assert isinstance(plan, PostponeBucketPlan)
assert plan.as_dict() == {('first',): 4, ('second',): 1}
assert planner.current_plan().as_dict() == plan.as_dict()

existing = PostponeBucketPlanner(table, known_num_buckets={('first',): 3})
assert existing.input_partition_stats(data) == {('second',): (2, 0)}
assert existing.plan(existing.input_partition_stats(data)).as_dict() == {
    ('first',): 3, ('second',): 1}
assert not any(name.startswith(('pypaimon.execution.python', 'pypaimon.execution.native', 'pypaimon_rust'))
               for name in sys.modules)
'''
    result = subprocess.run(
        [sys.executable, '-c', source, str(tmp_path / 'warehouse')],
        cwd=str(Path(__file__).resolve().parents[3]),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr


def _table(tmp_path):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        pa.schema([('id', pa.int32()), ('p', pa.string()), ('value', pa.int64())]),
        primary_keys=['id', 'p'], partition_keys=['p'], options={
            'bucket': '-2', 'write.native.enabled': 'true',
            'postpone.batch-write-fixed-bucket': 'true',
            'postpone.target-row-num-per-bucket': '2',
            'postpone.batch-write-fixed-bucket.max-parallelism': '8',
        }), False)
    return catalog.get_table('default.t')


def _plan(table):
    # Native planning returns decoded Split v1 metadata, including GenericRow
    # partitions. A tuple stand-in would hide invalid partition access here.
    splits = [DataSplit(
        files=[], partition=GenericRow([partition], table.partition_keys_fields),
        bucket=bucket, total_buckets=total, bucket_path=table.table_path,
    ) for partition, bucket, total in [('existing', 1, 3), ('pending', -2, 1)]]
    return Plan([deserialize_split_v1(
        serialize_split_v1(split), table.partition_keys_fields,
        table.trimmed_primary_keys_fields) for split in splits])


@pytest.fixture
def forbid_python_execution():
    with patch('pypaimon.execution.python.write.postpone_bucket.PythonPostponeBucketPlanner.__init__',
               side_effect=AssertionError('Python bucket planner was constructed')), \
            patch('pypaimon.execution.python.write.row_key_extractor.RowKeyExtractor.__init__',
                  side_effect=AssertionError('Python row-key extractor was constructed')), \
            patch('pypaimon.execution.python.read.scanner.file_scanner.FileScanner.__init__',
                  side_effect=AssertionError('Python file scanner was constructed')), \
            patch('pypaimon.execution.python.write.table_write.PythonTableWriteMixin.__init__',
                  side_effect=AssertionError('Python data writer was constructed')):
        yield


@pytest.mark.parametrize('overwrite', [False, True])
def test_native_ray_planning_uses_native_metadata_and_shared_input_routing(
        tmp_path, forbid_python_execution, overwrite):
    pytest.importorskip('ray')
    table = _table(tmp_path)
    core = Mock()
    writer = NativePostponeFixedBucketTableWrite(table, 'user', None, None, core)
    dataset = Mock()
    with patch('pypaimon.execution.native.write.postpone_bucket.native_runtime_available', return_value=True), \
            patch('pypaimon.execution.native.write.postpone_bucket.native_plan', return_value=_plan(table)) as plan, \
            patch('pypaimon.execution.datasink.ray_datasink._collect_partition_stats', return_value=(
                dataset, {('existing',): (2, 0), ('new',): (9, 0), ('pending',): (1, 0)})), \
            patch('pypaimon.execution.datasink.ray_datasink._write_primary_key_groups') as write:
        writer.write_ray(dataset, overwrite=overwrite)
        args = write.call_args[1]
        expected = {('existing',): 1 if overwrite else 3, ('new',): 8, ('pending',): 1}
        assert args['postpone_bucket_plan'].as_dict() == expected
        batch = pa.record_batch([[1, 2], ['existing', 'new'], [10, 20]], names=['id', 'p', 'value'])
        partitions, buckets = args['bucket_extractor'].extract_partition_bucket_batch(batch)
        assert partitions == [('existing',), ('new',)]
        assert 0 <= buckets[0] < expected[('existing',)]
        assert 0 <= buckets[1] < 8
        if overwrite:
            plan.assert_not_called()
        else:
            plan.assert_called_once_with(table, all_files=True)
        assert writer._python_writer is None
        core.write_arrow.assert_not_called()
    writer.close()


def test_native_ray_explicit_plan_does_not_load_metadata(tmp_path, forbid_python_execution):
    pytest.importorskip('ray')
    table = _table(tmp_path)
    explicit = PostponeBucketPlan({('existing',): 7})
    writer = NativePostponeFixedBucketTableWrite(table, 'user', None, explicit, Mock())
    dataset = Mock()
    with patch('pypaimon.execution.native.write.postpone_bucket.native_runtime_available', return_value=True), \
            patch('pypaimon.execution.native.write.postpone_bucket.native_plan',
                  side_effect=AssertionError('explicit bucket plan must not read metadata')), \
            patch('pypaimon.execution.datasink.ray_datasink._collect_partition_stats', return_value=(dataset, {})), \
            patch('pypaimon.execution.datasink.ray_datasink._write_primary_key_groups') as write:
        writer.write_ray(dataset)
        assert write.call_args[1]['postpone_bucket_plan'].as_dict() == explicit.as_dict()
    writer.close()


def test_native_metadata_preparation_failure_selects_python_before_input(tmp_path):
    from pypaimon.execution.dispatch import create_postpone_bucket_planner
    from pypaimon.write.postpone_bucket import PostponeBucketPlanner

    table = _table(tmp_path)
    with patch('pypaimon.execution.native.write.postpone_bucket.native_runtime_available', return_value=True), \
            patch('pypaimon.execution.native.write.postpone_bucket.native_plan',
                  side_effect=RuntimeError('native metadata unavailable')), \
            patch.object(PostponeBucketPlanner, '_load_bucket_metadata', return_value={('existing',): 3}) as load, \
            patch('pypaimon.write.postpone_bucket.PostponeBucketPlanner', wraps=PostponeBucketPlanner) as pure:
        planner = create_postpone_bucket_planner(table)
        assert planner.plan({('existing',): (100, 0), ('new',): (3, 0)}).as_dict() == {
            ('existing',): 3, ('new',): 2}
        pure.assert_called_once_with(table, known_num_buckets=None)
        load.assert_called_once_with(table)


def test_native_ray_execution_failure_does_not_replay_python_planning(tmp_path, forbid_python_execution):
    pytest.importorskip('ray')
    table = _table(tmp_path)
    writer = NativePostponeFixedBucketTableWrite(table, 'user', None, None, Mock())
    dataset = Mock()
    failure = TimeoutError('Ray task may have written files')
    with patch('pypaimon.execution.native.write.postpone_bucket.native_runtime_available', return_value=True), \
            patch('pypaimon.execution.native.write.postpone_bucket.native_plan', return_value=_plan(table)), \
            patch('pypaimon.execution.datasink.ray_datasink._collect_partition_stats', return_value=(dataset, {})), \
            patch('pypaimon.execution.datasink.ray_datasink._write_primary_key_groups', side_effect=failure) as write:
        with pytest.raises(TimeoutError) as raised:
            writer.write_ray(dataset)
        assert raised.value is failure
        write.assert_called_once()
        assert writer._python_writer is None
    writer.close()
