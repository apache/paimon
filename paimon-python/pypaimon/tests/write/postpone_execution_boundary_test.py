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
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.write.write_dispatch import NativePostponeFixedBucketTableWrite
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


def _table(tmp_path, partition_name='p'):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        pa.schema([('id', pa.int32()), (partition_name, pa.string()), ('value', pa.int64())]),
        primary_keys=['id', partition_name], partition_keys=[partition_name], options={
            'bucket': '-2', 'write.native.enabled': 'true',
            'postpone.batch-write-fixed-bucket': 'true',
            'postpone.target-row-num-per-bucket': '2',
            'postpone.batch-write-fixed-bucket.max-parallelism': '8',
        }), False)
    return catalog.get_table('default.t')


def _native_table(table):
    native = Mock()
    native.new_postpone_fixed_bucket_write_builder.return_value.resolve_bucket_plan.return_value = pa.record_batch([
        pa.array(['existing']), pa.array([3], type=pa.int32())], names=['p', 'total_buckets'])
    return native


@pytest.fixture(autouse=True)
def native_input_sdk(monkeypatch):
    """These boundary tests supply SDK results without a native installation."""
    def planner(_schema, known):
        core = Mock()
        core.current_plan.return_value = known
        core.plan.return_value = known
        return core

    def router(_schema):
        core = Mock()
        core.with_bucket_plan.return_value = core
        core.route.side_effect = lambda batch: pa.array([0] * batch.num_rows, type=pa.int32())
        return core

    sdk = SimpleNamespace(PostponeBucketPlanner=Mock(side_effect=planner), BucketRouter=Mock(side_effect=router))
    monkeypatch.setitem(sys.modules, 'pypaimon_rust.datafusion', sdk)
    return sdk


@pytest.fixture
def forbid_python_execution():
    with patch('pypaimon.execution.python.write.postpone_bucket.PythonPostponeBucketPlanner.__init__',
               side_effect=AssertionError('Python bucket planner was constructed')), \
            patch('pypaimon.execution.python.write.row_key_extractor.RowKeyExtractor.__init__',
                  side_effect=AssertionError('Python row-key extractor was constructed')), \
            patch('pypaimon.execution.python.read.scanner.file_scanner.FileScanner.__init__',
                  side_effect=AssertionError('Python file scanner was constructed')), \
            patch('pypaimon.execution.python.write.table_write.PythonTableWrite.__init__',
                  side_effect=AssertionError('Python data writer was constructed')):
        yield


@pytest.mark.parametrize('overwrite', [False, True])
def test_native_ray_planning_uses_native_metadata_and_input_routing(
        tmp_path, forbid_python_execution, overwrite):
    pytest.importorskip('ray')
    table = _table(tmp_path)
    core = Mock()
    writer = NativePostponeFixedBucketTableWrite(table, 'user', None, None, core)
    dataset = Mock()
    expected = {('existing',): 1 if overwrite else 3, ('new',): 8, ('pending',): 1}
    native_table = _native_table(table)
    with patch('pypaimon.execution.native.write.postpone_bucket.native_runtime_available', return_value=True), \
            patch('pypaimon.execution.native.write.postpone_bucket.create_native_write_table',
                  return_value=native_table), \
            patch('pypaimon.execution.native.write.input_routing.native_runtime_available', return_value=True), \
            patch('pypaimon.execution.native.write.input_routing.create_native_write_table',
                  return_value=native_table), \
            patch('pypaimon.execution.native.write.postpone_bucket.NativePostponeBucketPlanner.plan',
                  return_value=PostponeBucketPlan(expected)), \
            patch('pypaimon.write.ray_datasink._collect_partition_stats', return_value=(
                dataset, {('existing',): (2, 0), ('new',): (9, 0), ('pending',): (1, 0)})), \
            patch('pypaimon.write.ray_datasink._write_primary_key_groups') as write:
        writer.write_ray(dataset, overwrite=overwrite)
        args = write.call_args[1]
        assert args['postpone_bucket_plan'].as_dict() == expected
        batch = pa.record_batch([[1, 2], ['existing', 'new'], [10, 20]], names=['id', 'p', 'value'])
        buckets = args['bucket_router'].route(batch).to_pylist()
        assert 0 <= buckets[0] < expected[('existing',)]
        assert 0 <= buckets[1] < 8
        if overwrite:
            native_table.new_postpone_fixed_bucket_write_builder.assert_not_called()
        else:
            native_table.new_postpone_fixed_bucket_write_builder.return_value.resolve_bucket_plan.assert_called_once()
        assert writer._python_writer is None
        core.write_arrow.assert_not_called()
    writer.close()


def test_native_ray_explicit_plan_does_not_load_metadata(tmp_path, forbid_python_execution):
    pytest.importorskip('ray')
    table = _table(tmp_path)
    explicit = PostponeBucketPlan({('existing',): 7})
    writer = NativePostponeFixedBucketTableWrite(table, 'user', None, explicit, Mock())
    dataset = Mock()
    native_table = _native_table(table)
    with patch('pypaimon.execution.native.write.postpone_bucket.native_runtime_available', return_value=True), \
            patch('pypaimon.execution.native.write.postpone_bucket.create_native_write_table',
                  return_value=native_table), \
            patch('pypaimon.write.ray_datasink._collect_partition_stats', return_value=(dataset, {})), \
            patch('pypaimon.write.ray_datasink._write_primary_key_groups') as write:
        writer.write_ray(dataset)
        assert write.call_args[1]['postpone_bucket_plan'].as_dict() == explicit.as_dict()
        native_table.new_postpone_fixed_bucket_write_builder.assert_not_called()
    writer.close()


def test_native_metadata_preparation_failure_selects_python_before_input(tmp_path):
    from pypaimon.write.backend_dispatch import create_postpone_bucket_planner
    from pypaimon.execution.python.write.postpone_bucket import PythonPostponeBucketPlanner

    table = _table(tmp_path)
    with patch('pypaimon.execution.native.write.postpone_bucket.native_runtime_available', return_value=True), \
            patch('pypaimon.execution.native.write.postpone_bucket.create_native_write_table',
                  side_effect=RuntimeError('native metadata unavailable')), \
            patch.object(PythonPostponeBucketPlanner, '_load_bucket_metadata',
                         return_value={('existing',): 3}) as load, \
            patch('pypaimon.execution.python.write.postpone_bucket.PythonPostponeBucketPlanner',
                  wraps=PythonPostponeBucketPlanner) as pure:
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
            patch('pypaimon.execution.native.write.postpone_bucket.create_native_write_table',
                  return_value=_native_table(table)), \
            patch('pypaimon.write.ray_datasink._collect_partition_stats', return_value=(dataset, {})), \
            patch('pypaimon.write.ray_datasink._write_primary_key_groups', side_effect=failure) as write:
        with pytest.raises(TimeoutError) as raised:
            writer.write_ray(dataset)
        assert raised.value is failure
        write.assert_called_once()
        assert writer._python_writer is None
    writer.close()


def test_direct_public_bucket_planner_uses_native_file_metadata(tmp_path, forbid_python_execution):
    from pypaimon.write.postpone_bucket import PostponeBucketPlanner

    table = _table(tmp_path)
    with patch('pypaimon.execution.native.write.postpone_bucket.native_runtime_available', return_value=True), \
            patch('pypaimon.execution.native.write.postpone_bucket.create_native_write_table',
                  return_value=_native_table(table)) as files:
        planner = PostponeBucketPlanner(table)
    assert planner.current_plan().as_dict() == {('existing',): 3}
    assert planner.plan({('new',): (3, 0)}).as_dict() == {('existing',): 3, ('new',): 2}
    files.assert_called_once_with(table)


def test_native_bucket_metadata_reports_core_conflicts(tmp_path, forbid_python_execution):
    from pypaimon.execution.native.write.postpone_bucket import NativePostponeBucketPlanner

    table = _table(tmp_path)
    native = _native_table(table)
    failure = ValueError('Existing postpone files contain conflicting total bucket counts')
    native.new_postpone_fixed_bucket_write_builder.return_value.resolve_bucket_plan.side_effect = failure
    with patch('pypaimon.execution.native.write.postpone_bucket.create_native_write_table', return_value=native):
        with pytest.raises(ValueError, match='conflicting total bucket counts') as raised:
            NativePostponeBucketPlanner(table)
    assert raised.value is failure


def test_native_bucket_plan_keeps_a_partition_named_total_buckets(tmp_path):
    from pypaimon.execution.native.write.postpone_bucket import NativePostponeBucketPlanner

    table = _table(tmp_path, 'total_buckets')
    native = _native_table(table)
    native.new_postpone_fixed_bucket_write_builder.return_value.resolve_bucket_plan.return_value = pa.record_batch([
        pa.array(['west', 'east']), pa.array([3, 7], type=pa.int32())],
        names=['total_buckets', 'total_buckets'])
    with patch('pypaimon.execution.native.write.postpone_bucket.create_native_write_table', return_value=native):
        assert NativePostponeBucketPlanner._load_bucket_metadata(table) == {('west',): 3, ('east',): 7}
