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

"""Native Ray tasks build captured shards without Python materialization or replay."""

import sys
import subprocess
import textwrap
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.globalindex.create_global_index import GlobalIndexBuilder

pytestmark = [pytest.mark.python_plan, pytest.mark.python_read,
              pytest.mark.python_write, pytest.mark.python_commit]


class _Reference:
    def __init__(self, function, args):
        self.function, self.args, self.done = function, args, False

    def get(self):
        if not self.done:
            self.done = True
            try:
                self.value, self.error = self.function(*self.args), None
            except BaseException as error:
                self.error = error
        if self.error is not None:
            raise self.error
        return self.value


class _Ray(ModuleType):
    def __init__(self):
        super().__init__('ray')
        self.submitted, self.remote_options, self.delivery_failure = [], None, None

    def put(self, value):
        return value

    def remote(self, function):
        owner = self

        class Remote:
            def options(self, **options):
                owner.remote_options = options
                return self

            def remote(self, *args):
                ref = _Reference(function, args)
                owner.submitted.append(ref)
                return ref

        return Remote()

    def wait(self, refs, num_returns):
        return [refs[-1]], refs[:-1]

    def get(self, ref):
        value = ref.get()
        if self.delivery_failure is not None and ref is self.submitted[0]:
            raise self.delivery_failure
        return value


def _request(tmp_path):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([('id', pa.int32()), ('embedding', pa.list_(pa.float32()))])
    catalog.create_table('default.vectors', Schema.from_pyarrow_schema(schema, options={
        'bucket': '-1', 'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'write.native.enabled': 'true', 'read.native.enabled': 'false',
        'scan.native-plan.enabled': 'false', 'commit.native.enabled': 'false',
    }), False)
    return GlobalIndexBuilder(catalog.get_table('default.vectors'), 'embedding', 'ivf-flat', options={
        'ivf-flat.dimension': '2', 'ivf-flat.nlist': '1', 'global-index.row-count-per-shard': '2'})


@pytest.fixture
def native_protocol():
    module = 'pypaimon.execution.native.write.native_index_build'
    core = Mock()
    shards = [SimpleNamespace(ordinal=ordinal) for ordinal in range(3)]
    core.plan.return_value = SimpleNamespace(shards=lambda: shards)
    core.build_shard.side_effect = lambda shard: ['message-' + str(shard.ordinal)]
    native_table = SimpleNamespace(new_global_index_build_builder=lambda: core)
    sdk = ModuleType('pypaimon_rust.datafusion')
    sdk.GlobalIndexBuildBuilder = SimpleNamespace(supports_index_type=lambda kind: True)
    scheduler = _Ray()
    with patch.dict(sys.modules, {'pypaimon_rust.datafusion': sdk, 'ray': scheduler}), \
            patch(module + '.native_runtime_available', return_value=True), \
            patch(module + '._rest_catalog_supported', return_value=True), \
            patch(module + '._native_table', return_value=native_table), \
            patch(module + '.from_native_commit_messages', side_effect=lambda table, messages: messages), \
            patch('pypaimon.execution.python.globalindex.create_global_index.PythonGlobalIndexBuilder',
                  side_effect=AssertionError('Python index execution')):
        yield core, shards, scheduler


def test_native_ray_build_plans_once_and_builds_only_selected_shards(tmp_path, native_protocol):
    core, _, scheduler = native_protocol
    assert _request(tmp_path).build(execution='ray', concurrency=2) == ['message-0', 'message-1', 'message-2']
    core.plan.assert_called_once_with()
    core.build.assert_not_called()
    assert len(scheduler.submitted) == 3
    assert sorted(call[0][0].ordinal for call in core.build_shard.call_args_list) == [0, 1, 2]
    assert scheduler.remote_options['max_retries'] == 0
    assert scheduler.remote_options['retry_exceptions'] is False


def test_native_ray_delivery_failure_preserves_outputs_and_never_replays(tmp_path, native_protocol):
    core, _, scheduler = native_protocol
    request = _request(tmp_path)
    failure = RuntimeError('result delivery failed after the worker wrote its file')
    scheduler.delivery_failure = failure
    with patch.object(request._table.file_io, 'delete_quietly') as delete:
        with pytest.raises(RuntimeError) as raised:
            request.build(execution='ray', concurrency=2)
        assert raised.value is failure
        delete.assert_not_called()
    core.build.assert_not_called()
    assert len(core.build_shard.call_args_list) == 3
    assert all(ref.done for ref in scheduler.submitted)


def test_native_ray_plan_permission_failure_does_not_select_python(tmp_path, native_protocol):
    core, _, scheduler = native_protocol
    failure = PermissionError('index build denied')
    core.plan.side_effect = failure
    with pytest.raises(PermissionError) as raised:
        _request(tmp_path).build(execution='ray', concurrency=2)
    assert raised.value is failure
    core.build_shard.assert_not_called()
    assert not scheduler.submitted


def test_native_ray_empty_plan_does_not_submit_or_select_python(tmp_path, native_protocol):
    core, _, scheduler = native_protocol
    core.plan.return_value = SimpleNamespace(shards=lambda: [])
    assert _request(tmp_path).build(execution='ray', concurrency=2) == []
    core.plan.assert_called_once_with()
    core.build.assert_not_called()
    core.build_shard.assert_not_called()
    assert not scheduler.submitted


def test_native_ray_worker_failure_stops_admission_and_retains_outputs(tmp_path, native_protocol):
    core, _, scheduler = native_protocol
    failure = PermissionError('worker authorization changed')

    def build(shard):
        if shard.ordinal == 1:
            raise failure
        return ['message-' + str(shard.ordinal)]

    core.build_shard.side_effect = build
    request = _request(tmp_path)
    with patch.object(request._table.file_io, 'delete_quietly') as delete:
        with pytest.raises(PermissionError) as raised:
            request.build(execution='ray', concurrency=2)
        assert raised.value is failure
        delete.assert_not_called()
    core.build.assert_not_called()
    assert sorted(call[0][0].ordinal for call in core.build_shard.call_args_list) == [0, 1]
    assert len(scheduler.submitted) == 2
    assert all(ref.done for ref in scheduler.submitted)


def test_native_ray_scheduler_import_does_not_load_python_execution():
    script = """
        import importlib.abc
        import sys

        class RejectPythonExecution(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path=None, target=None):
                if fullname.startswith('pypaimon.execution.python'):
                    raise AssertionError('Python execution imported: ' + fullname)

        sys.meta_path.insert(0, RejectPythonExecution())
        from pypaimon.ray.vector_index_build import build_native_vector_index
        assert callable(build_native_vector_index)
    """
    subprocess.run([sys.executable, '-c', textwrap.dedent(script)], check=True)
