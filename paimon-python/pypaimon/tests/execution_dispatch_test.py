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

"""Backend boundaries and preparation-only fallback contracts."""

import ast
from pathlib import Path
import subprocess
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from pypaimon.write import backend_dispatch as dispatch


@pytest.mark.parametrize('result', [None, [], False, 0])
def test_preparation_preserves_empty_results(result):
    assert dispatch.try_prepare_native(lambda: result) is result


def test_preparation_failure_allows_fallback():
    factory = Mock(side_effect=ImportError('optional runtime unavailable'))
    assert dispatch.try_prepare_native(factory) is None
    factory.assert_called_once_with()


def test_fork_error_is_never_retried_by_python():
    class ForkSafetyError(RuntimeError):
        pass

    failure = ForkSafetyError('inherited native SDK state')
    wrapped = RuntimeError('preparation failed')
    wrapped.__cause__ = failure
    runtime = ModuleType('pypaimon_rust')
    runtime.ForkSafetyError = ForkSafetyError
    with patch.dict(sys.modules, {'pypaimon_rust': runtime}), \
            patch('pypaimon.execution.native.runtime.native_runtime_available', return_value=True):
        with pytest.raises(ForkSafetyError) as caught:
            dispatch.try_prepare_native(Mock(side_effect=wrapped))
    assert caught.value is failure


def test_native_write_error_cannot_trigger_python_replay():
    failure = OSError('write may already have created a file')
    native = SimpleNamespace(write_arrow_batch=Mock(side_effect=failure))
    table = SimpleNamespace(options=SimpleNamespace(native_write_enabled=lambda: True))
    with patch('pypaimon.read.merge_engine_support.check_sequence_field_supported'), \
            patch('pypaimon.execution.native.write.native_write.create_native_write', return_value=native), \
            patch.object(dispatch, 'create_python_write') as python_write:
        writer = dispatch.create_write(table, 'writer')
        with pytest.raises(OSError) as caught:
            writer.write_arrow_batch('batch')
        assert caught.value is failure
        python_write.assert_not_called()


def test_native_row_id_updater_keeps_the_public_return_type():
    from pypaimon.execution.native.write.native_update import NativeTableUpdateByRowId
    from pypaimon.write.table_update_by_row_id import TableUpdateByRowId

    native = object.__new__(NativeTableUpdateByRowId)
    with patch('pypaimon.execution.native.write.native_update.create_native_update_by_row_id',
               return_value=native):
        table = SimpleNamespace(options=SimpleNamespace(native_write_enabled=lambda: True))
        result = dispatch.prepare_native_write_operation('update_by_row_id', table, 'writer', 1)
    assert result is native
    assert isinstance(result, TableUpdateByRowId)
    assert issubclass(NativeTableUpdateByRowId, TableUpdateByRowId)


@pytest.mark.parametrize('phase', ['prepare', 'execute'])
def test_merge_failure_never_replays_python(phase):
    from pypaimon.table.data_evolution_merge_into import merge_into

    failure = RuntimeError('source input may already have been consumed')
    table = SimpleNamespace(options=SimpleNamespace(native_write_enabled=lambda: True))
    native = SimpleNamespace(prepare_commit=Mock(side_effect=failure))
    with patch('pypaimon.execution.native.write.native_merge_into.create_native_merge_into',
               side_effect=failure if phase == 'prepare' else None, return_value=native), \
            patch('pypaimon.execution.python.write.merge_into.merge_into') as python_merge:
        with pytest.raises(RuntimeError) as caught:
            merge_into(table, object(), on=['id'], commit_user='writer')
    assert caught.value is failure
    python_merge.assert_not_called()


def test_writer_fallback_stops_before_the_first_native_mutation():
    native = Mock()
    python = object()
    owner = SimpleNamespace(
        _python_writer=None, _written=False, _native_writer=native,
        _new_python_writer=Mock(return_value=python))
    assert dispatch.switch_writer_to_python(owner) is python
    assert dispatch.switch_writer_to_python(owner) is python
    native.close.assert_called_once_with()
    owner._new_python_writer.assert_called_once_with()

    committed_owner = SimpleNamespace(
        _python_writer=None, _written=True, _native_writer=Mock(), _new_python_writer=Mock())
    with pytest.raises(RuntimeError, match='after native data was written'):
        dispatch.switch_writer_to_python(committed_owner)
    committed_owner._native_writer.close.assert_not_called()
    committed_owner._new_python_writer.assert_not_called()


@pytest.mark.parametrize(('backend', 'forbidden'), [('python', 'native'), ('native', 'python')])
def test_backend_implementations_do_not_import_each_other(backend, forbidden):
    root = Path(__file__).parents[1] / 'execution' / backend
    forbidden_prefix = 'pypaimon.execution.' + forbidden
    violations = []
    for path in sorted(root.rglob('*.py')):
        for node in ast.walk(ast.parse(path.read_text(encoding='utf-8'))):
            imports = ([node.module] if isinstance(node, ast.ImportFrom)
                       else [alias.name for alias in node.names] if isinstance(node, ast.Import) else [])
            if any(name and (name == forbidden_prefix or name.startswith(forbidden_prefix + '.'))
                   for name in imports):
                violations.append('{}:{}'.format(path.relative_to(root), node.lineno))
    assert not violations, violations


def test_python_backend_imports_without_loading_the_native_extension():
    source = '''
import importlib.abc
import sys

class BlockNativeExtension(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'pypaimon_rust' or fullname.startswith('pypaimon_rust.'):
            raise AssertionError('Python execution loaded the Native extension: ' + fullname)

sys.meta_path.insert(0, BlockNativeExtension())
from pypaimon.execution.python.read.table_read import PythonTableReadMixin
from pypaimon.execution.python.read.table_scan import PythonTableScanMixin
from pypaimon.write.table_write import BatchTableWrite, StreamTableWrite
from pypaimon.write.table_commit import BatchTableCommit, StreamTableCommit
from pypaimon.write.table_update import BatchTableUpdate, StreamTableUpdate
from pypaimon.execution.native.read import native_search_plan
'''
    result = subprocess.run([sys.executable, '-c', source], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            universal_newlines=True, timeout=30)
    assert result.returncode == 0, result.stderr


def test_shared_scan_requests_and_update_authorization_do_not_load_backends():
    source = '''
import importlib.abc
import sys
from types import SimpleNamespace

class BlockExecution(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.startswith(('pypaimon.execution.', 'pypaimon_rust')):
            raise AssertionError('Shared request loaded an executor: ' + fullname)

sys.meta_path.insert(0, BlockExecution())
from pypaimon.read.scan_request import ScanRequest
from pypaimon.write.update_validation import _authorize_update

request = ScanRequest().with_shard(1, 3)
assert (request.idx_of_this_subtask, request.number_of_para_subtasks) == (1, 3)
try:
    ScanRequest().with_shard(3, 3)
except ValueError:
    pass
else:
    raise AssertionError('Invalid shard request was accepted')
table = SimpleNamespace(
    options=object(), identifier='db.table',
    catalog_environment=SimpleNamespace(table_query_auth=lambda options, identifier: None))
_authorize_update(table)
'''
    result = subprocess.run([sys.executable, '-c', source], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            universal_newlines=True, timeout=30)
    assert result.returncode == 0, result.stderr
