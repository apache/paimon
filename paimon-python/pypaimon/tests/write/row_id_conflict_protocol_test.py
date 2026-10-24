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

from dataclasses import replace
import pickle
import sys
from types import ModuleType
from unittest.mock import Mock, patch

import pytest

from pypaimon.tests.write.conflict_detection_test import _make_entry
from pypaimon.execution.python.write.commit.conflict_detection import ConflictDetection
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.row_id_conflict import RowIdExistenceConflict, check_row_id_conflicts


def _native_error_module():
    module = ModuleType('pypaimon_rust')

    class NativeRowIdConflict(RuntimeError):
        pass

    module.RowIdExistenceConflict = NativeRowIdConflict
    return module


def _native_conflict(module):
    error = module.RowIdExistenceConflict('an opaque transport message')
    error.file_name = "opaque '(4): file.parquet"
    error.first_row_id = 4
    error.row_count = 8
    error.bucket = 3
    error.current_first_row_id = None
    error.current_row_count = 9
    return error


@pytest.mark.parametrize('from_entry', [False, True])
def test_structured_conflict_survives_worker_serialization(from_entry):
    if from_entry:
        error = RowIdExistenceConflict(_make_entry('staged.parquet', first_row_id=7))
    else:
        error = RowIdExistenceConflict._from_fields(
            file_name='staged.parquet', first_row_id=7,
            row_count=100, bucket=0, current_row_count=101)
    restored = pickle.loads(pickle.dumps(error))
    assert str(restored) == str(error)
    assert restored.file_name == error.file_name
    assert restored.first_row_id == error.first_row_id
    assert restored.current_row_count == error.current_row_count
    assert (restored.entry is None) == (error.entry is None)


@pytest.mark.parametrize('batch', [False, True])
def test_native_commit_translates_only_typed_row_id_conflicts(batch):
    from pypaimon.execution.native.write.commit import NativeTableCommit
    from pypaimon.ray.row_id_conflict_rewriter import _find_row_id_conflict

    module = _native_error_module()
    native = _native_conflict(module)
    core = Mock()
    core.commit.side_effect = native
    commit = NativeTableCommit(core, is_batch=batch)
    with patch.dict(sys.modules, {'pypaimon_rust': module}):
        with pytest.raises(RowIdExistenceConflict) as raised:
            commit.commit([], 7)
    error = raised.value
    assert error.__cause__ is native
    assert error.entry is None
    assert (error.file_name, error.first_row_id, error.row_count, error.bucket) == (
        "opaque '(4): file.parquet", 4, 8, 3)
    assert error.current_first_row_id is None
    assert error.current_row_count == 9
    assert _find_row_id_conflict(error) is error
    core.abort.assert_not_called()


@pytest.mark.parametrize('opaque_message', ['unknown commit result', 'Row ID existence conflict: not a typed error'])
@pytest.mark.parametrize('native_available', [False, True])
def test_native_unknown_outcome_is_not_a_rewrite_signal(opaque_message, native_available):
    from pypaimon.execution.native.write.commit import NativeTableCommit
    from pypaimon.ray.row_id_conflict_rewriter import _find_row_id_conflict

    module = _native_error_module()
    failure = ValueError(opaque_message)
    failure.__cause__ = _native_conflict(module)
    core = Mock()
    core.commit.side_effect = failure
    with patch.dict(sys.modules, {'pypaimon_rust': module if native_available else None}):
        with pytest.raises(ValueError) as raised:
            NativeTableCommit(core, is_batch=True).commit([], 7)
    assert raised.value is failure
    assert _find_row_id_conflict(failure) is None
    core.abort.assert_not_called()


@pytest.mark.parametrize('refuse', [False, True])
def test_native_logical_check_never_constructs_python_commit_or_replays(refuse):
    message = CommitMessage((), 0, [_make_entry('staged.parquet', first_row_id=0).file], check_from_snapshot=1)
    table = Mock()
    table.options.native_commit_enabled.return_value = True
    core = Mock()
    failure = RuntimeError('logical overlap')
    if refuse:
        core.check_row_id_conflicts.side_effect = failure
        core.close.side_effect = RuntimeError('closing failed')
    with patch('pypaimon.write.update_validation._authorize_update') as authorize, patch(
            'pypaimon.execution.native.write.native_commit.prepare_native_row_id_conflict_check',
            return_value=(core, ['wire-message'])) as prepare, patch(
            'pypaimon.execution.python.write.commit.row_id_conflict.check_row_id_conflicts') as pure, patch(
            'pypaimon.execution.python.write.file_store_commit.FileStoreCommit') as publisher:
        if refuse:
            with pytest.raises(RuntimeError) as raised:
                check_row_id_conflicts(table, [message], 2, 1)
            assert raised.value is failure
        else:
            check_row_id_conflicts(table, [message], 2, 1)
    authorize.assert_called_once_with(table)
    prepare.assert_called_once_with(table, [message])
    core.check_row_id_conflicts.assert_called_once_with(['wire-message'], 2, 1)
    core.close.assert_called_once_with()
    core.abort.assert_not_called()
    pure.assert_not_called()
    publisher.assert_not_called()


@pytest.mark.parametrize('failure', [PermissionError('grant refused'), ValueError('unknown transport failure')])
def test_native_check_preparation_errors_are_never_replayed(failure):
    table = Mock()
    table.options.native_commit_enabled.return_value = True
    message = CommitMessage((), 0, [], check_from_snapshot=1)
    with patch('pypaimon.write.update_validation._authorize_update'), patch(
            'pypaimon.execution.native.write.native_commit.prepare_native_row_id_conflict_check',
            side_effect=failure), patch(
            'pypaimon.execution.python.write.commit.row_id_conflict.check_row_id_conflicts') as pure:
        with pytest.raises(type(failure)) as raised:
            check_row_id_conflicts(table, [message], 2, 1)
    assert raised.value is failure
    pure.assert_not_called()


@pytest.mark.parametrize('latest,baseline', [
    (2, 0), (1, 2), (0, 1), (2, -1), (2, 2), (2.5, 1), (True, 1), (2, False), (1 << 63, 1)])
def test_check_rejects_invalid_baseline_before_authorization_or_io(latest, baseline):
    table = Mock()
    message = CommitMessage((), 0, [], check_from_snapshot=1)
    with patch('pypaimon.write.update_validation._authorize_update') as authorize:
        with pytest.raises(ValueError):
            check_row_id_conflicts(table, [message], latest, baseline)
    authorize.assert_not_called()
    table.snapshot_manager.assert_not_called()


@pytest.mark.parametrize('baseline', [None, True, 1.0, '1', -1, 1 << 63])
def test_check_rejects_invalid_message_baseline_before_io(baseline):
    table = Mock()
    message = CommitMessage((), 0, [], check_from_snapshot=baseline)
    with patch('pypaimon.write.update_validation._authorize_update') as authorize:
        with pytest.raises(ValueError):
            check_row_id_conflicts(table, [message], 2, 1)
    authorize.assert_not_called()
    table.snapshot_manager.assert_not_called()


def test_pure_logical_check_uses_history_without_a_publisher(tmp_path):
    import pyarrow as pa
    from pypaimon import CatalogFactory, Schema

    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', False)
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        pa.schema([('id', pa.int32()), ('name', pa.string())]), options={
            'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true',
            'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
            'write.native.enabled': 'false', 'commit.native.enabled': 'false'}), False)
    table = catalog.get_table('default.t')
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    writer.write_arrow(pa.table({'id': pa.array([1, 2], pa.int32()), 'name': ['a', 'b']}))
    initial = writer.prepare_commit()
    writer.close()
    commit = builder.new_commit()
    commit.commit(initial)
    commit.close()
    update = table.new_batch_write_builder().new_update().with_update_type(['name'])
    changes = update.update_by_arrow_with_row_id(pa.table({'_ROW_ID': [0, 1], 'name': ['x', 'y']}))
    commit = table.new_batch_write_builder().new_commit()
    commit.commit(changes)
    commit.close()
    staged = replace(changes[0], new_files=[replace(changes[0].new_files[0], file_name='still-prepared.parquet')])
    before = sorted(path.name for path in tmp_path.rglob('*') if path.is_file())
    with patch('pypaimon.execution.python.write.file_store_commit.FileStoreCommit',
               side_effect=AssertionError('a publisher was constructed')), patch.object(
            table, 'new_snapshot_commit', side_effect=AssertionError('publication route requested')):
        with pytest.raises(RuntimeError, match='MERGE INTO') as raised:
            check_row_id_conflicts(table, [staged], 2, 1)
        assert not isinstance(raised.value, RowIdExistenceConflict)
        disjoint = replace(staged, new_files=[replace(staged.new_files[0], write_cols=['id'])])
        check_row_id_conflicts(table, [disjoint], 2, 1)
    assert before == sorted(path.name for path in tmp_path.rglob('*') if path.is_file())


@pytest.mark.parametrize('first_row_id,row_count', [(10, 100), (None, 100), (0, 200)])
def test_delete_rejects_reassigned_current_file(first_row_id, row_count):
    detection = ConflictDetection(True, None, None, None, None)
    current = _make_entry('same-file.parquet', first_row_id=first_row_id, row_count=row_count)
    stale = _make_entry('same-file.parquet', kind=1, first_row_id=0)
    conflict = detection.check_row_id_existence([current], [stale], None)
    assert conflict is not None
    assert conflict.entry is stale
    assert conflict.file_name == 'same-file.parquet'
    assert conflict.first_row_id == 0
    assert conflict.row_count == 100
    assert conflict.current_first_row_id == first_row_id
    assert conflict.current_row_count == row_count


def test_conflict_type_remains_available_from_previous_module():
    from pypaimon.write.row_id_conflict import RowIdExistenceConflict
    from pypaimon.execution.python.write.commit.conflict_detection import RowIdExistenceConflict as Previous

    assert Previous is RowIdExistenceConflict
    entry = _make_entry('stale.parquet', first_row_id=7)
    error = RowIdExistenceConflict(entry)
    assert error.entry is entry
    assert (error.file_name, error.first_row_id, error.row_count, error.bucket) == ('stale.parquet', 7, 100, 0)
    assert error.current_row_count is None


def test_ray_conflict_module_does_not_import_python_execution():
    # A fresh process catches imports hidden by the module cache of this test run.
    import subprocess
    import sys

    program = '''
import importlib.abc
import sys
class Guard(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'pypaimon.execution.python' or fullname.startswith('pypaimon.execution.python.'):
            raise AssertionError('Python execution imported: ' + fullname)
sys.meta_path.insert(0, Guard())
import pypaimon.ray.row_id_conflict_rewriter
'''
    subprocess.run([sys.executable, '-c', program], check=True)


def test_native_logical_check_does_not_import_python_execution():
    import subprocess

    program = '''
import importlib.abc
import sys
from tempfile import TemporaryDirectory
from types import ModuleType
from unittest.mock import patch
class Guard(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'pypaimon.execution.python' or fullname.startswith('pypaimon.execution.python.'):
            raise AssertionError('Python execution imported: ' + fullname)
sys.meta_path.insert(0, Guard())
class NativeMessage:
    @staticmethod
    def deserialize(payload, version):
        assert isinstance(payload, bytes) and payload and version == 14
        return NativeMessage()
runtime = ModuleType('pypaimon_rust')
runtime.datafusion = ModuleType('pypaimon_rust.datafusion')
runtime.datafusion.CommitMessage = NativeMessage
sys.modules['pypaimon_rust'] = runtime
sys.modules['pypaimon_rust.datafusion'] = runtime.datafusion
import pyarrow as pa
from pypaimon import CatalogFactory, Schema
from pypaimon.execution.native.write import native_commit
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.row_id_conflict import check_row_id_conflicts
class Core:
    checked = False
    closed = False
    def check_row_id_conflicts(self, messages, latest, baseline):
        from pypaimon_rust.datafusion import CommitMessage as NativeMessage
        assert len(messages) == 1 and isinstance(messages[0], NativeMessage)
        assert (latest, baseline) == (2, 1)
        self.checked = True
    def close(self):
        self.closed = True
with TemporaryDirectory() as path:
    catalog = CatalogFactory.create({'warehouse': path})
    catalog.create_database('default', False)
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        pa.schema([('id', pa.int32())]), options={
            'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true',
            'commit.native.enabled': 'true'}), False)
    table = catalog.get_table('default.t')
    core = Core()
    with patch.object(native_commit, 'create_native_commit', return_value=core), patch.object(
            table, 'new_snapshot_commit', side_effect=AssertionError('publisher requested')):
        check_row_id_conflicts(table, [CommitMessage((), 0, [], check_from_snapshot=1)], 2, 1)
    assert core.checked and core.closed
'''
    subprocess.run([sys.executable, '-c', program], check=True)


def test_ordinary_delete_and_empty_messages_are_not_existence_conflicts():
    current = _make_entry('ordinary.parquet')
    deleted = _make_entry('ordinary.parquet', kind=1)
    detection = ConflictDetection(True, None, None, None, None)
    assert detection.check_row_id_existence([current], [deleted], None) is None
    assert detection.check_row_id_existence([], [], None) is None
    disabled = ConflictDetection(False, None, None, None, None)
    stale = _make_entry('ordinary.parquet', kind=1, first_row_id=7)
    assert disabled.check_row_id_existence([current], [stale], 100) is None
