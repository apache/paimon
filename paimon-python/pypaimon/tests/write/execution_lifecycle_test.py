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

"""Execution selection must not allocate resources for an unused backend."""

from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.table_commit import BatchTableCommit, StreamTableCommit


def _table():
    options = Mock()
    options.native_commit_enabled.return_value = True
    options.snapshot_ignore_empty_commit.return_value = False
    return SimpleNamespace(options=options, identifier='test', new_snapshot_commit=Mock())


def _message():
    return CommitMessage((), 0, [], deleted_files=[Mock()])


@pytest.fixture
def native_bridge():
    native = Mock()
    with patch('pypaimon.execution.native.write.native_commit.create_native_commit', return_value=native), \
            patch('pypaimon.execution.native.write.native_commit.native_messages_supported', return_value=True), \
            patch('pypaimon.execution.native.write.native_commit.to_native_commit_messages',
                  return_value=['wire-message']):
        yield native


@pytest.mark.parametrize('operation', ['commit', 'overwrite', 'abort', 'truncate', 'partitions', 'close'])
def test_native_commit_never_constructs_python_resources(native_bridge, operation):
    table = _table()
    table.new_snapshot_commit.side_effect = AssertionError('Python snapshot committer was constructed')
    callback = Mock()
    with patch('pypaimon.execution.python.write.file_store_commit.FileStoreCommit',
               side_effect=AssertionError('Python file committer was constructed')):
        commit = BatchTableCommit(table, 'user', {} if operation == 'overwrite' else None)
        commit.add_commit_callback(callback)
        if operation in ('commit', 'overwrite'):
            commit.commit([_message()])
            if operation == 'overwrite':
                native_bridge.commit.assert_called_once_with(['wire-message'])
            else:
                native_bridge.commit.assert_called_once_with(BATCH_COMMIT_IDENTIFIER, ['wire-message'])
        elif operation == 'abort':
            commit.abort([_message()])
            native_bridge.abort.assert_called_once_with(['wire-message'])
        elif operation == 'truncate':
            commit.truncate_table()
            native_bridge.truncate_table.assert_called_once_with()
        elif operation == 'partitions':
            commit.truncate_partitions([{'pt': 'a'}])
            native_bridge.truncate_partitions.assert_called_once_with([{'pt': 'a'}])
        commit.close()
        commit.close()
    table.new_snapshot_commit.assert_not_called()
    callback.close.assert_called_once_with()
    if operation != 'close':
        native_bridge.close.assert_called_once_with()


def test_python_committer_is_created_once_only_when_fallback_is_selected(native_bridge):
    table = _table()
    pure = Mock()
    callback = Mock()
    with patch('pypaimon.execution.python.write.file_store_commit.FileStoreCommit', return_value=pure) as create:
        commit = StreamTableCommit(table, 'user')
        commit.add_commit_callback(callback)
        create.assert_not_called()
        table.new_snapshot_commit.assert_not_called()
        commit.commit([_message()], 1)
        create.assert_not_called()
        with patch('pypaimon.execution.native.write.native_commit.native_messages_supported', return_value=False):
            commit.commit([_message()], 2)
            commit.commit([_message()], 3)
        assert commit.file_store_commit is pure
        assert commit.file_store_commit is pure
        create.assert_called_once()
        table.new_snapshot_commit.assert_called_once_with()
        assert create.call_args[1]['close_commit_callbacks'] is False
        assert [call[1]['commit_identifier'] for call in pure.commit.call_args_list] == [2, 3]
        commit.close()
        commit.close()
    callback.close.assert_called_once_with()
    pure.close.assert_called_once_with()
    native_bridge.close.assert_called_once_with()


@pytest.mark.parametrize('operation', ['commit', 'overwrite', 'truncate', 'partitions'])
def test_native_publication_failure_never_constructs_python_or_aborts(native_bridge, operation, tmp_path):
    table = _table()
    table.new_snapshot_commit.side_effect = AssertionError('Python fallback after publication')
    prepared_file = tmp_path / 'prepared.parquet'
    prepared_file.write_bytes(b'prepared data')
    callback = Mock()
    failure = TimeoutError('publication outcome is unknown')
    method = {'overwrite': 'commit', 'truncate': 'truncate_table', 'partitions': 'truncate_partitions'}.get(
        operation, 'commit')
    getattr(native_bridge, method).side_effect = failure
    with patch('pypaimon.execution.python.write.file_store_commit.FileStoreCommit',
               side_effect=AssertionError('Python fallback after publication')):
        commit = BatchTableCommit(table, 'user', {} if operation == 'overwrite' else None)
        commit.add_commit_callback(callback)
        with pytest.raises(TimeoutError) as raised:
            if operation in ('commit', 'overwrite'):
                commit.commit([_message()])
            elif operation == 'truncate':
                commit.truncate_table()
            else:
                commit.truncate_partitions([{'pt': 'a'}])
        assert raised.value is failure
        assert prepared_file.read_bytes() == b'prepared data'
        native_bridge.abort.assert_not_called()
        if operation != 'partitions':
            with pytest.raises(RuntimeError, match='one-time'):
                commit.commit([_message()])
        commit.close()
    table.new_snapshot_commit.assert_not_called()
    callback.close.assert_called_once_with()


def test_ignored_empty_commit_and_close_do_not_allocate_either_backend():
    table = _table()
    table.options.snapshot_ignore_empty_commit.return_value = True
    table.new_snapshot_commit.side_effect = AssertionError('unused Python snapshot')
    callback = Mock()
    with patch('pypaimon.execution.native.write.native_commit.create_native_commit',
               side_effect=AssertionError('unused Native committer')):
        commit = BatchTableCommit(table, 'user', None)
        commit.add_commit_callback(callback)
        commit.commit([])
        commit.close()
        commit.close()
    table.new_snapshot_commit.assert_not_called()
    callback.close.assert_called_once_with()


def test_callback_closure_is_owned_once_even_if_native_close_fails(native_bridge):
    table = _table()
    callback = Mock()
    native_bridge.close.side_effect = OSError('Native close failed')
    commit = BatchTableCommit(table, 'user', None)
    commit.add_commit_callback(callback)
    commit.commit([_message()])
    with pytest.raises(OSError, match='Native close failed'):
        commit.close()
    commit.close()
    callback.close.assert_called_once_with()
    table.new_snapshot_commit.assert_not_called()


def _data_evolution_table(tmp_path):
    import pyarrow as pa
    from pypaimon import CatalogFactory, Schema

    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        pa.schema([('id', pa.int64()), ('value', pa.int64())]), options={
            'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
            'write.native.enabled': 'true', 'commit.native.enabled': 'true',
        }), False)
    return catalog.get_table('default.t')


def _core_builder():
    table = Mock()
    builder = table.new_batch_write_builder.return_value
    builder._with_commit_user.return_value = builder
    return table, builder


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('option', [
    'commit.max-retries', 'commit.timeout', 'commit.min-retry-wait',
    'commit.max-retry-wait', 'manifest.target-file-size',
    'data-evolution.row-id-conflict-rewrite.max-size', 'manifest.compression',
])
def test_invalid_commit_configuration_fails_before_allocating_either_backend(
        tmp_path, native, option):
    table = _data_evolution_table(tmp_path).copy({
        option: 'typo', 'commit.native.enabled': str(native).lower()})
    with patch.object(table, 'new_snapshot_commit',
                      side_effect=AssertionError('Python snapshot committer was constructed')) as snapshot, \
            patch('pypaimon.execution.python.write.file_store_commit.FileStoreCommit',
                  side_effect=AssertionError('Python file committer was constructed')) as pure, \
            patch('pypaimon.execution.native.write.native_commit.create_native_commit',
                  side_effect=AssertionError('Native committer was constructed')) as core:
        with pytest.raises(ValueError):
            table.new_batch_write_builder().new_commit()
        snapshot.assert_not_called()
        pure.assert_not_called()
        core.assert_not_called()


def test_native_writer_does_not_construct_python_writer_or_file_index(
        tmp_path):
    import pyarrow as pa
    from pypaimon.write.write_dispatch import NativeTableWrite

    table = _data_evolution_table(tmp_path)
    native_table, builder = _core_builder()
    expected = [_message()]
    with patch('pypaimon.execution.native.write.native_write.native_runtime_available', return_value=True), \
            patch('pypaimon.execution.native.write.native_write.create_native_write_table',
                  return_value=native_table), \
            patch('pypaimon.execution.native.write.native_write.from_native_commit_messages', return_value=expected), \
            patch('pypaimon.execution.python.write.table_write.PythonTableWriteMixin.__init__',
                  side_effect=AssertionError('Python data writer was constructed')), \
            patch('pypaimon.execution.python.write.row_id_file_index.RowIdFileIndex.from_table',
                  side_effect=AssertionError('Python file index was built')):
        writer = table.new_batch_write_builder().new_write()
        assert isinstance(writer, NativeTableWrite)
        writer.write_arrow_batch(pa.record_batch([[1], [10]], names=['id', 'value']))
        assert writer.prepare_commit() is expected
        writer.close()
    builder.new_write.return_value.write_arrow.assert_called_once()
    builder.new_write.return_value.close.assert_called_once_with()


def test_native_row_id_updater_does_not_construct_python_updater_or_file_index(tmp_path):
    import pyarrow as pa
    from pypaimon.write.table_update_by_row_id import TableUpdateByRowId

    table = _data_evolution_table(tmp_path)
    native_table, builder = _core_builder()
    expected = [_message()]
    with patch('pypaimon.execution.native.write.native_update._native_row_id_table', return_value=native_table), \
            patch('pypaimon.execution.native.write.native_update.from_native_commit_messages', return_value=expected), \
            patch('pypaimon.execution.python.write.table_update_by_row_id.PythonRowIdUpdateMixin.__init__',
                  side_effect=AssertionError('Python row-id updater was constructed')), \
            patch('pypaimon.execution.python.write.row_id_file_index.RowIdFileIndex.from_table',
                  side_effect=AssertionError('Python file index was built')):
        updater = table.new_batch_write_builder().new_update().new_update_by_row_id()
        assert isinstance(updater, TableUpdateByRowId)
        updates = pa.table({'_ROW_ID': [0], 'value': [11]})
        assert updater.update_columns(updates, ['value']) is expected
    builder.new_update.return_value.new_update_by_row_id.return_value.update_columns.assert_called_once_with(
        updates, ['value'])


def test_native_merge_does_not_construct_python_writer_or_file_index(tmp_path):
    import pyarrow as pa
    from pypaimon.write.merge_types import WhenMatched, source_col

    table = _data_evolution_table(tmp_path)
    native_table, builder = _core_builder()
    expected = [_message()]
    with patch('pypaimon.execution.native.write.native_merge_into._native_row_id_table', return_value=native_table), \
            patch('pypaimon.execution.native.write.native_merge_into.from_native_commit_messages',
                  return_value=expected), \
            patch('pypaimon.execution.python.write.table_write.PythonTableWriteMixin.__init__',
                  side_effect=AssertionError('Python writer was constructed')), \
            patch('pypaimon.execution.python.write.row_id_file_index.RowIdFileIndex.from_table',
                  side_effect=AssertionError('Python file index was built')):
        result = table.new_batch_write_builder().new_update().merge_into(
            pa.table({'id': [1], 'value': [11]}), on=['id'],
            when_matched=[WhenMatched.update({'value': source_col('value')})])
        assert result is expected
    builder.new_update.return_value.merge_into.assert_called_once()


def test_failed_python_construction_closes_snapshot_and_preserves_original_error():
    table = _table()
    table.options.native_commit_enabled.return_value = False
    snapshot = table.new_snapshot_commit.return_value
    snapshot.close.side_effect = OSError('cleanup failed')
    callback = Mock()
    failure = ValueError('Python constructor failed')
    with patch('pypaimon.execution.python.write.file_store_commit.FileStoreCommit', side_effect=failure):
        commit = BatchTableCommit(table, 'user', None)
        commit.add_commit_callback(callback)
        table.new_snapshot_commit.assert_not_called()
        with pytest.raises(ValueError) as caught:
            commit.commit([_message()])
        assert caught.value is failure
        snapshot.close.assert_called_once_with()
        callback.close.assert_not_called()
        commit.close()
        commit.close()
    callback.close.assert_called_once_with()


def test_python_close_failure_still_closes_callbacks_once():
    table = _table()
    table.options.native_commit_enabled.return_value = False
    pure = Mock()
    pure.close.side_effect = OSError('Python close failed')
    callback = Mock()
    with patch('pypaimon.execution.python.write.file_store_commit.FileStoreCommit', return_value=pure):
        commit = StreamTableCommit(table, 'user')
        commit.add_commit_callback(callback)
        commit.commit([_message()], 1)
        with pytest.raises(OSError, match='Python close failed'):
            commit.close()
        commit.close()
    pure.close.assert_called_once_with()
    callback.close.assert_called_once_with()


def test_native_abort_error_does_not_repeat_cleanup_through_python(native_bridge):
    table = _table()
    table.new_snapshot_commit.side_effect = AssertionError('Python cleanup was constructed')
    failure = OSError('native cleanup failed after deleting some files')
    native_bridge.abort.side_effect = failure
    commit = StreamTableCommit(table, 'user')
    with pytest.raises(OSError) as caught:
        commit.abort([_message()])
    assert caught.value is failure
    table.new_snapshot_commit.assert_not_called()
    native_bridge.abort.assert_called_once_with(['wire-message'])
    commit.close()


def test_native_callback_error_keeps_publication_outside_fallback_and_closes_once(native_bridge):
    from pypaimon.common.json_util import JSON
    from pypaimon.snapshot.snapshot import Snapshot

    table = _table()
    table.new_snapshot_commit.side_effect = AssertionError('callback failure must not select Python')
    callback = Mock()
    failure = LookupError('notification failed after publication')
    callback.call.side_effect = failure
    follower = Mock()
    follower.close.side_effect = RuntimeError('callback close failed')
    snapshot = Snapshot(
        version=3, id=7, schema_id=0, base_manifest_list='', delta_manifest_list='',
        total_record_count=0, delta_record_count=0, commit_user='user',
        commit_identifier=BATCH_COMMIT_IDENTIFIER, commit_kind='APPEND', time_millis=0)
    context = SimpleNamespace(
        commit_entries=[], identifier=BATCH_COMMIT_IDENTIFIER,
        _snapshot_json=lambda: JSON.to_json(snapshot))

    def publish(*args, **kwargs):
        native_bridge.add_commit_callback.call_args[0][0](context)

    native_bridge.commit.side_effect = publish
    commit = BatchTableCommit(table, 'user', None)
    commit.add_commit_callback(callback)
    commit.add_commit_callback(follower)
    with pytest.raises(LookupError) as caught:
        commit.commit([_message()])
    assert caught.value is failure
    assert callback.call.call_args[0][0].snapshot.id == 7
    callback.call.assert_called_once()
    follower.call.assert_not_called()
    native_bridge.abort.assert_not_called()
    table.new_snapshot_commit.assert_not_called()
    commit.close()
    commit.close()
    callback.close.assert_called_once_with()
    follower.close.assert_called_once_with()


@pytest.mark.parametrize('stream', [False, True])
@pytest.mark.parametrize('written', [False, True])
def test_native_writer_attribute_queries_never_select_python(tmp_path, stream, written):
    import pyarrow as pa
    from pypaimon.write.write_dispatch import NativeTableWrite
    from pypaimon.write.table_write import TableWrite

    table = _data_evolution_table(tmp_path)
    core = Mock()
    writer = NativeTableWrite(table, 'user', None, stream, core, 7)
    with patch('pypaimon.execution.python.write.table_write.PythonTableWriteMixin.__init__',
               side_effect=AssertionError('attribute query constructed Python writer')):
        if written:
            writer.write_arrow_batch(pa.record_batch([[1], [2]], names=['id', 'value']))
        for name in ('flush', 'missing_attribute'):
            assert not hasattr(writer, name)
            with pytest.raises(AttributeError):
                getattr(writer, name)
        # Every declared TableWrite method is explicit on the Native adapter.
        for name in dir(TableWrite):
            if not name.startswith('_') and callable(getattr(TableWrite, name)):
                assert callable(getattr(writer, name))
        assert writer.table_pyarrow_schema.names == ['id', 'value']
        assert writer.restore_snapshot_id == 7
        if stream:
            assert not hasattr(writer, 'batch_committed')
        else:
            assert writer.batch_committed is False
        assert writer._python_writer is None
        core.close.assert_not_called()
        writer.close()
    core.close.assert_called_once_with()


def test_native_writer_explicit_python_diagnostics_keep_lazy_fallback(tmp_path):
    from pypaimon.write.write_dispatch import NativeTableWrite

    table = _data_evolution_table(tmp_path)
    core = Mock()
    pure = SimpleNamespace(
        file_store_write=object(), row_key_extractor=object(),
        table_pyarrow_schema=object(), restore_snapshot_id=3, batch_committed=False)
    writer = NativeTableWrite(table, 'user', None, False, core)
    with patch.object(writer, '_new_python_writer', return_value=pure) as create:
        assert writer.file_store_write is pure.file_store_write
        assert writer.row_key_extractor is pure.row_key_extractor
        assert writer.table_pyarrow_schema is pure.table_pyarrow_schema
        assert writer.restore_snapshot_id == 3
        assert writer.batch_committed is False
        writer.file_store_write = 'files'
        writer.row_key_extractor = 'router'
        writer.table_pyarrow_schema = 'schema'
        writer.restore_snapshot_id = 5
        writer.batch_committed = True
        assert writer.file_store_write == 'files'
        assert writer.row_key_extractor == 'router'
        assert writer.table_pyarrow_schema == 'schema'
        assert writer.restore_snapshot_id == 5
        assert writer.batch_committed is True
        assert not hasattr(writer, 'missing_attribute')
        create.assert_called_once_with()
        core.close.assert_called_once_with()


@pytest.mark.parametrize('attribute', [
    'file_store_write', 'row_key_extractor', 'table_pyarrow_schema', 'restore_snapshot_id', 'batch_committed',
])
def test_native_writer_diagnostic_assignment_selects_python_before_changing_configuration(tmp_path, attribute):
    from pypaimon.write.write_dispatch import NativeTableWrite

    table = _data_evolution_table(tmp_path)
    core = Mock()
    writer = NativeTableWrite(table, 'user', None, False, core)
    pure = SimpleNamespace()
    value = object()
    with patch.object(writer, '_new_python_writer', return_value=pure) as create:
        setattr(writer, attribute, value)
        assert getattr(pure, attribute) is value
        create.assert_called_once_with()
        core.close.assert_called_once_with()


def test_native_batch_preparation_failure_retains_public_lifecycle_state_without_python(tmp_path):
    from pypaimon.write.write_dispatch import NativeTableWrite

    core = Mock()
    failure = OSError('batch preparation failed')
    core.prepare_commit.side_effect = failure
    writer = NativeTableWrite(_data_evolution_table(tmp_path), 'user', None, False, core)
    with patch('pypaimon.execution.python.write.table_write.PythonTableWriteMixin.__init__',
               side_effect=AssertionError('lifecycle query constructed Python writer')):
        assert writer.batch_committed is False
        with pytest.raises(OSError) as raised:
            writer.prepare_commit()
        assert raised.value is failure
        assert writer.batch_committed is True
        assert writer._python_writer is None
    writer.close()


@pytest.mark.parametrize('failed', [False, True])
def test_explicit_python_diagnostics_preserve_empty_native_batch_preparation_state(tmp_path, failed):
    from pypaimon.write.write_dispatch import NativeTableWrite

    core = Mock()
    if failed:
        core.prepare_commit.side_effect = OSError('batch preparation failed')
    else:
        core.prepare_commit.return_value = []
    writer = NativeTableWrite(_data_evolution_table(tmp_path), 'user', None, False, core)
    with patch('pypaimon.execution.native.write.native_write.from_native_commit_messages', return_value=[]):
        if failed:
            with pytest.raises(OSError, match='batch preparation failed'):
                writer.prepare_commit()
        else:
            assert writer.prepare_commit() == []
    assert writer.batch_committed is True
    assert writer._python_writer is None

    # Explicit diagnostic access may select Python, but cannot reopen a batch
    # whose one preparation attempt has already finished or failed.
    assert writer.file_store_write is writer._python_writer.file_store_write
    assert writer.batch_committed is True
    with pytest.raises(RuntimeError, match='one-time committing'):
        writer.prepare_commit()
    core.prepare_commit.assert_called_once_with()
    core.close.assert_called_once_with()
    writer.close()


def test_importing_native_write_does_not_initialize_python_group_by_kernels():
    import os
    import subprocess
    import sys
    from pathlib import Path

    script = '''
from unittest.mock import patch
import pyarrow as pa
with patch.object(pa, 'table', wraps=pa.table) as arrow_table:
    import pypaimon.execution.native.write.native_write
    import pypaimon.execution.python.write.row_key_extractor as pure
    assert pure._ARROW_GROUP_BY_SUPPORTED is None
    arrow_table.assert_not_called()
'''
    environment = os.environ.copy()
    environment['PYTHONPATH'] = str(Path(__file__).resolve().parents[3])
    result = subprocess.run([sys.executable, '-c', script], env=environment,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            universal_newlines=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize('supported', [False, True])
def test_python_grouping_probes_capability_only_when_selected_and_caches_result(supported):
    from pypaimon.execution.python.write import row_key_extractor

    with patch.object(row_key_extractor, '_ARROW_GROUP_BY_SUPPORTED', None), \
            patch.object(row_key_extractor, '_probe_arrow_group_by', return_value=supported) as probe:
        probe.assert_not_called()
        assert row_key_extractor._arrow_group_by_supported() is supported
        assert row_key_extractor._arrow_group_by_supported() is supported
        probe.assert_called_once_with()
