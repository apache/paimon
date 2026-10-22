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

from pathlib import Path
import subprocess
import sys
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon.execution.python.write.row_id_file_index import RowIdFileIndex
from pypaimon.execution.python.write.table_delete import PythonTableDeleteByRowId
from pypaimon.execution.python.write.table_update_by_row_id import PythonTableUpdateByRowId
from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
from pypaimon.write.table_delete import TableDeleteByRowId
from pypaimon.write.table_update_by_row_id import TableUpdateByRowId


def test_direct_row_id_update_caches_executor_and_delegates_messages():
    table, execution = Mock(), Mock()
    data = pa.table({'_ROW_ID': [0], 'value': [10]})
    messages = execution.update_columns.return_value
    with patch('pypaimon.write.backend_dispatch.create_row_id_update_execution', return_value=execution) as prepare, \
            patch.object(PythonTableUpdateByRowId, 'initialize', side_effect=AssertionError('Python initialized')):
        update = TableUpdateByRowId(table, 'user', BATCH_COMMIT_IDENTIFIER)
        assert update.update_columns(data, ['value']) is messages
        assert update.commit_messages is execution.commit_messages
        update._write_row_columns(data, ['value'], {'value': [10]})
        prepare.assert_called_once_with(table, 'user', BATCH_COMMIT_IDENTIFIER, None)
    execution.update_columns.assert_called_once_with(data, ['value'])
    execution._write_row_columns.assert_called_once_with(data, ['value'], {'value': [10]})
    assert not hasattr(update, '_merge_update_with_original')
    assert not hasattr(update, '_write_by_first_row_id')


def test_direct_row_id_execution_error_never_initializes_python():
    execution = Mock()
    execution.update_columns.side_effect = RuntimeError('already staged')
    with patch('pypaimon.write.backend_dispatch.create_row_id_update_execution', return_value=execution), \
            patch.object(PythonTableUpdateByRowId, 'initialize', side_effect=AssertionError('Python fallback')):
        update = TableUpdateByRowId(Mock(), 'user', BATCH_COMMIT_IDENTIFIER)
        with pytest.raises(RuntimeError, match='already staged'):
            update.update_columns(pa.table({'_ROW_ID': [0], 'value': [10]}), ['value'])


def test_direct_delete_caches_executor_and_preserves_execution_error():
    execution = Mock()
    with patch('pypaimon.write.backend_dispatch.create_row_id_delete_execution', return_value=execution) as prepare, \
            patch.object(PythonTableDeleteByRowId, 'initialize', side_effect=AssertionError('Python initialized')):
        table = Mock()
        delete = TableDeleteByRowId(table)
        assert delete.delete([1, 1]) is execution.delete.return_value
        execution.delete.side_effect = RuntimeError('already staged')
        with pytest.raises(RuntimeError, match='already staged'):
            delete.delete([2])
        prepare.assert_called_once_with(table, None)
    assert not hasattr(delete, '_scan_anchor_ranges')
    assert not hasattr(delete, '_write_deletion_vector_index')


def test_selected_python_row_id_coordinator_reuses_captured_index():
    info = RowIdFileIndex(snapshot_id=7, first_row_ids=[100])
    with patch('pypaimon.write.backend_dispatch.create_row_id_update_execution',
               side_effect=AssertionError('backend selected again')), \
            patch.object(RowIdFileIndex, 'from_table', side_effect=AssertionError('snapshot replanned')):
        update = TableUpdateByRowId._from_python(Mock(), 'user', BATCH_COMMIT_IDENTIFIER, info)
        captured = update._snapshot_files_info()
        assert captured.snapshot_id == 7
        assert captured.first_row_ids == [100]
        assert captured.first_row_id_index is info.first_row_id_index
        assert update.commit_messages == []
        assert isinstance(update._execution, PythonTableUpdateByRowId)


def test_selected_python_delete_coordinator_keeps_anchor_snapshot():
    table = Mock()
    anchors = [Mock()]
    with patch('pypaimon.write.backend_dispatch.create_row_id_delete_execution',
               side_effect=AssertionError('backend selected again')), \
            patch.object(PythonTableDeleteByRowId, '_scan_anchor_ranges', return_value=(7, anchors)) as scan:
        delete = TableDeleteByRowId._from_python(table)
        captured = delete._snapshot_anchor_ranges()
        assert captured.snapshot_id == 7
        assert captured.anchors is anchors
        scan.assert_called_once_with()
        assert isinstance(delete._execution, PythonTableDeleteByRowId)


def test_builder_row_id_fallback_prepares_native_only_once():
    from pypaimon.write.table_update import BatchTableUpdate

    table = Mock()
    index = RowIdFileIndex(snapshot_id=7, first_row_ids=[])
    with patch('pypaimon.write.update_dispatch._authorize_update'), \
            patch('pypaimon.write.update_dispatch.prepare_native_write_operation', return_value=None) as prepare, \
            patch('pypaimon.write.backend_dispatch.prepare_native_write_operation',
                  side_effect=AssertionError('Native prepared twice')), \
            patch.object(RowIdFileIndex, 'from_table', return_value=index):
        update = BatchTableUpdate(table, 'user').new_update_by_row_id()
    prepare.assert_called_once_with('update_by_row_id', table, 'user', BATCH_COMMIT_IDENTIFIER)
    assert update._snapshot_files_info().snapshot_id == 7


@pytest.mark.parametrize('operation', ['update', 'delete'])
def test_direct_native_row_id_api_does_not_import_python_execution(operation):
    source = r'''
import importlib.abc
import sys
from types import SimpleNamespace

class BlockPython(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.startswith('pypaimon.execution.python'):
            raise AssertionError('Native imported Python execution: ' + fullname)

sys.meta_path.insert(0, BlockPython())
import pyarrow as pa
from pypaimon.execution.native.write import native_update
from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
from pypaimon.write.table_delete import TableDeleteByRowId
from pypaimon.write.table_update_by_row_id import TableUpdateByRowId

class Execution:
    commit_messages = ['message']
    def update_columns(self, data, columns):
        assert data['_ROW_ID'].to_pylist() == [3] and columns == ['value']
        return self.commit_messages
    def delete(self, row_ids):
        assert row_ids == [3]
        return self.commit_messages

native_update.create_native_update_by_row_id = lambda *args: Execution()
native_update.create_native_row_id_delete = lambda *args: Execution()
table = SimpleNamespace(options=SimpleNamespace(native_write_enabled=lambda: True), identifier=object(),
                        catalog_environment=SimpleNamespace(table_query_auth=lambda *args: None))
if sys.argv[1] == 'update':
    update = TableUpdateByRowId(table, 'user', BATCH_COMMIT_IDENTIFIER)
    assert update.update_columns(pa.table({'_ROW_ID': [3], 'value': [10]}), ['value']) == ['message']
else:
    assert TableDeleteByRowId(table).delete([3]) == ['message']
assert not any(name.startswith('pypaimon.execution.python') for name in sys.modules)
'''
    result = subprocess.run([sys.executable, '-c', source, operation],
                            cwd=str(Path(__file__).resolve().parents[3]),
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            universal_newlines=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
