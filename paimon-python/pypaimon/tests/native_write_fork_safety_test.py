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

"""An inherited native SDK must not silently select another mutation path."""

import sys
from contextlib import ExitStack, contextmanager
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon.tests.native_write_routing_test import SCHEMA, _table
from pypaimon.write.table_upsert_by_key import TableUpsertByKey


pytestmark = [pytest.mark.python_write, pytest.mark.python_commit]
_OPERATIONS = ['batch-write', 'stream-write', 'commit', 'upsert', 'row-id-updater', 'update', 'batch-update',
               'predicate-update', 'delete', 'predicate-delete', 'delete-match']


class _ForkSafetyError(RuntimeError):
    pass


class _PythonFallbackEntered(RuntimeError):
    pass


@contextmanager
def _operation(tmp_path, operation):
    table = _table(tmp_path, mode='data-evolution', use_native=True)
    data = pa.table({'id': pa.array([1], pa.int64()), 'pt': ['a'], 'value': ['new']}, schema=SCHEMA)
    updater = table.new_batch_write_builder().new_update().with_update_type(['value'])
    pure = Mock(side_effect=_PythonFallbackEntered)
    with ExitStack() as context:
        if operation.endswith('write'):
            streaming = operation == 'stream-write'
            builder = table.new_stream_write_builder() if streaming else table.new_batch_write_builder()
            name = 'StreamTableWrite' if streaming else 'BatchTableWrite'
            context.enter_context(patch('pypaimon.write.table_write.' + name, pure))
            factory = 'create_native_write'
            module = 'pypaimon.execution.native.write.native_write.'
            invoke = builder.new_write
        elif operation == 'commit':
            committer = table.new_batch_write_builder().new_commit()
            context.callback(committer.close)
            context.enter_context(patch.object(committer.file_store_commit, 'commit', pure))
            context.enter_context(patch(
                'pypaimon.execution.native.write.native_commit.native_messages_supported', return_value=True))
            message = Mock()
            message.is_empty.return_value = False
            factory, module = 'create_native_commit', 'pypaimon.execution.native.write.native_commit.'
            invoke = lambda: committer.commit([message])
        else:
            module = 'pypaimon.execution.native.write.native_update.'
            if operation == 'upsert':
                upsert = TableUpsertByKey(table, 'test', 7)
                context.enter_context(patch.object(upsert, '_group_by_partition', pure))
                factory = 'create_native_upsert'
                invoke = lambda: upsert.upsert(data, ['id'])
            elif operation == 'row-id-updater':
                context.enter_context(patch('pypaimon.write.table_update.TableUpdateByRowId', pure))
                factory, invoke = 'create_native_update_by_row_id', updater.new_update_by_row_id
            elif operation in ('update', 'batch-update'):
                data = data.append_column('_ROW_ID', pa.array([0], pa.int64()))
                if operation == 'update':
                    context.enter_context(patch.object(updater, '_new_row_id_updater', pure))
                    invoke = lambda: updater.update_by_arrow_with_row_id(data)
                else:
                    context.enter_context(patch.object(updater, '_update_by_arrow_batches_with_row_id', pure))
                    invoke = lambda: updater.update_by_arrow_batches_with_row_id([data])
                factory = 'create_native_update'
            elif operation == 'predicate-update':
                context.enter_context(patch.object(updater, '_matched_update_scan_table', pure))
                factory = 'create_native_predicate_update'
                invoke = lambda: updater.update_by_predicate(None, {'value': 'new'})
            elif operation == 'delete':
                context.enter_context(patch('pypaimon.execution.python.write.table_update.TableDeleteByRowId', pure))
                factory = 'create_native_delete'
                invoke = lambda: updater.delete_by_row_id([0])
            elif operation == 'predicate-delete':
                context.enter_context(patch.object(updater, '_partition_only_delete_filter', pure))
                factory = 'create_native_predicate_delete'
                invoke = lambda: updater.delete_by_predicate(None)
            else:
                read_builder = Mock()
                read_builder.new_scan.return_value.plan_all_files.return_value.splits.return_value = [object()]
                context.enter_context(patch.object(updater, '_matched_update_scan_table', return_value=SimpleNamespace(
                    new_read_builder=lambda: read_builder)))
                read_builder.new_read = pure
                factory = 'native_predicate_row_ids'
                invoke = lambda: updater._matched_delete_row_ids(None)
        yield invoke, module + factory, pure


@pytest.mark.parametrize('operation', _OPERATIONS)
@pytest.mark.parametrize('chain', ['direct', 'cause', 'context'])
def test_native_preparation_propagates_fork_errors_without_python_mutations(tmp_path, operation, chain):
    module = ModuleType('pypaimon_rust')
    module.ForkSafetyError = _ForkSafetyError
    fork = _ForkSafetyError('OSS C++ SDK cannot be used after fork; use spawn workers')
    error = fork
    if chain != 'direct':
        error = RuntimeError('native preparation failed')
        setattr(error, '__cause__' if chain == 'cause' else '__context__', fork)
    with _operation(tmp_path, operation) as (invoke, factory, pure), patch.dict(
            sys.modules, {'pypaimon_rust': module}), patch(
            'pypaimon.execution.native.runtime.native_runtime_available', return_value=True), patch(
            factory, side_effect=error):
        with pytest.raises(_ForkSafetyError) as raised:
            invoke()
        assert raised.value is fork
        pure.assert_not_called()


@pytest.mark.parametrize('operation', _OPERATIONS)
@pytest.mark.parametrize('missing_runtime', [False, True], ids=['ordinary-error', 'missing-runtime'])
def test_ordinary_native_preparation_failures_keep_the_safe_python_fallback(tmp_path, operation, missing_runtime):
    error = ImportError('pypaimon-rust not installed') if missing_runtime else RuntimeError('native unavailable')
    module = ModuleType('pypaimon_rust')
    module.ForkSafetyError = _ForkSafetyError
    with _operation(tmp_path, operation) as (invoke, factory, pure), patch.dict(
            sys.modules, {'pypaimon_rust': module}), patch(
            'pypaimon.execution.native.runtime.native_runtime_available', return_value=not missing_runtime), patch(
            factory, side_effect=error):
        with pytest.raises(_PythonFallbackEntered):
            invoke()
        pure.assert_called_once()


@pytest.mark.parametrize('phase', ['prepare', 'execute'])
def test_merge_native_failures_never_restart_python_execution(tmp_path, phase):
    from pypaimon.table.data_evolution_merge_into import merge_into

    table = _table(tmp_path, mode='data-evolution', use_native=True)
    failure = _ForkSafetyError('Jindo SDK cannot be reused after process fork')
    native = Mock()
    native.prepare_commit.side_effect = failure
    with patch('pypaimon.execution.native.write.native_merge_into.create_native_merge_into',
               side_effect=failure if phase == 'prepare' else None, return_value=native), patch(
            'pypaimon.write.merge_inputs._prepare', side_effect=_PythonFallbackEntered) as pure:
        with pytest.raises(_ForkSafetyError) as raised:
            merge_into(table, pa.table({'id': [1]}), on='id', commit_user='job')
        assert raised.value is failure
        pure.assert_not_called()
