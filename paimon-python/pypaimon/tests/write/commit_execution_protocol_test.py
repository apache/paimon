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

"""Both commit executors implement the same publication and lifecycle protocol."""

import inspect
from unittest.mock import Mock

import pytest

from pypaimon.execution.native.write.commit import NativeTableCommit
from pypaimon.execution.python.write.table_commit import PythonTableCommit
from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.table_commit import BatchTableCommit


@pytest.mark.parametrize('method', [
    'commit', 'abort', 'truncate_table', 'truncate_partitions', 'close',
])
def test_commit_executors_share_operation_protocol(method):
    assert inspect.signature(getattr(PythonTableCommit, method)) == inspect.signature(
        getattr(NativeTableCommit, method))


def _executor(backend, core, overwrite=None):
    if backend == 'native':
        return NativeTableCommit(core, is_batch=overwrite is not None)
    return PythonTableCommit(core, overwrite_partition=overwrite)


@pytest.mark.parametrize('backend', ['python', 'native'])
@pytest.mark.parametrize('overwrite', [None, {}, {'pt': 'a'}])
@pytest.mark.parametrize('properties', [None, {'source': 'protocol'}])
def test_commit_executor_preserves_scope_messages_identifier_and_properties(
        backend, overwrite, properties):
    core = Mock()
    executor = _executor(backend, core, overwrite)
    messages = [object()]
    identifier = BATCH_COMMIT_IDENTIFIER if overwrite is not None else 17
    executor.commit(messages, identifier, properties)
    kwargs = {} if properties is None else {'snapshot_properties': properties}
    if backend == 'native':
        if overwrite is None:
            core.commit.assert_called_once_with(identifier, messages, **kwargs)
        else:
            core.commit.assert_called_once_with(messages, **kwargs)
    else:
        kwargs.update(commit_messages=messages, commit_identifier=identifier)
        if overwrite is None:
            core.commit.assert_called_once_with(**kwargs)
            core.overwrite.assert_not_called()
        else:
            core.overwrite.assert_called_once_with(overwrite_partition=overwrite, **kwargs)
            core.commit.assert_not_called()
    core.abort.assert_not_called()


@pytest.mark.parametrize('backend', ['python', 'native'])
@pytest.mark.parametrize('overwrite', [None, {}])
def test_commit_executor_preserves_prepared_files_when_publication_outcome_is_unknown(
        tmp_path, backend, overwrite):
    prepared = tmp_path / 'prepared.parquet'
    prepared.write_bytes(b'prepared')
    failure = TimeoutError('publication response was lost')
    core = Mock()
    getattr(core, 'overwrite' if backend == 'python' and overwrite is not None else 'commit').side_effect = failure
    executor = _executor(backend, core, overwrite)
    with pytest.raises(TimeoutError) as caught:
        executor.commit([object()], BATCH_COMMIT_IDENTIFIER)
    assert caught.value is failure
    assert prepared.read_bytes() == b'prepared'
    core.abort.assert_not_called()


@pytest.mark.parametrize('backend', ['python', 'native'])
def test_partition_truncate_does_not_consume_the_append_commit(backend):
    core = Mock()
    executor = (NativeTableCommit(core, is_batch=True) if backend == 'native'
                else PythonTableCommit(core))
    partitions = [{'pt': 'a'}]
    executor.truncate_partitions(partitions, BATCH_COMMIT_IDENTIFIER)
    executor.commit([], BATCH_COMMIT_IDENTIFIER)
    if backend == 'native':
        core.truncate_partitions.assert_called_once_with(partitions)
        core.commit.assert_called_once_with([])
    else:
        core.drop_partitions.assert_called_once_with(partitions, BATCH_COMMIT_IDENTIFIER)
        core.commit.assert_called_once_with(commit_messages=[], commit_identifier=BATCH_COMMIT_IDENTIFIER)


@pytest.mark.parametrize('backend', ['python', 'native'])
@pytest.mark.parametrize('failure', [False, True])
def test_public_commit_calls_one_executor_protocol_without_replay(backend, failure):
    options = Mock()
    options.native_commit_enabled.return_value = backend == 'native'
    options.snapshot_ignore_empty_commit.return_value = False
    table = Mock(options=options, identifier='default.test')
    owner = BatchTableCommit(table, 'user', None)
    executor = Mock()
    message = CommitMessage((), 0, [], deleted_files=[Mock()])
    messages = [message]
    prepared = messages if backend == 'python' else ['wire-message']
    owner._prepare_native_commit = Mock(return_value=(executor, prepared) if backend == 'native' else None)
    if backend == 'python':
        owner._python_commit = executor
    if failure:
        executor.commit.side_effect = TimeoutError('outcome unknown')
        with pytest.raises(TimeoutError, match='outcome unknown'):
            owner.commit(messages)
    else:
        owner.commit(messages, snapshot_properties={'checkpoint': 'batch'})
    executor.commit.assert_called_once_with(
        prepared, BATCH_COMMIT_IDENTIFIER,
        snapshot_properties=None if failure else {'checkpoint': 'batch'})
    table.new_snapshot_commit.assert_not_called()
    executor.abort.assert_not_called()
    with pytest.raises(RuntimeError, match='one-time'):
        owner.commit(messages)
