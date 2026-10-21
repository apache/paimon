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

"""Stream identifiers obey Java long and Rust i64 before execution starts."""

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock, patch

import numpy as np
import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.execution.native.write.native_write import NativeTableWrite as NativeWrite
from pypaimon.write.commit_identifier import validate_commit_identifier
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.table_commit import StreamTableCommit
from pypaimon.write.table_write import StreamTableWrite
from pypaimon.write.write_dispatch import NativeBatchTableWrite

pytestmark = [pytest.mark.python_write, pytest.mark.python_commit]


class IndexIdentifier:
    def __init__(self, value):
        self.value = value
        self.calls = 0

    def __index__(self):
        self.calls += 1
        return self.value


@pytest.mark.parametrize('value', [
    -(1 << 63), -1, 0, 1, (1 << 63) - 1, np.int64(42), np.int32(17), True,
])
def test_commit_identifier_preserves_the_signed_long_domain(value):
    normalized = validate_commit_identifier(value)
    assert type(normalized) is int
    assert normalized == value


def test_commit_identifier_accepts_the_index_protocol_once():
    identifier = IndexIdentifier(42)
    assert validate_commit_identifier(identifier) == 42
    assert identifier.calls == 1


_INVALID = [
    pytest.param(None, TypeError, id='none'),
    pytest.param(1.5, TypeError, id='float'),
    pytest.param('1', TypeError, id='text'),
    pytest.param(Decimal('1'), TypeError, id='decimal'),
    pytest.param(1 << 63, OverflowError, id='overflow'),
    pytest.param(-(1 << 63) - 1, OverflowError, id='underflow'),
]


@pytest.mark.parametrize('value, error', _INVALID)
def test_stream_prepare_rejects_invalid_identifiers_before_preparing_files(value, error):
    writer = object.__new__(StreamTableWrite)
    writer._prepare_commit = Mock(side_effect=AssertionError('files were prepared'))
    with pytest.raises(error):
        writer.prepare_commit(value)
    writer._prepare_commit.assert_not_called()


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('value, error', _INVALID)
def test_stream_commit_rejects_invalid_identifiers_before_selecting_a_backend(native, value, error):
    table = Mock()
    table.options.native_commit_enabled.return_value = native
    table.options.snapshot_ignore_empty_commit.return_value = False
    commit = StreamTableCommit(table, 'user')
    commit._prepare_native_commit = Mock(side_effect=AssertionError('Native preparation started'))
    messages = [CommitMessage((), 0, [], deleted_files=[Mock()])]
    with pytest.raises(error):
        commit.commit(messages, value)
    commit._prepare_native_commit.assert_not_called()
    table.new_snapshot_commit.assert_not_called()
    assert commit._python_commit is None
    commit.close()


@pytest.mark.parametrize('value, error', _INVALID)
def test_native_stream_prepare_validates_before_crossing_ffi(value, error):
    writer = object.__new__(NativeWrite)
    writer.stream = True
    writer._native_writer = Mock()
    with pytest.raises(error):
        writer.prepare_commit(value)
    writer._native_writer.prepare_commit.assert_not_called()


def test_native_stream_prepare_transports_the_normalized_index_value():
    writer = object.__new__(NativeWrite)
    writer.stream = True
    writer._native_writer = Mock()
    writer.table = object()
    identifier = IndexIdentifier(42)
    with patch('pypaimon.execution.native.write.native_write.from_native_commit_messages', return_value=[]):
        assert writer.prepare_commit(identifier) == []
    writer._native_writer.prepare_commit.assert_called_once_with(True, 42)
    assert identifier.calls == 1


def test_batch_writer_keeps_its_signature_when_explicit_diagnostics_select_python():
    writer = object.__new__(NativeBatchTableWrite)
    writer._execution = SimpleNamespace(stream=False)
    writer._python_writer = Mock()
    with pytest.raises(TypeError):
        writer.prepare_commit(42)
    writer._python_writer.prepare_commit.assert_not_called()


@pytest.mark.parametrize('identifier', [-(1 << 63), 42, (1 << 63) - 1])
def test_python_stream_publishes_only_a_normalized_long_identifier(tmp_path, identifier):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.ids', Schema.from_pyarrow_schema(
        pa.schema([('id', pa.int32())]), options={
            'scan.native-plan.enabled': 'false', 'read.native.enabled': 'false',
            'write.native.enabled': 'false', 'commit.native.enabled': 'false',
        }), False)
    table = catalog.get_table('default.ids')
    builder = table.new_stream_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.record_batch([pa.array([1], pa.int32())], names=['id']))
        messages = writer.prepare_commit(IndexIdentifier(identifier))
        paths = [file.physical_path() for message in messages for file in message.new_files]
        assert paths
        with pytest.raises(OverflowError):
            commit.commit(messages, 1 << 63)
        assert table.snapshot_manager().get_latest_snapshot() is None
        assert all(table.file_io.exists(path) for path in paths)
        commit.commit(messages, np.int64(identifier))
        snapshot = table.snapshot_manager().get_latest_snapshot()
        assert snapshot.commit_identifier == identifier
        assert type(snapshot.commit_identifier) is int
    finally:
        writer.close()
        commit.close()
