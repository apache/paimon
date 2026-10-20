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

"""Unused changelog format options do not prevent native Parquet writes."""

from contextlib import ExitStack
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.read.native_plan import native_runtime_available
from pypaimon.write.native_write import NativeTableWrite, create_native_write


pytestmark = [pytest.mark.python_write, pytest.mark.python_commit]
_NATIVE = pytest.mark.skipif(not native_runtime_available(), reason='pypaimon-rust is not installed')
_ENGINES = [False, pytest.param(True, marks=_NATIVE)]
_SCHEMA = pa.schema([('id', pa.int32()), ('pt', pa.string()), ('value', pa.int32())])


def _table(catalog, primary_key, producer='none'):
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        _SCHEMA, partition_keys=['pt'], primary_keys=['id', 'pt'] if primary_key else [], options={
            'file.format': 'parquet', 'bucket': '1' if primary_key else '-1',
            'changelog-producer': producer, 'changelog-file.format': 'avro',
            'dynamic-partition-overwrite': 'false',
            'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
            'write.native.enabled': 'false', 'commit.native.enabled': 'false',
        }), False)
    return catalog.get_table('default.t')


@pytest.mark.parametrize('primary_key,stream,partition,producer', [
    (False, False, None, 'none'), (False, True, None, 'none'),
    (True, False, None, 'none'), (True, True, None, 'none'),
    (True, False, {}, 'input'), (True, False, {'pt': 'a'}, 'input'),
])
def test_inactive_changelog_format_qualifies_for_native_write(
        native_rest_catalog, primary_key, stream, partition, producer):
    table = _table(native_rest_catalog, primary_key, producer)
    native_table = Mock()
    with patch('pypaimon.write.native_write.native_runtime_available', return_value=True), \
            patch('pypaimon.write.native_write.create_native_write_table', return_value=native_table) as create:
        writer = create_native_write(table, 'job', static_partition=partition, stream=stream)
    assert isinstance(writer, NativeTableWrite)
    create.assert_called_once_with(table)
    builder = (native_table.new_stream_write_builder.return_value.with_commit_user.return_value if stream
               else native_table.new_batch_write_builder.return_value._with_commit_user.return_value)
    if partition is not None:
        builder.with_overwrite.assert_called_once_with(partition)
        builder = builder.with_overwrite.return_value
    assert writer._native_writer is builder.new_write.return_value
    writer.close()


@pytest.mark.parametrize('stream,partition', [(False, None), (True, None), (True, {})])
def test_active_non_parquet_input_changelog_keeps_existing_fallback(
        native_rest_catalog, stream, partition):
    table = _table(native_rest_catalog, True, 'input')
    with patch('pypaimon.write.native_write.native_runtime_available', return_value=True), \
            patch('pypaimon.write.native_write.create_native_write_table') as create:
        assert create_native_write(table, 'job', static_partition=partition, stream=stream) is None
    create.assert_not_called()


def _write(table, rows, stream=False, overwrite=None):
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    if overwrite is not None:
        builder.overwrite(overwrite)
    native = table.options.native_write_enabled()
    with ExitStack() as stack:
        if native:
            stack.enter_context(patch.object(
                NativeTableWrite, '_switch_to_python', side_effect=AssertionError('Python write fallback')))
        writer, commit = builder.new_write(), builder.new_commit()
        if native:
            assert isinstance(writer, NativeTableWrite)
            for method in ('commit', 'overwrite'):
                stack.enter_context(patch.object(
                    commit.file_store_commit, method, side_effect=AssertionError('Python commit fallback')))
        try:
            messages = []
            # Stream checkpoints exercise reuse after the first prepared batch.
            for identifier, group in enumerate(([row] for row in rows) if stream else [rows], 1):
                writer.write_arrow(pa.Table.from_pylist(group, schema=_SCHEMA))
                current = writer.prepare_commit(identifier) if stream else writer.prepare_commit()
                assert current
                assert all(file.file_name.endswith('.parquet')
                           for message in current for file in message.new_files)
                assert all(not message.changelog_files for message in current)
                if stream:
                    commit.commit(current, identifier)
                else:
                    commit.commit(current)
                messages.extend(current)
        finally:
            writer.close()
            commit.close()
    assert table.snapshot_manager().get_latest_snapshot().changelog_manifest_list is None
    return messages


def _rows(table):
    builder = table.new_read_builder()
    return builder.new_read().to_arrow(builder.new_scan().plan().splits()).sort_by('id').to_pylist()


@pytest.mark.parametrize('native', _ENGINES)
@pytest.mark.parametrize('primary_key', [False, True], ids=['append', 'pk'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
def test_parquet_writes_ignore_unused_non_parquet_changelog_format(
        native_rest_catalog, native, primary_key, stream):
    table = _table(native_rest_catalog, primary_key).copy({
        'write.native.enabled': str(native).lower(), 'commit.native.enabled': str(native).lower()})
    rows = [dict(id=1, pt='a', value=10), dict(id=2, pt='b', value=20)]
    _write(table, rows, stream)
    assert _rows(table) == rows


@pytest.mark.parametrize('native', _ENGINES)
@pytest.mark.parametrize('primary_key', [False, True], ids=['append', 'pk'])
@pytest.mark.parametrize('partition', [{}, {'pt': 'a'}], ids=['all', 'partition'])
def test_overwrite_suppresses_inactive_non_parquet_input_changelog(
        native_rest_catalog, native, primary_key, partition):
    table = _table(native_rest_catalog, primary_key)
    seed = [dict(id=1, pt='a', value=10), dict(id=2, pt='b', value=20)]
    _write(table, seed)
    table = table.copy({'changelog-producer': 'input' if primary_key else 'none',
                        'write.native.enabled': str(native).lower(),
                        'commit.native.enabled': str(native).lower()})
    replacement = [dict(id=3, pt='a', value=30)]
    _write(table, replacement, overwrite=partition)
    expected = ([seed[1]] if partition else []) + replacement
    assert _rows(table) == sorted(expected, key=lambda row: row['id'])
