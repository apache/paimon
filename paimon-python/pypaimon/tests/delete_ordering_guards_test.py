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

import json
from dataclasses import replace
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.execution.python.write.file_store_commit import FileStoreCommit
from pypaimon.read.streaming_table_scan import StreamTableScan


def _table(tmp_path, extra=None):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    options = {
        'bucket': '1',
        'read.native.enabled': 'false',
        'write.native.enabled': 'false',
        'commit.native.enabled': 'false',
    }
    options.update(extra or {})
    catalog.create_table('default.guards', Schema.from_pyarrow_schema(
        pa.schema([('id', pa.int32()), ('value', pa.string())]),
        primary_keys=['id'], options=options), False)
    return catalog.get_table('default.guards')


def _stage(builder, key):
    writer = builder.new_write()
    try:
        writer.write_arrow(pa.table({'id': pa.array([key], pa.int32()), 'value': ['payload']}))
        return writer.prepare_commit()
    finally:
        writer.close()


def _append(table, key):
    builder = table.new_batch_write_builder()
    messages = _stage(builder, key)
    commit = builder.new_commit()
    try:
        commit.commit(messages)
    finally:
        commit.close()


def _guard_snapshot(table, properties):
    manager = table.snapshot_manager()
    snapshot = manager.get_latest_snapshot()
    path = manager.get_snapshot_path(snapshot.id)
    metadata = json.loads(table.file_io.read_file_utf8(path))
    metadata['properties'] = properties
    # A client must reject guard history before opening any referenced index.
    metadata['indexManifest'] = 'guard-index-manifest'
    table.file_io.overwrite_file_utf8(path, json.dumps(metadata))
    return metadata


@pytest.mark.parametrize('native', [False, True])
def test_enabled_guards_reject_planning_and_writing_before_backend_selection(tmp_path, native):
    table = _table(tmp_path, {
        'delete-ordering-guards.enabled': 'true',
        'scan.native-plan.enabled': str(native).lower(),
        'read.native.enabled': str(native).lower(),
        'write.native.enabled': str(native).lower(),
    })
    with patch('pypaimon.read.read_dispatch._native_scan_execution') as planner:
        for operation in ('plan', 'plan_all', 'scan_with_stats'):
            with pytest.raises(NotImplementedError, match='does not support delete ordering guards'):
                getattr(table.new_read_builder().new_scan(), operation)()
        planner.assert_not_called()
    with patch('pypaimon.write.backend_dispatch.try_prepare_native') as writer:
        with pytest.raises(NotImplementedError, match='does not support delete ordering guards'):
            table.new_batch_write_builder().new_write()
        writer.assert_not_called()


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('properties', [
    {'delete-ordering-guards.max-sequence-number': '1'},
    {'delete-ordering-guards.generations': '{"version":"1","*":"reset"}'},
    {'delete-ordering-guards.partitions': '{}'},
])
def test_retained_guard_history_rejects_reads_and_staged_commits(tmp_path, native, properties):
    table = _table(tmp_path)
    _append(table, 1)
    builder = table.new_read_builder()
    splits = builder.new_scan().plan().splits()
    write_builder = table.new_batch_write_builder()
    messages = _stage(write_builder, 2)
    commit = write_builder.new_commit()
    protected = _guard_snapshot(table, properties)
    selected = table.copy({
        'scan.native-plan.enabled': str(native).lower(),
        'read.native.enabled': str(native).lower(),
        'write.native.enabled': str(native).lower(),
        'commit.native.enabled': str(native).lower(),
    })
    selected_commit = selected.new_batch_write_builder().new_commit()
    try:
        with pytest.raises(NotImplementedError, match='retained guard history'):
            selected.new_read_builder().new_scan().plan()
        with patch('pypaimon.execution.native.read.table_read.NativeTableRead') as native_reader:
            for operation in ('to_arrow', 'to_arrow_batch_reader', 'to_iterator'):
                with pytest.raises(NotImplementedError, match='retained guard history'):
                    result = getattr(selected.new_read_builder().new_read(), operation)(splits)
                    if operation == 'to_iterator':
                        next(result)
            native_reader.assert_not_called()
        with pytest.raises(NotImplementedError, match='retained guard history'):
            selected.new_batch_write_builder().new_write()
        for operation, args in (('truncate_table', ()), ('truncate_partitions', ([],))):
            truncate = selected.new_batch_write_builder().new_commit()
            try:
                with patch.object(truncate, '_prepare_native_commit') as native_commit:
                    with pytest.raises(NotImplementedError, match='retained guard history'):
                        getattr(truncate, operation)(*args)
                    native_commit.assert_not_called()
            finally:
                truncate.close()
        for committer in (commit, selected_commit):
            with patch.object(committer, '_prepare_native_commit') as native_commit:
                with pytest.raises(NotImplementedError, match='retained guard history'):
                    committer.commit(messages)
                native_commit.assert_not_called()
        manager = table.snapshot_manager()
        assert manager.get_latest_snapshot().id == protected['id']
        assert json.loads(table.file_io.read_file_utf8(
            manager.get_snapshot_path(protected['id']))) == protected
    finally:
        commit.close()
        selected_commit.close()


def test_commit_retry_checks_its_exact_snapshot(tmp_path):
    table = _table(tmp_path)
    _append(table, 1)
    ordinary = table.snapshot_manager().get_latest_snapshot()
    guarded = replace(ordinary, properties={'delete-ordering-guards.max-sequence-number': '1'})
    commit = FileStoreCommit(table.new_snapshot_commit(), table, 'retry')
    try:
        with pytest.raises(NotImplementedError, match='delete ordering guards'):
            commit._try_commit_once(None, 'APPEND', [], [], 2, guarded)
        assert table.snapshot_manager().get_latest_snapshot().properties == ordinary.properties
    finally:
        commit.close()


@pytest.mark.python_plan
def test_stream_rechecks_guard_history_before_each_frame(tmp_path):
    table = _table(tmp_path)
    _append(table, 1)
    stream = StreamTableScan(table)
    try:
        checkpoint = stream.checkpoint()
        _guard_snapshot(table, {'delete-ordering-guards.max-sequence-number': '1'})
        with pytest.raises(NotImplementedError, match='delete ordering guards'):
            stream.plan()
        assert stream.checkpoint() == checkpoint
    finally:
        stream.close()


def test_ordinary_primary_key_table_remains_readable_and_writable(tmp_path):
    table = _table(tmp_path)
    _append(table, 1)
    builder = table.new_read_builder()
    rows = builder.new_read().to_arrow(builder.new_scan().plan().splits())
    assert rows.to_pylist() == [{'id': 1, 'value': 'payload'}]
