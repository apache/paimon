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

"""Append writers capture factory settings until Java's idle retirement boundary."""

import gc
import weakref
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.schema.data_types import AtomicType, DataField, PyarrowFieldParser
from pypaimon.tests.native_blob_uri_reader_test import Factory
from pypaimon.tests.write.write_factory_reconfiguration_test import _ENGINES, _read, _table, _write

pytestmark = [pytest.mark.python_plan, pytest.mark.python_read,
              pytest.mark.python_write, pytest.mark.python_commit]


def _blob_table(tmp_path, native):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    fields = [DataField(0, 'id', AtomicType('INT')), DataField(1, 'payload', AtomicType('BLOB')),
              DataField(2, 'pt', AtomicType('STRING'))]
    options = {'write.native.enabled': str(native).lower(), 'commit.native.enabled': 'false',
               'scan.native-plan.enabled': 'false', 'read.native.enabled': 'false',
               'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'}
    catalog.create_table('default.blobs', Schema(fields=fields, partition_keys=['pt'], options=options), False)
    return catalog.get_table('default.blobs')


def _blob_rows(table):
    read = table.new_read_builder()
    return sorted(read.new_read().to_arrow(read.new_scan().plan().splits()).to_pylist(),
                  key=lambda row: row['id'])


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('idle_prepares', [0, 1, 2])
@pytest.mark.parametrize('mode', ['arrow', 'row'])
def test_unaware_idle_prepare_reopens_with_current_blob_consumer(tmp_path, native, idle_prepares, mode):
    table = _blob_table(tmp_path, native)
    builder = table.new_stream_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    received = []
    rows = [{'id': 1, 'payload': b'old', 'pt': 'old'}, {'id': 2, 'payload': b'new', 'pt': 'old'}]
    try:
        assert writer.prepare_commit(0) == []
        writer.with_blob_consumer(lambda name, descriptor: received.append(('old', descriptor)) or True)
        _write(writer, table, rows[:1], mode)
        commit.commit(writer.prepare_commit(1), 1)
        for identifier in range(2, 2 + idle_prepares):
            assert writer.prepare_commit(identifier) == []
        writer.with_blob_consumer(lambda name, descriptor: received.append(('new', descriptor)) or True)
        _write(writer, table, rows[1:], mode)
        identifier = 2 + idle_prepares
        commit.commit(writer.prepare_commit(identifier), identifier)
        assert [label for label, _ in received] == ['old', 'new' if idle_prepares else 'old']
        assert [descriptor.length for _, descriptor in received] == [3, 3]
    finally:
        writer.close()
        commit.close()
    assert _blob_rows(table) == rows


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('first_identifier', [-(1 << 63), 0], ids=['minimum', 'ordinary'])
def test_minimum_identifier_remains_first_prepare_until_a_later_nonempty_increment(
        tmp_path, native, first_identifier):
    table = _blob_table(tmp_path, native)
    builder = table.new_stream_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    received = []
    rows = [dict(id=index, payload=value, pt='old')
            for index, value in enumerate([b'one', b'two', b'three'], 1)]
    try:
        writer.with_blob_consumer(lambda name, descriptor: received.append('old') or True)
        _write(writer, table, rows[:1])
        commit.commit(writer.prepare_commit(first_identifier), first_identifier)
        assert writer.prepare_commit(first_identifier + 1) == []
        writer.with_blob_consumer(lambda name, descriptor: received.append('second') or True)
        _write(writer, table, rows[1:2])
        commit.commit(writer.prepare_commit(first_identifier + 1), first_identifier + 1)
        assert received == ['old', 'old' if first_identifier == -(1 << 63) else 'second']
        # A later nonempty increment advances past MIN and permits retirement.
        assert writer.prepare_commit(first_identifier + 2) == []
        writer.with_blob_consumer(lambda name, descriptor: received.append('third') or True)
        _write(writer, table, rows[2:])
        commit.commit(writer.prepare_commit(first_identifier + 3), first_identifier + 3)
        assert received[-1] == 'third'
    finally:
        writer.close()
        commit.close()
    assert _blob_rows(table) == rows


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('factory', [None, Factory()], ids=['clear', 'set'])
def test_idle_retirement_does_not_reopen_uri_reader_factory_configuration(tmp_path, native, factory):
    table = _table(tmp_path, native)
    builder = table.new_stream_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.with_blob_uri_reader_factory(Factory())
        _write(writer, table, [{'id': 1, 'value': 10, 'pt': 'old'}])
        commit.commit(writer.prepare_commit(1), 1)
        assert writer.prepare_commit(2) == []
        if not native:
            assert writer._execution.file_store_write.data_writers == {}
        with pytest.raises(RuntimeError, match='before any write'):
            writer.with_blob_uri_reader_factory(factory)
        _write(writer, table, [{'id': 2, 'value': 20, 'pt': 'old'}])
        commit.commit(writer.prepare_commit(3), 3)
    finally:
        writer.close()
        commit.close()
    assert _read(table) == [
        {'id': 1, 'value': 10, 'pt': 'old'}, {'id': 2, 'value': 20, 'pt': 'old'}]


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('input_', ['row', 'batch', 'table', 'pandas'])
def test_ignored_events_do_not_lock_uri_reader_factory_configuration(tmp_path, native, input_):
    from pypaimon.tests.write.append_row_kind_test import _table as event_table, _write as write_event
    table, schema = event_table(tmp_path, native, **{'ignore-delete': 'true'})
    writer = table.new_stream_write_builder().new_write()
    try:
        write_event(writer, table, schema, [dict(id=1, op='-D', pt='old')], input_, True)
        assert writer.prepare_commit(1) == []
        assert writer.with_blob_uri_reader_factory(Factory()) is writer
        assert writer.with_blob_uri_reader_factory(None) is writer
        write_event(writer, table, schema, [dict(id=2, op='+I', pt='old')], input_, True)
        with pytest.raises(RuntimeError, match='before any write'):
            writer.with_blob_uri_reader_factory(None)
    finally:
        writer.close()


@pytest.mark.parametrize('failure_stage', ['create', 'write'])
def test_uri_reader_factory_locks_once_bucket_creation_succeeds(tmp_path, failure_stage):
    table = _table(tmp_path, False)
    writer = table.new_stream_write_builder().new_write()
    failure = OSError('bucket %s failed' % failure_stage)
    creation = ({'side_effect': failure} if failure_stage == 'create'
                else {'return_value': Mock(write=Mock(side_effect=failure))})
    try:
        with patch.object(writer._execution.file_store_write, '_create_data_writer', **creation):
            with pytest.raises(OSError) as caught:
                _write(writer, table, [{'id': 1, 'value': 10, 'pt': 'old'}])
        assert caught.value is failure
        if failure_stage == 'create':
            assert writer.with_blob_uri_reader_factory(Factory()) is writer
        else:
            with pytest.raises(RuntimeError, match='before any write'):
                writer.with_blob_uri_reader_factory(Factory())
    finally:
        writer.close()


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('latest_commit', ['same-id', 'other-user', 'higher-id'])
def test_fixed_idle_prepare_requires_strictly_newer_commit_of_same_user(tmp_path, native, latest_commit):
    table = _table(tmp_path, native, precomputed=True)
    builder = table.new_stream_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.with_write_type(['id', 'pt'])
        _write(writer, table, [{'id': 1, 'value': 10, 'pt': 'old'}], 'precomputed')
        commit.commit(writer.prepare_commit(1), 1)
        if not native:
            reference = weakref.ref(writer._execution.file_store_write.data_writers[(('old',), 0)])
        # Equality is insufficient, even if another checkpoint is being prepared.
        assert writer.prepare_commit(2) == []
        if not native:
            assert reference() is not None
        expected = [{'id': 1, 'value': None, 'pt': 'old'}]
        if latest_commit == 'other-user':
            other = table.new_stream_write_builder()
            other_writer, other_commit = other.new_write(), other.new_commit()
            try:
                _write(other_writer, table, [{'id': 8, 'value': 80, 'pt': 'helper'}], 'precomputed')
                other_commit.commit(other_writer.prepare_commit(99), 99)
            finally:
                other_writer.close()
                other_commit.close()
            expected.insert(0, {'id': 8, 'value': 80, 'pt': 'helper'})
        elif latest_commit == 'higher-id':
            _write(writer, table, [{'id': 8, 'value': 80, 'pt': 'helper'}], 'precomputed')
            commit.commit(writer.prepare_commit(2), 2)
            expected.insert(0, {'id': 8, 'value': None, 'pt': 'helper'})
        assert writer.prepare_commit(3) == []
        if not native:
            gc.collect()
            assert (reference() is None) == (latest_commit == 'higher-id')
        assert writer.with_write_type(['id', 'pt']) is writer
        _write(writer, table, [{'id': 2, 'value': 20, 'pt': 'old'}], 'precomputed')
        commit.commit(writer.prepare_commit(4), 4)
    finally:
        writer.close()
        commit.close()
    expected.append({'id': 2, 'value': None, 'pt': 'old'})
    assert _read(table) == sorted(expected, key=lambda row: (row['pt'], row['id'] or -1))


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
def test_idle_retirement_releases_captured_blob_consumer(tmp_path, native):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    fields = [DataField(0, 'id', AtomicType('INT')), DataField(1, 'payload', AtomicType('BLOB'))]
    options = {'write.native.enabled': str(native).lower(), 'commit.native.enabled': 'false',
               'scan.native-plan.enabled': 'false', 'read.native.enabled': 'false',
               'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'}
    catalog.create_table('default.blobs', Schema(fields=fields, options=options), False)
    table = catalog.get_table('default.blobs')
    builder = table.new_stream_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    received = []

    class Consumer:
        def __call__(self, name, descriptor):
            received.append((name, descriptor))
            return True

    consumer = Consumer()
    reference = weakref.ref(consumer)
    try:
        writer.with_blob_consumer(consumer)
        schema = PyarrowFieldParser.from_paimon_schema(fields)
        writer.write_arrow(pa.Table.from_pylist([{'id': 1, 'payload': b'old'}], schema=schema))
        commit.commit(writer.prepare_commit(1), 1)
        writer.with_blob_consumer(None)
        del consumer
        gc.collect()
        assert reference() is not None
        assert len(received) == 1
        assert writer.prepare_commit(2) == []
        gc.collect()
        assert reference() is None
        writer.write_arrow(pa.Table.from_pylist([{'id': 2, 'payload': b'new'}], schema=schema))
        commit.commit(writer.prepare_commit(3), 3)
        assert len(received) == 1
    finally:
        writer.close()
        commit.close()
    reader = table.new_read_builder()
    assert sorted(reader.new_read().to_arrow(reader.new_scan().plan().splits()).to_pylist(),
                  key=lambda row: row['id']) == [
        {'id': 1, 'payload': b'old'}, {'id': 2, 'payload': b'new'}]


@pytest.mark.parametrize('missing', [None, FileNotFoundError('expired during read')])
@pytest.mark.parametrize('lookup', ['latest', 'previous'])
def test_fixed_snapshot_search_stops_at_expired_gap(tmp_path, missing, lookup):
    table = _table(tmp_path, False, precomputed=True)
    writer = table.new_stream_write_builder().new_write().with_write_type(['id', 'pt'])
    try:
        _write(writer, table, [{'id': 1, 'value': 10, 'pt': 'old'}], 'precomputed')
        writer.prepare_commit(1)
        latest = SimpleNamespace(id=3, commit_user='other', commit_identifier=99)
        options = {'side_effect': missing} if isinstance(missing, Exception) else {'return_value': None}
        with patch('pypaimon.snapshot.snapshot_manager.SnapshotManager._get_latest_snapshot_from_filesystem',
                   **(options if lookup == 'latest' else {'return_value': latest})), patch(
                'pypaimon.snapshot.snapshot_manager.SnapshotManager.get_snapshot_by_id',
                **options) as get, patch(
                'pypaimon.snapshot.snapshot_manager.SnapshotManager.get_latest_snapshot',
                side_effect=AssertionError('writer cleanup must not use the catalog snapshot loader')):
            assert writer.prepare_commit(2) == []
        assert get.call_args_list == ([] if lookup == 'latest' else [((2,), {})])
        assert (('old',), 0) in writer._execution.file_store_write.data_writers
        _write(writer, table, [{'id': 2, 'value': 20, 'pt': 'old'}], 'precomputed')
        messages = writer.prepare_commit(3)
        assert [file.write_cols for message in messages for file in message.new_files] == [['id', 'pt']]
    finally:
        writer.close()


@pytest.mark.parametrize('lookup', ['latest', 'previous'])
def test_snapshot_lookup_failure_preserves_all_pending_files_for_retry(tmp_path, lookup):
    table = _table(tmp_path, False, precomputed=True)
    writer = table.new_stream_write_builder().new_write()
    try:
        _write(writer, table, [{'id': 1, 'value': 10, 'pt': 'old'}], 'precomputed')
        writer.prepare_commit(1)
        _write(writer, table, [{'id': 2, 'value': 20, 'pt': 'new'}], 'precomputed')
        failure = OSError('snapshot permission denied')
        latest = SimpleNamespace(id=3, commit_user='other', commit_identifier=99)
        before = set(tmp_path.rglob('*.parquet'))
        with patch('pypaimon.snapshot.snapshot_manager.SnapshotManager._get_latest_snapshot_from_filesystem',
                   **({'side_effect': failure} if lookup == 'latest' else {'return_value': latest})), patch(
                'pypaimon.snapshot.snapshot_manager.SnapshotManager.get_snapshot_by_id', side_effect=failure):
            with pytest.raises(OSError) as caught:
                writer.prepare_commit(2)
        assert caught.value is failure
        assert set(tmp_path.rglob('*.parquet')) == before
        messages = writer.prepare_commit(2)
        assert [message.partition for message in messages] == [('new',)]
        assert [file.row_count for message in messages for file in message.new_files] == [1]
    finally:
        writer.close()


@pytest.mark.parametrize('mode', ['arrow', 'row', 'precomputed'])
def test_fixed_active_writers_do_not_lookup_retirement_snapshots(tmp_path, mode):
    table = _table(tmp_path, False, precomputed=True)
    writer = table.new_stream_write_builder().new_write()
    try:
        _write(writer, table, [{'id': 1, 'value': 10, 'pt': 'old'}], mode)
        writer.prepare_commit(1)
        _write(writer, table, [{'id': 1, 'value': 20, 'pt': 'old'}], mode)
        with patch('pypaimon.snapshot.snapshot_manager.SnapshotManager._get_latest_snapshot_from_filesystem',
                   side_effect=AssertionError('active writers need no cleanup lookup')):
            messages = writer.prepare_commit(2)
        assert [file.row_count for message in messages for file in message.new_files] == [1]
    finally:
        writer.close()


@pytest.mark.parametrize('finish', ['retry', 'abort'])
def test_failed_prepare_does_not_retire_idle_writers_or_release_other_files(tmp_path, finish):
    table = _table(tmp_path, False)
    writer = table.new_stream_write_builder().new_write().with_write_type(['id', 'pt'])
    try:
        _write(writer, table, [{'id': 1, 'value': 10, 'pt': 'old'}])
        writer.prepare_commit(1)
        delivered_files = set(tmp_path.rglob('*.parquet'))
        _write(writer, table, [{'id': 2, 'value': 20, 'pt': 'new'},
                               {'id': 3, 'value': 30, 'pt': 'last'}])
        store = writer._execution.file_store_write
        old = store.data_writers[(('old',), 0)]
        last = store.data_writers[(('last',), 0)]
        failure = OSError('last partition cannot prepare')
        with patch.object(last, 'prepare_commit', side_effect=failure), patch.object(old, 'close') as close:
            with pytest.raises(OSError) as caught:
                writer.prepare_commit(2)
            assert caught.value is failure
            close.assert_not_called()
        assert set(store.data_writers) == {(('old',), 0), (('new',), 0), (('last',), 0)}
        if finish == 'abort':
            writer.abort()
            assert set(tmp_path.rglob('*.parquet')) == delivered_files
            return
        messages = writer.prepare_commit(2)
        assert {message.partition for message in messages} == {('new',), ('last',)}
        files = list(tmp_path.rglob('*.parquet'))
        assert len(files) == 3
        writer.abort()
        assert all(path.exists() for path in files)
    finally:
        writer.close()


def test_retirement_close_failure_keeps_active_files_for_retry(tmp_path):
    table = _table(tmp_path, False)
    writer = table.new_stream_write_builder().new_write()
    try:
        _write(writer, table, [{'id': 1, 'value': 10, 'pt': 'old'}])
        writer.prepare_commit(1)
        _write(writer, table, [{'id': 2, 'value': 20, 'pt': 'new'}])
        store = writer._execution.file_store_write
        old = store.data_writers[(('old',), 0)]
        failure = OSError('cannot close idle writer')
        with patch.object(old, 'close', side_effect=failure):
            with pytest.raises(OSError) as caught:
                writer.prepare_commit(2)
        assert caught.value is failure
        messages = writer.prepare_commit(2)
        assert [message.partition for message in messages] == [('new',)]
        assert [file.row_count for message in messages for file in message.new_files] == [1]
        assert set(store.data_writers) == {(('new',), 0)}
        files = list(tmp_path.rglob('*.parquet'))
        assert len(files) == 2
        writer.abort()
        assert all(path.exists() for path in files)
    finally:
        writer.close()
