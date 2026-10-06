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

"""Java-compatible row tracking metadata through the native REST commit bridge."""

from contextlib import ExitStack
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.manifest.manifest_sidecar import read_sidecar, SUFFIX
from pypaimon.utils.range import Range
from pypaimon.write.native_commit import native_messages_supported
from pypaimon.write.native_write import NativeTableWrite

pytestmark = pytest.mark.native_plan

_SCHEMA = pa.schema([('id', pa.int64()), ('v', pa.int64()), ('w', pa.int64()), ('p', pa.string())])


def _table(catalog, options=None):
    settings = {'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
                'commit.native.enabled': 'true', 'write.native.enabled': 'true',
                'manifest.sidecar.enabled': 'true'}
    settings.update(options or {})
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        _SCHEMA, partition_keys=['p'], options=settings), False)
    return catalog.get_table('default.t')


def _prepare(builder, ids, partition='a', identifier=None):
    writer = builder.new_write()
    try:
        assert isinstance(writer, NativeTableWrite) == builder.table.options.native_write_enabled()
        writer.write_arrow(pa.table({'id': ids, 'v': ids, 'w': ids,
                                    'p': [partition] * len(ids)}, schema=_SCHEMA))
        return writer.prepare_commit() if identifier is None else writer.prepare_commit(identifier)
    finally:
        writer.close()


def _commit(builder, messages, identifier=None, overwrite=False):
    commit = builder.new_commit()
    try:
        with patch.object(commit.file_store_commit, 'overwrite' if overwrite else 'commit',
                          side_effect=AssertionError('Python commit fallback')):
            if identifier is None:
                commit.commit(messages)
            else:
                commit.commit(messages, identifier)
    finally:
        commit.close()


def _read(table, native=False, metadata=False):
    read_table = table.copy({'read.native.enabled': str(native).lower(),
                             'scan.native-plan.enabled': str(native).lower()})
    builder = read_table.new_read_builder()
    if metadata:
        fields = ['id', 'v', 'w', 'p', '_ROW_ID']
        if not native:
            fields.append('_SEQUENCE_NUMBER')
        builder.with_projection(fields)
    read = builder.new_read()
    with ExitStack() as stack:
        if native:
            stack.enter_context(patch.object(
                read, '_create_split_read', side_effect=AssertionError('Python read fallback')))
        return read.to_arrow(builder.new_scan().plan().splits()).sort_by('id').to_pydict()


def _delta(table):
    snapshot = table.snapshot_manager().get_latest_snapshot()
    metas = ManifestListManager(table).read(snapshot.delta_manifest_list)
    manager = ManifestFileManager(table)
    entries = [entry for meta in metas for entry in manager.read(meta.file_name)]
    return snapshot, metas, entries


@pytest.mark.parametrize('native_write', [False, True])
@pytest.mark.parametrize('group', [False, True])
def test_append_groups_partitions_assigns_versions_and_publishes_sidecars(
        native_rest_catalog, native_write, group):
    table = _table(native_rest_catalog, {
        'write.native.enabled': str(native_write).lower(),
        'row-tracking.partition-group-on-commit': str(group).lower()})
    builder = table.new_batch_write_builder()
    messages = (_prepare(builder, [10, 11], 'a') + _prepare(builder, [20], 'b')
                + _prepare(builder, [12], 'a'))
    assert native_messages_supported(table, messages)
    _commit(builder, messages)
    snapshot, metas, entries = _delta(table)
    assert snapshot.next_row_id == 4
    assert [(e.file.first_row_id, e.file.row_count) for e in entries] == [
        (0, 2), (2, 1), (3, 1)]
    assert [e.partition.values for e in entries] == (
        [['a'], ['a'], ['b']] if group else [['a'], ['b'], ['a']])
    assert all((e.file.min_sequence_number, e.file.max_sequence_number) == (1, 1)
               for e in entries)
    for meta in metas:
        assert meta.extra_files == [meta.file_name + SUFFIX]
        path = table.table_path + '/manifest/' + meta.file_name
        assert read_sidecar(table.file_io, path, meta, [Range(0, 0)]).blocks
        assert not read_sidecar(table.file_io, path, meta, [Range(4, 4)]).blocks
    for native in (False, True):
        rows = _read(table, native, metadata=True)
        assert rows['id'] == [10, 11, 12, 20]
        assert rows['_ROW_ID'] == ([0, 1, 2, 3] if group else [0, 1, 3, 2])
        if not native:
            assert rows['_SEQUENCE_NUMBER'] == [1, 1, 1, 1]


@pytest.mark.parametrize('stream', [False, True])
def test_partial_column_update_keeps_row_ids_and_checks_conflicts(native_rest_catalog, stream):
    table = _table(native_rest_catalog)
    seed = table.new_batch_write_builder()
    _commit(seed, _prepare(seed, [1, 2]))
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()

    def prepare(column, value):
        update = builder.new_update()
        updater = update.new_update_by_row_id(7) if stream else update.new_update_by_row_id()
        return updater.update_columns(pa.table({'_ROW_ID': [0], column: [value]}), [column])

    first = prepare('v', 10)
    conflicting = prepare('v', 20)
    disjoint = prepare('w', 30)
    assert all(m.check_from_snapshot == 1 for m in first + conflicting + disjoint)
    _commit(builder, first, 7 if stream else None)
    # Give each logical transaction a separate commit identity.
    other = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    with pytest.raises(Exception, match='multiple MERGE INTO.*conflicts'):
        _commit(other, conflicting, 8 if stream else None)
    assert table.snapshot_manager().get_latest_snapshot().id == 2
    other = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    _commit(other, disjoint, 9 if stream else None)
    snapshot, _, entries = _delta(table)
    assert snapshot.next_row_id == 2
    assert all(e.file.first_row_id == 0 for e in entries)
    for native in (False, True):
        rows = _read(table, native, metadata=True)
        assert rows['v'] == [10, 2]
        assert rows['w'] == [30, 2]
        assert rows['_ROW_ID'] == [0, 1]


@pytest.mark.parametrize('native_write', [False, True])
def test_overwrite_keeps_deleted_versions_and_allocates_fresh_row_ids(native_rest_catalog, native_write):
    table = _table(native_rest_catalog, {'write.native.enabled': str(native_write).lower()})
    seed = table.new_batch_write_builder()
    _commit(seed, _prepare(seed, [1, 2], 'a') + _prepare(seed, [3], 'b'))
    builder = table.new_batch_write_builder().overwrite({'p': 'a'})
    _commit(builder, _prepare(builder, [4], 'a'), overwrite=True)
    snapshot, _, entries = _delta(table)
    assert snapshot.next_row_id == 4
    deleted = [e.file for e in entries if e.kind == 1]
    added = [e.file for e in entries if e.kind == 0]
    assert [(f.first_row_id, f.min_sequence_number, f.max_sequence_number)
            for f in deleted] == [(0, 1, 1)]
    assert [(f.first_row_id, f.min_sequence_number, f.max_sequence_number)
            for f in added] == [(3, 2, 2)]
    for native in (False, True):
        rows = _read(table, native, metadata=True)
        assert rows['id'] == [3, 4]
        assert rows['_ROW_ID'] == [2, 3]
        if not native:
            assert rows['_SEQUENCE_NUMBER'] == [1, 2]


def test_deletion_vector_commit_preserves_row_id_high_watermark(native_rest_catalog):
    table = _table(native_rest_catalog, {'deletion-vectors.enabled': 'true'})
    seed = table.new_batch_write_builder()
    _commit(seed, _prepare(seed, [1, 2, 3]))
    builder = table.new_batch_write_builder()
    messages = builder.new_update().delete_by_row_id([0, 2])
    _commit(builder, messages)
    snapshot = table.snapshot_manager().get_latest_snapshot()
    assert snapshot.next_row_id == 3
    assert snapshot.index_manifest
    for native in (False, True):
        rows = _read(table, native, metadata=True)
        assert rows['id'] == [2]
        assert rows['_ROW_ID'] == [1]


@pytest.mark.parametrize('rewrite_size', [None, '0 B', '256 MB'])
def test_preassigned_row_id_updates_use_native_java_validation(native_rest_catalog, rewrite_size):
    options = {} if rewrite_size is None else {
        'data-evolution.row-id-conflict-rewrite.max-size': rewrite_size}
    table = _table(native_rest_catalog, options)
    seed = table.new_batch_write_builder()
    _commit(seed, _prepare(seed, [1, 2]))
    builder = table.new_batch_write_builder()
    messages = builder.new_update().new_update_by_row_id().update_columns(
        pa.table({'_ROW_ID': [0], 'v': [99]}), ['v'])
    assert native_messages_supported(table, messages)
    _commit(builder, messages)
    snapshot = table.snapshot_manager().get_latest_snapshot()
    assert snapshot.id == 2
    assert snapshot.next_row_id == 2
    for native in (False, True):
        assert _read(table, native)['v'] == [99, 2]


@pytest.mark.parametrize('group_option', ['off', '0', ' true '])
def test_native_partition_group_option_uses_python_boolean_normalization(native_rest_catalog, group_option):
    table = _table(native_rest_catalog, {'row-tracking.partition-group-on-commit': group_option})
    builder = table.new_batch_write_builder()
    _commit(builder, _prepare(builder, [1], 'a') + _prepare(builder, [2], 'b')
            + _prepare(builder, [3], 'a'))
    _, _, entries = _delta(table)
    expected = [['a'], ['a'], ['b']] if table.options.row_tracking_partition_group_on_commit() else [
        ['a'], ['b'], ['a']]
    assert [entry.partition.values for entry in entries] == expected


def test_stream_abort_preserves_published_row_ids(native_rest_catalog):
    table = _table(native_rest_catalog)
    builder = table.new_stream_write_builder()
    messages = _prepare(builder, [1, 2], identifier=7)
    _commit(builder, messages, 7)
    assert table.snapshot_manager().get_latest_snapshot().next_row_id == 2
    pending = _prepare(builder, [3], identifier=8)
    pending_paths = [file.file_path for message in pending for file in message.new_files]
    assert pending_paths and all(table.file_io.exists(path) for path in pending_paths)
    commit = builder.new_commit()
    try:
        with patch.object(commit.file_store_commit, 'abort',
                          side_effect=AssertionError('Python abort fallback')):
            commit.abort(pending)
    finally:
        commit.close()
    assert all(table.file_io.exists(path) for path in pending_paths)
    assert _read(table, True, metadata=True)['_ROW_ID'] == [0, 1]
    _commit(builder, _prepare(builder, [4], identifier=9), 9)
    assert table.snapshot_manager().get_latest_snapshot().next_row_id == 3
    assert _read(table, True, metadata=True)['_ROW_ID'] == [0, 1, 2]


@pytest.mark.parametrize('external', [False, True])
def test_blob_files_align_with_normal_files_after_native_rest_commit(
        tmp_path, native_rest_catalog, external):
    from pypaimon.schema.data_types import AtomicType, DataField

    options = {'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
               'write.native.enabled': 'true', 'commit.native.enabled': 'true',
               'blob.target-file-size': '1 B', 'manifest.sidecar.enabled': 'true',
               'manifest.target-file-size': '1 B'}
    if external:
        options['data-file.external-paths'] = (tmp_path / 'external').as_uri()
    fields = [DataField(0, 'id', AtomicType('BIGINT')),
              DataField(1, 'p', AtomicType('STRING')),
              DataField(2, 'left', AtomicType('BLOB')),
              DataField(3, 'right', AtomicType('BLOB'))]
    native_rest_catalog.create_table('default.blobs', Schema(
        fields=fields, partition_keys=['p'], options=options), False)
    table = native_rest_catalog.get_table('default.blobs')
    schema = pa.schema([('id', pa.int64()), ('p', pa.string()),
                        ('left', pa.large_binary()), ('right', pa.large_binary())])
    builder = table.new_stream_write_builder()
    expected = []
    for identifier, partition in [(1, 'a/b'), (2, 'c')]:
        writer = builder.new_write()
        assert isinstance(writer, NativeTableWrite)
        rows = [{'id': identifier * 10, 'p': partition, 'left': b'x' * 80, 'right': b''},
                {'id': identifier * 10 + 1, 'p': partition, 'left': None, 'right': b'y' * 16}]
        try:
            writer.write_arrow(pa.Table.from_pylist(rows, schema=schema))
            messages = writer.prepare_commit(identifier)
            _commit(builder, messages, identifier)
        finally:
            writer.close()
        expected.extend(rows)
        snapshot, metas, entries = _delta(table)
        assert snapshot.next_row_id == identifier * 2
        normal = [entry.file for entry in entries if entry.file.file_name.endswith('.parquet')]
        blobs = [entry.file for entry in entries if entry.file.file_name.endswith('.blob')]
        assert len(normal) == 1 and blobs
        start = (identifier - 1) * 2
        assert normal[0].first_row_id == start
        for column in ('left', 'right'):
            column_files = [file for file in blobs if file.write_cols == [column]]
            assert column_files
            assert min(file.first_row_id for file in column_files) == start
            assert sum(file.row_count for file in column_files) == 2
        assert all(meta.extra_files == [meta.file_name + SUFFIX] for meta in metas)
        for native in (False, True):
            readable = table.copy({'read.native.enabled': str(native).lower(),
                                   'scan.native-plan.enabled': str(native).lower()})
            read = readable.new_read_builder()
            actual = read.new_read().to_arrow(read.new_scan().plan().splits()).sort_by('id')
            assert actual.to_pylist() == expected


@pytest.mark.parametrize('rewrite_size', [None, '256 MB'])
def test_native_blob_update_rejects_stale_range_before_publication(native_rest_catalog, rewrite_size):
    from dataclasses import replace

    from pypaimon.schema.data_types import AtomicType, DataField
    from pypaimon.write.commit_message import CommitMessage

    fields = [DataField(0, 'id', AtomicType('BIGINT')), DataField(1, 'payload', AtomicType('BLOB'))]
    options = {'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
               'write.native.enabled': 'true', 'commit.native.enabled': 'true'}
    if rewrite_size is not None:
        options['data-evolution.row-id-conflict-rewrite.max-size'] = rewrite_size
    native_rest_catalog.create_table('default.blobs', Schema(fields=fields, options=options), False)
    table = native_rest_catalog.get_table('default.blobs')
    schema = pa.schema([('id', pa.int64()), ('payload', pa.large_binary())])
    builder = table.new_batch_write_builder()

    def prepare(ids):
        writer = builder.new_write()
        try:
            writer.write_arrow(pa.table({'id': ids, 'payload': [b'value'] * len(ids)}, schema=schema))
            return writer.prepare_commit()
        finally:
            writer.close()

    # Simulate a current layout with two adjacent normal ranges, as can result
    # from another engine replacing a former [0, 9] normal file.
    _commit(builder, prepare(list(range(5))) + prepare(list(range(5, 10))))
    staged = prepare(list(range(10)))
    source = staged[0]
    blob = next(file for file in source.new_files if file.file_name.endswith('.blob'))
    assert blob.row_count == 10
    blob = replace(blob, first_row_id=0, min_sequence_number=0, max_sequence_number=0)
    stale = [CommitMessage(source.partition, source.bucket, [blob], check_from_snapshot=1)]
    assert native_messages_supported(table, stale)
    before = table.snapshot_manager().get_latest_snapshot()
    with pytest.raises(Exception, match='Row ID existence conflict|not covered by one data file range'):
        _commit(table.new_batch_write_builder(), stale)
    assert table.snapshot_manager().get_latest_snapshot().id == before.id
    assert table.snapshot_manager().get_latest_snapshot().next_row_id == 10
    # Both engines must still be able to read the original, valid snapshot.
    for native in (False, True):
        read = table.copy({'read.native.enabled': str(native).lower(),
                           'scan.native-plan.enabled': str(native).lower()}).new_read_builder()
        actual = read.new_read().to_arrow(read.new_scan().plan().splits()).sort_by('id')
        assert actual['id'].to_pylist() == list(range(10))
        assert actual['payload'].to_pylist() == [b'value'] * 10
    commit = builder.new_commit()
    try:
        commit.abort(staged)
    finally:
        commit.close()


@pytest.mark.parametrize('action', ['IGNORE', 'DROP_PARTITION_INDEX', 'THROW_ERROR'])
def test_native_update_honors_java_global_index_policy(native_rest_catalog, action):
    from pypaimon.tests.global_index_update_action_test import _index_entry
    from pypaimon.write.commit_message import CommitMessage
    from pypaimon.write.global_index_update_checker import scan_global_index_entries

    table = _table(native_rest_catalog, {'global-index.column-update-action': action})
    seed = table.new_batch_write_builder()
    _commit(seed, _prepare(seed, [1, 2]))
    # Commit policy only needs manifest metadata. This fixture intentionally
    # does not execute an index query or claim to refresh IGNORE's stale index.
    index = _index_entry('indexed-v', ('a',), 1)
    _commit(table.new_batch_write_builder(), [CommitMessage(('a',), 0, [], index_adds=[index])])
    builder = table.new_batch_write_builder()
    # Prepare without a Python index policy precheck, so the committer itself
    # must enforce the resolved table policy for externally produced messages.
    update_table = table.copy({'global-index.column-update-action': 'IGNORE'})
    messages = update_table.new_batch_write_builder().new_update().new_update_by_row_id().update_columns(
        pa.table({'_ROW_ID': [0], 'v': [99]}), ['v'])
    if action == 'THROW_ERROR':
        with pytest.raises(Exception, match='globally indexed columns'):
            _commit(builder, messages)
    else:
        _commit(builder, messages)
    snapshot = table.snapshot_manager().get_latest_snapshot()
    assert snapshot.id == (2 if action == 'THROW_ERROR' else 3)
    assert snapshot.next_row_id == 2
    indexes = scan_global_index_entries(table, snapshot)
    assert [entry.index_file.file_name for entry in indexes] == (
        [] if action == 'DROP_PARTITION_INDEX' else ['indexed-v'])
    assert _read(table, True)['v'] == ([1, 2] if action == 'THROW_ERROR' else [99, 2])
