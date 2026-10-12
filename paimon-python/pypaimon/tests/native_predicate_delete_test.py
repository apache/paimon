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

from contextlib import ExitStack
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.manifest.index_manifest_file import IndexManifestFile
from pypaimon.tests import native_plan_rest_test
from pypaimon.write.table_update import TableUpdate


pytestmark = pytest.mark.native_plan
rest_catalog = native_plan_rest_test.rest_catalog


def _table(catalog, options=None, mode='de'):
    schema = pa.schema([('id', pa.int32()), ('p', pa.string()), ('v', pa.int32()), ('q', pa.int32())])
    settings = {
        'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true',
        'deletion-vectors.enabled': 'true', 'read.batch-size': '1',
        'scan.native-plan.enabled': 'true', 'read.native.enabled': 'true',
        'write.native.enabled': 'true', 'commit.native.enabled': 'true',
    }
    settings.update(options or {})
    if mode != 'de':
        settings.update({'data-evolution.enabled': 'false', 'row-tracking.enabled': 'false',
                         'deletion-vectors.enabled': 'false'})
    if mode == 'pk':
        settings.setdefault('bucket', '1')
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        schema, partition_keys=['q', 'p'], primary_keys=['id', 'q', 'p'] if mode == 'pk' else [],
        options=settings), False)
    table = catalog.get_table('default.t')
    _append(table, [0, 1, 2, 3], ['a', 'a', 'b', 'c' if mode == 'pk' else None],
            [10, 20, 30, 40], [1, 1, 2, 2])
    return table


def _append(table, ids, partitions, values, q):
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        schema = pa.schema([('id', pa.int32()), ('p', pa.string()), ('v', pa.int32()), ('q', pa.int32())])
        writer.write_arrow(pa.table({'id': ids, 'p': partitions, 'v': values, 'q': q}, schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()


def _ids(table):
    table = table.copy({'scalar-index.search-mode': 'FULL'})
    read = table.new_read_builder().with_projection(['id'])
    return sorted(read.new_read().to_arrow(read.new_scan().plan().splits())['id'].to_pylist())


def _core_only():
    stack = ExitStack()
    for method in ('_delete_by_partition_filter', '_matched_delete_row_ids', '_delete_by_row_id'):
        stack.enter_context(patch.object(TableUpdate, method, side_effect=AssertionError(
            'Python predicate deletion selected: ' + method)))
    return stack


def _delete(table, predicate, stream=False, identifier=7):
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    update = builder.new_update()
    baseline = table.snapshot_manager().get_latest_snapshot().id
    with _core_only():
        messages = (update.delete_by_predicate(predicate, identifier) if stream
                    else update.delete_by_predicate(predicate))
    assert all(message.check_from_snapshot == baseline for message in messages)
    commit = builder.new_commit()
    try:
        commit.commit(messages, identifier) if stream else commit.commit(messages)
    finally:
        commit.close()
    return messages


@pytest.mark.parametrize('stream', [False, True])
@pytest.mark.parametrize('selection', ['data', 'all', 'partition', 'composite', 'or_null', 'missing'])
def test_rest_predicate_delete_matches_python_without_python_planning(native_rest_catalog, stream, selection):
    partition_only = selection in ('partition', 'composite', 'or_null')
    table = _table(native_rest_catalog, {
        'deletion-vectors.enabled': 'false' if partition_only else 'true'})
    pb = table.new_read_builder().new_predicate_builder()
    predicates = {
        'data': pb.equal('v', 20), 'all': None, 'partition': pb.equal('p', 'a'),
        'composite': pb.and_predicates([pb.equal('q', 1), pb.equal('p', 'a')]),
        'or_null': pb.or_predicates([pb.equal('p', 'a'), pb.is_null('p')]),
        'missing': pb.equal('id', 999),
    }
    expected = {'data': [0, 2, 3], 'all': [], 'partition': [2, 3],
                'composite': [2, 3], 'or_null': [2], 'missing': [0, 1, 2, 3]}
    messages = _delete(table, predicates[selection], stream)
    assert _ids(table) == expected[selection]
    if selection == 'missing':
        assert messages == []
    elif partition_only:
        assert sum(len(message.deleted_files) for message in messages) > 0
        assert sum(len(message.index_adds) for message in messages) == 0
    else:
        assert sum(len(message.deleted_files) for message in messages) == 0
        assert sum(len(message.index_adds) for message in messages) > 0


@pytest.mark.parametrize('stream', [False, True])
def test_predicate_delete_matches_latest_column_versions_and_existing_dvs(native_rest_catalog, stream):
    table = _table(native_rest_catalog)
    read = table.new_read_builder().with_projection(['id', '_ROW_ID'])
    data = read.new_read().to_arrow(read.new_scan().plan().splits()).to_pylist()
    row_id = next(row['_ROW_ID'] for row in data if row['id'] == 0)
    builder = table.new_batch_write_builder()
    messages = builder.new_update().update_by_arrow_with_row_id(pa.table({'_ROW_ID': [row_id], 'v': [99]}))
    commit = builder.new_commit()
    commit.commit(messages)
    commit.close()
    pb = table.new_read_builder().new_predicate_builder()
    _delete(table, pb.equal('id', 1), stream, 1)
    messages = _delete(table, pb.greater_than('v', 50), stream, 2)
    assert sum(len(message.index_deletes) for message in messages) == 1
    assert _ids(table) == [2, 3]
    assert _delete(table, pb.greater_than('v', 50), stream, 3) == []


@pytest.mark.parametrize('mode', ['PARTIAL', 'DETAIL'])
def test_predicate_delete_includes_unindexed_appends(native_rest_catalog, mode):
    table = _table(native_rest_catalog, {'global-index.enabled': 'true'})
    table.copy({'deletion-vectors.enabled': 'false'}).create_global_index('v', index_type='bitmap')
    _append(table, [4], ['a'], [20], [1])
    table = table.copy({'scalar-index.search-mode': mode})
    pb = table.new_read_builder().new_predicate_builder()
    _delete(table, pb.equal('v', 20))
    assert _ids(table) == [0, 2, 3]


def test_partition_delete_removes_selected_dv_and_scalar_indexes(native_rest_catalog):
    table = _table(native_rest_catalog, {'global-index.enabled': 'true'})
    table.copy({'deletion-vectors.enabled': 'false'}).create_global_index('v', index_type='bitmap')
    pb = table.new_read_builder().new_predicate_builder()
    _delete(table, pb.equal('id', 0))
    before = table.snapshot_manager().get_latest_snapshot()
    before_indexes = {entry.index_file.file_name for entry in IndexManifestFile(table).read(before.index_manifest)}
    messages = _delete(table, pb.equal('p', 'a'))
    deleted = {entry.index_file.file_name for message in messages for entry in message.index_deletes}
    assert len(deleted) == 2
    after = table.snapshot_manager().get_latest_snapshot()
    remaining = {entry.index_file.file_name for entry in IndexManifestFile(table).read(after.index_manifest)}
    assert remaining == before_indexes - deleted
    assert _ids(table) == [2, 3]


@pytest.mark.parametrize('mixed', [False, True])
def test_data_delete_rejects_disabled_dv_without_fallback(native_rest_catalog, mixed):
    table = _table(native_rest_catalog, {'deletion-vectors.enabled': 'false'})
    pb = table.new_read_builder().new_predicate_builder()
    predicate = pb.equal('id', 999)
    if mixed:
        predicate = pb.or_predicates([predicate, pb.equal('p', 'a')])
    with _core_only(), pytest.raises(NotImplementedError, match='deletion-vectors.enabled'):
        table.new_batch_write_builder().new_update().delete_by_predicate(predicate)
    assert _ids(table) == [0, 1, 2, 3]


def test_prepared_delete_rejects_concurrent_dv_changes_and_keeps_files(native_rest_catalog, tmp_path):
    table = _table(native_rest_catalog)
    pb = table.new_read_builder().new_predicate_builder()
    builder = table.new_batch_write_builder()
    with _core_only():
        messages = builder.new_update().delete_by_predicate(
            pb.equal('id', 0))
    _delete(table, pb.equal('id', 1))
    files = {path for path in tmp_path.rglob('*') if path.is_file()}
    commit = builder.new_commit()
    try:
        with pytest.raises((RuntimeError, ValueError),
                           match='Conflicting deletion vectors|Deletion vector index conflict'):
            commit.commit(messages)
    finally:
        commit.close()
    assert files <= {path for path in tmp_path.rglob('*') if path.is_file()}
    assert _ids(table) == [0, 2, 3]


def test_unavailable_native_operation_keeps_python_path(native_rest_catalog):
    table = _table(native_rest_catalog)
    predicate = table.new_read_builder().new_predicate_builder().equal('id', 0)
    with patch('pypaimon.write.native_update.create_native_predicate_delete', return_value=None):
        builder = table.new_batch_write_builder()
        messages = builder.new_update().delete_by_predicate(predicate)
    commit = builder.new_commit()
    try:
        commit.commit(messages)
    finally:
        commit.close()
    assert _ids(table) == [1, 2, 3]


@pytest.mark.parametrize('mode', ['append', 'pk'])
@pytest.mark.parametrize('stream', [False, True])
def test_partition_delete_supports_append_and_pk_tables_without_row_tracking(native_rest_catalog, mode, stream):
    table = _table(native_rest_catalog, mode=mode)
    predicate = table.new_read_builder().new_predicate_builder().equal('p', 'a')
    _delete(table, predicate, stream)
    assert _ids(table) == [2, 3]


def test_partition_delete_includes_pending_pk_files(native_rest_catalog):
    from pypaimon.manifest.manifest_list_manager import ManifestListManager
    from pypaimon.read.scanner.file_scanner import FileScanner

    table = _table(native_rest_catalog, {'bucket': '-2'}, mode='pk')
    predicate = table.new_read_builder().new_predicate_builder().equal('p', 'a')
    messages = _delete(table, predicate)
    assert all(message.bucket == -2 for message in messages)
    assert sum(len(message.deleted_files) for message in messages) == 1
    snapshot = table.snapshot_manager().get_latest_snapshot()
    entries = FileScanner(table, lambda: ([], None)).with_all_buckets().read_manifest_entries(
        ManifestListManager(table).read_all(snapshot))
    assert sorted((entry.partition.values[0], entry.partition.values[1]) for entry in entries
                  if entry.partition.values[1] is not None) == [(2, 'b'), (2, 'c')]
    assert len(entries) == 2


@pytest.mark.parametrize('partition_delete', [False, True])
def test_prepared_predicate_delete_preserves_later_appends(native_rest_catalog, partition_delete):
    table = _table(native_rest_catalog)
    builder = table.new_batch_write_builder()
    pb = table.new_read_builder().new_predicate_builder()
    with _core_only():
        messages = builder.new_update().delete_by_predicate(
            pb.equal('p', 'a') if partition_delete else pb.equal('id', 0))
    _append(table, [4], ['a'], [50], [1])
    commit = builder.new_commit()
    try:
        commit.commit(messages)
    finally:
        commit.close()
    assert _ids(table) == ([2, 3, 4] if partition_delete else [1, 2, 3, 4])


def test_match_reader_and_dv_writer_share_one_catalog_snapshot(rest_catalog):
    from pypaimon.api.api_response import GetTableSnapshotResponse
    from pypaimon.snapshot.table_snapshot import TableSnapshot

    catalog, server = rest_catalog
    table = _table(catalog)
    captured = table.snapshot_manager().get_latest_snapshot()
    _append(table, [4], ['a'], [20], [1])
    original = server._table_snapshot_handle
    calls = []

    def snapshot_response(*args):
        calls.append(args)
        if len(calls) == 1:
            response = GetTableSnapshotResponse(TableSnapshot(
                captured, captured.id, 0, captured.id, captured.time_millis))
            return server._mock_response(response, 200)
        return original(*args)

    builder = table.new_batch_write_builder()
    predicate = table.new_read_builder().new_predicate_builder().equal('v', 20)
    with patch.object(server, '_table_snapshot_handle', side_effect=snapshot_response), _core_only():
        messages = builder.new_update().delete_by_predicate(predicate)
    assert len(calls) == 1
    assert all(message.check_from_snapshot == captured.id for message in messages)
    commit = builder.new_commit()
    try:
        commit.commit(messages)
    finally:
        commit.close()
    # Row 4 was absent from the captured match snapshot, even though it now
    # matches the same predicate and is present before the delete is published.
    assert _ids(table) == [0, 2, 3, 4]


def test_catalog_snapshot_failure_never_falls_back_to_python(rest_catalog, tmp_path):
    from pypaimon.api.api_response import ErrorResponse

    catalog, server = rest_catalog
    table = _table(catalog)
    predicate = table.new_read_builder().new_predicate_builder().equal('v', 20)
    files = {path for path in tmp_path.rglob('*') if path.is_file()}
    error = ErrorResponse('SNAPSHOT', 't', 'snapshot unavailable', 503)
    with patch.object(server, '_table_snapshot_handle',
                      return_value=server._mock_response(error, 503)), _core_only():
        with pytest.raises(Exception, match='snapshot unavailable'):
            table.new_batch_write_builder().new_update().delete_by_predicate(predicate)
    assert _ids(table) == [0, 1, 2, 3]
    assert files <= {path for path in tmp_path.rglob('*') if path.is_file()}
