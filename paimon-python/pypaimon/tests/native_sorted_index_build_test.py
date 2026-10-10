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

"""Build Rust index files on REST tables and consume them through Python APIs."""

from datetime import date, datetime, time, timezone
from decimal import Decimal
import struct
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.api.api_response import ErrorResponse, GetTableSnapshotResponse
from pypaimon.globalindex.create_global_index import GlobalIndexBuilder
from pypaimon.globalindex.data_evolution_global_index_scanner import DataEvolutionGlobalIndexScanner
from pypaimon.index.index_file_handler import IndexFileHandler
from pypaimon.read.native_plan import _native_table, _predicate_to_native, native_method_available
from pypaimon.snapshot.table_snapshot import TableSnapshot
from pypaimon.tests import native_plan_rest_test
from pypaimon.write.native_commit import from_native_commit_messages
from pypaimon.write.table_commit import BatchTableCommit

rest_catalog = native_plan_rest_test.rest_catalog
pytestmark = [pytest.mark.native_plan, pytest.mark.skipif(
    not native_method_available('Table', 'new_global_index_build_builder'),
    reason='Rust index builder required')]

SCHEMA = pa.schema([('id', pa.int32()), ('name', pa.string()), ('pt', pa.int32())])
ROWS = [
    {'id': 3, 'name': 'c', 'pt': 0}, {'id': 1, 'name': 'a', 'pt': 1},
    {'id': 2, 'name': None, 'pt': None}, {'id': 4, 'name': 'b', 'pt': 0},
    {'id': 5, 'name': 'a', 'pt': 1},
]


def _create(catalog, *, schema=SCHEMA, partitioned=False, options=None):
    settings = {
        'file.format': 'parquet', 'bucket': '-1', 'row-tracking.enabled': 'true',
        'data-evolution.enabled': 'true', 'global-index.enabled': 'true',
        'write.native.enabled': 'true', 'commit.native.enabled': 'false',
        'read.native.enabled': 'false', 'scan.native-plan.enabled': 'true',
        'sorted-index.records-per-range': '2', 'read.batch-size': '1',
        'global-index.search-mode': 'full',
    }
    settings.update(options or {})
    catalog.create_table('default.indexes', Schema.from_pyarrow_schema(
        schema, partition_keys=['pt'] if partitioned else [], options=settings), False)
    return catalog.get_table('default.indexes')


def _append(table, rows, schema=SCHEMA):
    builder = table.copy({'write.native.enabled': 'false'}).new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pylist(rows, schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()


def _commit(table, messages):
    commit = table.new_batch_write_builder().new_commit()
    try:
        commit.commit(messages)
    finally:
        commit.close()


def _files(table):
    return [entry.index_file for entry in IndexFileHandler(table).scan(None)]


def _message_files(messages):
    return [entry.index_file for message in messages for entry in message.index_adds]


def _path(table, file):
    return file.external_path or table.path_factory().global_index_path_factory().to_path(file.file_name)


def _ids(table, predicate=None):
    builder = table.new_read_builder().with_projection(['id'])
    if predicate is not None:
        builder.with_filter(predicate)
    rows = builder.new_read().to_arrow(builder.new_scan().plan().splits())
    return sorted(rows.column('id').to_pylist())


def _build(table, kind, column='name', **kwargs):
    # A native build must never materialize or sort rows in Python.
    with patch.object(GlobalIndexBuilder, '_build_sorted_index', side_effect=AssertionError('Python index build ran')):
        return GlobalIndexBuilder(table, column, index_type=kind, **kwargs).build()


@pytest.mark.parametrize('kind', ['btree', 'bitmap'])
@pytest.mark.parametrize('partitioned', [False, True])
@pytest.mark.parametrize('native_commit', [False, True])
def test_build_is_unpublished_and_messages_work_with_either_committer(rest_catalog, kind, partitioned, native_commit):
    catalog, _ = rest_catalog
    table = _create(catalog, partitioned=partitioned, options={'commit.native.enabled': str(native_commit).lower()})
    _append(table, ROWS)
    before = table.snapshot_manager().get_latest_snapshot()
    messages = _build(table, kind)
    assert messages and all(not message.new_files and message.index_adds for message in messages)
    assert table.snapshot_manager().get_latest_snapshot().id == before.id
    assert _files(table) == []
    for file in _message_files(messages):
        assert file.index_type == kind
        assert file.global_index_meta.index_field_id == table.field_dict['name'].id
        assert table.file_io.exists(_path(table, file))
    _commit(table, messages)
    assert table.snapshot_manager().get_latest_snapshot().id == before.id + 1
    predicate = table.new_read_builder().new_predicate_builder().equal('name', 'a')
    with DataEvolutionGlobalIndexScanner.create(table, predicate=predicate) as scanner:
        result = scanner.scan(predicate)
        assert result is not None and result.is_exact()
        assert result.results().cardinality() == 2
    assert _ids(table, predicate) == [1, 5]
    nulls = table.new_read_builder().new_predicate_builder().is_null('name')
    assert _ids(table, nulls) == [2]
    after = table.snapshot_manager().get_latest_snapshot().id
    assert _build(table, kind) == []
    assert table.create_global_index('name', index_type=kind) == 0
    assert table.snapshot_manager().get_latest_snapshot().id == after


@pytest.mark.parametrize('kind', ['btree', 'bitmap'])
@pytest.mark.parametrize('selection', ['spec', 'predicate', 'null', 'union'])
def test_partition_selection_and_incremental_coverage(rest_catalog, kind, selection):
    catalog, _ = rest_catalog
    table = _create(catalog, partitioned=True)
    _append(table, ROWS)
    predicates = table.new_read_builder().new_predicate_builder()
    kwargs = ({'partition_filter': predicates.equal('pt', 1)} if selection == 'predicate' else
              {'partitions': {'pt': None}} if selection == 'null' else
              {'partitions': [{'pt': 0}, {'pt': 1}]} if selection == 'union' else {'partitions': {'pt': 1}})
    first = _build(table, kind, column='id', **kwargs)
    expected_partitions = {(None,)} if selection == 'null' else {(0,), (1,)} if selection == 'union' else {(1,)}
    assert {message.partition for message in first} == expected_partitions
    _commit(table, first)
    assert _build(table, kind, column='id', **kwargs) == []
    _append(table, [{'id': 6, 'name': 'a', 'pt': 1}, {'id': 7, 'name': 'z', 'pt': 0}])
    added = _build(table, kind, column='id', **kwargs)
    assert sum(file.row_count for file in _message_files(added)) == (
        0 if selection == 'null' else 2 if selection == 'union' else 1)
    if added:
        _commit(table, added)
    remaining = _build(table, kind, column='id')
    assert sum(file.row_count for file in _message_files(remaining)) == (
        6 if selection == 'null' else 1 if selection == 'union' else 4)
    _commit(table, remaining)
    assert _build(table, kind, column='id') == []
    assert _ids(table, predicates.greater_or_equal('id', 5)) == [5, 6, 7]


@pytest.mark.parametrize('kind', ['btree', 'bitmap'])
def test_external_paths_remain_readable_and_explicit_abort_removes_only_private_files(rest_catalog, tmp_path, kind):
    catalog, _ = rest_catalog
    root = (tmp_path / 'external-indexes').as_uri()
    table = _create(catalog, options={'global-index.external-path': root})
    _append(table, ROWS)
    messages = _build(table, kind)
    private = _message_files(messages)
    assert all(file.external_path.startswith(root + '/') for file in private)
    assert all(table.file_io.exists(file.external_path) for file in private)
    commit = table.new_batch_write_builder().new_commit()
    try:
        commit.abort(messages)
    finally:
        commit.close()
    assert all(not table.file_io.exists(file.external_path) for file in private)
    assert table.snapshot_manager().get_latest_snapshot().id == 1
    replacement = _build(table, kind)
    _commit(table, replacement)
    assert {file.file_name for file in private}.isdisjoint(file.file_name for file in _files(table))
    table = table.copy({'global-index.external-path': (tmp_path / 'changed-root').as_uri()})
    assert _ids(table, table.new_read_builder().new_predicate_builder().equal('name', 'a')) == [1, 5]


@pytest.mark.parametrize('kind', ['btree', 'bitmap'])
@pytest.mark.parametrize('response', ['first', 'empty'])
def test_rest_snapshot_response_controls_build_instead_of_newer_disk_snapshot(rest_catalog, kind, response):
    catalog, server = rest_catalog
    table = _create(catalog)
    _append(table, ROWS[:2])
    first = table.snapshot_manager().get_latest_snapshot()
    _append(table, ROWS[2:])
    reply = (GetTableSnapshotResponse(TableSnapshot(first, 1, 0, 1, first.time_millis)) if response == 'first'
             else GetTableSnapshotResponse())
    with patch.object(server, '_table_snapshot_handle', return_value=server._mock_response(reply, 200)):
        messages = _build(table, kind)
    count = sum(file.row_count for file in _message_files(messages))
    assert count == (2 if response == 'first' else 0)
    assert table.snapshot_manager().get_latest_snapshot().id == 2


@pytest.mark.parametrize('code', [403, 500, 503])
def test_rest_snapshot_errors_propagate_without_starting_python_build(rest_catalog, code):
    catalog, server = rest_catalog
    table = _create(catalog)
    _append(table, ROWS)
    reply = ErrorResponse('TABLE', 'indexes', 'snapshot unavailable', code)
    with patch.object(server, '_table_snapshot_handle', return_value=server._mock_response(reply, code)):
        with pytest.raises(Exception, match='snapshot unavailable|permission'):
            _build(table, 'btree')
    assert table.snapshot_manager().get_latest_snapshot().id == 1
    assert _files(table) == []
    index_directory = table.path_factory().index_path()
    assert not table.file_io.exists(index_directory) or not table.file_io.list_status(index_directory)


@pytest.mark.parametrize('kind', ['btree', 'bitmap'])
@pytest.mark.parametrize('column, data_type, values, literal', [
    ('value', pa.int64(), [-2, 3, None, -2], -2),
    ('value', pa.bool_(), [True, False, None, True], True),
    ('value', pa.date32(), [date(2025, 1, 1), date(2025, 1, 2), None, date(2025, 1, 1)], date(2025, 1, 1)),
    ('value', pa.decimal128(12, 2), [Decimal('1.25'), Decimal('2.50'), None, Decimal('1.25')], Decimal('1.25')),
    ('value', pa.time32('ms'), [time(1, 2, 3), time(2, 3, 4), None, time(1, 2, 3)], time(1, 2, 3)),
    ('value', pa.timestamp('us'), [datetime(2025, 1, 1), datetime(2025, 1, 2), None, datetime(2025, 1, 1)],
     datetime(2025, 1, 1)),
    ('value', pa.timestamp('us', tz='UTC'),
     [datetime(2025, 1, 1, tzinfo=timezone.utc), datetime(2025, 1, 2, tzinfo=timezone.utc), None,
      datetime(2025, 1, 1, tzinfo=timezone.utc)], datetime(2025, 1, 1, tzinfo=timezone.utc)),
])
def test_typed_keys_match_python_scalar_readers(rest_catalog, kind, column, data_type, values, literal):
    catalog, _ = rest_catalog
    schema = pa.schema([('id', pa.int32()), (column, data_type)])
    table = _create(catalog, schema=schema)
    _append(table, [{'id': i, column: value} for i, value in enumerate(values)], schema)
    _commit(table, _build(table, kind, column))
    predicate = table.new_read_builder().new_predicate_builder().equal(column, literal)
    with DataEvolutionGlobalIndexScanner.create(table, predicate=predicate) as scanner:
        result = scanner.scan(predicate)
        assert list(result.results()) == [0, 3]
    assert _ids(table, predicate) == [0, 3]


@pytest.mark.parametrize('kind', ['btree', 'bitmap'])
def test_binding_partition_conjunction_and_rejection_are_core_semantics(rest_catalog, kind):
    catalog, _ = rest_catalog
    table = _create(catalog, partitioned=True)
    _append(table, ROWS)
    predicates = table.new_read_builder().new_predicate_builder()
    native = _native_table(table).new_global_index_build_builder().with_index_column('id').with_index_type(kind)
    # Python partition predicates carry partition-row indices; bindings resolve by field name.
    native.with_partition_filter(_predicate_to_native(predicates.equal('pt', 0)))
    native.with_partition_filter(_predicate_to_native(predicates.equal('pt', 1)))
    assert native.build() == []
    assert native.execute() == 0
    with pytest.raises(Exception, match='only partition keys'):
        native.with_partition_filter(_predicate_to_native(predicates.equal('id', 1)))
    assert table.snapshot_manager().get_latest_snapshot().id == 1


@pytest.mark.parametrize('kind', ['btree', 'bitmap'])
def test_empty_table_build_is_a_noop(rest_catalog, kind):
    catalog, _ = rest_catalog
    table = _create(catalog)
    assert _build(table, kind) == []
    assert table.create_global_index('name', index_type=kind) == 0
    assert _native_table(table).new_global_index_build_builder().with_index_column('name').execute() == 0
    assert table.snapshot_manager().get_latest_snapshot() is None


def test_native_binding_can_build_and_execute_separate_indexes(rest_catalog):
    catalog, _ = rest_catalog
    table = _create(catalog)
    _append(table, ROWS)
    native = _native_table(table)
    builder = native.new_global_index_build_builder().with_index_column('name')
    prepared = builder.build()
    assert all(message.serialize() for message in prepared)
    _commit(table, from_native_commit_messages(table, prepared))
    assert builder.execute() == 0
    builder.with_index_type('bitmap').with_options({'sorted-index.records-per-range': '100'})
    assert builder.execute() == 1
    assert {file.index_type for file in _files(table)} == {'btree', 'bitmap'}
    assert table.snapshot_manager().get_latest_snapshot().id == 3


def test_both_index_types_can_share_one_explicit_commit(rest_catalog):
    catalog, _ = rest_catalog
    table = _create(catalog)
    _append(table, ROWS)
    messages = _build(table, 'btree') + _build(table, 'bitmap')
    assert table.snapshot_manager().get_latest_snapshot().id == 1
    _commit(table, messages)
    assert table.snapshot_manager().get_latest_snapshot().id == 2
    assert {file.index_type for file in _files(table)} == {'btree', 'bitmap'}
    assert _ids(table, table.new_read_builder().new_predicate_builder().equal('name', 'a')) == [1, 5]


@pytest.mark.parametrize('kind', ['btree', 'bitmap'])
def test_commit_response_failure_keeps_published_index_files(rest_catalog, kind):
    catalog, _ = rest_catalog
    table = _create(catalog)
    _append(table, ROWS)
    original = BatchTableCommit.commit
    published = []

    def publish_then_raise(commit, messages, *args, **kwargs):
        published.extend(_message_files(messages))
        original(commit, messages, *args, **kwargs)
        raise OSError('commit response lost')

    with patch.object(BatchTableCommit, 'commit', publish_then_raise):
        with pytest.raises(OSError, match='commit response lost'):
            table.create_global_index('name', index_type=kind)
    assert published and all(table.file_io.exists(_path(table, file)) for file in published)
    assert table.snapshot_manager().get_latest_snapshot().id == 2
    assert _ids(table, table.new_read_builder().new_predicate_builder().equal('name', 'a')) == [1, 5]


@pytest.mark.parametrize('kind', ['btree', 'bitmap'])
def test_invalid_native_options_do_not_fall_back_or_publish_files(rest_catalog, kind):
    catalog, _ = rest_catalog
    table = _create(catalog)
    _append(table, ROWS)
    with pytest.raises(Exception, match='greater than 0'):
        _build(table, kind, options={'sorted-index.records-per-range': 0})
    assert table.snapshot_manager().get_latest_snapshot().id == 1
    index_directory = table.path_factory().index_path()
    assert not table.file_io.exists(index_directory) or not table.file_io.list_status(index_directory)


@pytest.mark.parametrize('kind', ['btree', 'bitmap'])
def test_java_records_per_file_option_overrides_table_alias(rest_catalog, kind):
    catalog, _ = rest_catalog
    table = _create(catalog)
    _append(table, ROWS)
    messages = _build(table, kind, options={'sorted-index.records-per-file': 100})
    files = _message_files(messages)
    assert len(files) == 1
    assert files[0].row_count == len(ROWS)
    _commit(table, messages)
    assert _ids(table, table.new_read_builder().new_predicate_builder().equal('name', 'a')) == [1, 5]


@pytest.mark.parametrize('options', [{'write.native.enabled': 'false'}, {'data-evolution.enabled': 'false'}])
def test_ineligible_tables_choose_python_before_building(rest_catalog, options):
    catalog, _ = rest_catalog
    table = _create(catalog, options=options)
    _append(table, ROWS)
    original = GlobalIndexBuilder._build_sorted_index
    with patch.object(GlobalIndexBuilder, '_build_sorted_index', autospec=True, side_effect=original) as build:
        assert table.create_global_index('name') > 0
    build.assert_called_once()
    assert _ids(table, table.new_read_builder().new_predicate_builder().equal('name', 'a')) == [1, 5]


@pytest.mark.parametrize('kind', ['btree', 'bitmap'])
@pytest.mark.parametrize('native_commit', [False, True])
@pytest.mark.parametrize('same_commit', [False, True])
def test_target_column_changes_reject_staged_indexes_without_deleting_files(
        rest_catalog, kind, native_commit, same_commit):
    catalog, _ = rest_catalog
    table = _create(catalog, options={'commit.native.enabled': str(native_commit).lower()})
    _append(table, ROWS)
    messages = _build(table, kind)
    private = _message_files(messages)
    updater = table.new_batch_write_builder().new_update()
    updated = updater.update_by_arrow_with_row_id(pa.table({'_ROW_ID': [0], 'name': ['changed']}))
    if not same_commit:
        _commit(table, updated)
    before = table.snapshot_manager().get_latest_snapshot().id
    with pytest.raises(Exception, match='Global index source conflict'):
        _commit(table, messages + updated if same_commit else messages)
    assert table.snapshot_manager().get_latest_snapshot().id == before
    assert all(table.file_io.exists(_path(table, file)) for file in private)
    if not same_commit:
        assert _ids(table, table.new_read_builder().new_predicate_builder().equal('name', 'changed')) == [3]


@pytest.mark.parametrize('kind', ['btree', 'bitmap'])
@pytest.mark.parametrize('native_commit', [False, True])
def test_append_and_unrelated_column_changes_do_not_invalidate_staged_index(rest_catalog, kind, native_commit):
    catalog, _ = rest_catalog
    table = _create(catalog, options={'commit.native.enabled': str(native_commit).lower()})
    _append(table, ROWS)
    messages = _build(table, kind)
    assert all(struct.unpack('>iiq', file.global_index_meta.source_meta) == (0x44454958, 1, 1)
               for file in _message_files(messages))
    updater = table.new_batch_write_builder().new_update()
    _commit(table, updater.update_by_arrow_with_row_id(pa.table({'_ROW_ID': [0], 'id': pa.array([10], pa.int32())})))
    _append(table, [{'id': 6, 'name': 'a', 'pt': 0}])
    _commit(table, messages)
    assert _ids(table, table.new_read_builder().new_predicate_builder().equal('name', 'a')) == [1, 5, 6]


@pytest.mark.parametrize('kind', ['btree', 'bitmap'])
@pytest.mark.parametrize('method, literals, partitions', [
    ('in', [], set()), ('notIn', [], {(0,), (1,)}),
    ('in', [None, 0], {(0,)}), ('notIn', [None, 0], set()),
])
def test_partition_set_predicates_follow_java_null_semantics(rest_catalog, kind, method, literals, partitions):
    catalog, _ = rest_catalog
    table = _create(catalog, partitioned=True)
    _append(table, ROWS)
    from pypaimon.common.predicate import Predicate
    predicate = Predicate(method, 2, 'pt', literals)
    messages = _build(table, kind, partition_filter=predicate)
    assert {message.partition for message in messages} == partitions


@pytest.mark.parametrize('restricted', [False, True])
def test_query_authorization_remains_on_python_build_path(rest_catalog, restricted):
    from pypaimon.catalog.rest.rest_catalog import RESTCatalog
    from pypaimon.catalog.table_query_auth import TableQueryAuthResult
    from pypaimon.catalog.catalog_exception import TableNoPermissionException

    catalog, _ = rest_catalog
    table = _create(catalog)
    _append(table, ROWS)
    table = table.copy({'query-auth.enabled': 'true'})
    auth = TableQueryAuthResult(None, {'name': '{"name":"NULL"}'} if restricted else None)
    original = GlobalIndexBuilder._build_sorted_index
    with patch.object(RESTCatalog, 'auth_table_query', return_value=auth), \
            patch('pypaimon.globalindex.native_index_build._native_table',
                  side_effect=AssertionError('native auth build')), \
            patch.object(GlobalIndexBuilder, '_build_sorted_index', autospec=True, side_effect=original) as build:
        if restricted:
            with pytest.raises(TableNoPermissionException):
                GlobalIndexBuilder(table, 'name').build()
            build.assert_not_called()
        else:
            assert GlobalIndexBuilder(table, 'name').build()
            build.assert_called_once()


@pytest.mark.parametrize('kind', ['btree', 'bitmap'])
@pytest.mark.parametrize('native_commit', [False, True])
@pytest.mark.parametrize('remove_all', [False, True])
def test_deleted_column_deltas_and_removed_source_ranges_reject_staged_index(
        rest_catalog, kind, native_commit, remove_all):
    from pypaimon.write.commit_message import CommitMessage

    catalog, _ = rest_catalog
    table = _create(catalog, options={'commit.native.enabled': str(native_commit).lower()})
    _append(table, ROWS)
    updates = table.new_batch_write_builder().new_update().update_by_arrow_with_row_id(
        pa.table({'_ROW_ID': [0], 'name': ['changed']}))
    _commit(table, updates)
    messages = _build(table, kind)
    if remove_all:
        commit = table.new_batch_write_builder().new_commit()
        try:
            commit.truncate_table()
        finally:
            commit.close()
    else:
        deletions = [CommitMessage(partition=message.partition, bucket=message.bucket, new_files=[],
                                   deleted_files=message.new_files) for message in updates]
        _commit(table, deletions)
    with pytest.raises(Exception, match='Global index (source|row ID existence) conflict'):
        _commit(table, messages)
    assert all(table.file_io.exists(_path(table, file)) for file in _message_files(messages))


@pytest.mark.parametrize('native_commit', [False, True])
@pytest.mark.parametrize('source', [b'DEIX', struct.pack('>iiq', 0x44454958, 1, 99)])
def test_malformed_or_future_index_source_is_rejected(rest_catalog, native_commit, source):
    catalog, _ = rest_catalog
    table = _create(catalog, options={'commit.native.enabled': str(native_commit).lower()})
    _append(table, ROWS)
    messages = _build(table, 'btree')
    for file in _message_files(messages):
        file.global_index_meta.source_meta = source
    with pytest.raises(Exception, match='source metadata|source snapshot'):
        _commit(table, messages)
    assert table.snapshot_manager().get_latest_snapshot().id == 1
    assert all(table.file_io.exists(_path(table, file)) for file in _message_files(messages))
