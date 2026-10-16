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

"""REST system-table reads use Rust metadata providers with Java semantics."""

import json
import pickle
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.read.native_plan import _native_table, native_method_available
from pypaimon.schema.data_types import AtomicType
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.table.system.native_system_table import _NATIVE_TABLES
from pypaimon.table.system.system_table_loader import load
from pypaimon.tests import native_plan_rest_test

rest_catalog = native_plan_rest_test.rest_catalog
pytestmark = pytest.mark.skipif(
    not native_method_available('Table', 'new_system_read'),
    reason='Native system-table reader required')

SCHEMA = pa.schema([('id', pa.int32()), ('value', pa.string()), ('pt', pa.string())])


@pytest.fixture
def source(rest_catalog):
    catalog, server = rest_catalog
    catalog.create_table('default.metadata', Schema.from_pyarrow_schema(
        SCHEMA, partition_keys=['pt'], options={
            'file.format': 'parquet', 'metadata.stats-mode': 'full',
            'write.native.enabled': 'false', 'commit.native.enabled': 'false',
        }), False)
    table = catalog.get_table('default.metadata')
    return catalog, table, server


def _append(table, rows):
    builder = table.copy({'write.native.enabled': 'false'}).new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pylist(rows, schema=SCHEMA).select(table.field_names))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()


def _system(table, name, native):
    return load(name, table.copy({'read.native.enabled': str(native).lower()}))


def _read(table, projection=None, predicate=None, limit=None):
    builder = table.new_read_builder()
    if projection is not None:
        builder.with_projection(projection)
    if predicate is not None:
        builder.with_filter(predicate)
    if limit is not None:
        builder.with_limit(limit)
    return builder.new_read().to_arrow(builder.new_scan().plan().splits())


def _canonical(table, name):
    rows = table.to_pylist()
    if name == 'schemas':
        for row in rows:
            for field in ('fields', 'partition_keys', 'primary_keys', 'options'):
                row[field] = json.loads(row[field])
    return sorted(json.dumps(row, sort_keys=True, default=str) for row in rows)


@pytest.mark.parametrize('name', sorted(_NATIVE_TABLES))
@pytest.mark.parametrize('empty', [True, False])
def test_rest_system_table_parity_and_no_python_materialization(source, name, empty):
    catalog, table, _ = source
    if not empty:
        _append(table, [{'id': 1, 'value': 'old', 'pt': 'a'},
                        {'id': 2, 'value': None, 'pt': None}])
        table.create_tag('first', snapshot_id=1, time_retained='1d')
        catalog.create_tag(table.identifier, 'first', 1)
        catalog.create_branch(table.identifier, 'dev', tag_name='first')
        from pypaimon.consumer.consumer import Consumer
        table.consumer_manager().reset_consumer('reader', Consumer(2))
    expected = _read(_system(table, name, False))
    native = _system(table, name, True)
    with patch.object(type(native), '_build_arrow_table',
                      side_effect=AssertionError('Python metadata materialized')):
        actual = _read(native)
    if not empty and name in ('tags', 'branches', 'consumers'):
        assert actual.num_rows > 0
    assert actual.schema == expected.schema
    assert _canonical(actual, name) == _canonical(expected, name)


def test_projection_filter_limit_and_pickled_split(source):
    _, table, _ = source
    _append(table, [{'id': 1, 'value': 'a', 'pt': 'old'}])
    _append(table, [{'id': 2, 'value': 'b', 'pt': 'new'}])
    system = _system(table, 'snapshots', True)
    predicate = PredicateBuilder(system.row_type().fields).greater_than('snapshot_id', 1)
    assert _read(system, ['snapshot_id'], predicate, 1).to_pylist() == [{'snapshot_id': 2}]
    assert _read(system, ['snapshot_id'], limit=0).num_rows == 0
    builder = system.new_read_builder().with_projection(['snapshot_id'])
    splits = pickle.loads(pickle.dumps(builder.new_scan().plan().splits()))
    _append(table, [{'id': 3, 'value': 'c', 'pt': 'later'}])
    assert builder.new_read().to_arrow(splits).to_pylist() == [{'snapshot_id': 1}, {'snapshot_id': 2}]


@pytest.mark.parametrize('name', ['files', 'file_key_ranges', 'buckets', 'manifests', 'table_indexes'])
@pytest.mark.parametrize('view', ['first', 'empty', 'tag'])
def test_captured_metadata_view(source, name, view):
    catalog, table, _ = source
    _append(table, [{'id': 1, 'value': 'a', 'pt': 'old'}])
    first = table.snapshot_manager().get_latest_snapshot()
    table.create_tag('first', snapshot_id=1)
    catalog.create_tag(table.identifier, 'first', 1)
    _append(table, [{'id': 2, 'value': 'b', 'pt': 'new'}])
    if view == 'tag':
        table = table.copy({'scan.tag-name': 'first'})
    else:
        table = table._copy_with_snapshot(first if view == 'first' else None)
    expected = _read(_system(table, name, False))
    actual = _read(_system(table, name, True))
    assert _canonical(actual, name) == _canonical(expected, name)
    if view == 'empty':
        assert actual.num_rows == 0
    elif name in ('files', 'file_key_ranges'):
        assert actual.column('partition').to_pylist() == ['{old}']


def test_options_use_latest_persisted_schema(source):
    catalog, table, _ = source
    _append(table, [{'id': 1, 'value': 'a', 'pt': 'a'}])
    catalog.alter_table(table.identifier, [SchemaChange.set_option('metadata.marker', 'persisted')], False)
    table = table.copy({'scan.snapshot-id': '1', 'metadata.marker': 'dynamic'})
    for native in (False, True):
        result = _read(_system(table, 'options', native)).to_pylist()
        options = {row['key']: row['value'] for row in result}
        assert options['metadata.marker'] == 'persisted'
        assert 'scan.snapshot-id' not in options


def test_stats_follow_readded_column_ids(source):
    catalog, table, _ = source
    _append(table, [{'id': 1, 'value': 'old', 'pt': 'a'}])
    catalog.alter_table(table.identifier, [SchemaChange.drop_column('value')], False)
    catalog.alter_table(table.identifier, [SchemaChange.add_column('value', AtomicType('STRING'))], False)
    table = catalog.get_table(table.identifier)
    _append(table, [{'id': 2, 'value': 'new', 'pt': 'a'}])
    for native in (False, True):
        rows = sorted(_read(_system(table, 'files', native)).to_pylist(), key=lambda row: row['schema_id'])
        assert rows[0]['min_value_stats'] == '{id=1, pt=a, value=null}'
        assert rows[0]['null_value_counts'] == '{id=0, pt=0, value=1}'
        assert rows[1]['min_value_stats'] == '{id=2, pt=a, value=new}'


def test_native_error_is_not_retried_in_python(source):
    _, table, _ = source
    system = _system(table, 'files', True)
    with patch('pypaimon.table.system.native_system_table._native_table', side_effect=RuntimeError('denied')):
        with patch.object(type(system), '_build_arrow_table') as fallback:
            with pytest.raises(RuntimeError, match='denied'):
                _read(system)
            fallback.assert_not_called()


def test_native_reader_preserves_schema_on_empty_and_rejects_catalog_tables(source):
    _, table, _ = source
    native = _native_table(table)
    read = native.new_system_read('files')
    result = read.read()
    assert result.schema.names[16] == 'deleteRowCount'
    assert result.num_rows == 0
    assert [field.name() for field in read.read_type()] == result.schema.names
    with pytest.raises(Exception, match='requires a catalog|unknown'):
        native.new_system_read('partitions')
    with pytest.raises(Exception, match='requires a catalog|unknown'):
        native.new_system_read('unknown')


def test_writer_version_is_loaded_from_snapshot_metadata(source):
    _, table, _ = source
    _append(table, [{'id': 1, 'value': 'a', 'pt': 'a'}])
    path = table.snapshot_manager().snapshot_dir + '/snapshot-1'
    with table.file_io.new_input_stream(path) as stream:
        snapshot = json.loads(stream.read())
    snapshot['writerVersion'] = 'Java test writer'
    table.file_io.overwrite_file_utf8(path, json.dumps(snapshot))
    assert _read(_system(table, 'snapshots', True), ['writer_version']).to_pylist() == [
        {'writer_version': 'Java test writer'}]


@pytest.mark.parametrize('name', ['files', 'file_key_ranges', 'buckets', 'snapshots', 'schemas', 'consumers'])
def test_branch_namespace(source, name):
    from pypaimon.common.identifier import Identifier
    from pypaimon.consumer.consumer import Consumer
    catalog, table, _ = source
    _append(table, [{'id': 1, 'value': 'main', 'pt': 'a'}])
    table.create_tag('first', snapshot_id=1)
    catalog.create_tag(table.identifier, 'first', 1)
    catalog.create_branch(table.identifier, 'dev', tag_name='first')
    branch = catalog.get_table(Identifier('default', 'metadata', branch='dev'))
    branch.consumer_manager().reset_consumer('branch-reader', Consumer(17))
    table.consumer_manager().reset_consumer('main-reader', Consumer(91))
    actual = _read(_system(branch, name, True))
    expected = _read(_system(branch, name, False))
    assert _canonical(actual, name) == _canonical(expected, name)
    if name == 'consumers':
        assert actual.to_pylist() == [{'consumer_id': 'branch-reader', 'next_snapshot_id': 17}]


def test_vertical_file_statistics_use_physical_columns(rest_catalog):
    catalog, _ = rest_catalog
    schema = pa.schema([('id', pa.int32()), ('value', pa.int32())])
    catalog.create_table('default.vertical', Schema.from_pyarrow_schema(schema, options={
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'metadata.stats-mode': 'full', 'write.native.enabled': 'false',
        'commit.native.enabled': 'false', 'file.format': 'parquet',
    }), False)
    table = catalog.get_table('default.vertical')
    builder = table.new_batch_write_builder()
    left = builder.new_write().with_write_type(['id'])
    right = builder.new_write().with_write_type(['value'])
    commit = builder.new_commit()
    try:
        left.write_arrow(pa.table({'id': pa.array([1, 2], type=pa.int32())}))
        right.write_arrow(pa.table({'value': pa.array([41, 42], type=pa.int32())}))
        commit.commit(left.prepare_commit() + right.prepare_commit())
    finally:
        left.close()
        right.close()
        commit.close()
    actual = _read(_system(table, 'files', True))
    expected = _read(_system(table, 'files', False))
    assert _canonical(actual, 'files') == _canonical(expected, 'files')
    stats = {tuple(row['write_cols']): row for row in actual.to_pylist()}
    assert stats[('id',)]['min_value_stats'] == '{id=1, value=null}'
    assert stats[('value',)]['min_value_stats'] == '{id=null, value=41}'
    assert stats[('value',)]['null_value_counts'] == '{id=2, value=0}'


def test_primary_key_ranges_are_java_arrays(rest_catalog):
    catalog, _ = rest_catalog
    catalog.create_table('default.keyed', Schema.from_pyarrow_schema(
        SCHEMA, partition_keys=['pt'], primary_keys=['pt', 'id'], options={
            'bucket': '1', 'file.format': 'parquet', 'write.native.enabled': 'false',
            'commit.native.enabled': 'false'}), False)
    table = catalog.get_table('default.keyed')
    _append(table, [{'id': 2, 'value': 'b', 'pt': 'a'}, {'id': 1, 'value': 'a', 'pt': 'a'}])
    for name in ('files', 'file_key_ranges'):
        actual, expected = _read(_system(table, name, True)), _read(_system(table, name, False))
        assert _canonical(actual, name) == _canonical(expected, name)
        assert actual.column('min_key').to_pylist() == ['[1]']
        assert actual.column('max_key').to_pylist() == ['[2]']


def test_nonempty_table_indexes(rest_catalog):
    from pypaimon.tests.native_sorted_index_build_test import _create, _append as append, _build, _commit
    catalog, _ = rest_catalog
    table = _create(catalog)
    append(table, [{'id': 1, 'name': 'a', 'pt': 0}, {'id': 2, 'name': 'b', 'pt': 0}])
    _commit(table, _build(table, 'btree'))
    actual = _read(_system(table, 'table_indexes', True))
    expected = _read(_system(table, 'table_indexes', False))
    assert _canonical(actual, 'table_indexes') == _canonical(expected, 'table_indexes')
    rows = actual.to_pylist()
    assert rows and rows[0]['index_field_name'] == 'name'
    # Populate an index before fixing the view to empty: otherwise both paths
    # return no indexes even if the Python reader incorrectly reloads latest.
    empty = table._copy_with_snapshot(None)
    for native in (False, True):
        result = _read(_system(empty, 'table_indexes', native))
        assert result.num_rows == 0
        assert result.schema == actual.schema


def test_native_metadata_auth_guard(source):
    _, table, _ = source
    denied = _native_table(table.copy({'query-auth.enabled': 'true'}))
    with pytest.raises(Exception, match='query-auth.enabled'):
        denied.new_system_read('options')


@pytest.mark.parametrize('keyed', [False, True])
def test_metadata_temporal_cells_use_java_object_strings(rest_catalog, keyed):
    from datetime import date, datetime, time, timezone
    catalog, _ = rest_catalog
    schema = pa.schema([('day', pa.date32()), ('clock', pa.time32('ms')),
                        ('ts', pa.timestamp('ms')), ('ltz', pa.timestamp('ms', tz='UTC'))])
    catalog.create_table('default.temporal', Schema.from_pyarrow_schema(
        schema, primary_keys=['day', 'clock', 'ts'] if keyed else [], options={
            'bucket': '1' if keyed else '-1', 'file.format': 'parquet',
            'metadata.stats-mode': 'full', 'write.native.enabled': 'false',
            'commit.native.enabled': 'false'}), False)
    table = catalog.get_table('default.temporal')
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pylist([{
            'day': date(2024, 1, 2), 'clock': time(1, 2, 3, 4000),
            'ts': datetime(2024, 1, 2, 0, 0, 0, 123000),
            'ltz': datetime(2024, 1, 2, tzinfo=timezone.utc),
        }], schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    actual = _read(_system(table, 'files', True))
    expected = _read(_system(table, 'files', False))
    assert _canonical(actual, 'files') == _canonical(expected, 'files')
    row = actual.to_pylist()[0]
    assert row['min_value_stats'] == (
        '{clock=3723004, day=19724, ltz=2024-01-02T00:00, ts=2024-01-02T00:00:00.123}')
    if keyed:
        ranges = _read(_system(table, 'file_key_ranges', True))
        assert ranges.column('min_key').to_pylist() == ['[19724, 3723004, 2024-01-02T00:00:00.123]']


@pytest.mark.parametrize('name', ['files', 'file_key_ranges'])
def test_metadata_paths_keep_escaped_partition_components(source, name):
    _, table, _ = source
    _append(table, [{'id': 1, 'value': 'a', 'pt': 'a/b%=λ'}])
    actual = _read(_system(table, name, True))
    expected = _read(_system(table, name, False))
    assert _canonical(actual, name) == _canonical(expected, name)
    path = actual.column('file_path')[0].as_py()
    assert '/pt=a%2Fb%25%3Dλ/bucket-0/' in path
    from pypaimon.utils.path import to_file_io_path
    assert table.file_io.exists(to_file_io_path(path))
    assert actual.column('partition').to_pylist() == ['{a/b%=λ}']


def test_system_read_filters_before_projection_without_python_arrow_filter(source):
    from pypaimon.read.native_plan import _predicate_to_native
    _, table, _ = source
    _append(table, [{'id': 1, 'value': 'a', 'pt': 'a'}])
    _append(table, [{'id': 2, 'value': 'b', 'pt': 'b'}])
    system = _system(table, 'snapshots', True)
    predicate = PredicateBuilder(system.row_type().fields).greater_than('snapshot_id', 1)
    read = _native_table(table).new_system_read('snapshots').with_projection(['commit_user'])
    read.with_filter(_predicate_to_native(predicate)).with_limit(1)
    with patch.object(type(predicate), 'to_arrow', side_effect=AssertionError('Python Arrow filter ran')):
        result = read.read()
    assert result.num_rows == 1
    assert result.schema.names == ['commit_user']
    assert [field.name() for field in read.read_type()] == ['commit_user']
    read.with_limit(0)
    empty = read.read()
    assert empty.num_rows == 0
    assert empty.schema == result.schema


def test_captured_native_split_can_be_reused_with_another_read_configuration(source):
    _, table, _ = source
    _append(table, [{'id': 1, 'value': 'a', 'pt': 'a'}])
    system = _system(table, 'snapshots', True)
    builder = system.new_read_builder().with_projection(['snapshot_id'])
    splits = pickle.loads(pickle.dumps(builder.new_scan().plan().splits()))
    assert splits[0].row_count == 1
    other = system.new_read_builder().with_projection(['commit_user']).new_read()
    assert other.to_arrow(splits).num_rows == 1


def test_captured_native_split_reads_without_recreating_rest_environment(source):
    _, table, _ = source
    _append(table, [{'id': 1, 'value': 'a', 'pt': 'a'}])
    system = _system(table, 'snapshots', True)
    splits = pickle.loads(pickle.dumps(system.new_read_builder().new_scan().plan().splits()))
    reader = system.new_read_builder().with_projection(['snapshot_id']).new_read()
    with patch('pypaimon.table.system.native_system_table._native_table',
               side_effect=AssertionError('REST environment recreated')):
        assert reader.to_arrow(splits).to_pylist() == [{'snapshot_id': 1}]


def test_system_read_repeated_and_empty_projections(source):
    _, table, _ = source
    read = _native_table(table).new_system_read('options')
    read.with_projection(['value', 'key', 'value'])
    result = read.read()
    assert result.num_rows > 0
    assert result.schema.names == ['value', 'key', 'value']
    assert result.column(0).to_pylist() == result.column(2).to_pylist()
    read.with_projection([])
    empty = read.read()
    assert empty.num_columns == 0
    assert empty.num_rows == result.num_rows
    assert read.read_type() == []
