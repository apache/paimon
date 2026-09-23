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

from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.catalog.catalog_environment import CatalogEnvironment
from pypaimon.common.identifier import Identifier
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
from pypaimon.table.file_store_table import FileStoreTable
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.native_commit import (
    create_native_commit, native_commit_available, native_messages_supported)
from pypaimon.write.table_write import StreamTableWrite


requires_native = pytest.mark.skipif(
    not native_commit_available(), reason='pypaimon-rust runtime required')


def _table(tmp_path, mode='append', backend='filesystem'):
    options = {'warehouse': str(tmp_path / 'warehouse')}
    if backend == 'jdbc':
        options.update({'metastore': 'jdbc', 'uri': 'jdbc:sqlite:' + str(tmp_path / 'catalog.db')})
    catalog = CatalogFactory.create(options)
    catalog.create_database('default', True)
    options = {'file.format': 'parquet', 'commit.native.enabled': 'true'}
    if mode == 'pk':
        options['bucket'] = '1'
    elif mode == 'de':
        options.update({'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true'})
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        pa.schema([('id', pa.int64()), ('pt', pa.string())]),
        options=options, primary_keys=['id'] if mode == 'pk' else [],
        partition_keys=[] if mode in ('pk', 'unpartitioned') else ['pt']), False)
    table = catalog.get_table('default.t')
    if backend == 'path':
        table = FileStoreTable.from_path(table.table_path)
    if backend == 'jdbc':
        catalog.close()
    return table


def _prepare(builder, rows, identifier=None):
    writer = builder.new_write()
    try:
        writer.write_arrow(pa.Table.from_pylist(
            rows, schema=pa.schema([('id', pa.int64()), ('pt', pa.string())])))
        return writer.prepare_commit() if identifier is None else writer.prepare_commit(identifier)
    finally:
        writer.close()


def _rows(table):
    builder = table.new_read_builder()
    return sorted(builder.new_read().to_arrow(
        builder.new_scan().plan().splits()).to_pylist(), key=lambda row: row['id'])


def _must_not_fallback(commit, method='commit'):
    return patch.object(commit.file_store_commit, method, side_effect=AssertionError('Python fallback'))


def _seed(table):
    builder = table.copy({'commit.native.enabled': 'false'}).new_batch_write_builder()
    commit = builder.new_commit()
    try:
        commit.commit(_prepare(builder, [
            {'id': 1, 'pt': 'a'}, {'id': 2, 'pt': 'b'}, {'id': 3, 'pt': None}]))
    finally:
        commit.close()


def test_native_commit_is_opt_in():
    assert not CoreOptions(Options({})).native_commit_enabled()
    assert CoreOptions(Options({'commit.native.enabled': 'true'})).native_commit_enabled()


def test_overwrite_builder_api_is_batch_only(tmp_path):
    table = _table(tmp_path)
    batch = table.new_batch_write_builder()
    assert batch.overwrite({'pt': 'a'}) is batch
    assert batch.static_partition == {'pt': 'a'}
    stream = table.new_stream_write_builder()
    with pytest.raises(AttributeError):
        stream.overwrite({'pt': 'a'})
    with pytest.raises(TypeError):
        StreamTableWrite(table, stream.commit_user, {'pt': 'a'})


@requires_native
@pytest.mark.parametrize('backend', ['filesystem', 'path', 'jdbc'])
@pytest.mark.parametrize('mode', ['append', 'pk', 'de'])
def test_native_batch_roundtrip_preserves_identity(tmp_path, backend, mode):
    table = _table(tmp_path, mode, backend)
    builder = table.new_batch_write_builder()
    rows = [{'id': 1, 'pt': 'a'}, {'id': 2, 'pt': None}]
    messages = _prepare(builder, rows)
    commit = builder.new_commit()
    try:
        with _must_not_fallback(commit):
            commit.commit(messages)
        snapshot = table.snapshot_manager().get_latest_snapshot()
        assert snapshot.commit_user == builder.commit_user
        assert snapshot.commit_identifier == BATCH_COMMIT_IDENTIFIER
        assert _rows(table) == rows
        with pytest.raises(RuntimeError, match='one-time'):
            commit.commit(messages)
    finally:
        commit.close()


@requires_native
def test_native_stream_reuses_commit_user_and_identifiers(tmp_path):
    table = _table(tmp_path)
    builder = table.new_stream_write_builder()
    commit = builder.new_commit()
    try:
        with _must_not_fallback(commit):
            for identifier in (7, 8):
                commit.commit(_prepare(builder, [{'id': identifier, 'pt': None}], identifier), identifier)
                snapshot = table.snapshot_manager().get_latest_snapshot()
                assert snapshot.commit_user == builder.commit_user
                assert snapshot.commit_identifier == identifier
        assert _rows(table) == [{'id': 7, 'pt': None}, {'id': 8, 'pt': None}]
    finally:
        commit.close()


@requires_native
@pytest.mark.parametrize('mode,dynamic,spec', [
    ('append', True, {'pt': 'ignored'}),
    ('append', False, {'pt': 'a'}),
    ('de', True, {}),
    ('pk', True, {}),
    ('unpartitioned', True, {}),
])
def test_native_overwrite_replaces_only_target_rows(tmp_path, mode, dynamic, spec):
    table = _table(tmp_path, mode).copy({
        'dynamic-partition-overwrite': str(dynamic).lower(), 'commit.user-prefix': 'python'})
    _seed(table)
    builder = table.new_batch_write_builder().overwrite(spec)
    commit = builder.new_commit()
    try:
        messages = _prepare(builder, [{'id': 4, 'pt': 'a'}])
        with _must_not_fallback(commit, 'overwrite'):
            commit.commit(messages)
        expected = [{'id': 4, 'pt': 'a'}]
        if table.partition_keys:
            expected = [{'id': 2, 'pt': 'b'}, {'id': 3, 'pt': None}] + expected
        assert _rows(table) == expected
        snapshot = table.snapshot_manager().get_latest_snapshot()
        assert snapshot.commit_user == builder.commit_user
        assert snapshot.commit_identifier == BATCH_COMMIT_IDENTIFIER
        assert snapshot.commit_kind == 'OVERWRITE'
        with pytest.raises(RuntimeError, match='one-time'):
            commit.commit(messages)
    finally:
        commit.close()


@requires_native
@pytest.mark.parametrize('mode,dynamic,spec,remaining', [
    ('append', True, {}, [1, 2, 3]),
    ('append', False, {'pt': 'a'}, [2, 3]),
    ('append', False, {'pt': None}, [1, 2]),
    ('append', False, {'pt': '__DEFAULT_PARTITION__'}, [1, 2]),
    ('append', False, {}, []),
    ('unpartitioned', True, {}, []),
    ('pk', True, {}, []),
])
def test_native_empty_overwrite_semantics(tmp_path, mode, dynamic, spec, remaining):
    table = _table(tmp_path, mode).copy({'dynamic-partition-overwrite': str(dynamic).lower()})
    _seed(table)
    builder = table.new_batch_write_builder().overwrite(spec)
    commit = builder.new_commit()
    try:
        with _must_not_fallback(commit, 'overwrite'):
            commit.commit([])
        assert [row['id'] for row in _rows(table)] == remaining
        snapshot = table.snapshot_manager().get_latest_snapshot()
        if remaining == [1, 2, 3]:
            assert snapshot.id == 1
        else:
            assert snapshot.id == 2
            assert snapshot.commit_user == builder.commit_user
            assert snapshot.commit_kind == 'OVERWRITE'
    finally:
        commit.close()


@requires_native
@pytest.mark.parametrize('value', ['off', '0', ' false '])
def test_native_overwrite_normalizes_python_boolean_option(tmp_path, value):
    table = _table(tmp_path).copy({
        'dynamic-partition-overwrite': value, 'snapshot.ignore-empty-commit': value})
    _seed(table)
    commit = table.new_batch_write_builder().overwrite({'pt': 'a'}).new_commit()
    try:
        with _must_not_fallback(commit, 'overwrite'):
            commit.commit([])
        assert [row['id'] for row in _rows(table)] == [2, 3]
    finally:
        commit.close()


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('case', ['unpartitioned', 'static-empty', 'static-missing', 'dynamic-empty'])
def test_empty_overwrite_records_java_snapshot(tmp_path, native, case):
    if native and not native_commit_available():
        pytest.skip('pypaimon-rust runtime required')
    table = _table(tmp_path, 'unpartitioned' if case == 'unpartitioned' else 'append').copy({
        'commit.native.enabled': str(native).lower(),
        'dynamic-partition-overwrite': str(case == 'dynamic-empty').lower(),
    })
    if case == 'static-missing':
        _seed(table)
    builder = table.new_batch_write_builder().overwrite(
        {'pt': 'missing'} if case.startswith('static') else {})
    commit = builder.new_commit()
    try:
        if native:
            with _must_not_fallback(commit, 'overwrite'):
                commit.commit([])
        else:
            commit.commit([])
        snapshot = table.snapshot_manager().get_latest_snapshot()
        if case == 'dynamic-empty':
            assert snapshot is None
        else:
            assert snapshot.id == (2 if case == 'static-missing' else 1)
            assert snapshot.commit_user == builder.commit_user
            assert snapshot.commit_kind == 'OVERWRITE'
            assert snapshot.total_record_count == (3 if case == 'static-missing' else 0)
        assert [row['id'] for row in _rows(table)] == ([1, 2, 3] if case == 'static-missing' else [])
    finally:
        commit.close()


@requires_native
@pytest.mark.parametrize('mode', ['batch', 'stream'])
@pytest.mark.parametrize('ignore', [True, False])
def test_native_empty_commit_preserves_python_option(tmp_path, mode, ignore):
    table = _table(tmp_path).copy({'snapshot.ignore-empty-commit': str(ignore).lower()})
    commit = getattr(table, 'new_' + mode + '_write_builder')().new_commit()
    try:
        with _must_not_fallback(commit):
            if mode == 'batch':
                commit.commit([])
            else:
                commit.commit([], 7)
        snapshot = table.snapshot_manager().get_latest_snapshot()
        if ignore:
            assert snapshot is None
        else:
            assert snapshot.commit_identifier == (BATCH_COMMIT_IDENTIFIER if mode == 'batch' else 7)
            assert snapshot.total_record_count == 0
    finally:
        commit.close()


@requires_native
@pytest.mark.parametrize('overwrite', [False, True])
def test_native_abort_removes_uncommitted_files(tmp_path, overwrite):
    table = _table(tmp_path)
    builder = table.new_batch_write_builder()
    if overwrite:
        builder.overwrite()
    messages = _prepare(builder, [{'id': 1, 'pt': None}])
    files = list(tmp_path.rglob('data-*.parquet'))
    assert files
    commit = builder.new_commit()
    try:
        with patch.object(commit.file_store_commit, 'abort', side_effect=AssertionError('Python fallback')):
            commit.abort(messages)
        assert not any(path.exists() for path in files)
        assert table.snapshot_manager().get_latest_snapshot() is None
    finally:
        commit.close()


@pytest.mark.parametrize('failure', ['missing', 'construction', 'conversion'])
def test_preflight_failure_uses_python(tmp_path, failure):
    table = _table(tmp_path)
    builder = table.new_batch_write_builder()
    messages = _prepare(builder, [{'id': 1, 'pt': 'a'}])
    commit = builder.new_commit()
    native = Mock()
    with patch('pypaimon.write.native_commit.create_native_commit', return_value=native) as create:
        if failure == 'missing':
            create.return_value = None
        elif failure == 'construction':
            create.side_effect = RuntimeError('unsupported FileIO')
        with patch('pypaimon.write.native_commit.to_native_commit_messages',
                   side_effect=ValueError('unsupported payload')):
            commit.commit(messages)
    native.commit.assert_not_called()
    assert _rows(table) == [{'id': 1, 'pt': 'a'}]
    commit.close()


@pytest.mark.parametrize('method', ['commit', 'abort'])
@pytest.mark.parametrize('overwrite', [False, True])
def test_native_mutation_failure_never_falls_back_or_aborts(tmp_path, method, overwrite):
    table = _table(tmp_path)
    builder = table.new_batch_write_builder()
    if overwrite:
        builder.overwrite()
    messages = _prepare(builder, [{'id': 1, 'pt': 'a'}])
    commit = builder.new_commit()
    native = Mock()
    getattr(native, method).side_effect = OSError('outcome unknown')
    with patch('pypaimon.write.native_commit.create_native_commit', return_value=native), \
            patch('pypaimon.write.native_commit.to_native_commit_messages', return_value=['wire']), \
            patch.object(commit.file_store_commit, 'commit') as fallback, \
            patch.object(commit.file_store_commit, 'overwrite') as fallback_overwrite, \
            patch.object(commit.file_store_commit, 'abort') as abort:
        with pytest.raises(OSError, match='outcome unknown'):
            getattr(commit, method)(messages)
        fallback.assert_not_called()
        fallback_overwrite.assert_not_called()
        abort.assert_not_called()
        if method == 'commit':
            native.abort.assert_not_called()
            with pytest.raises(RuntimeError, match='one-time'):
                commit.commit(messages)
    assert list(tmp_path.rglob('data-*.parquet'))
    commit.close()


@requires_native
@pytest.mark.parametrize('overwrite', [False, True])
def test_publication_response_loss_does_not_duplicate_or_delete_files(tmp_path, overwrite):
    table = _table(tmp_path)
    builder = table.new_batch_write_builder()
    if overwrite:
        builder.overwrite()
    messages = _prepare(builder, [{'id': 1, 'pt': None}])
    native = create_native_commit(table, builder.commit_user, builder.static_partition)
    proxy = Mock(wraps=native)

    def publish_then_fail(*args):
        native.commit(*args)
        raise OSError('response lost')

    proxy.commit.side_effect = publish_then_fail
    commit = builder.new_commit()
    try:
        with patch('pypaimon.write.native_commit.create_native_commit', return_value=proxy), \
                _must_not_fallback(commit, 'overwrite' if overwrite else 'commit'), \
                patch.object(commit.file_store_commit, 'abort', side_effect=AssertionError('abort')):
            with pytest.raises(OSError, match='response lost'):
                commit.commit(messages)
        assert table.snapshot_manager().get_latest_snapshot().id == 1
        assert _rows(table) == [{'id': 1, 'pt': None}]
        proxy.abort.assert_not_called()
    finally:
        commit.close()


@pytest.mark.parametrize('properties', [{}, {'source': 'python'}])
@pytest.mark.parametrize('overwrite', [False, True])
def test_snapshot_properties_select_python_before_native(tmp_path, properties, overwrite):
    table = _table(tmp_path)
    builder = table.new_batch_write_builder()
    if overwrite:
        builder.overwrite()
    commit = builder.new_commit()
    with patch('pypaimon.write.native_commit.create_native_commit',
               side_effect=AssertionError('must not initialize native')) as create:
        commit.commit(_prepare(builder, [{'id': 1, 'pt': None}]), snapshot_properties=properties)
        create.assert_not_called()
    assert table.snapshot_manager().get_latest_snapshot().properties == (properties or None)
    commit.close()


@pytest.mark.parametrize('warmup', [False, True])
def test_callbacks_added_after_construction_select_python(tmp_path, warmup):
    if warmup and not native_commit_available():
        pytest.skip('native warmup requires the commit bindings')
    table = _table(tmp_path)
    builder = table.new_stream_write_builder()
    commit = builder.new_commit()
    if warmup:
        with _must_not_fallback(commit):
            commit.commit(_prepare(builder, [{'id': 6, 'pt': None}], 6), 6)
    callback = Mock()
    commit.add_commit_callback(callback)
    with patch('pypaimon.write.native_commit.create_native_commit') as create:
        commit.commit(_prepare(builder, [{'id': 1, 'pt': None}], 7), 7)
        create.assert_not_called()
    callback.call.assert_called_once()
    commit.close()
    callback.close.assert_called_once()


def test_truncate_uses_python(tmp_path):
    table = _table(tmp_path)
    _seed(table)
    with patch('pypaimon.write.native_commit.create_native_commit') as create:
        for method, args in [('truncate_partitions', ([{'pt': 'a'}],)), ('truncate_table', ())]:
            commit = table.new_batch_write_builder().new_commit()
            try:
                getattr(commit, method)(*args)
            finally:
                commit.close()
        create.assert_not_called()
    assert _rows(table) == []


def test_overwrite_conversion_failure_preserves_partition_scope(tmp_path):
    table = _table(tmp_path).copy({'dynamic-partition-overwrite': 'false'})
    _seed(table)
    builder = table.new_batch_write_builder().overwrite({'pt': 'a'})
    commit = builder.new_commit()
    native = Mock()
    try:
        messages = _prepare(builder, [{'id': 4, 'pt': 'a'}])
        with patch('pypaimon.write.native_commit.create_native_commit', return_value=native), \
                patch('pypaimon.write.native_commit.to_native_commit_messages',
                      side_effect=ValueError('unsupported payload')):
            commit.commit(messages)
        native.commit.assert_not_called()
        native.abort.assert_not_called()
        assert _rows(table) == [{'id': 2, 'pt': 'b'}, {'id': 3, 'pt': None}, {'id': 4, 'pt': 'a'}]
        assert table.snapshot_manager().get_latest_snapshot().commit_user == builder.commit_user
    finally:
        commit.close()


def test_overwrite_callbacks_select_python(tmp_path):
    table = _table(tmp_path)
    builder = table.new_batch_write_builder().overwrite()
    commit = builder.new_commit()
    callback = Mock()
    commit.add_commit_callback(callback)
    with patch('pypaimon.write.native_commit.create_native_commit') as create:
        commit.commit(_prepare(builder, [{'id': 1, 'pt': 'a'}]))
        create.assert_not_called()
    assert _rows(table) == [{'id': 1, 'pt': 'a'}]
    callback.call.assert_called_once()
    commit.close()
    callback.close.assert_called_once()


def test_disabled_option_never_initializes_native(tmp_path):
    table = _table(tmp_path).copy({'commit.native.enabled': 'false'})
    builder = table.new_batch_write_builder()
    commit = builder.new_commit()
    with patch('pypaimon.write.native_commit.create_native_commit') as create:
        commit.commit(_prepare(builder, [{'id': 1, 'pt': None}]))
        create.assert_not_called()
    assert _rows(table) == [{'id': 1, 'pt': None}]
    commit.close()


@pytest.mark.parametrize('kind', ['version-managed', 'custom-env', 'branch', 'custom-io'])
def test_incompatible_publication_environment_is_not_reconstructed(tmp_path, kind):
    table = _table(tmp_path)
    if kind == 'version-managed':
        table.catalog_environment.supports_version_management = True
    elif kind == 'custom-env':
        class CustomEnvironment(CatalogEnvironment):
            pass
        table.catalog_environment = CustomEnvironment()
    elif kind == 'branch':
        table.identifier = Identifier('default', 't', branch='dev')
    else:
        table.file_io = Mock()
    with patch('pypaimon.write.native_commit.native_commit_available', return_value=True), \
            patch('pypaimon.write.native_commit._resolved_schema_file_io_options') as resolve:
        assert create_native_commit(table, 'job') is None
        resolve.assert_not_called()


@pytest.mark.parametrize('missing_type,missing_method', [
    ('Table', 'from_resolved_schema'),
    ('CommitMessage', 'deserialize'),
    ('StreamWriteBuilder', 'with_commit_user'),
    ('BatchWriteBuilder', '_with_commit_user'),
    ('BatchWriteBuilder', 'with_overwrite'),
])
def test_incomplete_runtime_falls_back_without_reconstructing_table(
        tmp_path, missing_type, missing_method):
    table = _table(tmp_path)
    with patch('pypaimon.write.native_commit.native_method_available',
               side_effect=lambda cls, method: (cls, method) != (missing_type, missing_method)), \
            patch('pypaimon.write.native_commit._resolved_schema_file_io_options') as resolve:
        assert create_native_commit(table, 'job') is None
        resolve.assert_not_called()


def test_missing_runtime_falls_back_without_reconstructing_table(tmp_path):
    table = _table(tmp_path)
    with patch('pypaimon.write.native_commit.native_commit_available', return_value=False), \
            patch('pypaimon.write.native_commit._resolved_schema_file_io_options') as resolve:
        assert create_native_commit(table, 'job') is None
        resolve.assert_not_called()


def test_partial_row_id_and_compact_messages_preserve_python_recovery(tmp_path):
    table = _table(tmp_path, 'de')
    assert not native_messages_supported(table, [CommitMessage((), 0, [], check_from_snapshot=7)])
    assert not native_messages_supported(table, [CommitMessage((), 0, [Mock(first_row_id=1)])])
    assert not native_messages_supported(table, [CommitMessage((), 0, [], compact_after=[Mock()])])


def test_close_releases_python_resources_even_if_native_close_fails(tmp_path):
    commit = _table(tmp_path).new_batch_write_builder().new_commit()
    commit._native_commit = Mock()
    commit._native_commit.close.side_effect = OSError('close failure')
    with patch.object(commit.file_store_commit, 'close') as close:
        with pytest.raises(OSError, match='close failure'):
            commit.close()
        close.assert_called_once()
