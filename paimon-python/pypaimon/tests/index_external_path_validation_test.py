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

from contextlib import contextmanager
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.index.index_file_handler import IndexFileHandler
from pypaimon.table.data_evolution_merge_into import WhenMatched, WhenNotMatched
from pypaimon.write.table_delete import TableDeleteByRowId


pytestmark = pytest.mark.execution_defaults
_OPTION = 'global-index.external-paths'
_ERROR = "does not support 'global-index.external-paths'"


def _table(tmp_path, *, dynamic=False, options=None):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('db', True)
    table_options = {
        'bucket': '-1', 'file.format': 'parquet',
        'write.native.enabled': 'false', 'commit.native.enabled': 'false',
        'scan.native-plan.enabled': 'false', 'read.native.enabled': 'false',
    }
    if not dynamic:
        table_options.update({
            'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true',
            'deletion-vectors.enabled': 'true',
        })
    table_options.update(options or {})
    catalog.create_table('db.t', Schema.from_pyarrow_schema(
        pa.schema([('id', pa.int64()), ('value', pa.int64())]),
        primary_keys=['id'] if dynamic else [], options=table_options), False)
    return catalog.get_table('db.t')


def _append(table):
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.table({'id': [1, 2], 'value': [10, 20]}))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()


def _multiple_paths(tmp_path):
    return ', '.join((tmp_path / name).as_uri() for name in ('index-a', 'index-b')) + ','


@contextmanager
def _reject_before_write(table, tmp_path):
    paths_before = set(tmp_path.rglob('*'))
    snapshot_id = table.snapshot_manager().get_latest_snapshot().id
    with patch.object(table.file_io, 'new_output_stream', wraps=table.file_io.new_output_stream) as output:
        with pytest.raises(ValueError, match=_ERROR):
            yield
        output.assert_not_called()
    assert set(tmp_path.rglob('*')) == paths_before
    assert table.snapshot_manager().get_latest_snapshot().id == snapshot_id


@pytest.mark.parametrize('paths', ['', 'single', 'multiple'])
@pytest.mark.parametrize('native', ['false', 'true'])
@pytest.mark.parametrize('index_in_data_dir', ['false', 'true'])
@pytest.mark.parametrize('operation', ['row-id', 'predicate', 'stream', 'direct', 'captured', 'python'])
def test_delete_rejects_external_paths_option_before_files(tmp_path, paths, native, index_in_data_dir, operation):
    table = _table(tmp_path)
    _append(table)
    value = _multiple_paths(tmp_path) if paths == 'multiple' else (tmp_path / 'index-a').as_uri() if paths else ''
    table = table.copy({
        _OPTION: value, 'write.native.enabled': native, 'index-file-in-data-file-dir': index_in_data_dir,
    })
    with patch('pypaimon.write.backend_dispatch.try_prepare_native') as prepare, _reject_before_write(table, tmp_path):
        if operation == 'direct':
            TableDeleteByRowId(table).delete([0])
        elif operation == 'captured':
            from pypaimon.write.row_id_plan import capture_row_id_plan
            TableDeleteByRowId(table, capture_row_id_plan(table)).delete([0])
        elif operation == 'python':
            TableDeleteByRowId._from_python(table).delete([0])
        elif operation == 'stream':
            table.new_stream_write_builder().new_update().delete_by_row_id([0], 2)
        else:
            update = table.new_batch_write_builder().new_update()
            if operation == 'predicate':
                update.delete_by_predicate(update.new_predicate_builder().equal('id', 1))
            else:
                update.delete_by_row_id([0])
    prepare.assert_not_called()


@pytest.mark.parametrize('paths', ['', 'single', 'multiple'])
@pytest.mark.parametrize('native', ['false', 'true'])
@pytest.mark.parametrize('index_in_data_dir', ['false', 'true'])
@pytest.mark.parametrize('operation', ['batch', 'stream', 'overwrite', 'direct-batch', 'direct-stream', 'python'])
def test_dynamic_bucket_rejects_external_paths_option_before_data_files(
        tmp_path, paths, native, index_in_data_dir, operation):
    table = _table(tmp_path, dynamic=True)
    _append(table)
    value = _multiple_paths(tmp_path) if paths == 'multiple' else (tmp_path / 'index-a').as_uri() if paths else ''
    table = table.copy({
        _OPTION: value, 'write.native.enabled': native, 'index-file-in-data-file-dir': index_in_data_dir,
    })
    with patch('pypaimon.write.backend_dispatch.try_prepare_native') as prepare, _reject_before_write(table, tmp_path):
        if operation == 'python':
            from pypaimon.write.backend_dispatch import create_python_write
            create_python_write(table, 'writer')
        elif operation in ('direct-batch', 'direct-stream'):
            from pypaimon.write.table_write import BatchTableWrite, StreamTableWrite
            writer_type = BatchTableWrite if operation == 'direct-batch' else StreamTableWrite
            writer_type(table, 'writer')
        else:
            builder = (table.new_stream_write_builder() if operation == 'stream'
                       else table.new_batch_write_builder())
            if operation == 'overwrite':
                builder.overwrite({})
            builder.new_write()
    prepare.assert_not_called()


@pytest.mark.parametrize('native', ['false', 'true'])
def test_merge_delete_rejects_external_paths_option_before_update_or_insert(tmp_path, native):
    table = _table(tmp_path)
    _append(table)
    table = table.copy({_OPTION: _multiple_paths(tmp_path), 'write.native.enabled': native})
    with patch('pypaimon.write.backend_dispatch.prepare_native_merge_into') as prepare, \
            _reject_before_write(table, tmp_path):
        table.new_batch_write_builder().new_update().merge_into(
            pa.table({'id': [1, 2, 3], 'value': [11, 22, 33]}), on=['id'],
            when_matched=[WhenMatched.delete('s.id = 1'), WhenMatched.update('*')],
            when_not_matched=[WhenNotMatched('*')])
    prepare.assert_not_called()


def test_ray_merge_delete_rejects_external_paths_option_before_source_execution(tmp_path):
    from pypaimon.ray.data_evolution_merge_into import merge_into

    table = _table(tmp_path, options={_OPTION: _multiple_paths(tmp_path)})
    _append(table)
    # Exercise coordinator validation without starting optional Ray workers.
    with patch('pypaimon.ray.data_evolution_merge_into._require_ray_join'), _reject_before_write(table, tmp_path):
        merge_into(
            target='db.t', source=object(),
            catalog_options={'warehouse': str(tmp_path / 'warehouse')}, on=['id'],
            when_matched=[WhenMatched.delete()], when_not_matched=[WhenNotMatched('*')])


@pytest.mark.parametrize('paths', ['', 'single', 'multiple'])
@pytest.mark.parametrize('index_in_data_dir', ['false', 'true'])
def test_shared_allocator_rejects_external_paths_option(tmp_path, paths, index_in_data_dir):
    value = _multiple_paths(tmp_path) if paths == 'multiple' else (tmp_path / 'index-a').as_uri() if paths else ''
    table = _table(tmp_path, options={_OPTION: value, 'index-file-in-data-file-dir': index_in_data_dir})
    _append(table)
    factory = table.path_factory()
    with _reject_before_write(table, tmp_path):
        factory.new_bucket_index_path((), 0, 'index-test')
    with _reject_before_write(table, tmp_path):
        factory.global_index_path_factory().new_path()


@pytest.mark.parametrize('dynamic', [False, True])
def test_legacy_comma_uri_supports_bucket_index_writes_and_reads(tmp_path, dynamic):
    root = tmp_path.as_uri() + '/index,archive'
    table = _table(tmp_path, dynamic=dynamic, options={'global-index.external-path': root})
    _append(table)
    if not dynamic:
        builder = table.new_batch_write_builder()
        commit = builder.new_commit()
        try:
            commit.commit(builder.new_update().delete_by_row_id([0]))
        finally:
            commit.close()
    entries = IndexFileHandler(table).scan(table.snapshot_manager().get_latest_snapshot())
    assert entries
    for entry in entries:
        path = entry.index_file.external_path
        assert path.startswith(root + '/')
        assert table.file_io.exists(path)
    assert (tmp_path / 'index,archive').is_dir()
    assert not (tmp_path / 'index').exists()
    reader = table.new_read_builder()
    ids = reader.new_read().to_arrow(reader.new_scan().plan().splits())['id'].to_pylist()
    assert sorted(ids) == ([1, 2] if dynamic else [2])


@pytest.mark.parametrize('dynamic', [False, True])
def test_bucket_index_in_data_directory_uses_data_roots(tmp_path, dynamic):
    data_roots = [(tmp_path / name).as_uri() for name in ('data-a', 'data-b')]
    table = _table(tmp_path, dynamic=dynamic, options={
        'index-file-in-data-file-dir': 'true',
        'data-file.external-paths': ','.join(data_roots),
        'data-file.external-paths.strategy': 'round-robin',
    })
    _append(table)
    if not dynamic:
        builder = table.new_batch_write_builder()
        messages = builder.new_update().delete_by_row_id([0])
        commit = builder.new_commit()
        try:
            commit.commit(messages)
        finally:
            commit.close()
    entries = IndexFileHandler(table).scan(table.snapshot_manager().get_latest_snapshot())
    assert entries
    for entry in entries:
        path = entry.index_file.external_path
        assert path and table.file_io.exists(path)
        # Bucket external paths use Java's normalized file:/ URI spelling.
        assert any(path.startswith(root.replace('file:///', 'file:/') + '/') for root in data_roots)
    assert not (tmp_path / 'index-a').exists()
    assert not (tmp_path / 'index-b').exists()


def test_external_paths_option_allows_append_and_column_update(tmp_path):
    table = _table(tmp_path, options={_OPTION: _multiple_paths(tmp_path)})
    _append(table)
    builder = table.new_batch_write_builder()
    updater = builder.new_update().new_update_by_row_id()
    commit = builder.new_commit()
    try:
        commit.commit(updater.update_columns(pa.table({'_ROW_ID': [0], 'value': [11]}), ['value']))
    finally:
        commit.close()
    reader = table.new_read_builder()
    rows = reader.new_read().to_arrow(reader.new_scan().plan().splits()).sort_by('id').to_pylist()
    assert rows == [{'id': 1, 'value': 11}, {'id': 2, 'value': 20}]
    assert not IndexFileHandler(table).scan(table.snapshot_manager().get_latest_snapshot())


def test_external_paths_option_does_not_block_reading_or_aborting_existing_dv(tmp_path):
    table = _table(tmp_path, options={'global-index.external-path': (tmp_path / 'index-a').as_uri()})
    _append(table)
    builder = table.new_batch_write_builder()
    commit = builder.new_commit()
    try:
        commit.commit(builder.new_update().delete_by_row_id([0]))
    finally:
        commit.close()
    pending = builder.new_update().delete_by_row_id([1])
    paths = [entry.index_file.external_path for message in pending for entry in message.index_adds]
    assert paths and all(table.file_io.exists(path) for path in paths)
    configured = table.copy({_OPTION: _multiple_paths(tmp_path)})
    reader = configured.new_read_builder()
    assert reader.new_read().to_arrow(reader.new_scan().plan().splits())['id'].to_pylist() == [2]
    commit = configured.new_batch_write_builder().new_commit()
    try:
        commit.abort(pending)
    finally:
        commit.close()
    assert all(not table.file_io.exists(path) for path in paths)
    assert configured.snapshot_manager().get_latest_snapshot().id == 2
