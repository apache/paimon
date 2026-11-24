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

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.index.index_file_handler import IndexFileHandler


pytestmark = pytest.mark.execution_defaults
_OPTION = 'global-index.external-paths'


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


@pytest.mark.parametrize('native', ['false', 'true'])
def test_partition_delete_with_external_index_paths(tmp_path, native):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('db', True)
    catalog.create_table('db.t', Schema.from_pyarrow_schema(
        pa.schema([('p', pa.string()), ('id', pa.int64())]),
        partition_keys=['p'], options={
            'bucket': '-1', 'file.format': 'parquet', 'write.native.enabled': 'false',
            'commit.native.enabled': 'false', _OPTION: _multiple_paths(tmp_path),
        }), False)
    table = catalog.get_table('db.t')
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.table({'p': ['drop', 'keep'], 'id': [1, 2]}))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()

    table = table.copy({'write.native.enabled': native})
    builder = table.new_batch_write_builder()
    update = builder.new_update()
    messages = update.delete_by_predicate(update.new_predicate_builder().equal('p', 'drop'))
    assert sum(len(message.deleted_files) for message in messages) == 1
    assert all(not message.index_adds for message in messages)
    commit = builder.new_commit()
    try:
        commit.commit(messages)
    finally:
        commit.close()

    reader = table.new_read_builder()
    assert reader.new_read().to_arrow(reader.new_scan().plan().splits()).to_pylist() == [
        {'p': 'keep', 'id': 2},
    ]


@pytest.mark.parametrize('dynamic', [False, True])
def test_legacy_comma_uri_supports_bucket_index_writes_and_reads(tmp_path, dynamic):
    root = tmp_path.as_uri() + '/index,archive'
    table = _table(tmp_path, dynamic=dynamic, options={
        'global-index.external-path': root, _OPTION: _multiple_paths(tmp_path),
    })
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
