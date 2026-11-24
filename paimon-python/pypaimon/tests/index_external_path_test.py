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
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.index.index_file_handler import IndexFileHandler


pytestmark = pytest.mark.execution_defaults
_OPTION = 'global-index.external-paths'


def _table(tmp_path, *, dynamic=False, partitioned=False, options=None):
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
    fields = [('id', pa.int64()), ('value', pa.int64())]
    if partitioned:
        fields.append(('p', pa.string()))
    partition_keys = ['p'] if partitioned else []
    catalog.create_table('db.t', Schema.from_pyarrow_schema(
        pa.schema(fields), partition_keys=partition_keys,
        primary_keys=partition_keys + ['id'] if dynamic else [], options=table_options), False)
    return catalog.get_table('db.t')


def _append(table):
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        data = {'id': [1, 2], 'value': [10, 20]}
        if table.partition_keys:
            data['p'] = ['one', 'two']
        writer.write_arrow(pa.table(data))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()


def _multiple_paths(tmp_path):
    return ', '.join((tmp_path / name).as_uri() for name in ('index-a', 'index-b')) + ','


def test_global_index_external_paths(tmp_path):
    legacy = tmp_path.as_uri() + '/index,archive'
    roots = [(tmp_path / name).as_uri() for name in ('index-a', 'index-b')]
    options = Options({'global-index.external-path': legacy})
    core = CoreOptions(options)
    assert core.global_index_external_paths() == [legacy]
    options.set(CoreOptions.GLOBAL_INDEX_EXTERNAL_PATHS, _multiple_paths(tmp_path))
    assert core.global_index_external_paths() == roots
    options.set(CoreOptions.GLOBAL_INDEX_EXTERNAL_PATHS, '')
    assert core.global_index_external_paths() == []


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
@pytest.mark.parametrize('use_path_list', [False, True])
def test_python_external_bucket_index_writes_and_reads(tmp_path, dynamic, use_path_list):
    legacy = tmp_path.as_uri() + '/index,archive'
    options = {'global-index.external-path': legacy}
    roots = {legacy}
    if use_path_list:
        options[_OPTION] = _multiple_paths(tmp_path)
        roots = {(tmp_path / name).as_uri() for name in ('index-a', 'index-b')}
    table = _table(tmp_path, dynamic=dynamic, partitioned=use_path_list, options=options)
    _append(table)
    if not dynamic:
        builder = table.new_batch_write_builder()
        commit = builder.new_commit()
        try:
            rows = [0, 1] if use_path_list else [0]
            commit.commit(builder.new_update().delete_by_row_id(rows))
        finally:
            commit.close()
    entries = IndexFileHandler(table).scan(table.snapshot_manager().get_latest_snapshot())
    assert len(entries) == len(roots)
    paths = [entry.index_file.external_path for entry in entries]
    assert {path.rsplit('/', 1)[0] for path in paths} == roots
    for path in paths:
        assert table.file_io.exists(path)
    assert not (tmp_path / 'index').exists()
    reader = table.new_read_builder()
    ids = reader.new_read().to_arrow(reader.new_scan().plan().splits())['id'].to_pylist()
    assert sorted(ids) == ([1, 2] if dynamic else [] if use_path_list else [2])


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
