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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Dynamic bucket writer and HASH index interoperability with Rust core."""

from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.index.index_file_handler import IndexFileHandler
from pypaimon.write.native_commit import from_native_commit_messages
from pypaimon.write.native_write import NativeTableWrite

pytestmark = pytest.mark.native_plan
SCHEMA = pa.schema([('id', pa.int64()), ('p', pa.string()), ('v', pa.string())])


def _table(tmp_path, options=None, catalog=None):
    catalog = catalog or CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    opts = {'bucket': '-1', 'file.format': 'parquet',
            'dynamic-bucket.target-row-num': '2', 'write.native.enabled': 'true'}
    opts.update(options or {})
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        SCHEMA, primary_keys=['id', 'p'], partition_keys=['p'], options=opts), False)
    return catalog.get_table('default.t')


def _batch(ids, p='a', value='v'):
    return pa.RecordBatch.from_pylist(
        [{'id': i, 'p': p, 'v': value} for i in ids], schema=SCHEMA)


def _indexes(table):
    return IndexFileHandler(table).scan(table.snapshot_manager().get_latest_snapshot())


def _write(table, batch, native=True):
    table = table.copy({'write.native.enabled': str(native).lower()})
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    assert isinstance(writer, NativeTableWrite) == native
    try:
        writer.write_arrow_batch(batch)
        if native:
            assert writer._python_writer is None
        messages = writer.prepare_commit()
        commit.commit(messages)
        return messages
    finally:
        writer.close()
        commit.close()


def _rows(table, native):
    builder = table.copy({'scan.native-plan.enabled': str(native).lower(),
                          'read.native.enabled': str(native).lower()}).new_read_builder()
    splits = builder.new_scan().plan().splits()
    reader = builder.new_read()
    if native:
        with patch.object(reader, '_create_split_read', side_effect=AssertionError('fallback')):
            rows = reader.to_arrow(splits).to_pylist()
    else:
        rows = reader.to_arrow(splits).to_pylist()
    return sorted(rows, key=lambda row: (row['p'], row['id']))


def test_python_commit_accepts_java_hash_replacement(tmp_path):
    from pypaimon_rust.datafusion import PaimonCatalog

    table = _table(tmp_path, {'dynamic-bucket.target-row-num': '100'})
    for ids in ([1], [2]):
        native = PaimonCatalog({'warehouse': str(tmp_path)}).get_table('default.t')
        builder = native.new_batch_write_builder()
        writer = builder.new_write()
        writer.write_arrow(_batch(ids))
        messages = from_native_commit_messages(table, writer.prepare_commit())
        assert len(messages) == 1
        assert len(messages[0].index_adds) == 1
        assert messages[0].index_deletes == []
        table.new_batch_write_builder().new_commit().commit(messages)
        writer.close()
    assert len(_indexes(table)) == 1
    assert _indexes(table)[0].index_file.row_count == 2
    for native in (False, True):
        assert _rows(table, native) == _batch([1, 2]).to_pylist()


@pytest.mark.parametrize('bucket_local', [False, True])
@pytest.mark.parametrize('first_native', [False, True])
def test_dynamic_cross_backend_restart(tmp_path, bucket_local, first_native):
    table = _table(tmp_path, {
        'index-file-in-data-file-dir': str(bucket_local).lower(),
        'data-file.path-directory': 'data',
        'dynamic-bucket.max-buckets': '1',
    })
    for native, ids, value in [(first_native, [1, 2, 3], 'old'),
                               (not first_native, [2, 4], 'new'),
                               (first_native, [1, 5], 'last')]:
        messages = _write(table, _batch(ids, 'a/b', value), native)
        assert {m.bucket for m in messages} == {0}
        indexes = _indexes(table)
        assert len(indexes) == 1
        path = table.path_factory().bucket_index_path(
            tuple(indexes[0].partition.values), 0, indexes[0].index_file, table.file_io)
        assert table.file_io.exists(path)
    assert _indexes(table)[0].index_file.row_count == 5
    expected = [{'id': i, 'p': 'a/b', 'v': v} for i, v in
                [(1, 'last'), (2, 'new'), (3, 'old'), (4, 'new'), (5, 'last')]]
    for native in (False, True):
        assert _rows(table, native) == expected


@pytest.mark.parametrize('native_commit', [False, True])
def test_stream_dynamic_index_checkpoints(tmp_path, native_rest_catalog, native_commit):
    table = _table(tmp_path, {'commit.native.enabled': str(native_commit).lower(),
                              'dynamic-bucket.max-buckets': '1'}, native_rest_catalog)
    builder = table.new_stream_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    assert isinstance(writer, NativeTableWrite)
    try:
        for checkpoint, ids in enumerate(([1, 2], [2], [3]), 1):
            writer.write_arrow_batch(_batch(ids, value=str(checkpoint)))
            messages = writer.prepare_commit(checkpoint)
            adds = [entry for m in messages for entry in m.index_adds]
            assert len(adds) == (0 if checkpoint == 2 else 1)
            if native_commit:
                with patch.object(commit.file_store_commit, 'commit',
                                  side_effect=AssertionError('Python commit fallback')):
                    commit.commit(messages, checkpoint)
            else:
                commit.commit(messages, checkpoint)
        assert writer.prepare_commit(4) == []
        assert _indexes(table)[0].index_file.row_count == 3
    finally:
        writer.close()
        commit.close()
    for native in (False, True):
        assert _rows(table, native) == [
            {'id': 1, 'p': 'a', 'v': '1'}, {'id': 2, 'p': 'a', 'v': '2'},
            {'id': 3, 'p': 'a', 'v': '3'}]


@pytest.mark.parametrize('bucket_local', [False, True])
@pytest.mark.parametrize('commit_first', [False, True])
def test_dynamic_abort_index_ownership(tmp_path, bucket_local, commit_first):
    table = _table(tmp_path, {'index-file-in-data-file-dir': str(bucket_local).lower()})
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    assert isinstance(writer, NativeTableWrite)
    writer.write_arrow_batch(_batch([1]))
    messages = writer.prepare_commit()
    entry = messages[0].index_adds[0]
    path = table.path_factory().bucket_index_path(('a',), entry.bucket, entry.index_file)
    assert table.file_io.exists(path)
    if commit_first:
        builder.new_commit().commit(messages)
    writer.abort()
    assert table.file_io.exists(path) == commit_first
    assert all(table.file_io.exists(f.file_path) == commit_first
               for m in messages for f in m.new_files)


@pytest.mark.parametrize('native', [False, True])
def test_legacy_python_hash_location(tmp_path, native):
    # Old Python versions ignored index-file-in-data-file-dir and stored HASH
    # under table/index with no external_path. Opening that table must still work.
    table = _table(tmp_path, {'index-file-in-data-file-dir': 'false'})
    _write(table, _batch([1]), False)
    old = _indexes(table)[0]
    assert old.index_file.external_path is None
    table = table.copy({'index-file-in-data-file-dir': 'true'})
    _write(table, _batch([2]), native)
    assert len(_indexes(table)) == 1
    assert _indexes(table)[0].index_file.row_count == 2
    for native_read in (False, True):
        assert _rows(table, native_read) == _batch([1, 2]).to_pylist()


@pytest.mark.parametrize('missing_all', [False, True])
def test_native_rejects_incomplete_hash_index(tmp_path, missing_all):
    table = _table(tmp_path, {'dynamic-bucket.target-row-num': '1'})
    builder = table.copy({'write.native.enabled': 'false'}).new_batch_write_builder()
    writer = builder.new_write()
    writer.write_arrow_batch(_batch([1, 2]))
    messages = writer.prepare_commit()
    for message in messages:
        if missing_all or message.bucket == 1:
            message.index_adds.clear()
    builder.new_commit().commit(messages)
    writer.close()
    writer = table.new_batch_write_builder().new_write()
    assert isinstance(writer, NativeTableWrite)
    try:
        with pytest.raises(Exception, match='complete HASH index'):
            writer.write_arrow_batch(_batch([2], value='update'))
        with pytest.raises(Exception, match='cannot be reused'):
            writer.prepare_commit()
    finally:
        writer.close()
    assert table.snapshot_manager().get_latest_snapshot().id == 1


@pytest.mark.parametrize('native_commit', [False, True])
@pytest.mark.parametrize('dynamic', [False, True])
def test_dynamic_overwrite_resets_only_replaced_indexes(
        tmp_path, native_rest_catalog, native_commit, dynamic):
    table = _table(
        tmp_path, {'commit.native.enabled': str(native_commit).lower(),
                   'dynamic-partition-overwrite': str(dynamic).lower()}, native_rest_catalog)
    _write(table, _batch([1, 2], 'a'))
    _write(table, _batch([3], 'b'))
    previous_b = next(e.index_file.file_name for e in _indexes(table) if e.partition.values == ['b'])
    builder = table.new_batch_write_builder().overwrite({'p': 'a'})
    writer = builder.new_write()
    assert isinstance(writer, NativeTableWrite)
    writer.write_arrow_batch(_batch([4], 'a'))
    builder.new_commit().commit(writer.prepare_commit())
    writer.close()
    counts = sorted((tuple(e.partition.values), e.index_file.row_count) for e in _indexes(table))
    assert counts == [(('a',), 1), (('b',), 1)]
    assert next(e.index_file.file_name for e in _indexes(table) if e.partition.values == ['b']) == previous_b
    _write(table, _batch([4, 5], 'a', 'next'))
    for native in (False, True):
        assert _rows(table, native) == _batch([4, 5], 'a', 'next').to_pylist() + _batch([3], 'b').to_pylist()
    # Static empty overwrite removes the old partition's HASH as well as data.
    builder = table.copy({'dynamic-partition-overwrite': 'false'}).new_batch_write_builder().overwrite({'p': 'a'})
    writer = builder.new_write()
    builder.new_commit().commit(writer.prepare_commit())
    writer.close()
    assert [e.partition.values for e in _indexes(table)] == [['b']]


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('bucket_key', ['id', ''])
def test_dynamic_writer_rejects_custom_bucket_key(tmp_path, native, bucket_key):
    table = _table(tmp_path).copy({'write.native.enabled': str(native).lower(),
                                  'bucket-key': bucket_key})
    with pytest.raises(Exception, match="Cannot define 'bucket-key'"):
        table.new_batch_write_builder().new_write()


@pytest.mark.parametrize('bucket_local', [False, True])
def test_dynamic_external_indexes_cross_backends(tmp_path, bucket_local):
    external = tmp_path / 'external-index'
    options = {'index-file-in-data-file-dir': str(bucket_local).lower(),
               'data-file.path-directory': 'data',
               'dynamic-bucket.max-buckets': '1'}
    if bucket_local:
        options.update({'data-file.external-paths': external.as_uri(),
                        'data-file.external-paths.strategy': 'round-robin'})
    else:
        options['global-index.external-path'] = external.as_uri()
    table = _table(tmp_path, options)
    for native, ids in [(True, [1]), (False, [2]), (True, [3])]:
        _write(table, _batch(ids), native)
        indexes = _indexes(table)
        assert len(indexes) == 1
        path = indexes[0].index_file.external_path
        assert path is not None
        assert '/external-index/' in path
        assert ('/data/p=a/bucket-0/' in path) == bucket_local
        assert table.file_io.exists(path)
    for native in (False, True):
        assert _rows(table, native) == _batch([1, 2, 3]).to_pylist()
