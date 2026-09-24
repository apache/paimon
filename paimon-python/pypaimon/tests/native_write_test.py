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

"""End-to-end coverage of the optional native data writer bridge."""

from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.write.native_write import NativeTableWrite, native_write_available


requires_native = pytest.mark.skipif(
    not native_write_available(), reason='pypaimon-rust writer required')


def _table(tmp_path, primary_key=False, commit_native=True):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    options = {'file.format': 'parquet', 'write.native.enabled': 'true',
               'commit.native.enabled': str(commit_native).lower()}
    if primary_key:
        options['bucket'] = '1'
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        pa.schema([('id', pa.int64()), ('pt', pa.string())]),
        options=options, primary_keys=['id'] if primary_key else [],
        partition_keys=[] if primary_key else ['pt']), False)
    return catalog.get_table('default.t')


def _batch(ids, partitions):
    return pa.record_batch(
        [pa.array(ids, pa.int64()), pa.array(partitions, pa.string())],
        names=['id', 'pt'])


def _rows(table):
    builder = table.new_read_builder()
    return sorted(builder.new_read().to_arrow(
        builder.new_scan().plan().splits()).to_pylist(), key=lambda row: row['id'])


@pytest.mark.python_write
def test_native_write_is_opt_in():
    assert not CoreOptions(Options({})).native_write_enabled()
    assert CoreOptions(Options({'write.native.enabled': 'true'})).native_write_enabled()


@requires_native
@pytest.mark.parametrize('primary_key', [False, True])
@pytest.mark.parametrize('commit_native', [False, True])
def test_batch_native_write_commits_through_both_committers(
        tmp_path, primary_key, commit_native):
    table = _table(tmp_path, primary_key, commit_native)
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    assert isinstance(writer, NativeTableWrite)
    try:
        writer.write_arrow_batch(_batch([1], ['a']))
        writer.write_arrow(pa.Table.from_batches([_batch([2], ['b'])]))
        messages = writer.prepare_commit()
        assert messages and sum(file.row_count for msg in messages
                                for file in msg.new_files) == 2
        commit = builder.new_commit()
        try:
            commit.commit(messages)
        finally:
            commit.close()
    finally:
        writer.close()
    assert _rows(table) == [{'id': 1, 'pt': 'a'}, {'id': 2, 'pt': 'b'}]
    assert table.snapshot_manager().get_latest_snapshot().commit_user == builder.commit_user


@requires_native
def test_stream_native_write_reuses_writer_across_checkpoints(tmp_path):
    table = _table(tmp_path)
    builder = table.new_stream_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    assert isinstance(writer, NativeTableWrite)
    try:
        for identifier in (7, 8):
            writer.write_arrow_batch(_batch([identifier], ['a']))
            commit.commit(writer.prepare_commit(identifier), identifier)
        assert _rows(table) == [{'id': 7, 'pt': 'a'}, {'id': 8, 'pt': 'a'}]
        snapshot = table.snapshot_manager().get_latest_snapshot()
        assert snapshot.commit_user == builder.commit_user
        assert snapshot.commit_identifier == 8
    finally:
        writer.close()
        commit.close()


@requires_native
def test_native_overwrite_and_empty_overwrite(tmp_path):
    table = _table(tmp_path)
    seed = table.new_batch_write_builder()
    seed_writer = seed.new_write()
    seed_writer.write_arrow_batch(_batch([1, 2], ['a', 'b']))
    seed.new_commit().commit(seed_writer.prepare_commit())
    seed_writer.close()

    overwrite = table.new_batch_write_builder().overwrite({'pt': 'a'})
    writer = overwrite.new_write()
    assert isinstance(writer, NativeTableWrite)
    writer.write_arrow_batch(_batch([3], ['a']))
    overwrite.new_commit().commit(writer.prepare_commit())
    writer.close()
    assert _rows(table) == [{'id': 2, 'pt': 'b'}, {'id': 3, 'pt': 'a'}]

    static_table = table.copy({'dynamic-partition-overwrite': 'false'})
    empty = static_table.new_batch_write_builder().overwrite({'pt': 'a'})
    empty_writer = empty.new_write()
    assert isinstance(empty_writer, NativeTableWrite)
    empty.new_commit().commit(empty_writer.prepare_commit())
    empty_writer.close()
    assert _rows(table) == [{'id': 2, 'pt': 'b'}]


@requires_native
def test_advanced_api_switches_before_write_and_rejects_late_switch(tmp_path):
    table = _table(tmp_path)
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    assert isinstance(writer, NativeTableWrite)
    assert writer.with_write_type(['id', 'pt']) is writer._python_writer
    assert writer._native_writer is None
    writer.write_arrow_batch(_batch([1], ['a']))
    builder.new_commit().commit(writer.prepare_commit())
    writer.close()
    assert _rows(table) == [{'id': 1, 'pt': 'a'}]

    native = table.new_batch_write_builder().new_write()
    native.write_arrow_batch(_batch([2], ['a']))
    with pytest.raises(RuntimeError, match='after native data'):
        native.with_write_type(['id'])
    native.abort()


@requires_native
def test_unavailable_native_writer_falls_back_before_table_reconstruction(tmp_path):
    table = _table(tmp_path)
    with patch('pypaimon.write.native_write.native_write_available', return_value=False), \
            patch('pypaimon.write.native_write.create_native_write_table',
                  side_effect=AssertionError('must not reconstruct')):
        writer = table.new_batch_write_builder().new_write()
    assert not isinstance(writer, NativeTableWrite)
    writer.close()
