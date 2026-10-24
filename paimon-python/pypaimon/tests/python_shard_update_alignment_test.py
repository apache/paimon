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

"""Python shard updates follow the existing Rust row and lifecycle contract."""

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.schema.data_types import AtomicType, DataField, PyarrowFieldParser

pytestmark = [pytest.mark.python_plan, pytest.mark.python_read,
              pytest.mark.python_write, pytest.mark.python_commit]


@pytest.fixture
def python_shard_table(tmp_path):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('default', True)
    schema = pa.schema([('id', pa.int32()), ('value', pa.int32())])
    catalog.create_table('default.shards', Schema.from_pyarrow_schema(schema, options={
        'bucket': '-1', 'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'deletion-vectors.enabled': 'true', 'write.native.enabled': 'false',
        'commit.native.enabled': 'false', 'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
    }), False)
    return catalog.get_table('default.shards')


def _append(table, rows):
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pylist(rows, schema=PyarrowFieldParser.from_paimon_schema(table.fields)))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()


def _commit(table, messages, stream=False):
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    commit = builder.new_commit()
    try:
        commit.commit(messages, 37) if stream else commit.commit(messages)
    finally:
        commit.close()


def _updater(table, stream=False, projection=('id',), columns=('value',), shard=0, count=1):
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    update = builder.new_update()
    update.with_read_projection(None if projection is None else projection)
    if columns is not None:
        update.with_update_type(list(columns))
    return update.new_shard_updator(shard, count)


def _values(values):
    return pa.record_batch([pa.array(values, type=pa.int32())], names=['value'])


def _rows(table):
    read = table.new_read_builder()
    return read.new_read().to_arrow(read.new_scan().plan().splits()).sort_by('id').to_pylist()


@pytest.mark.parametrize('stream', [False, True])
def test_deleted_rows_do_not_consume_shard_update_values(python_shard_table, stream):
    table = python_shard_table
    _append(table, [dict(id=i, value=i * 10) for i in range(1, 5)])
    builder = table.new_batch_write_builder()
    _commit(table, builder.new_update().delete_by_row_id([1, 3]))
    shard = _updater(table, stream, projection=['id', '_ROW_ID'])
    with shard.arrow_reader() as reader:
        visible = pa.Table.from_batches(list(reader))
    assert visible.to_pydict() == dict(id=[1, 3], _ROW_ID=[0, 2])
    shard.update_by_arrow_batch(_values([111]))
    shard.update_by_arrow_batch(_values([333]))
    messages = shard.prepare_commit()
    assert len(messages) == 1
    assert all(file.row_count == 4 and file.first_row_id == 0 for message in messages for file in message.new_files)
    shard.close()
    shard.close()
    # Closing after preparation must preserve the files the caller may publish.
    _commit(table, messages, stream)
    assert _rows(table) == [dict(id=1, value=111), dict(id=3, value=333)]
    if stream:
        assert table.snapshot_manager().get_latest_snapshot().commit_identifier == 37


@pytest.mark.parametrize('bad', ['column', 'type', 'excess'])
def test_invalid_input_and_early_prepare_do_not_consume_pending_rows(python_shard_table, bad):
    table = python_shard_table
    _append(table, [dict(id=i, value=i * 10) for i in range(1, 4)])
    shard = _updater(table)
    with shard.arrow_reader() as reader:
        assert sum(batch.num_rows for batch in reader) == 3
    data = (pa.record_batch([pa.array([1])], names=['missing']) if bad == 'column' else
            pa.record_batch([pa.array(['invalid'])], names=['value']) if bad == 'type' else _values([1, 2, 3, 4]))
    with pytest.raises((ValueError, TypeError)):
        shard.update_by_arrow_batch(data)
    shard.update_by_arrow_batch(_values([100]))
    with pytest.raises(ValueError, match='same number of rows'):
        shard.prepare_commit()
    shard.update_by_arrow_batch(_values([200, 300]))
    messages = shard.prepare_commit()
    repeated = shard.prepare_commit()
    assert [file.file_name for msg in messages for file in msg.new_files] == [
        file.file_name for msg in repeated for file in msg.new_files]
    with pytest.raises(ValueError, match='already prepared'):
        shard.update_by_arrow_batch(_values([9]))
    shard.close()
    _commit(table, messages)
    assert _rows(table) == [dict(id=i, value=i * 100) for i in range(1, 4)]


def test_reader_must_finish_and_cannot_be_opened_twice(python_shard_table):
    table = python_shard_table
    for i in range(1, 3):
        _append(table, [dict(id=i, value=i * 10)])
    shard = _updater(table)
    with pytest.raises(ValueError, match='more rows than have been read'):
        shard.update_by_arrow_batch(_values([99]))
    reader = shard.arrow_reader()
    batch = reader.read_next_batch()
    shard.update_by_arrow_batch(_values([99] * batch.num_rows))
    reader.close()
    with pytest.raises(ValueError, match='same number of rows'):
        shard.prepare_commit()
    with pytest.raises(ValueError, match='only be opened once'):
        shard.arrow_reader()
    shard.close()
    assert _rows(table) == [dict(id=1, value=10), dict(id=2, value=20)]


@pytest.mark.parametrize('start_reader', [False, True])
def test_close_cancels_an_unprepared_shard(python_shard_table, start_reader):
    table = python_shard_table
    _append(table, [dict(id=1, value=10)])
    shard = _updater(table)
    if start_reader:
        shard.arrow_reader().read_next_batch()
    shard.close()
    shard.close()
    for operation in (shard.arrow_reader, shard.prepare_commit, lambda: shard.update_by_arrow_batch(_values([99]))):
        with pytest.raises(ValueError, match='closed'):
            operation()
    assert _rows(table) == [dict(id=1, value=10)]


@pytest.mark.parametrize('close_failure', [False, True])
def test_read_failure_poisoning_prevents_a_partial_commit(python_shard_table, monkeypatch, close_failure):
    from pypaimon.read.table_read import TableRead

    table = python_shard_table
    _append(table, [dict(id=1, value=10)])
    shard = _updater(table)

    class FailedSource:
        closed = False

        def __iter__(self):
            return self

        def __next__(self):
            raise RuntimeError('failed to read shard')

        def close(self):
            self.closed = True
            if close_failure:
                raise OSError('failed to close shard source')

    source = FailedSource()
    monkeypatch.setattr(TableRead, '_to_managed_arrow_batch_reader', lambda *args, **kwargs: source)
    reader = shard.arrow_reader()
    with pytest.raises(RuntimeError, match='failed to read shard'):
        reader.read_next_batch()
    assert source.closed
    with pytest.raises(ValueError, match='failed'):
        shard.prepare_commit()
    shard.close()


def test_reader_close_releases_source_before_reading_any_batch(python_shard_table, monkeypatch):
    from pypaimon.read.table_read import TableRead

    table = python_shard_table
    _append(table, [dict(id=1, value=10)])
    shard = _updater(table)

    class UnstartedSource:
        close_count = 0

        def close(self):
            self.close_count += 1
            assert self.close_count == 1, 'source was closed twice'

    source = UnstartedSource()
    monkeypatch.setattr(TableRead, '_to_managed_arrow_batch_reader', lambda *args, **kwargs: source)
    reader = shard.arrow_reader()
    reader.close()
    assert source.close_count == 1
    with pytest.raises(ValueError, match='same number of rows'):
        shard.prepare_commit()
    reader.close()
    shard.close()
    shard.close()
    assert source.close_count == 1


@pytest.mark.parametrize('all_deleted', [False, True])
def test_empty_and_fully_deleted_shards_have_no_updates(python_shard_table, all_deleted):
    table = python_shard_table
    _append(table, [dict(id=1, value=10)])
    if all_deleted:
        _commit(table, table.new_batch_write_builder().new_update().delete_by_row_id([0]))
    shard = _updater(table, shard=0 if all_deleted else 1, count=1 if all_deleted else 2)
    with shard.arrow_reader() as reader:
        assert list(reader) == []
    assert shard.prepare_commit() == []
    shard.close()


@pytest.mark.parametrize('projection, expected', [
    (None, ['id', 'value']), ([], []), ({'source': 'id'}, ['source']),
])
def test_projection_preserves_rows_without_exposing_internal_row_ids(python_shard_table, projection, expected):
    table = python_shard_table
    _append(table, [dict(id=i, value=i * 10) for i in range(1, 4)])
    shard = _updater(table, projection=projection)
    with shard.arrow_reader() as reader:
        count = 0
        for batch in reader:
            assert batch.schema.names == expected
            count += batch.num_rows
            shard.update_by_arrow_batch(_values([99] * batch.num_rows))
    assert count == 3
    _commit(table, shard.prepare_commit())
    shard.close()
    assert [row['value'] for row in _rows(table)] == [99, 99, 99]


def test_captured_shard_preserves_file_groups_and_ignores_new_rows(python_shard_table):
    table = python_shard_table
    for i in range(1, 3):
        _append(table, [dict(id=i, value=i * 10)])
    shard = _updater(table.copy({'target-file-row-num': '1'}))
    _append(table, [dict(id=3, value=30)])
    with shard.arrow_reader() as reader:
        assert pa.Table.from_batches(list(reader)).to_pydict() == {'id': [1, 2]}
    shard.update_by_arrow_batch(_values([100, 200]))
    messages = shard.prepare_commit()
    assert len(messages) == 2
    assert all(message.check_from_snapshot == 2 for message in messages)
    _commit(table, messages)
    shard.close()
    assert _rows(table) == [dict(id=1, value=100), dict(id=2, value=200), dict(id=3, value=30)]


def test_map_projection_keeps_internal_row_ids_separate(tmp_path):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('default', True)
    schema = pa.schema([
        ('id', pa.int32()), ('value', pa.int32()),
        ('payload', pa.struct([('a', pa.int32()), ('b', pa.int32())])),
        ('attributes', pa.map_(pa.string(), pa.int32())),
    ])
    catalog.create_table('default.nested', Schema.from_pyarrow_schema(schema, options={
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'write.native.enabled': 'false', 'commit.native.enabled': 'false',
        'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
    }), False)
    table = catalog.get_table('default.nested')
    original = [
        dict(id=1, value=10, payload=dict(a=100, b=3), attributes=[('key', 5), ('other', 9)]),
        dict(id=2, value=20, payload=None, attributes=None),
        dict(id=3, value=30, payload=dict(a=200, b=4), attributes=[]),
    ]
    _append(table, original)
    shard = _updater(table, projection=["attributes['key']"])
    with shard.arrow_reader() as reader:
        data = pa.Table.from_batches(list(reader), schema=reader.schema)
    assert data.to_pydict() == {'attributes_key': [5, None, None]}
    shard.update_by_arrow_batch(pa.record_batch([data.column(0).combine_chunks()], names=['value']))
    _commit(table, shard.prepare_commit())
    shard.close()
    expected_rows = [dict(row, value=value) for row, value in zip(original, data.column(0).to_pylist())]
    assert _rows(table) == expected_rows


def test_dedicated_blob_updates_keep_deleted_physical_positions(tmp_path):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('default', True)
    catalog.create_table('default.blobs', Schema([
        DataField(0, 'id', AtomicType('INT')), DataField(1, 'payload', AtomicType('BLOB')),
    ], options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
                'deletion-vectors.enabled': 'true', 'write.native.enabled': 'false',
                'commit.native.enabled': 'false', 'read.native.enabled': 'false',
                'scan.native-plan.enabled': 'false', 'blob.target-file-size': '8 B'}), False)
    table = catalog.get_table('default.blobs')
    _append(table, [dict(id=i, payload=b'old-' + bytes([i])) for i in range(3)])
    _commit(table, table.new_batch_write_builder().new_update().delete_by_row_id([1]))
    shard = _updater(table, columns=('payload',))
    with shard.arrow_reader() as reader:
        assert pa.Table.from_batches(list(reader)).to_pydict() == {'id': [0, 2]}
    shard.update_by_arrow_batch(pa.record_batch([
        pa.array([b'first', b'last'], type=pa.large_binary())], names=['payload']))
    messages = shard.prepare_commit()
    assert any(file.file_name.endswith('.blob') for message in messages for file in message.new_files)
    shard.close()
    _commit(table, messages)
    assert _rows(table) == [dict(id=0, payload=b'first'), dict(id=2, payload=b'last')]
