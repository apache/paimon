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

"""End-to-end coverage of the optional native batch row-ID update bridge."""

from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.write.native_update import create_native_delete, create_native_update
from pypaimon.write.table_delete import TableDeleteByRowId
from pypaimon.read.table_read import TableRead
from pypaimon.write.table_update import BatchTableUpdate, StreamTableUpdate
from pypaimon.write.table_update_by_row_id import TableUpdateByRowId
from pypaimon.write.table_upsert_by_key import TableUpsertByKey


pytestmark = pytest.mark.native_plan


@pytest.mark.parametrize('stream', [False, True])
def test_incremental_row_id_updater_uses_core_and_accumulates_columns(tmp_path, stream):
    from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
    from pypaimon.table.row.generic_row import GenericRow
    from pypaimon.write.native_update import NativeTableUpdateByRowId

    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([('id', pa.int32()), ('name', pa.string()), ('age', pa.int32())])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, options={
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'write.native.enabled': 'true',
    }), False)
    table = catalog.get_table('default.t')
    seed = table.new_batch_write_builder()
    writer = seed.new_write()
    writer.write_arrow(pa.table({'id': [1, 2], 'name': ['a', 'b'], 'age': [10, 20]}, schema=schema))
    seed.new_commit().commit(writer.prepare_commit())
    writer.close()
    snapshot_id = table.snapshot_manager().get_latest_snapshot().id
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    commit_id = 27 if stream else BATCH_COMMIT_IDENTIFIER
    # Factory configuration must not fix columns for this incremental API.
    update = builder.new_update().with_update_type(['id'])
    with patch.object(TableUpdateByRowId, '_load_existing_files_info',
                      side_effect=AssertionError('Python loaded the file index')):
        updater = update.new_update_by_row_id(commit_id) if stream else update.new_update_by_row_id()
        assert isinstance(updater, NativeTableUpdateByRowId)
        assert updater.commit_user == builder.commit_user
        assert updater.commit_identifier == commit_id
        assert updater.commit_messages == []
        first = updater.update_columns(pa.table({'_ROW_ID': [0], 'age': [11]}), ['age'])
        messages = updater.update_row_columns(
            GenericRow(['B'], fields=[table.field_dict['name']]), [1], ['name'])
        assert len(first) == 1
        assert len(messages) == len(updater.commit_messages) == 2
        assert all(message.check_from_snapshot == snapshot_id for message in messages)
        staged = set(tmp_path.rglob('*.parquet'))
        with pytest.raises(ValueError, match='overlapping first_row_ids'):
            updater.update_columns(pa.table({'_ROW_ID': [1], 'age': [99]}), ['age'])
        assert set(tmp_path.rglob('*.parquet')) == staged
        assert len(updater.commit_messages) == 2
    commit = builder.new_commit()
    if stream:
        commit.commit(updater.commit_messages, commit_id)
    else:
        commit.commit(updater.commit_messages)
    commit.close()
    assert table.snapshot_manager().get_latest_snapshot().commit_identifier == commit_id
    read = table.new_read_builder()
    actual = read.new_read().to_arrow(read.new_scan().plan().splits()).sort_by('id')
    assert actual.to_pydict() == {'id': [1, 2], 'name': ['a', 'B'], 'age': [11, 20]}


@pytest.mark.parametrize('stream', [False, True])
def test_partitioned_upsert_and_incremental_update_use_core(tmp_path, stream):
    from pypaimon.write.native_update import NativeTableUpdateByRowId
    from pypaimon.table.row.generic_row import GenericRow

    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([('p', pa.string()), ('id', pa.int32()), ('v', pa.int32())])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, partition_keys=['p'], options={
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'write.native.enabled': 'true',
    }), False)
    table = catalog.get_table('default.t')
    seed = table.new_batch_write_builder()
    writer = seed.new_write()
    writer.write_arrow(pa.table({'p': ['a', 'b', None], 'id': [1, 1, 1], 'v': [10, 20, 30]}, schema=schema))
    seed.new_commit().commit(writer.prepare_commit())
    writer.close()
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    update = builder.new_update()
    input_rows = pa.table({'p': ['a', 'c', None], 'id': [1, 1, 1], 'v': [11, 40, 31]}, schema=schema)
    with patch.object(TableUpsertByKey, '_upsert_partition',
                      side_effect=AssertionError('Python partition upsert selected')):
        messages = (update.upsert_by_arrow_with_key(input_rows, ['id'], 1) if stream
                    else update.upsert_by_arrow_with_key(input_rows, ['id']))
    commit = builder.new_commit()
    commit.commit(messages, 1) if stream else commit.commit(messages)
    commit.close()
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    update = builder.new_update()
    with patch.object(TableUpsertByKey, '_upsert_row_partition',
                      side_effect=AssertionError('Python partition row upsert selected')):
        rows = [GenericRow(['a', 1, 12], table.fields)]
        messages = (update.upsert_by_key(rows, ['id'], 2) if stream
                    else update.upsert_by_key(rows, ['id']))
    commit = builder.new_commit()
    commit.commit(messages, 2) if stream else commit.commit(messages)
    commit.close()
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    update = builder.new_update()
    read = table.new_read_builder().with_projection(['p', 'id', 'v', '_ROW_ID'])
    current = read.new_read().to_arrow(read.new_scan().plan().splits())
    selected = current.filter(pa.compute.equal(current['p'], 'a'))
    updater = update.new_update_by_row_id(3) if stream else update.new_update_by_row_id()
    assert isinstance(updater, NativeTableUpdateByRowId)
    messages = updater.update_columns(selected.select(['p', '_ROW_ID']), ['p'])
    commit = builder.new_commit()
    commit.commit(messages, 3) if stream else commit.commit(messages)
    commit.close()
    read = table.new_read_builder()
    actual = read.new_read().to_arrow(read.new_scan().plan().splits()).sort_by('p')
    assert actual.to_pydict() == {'p': ['a', 'b', 'c', None], 'id': [1, 1, 1, 1], 'v': [12, 20, 40, 31]}


def test_batch_row_id_update_uses_rust_and_python_commit(tmp_path):

    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([
        ('id', pa.int32()), ('name', pa.string()), ('age', pa.int32()),
    ])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, options={
        'row-tracking.enabled': 'true',
        'data-evolution.enabled': 'true',
        'write.native.enabled': 'true',
    }), False)
    table = catalog.get_table('default.t')
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    writer.write_arrow(pa.Table.from_pydict({
        'id': [1, 2, 3], 'name': ['a', 'b', 'c'], 'age': [10, 20, 30],
    }, schema=schema))
    builder.new_commit().commit(writer.prepare_commit())
    writer.close()

    pinned = table.copy({
        'scan.snapshot-id': str(table.snapshot_manager().get_latest_snapshot().id),
    })
    assert create_native_update(pinned, builder.commit_user, ['name']) is None

    update_builder = table.new_batch_write_builder()
    update = update_builder.new_update().with_update_type(['name', 'age'])
    changed = pa.Table.from_batches([
        pa.record_batch([
            pa.array([2], type=pa.int64()), pa.array(['C']),
            pa.array([31], type=pa.int32()),
        ], names=['_ROW_ID', 'name', 'age']),
        pa.record_batch([
            pa.array([0], type=pa.int64()), pa.array(['A']),
            pa.array([11], type=pa.int32()),
        ], names=['_ROW_ID', 'name', 'age']),
    ])
    with patch.object(TableUpdateByRowId, 'update_columns',
                      side_effect=AssertionError('Python update was selected')):
        messages = update.update_by_arrow_with_row_id(changed)
    assert messages
    assert all(file.file_path and table.file_io.exists(file.file_path)
               for message in messages for file in message.new_files)
    update_builder.new_commit().commit(messages)

    read_builder = table.new_read_builder()
    actual = read_builder.new_read().to_arrow(
        read_builder.new_scan().plan().splits()).sort_by('id')
    assert actual.select(['id', 'name', 'age']).to_pydict() == {
        'id': [1, 2, 3], 'name': ['A', 'b', 'C'], 'age': [11, 20, 31],
    }


def test_batch_row_id_delete_uses_rust_deletion_vectors(tmp_path):

    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([('id', pa.int32()), ('name', pa.string())])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, options={
        'row-tracking.enabled': 'true',
        'data-evolution.enabled': 'true',
        'deletion-vectors.enabled': 'true',
        'write.native.enabled': 'true',
    }), False)
    table = catalog.get_table('default.t')
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    writer.write_arrow(pa.Table.from_pydict({
        'id': [1, 2, 3], 'name': ['a', 'b', 'c'],
    }, schema=schema))
    builder.new_commit().commit(writer.prepare_commit())
    writer.close()

    pinned = table.copy({
        'scan.snapshot-id': str(table.snapshot_manager().get_latest_snapshot().id),
    })
    assert create_native_delete(pinned, builder.commit_user) is None

    with patch.object(TableDeleteByRowId, 'delete',
                      side_effect=AssertionError('Python delete was selected')):
        delete_builder = table.new_batch_write_builder()
        messages = delete_builder.new_update().delete_by_row_id([0, 2, 2])
        assert messages and sum(len(message.index_adds) for message in messages) == 1
        delete_builder.new_commit().commit(messages)

        predicate_builder = table.new_read_builder().new_predicate_builder()
        delete_builder = table.new_batch_write_builder()
        with patch.object(TableRead, 'to_arrow',
                          side_effect=AssertionError('Python predicate match selected')):
            messages = delete_builder.new_update().delete_by_predicate(
                predicate_builder.equal('id', 2))
        assert messages
        delete_builder.new_commit().commit(messages)

    read_builder = table.new_read_builder()
    actual = read_builder.new_read().to_arrow(read_builder.new_scan().plan().splits())
    assert actual.num_rows == 0


def test_stream_update_and_delete_use_native_writers(tmp_path):

    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([('id', pa.int32()), ('age', pa.int32())])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, options={
        'row-tracking.enabled': 'true',
        'data-evolution.enabled': 'true',
        'deletion-vectors.enabled': 'true',
        'write.native.enabled': 'true',
    }), False)
    table = catalog.get_table('default.t')
    initial = table.new_batch_write_builder()
    writer = initial.new_write()
    writer.write_arrow(pa.Table.from_pydict({
        'id': [1, 2, 3, 4], 'age': [10, 20, 30, 40],
    }, schema=schema))
    initial.new_commit().commit(writer.prepare_commit())
    writer.close()

    stream = table.new_stream_write_builder()
    committer = stream.new_commit()
    update = stream.new_update()
    with patch.object(TableUpdateByRowId, 'update_columns',
                      side_effect=AssertionError('Python row-ID update selected')):
        messages = update.update_by_arrow_with_row_id(
            pa.Table.from_pydict({'_ROW_ID': [0], 'age': [11]}), 10)
    committer.commit(messages, 10)

    predicate = table.new_read_builder().new_predicate_builder()
    with patch.object(StreamTableUpdate, '_build_predicate_update_table',
                      side_effect=AssertionError('Python assignments selected')), \
            patch.object(TableRead, 'to_arrow',
                         side_effect=AssertionError('Python predicate read selected')):
        messages = update.update_by_predicate(
            predicate.equal('id', 2), {'age': 22}, 20)
    committer.commit(messages, 20)

    with patch.object(TableDeleteByRowId, 'delete',
                      side_effect=AssertionError('Python row-ID delete selected')):
        messages = update.delete_by_row_id([2], 30)
        committer.commit(messages, 30)
        with patch.object(TableRead, 'to_arrow',
                          side_effect=AssertionError('Python predicate read selected')):
            messages = update.delete_by_predicate(predicate.equal('id', 4), 40)
        committer.commit(messages, 40)
    committer.close()

    read = table.new_read_builder()
    actual = read.new_read().to_arrow(read.new_scan().plan().splits()).sort_by('id')
    assert actual.select(['id', 'age']).to_pydict() == {
        'id': [1, 2], 'age': [11, 22],
    }
    assert table.snapshot_manager().get_latest_snapshot().commit_identifier == 40


def test_native_batch_update_preserves_input_table_boundaries(tmp_path):

    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([('id', pa.int32()), ('age', pa.int32())])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, options={
        'row-tracking.enabled': 'true',
        'data-evolution.enabled': 'true',
        'write.native.enabled': 'true',
    }), False)
    table = catalog.get_table('default.t')
    for ids in ([1, 2], [3, 4]):
        builder = table.new_batch_write_builder()
        writer = builder.new_write()
        writer.write_arrow(pa.Table.from_pydict({
            'id': ids, 'age': [value * 10 for value in ids],
        }, schema=schema))
        builder.new_commit().commit(writer.prepare_commit())
        writer.close()

    first = pa.Table.from_batches([
        pa.record_batch([pa.array([0], type=pa.int64()), pa.array([11])],
                        names=['_ROW_ID', 'age']),
        pa.record_batch([pa.array([1], type=pa.int64()), pa.array([22])],
                        names=['_ROW_ID', 'age']),
    ])
    with patch.object(BatchTableUpdate, '_update_by_arrow_batches_with_row_id',
                      side_effect=AssertionError('Python batch update selected')):
        with pytest.raises(ValueError, match='overlapping first_row_ids.*0'):
            table.new_batch_write_builder().new_update().with_update_type(
                ['age']).update_by_arrow_batches_with_row_id(iter([
                    first.slice(0, 1),
                    pa.Table.from_pydict({'_ROW_ID': [1], 'age': [23]}),
                ]))

        builder = table.new_batch_write_builder()
        messages = builder.new_update().update_by_arrow_batches_with_row_id(iter([
            first,
            pa.Table.from_pydict({'_ROW_ID': [2], 'age': [33]}),
            pa.Table.from_pydict({'_ROW_ID': [0], 'id': [10]}),
        ]))
    builder.new_commit().commit(messages)
    read = table.new_read_builder()
    actual = read.new_read().to_arrow(read.new_scan().plan().splits()).sort_by('id')
    assert actual.select(['id', 'age']).to_pydict() == {
        'id': [2, 3, 4, 10], 'age': [22, 33, 40, 11],
    }

    before = set(tmp_path.rglob('*.parquet'))

    def failed_input():
        yield first
        raise RuntimeError('input generator failed')

    with pytest.raises(RuntimeError, match='input generator failed'):
        table.new_batch_write_builder().new_update().update_by_arrow_batches_with_row_id(
            failed_input())
    assert set(tmp_path.rglob('*.parquet')) == before

    read_snapshot_id = table.snapshot_manager().get_latest_snapshot().id

    def interleaved_tables():
        yield pa.Table.from_pydict({'_ROW_ID': [0], 'age': [12]})
        concurrent = table.new_batch_write_builder()
        changed = concurrent.new_update().with_update_type(['age'])
        concurrent.new_commit().commit(changed.update_by_arrow_with_row_id(
            pa.Table.from_pydict({'_ROW_ID': [1], 'age': [23]})))

    with patch.object(BatchTableUpdate, '_update_by_arrow_batches_with_row_id',
                      side_effect=AssertionError('Python batch update selected')):
        staged = (table.new_batch_write_builder().new_update()
                  .with_update_type(['age'])
                  .update_by_arrow_batches_with_row_id(interleaved_tables()))
    assert staged and all(message.check_from_snapshot == read_snapshot_id
                          for message in staged)
    from pypaimon.write.file_store_commit import _abort_commit_messages
    _abort_commit_messages(table, staged)


def test_native_predicate_update_invokes_callable_by_file_group(tmp_path):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([('id', pa.int32()), ('age', pa.int32())])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, options={
        'row-tracking.enabled': 'true',
        'data-evolution.enabled': 'true',
        'write.native.enabled': 'true',
    }), False)
    table = catalog.get_table('default.t')
    for ids in ([1, 2], [3, 4]):
        builder = table.new_batch_write_builder()
        writer = builder.new_write()
        writer.write_arrow(pa.Table.from_pydict({
            'id': ids, 'age': [value * 10 for value in ids],
        }, schema=schema))
        builder.new_commit().commit(writer.prepare_commit())
        writer.close()

    seen = []
    predicate = table.new_read_builder().new_predicate_builder().greater_or_equal('id', 2)
    builder = table.new_batch_write_builder()
    with patch.object(BatchTableUpdate, '_build_predicate_update_table',
                      side_effect=AssertionError('Python assignments selected')), \
            patch.object(BatchTableUpdate, '_matched_update_scan_table',
                         side_effect=AssertionError('Python scan planning selected')):
        messages = builder.new_update().update_by_predicate(
            predicate,
            {'age': lambda matched: (
                seen.append(matched.num_rows) or
                pa.compute.add(matched['age'], 1)
            )},
            read_columns=['age'],
        )
    assert sorted(seen) == [1, 2]
    builder.new_commit().commit(messages)
    read_builder = table.new_read_builder()
    actual = read_builder.new_read().to_arrow(
        read_builder.new_scan().plan().splits()).sort_by('id')
    assert actual.select(['id', 'age']).to_pydict() == {
        'id': [1, 2, 3, 4], 'age': [10, 21, 31, 41],
    }


def test_native_upsert_matches_duplicate_source_and_target_keys(tmp_path):
    from pypaimon.table.row.generic_row import GenericRow
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([('id', pa.int32()), ('age', pa.int32())])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, options={
        'row-tracking.enabled': 'true',
        'data-evolution.enabled': 'true',
        'write.native.enabled': 'true',
    }), False)
    table = catalog.get_table('default.t')
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    writer.write_arrow(pa.Table.from_pydict({
        'id': [1, 1, 2], 'age': [10, 11, 20],
    }, schema=schema))
    builder.new_commit().commit(writer.prepare_commit())
    writer.close()

    updates = pa.Table.from_pydict({
        'id': [1, 1, 3], 'age': [100, 101, 30],
    }, schema=schema)
    builder = table.new_batch_write_builder()
    with patch.object(TableUpsertByKey, '_upsert_partition',
                      side_effect=AssertionError('Python upsert selected')):
        messages = builder.new_update().upsert_by_arrow_with_key(updates, ['id'])
    builder.new_commit().commit(messages)
    builder = table.new_batch_write_builder()
    with patch.object(TableUpsertByKey, '_upsert_row_partition',
                      side_effect=AssertionError('Python row upsert selected')):
        messages = builder.new_update().upsert_by_key([
            GenericRow([1, 201], table.fields),
            GenericRow([4, 40], table.fields),
        ], ['id'])
    builder.new_commit().commit(messages)
    read_builder = table.new_read_builder()
    actual = read_builder.new_read().to_arrow(
        read_builder.new_scan().plan().splits()).sort_by([('id', 'ascending'),
                                                          ('age', 'ascending')])
    assert actual.select(['id', 'age']).to_pydict() == {
        'id': [1, 1, 2, 3, 4], 'age': [201, 201, 20, 30, 40],
    }

    stream = table.new_stream_write_builder()
    stream_updates = pa.Table.from_pydict({
        'id': [2, 5], 'age': [25, 50],
    }, schema=schema)
    with patch.object(TableUpsertByKey, '_upsert_partition',
                      side_effect=AssertionError('Python stream selected')):
        messages = stream.new_update().upsert_by_arrow_with_key(
            stream_updates, ['id'], 77)
    stream.new_commit().commit(messages, 77)
    read_builder = table.new_read_builder()
    actual = read_builder.new_read().to_arrow(
        read_builder.new_scan().plan().splits())
    actual = actual.sort_by([('id', 'ascending'), ('age', 'ascending')])
    assert actual.select(['id', 'age']).to_pydict() == {
        'id': [1, 1, 2, 3, 4, 5],
        'age': [201, 201, 25, 30, 40, 50],
    }
    snapshot = table.snapshot_manager().get_latest_snapshot()
    assert snapshot.commit_identifier == 77

    # If the core operation is unavailable, fall back for the whole upsert.
    # No private matcher can leave Python coordinating native result indices.
    builder = table.new_batch_write_builder()
    fallback_updates = pa.Table.from_pydict({
        'id': [2, 6], 'age': [26, 60],
    }, schema=schema)
    with patch('pypaimon.write.native_update.create_native_upsert', return_value=None), \
            patch('pypaimon.write.native_update.create_native_update', return_value=None):
        messages = builder.new_update().upsert_by_arrow_with_key(fallback_updates, ['id'])
    builder.new_commit().commit(messages)
    read_builder = table.new_read_builder()
    actual = read_builder.new_read().to_arrow(
        read_builder.new_scan().plan().splits())
    actual = actual.sort_by([('id', 'ascending'), ('age', 'ascending')])
    assert actual.select(['id', 'age']).to_pydict() == {
        'id': [1, 1, 2, 3, 4, 5, 6],
        'age': [201, 201, 26, 30, 40, 50, 60],
    }

    from pypaimon.write.native_update import NativeTableUpsert
    before = set(tmp_path.rglob('*.parquet'))
    snapshot_id = table.snapshot_manager().get_latest_snapshot().id
    with patch.object(NativeTableUpsert, 'upsert', side_effect=RuntimeError('core upsert failed')), \
            patch.object(TableUpsertByKey, '_upsert_partition',
                         side_effect=AssertionError('retried as Python Arrow upsert')), \
            patch.object(TableUpsertByKey, '_upsert_row_partition',
                         side_effect=AssertionError('retried as Python row upsert')):
        with pytest.raises(RuntimeError, match='core upsert failed'):
            builder.new_update().upsert_by_arrow_with_key(fallback_updates, ['id'])
        with pytest.raises(RuntimeError, match='core upsert failed'):
            builder.new_update().upsert_by_key([GenericRow([2, 27], table.fields)], ['id'])
    assert table.snapshot_manager().get_latest_snapshot().id == snapshot_id
    assert set(tmp_path.rglob('*.parquet')) == before


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('values', [
    pa.array(['bad']), pa.array([2147483648], type=pa.int64()),
])
def test_row_id_cast_failure_does_not_write_nulls(tmp_path, native, values):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([('id', pa.int32()), ('value', pa.int32())])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, options={
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'write.native.enabled': str(native).lower(),
    }), False)
    table = catalog.get_table('default.t')
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    writer.write_arrow(pa.Table.from_pydict({'id': [1], 'value': [10]}, schema=schema))
    builder.new_commit().commit(writer.prepare_commit())
    writer.close()
    before = set(tmp_path.rglob('*.parquet'))
    update = pa.Table.from_arrays([pa.array([0], type=pa.int64()), values],
                                  names=['_ROW_ID', 'value'])
    with pytest.raises((ValueError, pa.ArrowInvalid)):
        table.new_batch_write_builder().new_update().update_by_arrow_with_row_id(update)
    assert set(tmp_path.rglob('*.parquet')) == before
    read = table.new_read_builder()
    actual = read.new_read().to_arrow(read.new_scan().plan().splits())
    assert actual.select(['id', 'value']).to_pydict() == {'id': [1], 'value': [10]}


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('values,target', [
    (pa.array([1.0]), pa.string()),
    (pa.array([1e-6], type=pa.float32()), pa.string()),
    (pa.array([-0.0]), pa.int32()),
    (pa.array([1.234]), pa.decimal128(10, 2)),
    (pa.array(['-1000e-5']), pa.decimal128(10, 2)),
    (pa.array(['1.234']), pa.decimal128(10, 2)),
    (pa.array([16777218], type=pa.int64()), pa.float32()),
    (pa.array([1], type=pa.timestamp('us')), pa.string()),
    (pa.array([1234], type=pa.timestamp('us')), pa.timestamp('ms')),
    (pa.array([1234], type=pa.timestamp('us')), pa.date32()),
    (pa.array([1.5]).dictionary_encode(), pa.int32()),
    (pa.array(['1.234']).dictionary_encode(), pa.decimal128(10, 2)),
    (pa.array([1.0]).dictionary_encode(), pa.string()),
    (pa.array([[1.5]]), pa.list_(pa.int32())),
    (pa.array([[1.0, None]]), pa.list_(pa.int32())),
    (pa.array([1234567], type=pa.timestamp('ns')), pa.time32('ms')),
    (pa.array([86400001000000], type=pa.timestamp('ns')), pa.time32('ms')),
    (pa.array([-1000000], type=pa.timestamp('ns')), pa.time32('ms')),
    (pa.array(['yes']), pa.bool_()),
    (pa.array(['f']), pa.bool_()),
    (pa.array(['TRUE']), pa.bool_()),
    (pa.array(['1970-01-01 00:00:00.123456']), pa.timestamp('ms')),
    (pa.array(['1970-01-01 00:00:00.0000']), pa.timestamp('ms')),
    (pa.array(['1970-01-01 00:00:00.123']), pa.timestamp('ms')),
    (pa.array(['1970-01-01T01']), pa.timestamp('ms')),
    (pa.array(['1970-01-01 00:00:00Z']), pa.timestamp('ms')),
    (pa.array([0], type=pa.date64()), pa.string()),
    (pa.array([0], type=pa.timestamp('us', 'Asia/Shanghai')), pa.timestamp('us')),
    (pa.array([-1000], type=pa.timestamp('us', 'Asia/Shanghai')), pa.timestamp('ms')),
    (pa.array([-1001], type=pa.timestamp('us', 'Asia/Shanghai')), pa.timestamp('ms')),
    (pa.array([42], type=pa.int64()), pa.binary()),
    (pa.array([None], type=pa.int64()), pa.binary()),
    (pa.array([b'1.25']), pa.float64()),
    (pa.array([b'1.25'], type=pa.large_binary()), pa.float64()),
    (pa.array([b'1.25']), pa.decimal128(6, 2)),
    (pa.array([b'1.234']), pa.decimal128(6, 2)),
    (pa.array([b'0042']), pa.int32()),
    (pa.array([b'0x2a']), pa.int32()),
    (pa.array([b'0Xffffffff'], type=pa.large_binary()), pa.int32()),
    (pa.array([b'+42']), pa.int32()),
    (pa.array(['0x2a']), pa.int32()),
    (pa.array(['+42']), pa.int32()),
    (pa.array([b'true']), pa.bool_()),
    (pa.array([b'yes']), pa.bool_()),
    (pa.array(['00:00:01.234567']), pa.time32('ms')),
    (pa.array(['2024-01-02 12:34:56']), pa.date32()),
    (pa.array(['2024-01-02']), pa.date32()),
    (pa.array([1.5]), pa.timestamp('ms')),
    (pa.array([1000000], type=pa.timestamp('us')), pa.float64()),
    (pa.array([1.5]), pa.date32()),
    (pa.array([1.5]), pa.time32('ms')),
    (pa.array([1], type=pa.int32()), pa.timestamp('ms')),
    (pa.array([2.675]), pa.decimal128(10, 2)),
    (pa.array([2.25], type=pa.float32()), pa.decimal128(10, 1)),
    (pa.array([0.1]), pa.decimal128(38, 20)),
])
def test_native_assignment_cast_matches_pyarrow(tmp_path, native, values, target):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([('id', pa.int32()), ('value', target)])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, options={
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'write.native.enabled': str(native).lower(),
    }), False)
    table = catalog.get_table('default.t')
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    writer.write_arrow(pa.Table.from_pydict({'id': [1], 'value': [None]}, schema=schema))
    builder.new_commit().commit(writer.prepare_commit())
    writer.close()
    before = set(tmp_path.rglob('*.parquet'))
    update = table.new_batch_write_builder()
    try:
        expected = values.cast(target).to_pylist()
    except pa.ArrowException:
        with pytest.raises((ValueError, pa.ArrowException)):
            update.new_update().update_by_predicate(None, {'value': values})
        assert set(tmp_path.rglob('*.parquet')) == before
        expected = [None]
    else:
        messages = update.new_update().update_by_predicate(None, {'value': values})
        update.new_commit().commit(messages)
    read = table.new_read_builder()
    actual = read.new_read().to_arrow(read.new_scan().plan().splits())
    assert actual['value'].to_pylist() == expected


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('callable_assignment', [False, True])
def test_predicate_assignment_order_with_later_partial_file(
        tmp_path, native, callable_assignment):
    from contextlib import nullcontext
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([('id', pa.int32()), ('age', pa.int32())])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, options={
        'row-tracking.enabled': 'true',
        'data-evolution.enabled': 'true',
        'write.native.enabled': str(native).lower(),
    }), False)
    table = catalog.get_table('default.t')
    for ids in ([1, 2], [3, 4]):
        builder = table.new_batch_write_builder()
        writer = builder.new_write()
        writer.write_arrow(pa.Table.from_pydict({
            'id': ids, 'age': [value * 10 for value in ids],
        }, schema=schema))
        builder.new_commit().commit(writer.prepare_commit())
        writer.close()
    builder = table.new_batch_write_builder()
    messages = builder.new_update().update_by_arrow_with_row_id(
        pa.table({'_ROW_ID': [2], 'age': [33]}))
    builder.new_commit().commit(messages)

    seen = []

    def assign(matched):
        seen.append(matched['id'].to_pylist())
        return pa.compute.add(matched['age'], 1)

    assignment = assign if callable_assignment else pa.chunked_array(
        [[101], [102, 103, 104]], type=pa.int32())
    guard = (patch.object(BatchTableUpdate, '_matched_update_scan_table',
                          side_effect=AssertionError('Python planning selected'))
             if native else nullcontext())
    with guard:
        messages = builder.new_update().update_by_predicate(
            None, {'age': assignment},
            read_columns=['id', 'age'] if callable_assignment else None)
    builder.new_commit().commit(messages)
    if callable_assignment:
        assert seen == [[1, 2], [3, 4]]
    read = table.new_read_builder()
    actual = read.new_read().to_arrow(read.new_scan().plan().splits()).sort_by('id')
    assert actual.select(['id', 'age']).to_pydict() == {
        'id': [1, 2, 3, 4],
        'age': [11, 21, 34, 41] if callable_assignment else [101, 102, 103, 104],
    }


@pytest.mark.parametrize('stream', [False, True])
def test_native_row_upsert_uses_public_operation_with_composite_null_keys(tmp_path, stream):
    from pypaimon.table.row.generic_row import GenericRow
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([
        ('id', pa.int32()), ('part', pa.string()), ('age', pa.int32()), ('keep', pa.string()),
    ])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, options={
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'write.native.enabled': 'true',
    }), False)
    table = catalog.get_table('default.t')
    seed = table.new_batch_write_builder()
    writer = seed.new_write()
    writer.write_arrow(pa.Table.from_pydict({
        'id': [1, 1, None, 2], 'part': ['a', 'a', 'n', 'b'],
        'age': [10, 11, 20, 30], 'keep': ['old'] * 4,
    }, schema=schema))
    seed.new_commit().commit(writer.prepare_commit())
    writer.close()
    fields = list(reversed(table.fields))
    rows = (GenericRow(values, fields) for values in [
        ['new', 101, 'a', 1], ['new', 102, 'a', 1],
        ['new', None, 'n', None], ['new', 40, 'c', 3],
    ])
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    update = builder.new_update().with_update_type(['age'])
    with patch.object(TableUpsertByKey, '_upsert_row_partition',
                      side_effect=AssertionError('Python upsert selected')):
        if stream:
            messages = update.upsert_by_key(rows, ['id', 'part'], 88)
            builder.new_commit().commit(messages, 88)
        else:
            messages = update.upsert_by_key(rows, ['id', 'part'])
            builder.new_commit().commit(messages)
    read = table.new_read_builder()
    actual = read.new_read().to_arrow(read.new_scan().plan().splits()).sort_by('id')
    assert actual.to_pydict() == {
        'id': [1, 1, 2, 3, None], 'part': ['a', 'a', 'b', 'c', 'n'],
        'age': [102, 102, 30, 40, None], 'keep': ['old', 'old', 'old', 'new', 'old'],
    }


@pytest.mark.parametrize('case', ['partial', 'float-key', 'empty-columns'])
def test_row_upsert_unsupported_inputs_keep_python_semantics(tmp_path, case):
    from pypaimon.table.row.generic_row import GenericRow
    from pypaimon.write.native_update import NativeTableUpsert
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([
        ('id', pa.float64() if case == 'float-key' else pa.int32()),
        ('age', pa.int32()), ('region', pa.string()),
    ])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        schema, options={
            'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
            'write.native.enabled': 'true',
        }), False)
    table = catalog.get_table('default.t')
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    writer.write_arrow(pa.Table.from_pydict({
        'id': [1, 2], 'age': [10, 20], 'region': ['east', 'west'],
    }, schema=schema))
    builder.new_commit().commit(writer.prepare_commit())
    writer.close()
    if case == 'partial':
        rows = [GenericRow([1, 11], table.fields[:2])]
    else:
        rows = [GenericRow(values, table.fields) for values in [
            [1, 11, 'east'], [3, 30, 'east'],
        ]]
    if case == 'empty-columns':
        before = set(tmp_path.rglob('*.parquet'))
        with patch.object(NativeTableUpsert, 'upsert',
                          side_effect=AssertionError('Empty row update became all columns')), \
                pytest.raises(ValueError, match='column_names cannot be empty'):
            builder.new_update().with_update_type([]).upsert_by_key(rows, ['id'])
        assert set(tmp_path.rglob('*.parquet')) == before
        read = table.new_read_builder()
        actual = read.new_read().to_arrow(read.new_scan().plan().splits()).sort_by('id')
        assert actual.to_pydict() == {
            'id': [1, 2], 'age': [10, 20], 'region': ['east', 'west'],
        }
        return
    with patch.object(NativeTableUpsert, 'upsert',
                      side_effect=AssertionError('Unsupported native upsert selected')):
        messages = builder.new_update().with_update_type(['age']).upsert_by_key(rows, ['id'])
    builder.new_commit().commit(messages)
    read = table.new_read_builder()
    actual = read.new_read().to_arrow(read.new_scan().plan().splits()).sort_by('id')
    assert actual.to_pydict() == {
        'id': [1, 2] if case == 'partial' else [1, 2, 3],
        'age': [11, 20] if case == 'partial' else [11, 20, 30],
        'region': ['east', 'west'] if case == 'partial' else ['east', 'west', 'east'],
    }


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('grouped', [False, True])
@pytest.mark.parametrize('empty_chunks', [False, True])
@pytest.mark.parametrize('values,target,row_id_type', [
    (pa.array([99], type=pa.int32()), pa.int32(), pa.int32()),
    (pa.array([99], type=pa.int32()), pa.int32(), pa.uint64()),
    (pa.array([99], type=pa.int32()), pa.int32(),
     pa.dictionary(pa.int8(), pa.int32())),
    (pa.array([1.0]), pa.string(), pa.int64()),
    (pa.array([0], type=pa.timestamp('us')), pa.string(), pa.int64()),
    (pa.array(['yes']), pa.bool_(), pa.int64()),
    (pa.array([1.5]), pa.int32(), pa.int64()),
    (pa.array([2 ** 63 - 1], type=pa.int64()), pa.float64(), pa.int64()),
    (pa.array([-1234567], type=pa.timestamp('ns')), pa.timestamp('ms'), pa.int64()),
    (pa.array([1234567], type=pa.timestamp('ns')), pa.time32('ms'), pa.int64()),
    (pa.array([99], type=pa.int32()), pa.int32(), pa.uint32()),
    (pa.array(['1970-01-01 00:00:00.123456']), pa.timestamp('ms'), pa.int32()),
    (pa.array(['1970-01-01 00:00:00.123']), pa.timestamp('ms'), pa.int32()),
    (pa.array([0], type=pa.date64()), pa.string(), pa.int32()),
    (pa.array([0], type=pa.timestamp('us', 'Asia/Shanghai')), pa.timestamp('us'), pa.int32()),
    (pa.array([-1001], type=pa.timestamp('us', 'Asia/Shanghai')), pa.timestamp('ms'), pa.int32()),
    (pa.array([42], type=pa.int64()), pa.binary(), pa.int32()),
    (pa.array([None], type=pa.int64()), pa.binary(), pa.int32()),
    (pa.array([b'1.25']), pa.float64(), pa.int32()),
    (pa.array([b'1.25'], type=pa.large_binary()), pa.float64(), pa.int32()),
    (pa.array([b'1.234']), pa.decimal128(6, 2), pa.int32()),
    (pa.array([b'0x2a']), pa.int32(), pa.int32()),
    (pa.array([b'+42']), pa.int32(), pa.int32()),
    (pa.array(['00:00:01.234567']), pa.time32('ms'), pa.int64()),
    (pa.array(['2024-01-02 12:34:56']), pa.date32(), pa.int64()),
    (pa.array(['2024-01-02']), pa.date32(), pa.int64()),
    (pa.array([1.5]), pa.timestamp('ms'), pa.int64()),
    (pa.array([1000000], type=pa.timestamp('us')), pa.float64(), pa.int64()),
    (pa.array([1.5]), pa.date32(), pa.int64()),
    (pa.array([1.5]), pa.time32('ms'), pa.int64()),
    (pa.array([1], type=pa.int32()), pa.timestamp('ms'), pa.int64()),
    (pa.array([2.675]), pa.decimal128(10, 2), pa.int64()),
    (pa.array([2.25], type=pa.float32()), pa.decimal128(10, 1), pa.int64()),
    (pa.array([0.1]), pa.decimal128(38, 20), pa.int64()),
])
def test_native_row_id_input_conversion_matches_python(
        tmp_path, native, grouped, empty_chunks, values, target, row_id_type):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([('id', pa.int32()), ('value', target)])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, options={
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'write.native.enabled': str(native).lower(),
    }), False)
    table = catalog.get_table('default.t')
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    writer.write_arrow(pa.table({'id': [1], 'value': [None]}, schema=schema))
    builder.new_commit().commit(writer.prepare_commit())
    writer.close()
    data = pa.table({'_ROW_ID': pa.array([0], type=row_id_type), 'value': values})
    if empty_chunks:
        batch = data.to_batches()[0]
        data = pa.Table.from_batches([batch.slice(0, 0), batch, batch.slice(1, 0)])
    before = set(tmp_path.rglob('*.parquet'))
    updater = builder.new_update()

    def prepare():
        if grouped:
            return updater.update_by_arrow_batches_with_row_id(iter([data]))
        return updater.update_by_arrow_with_row_id(data)

    try:
        expected = TableUpdateByRowId._coerce_column(values, target).to_pylist()
    except (ValueError, pa.ArrowException):
        with pytest.raises((ValueError, pa.ArrowException)):
            prepare()
        expected = [None]
        assert set(tmp_path.rglob('*.parquet')) == before
    else:
        # Ensure a native case does not silently exercise the Python fallback.
        if native:
            with patch.object(TableUpdateByRowId, 'update_columns',
                              side_effect=AssertionError('Python update selected')):
                messages = prepare()
        else:
            messages = prepare()
        builder.new_commit().commit(messages)
    read = table.new_read_builder()
    result = read.new_read().to_arrow(read.new_scan().plan().splits())
    assert result['value'].to_pylist() == expected


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('duplicate', ['_ROW_ID', 'value', 'unused'])
def test_row_id_update_rejects_only_referenced_duplicate_columns(tmp_path, native, duplicate):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([('id', pa.int32()), ('value', pa.int32())])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, options={
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'write.native.enabled': str(native).lower(),
    }), False)
    table = catalog.get_table('default.t')
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    writer.write_arrow(pa.table({'id': [1, 2], 'value': [10, 20]}, schema=schema))
    builder.new_commit().commit(writer.prepare_commit())
    writer.close()
    data = pa.Table.from_arrays([
        pa.array([0]), pa.array([100]), pa.array([1]), pa.array([2]),
    ], names=['_ROW_ID', 'value', duplicate, duplicate])
    before = set(tmp_path.rglob('*.parquet'))
    # Repeating configuration names remains valid; ambiguity is in input fields.
    updater = builder.new_update().with_update_type(['value', 'value'])
    if duplicate == 'unused':
        messages = updater.update_by_arrow_with_row_id(data)
        builder.new_commit().commit(messages)
    else:
        with pytest.raises((KeyError, ValueError, pa.ArrowException)):
            updater.update_by_arrow_with_row_id(data)
        assert set(tmp_path.rglob('*.parquet')) == before
    read = table.new_read_builder()
    actual = read.new_read().to_arrow(read.new_scan().plan().splits()).sort_by('id')
    assert actual['value'].to_pylist() == ([100, 20] if duplicate == 'unused' else [10, 20])


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('values,target', [
    (pa.array([{'a': 1, 'b': 1.5}], type=pa.struct([('a', pa.int32()), ('b', pa.float64())])),
     pa.struct([('a', pa.string()), ('b', pa.int32())])),
    (pa.array([[(1, 1.5)]], type=pa.map_(pa.int32(), pa.float64())), pa.map_(pa.string(), pa.int32())),
    (pa.array([{'a': 1, 'b': 2.0}], type=pa.struct([('a', pa.int32()), ('b', pa.float64())])),
     pa.struct([('a', pa.string()), ('b', pa.int32())])),
    (pa.StructArray.from_arrays([pa.array([2 ** 31, 7])], names=['a'], mask=pa.array([True, False])),
     pa.struct([('a', pa.int32())])),
    (pa.ListArray.from_arrays([0, 1, 2], pa.array([2 ** 31, 7]), mask=pa.array([True, False])),
     pa.list_(pa.int32())),
    (pa.MapArray.from_arrays([0, 1, 2], pa.array(['a', 'b']), pa.array([1.5, 7.5]),
                             mask=pa.array([True, False])), pa.map_(pa.string(), pa.int32())),
])
def test_core_nested_row_update_uses_whole_column_constructor_fallback(tmp_path, native, values, target):
    from pypaimon.write.native_commit import create_native_write_table
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([('id', pa.int32()), ('value', target)])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, options={
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'write.native.enabled': 'false',
    }), False)
    table = catalog.get_table('default.t')
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    writer.write_arrow(pa.table({'id': list(range(len(values))), 'value': [None] * len(values)}, schema=schema))
    builder.new_commit().commit(writer.prepare_commit())
    writer.close()
    # Exercise core nested conversion directly even when dispatch would fall back.
    builder = (create_native_write_table(table) if native else table).new_batch_write_builder()
    data = pa.table({'_ROW_ID': list(range(len(values))), 'value': values})
    before = set(tmp_path.rglob('*.parquet'))
    try:
        expected = TableUpdateByRowId._coerce_column(values, target).to_pylist()
    except (ValueError, pa.ArrowException):
        with pytest.raises((ValueError, pa.ArrowException)):
            builder.new_update().update_by_arrow_with_row_id(data)
        assert set(tmp_path.rglob('*.parquet')) == before
        expected = [None] * len(values)
    else:
        messages = builder.new_update().update_by_arrow_with_row_id(data)
        builder.new_commit().commit(messages)
    read = table.new_read_builder()
    actual = read.new_read().to_arrow(read.new_scan().plan().splits()).sort_by('id')
    assert actual['value'].to_pylist() == expected


@pytest.mark.parametrize('layout', [
    'legacy', 'canonical', 'ordinary', 'directory', 'external', 'float', 'double', 'external-binary',
])
@pytest.mark.parametrize('operation', ['row', 'predicate'])
def test_update_dispatch_preserves_existing_partition_paths(tmp_path, layout, operation):
    from pypaimon.write.native_commit import create_native_write_table
    from pypaimon.write.native_update import NativeBatchTableUpdate, NativePredicateTableUpdate
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('default', True)
    part_type = {'float': pa.float32(), 'double': pa.float64(), 'external-binary': pa.binary()}.get(
        layout, pa.string())
    schema = pa.schema([('id', pa.int32()), ('part', part_type), ('value', pa.int32())])
    options = {
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'write.native.enabled': str(layout == 'canonical').lower(),
    }
    if layout == 'directory':
        options['data-file.path-directory'] = 'data'
    if layout.startswith('external'):
        options['data-file.external-paths'] = (tmp_path / 'external').as_uri()
        options['data-file.external-paths.strategy'] = 'round-robin'
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        schema, partition_keys=['part'], options=options), False)
    table = catalog.get_table('default.t')
    seed_table = create_native_write_table(table) if layout == 'canonical' else table
    builder = seed_table.new_batch_write_builder()
    writer = builder.new_write()
    parts = ['east', 'west'] if layout == 'ordinary' else ['a/b', 'a%b']
    if layout in ('float', 'double'):
        parts = [1.5, 2.25]
    elif layout == 'external-binary':
        parts = [b'x', b'a/b']
    data = pa.table({'id': [1, 2], 'part': parts, 'value': [10, 20]}, schema=schema)
    writer.write_arrow(data.to_batches()[0] if layout == 'canonical' else data)
    builder.new_commit().commit(writer.prepare_commit())
    if layout != 'canonical':
        writer.close()
    table = table.copy({'write.native.enabled': 'true'})
    native_calls = []
    wrapper = NativeBatchTableUpdate if operation == 'row' else NativePredicateTableUpdate
    method = 'update_by_arrow_with_row_id' if operation == 'row' else 'update'
    original = getattr(wrapper, method)

    def track(*args, **kwargs):
        native_calls.append(operation)
        return original(*args, **kwargs)

    # Check a second update too: fallback outputs must remain readable on the
    # next operation, and canonical files must continue using native updates.
    with patch.object(wrapper, method, track):
        for value in [11, 12]:
            builder = table.new_batch_write_builder()
            if operation == 'row':
                messages = builder.new_update().update_by_arrow_with_row_id(
                    pa.table({'_ROW_ID': [0, 1], 'value': pa.array([value, value], pa.int32())}))
            else:
                messages = builder.new_update().update_by_predicate(None, {'value': value})
            builder.new_commit().commit(messages)
            read = table.new_read_builder()
            actual = read.new_read().to_arrow(read.new_scan().plan().splits()).sort_by('id')
            assert actual.to_pydict() == {'id': [1, 2], 'part': parts, 'value': [value, value]}
    expected_calls = 2 if layout in ('canonical', 'ordinary', 'external') else 0
    assert len(native_calls) == expected_calls
