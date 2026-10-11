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
import pyarrow.compute as pc
import pytest

from pypaimon import Schema

pytestmark = pytest.mark.native_plan


def _table(catalog, rows=None, partitions=False):
    schema = pa.schema([('p', pa.string()), ('id', pa.int32()), ('value', pa.int32())])
    catalog.create_table('default.shards', Schema.from_pyarrow_schema(
        schema, partition_keys=['p'] if partitions else [], options={
            'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true',
            'deletion-vectors.enabled': 'true', 'write.native.enabled': 'true',
        }), False)
    table = catalog.get_table('default.shards')
    if rows:
        _append(table, rows, schema)
    return table, schema


def _append(table, rows, schema):
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    try:
        writer.write_arrow(pa.Table.from_pylist(rows, schema=schema))
        builder.new_commit().commit(writer.prepare_commit())
    finally:
        writer.close()


def _updater(table, index=0, count=1, projection=('id',), columns=('value',), stream=False):
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    update = builder.new_update()
    update.with_read_projection(list(projection))
    update.with_update_type(list(columns))
    with patch('pypaimon.write.table_update.ShardTableUpdator',
               side_effect=AssertionError('Python shard mapping was used')):
        return update.new_shard_updator(index, count)


def _rows(table):
    builder = table.new_read_builder()
    return builder.new_read().to_arrow(builder.new_scan().plan().splits()).sort_by('id').to_pydict()


@pytest.mark.parametrize('stream', [False, True])
def test_native_shard_updates_use_the_captured_reader_order(native_rest_catalog, stream):
    table, schema = _table(native_rest_catalog, [
        dict(p='a', id=1, value=10), dict(p='a', id=2, value=20)])
    updater = _updater(table, stream=stream)
    _append(table, [dict(p='a', id=3, value=30)], schema)
    reader = updater.arrow_reader()
    for batch in reader:
        assert batch.schema.names == ['id']
        updater.update_by_arrow_batch(pa.record_batch([pc.multiply(batch['id'], 100)], names=['value']))
    messages = updater.prepare_commit()
    assert all(message.check_from_snapshot == 1 for message in messages)
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    builder.new_commit().commit(messages, 27) if stream else builder.new_commit().commit(messages)
    updater.close()
    assert _rows(table) == dict(p=['a', 'a', 'a'], id=[1, 2, 3], value=[100, 200, 30])


@pytest.mark.parametrize('partitions', [False, True])
@pytest.mark.parametrize('count', [1, 3, 7])
def test_native_shards_are_balanced_by_file_group(native_rest_catalog, partitions, count):
    table, schema = _table(native_rest_catalog, partitions=partitions)
    for i in range(5):
        _append(table, [dict(p=str(i % 2), id=i, value=i * 10)], schema)
    snapshot_id = table.snapshot_manager().get_latest_snapshot().id
    updaters = [_updater(table, i, count, projection=('_ROW_ID', 'id')) for i in range(count)]
    sizes = []
    for updater in updaters:
        with updater.arrow_reader() as reader:
            batches = list(reader)
        sizes.append(sum(batch.num_rows for batch in batches))
        if batches:
            ids = pa.Table.from_batches(batches)['id']
            # One input table may span physical files and partitions.
            updater.update_by_arrow_batch(pa.record_batch([pc.multiply(ids, 100).combine_chunks()], names=['value']))
        messages = updater.prepare_commit()
        assert all(message.check_from_snapshot == snapshot_id for message in messages)
        for message in messages:
            assert message.bucket == 0
            assert all(file.first_row_id is not None and file.write_cols == ['value'] for file in message.new_files)
        table.new_batch_write_builder().new_commit().commit(messages)
        updater.close()
    base, remainder = divmod(5, count)
    assert sizes == [base + (i < remainder) for i in range(count)]
    assert _rows(table)['value'] == [0, 100, 200, 300, 400]
    # Replacements and base files overlap, so they must still be one file group.
    with _updater(table, 0, 7).arrow_reader() as reader:
        assert sum(batch.num_rows for batch in reader) == 1


def test_native_shard_deleted_rows_do_not_shift_updates(native_rest_catalog):
    table, _ = _table(native_rest_catalog, [dict(p='a', id=i, value=i * 10) for i in range(1, 5)])
    builder = table.new_batch_write_builder()
    builder.new_commit().commit(builder.new_update().delete_by_row_id([1, 3]))
    updater = _updater(table, projection=('id', '_ROW_ID'))
    with updater.arrow_reader() as reader:
        data = pa.Table.from_batches(list(reader))
    assert data.to_pydict() == {'id': [1, 3], '_ROW_ID': [0, 2]}
    updater.update_by_arrow_batch(pa.record_batch([pa.array([111, 333], type=pa.int32())], names=['value']))
    messages = updater.prepare_commit()
    assert all(file.row_count == 4 and file.first_row_id == 0 for m in messages for file in m.new_files)
    table.new_batch_write_builder().new_commit().commit(messages)
    assert _rows(table) == dict(p=['a', 'a'], id=[1, 3], value=[111, 333])


@pytest.mark.parametrize('bad', ['wrong_column', 'wrong_type', 'excess_rows'])
def test_native_shard_invalid_batch_does_not_consume_row_ids(native_rest_catalog, bad):
    table, _ = _table(native_rest_catalog, [dict(p='a', id=i, value=i * 10) for i in range(1, 4)])
    updater = _updater(table)
    with updater.arrow_reader() as reader:
        assert sum(batch.num_rows for batch in reader) == 3
    if bad == 'wrong_column':
        data = pa.record_batch([pa.array([99], type=pa.int32())], names=['missing'])
    elif bad == 'wrong_type':
        data = pa.record_batch([pa.array(['invalid'])], names=['value'])
    else:
        data = pa.record_batch([pa.array([99] * 4, type=pa.int32())], names=['value'])
    with pytest.raises(ValueError):
        updater.update_by_arrow_batch(data)
    updater.update_by_arrow_batch(pa.record_batch([pa.array([100], type=pa.int32())], names=['value']))
    with pytest.raises(ValueError, match='same number of rows'):
        updater.prepare_commit()
    assert table.snapshot_manager().get_latest_snapshot().id == 1
    updater.update_by_arrow_batch(pa.record_batch([pa.array([200, 300], type=pa.int32())], names=['value']))
    messages = updater.prepare_commit()
    repeated = updater.prepare_commit()
    assert [f.file_name for m in messages for f in m.new_files] == [f.file_name for m in repeated for f in m.new_files]
    updater.close()
    table.new_batch_write_builder().new_commit().commit(messages)
    assert _rows(table)['value'] == [100, 200, 300]
    with pytest.raises(ValueError):
        updater.update_by_arrow_batch(data)


def test_native_shard_closed_reader_cannot_prepare_an_incomplete_update(native_rest_catalog):
    table, schema = _table(native_rest_catalog, [dict(p='a', id=1, value=10)])
    _append(table, [dict(p='a', id=2, value=20)], schema)
    updater = _updater(table)
    reader = updater.arrow_reader()
    batch = reader.read_next_batch()
    updater.update_by_arrow_batch(pa.record_batch([pa.array([99] * batch.num_rows, type=pa.int32())], names=['value']))
    reader.close()
    with pytest.raises(ValueError, match='same number of rows'):
        updater.prepare_commit()
    with pytest.raises(ValueError, match='only be opened once'):
        updater.arrow_reader()
    updater.close()
    assert _rows(table)['value'] == [10, 20]


def test_native_shard_no_row_count_rolling_and_full_column_update(native_rest_catalog):
    table, _ = _table(native_rest_catalog, [dict(p='a', id=i, value=i * 10) for i in range(5)])
    table = table.copy({'target-file-row-num': '1'})
    # All-column updates follow the same contract as a column subset.
    updater = _updater(table, projection=('id',), columns=('p', 'id', 'value'))
    with updater.arrow_reader() as reader:
        batches = list(reader)
    for batch in batches:
        ids = batch['id'].to_pylist()
        updater.update_by_arrow_batch(pa.record_batch([
            pa.array([i + 10 for i in ids], type=pa.int32()),
            pa.array(['new'] * len(ids)),
            pa.array([i + 100 for i in ids], type=pa.int32()),
        ], names=['id', 'p', 'value']))
    messages = updater.prepare_commit()
    assert len([file for m in messages for file in m.new_files]) == 1
    table.new_batch_write_builder().new_commit().commit(messages)
    assert _rows(table) == dict(p=['new'] * 5, id=list(range(10, 15)), value=list(range(100, 105)))


def test_native_empty_shard_and_configuration_validation(native_rest_catalog):
    table, _ = _table(native_rest_catalog)
    updater = _updater(table, 2, 4)
    with updater.arrow_reader() as reader:
        assert list(reader) == []
    assert updater.prepare_commit() == []
    for index, count in [(-1, 1), (0, 0), (2, 2)]:
        with pytest.raises(ValueError, match='Shard index'):
            table.new_batch_write_builder().new_update().new_shard_updator(index, count)
    with pytest.raises(ValueError, match='not be empty'):
        _updater(table, columns=())


def test_native_shard_rejects_partition_column_updates(native_rest_catalog):
    table, _ = _table(native_rest_catalog, partitions=True)
    with pytest.raises(NotImplementedError, match='partition column'):
        _updater(table, columns=('p',))


@pytest.mark.parametrize('data_type, original, replacement', [
    (pa.list_(pa.int32()), [[1, None], None], [[3, 4], []]),
    (pa.map_(pa.string(), pa.int32()), [[('a', 1)], None], [[('b', None)], []]),
    (pa.struct([('a', pa.int32()), ('b', pa.string())]),
     [dict(a=1, b='before'), None], [dict(a=None, b='after'), dict(a=2, b=None)]),
])
def test_native_shard_updates_existing_complex_column_types(native_rest_catalog, data_type, original, replacement):
    schema = pa.schema([('id', pa.int32()), ('value', data_type)])
    native_rest_catalog.create_table('default.complex_shards', Schema.from_pyarrow_schema(schema, options={
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'write.native.enabled': 'true',
    }), False)
    table = native_rest_catalog.get_table('default.complex_shards')
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    writer.write_arrow(pa.table({'id': [1, 2], 'value': original}, schema=schema))
    builder.new_commit().commit(writer.prepare_commit())
    writer.close()
    updater = _updater(table)
    with updater.arrow_reader() as reader:
        assert sum(batch.num_rows for batch in reader) == 2
    updater.update_by_arrow_batch(pa.record_batch([pa.array(replacement, type=data_type)], names=['value']))
    table.new_batch_write_builder().new_commit().commit(updater.prepare_commit())
    assert _rows(table) == dict(id=[1, 2], value=replacement)
