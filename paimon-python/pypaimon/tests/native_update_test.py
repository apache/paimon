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
from pypaimon.write.table_update import BatchTableUpdate
from pypaimon.write.table_upsert_by_key import TableUpsertByKey


@pytest.mark.native_plan
def test_batch_row_id_update_uses_rust_and_python_commit(tmp_path):
    from pypaimon_rust.datafusion import BatchWriteBuilder

    if not hasattr(BatchWriteBuilder, 'new_update'):
        pytest.skip('installed Rust binding does not expose batch update yet')
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
    with patch.object(BatchTableUpdate, '_update_by_arrow_with_row_id',
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


@pytest.mark.native_plan
def test_batch_row_id_delete_uses_rust_deletion_vectors(tmp_path):
    from pypaimon_rust.datafusion import BatchWriteBuilder

    if not hasattr(BatchWriteBuilder, 'new_delete'):
        pytest.skip('installed Rust binding does not expose batch delete yet')
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


@pytest.mark.native_plan
def test_native_predicate_update_invokes_callable_by_file_group(tmp_path):
    from pypaimon_rust.datafusion import BatchTableUpdate as RustUpdate
    if not hasattr(RustUpdate, 'add_assigned_table'):
        pytest.skip('installed Rust binding lacks native assignments')
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
                      side_effect=AssertionError('Python assignments selected')):
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


@pytest.mark.native_plan
def test_native_upsert_matches_duplicate_source_and_target_keys(tmp_path):
    try:
        from pypaimon_rust.datafusion import UpsertKeyMatcher
    except ImportError:
        pytest.skip('installed Rust binding lacks native upsert key matching')
    from pypaimon.table.row.generic_row import GenericRow
    assert UpsertKeyMatcher
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
    with patch.object(TableUpsertByKey, '_build_key_to_row_ids_map',
                      side_effect=AssertionError('Python key matcher selected')):
        messages = builder.new_update().upsert_by_arrow_with_key(updates, ['id'])
    builder.new_commit().commit(messages)
    builder = table.new_batch_write_builder()
    with patch.object(TableUpsertByKey, '_build_key_to_row_ids_map',
                      side_effect=AssertionError('Python row key matcher selected')):
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
