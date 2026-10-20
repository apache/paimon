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

"""Parquet vector sidecars must not force row-level updates back to Python."""

from contextlib import ExitStack
from unittest.mock import patch

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.read.table_read import TableRead
from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.table.data_evolution_merge_into import WhenMatched, WhenNotMatched, source_col
from pypaimon.write.table_update import TableUpdate
from pypaimon.write.table_update_by_row_id import TableUpdateByRowId
from pypaimon.write.table_upsert_by_key import TableUpsertByKey


pytestmark = pytest.mark.native_plan


def _table(tmp_path, native):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('db', True)
    schema = pa.schema([('id', pa.int32()), ('pt', pa.string()), ('score', pa.int32()),
                        ('embedding', pa.list_(pa.float32(), 2)), ('payload', pa.large_binary())])
    catalog.create_table('db.t', Schema.from_pyarrow_schema(schema, partition_keys=['pt'], options={
        'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true', 'deletion-vectors.enabled': 'true',
        'file.format': 'parquet', 'vector.file.format': 'parquet',
        'write.native.enabled': str(native).lower(), 'read.batch-size': '1',
    }), False)
    table = catalog.get_table('db.t')
    schema = PyarrowFieldParser.from_paimon_schema(table.fields)
    rows = [dict(id=i, pt='a' if i <= 2 else 'b', score=i,
                 embedding=[float(i), float(i + 1)], payload=('blob-%d' % i).encode()) for i in range(1, 5)]
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pylist(rows, schema=schema))
        messages = writer.prepare_commit()
        assert any(file.file_name.endswith('.vector.parquet') for message in messages for file in message.new_files)
        assert any(file.file_name.endswith('.blob') for message in messages for file in message.new_files)
        commit.commit(messages)
    finally:
        writer.close()
        commit.close()
    return table, schema


def _rows(table):
    for native in (False, True):
        view = table.copy({'read.native.enabled': str(native).lower(),
                           'scan.native-plan.enabled': str(native).lower()})
        builder = view.new_read_builder().with_projection(table.field_names + ['_ROW_ID'])
        rows = builder.new_read().to_arrow(builder.new_scan().plan().splits()).to_pylist()
        yield sorted(rows, key=lambda row: row['id'])


def _native_only(native):
    stack = ExitStack()
    if native:
        for owner, method in [(TableUpdateByRowId, '_load_existing_files_info'),
                              (TableUpsertByKey, '_upsert_partition'),
                              (TableUpdate, '_build_predicate_update_table')]:
            stack.enter_context(patch.object(owner, method, side_effect=AssertionError('Python update fallback')))
        stack.enter_context(patch('pypaimon.table.data_evolution_merge_into._build_tables',
                                  side_effect=AssertionError('Python merge fallback')))
    return stack


def _commit(builder, messages, stream):
    commit = builder.new_commit()
    try:
        commit.commit(messages, 71) if stream else commit.commit(messages)
    finally:
        commit.close()


def _replacement(schema, columns, ids):
    values = {'score': [42] * len(ids), 'embedding': [[9.0, 8.0]] * len(ids), 'payload': [b'new'] * len(ids)}
    return pa.table({name: pa.array(values[name], type=schema.field(name).type) for name in columns})


def _update(table, schema, native, stream, operation, columns, before):
    ids = {row['id']: row['_ROW_ID'] for row in before}
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    update = builder.new_update().with_update_type(columns)
    data = _replacement(schema, columns, [1, 3])
    with _native_only(native):
        if operation in ('row_id', 'incremental'):
            data = data.append_column('_ROW_ID', pa.array([ids[1], ids[3]], type=pa.int64()))
            if operation == 'row_id':
                messages = (update.update_by_arrow_with_row_id(data, 71) if stream
                            else update.update_by_arrow_with_row_id(data))
            else:
                updater = update.new_update_by_row_id(71) if stream else update.new_update_by_row_id()
                messages = updater.update_columns(data, columns)
        elif operation == 'upsert':
            data = data.append_column('id', pa.array([1, 3], type=pa.int32())).append_column('pt', ['a', 'b'])
            messages = (update.upsert_by_arrow_with_key(data, ['id'], 71) if stream
                        else update.upsert_by_arrow_with_key(data, ['id']))
        elif operation == 'predicate':
            predicate = update.new_predicate_builder().is_in('id', [1, 3])
            values = {'score': 42, 'embedding': [9.0, 8.0], 'payload': b'new'}
            assignments = {name: values[name] for name in columns}
            messages = (update.update_by_predicate(predicate, assignments, 71) if stream
                        else update.update_by_predicate(predicate, assignments))
        else:
            data = data.append_column('id', pa.array([1, 3], type=pa.int32())).append_column('pt', ['a', 'b'])
            kwargs = dict(on=['id', 'pt'], when_matched=[WhenMatched.update(
                {name: source_col(name) for name in columns})])
            messages = update.merge_into(data, commit_identifier=71, **kwargs) if stream else update.merge_into(
                data, **kwargs)
    _commit(builder, messages, stream)
    return messages


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
@pytest.mark.parametrize('operation', ['row_id', 'incremental', 'upsert', 'predicate', 'merge'])
@pytest.mark.parametrize('columns', [['score'], ['embedding'], ['score', 'embedding', 'payload']])
def test_parquet_vector_and_blob_partial_updates_use_native_core(
        tmp_path, native, stream, operation, columns):
    table, schema = _table(tmp_path, native)
    before = next(_rows(table))
    messages = _update(table, schema, native, stream, operation, columns, before)
    files = [file for message in messages for file in message.new_files]
    if native:
        assert any(file.file_name.endswith('.vector.parquet') for file in files) == ('embedding' in columns)
        assert any(file.file_name.endswith('.blob') for file in files) == ('payload' in columns)
        for file in files:
            if file.file_name.endswith('.vector.parquet'):
                assert file.write_cols == ['embedding']
                assert file.first_row_id is not None
    for rows in _rows(table):
        expected = []
        for original in before:
            row = dict(original)
            if row['id'] in (1, 3):
                for name, value in [('score', 42), ('embedding', [9.0, 8.0]), ('payload', b'new')]:
                    if name in columns:
                        row[name] = value
            expected.append(row)
        assert rows == expected


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
@pytest.mark.parametrize('native_read', [False, True], ids=['python-read', 'native-read'])
def test_shard_normal_updates_preserve_parquet_vector_and_blob_sidecars(tmp_path, native, stream, native_read):
    table, schema = _table(tmp_path, native)
    table = table.copy({'read.native.enabled': str(native_read).lower(), 'read.parallelism': '4'})
    before = next(_rows(table))
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    update = builder.new_update().with_update_type(['score'])
    update.with_read_projection(['id', 'score'])

    def reversed_groups(self, readers):
        # A valid completion order for parallel readers must not change which
        # row receives an update. Read actual files, but finish the last group first.
        for reader in reversed(readers):
            try:
                yield from reader
            finally:
                reader.close()

    with ExitStack() as stack:
        stack.enter_context(patch.object(TableRead, '_native_batches_parallel_streaming', reversed_groups))
        if native:
            stack.enter_context(patch('pypaimon.write.table_update.ShardTableUpdator.__init__',
                                      side_effect=AssertionError('Python shard fallback')))
        shard = update.new_shard_updator(0, 1)
        reader = shard.arrow_reader()
        try:
            for batch in reader:
                scores = pc.add(batch.column('score'), pa.scalar(10, type=pa.int32()))
                shard.update_by_arrow_batch(pa.RecordBatch.from_arrays([scores], names=['score']))
        finally:
            reader.close()
        messages = shard.prepare_commit()
    _commit(builder, messages, stream)
    for rows in _rows(table):
        assert rows == [dict(row, score=row['score'] + 10) for row in before]


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
def test_merge_reads_a_parquet_vector_source_in_native_core(tmp_path, native):
    table, schema = _table(tmp_path, native)
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_table('db.source', Schema.from_pyarrow_schema(schema, partition_keys=['pt'], options={
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'file.format': 'parquet', 'vector.file.format': 'parquet',
    }), False)
    source = catalog.get_table('db.source')
    seed = source.new_batch_write_builder()
    writer = seed.new_write()
    try:
        writer.write_arrow(pa.Table.from_pylist([
            dict(id=1, pt='a', score=42, embedding=[9.0, 8.0], payload=b'new'),
            dict(id=5, pt='b', score=50, embedding=[5.0, 6.0], payload=b'insert'),
        ], schema=schema))
        seed.new_commit().commit(writer.prepare_commit())
    finally:
        writer.close()
    before = next(_rows(table))
    builder = table.new_batch_write_builder()
    with _native_only(native):
        messages = builder.new_update().merge_into(
            source, on=['id', 'pt'],
            when_matched=[WhenMatched.update({name: source_col(name) for name in ('score', 'embedding', 'payload')})],
            when_not_matched=[WhenNotMatched('*')])
    _commit(builder, messages, False)
    for rows in _rows(table):
        assert len(rows) == 5
        assert rows[0] == dict(before[0], score=42, embedding=[9.0, 8.0], payload=b'new')
        assert rows[1:4] == before[1:]
        assert {k: v for k, v in rows[4].items() if k != '_ROW_ID'} == dict(
            id=5, pt='b', score=50, embedding=[5.0, 6.0], payload=b'insert')
