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

"""Pending files carry source events, not merged primary-key rows."""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pypaimon import CatalogFactory, Schema


def make_table(tmp_path, options=None, partition_keys=None):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    settings = {'bucket': '-2', 'file.format': 'parquet', 'write.native.enabled': 'false'}
    settings.update(options or {})
    schema = pa.schema([('id', pa.int32()), ('v', pa.int32()), ('op', pa.string())])
    catalog.create_table('default.events', Schema.from_pyarrow_schema(
        schema, primary_keys=['id'] + (partition_keys or []),
        partition_keys=partition_keys or [], options=settings), False)
    return catalog.get_table('default.events'), schema


def stored_rows(table, messages):
    rows = []
    for message in messages:
        for file in message.new_files:
            with table.file_io.new_input_stream(file.file_path) as source:
                rows.extend(pq.ParquetFile(source).read().to_pylist())
    return rows


@pytest.mark.parametrize('engine', ['deduplicate', 'first-row', 'partial-update', 'aggregation'])
def test_postpone_preserves_unsorted_duplicate_rows(tmp_path, engine):
    table, schema = make_table(tmp_path, {'merge-engine': engine})
    writer = table.new_batch_write_builder().new_write()
    try:
        writer.write_arrow(pa.table({'id': [3, 1, 3], 'v': [10, 20, 30],
                                    'op': ['+I'] * 3}, schema=schema))
        messages = writer.prepare_commit()
        assert [(row['id'], row['v']) for row in stored_rows(table, messages)] == [(3, 10), (1, 20), (3, 30)]
    finally:
        writer.close()


@pytest.mark.parametrize('row_input', [False, True])
@pytest.mark.parametrize('options,expected', [
    ({}, [0, 1, 2, 3]),
    ({'ignore-delete': 'true'}, [0, 2]),
    ({'ignore-update-before': 'true'}, [0, 2, 3]),
])
def test_python_postpone_row_kinds_are_filtered_before_writing(tmp_path, row_input, options, expected):
    from pypaimon.table.row.generic_row import GenericRow
    table, schema = make_table(tmp_path, {'rowkind.field': 'op', **options})
    writer = table.new_batch_write_builder().new_write()
    rows = [{'id': i, 'v': i, 'op': kind} for i, kind in enumerate(['+I', '-U', '+U', '-D'])]
    try:
        if row_input:
            for row in rows:
                writer.write_row(GenericRow(list(row.values()), table.fields))
        else:
            writer.write_arrow(pa.Table.from_pylist(rows, schema=schema))
        messages = writer.prepare_commit()
        physical = stored_rows(table, messages)
        assert [row['id'] for row in physical] == expected
        assert [row['_VALUE_KIND'] for row in physical] == expected
        assert sum(f.delete_row_count for m in messages for f in m.new_files) == sum(i in (1, 3) for i in expected)
    finally:
        writer.close()


@pytest.mark.parametrize('engine', ['first-row', 'partial-update', 'aggregation'])
def test_python_postpone_rejects_unsupported_retract_and_aborts_rolled_files(tmp_path, engine):
    table, schema = make_table(tmp_path, {
        'merge-engine': engine, 'rowkind.field': 'op', 'target-file-row-num': '1'})
    writer = table.new_stream_write_builder().new_write()
    try:
        writer.write_arrow(pa.table({'id': [1], 'v': [1], 'op': ['+I']}, schema=schema))
        assert list(tmp_path.rglob('*.parquet'))
        with pytest.raises((ValueError, NotImplementedError), match='(?i)retract|DELETE|non-INSERT'):
            writer.write_arrow(pa.table({'id': [1], 'v': [1], 'op': ['-D']}, schema=schema))
        assert not list(tmp_path.rglob('*.parquet'))
        with pytest.raises(RuntimeError, match='failed'):
            writer.prepare_commit(1)
    finally:
        writer.close()


def test_python_postpone_checkpoints_do_not_repeat_prepared_files(tmp_path):
    table, schema = make_table(tmp_path, {'target-file-row-num': '2', 'rowkind.field': 'op'})
    writer = table.new_stream_write_builder().new_write()
    try:
        for checkpoint in range(3):
            writer.write_arrow(pa.table({'id': [3, 1, 3], 'v': [10, 20, 30],
                                        'op': ['+I', '-U', '-D']}, schema=schema))
            messages = writer.prepare_commit(checkpoint)
            rows = stored_rows(table, messages)
            assert [row['id'] for row in rows] == [3, 1, 3]
            assert [row['_SEQUENCE_NUMBER'] for row in rows] == list(range(checkpoint * 3, checkpoint * 3 + 3))
            assert [file.row_count for message in messages for file in message.new_files] == [3]
            assert writer.prepare_commit(checkpoint + 100) == []
        paths = [f.file_path for m in messages for f in m.new_files]
        writer.abort()
        assert all(table.file_io.exists(path) for path in paths)
    finally:
        writer.close()


def test_python_aggregation_writer_uses_field_aggregators(tmp_path):
    table, schema = make_table(tmp_path, {
        'bucket': '1', 'merge-engine': 'aggregation', 'fields.v.aggregate-function': 'sum'})
    writer = table.new_batch_write_builder().new_write()
    try:
        writer.write_arrow(pa.table({'id': [3, 1, 3], 'v': [10, 20, 30], 'op': ['+I'] * 3}, schema=schema))
        rows = stored_rows(table, writer.prepare_commit())
        assert [(row['id'], row['v']) for row in rows] == [(1, 20), (3, 40)]
    finally:
        writer.close()


def test_python_postpone_prepare_failure_keeps_earlier_partition_owned(tmp_path, monkeypatch):
    table, schema = make_table(tmp_path, partition_keys=['v'])
    writer = table.new_stream_write_builder().new_write()
    writer.write_arrow(pa.table({'id': [1, 2], 'v': [10, 20], 'op': ['+I', '+I']}, schema=schema))
    writers = list(writer.file_store_write.data_writers.values())
    assert len(writers) == 2

    def fail_prepare():
        raise OSError('injected prepare failure')

    monkeypatch.setattr(writers[1], 'prepare_commit', fail_prepare)
    try:
        with pytest.raises(OSError, match='injected'):
            writer.prepare_commit(1)
        assert list(tmp_path.rglob('*.parquet'))
    finally:
        writer.close()
    assert not list(tmp_path.rglob('*.parquet'))
