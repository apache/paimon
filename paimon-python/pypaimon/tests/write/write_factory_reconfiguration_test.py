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

"""Writer-factory changes apply to future buckets, as in Java."""

from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.execution.native.runtime import native_runtime_available
from pypaimon.table.row.generic_row import GenericRow

pytestmark = [pytest.mark.python_plan, pytest.mark.python_read,
              pytest.mark.python_write, pytest.mark.python_commit]
_ENGINES = [False, pytest.param(True, marks=[pytest.mark.native_plan, pytest.mark.skipif(
    not native_runtime_available(), reason='pypaimon-rust not installed')])]


def _table(tmp_path, native, precomputed=False, unwritten_not_null=False):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    fields = [pa.field('id', pa.int32()), pa.field('value', pa.int32())]
    if unwritten_not_null:
        fields.append(pa.field('omitted', pa.int32(), nullable=False))
    schema = pa.schema(fields + [pa.field('pt', pa.string())])
    options = {'write.native.enabled': str(native).lower(), 'commit.native.enabled': 'false',
               'scan.native-plan.enabled': 'false', 'read.native.enabled': 'false'}
    if precomputed:
        options.update({'bucket': '2', 'bucket-key': 'id'})
    catalog.create_table('default.factory', Schema.from_pyarrow_schema(
        schema, partition_keys=['pt'], options=options), False)
    return catalog.get_table('default.factory')


def _write(writer, table, rows, mode='arrow', columns=None):
    columns = columns if columns is not None else table.field_names
    schema = pa.schema([writer.table_pyarrow_schema.field(name) for name in columns])
    data = pa.Table.from_pylist([{name: row[name] for name in columns} for row in rows], schema=schema)
    if mode == 'row':
        fields = [table.field_dict[name] for name in columns]
        for row in rows:
            writer.write_row(GenericRow([row[name] for name in columns], fields))
    elif mode == 'precomputed':
        for index in range(len(rows)):
            writer.write_arrow(data.slice(index, 1), bucket=0)
    else:
        writer.write_arrow(data)


def _read(table):
    builder = table.new_read_builder()
    rows = builder.new_read().to_arrow(builder.new_scan().plan().splits()).to_pylist()
    return sorted(rows, key=lambda row: (row['pt'], row['id'] or -1, row['value'] or -1))


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('mode', ['arrow', 'row', 'precomputed'])
@pytest.mark.parametrize('initial_full', [False, True], ids=['subset', 'full'])
def test_existing_bucket_keeps_its_write_type_across_factory_changes_and_prepare(
        tmp_path, native, mode, initial_full):
    table = _table(tmp_path, native, mode == 'precomputed')
    builder = table.new_stream_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        if not initial_full:
            writer.with_write_type(['id', 'pt'])
        _write(writer, table, [{'id': 1, 'value': 10, 'pt': 'old'}], mode)
        commit.commit(writer.prepare_commit(1), 1)
        writer.with_write_type(['value', 'pt'])
        _write(writer, table, [{'id': 2, 'value': 20, 'pt': 'old'},
                               {'id': 3, 'value': 30, 'pt': 'new'}], mode)
        messages = writer.prepare_commit(2)
        assert {message.partition: [file.write_cols for file in message.new_files]
                for message in messages} == {
            ('old',): [None if initial_full else ['id', 'pt']],
            ('new',): [['value', 'pt']],
        }
        commit.commit(messages, 2)
    finally:
        writer.close()
        commit.close()
    assert _read(table) == [
        {'id': None, 'value': 30, 'pt': 'new'},
        {'id': 1, 'value': 10 if initial_full else None, 'pt': 'old'},
        {'id': 2, 'value': 20 if initial_full else None, 'pt': 'old'},
    ]


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('mode', ['arrow', 'row'])
def test_historical_subset_is_valid_only_for_its_existing_bucket(tmp_path, native, mode):
    table = _table(tmp_path, native)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.with_write_type(['id', 'pt'])
        _write(writer, table, [{'id': 1, 'value': 10, 'pt': 'old'}], mode)
        writer.with_write_type(['value', 'pt'])
        _write(writer, table, [{'id': 2, 'pt': 'old'}], mode, ['id', 'pt'])
        _write(writer, table, [{'value': 30, 'pt': 'new'}], mode, ['value', 'pt'])
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    assert _read(table) == [
        {'id': None, 'value': 30, 'pt': 'new'},
        {'id': 1, 'value': None, 'pt': 'old'},
        {'id': 2, 'value': None, 'pt': 'old'},
    ]


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('transport', ['current', 'historical'])
def test_incompatible_mixed_batch_is_rejected_before_any_group_is_staged(tmp_path, native, transport):
    table = _table(tmp_path, native)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.with_write_type(['id', 'pt'])
        _write(writer, table, [{'id': 1, 'value': 10, 'pt': 'old'}])
        writer.with_write_type(['value', 'pt'])
        if transport == 'current':
            rows, columns = [{'value': 20, 'pt': 'new'}, {'value': 30, 'pt': 'old'}], ['value', 'pt']
        else:
            rows, columns = [{'id': 2, 'pt': 'old'}, {'id': 3, 'pt': 'new'}], ['id', 'pt']
        before = set(tmp_path.rglob('*.parquet'))
        with pytest.raises((ValueError, RuntimeError)):
            _write(writer, table, rows, columns=columns)
        assert set(tmp_path.rglob('*.parquet')) == before
        if not native:
            assert set(writer.file_store_write.data_writers) == {(('old',), 0)}
        # A failed batch must retain earlier pending rows and admit a valid one.
        _write(writer, table, [{'id': 4, 'value': 40, 'pt': 'old'},
                               {'id': 5, 'value': 50, 'pt': 'new'}])
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    assert _read(table) == [
        {'id': None, 'value': 50, 'pt': 'new'},
        {'id': 1, 'value': None, 'pt': 'old'},
        {'id': 4, 'value': None, 'pt': 'old'},
    ]


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
def test_reordered_full_named_rows_keep_the_old_bucket_column_names(tmp_path, native):
    table = _table(tmp_path, native, unwritten_not_null=True)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.with_write_type(['pt', 'id'])
        _write(writer, table, [{'id': 1, 'value': 10, 'pt': 'old', 'omitted': None}],
               'row', ['pt', 'value', 'id', 'omitted'])
        writer.with_write_type(['pt', 'value'])
        _write(writer, table, [{'id': 2, 'value': 20, 'pt': 'old', 'omitted': None},
                               {'id': 3, 'value': 30, 'pt': 'new', 'omitted': None}],
               'row', ['value', 'id', 'pt', 'omitted'])
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    assert _read(table) == [
        {'id': None, 'value': 30, 'pt': 'new', 'omitted': None},
        {'id': 1, 'value': None, 'pt': 'old', 'omitted': None},
        {'id': 2, 'value': None, 'pt': 'old', 'omitted': None},
    ]


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
def test_row_missing_an_old_bucket_column_cannot_reinterpret_current_factory_values(tmp_path, native):
    table = _table(tmp_path, native)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.with_write_type(['id', 'pt'])
        _write(writer, table, [{'id': 1, 'value': 10, 'pt': 'old'}], 'row')
        writer.with_write_type(['value', 'pt'])
        with pytest.raises((ValueError, RuntimeError)):
            _write(writer, table, [{'value': 99, 'pt': 'old'}], 'row', ['value', 'pt'])
        _write(writer, table, [{'id': 2, 'value': 20, 'pt': 'old'}], 'row')
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    assert _read(table) == [
        {'id': 1, 'value': None, 'pt': 'old'},
        {'id': 2, 'value': None, 'pt': 'old'},
    ]


def test_failed_bucket_creation_does_not_capture_a_factory_layout(tmp_path):
    table = _table(tmp_path, False)
    writer = table.new_batch_write_builder().new_write().with_write_type(['value', 'pt'])
    try:
        store = writer.file_store_write
        with patch.object(store, '_create_data_writer', side_effect=OSError('cannot create writer')):
            with pytest.raises(OSError, match='cannot create writer'):
                _write(writer, table, [{'id': 1, 'value': 10, 'pt': 'old'}])
        assert store.data_writers == {} and store._write_columns_by_bucket == {}
        writer.with_write_type(['id', 'pt'])
        _write(writer, table, [{'id': 2, 'value': 20, 'pt': 'old'}])
        messages = writer.prepare_commit()
        assert [file.write_cols for message in messages for file in message.new_files] == [['id', 'pt']]
    finally:
        writer.close()


def test_requested_write_columns_are_distinct_from_optimized_physical_metadata(tmp_path):
    table = _table(tmp_path, False)
    writer = table.new_batch_write_builder().new_write().with_write_type(['pt', 'id'])
    try:
        store = writer.file_store_write
        optimized = Mock(write_cols=['id'])
        with patch.object(store, '_create_data_writer', return_value=optimized):
            _write(writer, table, [{'id': 1, 'value': 10, 'pt': 'old'}])
            writer.with_write_type(['pt', 'value'])
            _write(writer, table, [{'id': 2, 'value': 20, 'pt': 'old'}])
        assert store._write_columns_by_bucket[(('old',), 0)] == ('pt', 'id')
        assert [call[0][0].to_pydict() for call in optimized.write.call_args_list] == [
            {'pt': ['old'], 'id': [1]}, {'pt': ['old'], 'id': [2]}]
        store.data_writers.clear()
    finally:
        writer.close()
