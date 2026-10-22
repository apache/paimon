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

"""A writer uses one ordered write type after accepting its first row."""

import io
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


def _table(tmp_path, native, precomputed=False, unwritten_not_null=False, blob=False):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    fields = [pa.field('id', pa.int32()), pa.field('value', pa.int32())]
    if unwritten_not_null:
        fields.append(pa.field('omitted', pa.int32(), nullable=False))
    if blob:
        fields.append(pa.field('payload', pa.large_binary()))
    schema = pa.schema(fields + [pa.field('pt', pa.string())])
    options = {'write.native.enabled': str(native).lower(), 'commit.native.enabled': 'false',
               'scan.native-plan.enabled': 'false', 'read.native.enabled': 'false'}
    if precomputed:
        options.update({'bucket': '2', 'bucket-key': 'id'})
    if blob:
        options.update({'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'})
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
@pytest.mark.parametrize('stage', ['write', 'checkpoint', 'idle'])
@pytest.mark.parametrize('initial_full', [False, True], ids=['subset', 'full'])
def test_write_type_is_fixed_after_first_write_even_after_idle_retirement(
        tmp_path, native, mode, stage, initial_full):
    table = _table(tmp_path, native, mode == 'precomputed')
    builder = table.new_stream_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    columns = table.field_names if initial_full else ['id', 'pt']
    try:
        if not initial_full:
            writer.with_write_type(columns)
        _write(writer, table, [{'id': 1, 'value': 10, 'pt': 'old'}], mode)
        if stage != 'write':
            commit.commit(writer.prepare_commit(1), 1)
        if stage == 'idle':
            assert writer.prepare_commit(2) == []
        assert writer.with_write_type(list(columns)) is writer
        # Column order is part of the type, even when the field set is equal.
        for different in (list(reversed(columns)), ['value', 'pt']):
            with pytest.raises(RuntimeError, match='before any write'):
                writer.with_write_type(different)
        _write(writer, table, [{'id': 2, 'value': 20, 'pt': 'old'},
                               {'id': 3, 'value': 30, 'pt': 'new'}], mode)
        messages = writer.prepare_commit(3)
        assert all(file.write_cols == (None if initial_full else columns)
                   for message in messages for file in message.new_files)
        commit.commit(messages, 3)
    finally:
        writer.close()
        commit.close()
    assert _read(table) == [
        {'id': 3, 'value': 30 if initial_full else None, 'pt': 'new'},
        {'id': 1, 'value': 10 if initial_full else None, 'pt': 'old'},
        {'id': 2, 'value': 20 if initial_full else None, 'pt': 'old'},
    ]


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('input_', ['row', 'batch', 'table', 'pandas'])
@pytest.mark.parametrize('kind', [None, '-D', '-U'], ids=['empty', 'delete', 'update-before'])
def test_empty_or_ignored_input_does_not_fix_write_type(tmp_path, native, input_, kind):
    from pypaimon.tests.write.append_row_kind_test import _table as event_table, _write as write_event
    table, schema = event_table(tmp_path, native, **{'ignore-delete': 'true'})
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.with_write_type(['id', 'pt'])
        values = [] if kind is None else [dict(id=1, op=kind, pt='ignored')]
        if kind is None and input_ == 'batch':
            writer.write_arrow_batch(pa.RecordBatch.from_arrays(
                [pa.array([], type=field.type) for field in schema], schema=schema))
        else:
            write_event(writer, table, schema, values, input_, True)
        assert writer.with_write_type(['op', 'pt']) is writer
        write_event(writer, table, schema, [dict(id=2, op='+I', pt='kept')], input_, True)
        assert writer.with_write_type(['op', 'pt']) is writer
        with pytest.raises(RuntimeError, match='before any write'):
            writer.with_write_type(['id', 'pt'])
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    read = table.new_read_builder()
    assert read.new_read().to_arrow(read.new_scan().plan().splits()).to_pylist() == [
        dict(id=None, op='+I', pt='kept')]


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('mode', ['arrow', 'row'])
def test_different_write_types_use_different_writers(tmp_path, native, mode):
    table = _table(tmp_path, native)
    builder = table.new_batch_write_builder()
    left = builder.new_write().with_write_type(['id', 'pt'])
    right = builder.new_write().with_write_type(['value', 'pt'])
    commit = builder.new_commit()
    try:
        _write(left, table, [{'id': 1, 'value': 10, 'pt': 'old'}], mode)
        _write(right, table, [{'id': 2, 'value': 20, 'pt': 'old'}], mode)
        commit.commit(left.prepare_commit() + right.prepare_commit())
    finally:
        left.close()
        right.close()
        commit.close()
    assert _read(table) == [
        {'id': None, 'value': 20, 'pt': 'old'}, {'id': 1, 'value': None, 'pt': 'old'}]


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
def test_incompatible_batch_is_rejected_before_any_partition_is_staged(tmp_path, native):
    table = _table(tmp_path, native)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.with_write_type(['id', 'pt'])
        _write(writer, table, [{'id': 1, 'value': 10, 'pt': 'old'}])
        before = set(tmp_path.rglob('*.parquet'))
        with pytest.raises(ValueError):
            _write(writer, table, [{'value': 20, 'pt': 'new'}, {'value': 30, 'pt': 'old'}],
                   columns=['value', 'pt'])
        assert set(tmp_path.rglob('*.parquet')) == before
        _write(writer, table, [{'id': 2, 'value': 20, 'pt': 'old'},
                               {'id': 3, 'value': 30, 'pt': 'new'}])
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    assert _read(table) == [
        {'id': 3, 'value': None, 'pt': 'new'},
        {'id': 1, 'value': None, 'pt': 'old'}, {'id': 2, 'value': None, 'pt': 'old'}]


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
def test_reordered_named_rows_keep_the_selected_column_order(tmp_path, native):
    table = _table(tmp_path, native, unwritten_not_null=True)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.with_write_type(['pt', 'id'])
        _write(writer, table, [{'id': 1, 'value': 10, 'pt': 'old', 'omitted': None}],
               'row', ['pt', 'value', 'id', 'omitted'])
        _write(writer, table, [{'id': 2, 'value': 20, 'pt': 'old', 'omitted': None}],
               'row', ['value', 'id', 'pt', 'omitted'])
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    assert _read(table) == [
        {'id': 1, 'value': None, 'pt': 'old', 'omitted': None},
        {'id': 2, 'value': None, 'pt': 'old', 'omitted': None}]


def test_failed_bucket_creation_does_not_fix_write_type(tmp_path):
    table = _table(tmp_path, False)
    writer = table.new_batch_write_builder().new_write().with_write_type(['value', 'pt'])
    try:
        store = writer.file_store_write
        with patch.object(store, '_create_data_writer', side_effect=OSError('cannot create writer')):
            with pytest.raises(OSError, match='cannot create writer'):
                _write(writer, table, [{'id': 1, 'value': 10, 'pt': 'old'}])
        assert store.data_writers == {}
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
            assert writer.with_write_type(['pt', 'id']) is writer
            _write(writer, table, [{'id': 2, 'value': 20, 'pt': 'old'}])
        assert [call[0][0].to_pydict() for call in optimized.write.call_args_list] == [
            {'pt': ['old'], 'id': [1]}, {'pt': ['old'], 'id': [2]}]
        store.data_writers.clear()
    finally:
        writer.close()


@pytest.mark.parametrize('backend', ['python', 'native-mock'])
@pytest.mark.parametrize('blob_input', ['none', 'omitted', 'selected'])
def test_first_row_encoding_failure_does_not_fix_write_type(tmp_path, backend, blob_input):
    from pypaimon.execution.native.write.native_write import NativeTableWrite as NativeWrite
    from pypaimon.table.row.blob import Blob

    table = _table(tmp_path, False, blob=blob_input != 'none')
    builder = table.new_batch_write_builder()
    core = Mock(spec_set=['with_blob_uri_reader_factory', 'with_write_type', 'write_arrow', 'close'])
    writer = builder.new_write() if backend == 'python' else NativeWrite(table, 'job', None, False, core)
    payload = Mock(spec=Blob)
    payload.new_input_stream.side_effect = AssertionError('unused Blob was opened')
    payload.to_descriptor.side_effect = AssertionError('unused Blob was encoded')
    columns = ['id', 'pt'] if blob_input != 'selected' else ['id', 'payload', 'pt']
    values = {'id': 'invalid-int', 'pt': 'old'}
    if blob_input != 'none':
        values['payload'] = payload
    try:
        writer.with_write_type(columns)
        with pytest.raises(pa.ArrowInvalid):
            writer.write_row(GenericRow(list(values.values()), [table.field_dict[name] for name in values]))
        assert not list(tmp_path.rglob('*.parquet')) and not list(tmp_path.rglob('*.blob'))
        if backend == 'python':
            assert writer.file_store_write.data_writers == {}
            assert not writer.file_store_write._ever_written
        else:
            core.write_arrow.assert_not_called()
            assert not writer._written
        assert writer.with_write_type(['value', 'pt']) is writer
        values['id'] = 1
        values['value'] = 20
        writer.write_row(GenericRow(list(values.values()), [table.field_dict[name] for name in values]))
        if backend == 'python':
            commit = builder.new_commit()
            try:
                commit.commit(writer.prepare_commit())
            finally:
                commit.close()
            expected = {'id': None, 'value': 20, 'pt': 'old'}
            if blob_input != 'none':
                expected['payload'] = None
            assert _read(table) == [expected]
        else:
            core.write_arrow.assert_called_once()
            assert core.write_arrow.call_args[0][0].to_pydict() == {'value': [20], 'pt': ['old']}
        payload.new_input_stream.assert_not_called()
        payload.to_descriptor.assert_not_called()
    finally:
        writer.close()


def test_blob_row_encoding_retry_consumes_the_selected_live_blob_once(tmp_path):
    from pypaimon.table.row.blob import Blob

    class InputBlob(Blob):
        opened = 0

        def to_data(self):
            raise AssertionError('live Blob was materialized')

        def to_descriptor(self):
            raise AssertionError('live Blob was encoded')

        def new_input_stream(self):
            self.opened += 1
            return io.BytesIO(b'payload')

    table = _table(tmp_path, False, blob=True)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    payload = InputBlob()
    try:
        writer.with_write_type(['id', 'payload', 'pt'])
        with pytest.raises(pa.ArrowInvalid):
            writer.write_row(GenericRow(['invalid-int', payload, 'old'],
                                        [table.field_dict[name] for name in ['id', 'payload', 'pt']]))
        assert payload.opened == 0
        writer.with_write_type(['value', 'payload', 'pt'])
        writer.write_row(GenericRow([20, payload, 'old'],
                                    [table.field_dict[name] for name in ['value', 'payload', 'pt']]))
        assert payload.opened == 1
        commit.commit(writer.prepare_commit())
        assert payload.opened == 1
    finally:
        writer.close()
        commit.close()
    assert _read(table) == [dict(id=None, value=20, payload=b'payload', pt='old')]


def test_failure_after_row_encoding_keeps_the_created_write_type_fixed(tmp_path):
    from pypaimon.execution.python.write.writer.append_only_data_writer import AppendOnlyDataWriter

    table = _table(tmp_path, False)
    writer = table.new_batch_write_builder().new_write().with_write_type(['id', 'pt'])
    failure = OSError('writer failed after row encoding')
    try:
        with patch.object(AppendOnlyDataWriter, 'write', side_effect=failure):
            with pytest.raises(OSError) as caught:
                writer.write_row(GenericRow([1, 'old'], [table.field_dict[name] for name in ['id', 'pt']]))
        assert caught.value is failure
        assert writer.with_write_type(['id', 'pt']) is writer
        with pytest.raises(RuntimeError, match='before any write'):
            writer.with_write_type(['value', 'pt'])
    finally:
        writer.close()
