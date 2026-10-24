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

"""Writer executors share operations and keep mode-specific public boundaries."""

import inspect
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.execution.native.write.native_write import NativeTableWrite as NativeWrite
from pypaimon.execution.python.write.table_write import PythonTableWrite
from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
from pypaimon.write.table_write import BatchTableWrite, StreamTableWrite
from pypaimon.write.write_dispatch import (
    NativeBatchTableWrite, NativePostponeFixedBucketTableWrite,
    NativeStreamTableWrite, NativeTableWrite,
)

pytestmark = [pytest.mark.python_plan, pytest.mark.python_read,
              pytest.mark.python_write, pytest.mark.python_commit]


def _parameters(method):
    return [parameter.replace(annotation=inspect.Parameter.empty)
            for parameter in inspect.signature(method).parameters.values()]


@pytest.mark.parametrize('operation', [
    'write_arrow', 'write_arrow_batch', 'write_row', 'write_pandas',
    'with_write_type', 'with_blob_consumer', 'with_blob_uri_reader_factory',
    'roll_before_group_if_needed', 'prepare_commit', 'close', 'abort',
])
def test_bound_writer_executors_share_operation_parameters(operation):
    assert _parameters(getattr(PythonTableWrite, operation)) == _parameters(getattr(NativeWrite, operation))


@pytest.mark.parametrize('python, native', [
    (BatchTableWrite, NativeBatchTableWrite),
    (StreamTableWrite, NativeStreamTableWrite),
])
def test_public_writer_prepare_signatures_match(python, native):
    assert inspect.signature(python.prepare_commit) == inspect.signature(native.prepare_commit)
    assert inspect.signature(NativePostponeFixedBucketTableWrite.prepare_commit) == inspect.signature(
        BatchTableWrite.prepare_commit)


def _table(tmp_path):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.writer', Schema.from_pyarrow_schema(
        pa.schema([('id', pa.int32())]), options={
            'write.native.enabled': 'false', 'commit.native.enabled': 'false',
            'scan.native-plan.enabled': 'false', 'read.native.enabled': 'false',
        }), False)
    return catalog.get_table('default.writer')


@pytest.mark.parametrize('stream', [False, True])
def test_native_writer_factory_selects_a_concrete_mode_without_python_execution(tmp_path, stream):
    table = _table(tmp_path)
    core = Mock()
    with patch('pypaimon.execution.python.write.table_write.PythonTableWrite.__init__',
               side_effect=AssertionError('Native constructed Python execution')):
        writer = NativeTableWrite(table, 'writer', None, stream, core)
        assert type(writer) is (NativeStreamTableWrite if stream else NativeBatchTableWrite)
        assert isinstance(writer, NativeTableWrite)
        assert writer._python_writer is None
        writer.close()
    core.close.assert_called_once_with()


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('args, kwargs', [
    ((None,), {}), ((1,), {}), ((), {'commit_identifier': None}),
    ((), {'commit_identifier': BATCH_COMMIT_IDENTIFIER}), ((1, 2), {}),
])
def test_batch_prepare_rejects_every_argument_before_mutating_either_backend(
        tmp_path, native, args, kwargs):
    table = _table(tmp_path)
    core = Mock()
    writer = (NativeTableWrite(table, 'writer', None, False, core) if native else
              table.new_batch_write_builder().new_write())
    try:
        prepare = Mock(side_effect=AssertionError('invalid prepare started'))
        if native:
            core.prepare_commit = prepare
        else:
            writer._prepare_commit = prepare
        with pytest.raises(TypeError):
            writer.prepare_commit(*args, **kwargs)
        prepare.assert_not_called()
        assert not hasattr(writer, 'batch_committed')
        if native:
            assert writer._execution._batch_committed is False
        else:
            assert writer._batch_committed is False
    finally:
        writer.close()


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('args', [(), (None,), ('1',), (1.5,), (1 << 63,), (1, 2)])
def test_stream_prepare_requires_one_signed_identifier_before_io(tmp_path, native, args):
    table = _table(tmp_path)
    core = Mock()
    writer = (NativeTableWrite(table, 'writer', None, True, core) if native else
              table.new_stream_write_builder().new_write())
    try:
        with pytest.raises((TypeError, OverflowError)):
            writer.prepare_commit(*args)
        core.prepare_commit.assert_not_called()
        if not native:
            assert not writer._execution.file_store_write.data_writers
    finally:
        writer.close()


def test_bound_python_writer_uses_executor_helpers_and_live_context():
    batches = []
    original = pa.record_batch([pa.array([1], pa.int32())], names=['id'])
    replacement = pa.record_batch([pa.array([2], pa.int32())], names=['id'])
    owner = SimpleNamespace()

    class SelectingWriter(PythonTableWrite):
        def _prepare_arrow_schema(self, data):
            return self.data

    owner.write_arrow_batch = lambda batch, bucket=None: batches.append((batch, bucket))
    execution = SelectingWriter(owner)
    execution.data = original
    execution.write_arrow('first', 1)
    execution.data = replacement
    execution.write_arrow('second', 2)
    assert batches == [(original, 1), (replacement, 2)]
    assert execution.public_writer is owner
    assert not hasattr(owner, 'data')
    assert not hasattr(execution, 'writer')
    assert not hasattr(execution, '__getattr__')


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('identifier', [None, '1', 1.5, 1 << 63])
def test_internal_prepare_validates_the_identifier_before_file_preparation(native, identifier):
    source = Mock()
    if native:
        execution = object.__new__(NativeWrite)
        execution.stream = True
        execution._native_writer = source
    else:
        execution = PythonTableWrite(SimpleNamespace())
        execution.file_store_write = source
    with pytest.raises((TypeError, OverflowError)):
        execution.prepare_commit(identifier)
    source.prepare_commit.assert_not_called()
    source._prepare_commit_messages.assert_not_called()


@pytest.fixture
def row_table(tmp_path):
    schema = pa.schema([('pt', pa.int32()), ('id', pa.int32()),
                        pa.field('value', pa.int32(), nullable=False), ('unused', pa.large_binary())])
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.rows', Schema.from_pyarrow_schema(schema, partition_keys=['pt'], options={
        'bucket': '-1', 'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'ignore-update-before': 'true', 'write.native.enabled': 'false',
    }), False)
    return catalog.get_table('default.rows')


def _named_row(table, values, kind=None):
    from pypaimon.table.row.generic_row import GenericRow
    row = GenericRow(list(values.values()), [table.field_dict[name] for name in values])
    if kind is not None:
        row.row_kind = kind
    return row


def _selected_writer(table, columns=None):
    core = Mock(spec_set=['with_blob_uri_reader_factory', 'with_write_type', 'write_arrow', 'close'])
    writer = NativeWrite(table, 'rows', None, True, core)
    columns = columns if columns is not None else ['id']
    writer.with_write_type(columns)
    values = {'id': 111, 'pt': 1, 'value': 222}
    writer.write_row(_named_row(table, {name: values[name] for name in dict.fromkeys(columns + ['pt'])}))
    core.write_arrow.reset_mock()
    return writer, core


def test_native_full_named_row_encodes_selected_columns_and_leaves_unused_blob_untouched(row_table):
    from pypaimon.table.row.blob import Blob
    blob = Mock(spec=Blob)
    blob.new_input_stream.side_effect = AssertionError('unwritten Blob was opened')
    blob.to_descriptor.side_effect = AssertionError('unwritten Blob was encoded')
    writer, core = _selected_writer(row_table)
    try:
        writer.write_row(_named_row(row_table, {'unused': blob, 'value': 333, 'pt': 1, 'id': 222}))
        core.write_arrow.assert_called_once()
        payload = core.write_arrow.call_args[0][0]
        assert payload.schema.names == ['id', 'pt']
        assert payload.to_pydict() == {'id': [222], 'pt': [1]}
        blob.new_input_stream.assert_not_called()
        blob.to_descriptor.assert_not_called()
        assert writer._blob_rows.readers == {}
    finally:
        writer.close()


def test_native_partial_named_row_omits_unselected_non_null_payload(row_table):
    writer, core = _selected_writer(row_table)
    try:
        writer.write_row(_named_row(row_table, {'id': 222, 'pt': 1}))
        payload = core.write_arrow.call_args[0][0]
        assert payload.schema.names == ['id', 'pt']
        assert payload.to_pydict() == {'id': [222], 'pt': [1]}
    finally:
        writer.close()


def test_native_type_change_rejected_by_core_preserves_adapter_configuration(row_table):
    writer, core = _selected_writer(row_table)
    try:
        failure = ValueError('with_write_type must be called before any write operation')
        core.with_write_type.side_effect = failure
        with pytest.raises(RuntimeError, match='before any write') as caught:
            writer.with_write_type(['value'])
        assert caught.value.__cause__ is failure
        assert writer._schema.names == writer._write_cols == ['id']
        writer.write_row(_named_row(row_table, {'pt': 1, 'id': 222}))
        assert core.write_arrow.call_args[0][0].to_pydict() == {'id': [222], 'pt': [1]}
    finally:
        writer.close()


@pytest.mark.parametrize('columns', [['id', 'pt'], ['pt', 'id']])
def test_native_named_row_preserves_selected_column_order(row_table, columns):
    writer, core = _selected_writer(row_table, columns)
    try:
        writer.write_row(_named_row(row_table, {'pt': 1, 'id': 222}))
        payload = core.write_arrow.call_args[0][0]
        assert payload.schema.names == columns
        assert payload.to_pydict() == {'id': [222], 'pt': [1]}
    finally:
        writer.close()


def test_native_selected_not_null_validation_precedes_encoding(row_table):
    writer, core = _selected_writer(row_table, ['value'])
    try:
        with patch.object(writer._blob_rows, 'to_batch', side_effect=AssertionError('payload was encoded')):
            with pytest.raises(ValueError, match='value'):
                writer.write_row(_named_row(row_table, {'pt': 1, 'id': 222, 'value': None, 'unused': None}))
        core.write_arrow.assert_not_called()
    finally:
        writer.close()


def test_native_row_missing_selected_value_fails_before_encoding_or_staging(row_table):
    writer, core = _selected_writer(row_table)
    try:
        with patch.object(writer._blob_rows, 'to_batch', side_effect=AssertionError('payload was encoded')):
            with pytest.raises(ValueError, match='id'):
                writer.write_row(_named_row(row_table, {'pt': 1, 'value': 333}))
        core.write_arrow.assert_not_called()
    finally:
        writer.close()


def test_native_ignored_row_does_not_encode_or_stage_payloads(row_table):
    from pypaimon.table.row.row_kind import RowKind
    writer, core = _selected_writer(row_table)
    try:
        with patch.object(writer._blob_rows, 'to_batch', side_effect=AssertionError('ignored payload was encoded')):
            writer.write_row(_named_row(row_table, {'pt': 1, 'id': 222, 'value': 333}, RowKind.UPDATE_BEFORE))
        core.write_arrow.assert_not_called()
    finally:
        writer.close()


def test_native_unpartitioned_rows_only_call_the_write_interface(tmp_path):
    table = _table(tmp_path)
    core = Mock(spec_set=['with_write_type', 'write_arrow', 'close'])
    writer = NativeWrite(table, 'rows', None, True, core)
    try:
        writer.with_write_type(['id'])
        writer.write_row(_named_row(table, {'id': 1}))
        writer.write_row(_named_row(table, {'id': 2}))
        assert core.write_arrow.call_count == 2
        assert core.write_arrow.call_args[0][0].to_pydict() == {'id': [2]}
    finally:
        writer.close()


@pytest.mark.parametrize('postpone', [False, True])
@pytest.mark.parametrize('input_', ['table', 'batch', 'row', 'pandas'])
def test_python_executor_writes_without_private_writer_hooks(tmp_path, postpone, input_):
    """Routing and prepare stay in the executor, including postpone overrides."""
    from pypaimon.execution.python.write.postpone_batch_table_write import PythonPostponeFixedBucketWrite
    from pypaimon.table.row.generic_row import GenericRow

    schema = pa.schema([
        pa.field('id', pa.int32(), nullable=False),
        pa.field('pt', pa.string(), nullable=False),
        ('value', pa.int32()),
    ])
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.input', Schema.from_pyarrow_schema(
        schema, primary_keys=['id', 'pt'] if postpone else [], partition_keys=['pt'], options={
            'bucket': '-2' if postpone else '1',
            'postpone.target-row-num-per-bucket': '1',
            'write.native.enabled': 'false', 'commit.native.enabled': 'false',
            'scan.native-plan.enabled': 'false', 'read.native.enabled': 'false',
        }), False)
    table = catalog.get_table('default.input')
    builder = table.new_batch_write_builder()
    # The context exposes only public batch delegation, with no private schema
    # or routing hooks from the public writer implementation.
    owner = SimpleNamespace()
    execution = (PythonPostponeFixedBucketWrite(owner) if postpone else PythonTableWrite(owner))
    owner.write_arrow_batch = execution.write_arrow_batch
    execution.initialize(table, builder.commit_user)
    assert execution.table is table
    for attribute in ('file_store_write', 'row_key_extractor', 'table_pyarrow_schema', 'restore_snapshot_id'):
        assert not hasattr(owner, attribute)
    commit = builder.new_commit()
    rows = [{'id': 1, 'pt': 'a', 'value': 10}, {'id': 2, 'pt': 'b', 'value': 20}]
    data = pa.Table.from_pylist(rows, schema=schema)
    try:
        if input_ == 'table':
            execution.write_arrow(data)
        elif input_ == 'batch':
            execution.write_arrow_batch(data.to_batches()[0])
        elif input_ == 'row':
            for row in rows:
                execution.write_row(GenericRow([row[name] for name in schema.names], table.fields))
        else:
            execution.write_pandas(data.to_pandas())
        messages = execution.prepare_commit(BATCH_COMMIT_IDENTIFIER)
        assert len(messages) == 2
        assert {tuple(message.partition) for message in messages} == {('a',), ('b',)}
        assert all(message.bucket == 0 for message in messages)
        commit.commit(messages)
    finally:
        execution.close()
        commit.close()
    read = table.new_read_builder()
    actual = read.new_read().to_arrow(read.new_scan().plan().splits()).to_pylist()
    assert sorted(actual, key=lambda row: row['id']) == rows
