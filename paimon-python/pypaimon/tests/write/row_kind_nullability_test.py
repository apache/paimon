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

"""Filtered events retain Java's row-kind-specific top-level NULL checks."""

from types import SimpleNamespace
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.read.native_plan import native_runtime_available
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.row.row_kind import RowKind
from pypaimon.write.native_write import NativeTableWrite
from pypaimon.write.row_kind import skip_write_row


pytestmark = [pytest.mark.python_write, pytest.mark.python_commit]
_NATIVE = pytest.mark.skipif(not native_runtime_available(), reason='pypaimon-rust not installed')
_ENGINES = [False, pytest.param(True, marks=[pytest.mark.native_plan, _NATIVE])]


def _table(tmp_path, **options):
    schema = pa.schema([pa.field('id', pa.int32(), nullable=False),
                        pa.field('op', pa.string()),
                        pa.field('seq', pa.int64(), nullable=False),
                        pa.field('value', pa.int32(), nullable=False)])
    config = {'bucket': '1', 'rowkind.field': 'op', 'ignore-delete': 'true',
              'sequence.field': 'seq', 'write.native.enabled': 'false',
              'commit.native.enabled': 'false', 'read.native.enabled': 'false',
              'scan.native-plan.enabled': 'false'}
    config.update(options)
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        schema, primary_keys=['id'], options=config), False)
    return catalog.get_table('default.t'), schema


def _write(writer, table, schema, rows, input_):
    if input_ == 'row':
        for row in rows:
            writer.write_row(GenericRow([row[field.name] for field in table.fields], table.fields))
    elif input_ == 'batch':
        writer.write_arrow_batch(pa.RecordBatch.from_pylist(rows, schema=schema))
    else:
        writer.write_arrow(pa.Table.from_pylist(rows, schema=schema))


@pytest.mark.parametrize('input_', ['row', 'batch', 'table'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
@pytest.mark.parametrize('kind,field', [('-D', 'id'), ('-D', 'seq'), ('-U', 'value'), ('+I', 'value')])
def test_filtered_events_check_required_nulls_before_routing(tmp_path, input_, stream, kind, field):
    table, schema = _table(tmp_path)
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    writer = builder.new_write()
    invalid = dict(id=1, op=kind, seq=1, value=10)
    invalid[field] = None
    try:
        with pytest.raises(ValueError, match=r'non-null column\(%s\)' % field), \
                patch.object(writer.row_key_extractor, 'extract_partition_bucket_groups',
                             side_effect=AssertionError('invalid event reached bucket routing')):
            _write(writer, table, schema, [invalid], input_)
        assert not writer.file_store_write.data_writers
    finally:
        writer.close()


@pytest.mark.parametrize('input_', ['row', 'batch', 'table'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
def test_ignored_delete_may_omit_non_key_not_null_values(tmp_path, input_, stream, native):
    table, schema = _table(tmp_path, **{'write.native.enabled': str(native).lower()})
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    expected = dict(id=2, op='+I', seq=2, value=42)
    try:
        assert isinstance(writer, NativeTableWrite) == native
        _write(writer, table, schema, [dict(id=1, op='-D', seq=1, value=None), expected], input_)
        if native:
            assert writer._python_writer is None
        messages = writer.prepare_commit(1) if stream else writer.prepare_commit()
        commit.commit(messages, 1) if stream else commit.commit(messages)
    finally:
        writer.close()
        commit.close()
    builder = table.new_read_builder()
    assert builder.new_read().to_arrow(builder.new_scan().plan().splits()).to_pylist() == [expected]


@pytest.mark.native_plan
@_NATIVE
@pytest.mark.parametrize('input_', ['row', 'batch', 'table'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
@pytest.mark.parametrize('kind,field', [('-D', 'id'), ('-D', 'seq'), ('-U', 'value')])
def test_native_filtered_events_keep_required_null_checks(tmp_path, input_, stream, kind, field):
    table, schema = _table(tmp_path, **{'write.native.enabled': 'true'})
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    writer = builder.new_write()
    invalid = dict(id=1, op=kind, seq=1, value=10)
    invalid[field] = None
    try:
        assert isinstance(writer, NativeTableWrite)
        with pytest.raises(ValueError, match=r'non-null column\(%s\)' % field):
            _write(writer, table, schema, [invalid], input_)
        assert writer._python_writer is None
        assert not list(tmp_path.rglob('*.parquet'))
        assert table.snapshot_manager().get_latest_snapshot() is None
    finally:
        writer.close()


def test_delete_null_exception_is_limited_to_non_cross_partition_deduplicate(tmp_path):
    table, _ = _table(tmp_path)
    values = dict(id=1, op='-D', seq=1, value=None)
    assert skip_write_row(table, values)
    for variant, cross_partition in [(table.copy({'merge-engine': 'first-row'}), False), (table, True)]:
        with patch.object(variant, 'cross_partition_update', cross_partition), \
                pytest.raises(ValueError, match=r'non-null column\(value\)'):
            skip_write_row(variant, values)


@pytest.mark.parametrize('kind,field', [
    (RowKind.DELETE, 'id'), (RowKind.DELETE, 'seq'), (RowKind.UPDATE_BEFORE, 'value')])
def test_internal_row_kinds_check_required_nulls_before_filtering(tmp_path, kind, field):
    table, _ = _table(tmp_path, **{'rowkind.field': None})
    writer = table.new_batch_write_builder().new_write()
    values = dict(id=1, op=None, seq=1, value=10)
    values[field] = None
    try:
        with pytest.raises(ValueError, match=r'non-null column\(%s\)' % field):
            writer.write_row(GenericRow([values[field.name] for field in table.fields], table.fields, kind))
        assert not writer.file_store_write.data_writers
    finally:
        writer.close()


def test_native_bridge_leaves_nested_value_validation_to_core(tmp_path):
    transport = pa.schema([('items', pa.list_(pa.int32()))])
    declared = pa.schema([('items', pa.list_(pa.field('element', pa.int32(), nullable=False)))])
    data = pa.RecordBatch.from_arrays([pa.array([[None]], type=transport.field(0).type)], schema=transport)
    writer = NativeTableWrite.__new__(NativeTableWrite)
    writer._table_schema = writer._schema = declared
    writer._write_cols = None
    with patch('pypaimon.write.native_write.normalize_arrow_write_nullability',
               side_effect=AssertionError('bridge inspected values before core filtering'), create=True):
        assert writer._prepare_native_arrow_data(data) is data


def test_schema_only_writer_keeps_strict_nested_validation():
    from pypaimon.write.table_write import TableWrite

    writer = TableWrite.__new__(TableWrite)
    writer.file_store_write = SimpleNamespace(write_cols=None)
    writer.table_pyarrow_schema = pa.schema([
        ('items', pa.list_(pa.field('element', pa.int32(), nullable=False)))])
    data = pa.table({'items': [[None]]}, schema=pa.schema([('items', pa.list_(pa.int32()))]))
    with pytest.raises(ValueError, match='consistent'):
        writer._prepare_arrow_data(data)


def test_python_filters_before_nested_normalization(tmp_path):
    from pypaimon.write.table_write import TableWrite

    table, _ = _table(tmp_path)
    writer = table.new_batch_write_builder().new_write()
    declared = pa.schema([field if field.name != 'value' else field.with_type(
        pa.list_(pa.field('element', pa.int32(), nullable=False))) for field in writer.table_pyarrow_schema])
    # Use a real table for options and routing, but a schema-only writer facade
    # to observe normalization before any actual Parquet output is attempted.
    writer.table_pyarrow_schema = declared
    transport = pa.schema([field if field.name != 'value' else field.with_type(pa.list_(pa.int32()))
                           for field in declared])
    data = pa.table({'id': [1, 2], 'op': ['-D', '+I'], 'seq': [1, 2], 'value': [[None], [42]]}, schema=transport)

    def normalize(selected, expected):
        assert selected['id'].to_pylist() == [2]
        assert expected == declared
        return selected.cast(expected)

    try:
        with patch('pypaimon.write.table_write.normalize_arrow_write_nullability', side_effect=normalize):
            selected = TableWrite._prepare_arrow_data(writer, data)
        assert selected.to_pydict() == {'id': [2], 'op': ['+I'], 'seq': [2], 'value': [[42]]}
    finally:
        writer.close()


@pytest.mark.parametrize('ignored', [False, True])
def test_python_retained_nested_schema_stays_strict_without_native_extension(tmp_path, ignored):
    table, _ = _table(tmp_path)
    writer = table.new_batch_write_builder().new_write()
    writer.table_pyarrow_schema = pa.schema([field if field.name != 'value' else field.with_type(
        pa.list_(pa.field('element', pa.int32(), nullable=False))) for field in writer.table_pyarrow_schema])
    transport = pa.schema([field if field.name != 'value' else field.with_type(pa.list_(pa.int32()))
                           for field in writer.table_pyarrow_schema])
    data = pa.table({'id': [1], 'op': ['-D' if ignored else '+I'], 'seq': [1], 'value': [[None]]}, schema=transport)
    try:
        with patch('pypaimon.read.native_plan.native_runtime_available', return_value=False):
            if ignored:
                assert writer._prepare_arrow_data(data).num_rows == 0
            else:
                with pytest.raises(ValueError, match='consistent'):
                    writer._prepare_arrow_data(data)
        assert not writer.file_store_write.data_writers
    finally:
        writer.close()
