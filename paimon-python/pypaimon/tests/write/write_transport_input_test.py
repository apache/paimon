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

"""Selected Arrow values can carry separate routing and event columns."""

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.execution.native.runtime import native_runtime_available

pytestmark = [pytest.mark.python_plan, pytest.mark.python_read,
              pytest.mark.python_write, pytest.mark.python_commit]
_NATIVE = [False, pytest.param(True, marks=[pytest.mark.native_plan, pytest.mark.skipif(
    not native_runtime_available(), reason='pypaimon-rust not installed')])]


def _table(tmp_path, native, bucket=-1, generated=False):
    fields = pa.schema([
        pa.field('id', pa.int32(), nullable=False),
        pa.field('value', pa.int32(), nullable=False),
        ('other', pa.int32()),
        ('unused', pa.struct([pa.field('item', pa.string(), nullable=False)])),
        pa.field('pt', pa.string(), nullable=False),
        pa.field('op', pa.string(), nullable=False),
    ])
    options = {'write.native.enabled': str(native).lower(), 'commit.native.enabled': 'false',
               'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
               'bucket': str(bucket)}
    if bucket > 0:
        options['bucket-key'] = 'id'
    else:
        options.update({'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'})
    if generated:
        options.update({'rowkind.field': 'op', 'ignore-delete': 'true'})
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.transport', Schema.from_pyarrow_schema(
        fields, partition_keys=['pt'], options=options), False)
    return catalog.get_table('default.transport'), fields


def _batch(schema, columns, rows):
    return pa.Table.from_pylist(rows, schema=pa.schema([schema.field(name) for name in columns])).to_batches()[0]


def _values(table):
    read = table.new_read_builder().with_projection(['value', 'other'])
    return sorted(read.new_read().to_arrow(read.new_scan().plan().splits()).to_pylist(),
                  key=lambda row: row['value'])


@pytest.mark.parametrize('native', _NATIVE, ids=['python', 'native'])
@pytest.mark.parametrize('input_', ['batch', 'table'])
def test_selected_values_accept_partition_transport(tmp_path, native, input_):
    table, schema = _table(tmp_path, native)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.with_write_type(['value', 'other'])
        input_schema = pa.schema([field.with_type(pa.large_string()) if field.name == 'pt' else field
                                  for field in schema])
        batch = _batch(input_schema, ['pt', 'value', 'other'], [{'pt': 'old', 'value': 10, 'other': 20}])
        writer.write_arrow(batch if input_ == 'batch' else pa.Table.from_batches([batch]))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    assert _values(table) == [{'value': 10, 'other': 20}]


@pytest.mark.parametrize('native', _NATIVE, ids=['python', 'native'])
@pytest.mark.parametrize('full', [False, True], ids=['transport', 'full-table'])
@pytest.mark.parametrize('generated', [False, True], ids=['insert', 'rowkind'])
def test_unselected_nulls_and_nested_payloads_do_not_validate_as_written_values(
        tmp_path, native, full, generated):
    table, schema = _table(tmp_path, native, generated=generated)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    row = {'id': None, 'value': 10, 'other': 20, 'unused': {'item': None},
           'pt': None, 'op': '+I' if generated else None}
    input_schema = pa.schema([
        field.with_type(pa.struct([('item', pa.large_string())])) if field.name == 'unused' else
        field.with_type(pa.large_string()) if field.name == 'pt' else field for field in schema])
    columns = schema.names if full else ['value', 'pt', 'other'] + (['op'] if generated else [])
    try:
        writer.with_write_type(['value', 'other'])
        writer.write_arrow(_batch(input_schema, columns, [row]))
        messages = writer.prepare_commit()
        assert [message.partition for message in messages] == [(None,)]
        assert all(file.write_cols == ['value', 'other'] for message in messages for file in message.new_files)
        commit.commit(messages)
    finally:
        writer.close()
        commit.close()
    assert _values(table) == [{'value': 10, 'other': 20}]


@pytest.mark.parametrize('native', _NATIVE, ids=['python', 'native'])
def test_selected_null_is_checked_before_event_filtering(tmp_path, native):
    table, schema = _table(tmp_path, native, generated=True)
    writer = table.new_batch_write_builder().new_write().with_write_type(['value', 'other'])
    try:
        with pytest.raises(ValueError, match='Cannot write null to non-null column.*value'):
            writer.write_arrow(_batch(schema, ['value', 'other', 'pt', 'op'], [
                {'value': None, 'other': 20, 'pt': 'ignored', 'op': '-D'}]))
        assert writer.with_write_type(['other']) is writer
        assert not list(tmp_path.rglob('*.parquet'))
    finally:
        writer.close()


@pytest.mark.parametrize('native', _NATIVE, ids=['python', 'native'])
def test_ignored_transport_needs_no_routing_values_or_captured_layout(tmp_path, native):
    table, schema = _table(tmp_path, native, generated=True)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.with_write_type(['value', 'other'])
        writer.write_arrow(_batch(schema, ['value', 'other', 'op'], [
            {'value': 10, 'other': 20, 'op': '-D'}]))
        assert writer.with_write_type(['value']) is writer
        writer.write_arrow(_batch(schema, ['value', 'pt', 'op'], [
            {'value': 30, 'pt': 'keep', 'op': '+I'}]))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    assert _values(table) == [{'value': 30, 'other': None}]


@pytest.mark.parametrize('native', _NATIVE, ids=['python', 'native'])
@pytest.mark.parametrize('columns', [
    ['other', 'value', 'pt'],
    ['value', 'other', 'pt', 'id'],
    ['value', 'other', 'pt', 'op'],
    ['value', 'other', 'pt', 'pt'],
    ['value', 'other', 'pt', 'unknown'],
    ['other', 'pt'],
], ids=['reordered', 'unneeded-hash-key', 'unconfigured-event', 'duplicate', 'unknown', 'missing-selected'])
def test_transport_rejects_payload_extras_and_changed_selected_layout(tmp_path, native, columns):
    table, schema = _table(tmp_path, native)
    writer = table.new_batch_write_builder().new_write().with_write_type(['value', 'other'])
    fields = [pa.field('unknown', pa.int32()) if name == 'unknown' else schema.field(name)
              for name in columns]
    arrays = [pa.array(['old'] if field.name in ('pt', 'op') else [10], type=field.type)
              for field in fields]
    try:
        with pytest.raises(ValueError):
            writer.write_arrow(pa.RecordBatch.from_arrays(arrays, schema=pa.schema(fields)))
        assert writer.with_write_type(['other']) is writer
        assert not list(tmp_path.rglob('*.parquet'))
    finally:
        writer.close()


@pytest.mark.parametrize('native', _NATIVE, ids=['python', 'native'])
@pytest.mark.parametrize('explicit', [False, True], ids=['automatic', 'explicit'])
def test_fixed_transport_requires_hash_keys_only_for_automatic_assignment(tmp_path, native, explicit):
    table, schema = _table(tmp_path, native, bucket=4)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    row = {'id': 7, 'value': 10, 'other': 20, 'pt': 'old'}
    try:
        writer.with_write_type(['value', 'other'])
        transport = _batch(schema, ['value', 'other', 'pt'], [row])
        if explicit:
            writer.write_arrow(transport, bucket=3)
        else:
            with pytest.raises(ValueError, match='routing column.*id'):
                writer.write_arrow(transport)
            assert writer.with_write_type(['other']) is writer
            writer.with_write_type(['value', 'other'])
            writer.write_arrow(_batch(schema, ['pt', 'id', 'value', 'other'], [row]))
        messages = writer.prepare_commit()
        assert len(messages) == 1
        if explicit:
            assert messages[0].bucket == 3
        commit.commit(messages)
    finally:
        writer.close()
        commit.close()
    assert _values(table) == [{'value': 10, 'other': 20}]


@pytest.mark.parametrize('native', _NATIVE, ids=['python', 'native'])
def test_transport_still_requires_partition_for_surviving_rows(tmp_path, native):
    table, schema = _table(tmp_path, native)
    writer = table.new_batch_write_builder().new_write().with_write_type(['value', 'other'])
    try:
        with pytest.raises(ValueError, match='routing column.*pt'):
            writer.write_arrow(_batch(schema, ['value', 'other'], [{'value': 10, 'other': 20}]))
        assert writer.with_write_type(['other']) is writer
    finally:
        writer.close()
