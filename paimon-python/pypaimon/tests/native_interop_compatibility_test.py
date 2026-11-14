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

"""Native defaults preserve the physical contracts shared with Java."""

import json
from unittest.mock import patch
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.data.map_shared_shredding import is_shared_shredding
from pypaimon.data.generic_variant import GenericVariant
from pypaimon.execution.native.read.table_read import NativeTableRead, native_read
from pypaimon.execution.native.runtime import native_runtime_available


pytestmark = [pytest.mark.execution_defaults, pytest.mark.skipif(
    not native_runtime_available(), reason='pypaimon-rust is required')]
_SCHEMA = pa.schema([('id', pa.int32()), ('metrics', pa.map_(pa.string(), pa.int64()))])
_ROWS = [{'id': 1, 'metrics': [('hot', 10), ('warm', 20), ('overflow', 30)]},
         {'id': 2, 'metrics': [('hot', None), ('new', 40)]},
         {'id': 3, 'metrics': []}, {'id': 4, 'metrics': None}]


def _table(tmp_path, layout, **options):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    options.update({'fields.metrics.map.storage-layout': layout,
                    'fields.metrics.map.shared-shredding.max-columns': '2'})
    catalog.create_table('default.t', Schema.from_pyarrow_schema(_SCHEMA, options=options), False)
    return catalog.get_table('default.t')


def _write(table):
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pylist(_ROWS, schema=_SCHEMA))
        messages = writer.prepare_commit()
        commit.commit(messages)
        return messages
    finally:
        writer.close()
        commit.close()


@pytest.mark.parametrize('layout', ['default', 'shared-shredding'])
@pytest.mark.parametrize('selected', [False, True])
def test_native_defaults_read_python_physical_map_files(tmp_path, layout, selected):
    table = _table(tmp_path, layout, **{'write.native.enabled': 'false'})
    messages = _write(table)
    physical = pq.read_schema(messages[0].new_files[0].file_path).field('metrics')
    assert is_shared_shredding(physical) == (layout == 'shared-shredding')
    builder = table.new_read_builder()
    if selected:
        builder.with_projection(['id', "metrics['hot']", "metrics['overflow']"])
    with patch('pypaimon.execution.native.read.table_read.native_read', wraps=native_read) as native:
        result = builder.new_read().to_arrow(builder.new_scan().plan().splits(), parallelism=1).sort_by('id')
    native.assert_called_once()
    if selected:
        assert result.to_pydict() == {
            'id': [1, 2, 3, 4], 'metrics_hot': [10, None, None, None],
            'metrics_overflow': [30, None, None, None]}
    else:
        assert result.to_pylist() == _ROWS


@pytest.mark.parametrize('layout', ['default', 'shared-shredding'])
def test_native_default_writer_preserves_configured_map_layout(tmp_path, layout):
    table = _table(tmp_path, layout)
    messages = _write(table)
    physical = pq.read_schema(messages[0].new_files[0].file_path).field('metrics')
    assert is_shared_shredding(physical) == (layout == 'shared-shredding')


def test_native_default_update_preserves_shared_shredding_map_layout(tmp_path):
    table = _table(tmp_path, 'shared-shredding', **{
        'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true'})
    _write(table.copy({'write.native.enabled': 'false'}))
    builder = table.new_batch_write_builder()
    messages = builder.new_update().with_update_type(['metrics']).update_by_arrow_with_row_id(
        pa.table({'_ROW_ID': pa.array([0], type=pa.int64()),
                  'metrics': pa.array([[('hot', 99)]], type=_SCHEMA.field('metrics').type)}))
    physical = pq.read_schema(messages[0].new_files[0].file_path).field('metrics')
    assert is_shared_shredding(physical)
    commit = builder.new_commit()
    try:
        commit.commit(messages)
    finally:
        commit.close()
    reader = table.new_read_builder()
    actual = reader.new_read().to_arrow(reader.new_scan().plan().splits()).sort_by('id').to_pylist()
    assert actual == [dict(_ROWS[0], metrics=[('hot', 99)])] + _ROWS[1:]


@pytest.mark.parametrize('layout', ['default', 'shared-shredding'])
@pytest.mark.parametrize('overrides', [
    {}, {'file.format': 'parquet'}, {'fields.metrics.map.storage-layout': 'default'}])
def test_orc_shared_shredding_is_qualified_before_native_read(tmp_path, layout, overrides):
    table = _table(tmp_path, layout, **{'file.format': 'orc'}).copy(overrides)
    split = SimpleNamespace(files=[SimpleNamespace(file_name='data.orc', schema_id=0)], _native_split=object())
    with patch('pypaimon.execution.native.read.table_read.native_read', return_value=[]) as native:
        result = NativeTableRead(table.new_read_builder().new_read()).read_batches([split], _SCHEMA)
        if layout == 'shared-shredding':
            assert result is None
            native.assert_not_called()
        else:
            assert list(result) == []
            native.assert_called_once()


@pytest.mark.parametrize('stream', [False, True])
def test_native_default_bucketed_append_restores_advancing_sequence_ranges(native_rest_catalog, stream):
    catalog = native_rest_catalog
    schema = pa.schema([('id', pa.int32()), ('seq', pa.int32())])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        schema, options={'bucket': '1', 'bucket-key': 'id'}), False)
    table = catalog.get_table('default.t')
    ranges = []
    for seq in range(6):
        builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        try:
            writer.write_arrow(pa.Table.from_pydict({'id': [1], 'seq': [seq]}, schema=schema))
            messages = writer.prepare_commit(seq) if stream else writer.prepare_commit()
            ranges.extend((f.min_sequence_number, f.max_sequence_number)
                          for message in messages for f in message.new_files)
            commit.commit(messages, seq) if stream else commit.commit(messages)
            assert commit._native_commit is not None
        finally:
            writer.close()
            commit.close()
    # Java compaction sorts ordinary append files by these ranges.
    assert ranges == [(seq, seq) for seq in range(2, 8)]


@pytest.mark.parametrize('kind', ['ordinary', 'fork', 'cause', 'context'])
def test_orc_schema_probe_selects_fallback_but_propagates_fork_errors(tmp_path, kind):
    from pypaimon_rust import ForkSafetyError

    table = _table(tmp_path, 'default', **{'file.format': 'orc'})
    split = SimpleNamespace(files=[SimpleNamespace(file_name='data.orc', schema_id=0)], _native_split=object())
    error = OSError('schema probe failed')
    if kind != 'ordinary':
        fork = ForkSafetyError('Native runtime cannot be reused after fork')
        if kind == 'fork':
            error = fork
        elif kind == 'cause':
            error.__cause__ = fork
        else:
            error.__context__ = fork
    with patch.object(table.schema_manager, 'get_schema', side_effect=error), \
            patch('pypaimon.execution.native.read.table_read.native_read') as native:
        read = NativeTableRead(table.new_read_builder().new_read())
        if kind == 'ordinary':
            assert read.read_batches([split], _SCHEMA) is None
        else:
            with pytest.raises(ForkSafetyError):
                read.read_batches([split], _SCHEMA)
    native.assert_not_called()


@pytest.mark.parametrize('option', ['variant.shreddingSchema', 'parquet.variant.shreddingSchema'])
def test_native_default_shredded_variant_retains_java_field_ids(tmp_path, option):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    values = [GenericVariant.from_python({'age': 30, 'city': 'Beijing'}),
              GenericVariant.from_python([1, 2, 3]), None]
    payload = GenericVariant.to_arrow_array(values)
    schema = pa.schema([('id', pa.int32()), ('payload', payload.type)])
    shredding = json.dumps({'type': 'ROW', 'fields': [
        {'name': 'payload', 'type': {'type': 'ROW', 'fields': [
            {'name': 'age', 'type': 'BIGINT'}, {'name': 'city', 'type': 'STRING'}]}}]})
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, options={
        option: shredding, 'variant.shredding.enabled': 'false'}), False)
    table = catalog.get_table('default.t')
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_arrays([pa.array([1, 2, 3], type=pa.int32()), payload], schema=schema))
        messages = writer.prepare_commit()
        commit.commit(messages)
    finally:
        writer.close()
        commit.close()

    def check_ids(fields):
        for field in fields:
            assert field.metadata and b'PARQUET:field_id' in field.metadata
            if pa.types.is_struct(field.type):
                check_ids(field.type)

    physical = pq.read_schema(messages[0].new_files[0].file_path).field('payload')
    assert 'typed_value' in physical.type.names
    check_ids(physical.type)
    reader = table.new_read_builder()
    actual = reader.new_read().to_arrow(reader.new_scan().plan().splits()).sort_by('id')['payload'].to_pylist()
    assert [None if value is None else GenericVariant.from_arrow_struct(value).to_python()
            for value in actual] == [{'age': 30, 'city': 'Beijing'}, [1, 2, 3], None]
