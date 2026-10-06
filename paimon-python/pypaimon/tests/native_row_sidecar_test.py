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

import json
import os
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.data.generic_variant import GenericVariant
from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.write.native_update import NativeTableUpdateByRowId
from pypaimon.write.table_update_by_row_id import TableUpdateByRowId
from pypaimon.write.native_write import NativeTableWrite, native_write_available

pytestmark = [pytest.mark.native_plan, pytest.mark.skipif(
    not native_write_available(), reason='pypaimon-rust runtime required')]


@pytest.mark.parametrize('stream', [False, True])
def test_row_sidecar_options_keep_batch_and_stream_writes_native(tmp_path, stream):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([('id', pa.int32()), ('value', pa.string())])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, options={
        'file.format': 'parquet', 'write.native.enabled': 'true',
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'data-evolution.row-sidecar.enabled': 'true'}), False)
    table = catalog.get_table('default.t')
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    writer = builder.new_write()
    assert isinstance(writer, NativeTableWrite)
    try:
        writer.write_arrow(pa.Table.from_pydict({'id': [1, 2], 'value': ['a', 'b']}, schema=schema))
        messages = writer.prepare_commit(7) if stream else writer.prepare_commit()
        for message in messages:
            for file in message.new_files:
                assert file.extra_files == [file.file_name + '.row']
                assert all(table.file_io.exists(path) for path in file.collect_files())
        commit = builder.new_commit()
        try:
            commit.commit(messages, 7) if stream else commit.commit(messages)
        finally:
            commit.close()
    finally:
        writer.close()


def _table(tmp_path, schema=None, options=None):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    if schema is None:
        schema = pa.schema([('id', pa.int32()), ('value', pa.string())])
    settings = {'file.format': 'parquet', 'write.native.enabled': 'true',
                'read.native.enabled': 'true', 'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true', 'deletion-vectors.enabled': 'true',
                'data-evolution.row-sidecar.enabled': 'true'}
    settings.update(options or {})
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, options=settings), False)
    return catalog.get_table('default.t')


def _commit(builder, messages, stream=False, identifier=1):
    commit = builder.new_commit()
    try:
        commit.commit(messages, identifier) if stream else commit.commit(messages)
    finally:
        commit.close()


def _append(table, start=0, count=100):
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    try:
        schema = PyarrowFieldParser.from_paimon_schema(table.fields)
        writer.write_arrow(pa.table({'id': list(range(start, start + count)),
                                    'value': ['v%d' % i for i in range(start, start + count)]},
                                    schema=schema))
        messages = writer.prepare_commit()
        _commit(builder, messages)
        return messages
    finally:
        writer.close()


def _read(table, projection, row_id, native=True):
    builder = table.new_read_builder().with_projection(projection)
    predicate = (table.new_read_builder().with_projection(['_ROW_ID'])
                 .new_predicate_builder().equal('_ROW_ID', row_id))
    builder.with_filter(predicate)
    read = builder.new_read()
    splits = builder.new_scan().plan().splits()
    if native:
        # Fail if a successful assertion actually used Python's ROW decoder.
        with patch.object(read, '_create_split_read', side_effect=AssertionError('Python fallback')):
            return read.to_arrow(splits)
    return read.to_arrow(splits)


def _remove_primaries(messages):
    for message in messages:
        for file in message.new_files:
            os.remove(file.external_path or file.file_path)


@pytest.mark.parametrize('native_write', [False, True])
@pytest.mark.parametrize('native_read', [False, True])
def test_row_sidecars_interoperate_between_python_and_rust(tmp_path, native_write, native_read):
    table = _table(tmp_path, options={'write.native.enabled': str(native_write).lower()})
    _append(table, -20, 20)
    messages = _append(table)
    _remove_primaries(messages)
    read_table = table.copy({'read.native.enabled': str(native_read).lower(),
                             'data-evolution.row-sidecar.enabled': 'false'})
    assert _read(read_table, ['id', 'value', '_ROW_ID'], 25, native_read).to_pydict() == {
        'id': [5], 'value': ['v5'], '_ROW_ID': [25]}


@pytest.mark.parametrize('stream', [False, True])
@pytest.mark.parametrize('incremental', [False, True])
def test_sidecars_keep_updates_native_and_apply_deletion_vectors(tmp_path, stream, incremental):
    table = _table(tmp_path)
    initial = _append(table)
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    update = builder.new_update()
    data = pa.table({'_ROW_ID': [5, 7], 'value': ['changed-5', 'changed-7']})
    if incremental:
        updater = update.new_update_by_row_id(1) if stream else update.new_update_by_row_id()
        assert isinstance(updater, NativeTableUpdateByRowId)
        messages = updater.update_columns(data, ['value'])
    else:
        with patch.object(TableUpdateByRowId, 'update_columns',
                          side_effect=AssertionError('Python update')):
            messages = (update.update_by_arrow_with_row_id(data, 1) if stream
                        else update.update_by_arrow_with_row_id(data))
    for message in messages:
        for file in message.new_files:
            assert file.extra_files == [file.file_name + '.row']
    _commit(builder, messages, stream)
    delete_builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    delete = delete_builder.new_update()
    deletes = delete.delete_by_row_id([6], 2) if stream else delete.delete_by_row_id([6])
    _commit(delete_builder, deletes, stream, 2)
    _remove_primaries(initial + messages)
    assert _read(table, ['id', 'value', '_ROW_ID'], 5).to_pydict() == {
        'id': [5], 'value': ['changed-5'], '_ROW_ID': [5]}
    assert _read(table, ['id', 'value', '_ROW_ID'], 6).num_rows == 0
    assert _read(table, ['id', 'value', '_ROW_ID'], 7).to_pydict() == {
        'id': [7], 'value': ['changed-7'], '_ROW_ID': [7]}


@pytest.mark.parametrize('layout', ['plain', 'map', 'variant'])
def test_sidecars_store_logical_nested_map_and_variant_values(tmp_path, layout):
    schema = pa.schema([
        ('id', pa.int32()),
        ('profile', pa.struct([('score', pa.int32()), ('unused', pa.string())])),
        ('attrs', pa.map_(pa.string(), pa.int64())),
        *([('payload', GenericVariant.to_arrow_array([]).type)] if layout != 'map' else []),
    ])
    options = {}
    if layout == 'map':
        options.update({'fields.attrs.map.storage-layout': 'shared-shredding',
                        'fields.attrs.map.shared-shredding.max-columns': '1'})
    elif layout == 'variant':
        options['variant.shreddingSchema'] = json.dumps({'type': 'ROW', 'fields': [{
            'name': 'payload', 'type': {'type': 'ROW', 'fields': [
                {'name': 'score', 'type': 'INT'}, {'name': 'unused', 'type': 'STRING'}]}}]})
    table = _table(tmp_path, schema, options)
    schema = PyarrowFieldParser.from_paimon_schema(table.fields)
    values = {
        'id': list(range(100)),
        'profile': [{'score': i, 'unused': 'ignore'} if i != 6 else None for i in range(100)],
        'attrs': [[('wanted', i), ('other', -i)] if i != 6 else None for i in range(100)],
    }
    if layout != 'map':
        values['payload'] = GenericVariant.to_arrow_array([
            GenericVariant.from_python({'score': i, 'unused': 'ignore'}) if i != 6 else None
            for i in range(100)])
    data = pa.table(values, schema=schema)
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    assert isinstance(writer, NativeTableWrite)
    try:
        writer.write_arrow(data)
        messages = writer.prepare_commit()
        _commit(builder, messages)
    finally:
        writer.close()
    _remove_primaries(messages)
    projection = {'score': 'profile.score', 'wanted': "attrs['wanted']",
                  'missing': "attrs['missing']",
                  'row_id': '_ROW_ID'}
    if layout != 'map':
        projection['variant'] = "variant_get(payload, '$.score', 'float')"
    for row_id in [5, 6]:
        value = 5 if row_id == 5 else None
        expected = {'score': [value], 'wanted': [value], 'missing': [None], 'row_id': [row_id]}
        if layout != 'map':
            expected['variant'] = [value]
        assert _read(table, projection, row_id).to_pydict() == expected


@pytest.mark.parametrize('enabled', [None, 'false', 'true'])
def test_missing_sidecar_can_fall_back_to_native_parquet(tmp_path, enabled):
    options = {} if enabled is None else {'scan.ignore-lost-files': enabled}
    table = _table(tmp_path, options=options)
    messages = _append(table)
    for message in messages:
        for file in message.new_files:
            os.remove(next(path for path in file.collect_files() if path.endswith('.row')))
    if enabled == 'true':
        assert _read(table, ['id', 'value'], 5).to_pydict() == {'id': [5], 'value': ['v5']}
    else:
        with pytest.raises(ValueError, match=r'parquet\.row'):
            _read(table, ['id', 'value'], 5)


@pytest.mark.parametrize('enabled', ['false', 'off', 'true', 'on'])
def test_sidecar_generation_uses_parsed_boolean_options(tmp_path, enabled):
    table = _table(tmp_path, options={'data-evolution.row-sidecar.enabled': enabled})
    messages = _append(table, count=2)
    for message in messages:
        for file in message.new_files:
            assert bool(file.extra_files) == (enabled in ('true', 'on'))


@pytest.mark.parametrize('blob_type', [
    pa.large_binary(), pa.list_(pa.large_binary()), pa.map_(pa.string(), pa.large_binary())])
def test_dedicated_blob_writers_keep_java_sidecar_exclusion(tmp_path, blob_type):
    schema = pa.schema([('id', pa.int32()), ('payload', blob_type)])
    table = _table(tmp_path, schema)
    if pa.types.is_list(blob_type):
        values = [[b'a', None], None]
    elif pa.types.is_map(blob_type):
        values = [[('key', b'a')], None]
    else:
        values = [b'a', None]
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    assert isinstance(writer, NativeTableWrite)
    try:
        writer.write_arrow(pa.table({'id': [1, 2], 'payload': values}, schema=schema))
        messages = writer.prepare_commit()
        files = [file for message in messages for file in message.new_files]
        assert any(file.file_name.endswith('.blob') for file in files)
        assert any(file.file_name.endswith('.parquet') for file in files)
        assert all(not file.extra_files for file in files)
        _commit(builder, messages)
    finally:
        writer.close()


def test_explicit_abort_owns_sidecars_but_close_after_prepare_preserves_them(tmp_path):
    table = _table(tmp_path)
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    writer.write_arrow(pa.table({'id': [1], 'value': ['a']},
                                schema=PyarrowFieldParser.from_paimon_schema(table.fields)))
    messages = writer.prepare_commit()
    paths = {path for message in messages for file in message.new_files for path in file.collect_files()}
    assert len(paths) == 2
    writer.close()
    assert all(table.file_io.exists(path) for path in paths)
    commit = builder.new_commit()
    try:
        # These messages were never submitted and are now permanently abandoned.
        commit.abort(messages)
    finally:
        commit.close()
    assert all(not table.file_io.exists(path) for path in paths)


@pytest.mark.parametrize('external', [False, True])
def test_old_sidecar_schema_evolves_by_field_id(tmp_path, external):
    from pypaimon.schema.data_types import AtomicType
    from pypaimon.schema.schema_change import SchemaChange

    options = {'data-file.external-paths': str(tmp_path / 'external data')} if external else {}
    table = _table(tmp_path, options=options)
    messages = _append(table)
    _remove_primaries(messages)
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.alter_table('default.t', [
        SchemaChange.rename_column('value', 'renamed_value'),
        SchemaChange.update_column_type('id', AtomicType('BIGINT')),
        SchemaChange.add_column('added', AtomicType('INT')),
    ], False)
    table = catalog.get_table('default.t')
    actual = _read(table, ['renamed_value', 'added', 'id', '_ROW_ID'], 5)
    assert actual.to_pydict() == {
        'renamed_value': ['v5'], 'added': [None], 'id': [5], '_ROW_ID': [5]}
    assert actual.schema.field('id').type == pa.int64()


def test_bitmap_and_limit_keep_primary_without_requested_row_ranges(tmp_path):
    table = _table(tmp_path, options={'file-index.bitmap.columns': 'id'})
    messages = _append(table)
    for message in messages:
        for file in message.new_files:
            os.remove(next(path for path in file.collect_files() if path.endswith('.row')))
    for mode in ['bitmap', 'limit']:
        builder = table.new_read_builder()
        if mode == 'bitmap':
            builder.with_filter(builder.new_predicate_builder().equal('id', 5))
        else:
            builder.with_limit(1)
        read = builder.new_read()
        with patch.object(read, '_create_split_read', side_effect=AssertionError('Python fallback')):
            actual = read.to_arrow(builder.new_scan().plan().splits())
        expected_id = 5 if mode == 'bitmap' else 0
        assert actual.to_pydict() == {'id': [expected_id], 'value': ['v%d' % expected_id]}


@pytest.mark.parametrize('option', ['blob-descriptor-field', 'blob.stored-descriptor-fields'])
@pytest.mark.parametrize('native_read', [False, True])
def test_sidecars_materialize_inline_blob_references(tmp_path, option, native_read):
    from pypaimon.table.row.blob import BlobDescriptor

    schema = pa.schema([('id', pa.int32()), ('payload', pa.large_binary()), ('value', pa.string())])
    table = _table(tmp_path, schema, {
        option: 'payload', 'read.native.enabled': str(native_read).lower()})
    # Actual bytes may themselves look exactly like a reference. ROW decoding
    # must return them intact, without resolving that inner descriptor.
    payload = BlobDescriptor(str(tmp_path / 'must-not-read'), 0, 1).serialize()
    source = tmp_path / 'blob-source'
    source.write_bytes(b'prefix' + payload + b'suffix')
    reference = BlobDescriptor(str(source), 6, len(payload)).serialize()
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    assert isinstance(writer, NativeTableWrite)
    try:
        writer.write_arrow(pa.table({
            'id': list(range(100)), 'value': ['v%d' % i for i in range(100)],
            'payload': [reference if i != 6 else None for i in range(100)],
        }, schema=schema))
        messages = writer.prepare_commit()
        _commit(builder, messages)
    finally:
        writer.close()
    descriptor_table = table.copy({'blob-as-descriptor': 'true'})
    assert _read(descriptor_table, ['payload'], 5, native=native_read).to_pydict() == {'payload': [reference]}
    read = descriptor_table.new_read_builder().with_projection(['payload'])
    assert read.new_read().to_arrow(read.new_scan().plan().splits())['payload'][5].as_py() == reference
    update_builder = table.copy({'write.native.enabled': 'true'}).new_batch_write_builder()
    with patch.object(TableUpdateByRowId, 'update_columns', side_effect=AssertionError('Python update')):
        updates = update_builder.new_update().update_by_arrow_with_row_id(
            pa.table({'_ROW_ID': [5], 'value': ['changed-5']}))
    _commit(update_builder, updates)
    source.unlink()
    _remove_primaries(messages + updates)
    assert _read(table, ['id', 'payload', 'value', '_ROW_ID'], 5, native=native_read).to_pydict() == {
        'id': [5], 'payload': [payload], 'value': ['changed-5'], '_ROW_ID': [5]}
    assert _read(table, ['payload'], 6, native=native_read).to_pydict() == {'payload': [None]}
