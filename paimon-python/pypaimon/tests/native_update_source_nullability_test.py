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

"""Source Arrow nullability must not validate values discarded by update actions."""

from contextlib import ExitStack
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.read.native_plan import native_runtime_available
from pypaimon.table.data_evolution_merge_into import WhenMatched, source_col
from pypaimon.table.row.generic_row import GenericRow


pytestmark = [pytest.mark.python_write, pytest.mark.python_commit]
_NATIVE = pytest.mark.skipif(not native_runtime_available(), reason='pypaimon-rust not installed')
_ENGINES = [False, pytest.param(True, marks=[pytest.mark.native_plan, _NATIVE])]
_OPERATIONS = ['upsert_arrow', 'upsert_rows', 'merge']


def _table(catalog, native, value_nullable=False):
    schema = pa.schema([pa.field('id', pa.int32(), nullable=False),
                        pa.field('value', pa.int32(), nullable=value_nullable),
                        pa.field('keep', pa.int32(), nullable=False)])
    catalog.create_table('default.updates', Schema.from_pyarrow_schema(schema, options={
        'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true',
        'write.native.enabled': str(native).lower(), 'commit.native.enabled': 'false',
        'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
    }), False)
    table = catalog.get_table('default.updates')
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pylist([dict(id=1, value=10, keep=100)], schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    return table, schema


def _source(schema, operation, invalid):
    if invalid:
        rows = [dict(id=1, value=None, keep=100)]
    elif operation == 'merge':
        # The second row does not match and there is no INSERT clause. Its
        # NULL cannot be validated as though it were written to the target.
        rows = [dict(id=1, value=20, keep=None), dict(id=2, value=None, keep=None)]
    else:
        # Last-write-wins discards the first invalid value. The selected row
        # updates only value, so keep is never a target assignment.
        rows = [dict(id=1, value=None, keep=None), dict(id=1, value=20, keep=None)]
    if operation == 'upsert_rows':
        return rows
    source = pa.Table.from_pylist([dict(id=0, value=0, keep=0)] + rows, schema=schema).slice(1)
    source.validate(full=True)
    return source.replace_schema_metadata({'origin': 'external-update'})


def _messages(table, source, native, stream, operation):
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    update = builder.new_update()
    kwargs = dict(commit_identifier=31) if stream else {}
    with ExitStack() as stack:
        if native:
            for method in ('_upsert_partition', '_upsert_row_partition'):
                stack.enter_context(patch('pypaimon.write.table_upsert_by_key.TableUpsertByKey.' + method,
                                          side_effect=AssertionError('Python upsert fallback')))
            stack.enter_context(patch('pypaimon.table.data_evolution_merge_into._build_tables',
                                      side_effect=AssertionError('Python MERGE fallback')))
        if operation == 'merge':
            messages = update.merge_into(source, on=['id'], when_matched=[
                WhenMatched.update({'value': source_col('value')})], **kwargs)
        elif operation == 'upsert_rows':
            rows = [GenericRow([row[name] for name in table.field_names], table.fields) for row in source]
            messages = update.with_update_type(['value']).upsert_by_key(rows, ['id'], **kwargs)
        else:
            messages = update.with_update_type(['value']).upsert_by_arrow_with_key(source, ['id'], **kwargs)
    return builder, messages


def _rows(table):
    builder = table.new_read_builder()
    return builder.new_read().to_arrow(builder.new_scan().plan().splits()).to_pylist()


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
@pytest.mark.parametrize('operation', _OPERATIONS)
def test_update_validates_selected_target_values_not_source_transport(
        native_rest_catalog, native, stream, operation):
    table, schema = _table(native_rest_catalog, native)
    source = _source(schema, operation, invalid=False)
    builder, messages = _messages(table, source, native, stream, operation)
    commit = builder.new_commit()
    try:
        commit.commit(messages, 31) if stream else commit.commit(messages)
    finally:
        commit.close()
    assert _rows(table) == [dict(id=1, value=20, keep=100)]
    if isinstance(source, pa.Table):
        assert source.schema.metadata == {b'origin': b'external-update'}
        assert all(not field.nullable for field in source.schema)


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
@pytest.mark.parametrize('operation', _OPERATIONS)
def test_selected_target_not_null_value_is_still_required(
        native_rest_catalog, native, stream, operation):
    table, schema = _table(native_rest_catalog, native)
    snapshot = table.snapshot_manager().get_latest_snapshot().id
    with pytest.raises(ValueError, match='(?i)null'):
        _messages(table, _source(schema, operation, invalid=True), native, stream, operation)
    assert table.snapshot_manager().get_latest_snapshot().id == snapshot
    assert _rows(table) == [dict(id=1, value=10, keep=100)]


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('operation', _OPERATIONS)
def test_selected_nullable_target_accepts_null(native_rest_catalog, native, operation):
    table, schema = _table(native_rest_catalog, native, value_nullable=True)
    builder, messages = _messages(table, _source(schema, operation, invalid=True), native, False, operation)
    commit = builder.new_commit()
    try:
        commit.commit(messages)
    finally:
        commit.close()
    assert _rows(table) == [dict(id=1, value=None, keep=100)]


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
@pytest.mark.parametrize('reverse', [False, True], ids=['null-last', 'null-first'])
def test_merge_chunked_source_keeps_one_transport_schema(
        native_rest_catalog, native, stream, reverse):
    table, schema = _table(native_rest_catalog, native)
    schema = schema.with_metadata({'origin': 'chunked-merge'})
    # Each chunk needs a different relaxed transport field. The NULL key and
    # unmatched value never reach a target assignment, regardless of chunk order.
    rows = [dict(id=1, value=20, keep=100), dict(id=2, value=None, keep=None),
            dict(id=None, value=30, keep=100)]
    chunks = [pa.RecordBatch.from_pylist([dict(id=0, value=0, keep=0), row], schema=schema).slice(1)
              for row in rows]
    source = pa.Table.from_batches(chunks[::-1] if reverse else chunks)
    encoded = pa.chunked_array([pa.DictionaryArray.from_arrays(
        pa.array([0], type=pa.int8()), pa.array([None], type=pa.int32())) for _ in chunks])
    source = source.append_column(pa.field('encoded_unused', encoded.type, nullable=False), encoded)
    schema = source.schema
    assert source.column('encoded_unused').null_count == 0
    assert source.column('encoded_unused').to_pylist() == [None, None, None]
    assert len(source.to_batches()) == 3
    builder, messages = _messages(table, source, native, stream, 'merge')
    commit = builder.new_commit()
    try:
        commit.commit(messages, 31) if stream else commit.commit(messages)
    finally:
        commit.close()
    assert _rows(table) == [dict(id=1, value=20, keep=100)]
    assert source.schema.equals(schema, check_metadata=True)


def _blob_table(catalog, native, payload_type, nullable, before):
    schema = pa.schema([pa.field('id', pa.int32()),
                        pa.field('payload', payload_type, nullable=nullable)])
    catalog.create_table('default.blobs', Schema.from_pyarrow_schema(schema, options={
        'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true',
        'write.native.enabled': str(native).lower(), 'commit.native.enabled': 'false',
        'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
    }), False)
    table = catalog.get_table('default.blobs')
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pylist([dict(id=1, payload=before)], schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    return table, schema


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('input_', ['arrow', 'row'])
@pytest.mark.parametrize('nullable', [False, True], ids=['not-null', 'nullable'])
def test_selected_blob_nullability_applies_to_dedicated_files(
        native_rest_catalog, native, input_, nullable):
    table, schema = _blob_table(native_rest_catalog, native, pa.large_binary(), nullable, b'before')
    snapshot = table.snapshot_manager().get_latest_snapshot().id
    builder = table.new_batch_write_builder()
    with ExitStack() as stack:
        if native:
            stack.enter_context(patch(
                'pypaimon.write.table_update_by_row_id.TableUpdateByRowId._load_existing_files_info',
                side_effect=AssertionError('Python row-ID update fallback')))
        updater = builder.new_update().new_update_by_row_id()

        def update():
            if input_ == 'row':
                return updater.update_row_columns(GenericRow([None], [table.field_dict['payload']]),
                                                  [0], ['payload'])
            source = pa.Table.from_arrays([pa.array([0], type=pa.int64()),
                                          pa.array([None], type=pa.large_binary())], schema=pa.schema([
                                              pa.field('_ROW_ID', pa.int64()), schema.field('payload')]))
            return updater.update_columns(source, ['payload'])

        if nullable:
            messages = update()
        else:
            with pytest.raises(ValueError, match='(?i)null'):
                update()
            messages = []
    if nullable:
        commit = builder.new_commit()
        try:
            commit.commit(messages)
        finally:
            commit.close()
    else:
        assert table.snapshot_manager().get_latest_snapshot().id == snapshot
    assert _rows(table) == [dict(id=1, payload=None if nullable else b'before')]


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('payload_type,before,after', [
    (pa.list_(pa.large_binary()), [b'before'], [None]),
    (pa.map_(pa.string(), pa.large_binary()), [('key', b'before')], [('key', None)]),
], ids=['array', 'map'])
def test_not_null_blob_containers_keep_nullable_children(
        native_rest_catalog, native, payload_type, before, after):
    table, schema = _blob_table(native_rest_catalog, native, payload_type, False, before)
    builder = table.new_batch_write_builder()
    source = pa.Table.from_arrays([pa.array([0], type=pa.int64()), pa.array([after], type=payload_type)],
                                  schema=pa.schema([pa.field('_ROW_ID', pa.int64()), schema.field('payload')]))
    with ExitStack() as stack:
        if native:
            stack.enter_context(patch(
                'pypaimon.write.table_update_by_row_id.TableUpdateByRowId._load_existing_files_info',
                side_effect=AssertionError('Python row-ID update fallback')))
        messages = builder.new_update().new_update_by_row_id().update_columns(source, ['payload'])
    commit = builder.new_commit()
    try:
        commit.commit(messages)
    finally:
        commit.close()
    assert _rows(table) == [dict(id=1, payload=after)]
