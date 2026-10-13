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

"""Native partial updates retain Java's shredded VARIANT physical layout."""

import json
from contextlib import ExitStack
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.data.generic_variant import GenericVariant
from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.write.native_update import NativeTableUpdateByRowId
from pypaimon.write.native_write import NativeTableWrite
from pypaimon.write.table_update import TableUpdate
from pypaimon.write.table_update_by_row_id import TableUpdateByRowId
from pypaimon.write.table_upsert_by_key import TableUpsertByKey

pytestmark = pytest.mark.native_plan


def _variants(values):
    return GenericVariant.to_arrow_array([
        GenericVariant.from_python(value) if value is not None else None for value in values])


def _table(tmp_path, key, enabled, native_write=True):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([('id', pa.int32()), ('payload', _variants([]).type),
                        ('unchanged', _variants([]).type), ('value', pa.int32())])
    shredding = json.dumps({'type': 'ROW', 'fields': [
        {'name': name, 'type': {'type': 'ROW', 'fields': [
            {'name': 'count', 'type': 'BIGINT'}, {'name': 'name', 'type': 'STRING'},
        ]}} for name in ('payload', 'unchanged')
    ]})
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, options={
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'write.native.enabled': str(native_write).lower(), 'target-file-row-num': '2',
        # This Python-only option does not exist in Java. It must not disable
        # a configured physical schema or reassembly on either reader.
        'variant.shredding.enabled': str(enabled).lower(), key: shredding,
    }), False)
    table = catalog.get_table('default.t')
    schema = PyarrowFieldParser.from_paimon_schema(table.fields)
    original = [{'count': i, 'name': 'old', 'overflow': [i, None]} for i in range(4)]
    unchanged = [{'count': i + 10, 'name': 'untouched', 'extra': True} for i in range(4)]
    data = pa.table({'id': [0, 1, 2, 3], 'payload': _variants(original),
                     'unchanged': _variants(unchanged), 'value': [10, 11, 12, 13]}, schema=schema)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    assert isinstance(writer, NativeTableWrite) == native_write
    try:
        # Rolling occurs at batch boundaries. Keep the two incremental calls
        # in distinct file groups, as required by the public updater contract.
        writer.write_arrow(data.slice(0, 2))
        writer.write_arrow(data.slice(2))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    return table, schema, original, unchanged


def _read(table, native, projection=None):
    table = table.copy({'read.native.enabled': str(native).lower(),
                        'scan.native-plan.enabled': str(native).lower()})
    builder = table.new_read_builder()
    if projection is not None:
        builder.with_projection(projection)
    splits = builder.new_scan().plan().splits()
    read = builder.new_read()
    with ExitStack() as stack:
        if native:
            stack.enter_context(patch.object(read, '_create_split_read',
                                             side_effect=AssertionError('Python read fallback')))
        data = read.to_arrow(splits).sort_by('id')
    rows = data.to_pylist()
    for row in rows:
        for name in ('payload', 'unchanged'):
            if name in row and row[name] is not None:
                row[name] = GenericVariant.from_arrow_struct(row[name]).to_python()
    return rows


def _native_only():
    stack = ExitStack()
    for cls, method in [(TableUpdateByRowId, '_load_existing_files_info'),
                        (TableUpdate, '_build_predicate_update_table'),
                        (TableUpsertByKey, '_upsert_partition')]:
        stack.enter_context(patch.object(cls, method, side_effect=AssertionError('Python update fallback')))
    return stack


def test_python_configured_variant_schema_uses_java_semantics(tmp_path):
    table, _, original, unchanged = _table(tmp_path, 'variant.shreddingSchema', False, native_write=False)
    for native in (False, True):
        rows = _read(table, native)
        assert [row['payload'] for row in rows] == original
        assert [row['unchanged'] for row in rows] == unchanged
    files = table.new_read_builder().new_scan().plan().splits()[0].files
    for file in files:
        physical = pq.read_schema(file.file_path)
        assert 'typed_value' in [field.name for field in physical.field('payload').type]


@pytest.mark.parametrize('stream', [False, True])
@pytest.mark.parametrize('key,enabled', [
    ('variant.shreddingSchema', True), ('parquet.variant.shreddingSchema', True),
    ('variant.shreddingSchema', False),
])
@pytest.mark.parametrize('operation', ['row_id', 'incremental', 'predicate', 'upsert', 'ordinary_column'])
def test_native_variant_partial_updates(tmp_path, stream, key, enabled, operation):
    table, schema, original, untouched = _table(tmp_path, key, enabled)
    ids = {row['id']: row['_ROW_ID'] for row in _read(table, True, ['id', '_ROW_ID'])}
    replacement = {'count': 100, 'name': 'new', 'overflow': {'nested': [1, None]}}
    changed = _variants([replacement, None])
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    update = builder.new_update().with_update_type(['payload'])
    data = pa.table({'_ROW_ID': [ids[0], ids[2]], 'payload': changed})
    with _native_only():
        if operation == 'row_id':
            messages = (update.update_by_arrow_with_row_id(data, 7) if stream
                        else update.update_by_arrow_with_row_id(data))
        elif operation == 'incremental':
            updater = update.new_update_by_row_id(7) if stream else update.new_update_by_row_id()
            assert isinstance(updater, NativeTableUpdateByRowId)
            updater.update_columns(data.slice(0, 1), ['payload'])
            updater.update_columns(data.slice(1), ['payload'])
            messages = updater.commit_messages
        elif operation == 'predicate':
            predicate = PredicateBuilder(table.fields).is_in('id', [0, 2])

            def assignment(matched):
                return _variants([replacement if index == 0 else None
                                  for index in matched['id'].to_pylist()])

            args = (predicate, {'payload': assignment})
            messages = (update.update_by_predicate(*args, 7, read_columns=['id']) if stream
                        else update.update_by_predicate(*args, read_columns=['id']))
        elif operation == 'ordinary_column':
            update = builder.new_update().with_update_type(['value'])
            data = data.drop(['payload']).append_column('value', pa.array([100, 200], type=pa.int32()))
            messages = (update.update_by_arrow_with_row_id(data, 7) if stream
                        else update.update_by_arrow_with_row_id(data))
        else:
            source = pa.table({'id': [0, 2, 4], 'payload': _variants([replacement, None, [1, 2]]),
                               'unchanged': _variants([None, None, {'count': 14, 'name': 'untouched'}]),
                               'value': [999, 999, 14]}, schema=schema)
            messages = (update.upsert_by_arrow_with_key(source, ['id'], 7) if stream
                        else update.upsert_by_arrow_with_key(source, ['id']))
    assert messages
    for message in messages:
        for file in message.new_files:
            physical = pq.read_schema(file.file_path)
            if 'payload' in physical.names:
                names = [field.name for field in physical.field('payload').type]
                assert 'typed_value' in names
            if file.write_cols == ['value']:
                assert physical.names == ['value']
    commit = builder.new_commit()
    try:
        commit.commit(messages, 7) if stream else commit.commit(messages)
    finally:
        commit.close()
    expected_payload = original if operation == 'ordinary_column' else [replacement, original[1], None, original[3]]
    expected_values = [100, 11, 200, 13] if operation == 'ordinary_column' else [10, 11, 12, 13]
    if operation == 'upsert':
        expected_payload = expected_payload + [[1, 2]]
        expected_values += [14]
        untouched = untouched + [{'count': 14, 'name': 'untouched'}]
    for native in (False, True):
        rows = _read(table, native)
        assert [row['payload'] for row in rows] == expected_payload
        assert [row['unchanged'] for row in rows] == untouched
        assert [row['value'] for row in rows] == expected_values
