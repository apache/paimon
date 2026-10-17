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

"""Equivalent Arrow layouts must not send upsert matching back to Python."""

from contextlib import ExitStack
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.write.table_update import TableUpdate
from pypaimon.write.table_update_by_row_id import TableUpdateByRowId
from pypaimon.write.table_upsert_by_key import TableUpsertByKey


pytestmark = pytest.mark.native_plan

_LAYOUTS = [
    (pa.string(), pa.large_string(), ['old', '', None, '中文']),
    (pa.list_(pa.string()), pa.list_(pa.large_string()),
     [['old'], [], None, ['中文', None, '']]),
    (pa.struct([('text', pa.string()), ('count', pa.int32())]),
     pa.struct([('text', pa.large_string()), ('count', pa.int32())]),
     [{'text': 'old', 'count': 1}, None, {'text': None, 'count': 2}, {'text': '中文', 'count': 3}]),
    (pa.map_(pa.string(), pa.list_(pa.string())),
     pa.map_(pa.large_string(), pa.list_(pa.large_string())),
     [[('old', ['first'])], [], None, [('中文', [None, ''])]]),
]


def _table(catalog, key_type, value_type, native):
    catalog.create_database('default', True)
    schema = pa.schema([('key', key_type), ('pt', pa.string()),
                        ('value', value_type), ('keep', pa.int32())])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, partition_keys=['pt'], options={
        'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true',
        'write.native.enabled': str(native).lower(),
    }), False)
    table = catalog.get_table('default.t')
    return table, PyarrowFieldParser.from_paimon_schema(table.fields)


def _commit(builder, messages, stream=False):
    commit = builder.new_commit()
    try:
        commit.commit(messages, 7) if stream else commit.commit(messages)
    finally:
        commit.close()


def _seed(table, schema, keys, values):
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    try:
        writer.write_arrow(pa.table({'key': keys, 'pt': ['a', 'a', 'a', 'b'],
                                    'value': values, 'keep': [0, 1, 2, 3]}, schema=schema))
        _commit(builder, writer.prepare_commit())
    finally:
        writer.close()


def _native_only(native):
    stack = ExitStack()
    if native:
        for owner, method in [(TableUpsertByKey, '_upsert_partition'),
                              (TableUpdateByRowId, '_load_existing_files_info'),
                              (TableUpdate, '_build_predicate_update_table')]:
            stack.enter_context(patch.object(owner, method, side_effect=AssertionError('Python update fallback')))
    return stack


def _check(table, values, key, new_key='new'):
    # Check both readers, including the row identity of the two updated rows.
    for native in (False, True):
        view = table.copy({'read.native.enabled': str(native).lower(),
                           'scan.native-plan.enabled': str(native).lower()})
        reader = view.new_read_builder().with_projection(['key', 'pt', 'value', 'keep', '_ROW_ID'])
        result = reader.new_read().to_arrow(reader.new_scan().plan().splits()).to_pylist()
        existing = sorted([row for row in result if row['keep'] is not None], key=lambda row: row['keep'])
        assert [row['value'] for row in existing] == [values[3], values[3], values[2], values[0]]
        assert [row['key'] for row in existing] == [key, key, None, key]
        assert [row['pt'] for row in existing] == ['a', 'a', 'a', 'b']
        assert sorted(row['_ROW_ID'] for row in existing) == [0, 1, 2, 3]
        added = [row for row in result if row['keep'] is None]
        assert len(added) == 1
        assert added[0]['key'] == new_key
        assert added[0]['value'] == values[1] and added[0]['pt'] == 'a'
        assert added[0]['_ROW_ID'] == 4


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('stream', [False, True])
@pytest.mark.parametrize('value_type,input_type,values', _LAYOUTS)
def test_upsert_large_strings_in_keys_partitions_and_nested_values(
        tmp_path, native, stream, value_type, input_type, values):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    table, schema = _table(catalog, pa.string(), value_type, native)
    _seed(table, schema, ['key', 'key', None, 'key'], [values[0]] * 4)
    source = pa.table({
        'value': pa.array([values[0], values[1], values[2], values[3]], type=input_type),
        'pt': pa.array(['a'] * 4, type=pa.large_string()),
        'key': pa.array(['key', 'new', None, 'key'], type=pa.large_string()),
    })
    # Chunks have separate string offsets; the duplicate key spans chunks.
    source = pa.concat_tables([source.slice(0, 1), source.slice(1, 2), source.slice(3)])
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    with _native_only(native):
        update = builder.new_update().with_update_type(['value'])
        messages = (update.upsert_by_arrow_with_key(source, ['key'], 7) if stream
                    else update.upsert_by_arrow_with_key(source, ['key']))
    _commit(builder, messages, stream)
    _check(table, values, 'key')


@pytest.mark.parametrize('native', [False, True])
def test_upsert_equivalent_binary_keys_on_rest_table(native_rest_catalog, native):
    table, schema = _table(native_rest_catalog, pa.binary(), pa.string(), native)
    _seed(table, schema, [b'ab', b'ab', None, b'ab'], ['old'] * 4)
    source = pa.table({'key': pa.array([b'ab', b'cd', None, b'ab'], type=pa.binary(2)),
                       'pt': ['a'] * 4,
                       'value': pa.array(['discarded', '', None, '中文'], type=pa.large_string())})
    builder = table.new_batch_write_builder()
    with _native_only(native):
        messages = builder.new_update().with_update_type(['value']).upsert_by_arrow_with_key(source, ['key'])
    _commit(builder, messages)
    _check(table, ['old', '', None, '中文'], b'ab', b'cd')


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('visible_null', [False, True])
@pytest.mark.parametrize('operation', ['upsert', 'row_id', 'predicate'])
def test_large_strings_preserve_required_nested_fields(tmp_path, native, visible_null, operation):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    value_type = pa.struct([pa.field('text', pa.string(), nullable=False)])
    table, schema = _table(catalog, pa.string(), value_type, native)
    _seed(table, schema, ['key', 'key', None, 'key'], [{'text': 'old'}] * 4)
    read = table.new_read_builder().with_projection(['keep', '_ROW_ID'])
    row_ids = {row['keep']: row['_ROW_ID'] for row in
               read.new_read().to_arrow(read.new_scan().plan().splits()).to_pylist()}
    source = pa.table({
        'key': ['key', None], 'pt': ['a', 'a'],
        'value': pa.array([None, {'text': None if visible_null else '中文'}],
                          type=pa.struct([('text', pa.large_string())])),
    })
    files = set(tmp_path.rglob('*.parquet'))
    builder = table.new_batch_write_builder()
    with _native_only(native):
        update = builder.new_update().with_update_type(['value'])

        def prepare():
            if operation == 'upsert':
                return update.upsert_by_arrow_with_key(source, ['key'])
            if operation == 'row_id':
                return update.update_by_arrow_with_row_id(
                    source.select(['value']).append_column(
                        '_ROW_ID', pa.array([row_ids[0], row_ids[2]], type=pa.int64())))
            predicate = update.new_predicate_builder().equal('pt', 'a')

            def assignment(matched):
                # The two equal keys fan out to both original rows; NULL is
                # selected independently, without changing the other partition.
                return pa.array([None if row['key'] is not None else source['value'][1].as_py()
                                 for row in matched.to_pylist()], type=pa.struct([
                                     pa.field('text', pa.large_string(), nullable=False)]))

            return update.update_by_predicate(predicate, {'value': assignment}, read_columns=['key'])

        if visible_null:
            with pytest.raises((ValueError, pa.ArrowInvalid)):
                prepare()
            assert set(tmp_path.rglob('*.parquet')) == files
        else:
            messages = prepare()
            _commit(builder, messages)
            reader = table.new_read_builder()
            result = reader.new_read().to_arrow(reader.new_scan().plan().splits()).sort_by('keep')
            assert result['value'].to_pylist() == [
                None, {'text': 'old'} if operation == 'row_id' else None,
                {'text': '中文'}, {'text': 'old'}]
