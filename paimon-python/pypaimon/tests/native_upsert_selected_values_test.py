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

"""Upsert converts only values selected by matching and last-write-wins."""

import datetime
from contextlib import ExitStack
from decimal import Decimal
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.write.table_upsert_by_key import TableUpsertByKey


pytestmark = pytest.mark.native_plan


def _table(tmp_path, native, value_type=pa.int32()):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([('key', pa.int32()), ('pt', pa.string()),
                        ('value', value_type), ('keep', pa.int32())])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, partition_keys=['pt'], options={
        'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true',
        'write.native.enabled': str(native).lower(),
    }), False)
    table = catalog.get_table('default.t')
    return table, PyarrowFieldParser.from_paimon_schema(table.fields)


def _seed(table, schema, value):
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    commit = builder.new_commit()
    try:
        writer.write_arrow(pa.table({'key': [1, 1, 1], 'pt': ['a', 'a', 'b'],
                                    'value': [value] * 3, 'keep': [10, 11, 12]}, schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    read = table.new_read_builder().with_projection(['keep', '_ROW_ID'])
    rows = read.new_read().to_arrow(read.new_scan().plan().splits()).to_pylist()
    return [row['_ROW_ID'] for row in sorted(rows, key=lambda row: row['keep'])]


def _upsert(table, source, native, stream, columns=('value',)):
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    with ExitStack() as stack:
        if native:
            stack.enter_context(patch.object(
                TableUpsertByKey, '_upsert_partition',
                side_effect=AssertionError('upsert fell back to Python')))
        update = builder.new_update().with_update_type(list(columns))
        messages = (update.upsert_by_arrow_with_key(source, ['key'], 17) if stream
                    else update.upsert_by_arrow_with_key(source, ['key']))
    commit = builder.new_commit()
    try:
        commit.commit(messages, 17) if stream else commit.commit(messages)
    finally:
        commit.close()
    return messages


def _rows(table, native_modes=(False, True)):
    for native in native_modes:
        view = table.copy({'read.native.enabled': str(native).lower(),
                           'scan.native-plan.enabled': str(native).lower()})
        builder = view.new_read_builder().with_projection(['key', 'pt', 'value', 'keep', '_ROW_ID'])
        rows = builder.new_read().to_arrow(builder.new_scan().plan().splits()).to_pylist()
        yield sorted(rows, key=lambda row: row['keep'])


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('stream', [False, True])
@pytest.mark.parametrize('value_type,input_type,initial,value,expected', [
    (pa.int32(), pa.string(), 0, '42', 42),
    (pa.int32(), pa.float64(), 0, -42.9, -42),
    (pa.int64(), pa.int32(), 0, 42, 42),
    (pa.int32(), pa.null(), 0, None, None),
    (pa.decimal128(8, 2), pa.string(), Decimal('0.00'), '42.25', Decimal('42.25')),
    (pa.list_(pa.int32()), pa.list_(pa.string()), [], ['42', None, '-7'], [42, None, -7]),
    (pa.struct([('n', pa.int32())]), pa.struct([('n', pa.string())]),
     {'n': 0}, {'n': '42'}, {'n': 42}),
    (pa.binary(), pa.string(), b'old', '中文', '中文'.encode()),
    (pa.binary(), pa.null(), b'old', None, None),
    (pa.large_binary(), pa.string(), b'old', 'new', b'new'),
    (pa.large_binary(), pa.null(), b'old', None, None),
    (pa.int32(), pa.binary(), 0, b'42', 42),
    (pa.list_(pa.int32()), pa.list_(pa.binary()), [], [b'42', None], [42, None]),
    (pa.struct([('data', pa.binary()), ('n', pa.int32())]),
     pa.struct([('data', pa.binary()), ('n', pa.string())]),
     {'data': b'old', 'n': 0}, {'data': b'new', 'n': '42'}, {'data': b'new', 'n': 42}),
    (pa.struct([('data', pa.binary()), ('n', pa.string())]),
     pa.struct([('data', pa.binary()), ('n', pa.int32())]),
     {'data': b'old', 'n': 'old'}, {'data': b'new', 'n': 42}, {'data': b'new', 'n': '42'}),
    (pa.map_(pa.string(), pa.binary()), pa.struct([('a', pa.string())]),
     [('a', b'old')], {'a': 'new'}, [('a', b'new')]),
    (pa.map_(pa.string(), pa.large_binary()), pa.map_(pa.string(), pa.string()),
     [('a', b'old')], [('a', 'new')], [('a', b'new')]),
    (pa.list_(pa.int32()), pa.struct([('dummy', pa.int32())]), [], None, None),
])
def test_matched_values_use_row_id_update_casts(
        tmp_path, native, stream, value_type, input_type, initial, value, expected):
    table, schema = _table(tmp_path, native, value_type)
    row_ids = _seed(table, schema, initial)
    source = pa.table({'key': pa.array([1], type=pa.int32()), 'pt': ['a'],
                       'value': pa.array([value], type=input_type)})
    _upsert(table, source, native, stream)
    for rows in _rows(table):
        assert [row['value'] for row in rows] == [expected, expected, initial]
        assert [row['keep'] for row in rows] == [10, 11, 12]
        assert [row['_ROW_ID'] for row in rows] == row_ids


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('stream', [False, True])
def test_matched_rows_ignore_unwritten_payload_columns(tmp_path, native, stream):
    table, schema = _table(tmp_path, native)
    _seed(table, schema, 0)
    source = pa.table({'key': pa.array([1], type=pa.int32()), 'pt': ['a'],
                       'value': pa.array([42], type=pa.int32()), 'keep': ['not an integer']})
    _upsert(table, source, native, stream)
    for rows in _rows(table):
        assert [row['value'] for row in rows] == [42, 42, 0]
        assert [row['keep'] for row in rows] == [10, 11, 12]


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('stream', [False, True])
@pytest.mark.parametrize('encoded_payload', [False, True], ids=['map', 'dictionary-map'])
@pytest.mark.parametrize('target_key,input_key,initial,value,expected', [
    (pa.int32(), pa.int64(), 1, 2, 2),
    (pa.string(), pa.dictionary(pa.int8(), pa.string()), 'old', 'new', 'new'),
    (pa.decimal128(10, 2), pa.decimal128(8, 3), Decimal('1.00'), Decimal('1.235'), Decimal('1.24')),
    (pa.time32('ms'), pa.time64('us'), datetime.time(1), datetime.time(1, 2, 3, 123456),
     datetime.time(1, 2, 3, 123000)),
])
def test_matched_blob_map_keys_follow_java_serialization(
        tmp_path, native, stream, encoded_payload, target_key, input_key, initial, value, expected):
    table, schema = _table(tmp_path, native, pa.map_(target_key, pa.large_binary()))
    row_ids = _seed(table, schema, [(initial, b'old')])
    keys = (pa.array([value], type=input_key.value_type).dictionary_encode()
            if pa.types.is_dictionary(input_key) else pa.array([value], type=input_key))
    payload = pa.MapArray.from_arrays(
        pa.array([0, 1], type=pa.int32()), keys, pa.array([b'new'], type=pa.large_binary()))
    if encoded_payload:
        payload = pa.DictionaryArray.from_arrays(pa.array([0], type=pa.int8()), payload)
    source = pa.table({'key': pa.array([1], type=pa.int32()), 'pt': ['a'], 'value': payload})
    _upsert(table, source, native, stream)
    for rows in _rows(table):
        assert [row['value'] for row in rows] == [[(expected, b'new')]] * 2 + [[(initial, b'old')]]
        assert [row['_ROW_ID'] for row in rows] == row_ids
    # An unmatched outer key is an ordinary append, whose Arrow types remain strict.
    source = source.set_column(0, 'key', pa.array([2], type=pa.int32()))
    with pytest.raises((ValueError, TypeError, NotImplementedError)):
        _upsert(table, source, native, stream)
    for rows in _rows(table):
        assert [row['_ROW_ID'] for row in rows] == row_ids


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('key_type,input_type,initial,values', [
    (pa.time32('ms'), pa.time64('us'), datetime.time(1),
     [datetime.time(1, 2, 3, 123001), datetime.time(1, 2, 3, 123999)]),
    (pa.decimal128(10, 2), pa.decimal128(8, 3), Decimal('1.00'), [Decimal('1.231'), Decimal('1.234')]),
])
def test_blob_map_duplicate_keys_are_rejected_after_java_normalization(
        tmp_path, native, key_type, input_type, initial, values):
    table, schema = _table(tmp_path, native, pa.map_(key_type, pa.large_binary()))
    row_ids = _seed(table, schema, [(initial, b'old')])
    source = pa.table({'key': pa.array([1], type=pa.int32()), 'pt': ['a'],
                       'value': pa.array([[(value, b'new') for value in values]],
                                         type=pa.map_(input_type, pa.large_binary()))})
    with pytest.raises(ValueError, match='keys must be unique'):
        _upsert(table, source, native, False)
    for rows in _rows(table):
        assert [row['value'] for row in rows] == [[(initial, b'old')]] * 3
        assert [row['_ROW_ID'] for row in rows] == row_ids


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('stream', [False, True])
def test_discarded_duplicate_values_are_not_cast(tmp_path, native, stream):
    table, schema = _table(tmp_path, native)
    _seed(table, schema, 0)
    first = pa.table({'key': pa.array([1], type=pa.int32()), 'pt': ['a'], 'value': ['invalid']})
    last = pa.table({'key': pa.array([1], type=pa.int32()), 'pt': ['a'], 'value': ['42']})
    # The winning key is in a separate source chunk.
    _upsert(table, pa.concat_tables([first, last]), native, stream)
    for rows in _rows(table):
        assert [row['value'] for row in rows] == [42, 42, 0]


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('stream', [False, True])
def test_discarded_duplicates_do_not_validate_required_nested_children(tmp_path, native, stream):
    value_type = pa.struct([pa.field('n', pa.int32(), nullable=False), pa.field('label', pa.string())])
    table, schema = _table(tmp_path, native, value_type)
    _seed(table, schema, {'n': 0, 'label': 'old'})
    source = pa.table({'key': pa.array([1, 1], type=pa.int32()), 'pt': ['a', 'a'],
                       'value': pa.array([{'n': None, 'label': 'discarded'}, {'n': 42, 'label': 'final'}],
                                         type=pa.struct([('n', pa.int32()), ('label', pa.string())]))})
    _upsert(table, source, native, stream)
    # Python's format reader cannot restore required ROW child metadata. Read
    # the files from both update implementations through the native reader.
    for rows in _rows(table, native_modes=(True,)):
        assert [row['value'] for row in rows] == [
            {'n': 42, 'label': 'final'}, {'n': 42, 'label': 'final'}, {'n': 0, 'label': 'old'}]


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('stream', [False, True])
def test_invalid_surviving_update_value_propagates_without_retry(tmp_path, native, stream):
    table, schema = _table(tmp_path, native)
    _seed(table, schema, 0)
    source = pa.table({'key': pa.array([1, 1], type=pa.int32()), 'pt': ['a', 'a'],
                       'value': ['42', 'invalid']})
    with pytest.raises((ValueError, TypeError)):
        _upsert(table, source, native, stream)
    for rows in _rows(table):
        assert [row['value'] for row in rows] == [0, 0, 0]


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('stream', [False, True])
def test_new_rows_keep_ordinary_write_type_validation(tmp_path, native, stream):
    table, schema = _table(tmp_path, native)
    row_ids = _seed(table, schema, 0)
    source = pa.table({'key': pa.array([2], type=pa.int32()), 'pt': ['a'], 'value': ['42']})
    with pytest.raises((ValueError, TypeError, NotImplementedError)):
        _upsert(table, source, native, stream)
    for rows in _rows(table):
        assert [row['value'] for row in rows] == [0, 0, 0]
        assert [row['_ROW_ID'] for row in rows] == row_ids


@pytest.mark.parametrize('target_type,source_type,initial,value', [
    (pa.binary(), pa.large_binary(), b'old', b'new'),
    (pa.large_binary(), pa.binary(), b'old', b'new'),
    (pa.map_(pa.string(), pa.binary()),
     pa.map_(pa.field('k', pa.string(), nullable=False), pa.field('v', pa.large_binary())),
     [('a', b'old')], [('a', b'new')]),
    (pa.map_(pa.string(), pa.binary()), pa.struct([('a', pa.large_binary())]),
     [('a', b'old')], {'a': b'new'}),
    (pa.struct([('a', pa.binary())]), pa.map_(pa.string(), pa.large_binary()),
     {'a': b'old'}, [('a', b'new')]),
    (pa.map_(pa.string(), pa.binary()),
     pa.list_(pa.list_(pa.large_binary())),
     [('a', b'old')], [[b'a', b'new']]),
])
def test_native_upsert_does_not_add_binary_width_conversions(
        tmp_path, target_type, source_type, initial, value):
    table, schema = _table(tmp_path, True, target_type)
    row_ids = _seed(table, schema, initial)
    source = pa.table({'key': pa.array([1], type=pa.int32()), 'pt': ['a'],
                       'value': pa.array([value], type=source_type)})
    with pytest.raises(ValueError, match='Arrow write type .* is incompatible with table type'):
        _upsert(table, source, True, False)
    for rows in _rows(table):
        assert [row['value'] for row in rows] == [initial] * 3
        assert [row['_ROW_ID'] for row in rows] == row_ids
