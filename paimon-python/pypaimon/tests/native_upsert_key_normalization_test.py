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

"""Logical upsert keys must not depend on Arrow widths or dictionaries."""

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

_KEYS = [
    (pa.int8(), pa.int64(), 1, 2),
    (pa.int16(), pa.uint32(), 1, 2),
    (pa.int32(), pa.int64(), 1, 2),
    (pa.int64(), pa.uint64(), 1, 2),
    (pa.decimal128(10, 2), pa.decimal128(15, 4), Decimal('1.25'), Decimal('-2.50')),
    (pa.date32(), pa.date64(), datetime.date(1969, 12, 31), datetime.date(1970, 1, 2)),
    (pa.time32('ms'), pa.time64('us'), datetime.time(1, 2, 3, 400000), datetime.time(4, 5, 6)),
    (pa.timestamp('ms'), pa.timestamp('us'), datetime.datetime(1969, 12, 31, 23, 59, 59, 999000),
     datetime.datetime(1970, 1, 2)),
    (pa.timestamp('us'), pa.timestamp('ms'), datetime.datetime(1969, 12, 31, 23, 59, 59, 999000),
     datetime.datetime(1970, 1, 2)),
    (pa.timestamp('us', 'UTC'), pa.timestamp('ms', 'America/New_York'),
     datetime.datetime(2024, 7, 1, tzinfo=datetime.timezone.utc),
     datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)),
    (pa.string(), pa.dictionary(pa.int8(), pa.string()), '中文', 'other'),
    (pa.int32(), pa.dictionary(pa.int16(), pa.int64()), 1, 2),
    (pa.binary(), pa.dictionary(pa.int8(), pa.binary()), b'key', b'other'),
]


def _array(values, data_type):
    if pa.types.is_dictionary(data_type):
        return pa.array(values, type=data_type.value_type).dictionary_encode().cast(data_type)
    return pa.array(values, type=data_type)


def _table(tmp_path, native, key_type, key, other, partitioned=False):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([('key', key_type), ('pt', pa.string()), ('value', pa.int32()), ('keep', pa.int32())])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        schema, partition_keys=['pt'] if partitioned else None, options={
            'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true',
            'write.native.enabled': str(native).lower(),
        }), False)
    table = catalog.get_table('default.t')
    schema = PyarrowFieldParser.from_paimon_schema(table.fields)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.table({'key': [key, key, other, None], 'pt': ['a', 'a', 'b', 'a'],
                                    'value': [0] * 4, 'keep': [0, 1, 2, 3]}, schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    return table, schema


def _upsert(table, source, native, stream, columns=('value',)):
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    with ExitStack() as stack:
        if native:
            stack.enter_context(patch.object(
                TableUpsertByKey, '_upsert_partition', side_effect=AssertionError('Python key matching')))
        update = builder.new_update()
        if columns is not None:
            update.with_update_type(list(columns))
        messages = (update.upsert_by_arrow_with_key(source, ['key'], 37) if stream
                    else update.upsert_by_arrow_with_key(source, ['key']))
    commit = builder.new_commit()
    try:
        commit.commit(messages, 37) if stream else commit.commit(messages)
    finally:
        commit.close()


def _rows(table):
    for native in (False, True):
        view = table.copy({'read.native.enabled': str(native).lower(),
                           'scan.native-plan.enabled': str(native).lower()})
        builder = view.new_read_builder().with_projection(['key', 'pt', 'value', 'keep', '_ROW_ID'])
        rows = builder.new_read().to_arrow(builder.new_scan().plan().splits()).to_pylist()
        yield sorted(rows, key=lambda row: row['keep'])


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
@pytest.mark.parametrize('key_type,input_type,key,other', _KEYS)
def test_upsert_normalizes_match_keys_without_changing_row_identity(
        tmp_path, native, stream, key_type, input_type, key, other):
    table, _ = _table(tmp_path, native, key_type, key, other)
    before = next(_rows(table))
    # Different dictionaries and the last duplicate occupy separate chunks.
    chunks = [pa.table({'key': _array([value], input_type), 'value': pa.array([number], type=pa.int32())})
              for value, number in [(key, 1), (None, 9), (key, 42)]]
    _upsert(table, pa.concat_tables(chunks), native, stream)
    for rows in _rows(table):
        assert [row['value'] for row in rows] == [42, 42, 0, 9]
        assert [{k: v for k, v in row.items() if k != 'value'} for row in rows] == [
            {k: v for k, v in row.items() if k != 'value'} for row in before]


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
def test_partition_key_dictionaries_and_integer_widths_share_one_identity(tmp_path, native, stream):
    table, _ = _table(tmp_path, native, pa.int32(), 1, 2, partitioned=True)
    source = pa.table({'key': pa.array([1, 2, 1], type=pa.int64()),
                       'pt': _array(['a', 'b', 'a'], pa.dictionary(pa.int8(), pa.string())),
                       'value': pa.array([1, 7, 42], type=pa.int32())})
    before = next(_rows(table))
    _upsert(table, source, native, stream)
    for rows in _rows(table):
        assert [row['value'] for row in rows] == [42, 42, 7, 0]
        assert [row['_ROW_ID'] for row in rows] == [row['_ROW_ID'] for row in before]


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('key_type,input_type,key,other,new_key', [
    (pa.int32(), pa.int64(), 1, 2, 3),
    (pa.decimal128(10, 2), pa.decimal128(15, 4), Decimal('1.00'), Decimal('2.00'), Decimal('3.00')),
    (pa.timestamp('ms'), pa.timestamp('us'), datetime.datetime(2024, 1, 1),
     datetime.datetime(2024, 1, 2), datetime.datetime(2024, 1, 3)),
])
def test_new_rows_retain_original_key_write_type_validation(
        tmp_path, native, key_type, input_type, key, other, new_key):
    table, _ = _table(tmp_path, native, key_type, key, other)
    before = next(_rows(table))
    source = pa.table({'key': _array([new_key], input_type), 'value': pa.array([42], type=pa.int32())})
    with pytest.raises((ValueError, TypeError, NotImplementedError)):
        _upsert(table, source, native, False)
    for rows in _rows(table):
        assert rows == before


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('key_type,input_type,key,other,invalid', [
    (pa.int8(), pa.int64(), 1, 2, 256),
    (pa.int64(), pa.uint64(), 1, 2, 2 ** 63),
    (pa.decimal128(10, 2), pa.decimal128(15, 4), Decimal('1.25'), Decimal('2.50'), Decimal('1.2501')),
    (pa.timestamp('ms'), pa.timestamp('us'), datetime.datetime(2024, 1, 1),
     datetime.datetime(2024, 1, 2), datetime.datetime(2024, 1, 1, 0, 0, 0, 1)),
])
def test_lossy_key_conversion_cannot_update_an_existing_row(
        tmp_path, native, key_type, input_type, key, other, invalid):
    table, _ = _table(tmp_path, native, key_type, key, other)
    before = next(_rows(table))
    source = pa.table({'key': _array([invalid], input_type), 'value': pa.array([42], type=pa.int32())})
    with pytest.raises((ValueError, TypeError, NotImplementedError)):
        _upsert(table, source, native, False)
    for rows in _rows(table):
        assert rows == before


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
@pytest.mark.parametrize('key_type,key,other', [
    (pa.int32(), 1, 2), (pa.string(), '中文', 'other'), (pa.binary(), b'key', b'other'),
])
def test_dictionary_keys_can_also_be_written_by_full_row_updates(tmp_path, native, stream, key_type, key, other):
    table, _ = _table(tmp_path, native, key_type, key, other)
    before = next(_rows(table))
    source = pa.table({'key': _dictionary_array([key], key_type), 'pt': ['updated'],
                       'value': pa.array([42], type=pa.int32()), 'keep': pa.array([99], type=pa.int32())})
    _upsert(table, source, native, stream, columns=None)
    for rows in _rows(table):
        by_id = {row['_ROW_ID']: row for row in rows}
        for original in before:
            expected = dict(original)
            if original['key'] == key:
                expected.update(pt='updated', value=42, keep=99)
            assert by_id[original['_ROW_ID']] == expected


def _dictionary_array(values, value_type):
    return pa.array(values, type=value_type).dictionary_encode()


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
def test_untyped_null_keys_match_only_existing_null_key_rows(tmp_path, native):
    table, _ = _table(tmp_path, native, pa.int32(), 1, 2)
    source = pa.table({'key': pa.nulls(1), 'value': pa.array([9], type=pa.int32())})
    _upsert(table, source, native, False)
    for rows in _rows(table):
        assert [row['value'] for row in rows] == [0, 0, 0, 9]


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
def test_dictionary_null_values_and_null_indices_share_upsert_identity(tmp_path, native, stream):
    table, _ = _table(tmp_path, native, pa.string(), 'key', 'other')
    keys = pa.DictionaryArray.from_arrays(pa.array([0, 1, None], type=pa.int8()), pa.array([None, 'key']))
    source = pa.table({'key': keys, 'value': pa.array([9, 42, 11], type=pa.int32())})
    _upsert(table, source, native, stream)
    for rows in _rows(table):
        assert [row['value'] for row in rows] == [42, 42, 0, 11]
