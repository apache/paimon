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
import mmap
import os
import struct
import sys
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
    (pa.float32(), pa.float64(), 1.5, -2.25),
    (pa.float64(), pa.float32(), 1.5, -2.25),
    (pa.float64(), pa.dictionary(pa.int8(), pa.float32()), 1.5, -2.25),
    (pa.binary(), pa.large_binary(), b'\xff\0', b'other'),
    (pa.large_binary(), pa.binary(), b'\xff\0', b'other'),
    (pa.binary(), pa.dictionary(pa.int8(), pa.large_binary()), b'\xff\0', b'other'),
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

_NUMERIC_KEYS = [
    (pa.int32(), pa.float64(), 1, 1.0, 2),
    (pa.int64(), pa.float64(), 2 ** 53 + 2, float(2 ** 53 + 2), 2 ** 53 + 4),
    (pa.float32(), pa.int64(), 1.0, 1, 2.0),
    (pa.float64(), pa.int64(), float(2 ** 53 + 2), 2 ** 53 + 2, float(2 ** 53 + 4)),
    (pa.decimal128(10, 2), pa.int64(), Decimal('1.00'), 1, Decimal('2.00')),
    (pa.int32(), pa.decimal128(15, 4), 1, Decimal('1.0000'), 2),
    (pa.decimal128(10, 2), pa.float64(), Decimal('1.50'), 1.5, Decimal('2.25')),
    (pa.float64(), pa.decimal128(10, 2), 1.5, Decimal('1.50'), 2.25),
    (pa.decimal128(38, 38), pa.float64(), Decimal('0'), 0.0, Decimal('1E-38')),
    (pa.float32(), pa.decimal256(40, 4), 1.5, Decimal('1.5000'), 2.25),
    (pa.int64(), pa.dictionary(pa.int8(), pa.float64()), 1, 1.0, 2),
    (pa.float64(), pa.dictionary(pa.int8(), pa.decimal128(10, 2)), 1.5, Decimal('1.50'), 2.25),
    (pa.float64(), pa.decimal256(76, 55), 0.1, Decimal.from_float(0.1), 0.25),
    (pa.float64(), pa.decimal256(76, 70), 2.0 ** -70, Decimal.from_float(2.0 ** -70), 2.0 ** -69),
    (pa.float64(), pa.decimal256(76, -20), 1e20, Decimal('1E+20'), 2e20),
    (pa.float32(), pa.uint32(), float(2 ** 24 + 2), 2 ** 24 + 2, float(2 ** 24 + 4)),
    (pa.float64(), pa.uint64(), float(2 ** 64 - 2048), 2 ** 64 - 2048, 1.0),
    (pa.decimal128(38, 38), pa.int64(), Decimal('0'), 0, Decimal('1E-38')),
]

for _width in (32, 64):
    _factory = getattr(pa, 'decimal{}'.format(_width), None)
    if _factory is not None:
        _NUMERIC_KEYS.append((pa.float64(), _factory(9, 2), 1.5, Decimal('1.50'), 2.25))


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
@pytest.mark.parametrize('key_type,input_type,key,input_key,other', _NUMERIC_KEYS)
def test_numeric_keys_match_by_exact_value_across_arrow_types(
        tmp_path, native, stream, key_type, input_type, key, input_key, other):
    table, _ = _table(tmp_path, native, key_type, key, other)
    before = next(_rows(table))
    chunks = [pa.table({'key': _array([value], input_type), 'value': pa.array([number], type=pa.int32())})
              for value, number in [(input_key, 1), (None, 9), (input_key, 42)]]
    _upsert(table, pa.concat_tables(chunks), native, stream)
    for rows in _rows(table):
        assert [row['value'] for row in rows] == [42, 42, 0, 9]
        assert [{k: v for k, v in row.items() if k != 'value'} for row in rows] == [
            {k: v for k, v in row.items() if k != 'value'} for row in before]


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('key_type,input_type,key,invalid,other', [
    (pa.int32(), pa.float64(), 1, 1.00000001, 2),
    (pa.int64(), pa.float64(), 2 ** 63 - 1, float(2 ** 63), 2),
    (pa.float64(), pa.int64(), float(2 ** 53), 2 ** 53 + 1, 2.0),
    (pa.decimal128(10, 2), pa.float64(), Decimal('0.10'), 0.1, Decimal('0.20')),
    (pa.float64(), pa.decimal128(10, 2), 0.1, Decimal('0.10'), 0.2),
])
def test_numeric_key_conversion_cannot_round_into_an_existing_row(
        tmp_path, native, key_type, input_type, key, invalid, other):
    table, _ = _table(tmp_path, native, key_type, key, other)
    before = next(_rows(table))
    source = pa.table({'key': _array([invalid], input_type), 'value': pa.array([42], type=pa.int32())})
    with pytest.raises((ValueError, TypeError, NotImplementedError)):
        _upsert(table, source, native, False)
    for rows in _rows(table):
        assert rows == before


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('key_type,input_type,key,input_key,other', _NUMERIC_KEYS[:8])
def test_mixed_numeric_upsert_validates_original_unmatched_rows_before_updates(
        tmp_path, native, key_type, input_type, key, input_key, other):
    table, _ = _table(tmp_path, native, key_type, key, other)
    before = next(_rows(table))
    source = pa.table({'key': _array([input_key, input_key + 4], input_type),
                       'value': pa.array([42, 99], type=pa.int32())})
    with pytest.raises((ValueError, TypeError, NotImplementedError)):
        _upsert(table, source, native, False)
    for rows in _rows(table):
        assert rows == before


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
@pytest.mark.parametrize('key_type,input_type,key,input_key,other', _NUMERIC_KEYS[:3] + _NUMERIC_KEYS[4:8])
def test_full_row_numeric_upsert_preserves_row_identity_and_payload_casts(
        tmp_path, native, stream, key_type, input_type, key, input_key, other):
    table, _ = _table(tmp_path, native, key_type, key, other)
    before = next(_rows(table))
    source = pa.table({'key': _array([input_key], input_type), 'pt': ['updated'],
                       'value': pa.array([42], type=pa.int32()), 'keep': pa.array([99], type=pa.int32())})
    _upsert(table, source, native, stream, columns=None)
    for rows in _rows(table):
        by_id = {row['_ROW_ID']: row for row in rows}
        for original in before:
            expected = dict(original)
            if original['key'] == key:
                expected.update(pt='updated', value=42, keep=99)
            assert by_id[original['_ROW_ID']] == expected


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
def test_exact_match_does_not_relax_updated_key_assignment_range(tmp_path, native):
    key = float(2 ** 53 + 2)
    table, _ = _table(tmp_path, native, pa.float64(), key, float(2 ** 53 + 4))
    before = next(_rows(table))
    source = pa.table({'key': pa.array([2 ** 53 + 2], type=pa.int64()), 'pt': ['updated'],
                       'value': pa.array([42], type=pa.int32()), 'keep': pa.array([99], type=pa.int32())})
    # Matching only needs an exact numeric identity. Updating the key payload
    # still uses the ordinary integer-to-double assignment range (<= 2^53).
    with pytest.raises((ValueError, TypeError, NotImplementedError)):
        _upsert(table, source, native, False, columns=None)
    for rows in _rows(table):
        assert rows == before


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
@pytest.mark.parametrize('key_type,input_type,key,other', [
    (pa.int32(), pa.int64(), 1, 2),
    (pa.float64(), pa.float32(), 1.5, -2.25),
    (pa.binary(), pa.large_binary(), b'key', b'other'),
])
def test_partition_dictionaries_and_equivalent_key_layouts_share_one_identity(
        tmp_path, native, stream, key_type, input_type, key, other):
    table, _ = _table(tmp_path, native, key_type, key, other, partitioned=True)
    source = pa.table({'key': _array([key, other, key], input_type),
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
    (pa.float32(), pa.float64(), 1.5, -2.25, 3.5),
    (pa.float64(), pa.float32(), 1.5, -2.25, 3.5),
    (pa.binary(), pa.large_binary(), b'key', b'other', b'new'),
    (pa.large_binary(), pa.binary(), b'key', b'other', b'new'),
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
    (pa.float32(), pa.float64(), 1.0, 2.0, 1.00000001),
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


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
def test_decimal_zero_and_null_keys_rescale_across_the_full_scale_range(tmp_path, native, stream):
    table, _ = _table(tmp_path, native, pa.decimal128(38, 38), Decimal('0'), Decimal('1e-38'))
    source = pa.table({'key': pa.array([Decimal('0'), None], type=pa.decimal128(38, -2)),
                       'value': pa.array([42, 9], type=pa.int32())})
    _upsert(table, source, native, stream)
    for rows in _rows(table):
        assert [row['value'] for row in rows] == [42, 42, 0, 9]


@pytest.mark.skipif(os.name != 'posix' or sys.maxsize <= 2 ** 32, reason='Requires 64-bit POSIX mmap')
@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
def test_large_binary_key_slice_after_two_gib_matches_visible_bytes(tmp_path, native):
    table, _ = _table(tmp_path, native, pa.binary(), b'ab', b'other')
    before = next(_rows(table))
    start = 2 ** 31 + 17
    # Reserve address space without allocating or reading a 2 GiB prefix.
    memory = mmap.mmap(-1, start + 2)
    memory[start:start + 2] = b'ab'
    keys = pa.Array.from_buffers(pa.large_binary(), 1, [
        None, pa.py_buffer(struct.pack('<qq', start, start + 2)), pa.py_buffer(memory)])
    source = pa.table({'key': keys, 'value': pa.array([42], type=pa.int32())})
    _upsert(table, source, native, False)
    for rows in _rows(table):
        assert [row['value'] for row in rows] == [42, 42, 0, 0]
        assert [{k: v for k, v in row.items() if k != 'value'} for row in rows] == [
            {k: v for k, v in row.items() if k != 'value'} for row in before]


@pytest.mark.skipif(os.name != 'posix' or sys.maxsize <= 2 ** 32, reason='Requires 64-bit POSIX mmap')
@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('mixed', [False, True], ids=['all_null', 'mixed'])
def test_binary_match_keys_ignore_storage_owned_by_null_values(tmp_path, native, mixed):
    table, _ = _table(tmp_path, native, pa.binary(), b'ab', b'other')
    before = next(_rows(table))
    start = 2 ** 31 + 17
    memory = mmap.mmap(-1, start + 2)
    memory[start:start + 2] = b'ab'
    offsets = [0, start, start + 2] if mixed else [0, start]
    keys = pa.Array.from_buffers(pa.large_binary(), len(offsets) - 1, [
        pa.py_buffer(b'\x02'), pa.array(offsets, type=pa.int64()).buffers()[1], pa.py_buffer(memory)],
        null_count=1)
    source = pa.table({'key': keys, 'value': pa.array([9, 42] if mixed else [9], type=pa.int32())})
    _upsert(table, source, native, False)
    for rows in _rows(table):
        assert [row['value'] for row in rows] == ([42, 42, 0, 9] if mixed else [0, 0, 0, 9])
        assert [{k: v for k, v in row.items() if k != 'value'} for row in rows] == [
            {k: v for k, v in row.items() if k != 'value'} for row in before]
