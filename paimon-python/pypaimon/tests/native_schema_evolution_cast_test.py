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

"""Read existing Python schema changes through Native using REST tables."""

from contextlib import ExitStack
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.read.native_plan import native_runtime_available
from pypaimon.schema.data_types import AtomicType, DataField, MapType, PyarrowFieldParser, RowType
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.tests import native_plan_rest_test

rest_catalog = native_plan_rest_test.rest_catalog


pytestmark = [pytest.mark.native_plan, pytest.mark.skipif(
    not native_runtime_available(), reason='Rust reader required')]


def _write(table, data):
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(data)
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()


def _table(catalog, mode, arrow_type, values, paimon_type=None):
    options = {'file.format': 'parquet', 'read.batch-size': '2'}
    if mode == 'pk':
        options['bucket'] = '1'
    elif mode == 'de':
        options.update({'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true'})
    schema = pa.schema([('id', pa.int64()), ('v', arrow_type)])
    table_schema = Schema.from_pyarrow_schema(
        schema, primary_keys=['id'] if mode == 'pk' else [], options=options)
    if paimon_type is not None:
        table_schema.fields[1].type = paimon_type
    catalog.create_table('default.t', table_schema, False)
    table = catalog.get_table('default.t')
    _write(table, pa.table({'id': list(range(len(values))), 'v': pa.array(values, type=arrow_type)}))


def _read(table, native, predicate=None, projection=None):
    table = table.copy({'read.native.enabled': str(native).lower(),
                        'scan.native-plan.enabled': str(native).lower()})
    builder = table.new_read_builder()
    if predicate is not None:
        builder.with_filter(predicate)
    if projection is not None:
        builder.with_projection(projection)
    scan = builder.new_scan()
    with ExitStack() as stack:
        if native:
            stack.enter_context(patch.object(scan.file_scanner, 'scan',
                                             side_effect=AssertionError('Native plan fell back')))
            stack.enter_context(patch('pypaimon.read.table_read.TableRead._create_split_read',
                                      side_effect=AssertionError('Native read fell back')))
        plan = scan.plan()
        result = builder.new_read().to_arrow(plan.splits())
    return result.sort_by([('id', 'ascending')])


_TEMPORAL_STRING_CASES = [
    (pa.time32('ms'), [0, 100, 101, 12345678, None],
     ['00:00:00.0', '00:00:00.1', '00:00:00.101', '03:25:45.678', None]),
    (pa.timestamp('ms'), [0, 100, -1, 12345678, None],
     ['1970-01-01 00:00:00.000', '1970-01-01 00:00:00.100',
      '1969-12-31 23:59:59.999', '1970-01-01 03:25:45.678', None]),
    (pa.timestamp('ns'), [0, 123456789, -1, 1000000000, None],
     ['1970-01-01 00:00:00.000000000', '1970-01-01 00:00:00.123456789',
      '1969-12-31 23:59:59.999999999', '1970-01-01 00:00:01.000000000', None]),
]


@pytest.mark.parametrize('mode', ['append', 'pk', 'de'])
@pytest.mark.parametrize('source_type,values,expected', _TEMPORAL_STRING_CASES)
@pytest.mark.parametrize('target_name', ['STRING', 'VARCHAR(12)', 'CHAR(35)'])
def test_temporal_character_strings_read_old_rest_files(
        rest_catalog, mode, source_type, values, expected, target_name):
    _check_temporal_string_reads(rest_catalog, mode, source_type, values, expected, target_name)


def _check_temporal_string_reads(rest_catalog, mode, source_type, values, expected, target_name):
    catalog, _ = rest_catalog
    _table(catalog, mode, source_type, values,
           AtomicType('TIME(3)') if pa.types.is_time(source_type) else None)
    catalog.alter_table('default.t', [SchemaChange.update_column_type('v', AtomicType(target_name))], False)
    table = catalog.get_table('default.t')
    if target_name == 'VARCHAR(12)':
        expected = [value[:12] if value is not None else None for value in expected]
    elif target_name == 'CHAR(35)':
        expected = [value.ljust(35) if value is not None else None for value in expected]
    predicate = table.new_read_builder().new_predicate_builder().equal('v', expected[0])
    for native in (False, True):
        assert _read(table, native).column('v').to_pylist() == expected
        matches = [index for index, value in enumerate(expected) if value == expected[0]]
        assert _read(table, native, predicate, ['id']).column('id').to_pylist() == matches


@pytest.mark.parametrize('mode', ['append', 'de'])
@pytest.mark.parametrize('target_name', ['STRING', 'VARCHAR(12)', 'CHAR(35)'])
def test_wide_timestamp_strings_read_old_rest_files(rest_catalog, mode, target_name):
    # The existing Python PK writer materializes datetimes while merging rows,
    # so it cannot create this input; append and DE retain the raw Arrow ticks.
    _check_temporal_string_reads(rest_catalog, mode, pa.timestamp('ms'),
                                 [253402300800000, -62167219200000, 2 ** 63 - 1, -(2 ** 63), None],
                                 ['0000-01-01 00:00:00.000', '0000-01-01 00:00:00.000',
                                  '/+-.-04-06 07:12:55.807', '5471-09-26 16:47:04.192', None], target_name)


@pytest.mark.parametrize('mode', ['append', 'pk', 'de'])
def test_ltz_string_uses_local_zone_without_zone_suffix(rest_catalog, mode):
    catalog, _ = rest_catalog
    # Check a winter and a summer instant: the system zone may observe DST.
    instants = [datetime(2024, 1, 1, tzinfo=timezone.utc), datetime(2024, 7, 1, tzinfo=timezone.utc)]
    expected = [value.astimezone().strftime('%Y-%m-%d %H:%M:%S') + '.000' for value in instants] + [None]
    _table(catalog, mode, pa.timestamp('ms', 'UTC'), instants + [None])
    catalog.alter_table('default.t', [SchemaChange.update_column_type('v', AtomicType('STRING'))], False)
    for native in (False, True):
        assert _read(catalog.get_table('default.t'), native).column('v').to_pylist() == expected


_NUMERIC_CASES = [
    (pa.int64(), 'TINYINT', [128, -129, 2 ** 63 - 1, -(2 ** 63), None], [-128, 127, -1, 0, None]),
    (pa.int64(), 'SMALLINT', [32768, -32769, 2 ** 63 - 1, None], [-32768, 32767, -1, None]),
    (pa.int64(), 'INT', [2 ** 31, -(2 ** 31) - 1, 2 ** 63 - 1, None],
     [-(2 ** 31), 2 ** 31 - 1, -1, None]),
    (pa.float64(), 'TINYINT', [128.9, -129.9, 1e30, -1e30, None], [-128, 127, -1, 0, None]),
    (pa.float64(), 'INT', [42.9, -42.9, 1e30, -1e30, None],
     [42, -42, 2 ** 31 - 1, -(2 ** 31), None]),
    (pa.float64(), 'BIGINT', [42.9, -42.9, 1e30, -1e30, None],
     [42, -42, 2 ** 63 - 1, -(2 ** 63), None]),
    (pa.decimal128(38, 2), 'BIGINT',
     [Decimal('42.99'), Decimal('-42.99'), Decimal('9223372036854775808.00'), None],
     [42, -42, -(2 ** 63), None]),
    (pa.decimal128(10, 2), 'TINYINT', [Decimal('128.99'), Decimal('-129.99'), None], [-128, 127, None]),
]


@pytest.mark.parametrize('source_type,target_name,values,expected', _NUMERIC_CASES)
def test_single_value_numeric_stats_do_not_prune_converted_match(
        rest_catalog, source_type, target_name, values, expected):
    catalog, _ = rest_catalog
    _table(catalog, 'append', source_type, values[:1])
    catalog.alter_table('default.t', [SchemaChange.update_column_type('v', AtomicType(target_name))], False)
    table = catalog.get_table('default.t')
    predicate = table.new_read_builder().new_predicate_builder().equal('v', expected[0])
    for native in (False, True):
        assert _read(table, native, predicate, ['id']).column('id').to_pylist() == [0]


@pytest.mark.parametrize('zone', [None, 'UTC'])
def test_single_value_timestamp_stats_do_not_prune_truncated_match(rest_catalog, zone):
    catalog, _ = rest_catalog
    _table(catalog, 'append', pa.timestamp('ms', zone), [123])
    target_name = 'TIMESTAMP_LTZ(1)' if zone else 'TIMESTAMP(1)'
    catalog.alter_table('default.t', [SchemaChange.update_column_type('v', AtomicType(target_name))], False)
    table = catalog.get_table('default.t')
    literal = pa.scalar(100, type=pa.timestamp('ms', zone)).as_py()
    predicate = table.new_read_builder().new_predicate_builder().equal('v', literal)
    for native in (False, True):
        assert _read(table, native, predicate, ['id']).column('id').to_pylist() == [0]


@pytest.mark.parametrize('mode', ['append', 'pk', 'de'])
@pytest.mark.parametrize('source_type,target_name,values,expected', _NUMERIC_CASES,
                         ids=['long-byte', 'long-short', 'long-int', 'double-byte', 'double-int',
                              'double-long', 'decimal-long', 'decimal-byte'])
def test_numeric_narrowing_reads_old_and_new_rest_files(
        rest_catalog, mode, source_type, target_name, values, expected):
    catalog, _ = rest_catalog
    _table(catalog, mode, source_type, values)
    catalog.alter_table('default.t', [SchemaChange.update_column_type('v', AtomicType(target_name))], False)
    table = catalog.get_table('default.t')
    for native in (False, True):
        assert _read(table, native).column('v').to_pylist() == expected
    target_type = PyarrowFieldParser.from_paimon_type(AtomicType(target_name))
    _write(table, pa.table({'id': [len(values)], 'v': pa.array([7], type=target_type)}))
    table = catalog.get_table('default.t')
    predicate = table.new_read_builder().new_predicate_builder().equal('v', expected[0])
    for native in (False, True):
        assert _read(table, native).column('v').to_pylist() == expected + [7]
    matches = {native: _read(table, native, predicate, ['id']).column('id').to_pylist()
               for native in (False, True)}
    assert matches == {False: [0], True: [0]}


@pytest.mark.parametrize('mode', ['append', 'pk', 'de'])
@pytest.mark.parametrize('precision', [0, 1, 2, 4, 5, 7, 8])
@pytest.mark.parametrize('zone', [None, 'UTC'])
def test_timestamp_declared_precision_reads_old_rest_files(rest_catalog, mode, precision, zone):
    catalog, _ = rest_catalog
    raw = [123456789, -1, -123456789, 1000000000, None]
    _table(catalog, mode, pa.timestamp('ns', zone), raw)
    type_name = 'TIMESTAMP_LTZ' if zone else 'TIMESTAMP'
    catalog.alter_table('default.t', [SchemaChange.update_column_type(
        'v', AtomicType('%s(%d)' % (type_name, precision)))], False)
    table = catalog.get_table('default.t')
    target_type = PyarrowFieldParser.from_paimon_type(table.field_dict['v'].type)
    ticks_per_second = {'s': 1, 'ms': 1000, 'us': 1000000, 'ns': 1000000000}[target_type.unit]
    quantum = 10 ** (9 - precision)
    expected = [None if value is None else (value // quantum * quantum) // (1000000000 // ticks_per_second)
                for value in raw]
    for native in (False, True):
        result = _read(table, native).column('v')
        assert result.type == target_type
        assert result.cast(pa.int64()).to_pylist() == expected
        if precision <= 6:
            literal = pa.scalar(expected[0], type=pa.int64()).cast(target_type).as_py()
            predicate = table.new_read_builder().new_predicate_builder().equal('v', literal)
            assert _read(table, native, predicate, ['id']).column('id').to_pylist() == [0]


@pytest.mark.parametrize('mode', ['append', 'pk', 'de'])
def test_nested_narrowing_preserves_null_containers_and_leaf_projection(rest_catalog, mode):
    catalog, _ = rest_catalog
    schema = pa.schema([
        ('id', pa.int64()),
        ('r', pa.struct([('n', pa.float64()), ('t', pa.timestamp('ns'))])),
        ('a', pa.list_(pa.int64())),
        ('m', pa.map_(pa.string(), pa.timestamp('ns'))),
    ])
    options = {'file.format': 'parquet', 'read.batch-size': '1'}
    if mode == 'pk':
        options['bucket'] = '1'
    elif mode == 'de':
        options.update({'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true'})
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        schema, primary_keys=['id'] if mode == 'pk' else [], options=options), False)
    _write(catalog.get_table('default.t'), pa.Table.from_pylist([
        {'id': 0, 'r': {'n': 128.9, 't': 123456789}, 'a': [128, None, -129],
         'm': [('one', 123456789), ('two', -1)]},
        {'id': 1, 'r': None, 'a': None, 'm': None},
        {'id': 2, 'r': {'n': -129.9, 't': -1}, 'a': [], 'm': []},
    ], schema=schema))
    catalog.alter_table('default.t', [
        SchemaChange.update_column_type(['r', 'n'], AtomicType('TINYINT')),
        SchemaChange.update_column_type(['r', 't'], AtomicType('TIMESTAMP(1)')),
        SchemaChange.update_column_type(['a', 'element'], AtomicType('TINYINT')),
        SchemaChange.update_column_type(['m', 'value'], AtomicType('TIMESTAMP(1)')),
    ], False)
    table = catalog.get_table('default.t')
    expected_a = [[-128, None, 127], None, []]
    expected_m = pa.array([[('one', 100), ('two', -100)], None, []],
                          type=pa.map_(pa.string(), pa.timestamp('ms'))).to_pylist()
    for native in (False, True):
        actual = _read(table, native)
        assert actual.column('a').to_pylist() == expected_a
        assert actual.column('m').to_pylist() == expected_m
        # ROW field ids belong to the catalog; assert values independently of them.
        assert actual.column('r').flatten()[0].to_pylist() == [-128, None, 127]
        assert actual.column('r').flatten()[1].cast(pa.int64()).to_pylist() == [100, None, -100]
        leaves = _read(table, native, projection=['id', 'r.n', 'r.t'])
        assert leaves.column('r_n').to_pylist() == [-128, None, 127]
        assert leaves.column('r_t').cast(pa.int64()).to_pylist() == [100, None, -100]


_TEMPORAL_CASES = [
    (pa.timestamp('ms'), 'BIGINT', [42001, -1, -42001, 2147483650000, None],
     [42, -1, -43, 2147483650, None]),
    (pa.timestamp('us'), 'BIGINT', [42000001, -1, -42000001, 2147483650000000, None],
     [42, -1, -43, 2147483650, None]),
    (pa.timestamp('ns'), 'BIGINT', [42000000001, -1, -42000000001, 2147483650000000000, None],
     [42, -1, -43, 2147483650, None]),
    (pa.timestamp('ms', 'UTC'), 'BIGINT', [42001, -1, -42001, 2147483650000, None],
     [42, -1, -43, 2147483650, None]),
    (pa.timestamp('us', 'UTC'), 'BIGINT', [42000001, -1, -42000001, 2147483650000000, None],
     [42, -1, -43, 2147483650, None]),
    (pa.timestamp('ns', 'UTC'), 'BIGINT', [42000000001, -1, -42000000001, 2147483650000000000, None],
     [42, -1, -43, 2147483650, None]),
    (pa.int32(), 'TIMESTAMP(0)', [42, -42, 2147483647, -2147483648, None],
     [42, -42, 2147483647, -2147483648, None]),
    (pa.int32(), 'TIMESTAMP(1)', [42, -42, 2147483647, -2147483648, None],
     [42000, -42000, 2147483647000, -2147483648000, None]),
    (pa.int32(), 'TIMESTAMP(4)', [42, -42, 2147483647, -2147483648, None],
     [42000000, -42000000, 2147483647000000, -2147483648000000, None]),
    (pa.int32(), 'TIMESTAMP(7)', [42, -42, 2147483647, -2147483648, None],
     [42000000000, -42000000000, 2147483647000000000, -2147483648000000000, None]),
    (pa.int64(), 'TIMESTAMP(0)', [42, -42, 2147483650, 2 ** 63 - 1, -(2 ** 63), None],
     [42, -42, 2147483650, -1, 0, None]),
    (pa.int64(), 'TIMESTAMP(1)', [42, -42, 2147483650, 2 ** 63 - 1, -(2 ** 63), None],
     [42000, -42000, 2147483650000, -1000, 0, None]),
    (pa.int64(), 'TIMESTAMP(4)', [42, -42, 2147483650, 2 ** 63 - 1, -(2 ** 63), None],
     [42000000, -42000000, 2147483650000000, -1000000, 0, None]),
    (pa.int64(), 'TIMESTAMP(7)', [42, -42, 2147483650, 2 ** 63 - 1, -(2 ** 63), None],
     [42000000000, -42000000000, 2147483650000000000, -1000000000, 0, None]),
]


@pytest.mark.parametrize('mode', ['append', 'pk', 'de'])
@pytest.mark.parametrize('source_type,target_name,values,expected', _TEMPORAL_CASES)
def test_epoch_seconds_evolution_reads_old_and_new_rest_files(
        rest_catalog, mode, source_type, target_name, values, expected):
    catalog, _ = rest_catalog
    _table(catalog, mode, source_type, values)
    target = AtomicType(target_name)
    catalog.alter_table('default.t', [SchemaChange.update_column_type('v', target)], False)
    table = catalog.get_table('default.t')
    target_type = PyarrowFieldParser.from_paimon_type(target)
    for native in (False, True):
        result = _read(table, native).column('v')
        assert result.type == target_type
        assert result.cast(pa.int64()).to_pylist() == expected
    # Both schema versions must materialize the current type in one read.
    _write(table, pa.table({'id': [len(values)], 'v': pa.array([expected[0]], type=target_type)}))
    table = catalog.get_table('default.t')
    first = pa.scalar(expected[0], type=target_type).as_py()
    pb = table.new_read_builder().new_predicate_builder()
    for native in (False, True):
        assert _read(table, native).column('v').cast(pa.int64()).to_pylist() == expected + [expected[0]]
        assert _read(table, native, pb.equal('v', first), ['id']).column('id').to_pylist() == [0, len(values)]
        assert _read(table, native, pb.is_null('v'), ['id']).column('id').to_pylist() == [len(values) - 1]


@pytest.mark.parametrize('source_type,target_name,values,expected', _TEMPORAL_CASES)
def test_single_value_epoch_stats_do_not_prune_converted_match(
        rest_catalog, source_type, target_name, values, expected):
    catalog, _ = rest_catalog
    _table(catalog, 'append', source_type, values[:1])
    target = AtomicType(target_name)
    catalog.alter_table('default.t', [SchemaChange.update_column_type('v', target)], False)
    table = catalog.get_table('default.t')
    literal = pa.scalar(expected[0], type=PyarrowFieldParser.from_paimon_type(target)).as_py()
    predicate = table.new_read_builder().new_predicate_builder().equal('v', literal)
    for native in (False, True):
        assert _read(table, native, predicate, ['id']).column('id').to_pylist() == [0]


@pytest.mark.parametrize('mode', ['append', 'pk', 'de'])
@pytest.mark.parametrize('integer_type', [pa.int32(), pa.int64()])
def test_nested_epoch_seconds_evolution_and_projection(rest_catalog, mode, integer_type):
    catalog, _ = rest_catalog
    schema = pa.schema([
        ('id', pa.int64()),
        ('r', pa.struct([('t', pa.timestamp('us')), ('s', integer_type)])),
        ('a', pa.list_(pa.timestamp('ns'))),
        ('m', pa.map_(pa.string(), integer_type)),
    ])
    options = {'file.format': 'parquet', 'read.batch-size': '1'}
    if mode == 'pk':
        options['bucket'] = '1'
    elif mode == 'de':
        options.update({'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true'})
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        schema, primary_keys=['id'] if mode == 'pk' else [], options=options), False)
    boundary = 2 ** 63 - 1 if integer_type == pa.int64() else -(2 ** 31)
    expected_boundary = -1000000 if integer_type == pa.int64() else -2147483648000000
    _write(catalog.get_table('default.t'), pa.Table.from_pylist([
        {'id': 0, 'r': {'t': 42000001, 's': 42}, 'a': [42000000001, None, -1],
         'm': [('positive', 42), ('negative', -42)]},
        {'id': 1, 'r': None, 'a': None, 'm': None},
        {'id': 2, 'r': {'t': -1, 's': boundary}, 'a': [], 'm': []},
    ], schema=schema))
    catalog.alter_table('default.t', [
        SchemaChange.update_column_type(['r', 't'], AtomicType('BIGINT')),
        SchemaChange.update_column_type(['r', 's'], AtomicType('TIMESTAMP(6)')),
        SchemaChange.update_column_type(['a', 'element'], AtomicType('BIGINT')),
        SchemaChange.update_column_type(['m', 'value'], AtomicType('TIMESTAMP(3)')),
    ], False)
    table = catalog.get_table('default.t')
    expected_map = pa.array([[('positive', 42000), ('negative', -42000)], None, []],
                            type=pa.map_(pa.string(), pa.timestamp('ms'))).to_pylist()
    for native in (False, True):
        actual = _read(table, native)
        assert actual.column('r').flatten()[0].to_pylist() == [42, None, -1]
        assert actual.column('r').flatten()[1].cast(pa.int64()).to_pylist() == [42000000, None, expected_boundary]
        assert actual.column('a').to_pylist() == [[42, None, -1], None, []]
        assert actual.column('m').to_pylist() == expected_map
        leaves = _read(table, native, projection=['id', 'r.t', 'r.s'])
        assert leaves.column('r_t').to_pylist() == [42, None, -1]
        assert leaves.column('r_s').cast(pa.int64()).to_pylist() == [42000000, None, expected_boundary]
        selected = _read(table, native, projection=[
            'id', "m['positive']", "m['negative']", "m['missing']"])
        assert selected.column('m_positive').cast(pa.int64()).to_pylist() == [42000, None, None]
        assert selected.column('m_negative').cast(pa.int64()).to_pylist() == [-42000, None, None]
        assert selected.column('m_missing').null_count == 3


_NESTED_TEMPORAL_STRING_CASES = [
    (pa.list_(pa.timestamp('ns')), None, [[0, -1, None], None, [], [1000000000]],
     ['[1970-01-01 00:00:00.000000000, 1969-12-31 23:59:59.999999999, null]', None,
      '[]', '[1970-01-01 00:00:01.000000000]']),
    (pa.map_(pa.string(), pa.time32('ms')), MapType(True, AtomicType('STRING'), AtomicType('TIME(3)')),
     [[('x', 0), ('y', 101), ('n', None)], None, [], [('p', 100)]],
     ['{x -> 00:00:00.0, y -> 00:00:00.101, n -> null}', None, '{}', '{p -> 00:00:00.1}']),
    (pa.struct([('t', pa.time32('ms')), ('ts', pa.timestamp('ns'))]),
     RowType(True, [DataField(2, 't', AtomicType('TIME(3)')), DataField(3, 'ts', AtomicType('TIMESTAMP(9)'))]),
     [{'t': 0, 'ts': -1}, None, {'t': None, 'ts': None}, {'t': 100, 'ts': 0}],
     ['{00:00:00.0, 1969-12-31 23:59:59.999999999}', None, '{null, null}',
      '{00:00:00.1, 1970-01-01 00:00:00.000000000}']),
]


@pytest.mark.parametrize('mode', ['append', 'pk', 'de'])
@pytest.mark.parametrize('target_name', ['STRING', 'CHAR(8)'])
@pytest.mark.parametrize('arrow_type,paimon_type,values,expected', _NESTED_TEMPORAL_STRING_CASES)
def test_constructed_string_evolution_uses_temporal_child_casts(
        rest_catalog, mode, target_name, arrow_type, paimon_type, values, expected):
    catalog, _ = rest_catalog
    _table(catalog, mode, arrow_type, values, paimon_type)
    catalog.alter_table('default.t', [SchemaChange.update_column_type('v', AtomicType(target_name))], False)
    table = catalog.get_table('default.t')
    # Java's constructed cast does not trim/pad the overall result. Its scalar
    # children use STRING, retaining their own declared temporal precision.
    for native in (False, True):
        assert _read(table, native).column('v').to_pylist() == expected


@pytest.mark.parametrize('mode', ['append', 'de'])
@pytest.mark.parametrize('target_name', ['STRING', 'VARCHAR(12)', 'CHAR(35)'])
def test_wide_ltz_string_does_not_fail_a_mixed_batch(rest_catalog, mode, target_name):
    catalog, _ = rest_catalog
    values = [0, 2 ** 63 - 1, None, -(2 ** 63), 2 ** 63 - 1 - 181 * 86400000]
    _table(catalog, mode, pa.timestamp('ms', 'UTC'), values)
    catalog.alter_table('default.t', [SchemaChange.update_column_type('v', AtomicType(target_name))], False)
    table = catalog.get_table('default.t')
    # The Python executor supports the complete signed millisecond range. Core
    # unit tests check its matching Java strings for UTC, Shanghai and DST zones.
    expected = _read(table, False).column('v').to_pylist()
    assert expected[2] is None
    assert all(value is not None for i, value in enumerate(expected) if i != 2)
    assert _read(table, True).column('v').to_pylist() == expected
