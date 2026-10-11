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
from decimal import Decimal
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.read.native_plan import native_runtime_available
from pypaimon.schema.data_types import AtomicType, PyarrowFieldParser
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


def _table(catalog, mode, arrow_type, values):
    options = {'file.format': 'parquet', 'read.batch-size': '2'}
    if mode == 'pk':
        options['bucket'] = '1'
    elif mode == 'de':
        options.update({'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true'})
    schema = pa.schema([('id', pa.int64()), ('v', arrow_type)])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        schema, primary_keys=['id'] if mode == 'pk' else [], options=options), False)
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
