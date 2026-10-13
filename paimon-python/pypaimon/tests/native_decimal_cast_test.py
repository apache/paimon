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

"""Java decimal golden values through Python and Native REST reads."""

from decimal import Decimal

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.read.native_plan import native_runtime_available
from pypaimon.schema.data_types import AtomicType, PyarrowFieldParser
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.tests import native_plan_rest_test
from pypaimon.tests.native_schema_evolution_cast_test import _read, _table, _write

rest_catalog = native_plan_rest_test.rest_catalog


pytestmark = [pytest.mark.native_plan, pytest.mark.skipif(
    not native_runtime_available(), reason='Rust reader required')]


_CASES = [
    (pa.float64(), 'DECIMAL(5,2)', [1.005, -1.005, 0.145, 999.995, 1e30, None],
     ['1.01', '-1.01', '0.15', None, None, None]),
    (pa.float32(), 'DECIMAL(5,2)', [1.005, -1.005, 0.145, 999.995, 1e30, None],
     ['1.00', '-1.00', '0.14', '999.99', None, None]),
    (pa.float64(), 'DECIMAL(38,20)', [1.2345678901234567, 1.005, 1e30, None],
     ['1.23456789012345670000', '1.00500000000000000000', None, None]),
    (pa.float32(), 'DECIMAL(38,20)', [1.2345679, 1.005, 1e30, None],
     ['1.23456788063049320000', '1.00499999523162840000', None, None]),
    (pa.string(), 'DECIMAL(5,2)', ['1e-2', '+.005', '2.675', '999.995', '1e100', None],
     ['0.01', '0.01', '2.68', None, None, None]),
    (pa.decimal128(10, 2), 'DECIMAL(3,2)', [Decimal('9.99'), Decimal('10.00'), None],
     ['9.99', None, None]),
    (pa.decimal128(10, 2), 'DECIMAL(4,3)', [Decimal('9.99'), Decimal('-10.00'), None],
     ['9.990', None, None]),
    (pa.decimal128(6, 3), 'DECIMAL(3,2)', [Decimal('9.994'), Decimal('9.995'), Decimal('-9.995'), None],
     ['9.99', None, None, None]),
]


@pytest.mark.parametrize('mode', ['append', 'pk', 'de'])
@pytest.mark.parametrize('source,target,values,expected', _CASES)
def test_decimal_schema_changes_read_mixed_rest_files(rest_catalog, mode, source, target, values, expected):
    catalog, _ = rest_catalog
    _table(catalog, mode, source, values)
    catalog.alter_table('default.t', [SchemaChange.update_column_type('v', AtomicType(target))], False)
    table = catalog.get_table('default.t')
    target_arrow = PyarrowFieldParser.from_paimon_type(table.field_dict['v'].type)
    expected = [None if value is None else Decimal(value) for value in expected]
    # Read both old and new schema files, with a batch size smaller than the input.
    _write(table, pa.table({'id': [len(values)], 'v': pa.array([expected[0]], type=target_arrow)}))
    table = catalog.get_table('default.t')
    pb = table.new_read_builder().new_predicate_builder()
    for native in (False, True):
        actual = _read(table, native).column('v')
        assert actual.type == target_arrow
        assert actual.to_pylist() == expected + [expected[0]]
        # The converted column remains a residual-filter input when projected out.
        assert _read(table, native, pb.equal('v', expected[0]), ['id']).column('id').to_pylist() == [
            i for i, value in enumerate(expected + [expected[0]]) if value == expected[0]]
        assert _read(table, native, pb.is_null('v'), ['id']).column('id').to_pylist() == [
            i for i, value in enumerate(expected) if value is None]


@pytest.mark.parametrize('source,target,values,expected', _CASES)
def test_decimal_single_value_stats_do_not_prune_converted_matches(rest_catalog, source, target, values, expected):
    catalog, _ = rest_catalog
    _table(catalog, 'append', source, values[:1])
    catalog.alter_table('default.t', [SchemaChange.update_column_type('v', AtomicType(target))], False)
    table = catalog.get_table('default.t')
    predicate = table.new_read_builder().new_predicate_builder().equal('v', Decimal(expected[0]))
    for native in (False, True):
        assert _read(table, native, predicate, ['id']).column('id').to_pylist() == [0]


@pytest.mark.parametrize('mode', ['append', 'pk', 'de'])
def test_nested_decimal_casts_preserve_containers_and_projection(rest_catalog, mode):
    catalog, _ = rest_catalog
    schema = pa.schema([
        ('id', pa.int64()), ('r', pa.struct([('v', pa.float32())])),
        ('a', pa.list_(pa.float64())), ('m', pa.map_(pa.string(), pa.string())),
    ])
    options = {'file.format': 'parquet', 'read.batch-size': '1'}
    if mode == 'pk':
        options['bucket'] = '1'
    elif mode == 'de':
        options.update({'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true'})
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        schema, primary_keys=['id'] if mode == 'pk' else [], options=options), False)
    _write(catalog.get_table('default.t'), pa.Table.from_pylist([
        {'id': 0, 'r': {'v': 1.2345679}, 'a': [1.005, -1.005, 999.995, None],
         'm': [('one', '1e-2'), ('two', '+.005'), ('big', '1e100')]},
        {'id': 1, 'r': None, 'a': None, 'm': None},
        {'id': 2, 'r': {'v': None}, 'a': [], 'm': []},
    ], schema=schema))
    catalog.alter_table('default.t', [
        SchemaChange.update_column_type(['r', 'v'], AtomicType('DECIMAL(38,20)')),
        SchemaChange.update_column_type(['a', 'element'], AtomicType('DECIMAL(5,2)')),
        SchemaChange.update_column_type(['m', 'value'], AtomicType('DECIMAL(5,2)')),
    ], False)
    table = catalog.get_table('default.t')
    for native in (False, True):
        actual = _read(table, native)
        assert actual.column('r').to_pylist() == [{'v': Decimal('1.23456788063049320000')}, None, {'v': None}]
        assert actual.column('a').to_pylist() == [[Decimal('1.01'), Decimal('-1.01'), None, None], None, []]
        assert actual.column('m').to_pylist() == [[('one', Decimal('0.01')), ('two', Decimal('0.01')),
                                                  ('big', None)], None, []]
        assert _read(table, native, projection=['id', 'r.v']).column('r_v').to_pylist() == [
            Decimal('1.23456788063049320000'), None, None]


@pytest.mark.parametrize('mode', ['append', 'pk', 'de'])
def test_invalid_decimal_old_file_fails_in_both_readers(rest_catalog, mode):
    catalog, _ = rest_catalog
    _table(catalog, mode, pa.string(), [None, '1.2.3'])
    catalog.alter_table('default.t', [SchemaChange.update_column_type('v', AtomicType('DECIMAL(5,2)'))], False)
    for native in (False, True):
        with pytest.raises(Exception, match='Invalid decimal schema-evolution value'):
            _read(catalog.get_table('default.t'), native)
