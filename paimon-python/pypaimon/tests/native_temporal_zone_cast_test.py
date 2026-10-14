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

"""Existing temporal changes read natively from old REST Parquet files."""

import os

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

# Golden outputs from Java CastExecutors; UTC is also exercised by CI.
_JAVA_CASTS = {
    'America/New_York': [
        [18000000, 17999900, 1710055800100, 1730611800100, 1710070200100, 1730633400100, None],
        [-18000000, -18000100, 1710019800100, 1730583000100, 1710041400100, 1730597400100, None],
        [-1, -1, 19791, 20029, 19792, 20030, None],
        [68400000, 68399999, 77400123, 77400123, 12600123, 5400123, None],
    ],
    'Asia/Shanghai': [
        [-28800000, -28800100, 1710009000100, 1730568600100, 1710027000100, 1730586600100, None],
        [28800000, 28799900, 1710066600100, 1730626200100, 1710084600100, 1730644200100, None],
        [0, 0, 19792, 20030, 19792, 20030, None],
        [28800000, 28799999, 37800123, 34200123, 55800123, 52200123, None],
    ],
    'UTC': [
        [0, -100, 1710037800100, 1730597400100, 1710055800100, 1730615400100, None],
        [0, -100, 1710037800100, 1730597400100, 1710055800100, 1730615400100, None],
        [0, -1, 19792, 20030, 19792, 20030, None],
        [0, 86399999, 9000123, 5400123, 27000123, 23400123, None],
    ],
}
_JAVA_WIDE = {
    'America/New_York': [
        [-53757322487462200, 53757322519624100],
        [9223372036840375800, 9223372036837013800],
        [-622191233, 622191233],
        [11575807, 42662192],
    ],
    'Asia/Shanghai': [
        [-53757322534367200, 53757322476424100],
        [-9223372036825975900, -9223372036825632900],
        [-622191233, 622191234],
        [54775807, 3167192],
    ],
    'UTC': [
        [-53757322505224200, 53757322505224100],
        [9223372036854775800, 9223372036854775716],
        [-622191233, 622191233],
        [25975807, 60424192],
    ],
}

_VALUES = [0, -1, 1710037800123, 1730597400123, 1710055800123, 1730615400123, None]


def _zone():
    # Run non-UTC cases in separate processes so Native's cached system zone
    # and Python's libc zone are initialized under the same environment.
    name = os.environ.get('TZ', 'UTC')
    if name not in _JAVA_CASTS:
        pytest.skip('Java golden values are defined for UTC, New York and Shanghai')
    return name


def _case(operation):
    date_values = {'UTC': [0, -86400000, 1717200000000, 1325203200000, None],
                   'Asia/Shanghai': [-28800000, -115200000, 1717171200000, 1325174400000, None],
                   'America/New_York': [18000000, -68400000, 1717214400000, 1325221200000, None]}
    zone = _zone()
    if operation < 4:
        source, target = [
            (pa.timestamp('ms'), 'TIMESTAMP_LTZ(1)'),
            (pa.timestamp('ms', 'UTC'), 'TIMESTAMP(1)'),
            (pa.timestamp('ms', 'UTC'), 'DATE'),
            (pa.timestamp('ms', 'UTC'), 'TIME(1)'),
        ][operation]
        return source, _VALUES, target, _JAVA_CASTS[zone][operation]
    if operation == 4:
        return pa.date32(), [0, -1, 19875, 15338, None], 'TIMESTAMP_LTZ(3)', date_values[zone]
    offset = {'UTC': 0, 'Asia/Shanghai': 28800000, 'America/New_York': -18000000}[zone]
    return pa.int64(), [0, -1, 42, None], 'TIMESTAMP_LTZ(3)', [offset, offset - 1000, offset + 42000, None]


def _raw(column):
    kind = column.type
    return column.cast(pa.int32() if pa.types.is_date(kind) or pa.types.is_time(kind) else pa.int64()).to_pylist()


@pytest.mark.parametrize('mode', ['append', 'pk', 'de'])
@pytest.mark.parametrize('operation', range(6))
def test_existing_temporal_changes_read_old_rest_files(rest_catalog, mode, operation):
    source, values, target, expected = _case(operation)
    catalog, _ = rest_catalog
    _table(catalog, mode, source, values)
    catalog.alter_table('default.t', [SchemaChange.update_column_type('v', AtomicType(target))], False)
    table = catalog.get_table('default.t')
    for native in (False, True):
        result = _read(table, native)
        assert _raw(result.column('v')) == expected
        assert result.column('id').to_pylist() == list(range(len(values)))
    target_arrow = PyarrowFieldParser.from_paimon_type(AtomicType(target))
    _write(table, pa.table({'id': [len(values)], 'v': pa.array([expected[0]], type=target_arrow)}))
    table = catalog.get_table('default.t')
    literal = pa.scalar(expected[0], type=target_arrow).as_py()
    predicate = table.new_read_builder().new_predicate_builder().equal('v', literal)
    matched_ids = [index for index, value in enumerate(expected) if value == expected[0]] + [len(values)]
    for native in (False, True):
        assert _raw(_read(table, native).column('v')) == expected + [expected[0]]
        assert _read(table, native, predicate, ['id']).column('id').to_pylist() == matched_ids


@pytest.mark.parametrize('mode', ['append', 'pk', 'de'])
def test_local_zone_casts_reach_row_array_and_selected_map_values(rest_catalog, mode):
    zone = _zone()
    catalog, _ = rest_catalog
    schema = pa.schema([
        ('id', pa.int64()),
        ('r', pa.struct([('t', pa.timestamp('ms')), ('d', pa.date32())])),
        ('a', pa.list_(pa.timestamp('ms', 'UTC'))),
        ('m', pa.map_(pa.string(), pa.timestamp('ms', 'UTC'))),
    ])
    options = {'read.batch-size': '2', 'bucket': '1'} if mode == 'pk' else {'read.batch-size': '2'}
    if mode == 'de':
        options.update({'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true'})
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        schema, primary_keys=['id'] if mode == 'pk' else [], options=options), False)
    _write(catalog.get_table('default.t'), pa.Table.from_pylist([
        dict(id=0, r={'t': 0, 'd': 0}, a=[0, -1, None], m=[('first', 0), ('last', -1)]),
        dict(id=1, r=None, a=None, m=None),
        dict(id=2, r={'t': _VALUES[2], 'd': 19875}, a=[], m=[]),
    ], schema=schema))
    catalog.alter_table('default.t', [
        SchemaChange.update_column_type(['r', 't'], AtomicType('TIMESTAMP_LTZ(1)')),
        SchemaChange.update_column_type(['r', 'd'], AtomicType('TIMESTAMP_LTZ(3)')),
        SchemaChange.update_column_type(['a', 'element'], AtomicType('DATE')),
        SchemaChange.update_column_type(['m', 'value'], AtomicType('TIME(1)')),
    ], False)
    table = catalog.get_table('default.t')
    expected_t = [_JAVA_CASTS[zone][0][0], None, _JAVA_CASTS[zone][0][2]]
    expected_d = [_case(4)[3][0], None, _case(4)[3][2]]
    expected_a = pa.array([[_JAVA_CASTS[zone][2][0], _JAVA_CASTS[zone][2][1], None], None, []],
                          type=pa.list_(pa.date32())).to_pylist()
    for native in (False, True):
        result = _read(table, native)
        assert _raw(result.column('r').flatten()[0]) == expected_t
        assert _raw(result.column('r').flatten()[1]) == expected_d
        assert result.column('a').to_pylist() == expected_a
        leaves = _read(table, native, projection=['id', 'r.t', 'r.d'])
        assert _raw(leaves.column('r_t')) == expected_t
        assert _raw(leaves.column('r_d')) == expected_d
        selected = _read(table, native, projection=['id', "m['first']", "m['last']", "m['missing']"])
        assert _raw(selected.column('m_first')) == [_JAVA_CASTS[zone][3][0], None, None]
        assert _raw(selected.column('m_last')) == [_JAVA_CASTS[zone][3][1], None, None]
        assert selected.column('m_missing').null_count == 3


@pytest.mark.parametrize('mode', ['append', 'de'])
@pytest.mark.parametrize('operation', range(4))
def test_wide_temporal_casts_preserve_mixed_rest_batches(rest_catalog, mode, operation):
    zone = _zone()
    source, _, target, ordinary = _case(operation)
    catalog, _ = rest_catalog
    _table(catalog, mode, source, [0, 2 ** 63 - 1, None, -(2 ** 63)])
    catalog.alter_table('default.t', [SchemaChange.update_column_type('v', AtomicType(target))], False)
    table = catalog.get_table('default.t')
    expected = [ordinary[0], _JAVA_WIDE[zone][operation][0], None, _JAVA_WIDE[zone][operation][1]]
    for native in (False, True):
        result = _read(table, native).column('v')
        assert _raw(result) == expected
        assert result.null_count == 1


@pytest.mark.parametrize('mode', ['append', 'de'])
def test_local_timestamp_can_cross_library_bounds_in_rest_reads(rest_catalog, mode):
    zone = _zone()
    values = [-377705023201000, -377705023200000, 253402207199000, 253402207200000, None]
    expected = {
        'UTC': values,
        'Asia/Shanghai': [-377705052344000, -377705052343000, 253402178399000, 253402178400000, None],
        'America/New_York': [-377705005439000, -377705005438000, 253402225199000, 253402225200000, None],
    }[zone]
    catalog, _ = rest_catalog
    _table(catalog, mode, pa.timestamp('ms'), values)
    catalog.alter_table('default.t', [SchemaChange.update_column_type('v', AtomicType('TIMESTAMP_LTZ(3)'))], False)
    for native in (False, True):
        result = _read(catalog.get_table('default.t'), native).column('v')
        assert _raw(result) == expected
        assert result.null_count == 1
