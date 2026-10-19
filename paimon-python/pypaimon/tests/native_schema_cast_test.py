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

"""Python schema casts transport Arrow arrays into the core Java executors."""

from decimal import Decimal
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon.casting.schema_evolution import cast_array_for_schema_evolution
from pypaimon.schema.data_types import (ArrayType, AtomicType, DataField, DataTypeParser,
                                        MapType, PyarrowFieldParser, RowType)
from pypaimon.tests import native_schema_evolution_cast_test

rest_catalog = native_schema_evolution_cast_test.rest_catalog
pytestmark = native_schema_evolution_cast_test.pytestmark

_CASES = [
    ('STRING', 'FLOAT', [' 1.25 ', '0x1.8p2', '1.25f', None], [1.25, 6.0, 1.25, None]),
    ('STRING', 'DOUBLE', [' 1.25 ', '0x1.8p2', '1.25D', None], [1.25, 6.0, 1.25, None]),
    ('FLOAT', 'STRING', [0.0001, 1e23, 16777216, None], ['1.0E-4', '1.0E23', '1.6777216E7', None]),
    ('DOUBLE', 'STRING', [0.0001, 1e23, None], ['1.0E-4', '9.999999999999999E22', None]),
    ('STRING', 'CHAR(3)', ['🙂中', 'a', 'abcdef', None], ['🙂中 ', 'a  ', 'abc', None]),
    ('STRING', 'VARCHAR(2)', ['🙂中x', 'a', None], ['🙂中', 'a', None]),
    ('STRING', 'BINARY(3)', ['ab', 'abcd', None], [b'ab\x00', b'abc', None]),
    ('STRING', 'VARBINARY(1)', ['🙂', None], [b'\xf0', None]),
    ('INT', 'DECIMAL(4,2)', [1, 100, None], [Decimal('1.00'), None, None]),
    ('STRING', 'DATE', ['42', '2001-02-28', None], [42, 11381, None]),
    ('STRING', 'TIMESTAMP(1)', ['1970-01-01 00:00:00.123456', None], [100, None]),
    ('TIMESTAMP(3)', 'TIMESTAMP(1)', [-(1 << 63), -1, 123, None],
     [9223372036854775716, -100, 100, None]),
    ('INT', 'TIMESTAMP(6)', [1, -1, None], [1000000, -1000000, None]),
    ('INTEGER', 'BIGINT', [1, -1, None], [1, -1, None]),
    ('DECIMAL', 'STRING', [Decimal('12'), None], ['12', None]),
    ('TIME', 'STRING', [0, 1000, None], ['00:00:00', '00:00:01', None]),
    ('TIMESTAMP', 'STRING', [0, 100000, None],
     ['1970-01-01 00:00:00.000000', '1970-01-01 00:00:00.100000', None]),
    ('TIMESTAMP_LTZ', 'TIMESTAMP_LTZ(3)', [0, 100000, None], [0, 100, None]),
]


def _type(name):
    logical = DataTypeParser.parse_data_type(name)
    return logical, PyarrowFieldParser.from_paimon_type(logical)


@pytest.mark.parametrize('source,target,values,expected', _CASES,
                         ids=lambda value: value if isinstance(value, str) else None)
@pytest.mark.parametrize('chunked', [False, True])
def test_python_schema_casts_use_rust(source, target, values, expected, chunked):
    source_type, source_arrow = _type(source)
    target_type, target_arrow = _type(target)
    array = pa.array(values, type=source_arrow)
    if chunked:
        array = pa.chunked_array([array.slice(0, 1), array.slice(1), pa.array([], type=source_arrow)])
    with patch('pypaimon.casting.schema_evolution.cast_decimal', side_effect=AssertionError('Python scalar cast')), \
            patch('pypaimon.casting.schema_evolution.cast_temporal',
                  side_effect=AssertionError('Python temporal cast')):
        result = cast_array_for_schema_evolution(array, target_arrow, source_type, target_type)
    if chunked:
        assert isinstance(result, pa.ChunkedArray)
        result = result.combine_chunks()
    assert result.equals(pa.array(expected, type=target_arrow))


@pytest.mark.parametrize('mode', ['append', 'pk', 'de'])
@pytest.mark.parametrize('source,target,values,expected', _CASES,
                         ids=lambda value: value if isinstance(value, str) else None)
def test_existing_scalar_evolution_uses_native_rest_reader(rest_catalog, mode, source, target, values, expected):
    if mode == 'pk' and source == 'TIMESTAMP(3)' and target == 'TIMESTAMP(1)':
        pytest.skip('The existing Python PK writer cannot materialize the extreme timestamp input')
    catalog, _ = rest_catalog
    source_type, source_arrow = _type(source)
    _, target_arrow = _type(target)
    native_schema_evolution_cast_test._table(catalog, mode, source_arrow, values, source_type)
    from pypaimon.schema.schema_change import SchemaChange
    catalog.alter_table('default.t', [SchemaChange.update_column_type('v', AtomicType(target))], False)
    result = native_schema_evolution_cast_test._read(catalog.get_table('default.t'), True)
    assert result.column('v').combine_chunks().equals(pa.array(expected, type=target_arrow))


@pytest.mark.parametrize('target', ['INT', 'FLOAT', 'DOUBLE', 'DATE', 'TIMESTAMP(3)'])
def test_invalid_scalar_text_is_an_error_instead_of_null(target):
    logical, arrow = _type(target)
    with pytest.raises((ValueError, RuntimeError)):
        cast_array_for_schema_evolution(pa.array(['bad']), arrow, AtomicType('STRING'), logical)


def test_empty_chunked_input_retains_the_requested_type():
    result = cast_array_for_schema_evolution(
        pa.chunked_array([], type=pa.string()), pa.int32(), AtomicType('STRING'), AtomicType('INT'))
    assert result.type == pa.int32() and len(result) == 0


def test_time_precision_changes_are_not_executable():
    from pypaimon.casting.data_type_casts import can_execute_cast
    assert not can_execute_cast(AtomicType('TIME(3)'), AtomicType('TIME(1)'))
    with pytest.raises(NotImplementedError):
        cast_array_for_schema_evolution(
            pa.array([], type=pa.time32('ms')), pa.time32('ms'), AtomicType('TIME(3)'), AtomicType('TIME(1)'))


@pytest.mark.parametrize('source,target', [('TIME', 'TIME(0)'), ('TIME(3)', 'TIME( 3 )'), ('TIME(3)', 'TIME(03)')])
def test_time_equivalent_precision_spellings_are_identity(source, target):
    from pypaimon.casting.data_type_casts import can_execute_cast
    assert can_execute_cast(AtomicType(source), AtomicType(target))
    data = pa.array([100, None], type=pa.time32('ms'))
    assert cast_array_for_schema_evolution(data, data.type, AtomicType(source), AtomicType(target)).equals(data)


def _decimal_container(kind, precision):
    leaf = AtomicType('DECIMAL({})'.format(precision), False)
    if kind == 'array':
        return ArrayType(True, leaf), ['v', 'element']
    if kind == 'row':
        return RowType(True, [DataField(2, 'd', leaf)]), ['v', 'd']
    if kind == 'map':
        return MapType(True, AtomicType('STRING', False), leaf), ['v', 'value']
    return leaf, 'v'


_DECIMAL_CONTAINERS = [
    ('leaf', [Decimal('9.995'), Decimal('1.000')], [None, Decimal('1.00')]),
    ('array', [[Decimal('9.995'), Decimal('1.000')], None, []], [[None, Decimal('1.00')], None, []]),
    ('row', [{'d': Decimal('9.995')}, None, {'d': Decimal('1.000')}],
     [{'d': None}, None, {'d': Decimal('1.00')}]),
    ('map', [[('a', Decimal('9.995')), ('b', Decimal('1.000'))], None, []],
     [[('a', None), ('b', Decimal('1.00'))], None, []]),
]


@pytest.mark.parametrize('kind,values,expected', _DECIMAL_CONTAINERS)
@pytest.mark.parametrize('chunked', [False, True])
def test_not_null_casts_can_return_java_runtime_nulls(kind, values, expected, chunked):
    source, _ = _decimal_container(kind, '6,3')
    target, _ = _decimal_container(kind, '3,2')
    data = pa.array(values, type=PyarrowFieldParser.from_paimon_type(source))
    if chunked:
        data = pa.chunked_array([data.slice(0, 1), data.slice(1)])
    result = cast_array_for_schema_evolution(data, PyarrowFieldParser.from_paimon_type(target), source, target)
    assert result.to_pylist() == expected
    result.validate(full=True)


@pytest.mark.parametrize('mode', ['append', 'pk', 'de'])
@pytest.mark.parametrize('kind,values,expected', _DECIMAL_CONTAINERS)
def test_not_null_decimal_evolution_uses_native_rest_reader(rest_catalog, mode, kind, values, expected):
    catalog, _ = rest_catalog
    source, path = _decimal_container(kind, '6,3')
    native_schema_evolution_cast_test._table(
        catalog, mode, PyarrowFieldParser.from_paimon_type(source), values, source)
    from pypaimon.schema.schema_change import SchemaChange
    catalog.alter_table('default.t', [SchemaChange.update_column_type(path, AtomicType('DECIMAL(3,2)'), True)], False)
    table = catalog.get_table('default.t')
    for native in [False, True]:
        result = native_schema_evolution_cast_test._read(table, native)
        assert result.column('v').to_pylist() == expected
    # Read metadata permits cast-produced NULLs; persisted constraints are intact.
    logical = catalog.get_table('default.t').fields[1].type
    if kind == 'array':
        logical = logical.element
    elif kind == 'row':
        logical = logical.fields[0].type
    elif kind == 'map':
        logical = logical.value
    assert not logical.nullable


@pytest.mark.parametrize('target,expected', [
    ('DECIMAL(10,2)', [Decimal('1.24'), None]), ('INT', [1, None]), ('STRING', ['1.235', None]),
])
def test_decimal256_physical_storage_uses_the_logical_decimal_type(target, expected):
    logical, arrow = _type(target)
    data = pa.array([Decimal('1.235'), None], type=pa.decimal256(20, 3))
    result = cast_array_for_schema_evolution(data, arrow, AtomicType('DECIMAL(20,3)'), logical)
    assert result.equals(pa.array(expected, type=arrow))


@pytest.mark.parametrize('chunked', [False, True])
def test_large_list_rows_align_by_field_id_in_the_shared_core(chunked):
    source_row = RowType(True, [DataField(1, 'old', AtomicType('STRING')),
                                DataField(2, 'dropped', AtomicType('INT'))])
    target_row = RowType(True, [DataField(2, 'dropped', AtomicType('BIGINT')),
                                DataField(1, 'renamed', AtomicType('INT')),
                                DataField(3, 'added', AtomicType('INT'))])
    source, target = ArrayType(True, source_row), ArrayType(True, target_row)
    physical = pa.large_list(pa.struct([('old', pa.large_string()), ('dropped', pa.int32())]))
    data = pa.array([[{'old': 'bad', 'dropped': 0}],
                     [{'old': '1.9', 'dropped': 7}, None], None, [],
                     [{'old': 'bad', 'dropped': 0}]], type=physical).slice(1, 3)
    if chunked:
        data = pa.chunked_array([data.slice(0, 1), data.slice(1)])
    result = cast_array_for_schema_evolution(data, PyarrowFieldParser.from_paimon_type(target), source, target)
    assert result.to_pylist() == [[{'dropped': 7, 'renamed': 1, 'added': None}, None], None, []]


@pytest.mark.parametrize('native_write', [False, True])
@pytest.mark.parametrize('kind,values,invalid', [
    ('array', [[Decimal('1.000')], None, []], [[None]]),
    ('row', [{'d': Decimal('1.000')}, None], [{'d': None}]),
    ('map', [[('a', Decimal('1.000'))], None, []], [[('a', None)]]),
])
def test_read_results_write_back_with_not_null_constraints(rest_catalog, native_write, kind, values, invalid):
    catalog, _ = rest_catalog
    logical, _ = _decimal_container(kind, '6,3')
    native_schema_evolution_cast_test._table(
        catalog, 'append', PyarrowFieldParser.from_paimon_type(logical), values, logical)
    table = catalog.get_table('default.t')
    data = native_schema_evolution_cast_test._read(table, True)
    table = table.copy({'write.native.enabled': str(native_write).lower()})
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    from pypaimon.write.native_write import NativeTableWrite
    assert isinstance(writer, NativeTableWrite) == native_write
    try:
        writer.write_arrow(data)
        commit.commit(writer.prepare_commit())
        bad = pa.Table.from_arrays(
            [pa.array([100], type=pa.int64()), pa.array(invalid, type=data.schema.field('v').type)],
            schema=data.schema)
        with pytest.raises(ValueError):
            writer.write_arrow(bad)
    finally:
        writer.close()
        commit.close()
    output = native_schema_evolution_cast_test._read(catalog.get_table('default.t'), True)
    assert output.num_rows == 2 * len(values)
