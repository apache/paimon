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

from decimal import Decimal

import pyarrow as pa
import pytest

from pypaimon.casting.schema_evolution import cast_array_for_schema_evolution
from pypaimon.schema.data_types import AtomicType


@pytest.mark.parametrize('source_type,values,target_type,expected', [
    (pa.float32(), [128.9, -129.9, 1e30, -1e30, None], pa.int8(), [-128, 127, -1, 0, None]),
    (pa.float64(), [32768.9, -32769.9, 1e30, -1e30, None], pa.int16(), [-32768, 32767, -1, 0, None]),
    (pa.float64(), [42.9, -42.9, 1e30, -1e30, None], pa.int32(),
     [42, -42, 2 ** 31 - 1, -(2 ** 31), None]),
    (pa.float64(), [42.9, -42.9, 1e30, -1e30, None], pa.int64(),
     [42, -42, 2 ** 63 - 1, -(2 ** 63), None]),
    (pa.decimal128(38, 2),
     [Decimal('999999999999999999999999999999999999.99'), Decimal('-42.99'), None],
     pa.int64(), [-5527149226598858753, -42, None]),
])
@pytest.mark.parametrize('chunked', [False, True])
def test_java_numeric_narrowing_of_sliced_and_chunked_arrays(source_type, values, target_type, expected, chunked):
    source = pa.array([None] + values + [None], type=source_type).slice(1, len(values))
    if chunked:
        source = pa.chunked_array([source.slice(0, 2), source.slice(2)])
    output = cast_array_for_schema_evolution(source, target_type)
    assert output.type == target_type
    assert output.to_pylist() == expected


@pytest.mark.parametrize('zone', [None, 'UTC'])
@pytest.mark.parametrize('precision', [7, 8])
def test_timestamp_floor_overflow_returns_null(zone, precision):
    source = pa.array([-(2 ** 63), -(2 ** 63) + 1000, -1, None], type=pa.timestamp('ns', zone))
    name = 'TIMESTAMP_LTZ' if zone else 'TIMESTAMP'
    output = cast_array_for_schema_evolution(
        source, source.type, AtomicType(name + '(9)'), AtomicType('%s(%d)' % (name, precision)))
    quantum = 10 ** (9 - precision)
    assert output.cast(pa.int64()).to_pylist() == [None, (-(2 ** 63) + 1000) // quantum * quantum, -quantum, None]


@pytest.mark.parametrize('unit,ticks', [('s', 1), ('ms', 1000), ('us', 1000000), ('ns', 1000000000)])
@pytest.mark.parametrize('zone', [None, 'UTC'])
@pytest.mark.parametrize('chunked', [False, True])
def test_timestamp_to_bigint_uses_epoch_seconds_flooring(unit, ticks, zone, chunked):
    fraction = 0 if unit == 's' else 1
    values = [ticks + fraction, -1, -ticks - fraction, 0,
              (2 ** 31 + 2) * ticks, -(2 ** 31 + 2) * ticks, None]
    expected = [1, -1, -1 if unit == 's' else -2, 0, 2 ** 31 + 2, -(2 ** 31 + 2), None]
    source = pa.array([None] + values + [None], type=pa.timestamp(unit, zone)).slice(1, len(values))
    if chunked:
        source = pa.chunked_array([source.slice(0, 2), source.slice(2, 0), source.slice(2)])
    result = cast_array_for_schema_evolution(source, pa.int64())
    assert result.type == pa.int64()
    assert result.to_pylist() == expected


@pytest.mark.parametrize('unit,ticks', [('s', 1), ('ms', 1000), ('us', 1000000), ('ns', 1000000000)])
@pytest.mark.parametrize('chunked', [False, True])
@pytest.mark.parametrize('source_type', [pa.int32(), pa.int64()])
def test_integer_to_timestamp_uses_epoch_seconds_and_java_millis_wrapping(unit, ticks, chunked, source_type):
    values = [42, -42, 1700000000, None]
    expected = [42 * ticks, -42 * ticks, 1700000000 * ticks, None]
    if source_type == pa.int64():
        values.extend([2 ** 63 - 1, -(2 ** 63), 2 ** 63 - 2])
        expected.extend([-ticks, 0, -2 * ticks])
    else:
        values.extend([2 ** 31 - 1, -(2 ** 31), 0])
        expected.extend([(2 ** 31 - 1) * ticks, -(2 ** 31) * ticks, 0])
    source = pa.array([None] + values + [None], type=source_type).slice(1, len(values))
    if chunked:
        source = pa.chunked_array([source.slice(0, 1), source.slice(1)])
    result = cast_array_for_schema_evolution(source, pa.timestamp(unit))
    assert result.type == pa.timestamp(unit)
    assert result.cast(pa.int64()).to_pylist() == expected


@pytest.mark.parametrize('zone,expected', [
    (None, [9223372036854775, -9223372036854776, None]),
    ('UTC', [-53757322505225, 53757322505224, None]),
])
def test_timestamp_to_bigint_matches_java_extreme_millis_round_trip(zone, expected):
    # Golden outputs from Java CastExecutors, including LTZ's int epoch day.
    source = pa.array([2 ** 63 - 1, -(2 ** 63), None], type=pa.timestamp('ms', zone))
    assert cast_array_for_schema_evolution(source, pa.int64()).to_pylist() == expected


@pytest.mark.parametrize('unit,expected', [
    ('s', [9223372036854775, -9223372036854775, None]),
    ('ms', [9223372036854775000, -9223372036854775000, None]),
    ('us', [None, None, None]),
    ('ns', [None, None, None]),
])
def test_bigint_to_timestamp_keeps_lenient_arrow_storage_overflow(unit, expected):
    source = pa.array([9223372036854775, -9223372036854775, None], type=pa.int64())
    result = cast_array_for_schema_evolution(source, pa.timestamp(unit))
    assert result.cast(pa.int64()).to_pylist() == expected


@pytest.mark.parametrize('precision,whole,nonzero', [
    (0, '1970-01-01 00:00:00', '1970-01-01 00:00:00.123456789'),
    (1, '1970-01-01 00:00:00.0', '1970-01-01 00:00:00.123456789'),
    (3, '1970-01-01 00:00:00.000', '1970-01-01 00:00:00.123456789'),
    (6, '1970-01-01 00:00:00.000000', '1970-01-01 00:00:00.123456789'),
    (9, '1970-01-01 00:00:00.000000000', '1970-01-01 00:00:00.123456789'),
])
@pytest.mark.parametrize('chunked', [False, True])
def test_java_timestamp_string_preserves_nanos_and_declared_precision(precision, whole, nonzero, chunked):
    source = pa.array([None, 0, 123456789, -1, None, 0], type=pa.timestamp('ns')).slice(1, 4)
    if chunked:
        source = pa.chunked_array([source.slice(0, 2), source.slice(2)])
    result = cast_array_for_schema_evolution(
        source, pa.string(), AtomicType('TIMESTAMP(%d)' % precision), AtomicType('STRING'))
    assert result.to_pylist() == [whole, nonzero, '1969-12-31 23:59:59.999999999', None]


@pytest.mark.parametrize('precision,expected', [
    (0, ['00:00:00', '00:00:00', '00:00:00', '23:59:59']),
    (1, ['00:00:00.0', '00:00:00.1', '00:00:00.1', '23:59:59.9']),
    (2, ['00:00:00.0', '00:00:00.1', '00:00:00.10', '23:59:59.99']),
    (3, ['00:00:00.0', '00:00:00.1', '00:00:00.101', '23:59:59.999']),
    (9, ['00:00:00.0', '00:00:00.1', '00:00:00.101', '23:59:59.999']),
])
def test_java_time_string_fraction_and_negative_millis(precision, expected):
    source = pa.array([None, 0, 100, 101, -1, None, 0], type=pa.time32('ms')).slice(1, 5)
    result = cast_array_for_schema_evolution(
        source, pa.string(), AtomicType('TIME(%d)' % precision), AtomicType('STRING'))
    assert result.to_pylist() == expected + [None]


@pytest.mark.parametrize('raw,expected', [
    (253402300800000, '0000-01-01 00:00:00.000'),  # year 10000: Java int4
    (-62167219200000, '0000-01-01 00:00:00.000'),  # year 0
    (-62198755200000, '000/-01-01 00:00:00.000'),  # year -1: signed remainder
    (2 ** 63 - 1, '/+-.-04-06 07:12:55.807'),  # signed epoch-day narrowing
    (-(2 ** 63), '5471-09-26 16:47:04.192'),
])
def test_timestamp_string_keeps_arrow_dates_outside_python_datetime_range(raw, expected):
    source = pa.array([raw, None], type=pa.timestamp('ms'))
    assert cast_array_for_schema_evolution(
        source, pa.string(), AtomicType('TIMESTAMP(3)'), AtomicType('STRING')).to_pylist() == [expected, None]


def test_timestamp_seconds_wrapping_keeps_wrapped_millisecond_fraction():
    source = pa.array([9223372036854776, None], type=pa.timestamp('s'))
    assert cast_array_for_schema_evolution(
        source, pa.string(), AtomicType('TIMESTAMP(3)'), AtomicType('STRING')).to_pylist() == [
        '5471-09-26 16:47:04.384', None]
