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

"""External DECIMAL inputs retain integer values across native update entry points."""

from decimal import Decimal

import pyarrow as pa
import pytest

from pypaimon.tests.native_update_container_casts_test import _OPERATIONS, _apply, _check, _table
from pypaimon.tests.native_update_encoded_inputs_test import _wrap


pytestmark = pytest.mark.native_plan


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
@pytest.mark.parametrize('operation', _OPERATIONS)
@pytest.mark.parametrize('shape', ['root', 'row', 'array', 'map'])
@pytest.mark.parametrize('target', [pa.int32(), pa.int64()])
@pytest.mark.parametrize('factory,precision,scale', [
    (pa.decimal128, 38, -128), (pa.decimal256, 76, -90),
])
def test_zero_decimal_updates_do_not_overflow_scale_powers(
        tmp_path, native, stream, operation, shape, target, factory, precision, scale):
    # A zero can use any valid external scale without evaluating 10**abs(scale).
    zero = Decimal(0)
    source = pa.array([zero, None, zero, zero], type=factory(precision, scale))
    source, target, expected = _wrap(source, target, [0, None, 0, 0], shape)
    table = _table(tmp_path, target, native)
    _apply(table, source, native, stream, operation)
    _check(table, expected)


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
@pytest.mark.parametrize('operation', _OPERATIONS)
@pytest.mark.parametrize('shape', ['root', 'row', 'array', 'map'])
@pytest.mark.parametrize('encoded', [False, True], ids=['plain', 'dictionary'])
def test_wide_decimal_integer_values_are_reduced_before_narrowing(
        tmp_path, native, stream, operation, shape, encoded):
    source = pa.array([Decimal('123.' + '0' * 60), None,
                       Decimal('-456.' + '0' * 60), Decimal('0.' + '0' * 60)],
                      type=pa.decimal256(76, 60))
    if encoded:
        source = source.dictionary_encode()
    source, target, expected = _wrap(source, pa.int32(), [123, None, -456, 0], shape)
    table = _table(tmp_path, target, native)
    _apply(table, source, native, stream, operation)
    _check(table, expected)


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
@pytest.mark.parametrize('operation', ['row_id', 'incremental', 'upsert'])
@pytest.mark.parametrize('shape', ['root', 'row', 'array', 'map'])
@pytest.mark.parametrize('factory,precision', [(pa.decimal128, 38), (pa.decimal256, 76)])
def test_row_updates_truncate_decimal_fractions_toward_zero(
        tmp_path, native, stream, operation, shape, factory, precision):
    source = pa.array([Decimal('-128.99'), None, Decimal('127.99'), Decimal('-0.99')],
                      type=factory(precision, 2))
    source, target, expected = _wrap(source, pa.int8(), [-128, None, 127, 0], shape)
    table = _table(tmp_path, target, native)
    _apply(table, source, native, stream, operation)
    _check(table, expected)


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
@pytest.mark.parametrize('operation', ['predicate_array', 'predicate_callable', 'merge'])
@pytest.mark.parametrize('shape', ['root', 'row', 'array', 'map'])
def test_evaluated_assignments_reject_decimal_fraction_loss(tmp_path, native, stream, operation, shape):
    source = pa.array([Decimal('1.00'), None, Decimal('-2.01'), Decimal('3.00')],
                      type=pa.decimal256(76, 2))
    source, target, _ = _wrap(source, pa.int32(), [1, None, -2, 3], shape)
    table = _table(tmp_path, target, native)
    snapshot = table.snapshot_manager().get_latest_snapshot().id
    with pytest.raises((ValueError, TypeError)):
        _apply(table, source, native, stream, operation)
    assert table.snapshot_manager().get_latest_snapshot().id == snapshot
    _check(table, [None] * 4)


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
@pytest.mark.parametrize('operation', _OPERATIONS)
@pytest.mark.parametrize('target,minimum,maximum', [
    (pa.int8(), -(2 ** 7), 2 ** 7 - 1),
    (pa.int16(), -(2 ** 15), 2 ** 15 - 1),
    (pa.int32(), -(2 ** 31), 2 ** 31 - 1),
    (pa.int64(), -(2 ** 63), 2 ** 63 - 1),
])
def test_decimal_integer_boundaries_are_written_without_wrapping(
        tmp_path, native, stream, operation, target, minimum, maximum):
    source = pa.array([Decimal(minimum), None, Decimal(maximum), Decimal(0)],
                      type=pa.decimal256(76, 2))
    table = _table(tmp_path, target, native)
    _apply(table, source, native, stream, operation)
    _check(table, [minimum, None, maximum, 0])


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('operation', ['row_id', 'predicate_array', 'upsert', 'merge'])
@pytest.mark.parametrize('value', ['-129', '128', '1E+30'])
def test_decimal_integer_overflow_keeps_snapshot_unchanged(tmp_path, native, operation, value):
    scale = -30 if value.endswith('30') else 0
    source = pa.array([Decimal(0), None, Decimal(value), Decimal(0)], type=pa.decimal128(38, scale))
    table = _table(tmp_path, pa.int8(), native)
    snapshot = table.snapshot_manager().get_latest_snapshot().id
    with pytest.raises((ValueError, TypeError)):
        _apply(table, source, native, False, operation)
    assert table.snapshot_manager().get_latest_snapshot().id == snapshot
    _check(table, [None] * 4)
