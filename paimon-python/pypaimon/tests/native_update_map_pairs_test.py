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

"""External MAP pairs can construct ARRAY elements in existing row updates."""

from decimal import Decimal

import pyarrow as pa
import pytest

from pypaimon.tests.native_update_container_casts_test import _apply, _check, _table
from pypaimon.tests.native_update_encoded_inputs_test import _wrap


pytestmark = pytest.mark.native_plan


def _pairs(kind):
    keys = [99, 1, 3, 99, 99, 5, 99]
    values = [99, 2, None, 99, 99, -6, 99]
    target = pa.int32()
    key_type, value_type = pa.int64(), pa.int64()
    if kind == 'float':
        values = [1e100, 2.75, None, 1e100, 1e100, -6.75, 1e100]
        value_type = pa.float64()
    elif kind == 'decimal':
        values = [Decimal(str(v)) if v is not None else None for v in values]
        values[1], values[5] = Decimal('2.75'), Decimal('-6.75')
        value_type = pa.decimal128(10, 2)
    elif kind in ('string', 'binary'):
        keys = ['padding', '一', '', 'hidden', 'hidden', 'z', 'padding']
        values = ['padding', '二', None, 'hidden', 'hidden', 'a\0b', 'padding']
        key_type, value_type, target = pa.string(), pa.large_string(), pa.string()
        if kind == 'binary':
            keys = [v.encode() for v in keys]
            values = [None if v is None else v.encode() for v in values]
            key_type, value_type, target = pa.binary(), pa.large_binary(), pa.binary()
    elif kind == 'array':
        keys = [[v] for v in keys]
        values = [None if v is None else [v, None] for v in values]
        key_type, value_type, target = pa.list_(pa.int32()), pa.large_list(pa.int64()), pa.list_(pa.int32())
    source = pa.MapArray.from_arrays(
        pa.array([0, 1, 3, 5, 5, 6, 7], type=pa.int32()),
        pa.array(keys, type=key_type), pa.array(values, type=value_type),
        mask=pa.array([False, False, True, False, False, False]),
    ).slice(1, 4)
    expected = [[[keys[1], values[1]], [keys[2], values[2]]], None, [], [[keys[5], values[5]]]]
    if kind in ('float', 'decimal'):
        expected[0][0][1], expected[3][0][1] = 2, -6
    return source, pa.list_(pa.list_(target)), expected


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
@pytest.mark.parametrize('operation', ['row_id', 'incremental', 'upsert'])
@pytest.mark.parametrize('shape', ['root', 'row', 'array', 'map'])
@pytest.mark.parametrize('kind', ['integer', 'float', 'decimal', 'string', 'binary', 'array'])
@pytest.mark.parametrize('encoded', [False, True], ids=['map', 'dictionary_map'])
def test_map_pairs_construct_nested_arrays(tmp_path, native, stream, operation, shape, kind, encoded):
    source, target, expected = _pairs(kind)
    if encoded:
        source = pa.DictionaryArray.from_arrays(pa.array([0, 1, 2, 3], type=pa.int8()), source)
    source, target, expected = _wrap(source, target, expected, shape)
    table = _table(tmp_path, target, native)
    _apply(table, source, native, stream, operation)
    _check(table, expected)


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('operation', ['predicate_array', 'predicate_callable', 'merge'])
@pytest.mark.parametrize('shape', ['root', 'row', 'array', 'map'])
def test_map_pairs_keep_safe_assignment_boundary(tmp_path, native, operation, shape):
    source, target, expected = _wrap(*_pairs('integer'), shape)
    table = _table(tmp_path, target, native)
    snapshot = table.snapshot_manager().get_latest_snapshot().id
    with pytest.raises((ValueError, TypeError, NotImplementedError)):
        _apply(table, source, native, False, operation)
    assert table.snapshot_manager().get_latest_snapshot().id == snapshot
    _check(table, [None] * len(expected))


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('operation', ['row_id', 'incremental', 'upsert'])
@pytest.mark.parametrize('side', ['key', 'value'])
def test_map_pair_integer_overflow_cannot_change_rows(tmp_path, native, operation, side):
    keys, values = [1, 2, 3, 4], [5, 6, 7, 8]
    (keys if side == 'key' else values)[2] = 2 ** 40
    source = pa.MapArray.from_arrays(pa.array(range(5), type=pa.int32()), pa.array(keys), pa.array(values))
    table = _table(tmp_path, pa.list_(pa.list_(pa.int32())), native)
    snapshot = table.snapshot_manager().get_latest_snapshot().id
    with pytest.raises((ValueError, TypeError, OverflowError)):
        _apply(table, source, native, False, operation)
    assert table.snapshot_manager().get_latest_snapshot().id == snapshot
    _check(table, [None] * 4)
