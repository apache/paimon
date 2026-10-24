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

"""NULL binary storage is not an input to text or numeric parsing."""

import struct
from decimal import Decimal

import pyarrow as pa
import pytest

from pypaimon.tests.native_update_container_casts_test import _OPERATIONS, _apply, _check, _table
from pypaimon.tests.native_update_encoded_inputs_test import _wrap


pytestmark = pytest.mark.native_plan


def _binary_input(kind, large):
    targets = {'string': pa.string(), 'integer': pa.int32(), 'float': pa.float64(),
               'decimal': pa.decimal128(10, 2), 'boolean': pa.bool_()}
    left, right = (b'true', b'false') if kind == 'boolean' else (b'42', b'-7')
    parts = [b'unused', b'\xff\xff', left, b'\xc3', right, b'unused']
    offsets = [0]
    for part in parts:
        offsets.append(offsets[-1] + len(part))
    source = pa.Array.from_buffers(
        pa.large_binary() if large else pa.binary(), len(parts),
        [pa.py_buffer(b'\x35'),
         pa.py_buffer(struct.pack('<' + str(len(offsets)) + ('q' if large else 'i'), *offsets)),
         pa.py_buffer(b''.join(parts))],
    ).slice(1, 4)
    expected = [None, '42', None, '-7']
    if kind == 'integer':
        expected = [None, 42, None, -7]
    elif kind == 'float':
        expected = [None, 42.0, None, -7.0]
    elif kind == 'decimal':
        expected = [None, Decimal('42.00'), None, Decimal('-7.00')]
    elif kind == 'boolean':
        expected = [None, True, None, False]
    return source, targets[kind], expected


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
@pytest.mark.parametrize('operation', _OPERATIONS)
@pytest.mark.parametrize('shape', ['root', 'row', 'array', 'map'])
@pytest.mark.parametrize('kind', ['string', 'integer', 'float', 'decimal', 'boolean'])
@pytest.mark.parametrize('large', [False, True], ids=['binary', 'large_binary'])
def test_null_binary_bytes_do_not_participate_in_casts(tmp_path, native, stream, operation, shape, kind, large):
    source, target, expected = _wrap(*_binary_input(kind, large), shape)
    table = _table(tmp_path, target, native)
    _apply(table, source, native, stream, operation)
    _check(table, expected)


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('operation', _OPERATIONS)
@pytest.mark.parametrize('kind', ['string', 'integer', 'float', 'decimal', 'boolean'])
def test_dictionary_binary_text_uses_logical_validity(tmp_path, native, operation, kind):
    source, target, expected = _binary_input(kind, True)
    indices = [3, None, 0, 1]
    source = pa.DictionaryArray.from_arrays(pa.array(indices, type=pa.int8()), source)
    table = _table(tmp_path, target, native)
    _apply(table, source, native, False, operation)
    _check(table, [None if i is None else expected[i] for i in indices])


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('operation', _OPERATIONS)
def test_fixed_binary_text_skips_null_storage(tmp_path, native, operation):
    source = pa.Array.from_buffers(pa.binary(2), 6, [
        pa.py_buffer(b'\x35'), pa.py_buffer(b'00\xff\xff42\xc3\xff-700')]).slice(1, 4)
    table = _table(tmp_path, pa.string(), native)
    _apply(table, source, native, False, operation)
    _check(table, [None, '42', None, '-7'])


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('operation', _OPERATIONS)
@pytest.mark.parametrize('kind', ['string', 'integer', 'float', 'decimal', 'boolean'])
def test_visible_invalid_binary_text_cannot_change_rows(tmp_path, native, operation, kind):
    source, target, _ = _binary_input(kind, False)
    source = pa.array([source[1].as_py(), None, b'\xff', source[3].as_py()])
    table = _table(tmp_path, target, native)
    snapshot = table.snapshot_manager().get_latest_snapshot().id
    with pytest.raises((ValueError, TypeError)):
        _apply(table, source, native, False, operation)
    assert table.snapshot_manager().get_latest_snapshot().id == snapshot
    _check(table, [None] * 4)
