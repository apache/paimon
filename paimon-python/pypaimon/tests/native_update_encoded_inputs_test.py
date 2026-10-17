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

"""Arrow encodings used by existing update APIs must retain their values."""

from decimal import Decimal

import numpy as np
import pyarrow as pa
import pytest

from pypaimon.tests.native_update_container_casts_test import (
    _OPERATIONS, _apply, _check, _native_only, _table,
)


pytestmark = pytest.mark.native_plan


def _wrap(source, target, expected, shape):
    if shape == 'row':
        source = pa.StructArray.from_arrays([source], names=['nested'])
        target = pa.struct([('nested', target)])
        expected = [{'nested': value} for value in expected]
    elif shape == 'array':
        source = pa.ListArray.from_arrays(pa.array(range(len(source) + 1), type=pa.int32()), source)
        target = pa.list_(target)
        expected = [[value] for value in expected]
    elif shape == 'map':
        keys = [str(i) for i in range(len(source))]
        source = pa.MapArray.from_arrays(pa.array(range(len(source) + 1), type=pa.int32()),
                                         pa.array(keys), source)
        target = pa.map_(pa.string(), target)
        expected = [[(key, value)] for key, value in zip(keys, expected)]
    return source, target, expected


def _scalar_case(kind):
    if kind == 'fixed_binary':
        source = pa.array([b'old', b'a\0b', None, '中'.encode(), b'xyz', b'end'], type=pa.binary(3)).slice(1, 4)
        return source, pa.string(), ['a\0b', None, '中', 'xyz']
    if kind == 'empty_fixed_binary':
        return pa.array([b'', None, b'', b''], type=pa.binary(0)), pa.string(), ['', None, '', '']
    if kind == 'half_float':
        values = np.array([0.1, 0.0, -3.25, 2 ** -24], dtype=np.float16)
        source = pa.array(values, mask=np.array([False, True, False, False]))
        return source, pa.string(), ['0.0999755859375', None, '-3.25', '5.960464477539063e-8']
    coefficient = '1508079088755887087797858531882816082'
    value = Decimal(coefficient[:-1] + '.' + coefficient[-1])
    factory = pa.decimal256 if kind.startswith('decimal256') else pa.decimal128
    precision = 76 if kind.startswith('decimal256_wide') else 38
    source = pa.array([value, None, value.copy_negate(), Decimal('1.0')], type=factory(precision, 1))
    expected = [1.508079088755887e35, None, -1.508079088755887e35, 1.0]
    target = pa.float64()
    if kind.endswith('_float'):
        target = pa.float32()
        expected = [None if item is None else float(np.float32(item)) for item in expected]
    return source, target, expected


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
@pytest.mark.parametrize('operation', _OPERATIONS)
@pytest.mark.parametrize('shape', ['root', 'row', 'array', 'map'])
@pytest.mark.parametrize('kind', ['fixed_binary', 'empty_fixed_binary', 'half_float',
                                  'decimal_double', 'decimal_float', 'decimal256_double', 'decimal256_float',
                                  'decimal256_wide_double', 'decimal256_wide_float'])
def test_scalar_input_encodings_keep_values(tmp_path, native, stream, operation, shape, kind):
    source, target, expected = _wrap(*_scalar_case(kind), shape)
    table = _table(tmp_path, target, native)
    _apply(table, source, native, stream, operation)
    _check(table, expected)


def _numeric_union(dense, kind='number'):
    if dense:
        left, right = [-1, None, 2], [3, 1, 2 ** 40]
    else:
        left = [-1, 0, 2, 0, None, 0]
        right = [2 ** 40, 1, 2 ** 40, 3, 2 ** 40, 2 ** 40]
    if kind == 'array':
        left_type, right_type, target = pa.list_(pa.int32()), pa.large_list(pa.int64()), pa.list_(pa.int64())
        left = [None if value is None else [value] for value in left]
        right = [[value] for value in right]
        expected = [[1], [2], [3], None]
    elif kind == 'row':
        left_type, right_type, target = pa.struct([('n', pa.int32())]), pa.struct([('n', pa.int64())]), \
            pa.struct([('n', pa.int64())])
        left = [None if value is None else {'n': value} for value in left]
        right = [{'n': value} for value in right]
        expected = [{'n': 1}, {'n': 2}, {'n': 3}, None]
    else:
        left_type, right_type, target = pa.int32(), pa.int64(), pa.int32()
        expected = [1, 2, 3, None]
    children = [pa.array(left, type=left_type), pa.array(right, type=right_type),
                pa.array(['incompatible inactive child'] * (1 if dense else 6))]
    ids = pa.array([7, 42, 7, 42, 7, 42], type=pa.int8())
    kwargs = dict(field_names=['small', 'wide', 'unused'], type_codes=[7, 42, 99])
    union = (pa.UnionArray.from_dense(ids, pa.array([0, 1, 2, 0, 1, 2], type=pa.int32()), children, **kwargs)
             if dense else pa.UnionArray.from_sparse(ids, children, **kwargs))
    return union.slice(1, 4), target, expected


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
@pytest.mark.parametrize('operation', ['row_id', 'incremental', 'upsert'])
@pytest.mark.parametrize('shape', ['root', 'row', 'array', 'map'])
@pytest.mark.parametrize('dense', [False, True], ids=['sparse', 'dense'])
@pytest.mark.parametrize('kind', ['number', 'array', 'row'])
def test_union_inputs_select_only_active_values(tmp_path, native, stream, operation, shape, dense, kind):
    source, target, expected = _wrap(*_numeric_union(dense, kind), shape)
    table = _table(tmp_path, target, native)
    _apply(table, source, native, stream, operation)
    _check(table, expected)


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
@pytest.mark.parametrize('operation', ['row_id', 'incremental', 'upsert'])
@pytest.mark.parametrize('shape', ['root', 'row', 'array', 'map'])
@pytest.mark.parametrize('dense', [False, True], ids=['sparse', 'dense'])
def test_dictionary_union_keeps_null_indices_and_active_nulls(tmp_path, native, stream, operation, shape, dense):
    source, target, expected = _numeric_union(dense)
    source = pa.DictionaryArray.from_arrays(pa.array([3, None, 0, 1], type=pa.int8()), source)
    expected = [expected[3], None, expected[0], expected[1]]
    source, target, expected = _wrap(source, target, expected, shape)
    table = _table(tmp_path, target, native)
    _apply(table, source, native, stream, operation)
    _check(table, expected)


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
@pytest.mark.parametrize('operation', ['row_id', 'incremental', 'upsert'])
@pytest.mark.parametrize('shape,dense', [('row', False), ('row', True), ('fixed_array', True)],
                         ids=['sparse-row', 'dense-row', 'dense-fixed-array'])
@pytest.mark.parametrize('encoded', [False, True], ids=['union', 'dictionary_union'])
def test_union_children_below_null_parents_are_not_converted(
        tmp_path, native, stream, operation, shape, encoded, dense):
    ids = pa.array([42, 7, 7, 42], type=pa.int8())
    if dense:
        source = pa.UnionArray.from_dense(
            ids, pa.array([0, 0, 1, 1], type=pa.int32()),
            [pa.array([1, 2], type=pa.int32()), pa.array(['hidden', 'also hidden'])], type_codes=[7, 42])
    else:
        source = pa.UnionArray.from_sparse(
            ids, [pa.array([0, 1, 2, 0], type=pa.int32()), pa.array(['hidden'] * 4)], type_codes=[7, 42])
    if encoded:
        source = pa.DictionaryArray.from_arrays(pa.array([0, 1, 2, 3], type=pa.int8()), source)
    nulls = pa.array([True, False, False, True])
    if shape == 'row':
        source = pa.StructArray.from_arrays([source], names=['n'], mask=nulls)
        target, expected = pa.struct([('n', pa.int32())]), [None, {'n': 1}, {'n': 2}, None]
    else:
        source = pa.FixedSizeListArray.from_arrays(source, 1, mask=nulls)
        target, expected = pa.list_(pa.int32()), [None, [1], [2], None]
    table = _table(tmp_path, target, native)
    _apply(table, source, native, stream, operation)
    _check(table, expected)


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
@pytest.mark.parametrize('operation', ['row_id', 'incremental', 'upsert'])
@pytest.mark.parametrize('dense', [False, True], ids=['sparse', 'dense'])
def test_map_union_values_do_not_fill_missing_row_fields(tmp_path, native, stream, operation, dense):
    ids = pa.array([7] * 4, type=pa.int8())
    children = [pa.array([1, 2, 3, 4], type=pa.int32())]
    values = (pa.UnionArray.from_dense(ids, pa.array(range(4), type=pa.int32()), children, type_codes=[7])
              if dense else pa.UnionArray.from_sparse(ids, children, type_codes=[7]))
    source = pa.MapArray.from_arrays(pa.array(range(5), type=pa.int32()), pa.array(['n'] * 4), values)
    target = pa.struct([('n', pa.int32()), ('v', pa.string())])
    table = _table(tmp_path, target, native)
    _apply(table, source, native, stream, operation)
    _check(table, [{'n': i, 'v': None} for i in range(1, 5)])


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('operation', ['row_id', 'incremental', 'upsert'])
def test_union_import_aligns_foreign_children_and_row_siblings(tmp_path, native, operation):
    raw = bytearray(b'x' + np.array([1, 2, 3, 4], dtype='<i4').tobytes())
    buffer = pa.py_buffer(memoryview(raw)[1:])
    assert buffer.address % 4 != 0
    numbers = pa.Array.from_buffers(pa.int32(), 4, [None, buffer])
    union = pa.UnionArray.from_sparse(pa.array([7] * 4, type=pa.int8()), [numbers], type_codes=[7])
    source = pa.StructArray.from_arrays([union, numbers], names=['n', 'sibling'])
    target = pa.struct([('n', pa.int32()), ('sibling', pa.int32())])
    table = _table(tmp_path, target, native)
    _apply(table, source, native, False, operation)
    _check(table, [{'n': i, 'sibling': i} for i in range(1, 5)])


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
@pytest.mark.parametrize('operation', ['row_id', 'incremental', 'upsert'])
@pytest.mark.parametrize('shape', ['nested_sibling', 'nested_child', 'masked_struct', 'masked_fixed_array'])
def test_sparse_union_import_normalizes_all_parent_coordinates(tmp_path, native, stream, operation, shape):
    ids = pa.array([7] * 6, type=pa.int8())
    numbers = pa.array([None, 1, 2, None, 4, None], type=pa.int32())
    nested_type = pa.struct([('a', pa.struct([('b', pa.int32())]))])
    nested = pa.array([{'a': {'b': i}} for i in range(6)], type=nested_type)
    union = pa.UnionArray.from_sparse(ids, [pa.array(range(6), type=pa.int32())], type_codes=[7])
    if shape == 'nested_child':
        source = pa.UnionArray.from_sparse(ids, [nested], type_codes=[7])
        target = nested_type
        expected = nested.slice(1, 4).to_pylist()
    else:
        if shape == 'nested_sibling':
            ordinary, ordinary_type = nested, nested_type
        elif shape == 'masked_struct':
            ordinary = pa.StructArray.from_arrays(
                [numbers], fields=[pa.field('n', pa.int32(), nullable=False)], mask=numbers.is_null())
            ordinary_type = pa.struct([('n', pa.int32())])
        else:
            ordinary = pa.FixedSizeListArray.from_arrays(
                numbers, type=pa.list_(pa.field('item', pa.int32(), nullable=False), 1), mask=numbers.is_null())
            ordinary_type = pa.list_(pa.int32())
        source = pa.StructArray.from_arrays([union, ordinary], names=['u', 'ordinary'])
        target = pa.struct([('u', pa.int32()), ('ordinary', ordinary_type)])
        expected = [{'u': i, 'ordinary': item} for i, item in enumerate(ordinary.to_pylist()[1:5], 1)]
    source = source.slice(1, 4)
    source.validate(full=True)
    table = _table(tmp_path, target, native)
    _apply(table, source, native, stream, operation)
    _check(table, expected)


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('operation', _OPERATIONS)
@pytest.mark.parametrize('precision,scale', [(39, 1), (76, 39)])
def test_wide_decimal_floating_input_does_not_require_decimal128(tmp_path, native, operation, precision, scale):
    coefficient = '1508079088755887087797858531882816082' + '0' * (scale - 1)
    value = Decimal(coefficient + 'e-' + str(scale))
    source = pa.array([value, None, value.copy_negate(), Decimal('1.0')], type=pa.decimal256(precision, scale))
    table = _table(tmp_path, pa.float64(), native)
    _apply(table, source, native, False, operation)
    _check(table, [1.508079088755887e35, None, -1.508079088755887e35, 1.0])


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('operation', ['predicate_array', 'predicate_callable', 'merge'])
@pytest.mark.parametrize('shape', ['root', 'row', 'array', 'map'])
def test_union_does_not_relax_safe_assignment_contract(tmp_path, native, operation, shape):
    source, target, _ = _wrap(*_numeric_union(True), shape)
    table = _table(tmp_path, target, native)
    snapshot = table.snapshot_manager().get_latest_snapshot().id
    with pytest.raises((ValueError, TypeError, NotImplementedError)):
        _apply(table, source, native, False, operation)
    assert table.snapshot_manager().get_latest_snapshot().id == snapshot
    _check(table, [None] * 4)


def _pair_union():
    pairs = pa.array([[[1, 2]], [[3, 4]], []], type=pa.list_(pa.list_(pa.int32())))
    source = pa.UnionArray.from_dense(
        pa.array([7, 42, 7, 7], type=pa.int8()), pa.array([0, 0, 1, 2], type=pa.int32()),
        [pairs, pa.nulls(1)], field_names=['pairs', 'null'], type_codes=[7, 42])
    return source, pa.map_(pa.int32(), pa.int64()), [[(1, 2)], None, [(3, 4)], []]


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('operation', ['row_id', 'incremental', 'upsert'])
def test_union_root_map_uses_existing_pair_constructor(tmp_path, native, operation):
    source, target, expected = _pair_union()
    table = _table(tmp_path, target, native)
    _apply(table, source, native, False, operation)
    _check(table, expected)


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('shape', ['row', 'array', 'map'])
def test_union_map_pair_constructor_does_not_leak_into_nested_values(tmp_path, native, shape):
    source, target, _ = _wrap(*_pair_union(), shape)
    table = _table(tmp_path, target, native)
    snapshot = table.snapshot_manager().get_latest_snapshot().id
    with pytest.raises((ValueError, TypeError, NotImplementedError)):
        _apply(table, source, native, False, 'row_id')
    assert table.snapshot_manager().get_latest_snapshot().id == snapshot
    _check(table, [None] * 4)


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('operation', ['row_id', 'incremental', 'upsert'])
def test_union_wrapped_dict_preserves_explicit_null_map_values(tmp_path, native, operation):
    rows = pa.array([{'a': 1, 'b': None}, {'a': 2, 'b': 3}], type=pa.struct([('a', pa.int32()), ('b', pa.int32())]))
    source = pa.UnionArray.from_dense(
        pa.array([42, 42, 42, 42], type=pa.int8()), pa.array([0, 1, 0, 1], type=pa.int32()),
        [rows], field_names=['row'], type_codes=[42])
    target = pa.map_(pa.string(), pa.int64())
    table = _table(tmp_path, target, native)
    _apply(table, source, native, False, operation)
    _check(table, [[('a', 1), ('b', None)], [('a', 2), ('b', 3)]] * 2)


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('operation', ['row_id', 'upsert'])
def test_union_constructor_does_not_retry_safe_casts_per_child(tmp_path, native, operation):
    source = pa.UnionArray.from_dense(
        pa.array([7, 42, 7, 42], type=pa.int8()), pa.array([0, 0, 1, 1], type=pa.int32()),
        [pa.array([1.5, 2.5]), pa.array(['3', '4'])], type_codes=[7, 42])
    table = _table(tmp_path, pa.int32(), native)
    snapshot = table.snapshot_manager().get_latest_snapshot().id
    with pytest.raises((ValueError, TypeError, NotImplementedError)):
        _apply(table, source, native, False, operation)
    assert table.snapshot_manager().get_latest_snapshot().id == snapshot
    _check(table, [None] * 4)


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('operation', _OPERATIONS)
def test_invalid_fixed_binary_text_does_not_commit(tmp_path, native, operation):
    source = pa.array([b'abc', None, b'\xffab', b'xyz'], type=pa.binary(3))
    table = _table(tmp_path, pa.string(), native)
    snapshot = table.snapshot_manager().get_latest_snapshot().id
    with pytest.raises((ValueError, TypeError)):
        _apply(table, source, native, False, operation)
    assert table.snapshot_manager().get_latest_snapshot().id == snapshot
    _check(table, [None] * 4)


def _decimal_type(width, precision, scale):
    factory = getattr(pa, 'decimal{}'.format(width), None)
    if factory is None:
        pytest.skip('Arrow does not provide this decimal encoding')
    return factory(precision, scale)


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('operation', _OPERATIONS)
@pytest.mark.parametrize('width,precision,scale', [
    (32, 9, -128), (64, 18, -90), (128, 38, -2), (256, 60, -40), (256, 76, -128),
])
def test_zero_decimal_inputs_cross_physical_scale_boundaries(tmp_path, native, operation, width, precision, scale):
    source_type = _decimal_type(width, precision, scale)
    source = pa.array([Decimal('0'), None, Decimal('0'), None], type=source_type)
    target = pa.decimal128(38, 38)
    table = _table(tmp_path, target, native)
    _apply(table, source, native, False, operation)
    _check(table, [Decimal('0'), None, Decimal('0'), None])


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('operation', _OPERATIONS)
@pytest.mark.parametrize('width,precision', [(32, 9), (64, 18), (128, 38), (256, 76)])
def test_decimal_input_widths_keep_exact_rescaling(tmp_path, native, operation, width, precision):
    source_type = _decimal_type(width, precision, 3)
    source = pa.array([Decimal('1.250'), None, Decimal('-2.500'), Decimal('3.000')], type=source_type)
    target = pa.decimal128(10, 2)
    table = _table(tmp_path, target, native)
    _apply(table, source, native, False, operation)
    _check(table, [Decimal('1.25'), None, Decimal('-2.50'), Decimal('3.00')])


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
def test_union_update_casts_do_not_relax_append_validation(tmp_path, native):
    source, target, _ = _numeric_union(True)
    table = _table(tmp_path, target, native)
    data = pa.table({'id': pa.array([0, 1, 2, 99], type=pa.int32()), 'value': source})
    builder = table.new_batch_write_builder()
    snapshot = table.snapshot_manager().get_latest_snapshot().id
    with _native_only(native), pytest.raises((ValueError, TypeError, NotImplementedError)):
        builder.new_update().with_update_type(['value']).upsert_by_arrow_with_key(data, ['id'])
    assert table.snapshot_manager().get_latest_snapshot().id == snapshot
    _check(table, [None] * 4)
