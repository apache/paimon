# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
from decimal import Decimal
from unittest.mock import patch

import pyarrow as pa
import pyarrow.compute as pc

from pypaimon.data.generic_variant import GenericVariant
from pypaimon.data.variant_path import variant_get, variant_replace
from pypaimon.data.variant_shredding import _encode_scalar_to_value_bytes


def _variants(values):
    return GenericVariant.to_arrow_array([
        GenericVariant.from_python(value) if value is not None else None
        for value in values
    ])


def _decode(column):
    return [
        None if value is None
        else GenericVariant.from_arrow_struct(value).to_python()
        for value in column.to_pylist()
    ]


def _float_variants(values):
    metadata = b'\x01\x00'
    return GenericVariant.to_arrow_array([
        GenericVariant(
            _encode_scalar_to_value_bytes(value, pa.float32()), metadata)
        for value in values
    ])


class TestVariantGet(unittest.TestCase):

    def test_get_nested_path_and_missing_values(self):
        column = pa.chunked_array([
            _variants([{'a.b': [{'value': 1.5}]}, None]),
            _variants([{'other': 2.0}, {'a.b': [{'value': -3.5}]}]),
        ])

        result = variant_get(
            column, '$["a.b"][0].value', pa.float64())

        self.assertIsInstance(result, pa.ChunkedArray)
        self.assertEqual(result.num_chunks, 2)
        self.assertEqual(result.to_pylist(), [1.5, None, None, -3.5])

    def test_get_float_without_full_decode(self):
        column = _float_variants([1.25, -2.5])

        with patch.object(
                GenericVariant, 'to_python',
                side_effect=AssertionError("full decode is not allowed")):
            result = variant_get(column, '$', pa.float32())

        self.assertEqual(result.to_pylist(), [1.25, -2.5])

    def test_get_path_mapping_in_one_pass(self):
        column = _variants([
            {'velocity': {'y': 1.0, 'z': -2.0}},
            {'velocity': {'y': 3.0, 'z': -4.0}},
        ])

        result = variant_get(column, {
            '$.velocity.y': pa.float64(),
            '$.velocity.z': pa.float64(),
        })

        self.assertEqual(result['$.velocity.y'].to_pylist(), [1.0, 3.0])
        self.assertEqual(result['$.velocity.z'].to_pylist(), [-2.0, -4.0])

    def test_get_matches_java_cast_semantics(self):
        column = _variants([{
            'long': 123,
            'object': {'age': 2},
            'array': [1, '2'],
        }])

        self.assertEqual(
            variant_get(column, '$.long', pa.string()).to_pylist(), ['123'])
        self.assertEqual(
            variant_get(column, '$.object', pa.string()).to_pylist(),
            ['{"age":2}'],
        )
        self.assertEqual(
            variant_get(column, '$.array', pa.string()).to_pylist(),
            ['[1,"2"]'],
        )
        self.assertEqual(
            variant_get(
                column,
                '$.object',
                pa.struct([('age', pa.int32()), ('name', pa.string())]),
            ).to_pylist(),
            [{'age': 2, 'name': None}],
        )
        self.assertEqual(
            variant_get(
                column, '$.array', pa.list_(pa.int32())).to_pylist(),
            [[1, 2]],
        )
        self.assertEqual(
            variant_get(
                column, '$.object',
                pa.map_(pa.string(), pa.int32())).to_pylist(),
            [[('age', 2)]],
        )
        with self.assertRaisesRegex(ValueError, "Invalid cast"):
            variant_get(column, '$.object', pa.int32())

    def test_get_decimal_is_exact(self):
        expected = Decimal('12345678901234567890123456789012345678')
        column = _variants([{'value': expected}])

        result = variant_get(
            column, '$.value', pa.decimal128(38, 0))

        self.assertEqual(result.to_pylist(), [expected])

    def test_get_copies_only_selected_subtree(self):
        column = _variants([{'small': 'x', 'large': b'x' * (2 * 1024 * 1024)}])
        decoded_sizes = []
        original = GenericVariant.to_python

        def decode(selected):
            decoded_sizes.append(len(selected.value()))
            return original(selected)

        with patch.object(GenericVariant, 'to_python', decode):
            result = variant_get(column, '$.small', pa.string())

        self.assertEqual(result.to_pylist(), ['x'])
        self.assertEqual(decoded_sizes, [2])

    def test_get_rejects_invalid_arguments(self):
        column = _variants([{'value': 1}])
        with self.assertRaisesRegex(ValueError, "Invalid VARIANT path"):
            variant_get(column, 'value', pa.int64())
        with self.assertRaisesRegex(TypeError, "PyArrow data type"):
            variant_get(column, '$.value', 'BIGINT')


class TestVariantReplace(unittest.TestCase):

    def test_truncated_value_does_not_cross_row_boundary(self):
        first = GenericVariant.from_python({'value': 1.0})
        second = GenericVariant.from_python({'value': 2.0})
        column = pa.StructArray.from_arrays(
            [
                pa.array([first.value()[:-8], second.value()]),
                pa.array([first.metadata(), second.metadata()]),
            ],
            names=['value', 'metadata'],
        )

        with self.assertRaisesRegex(ValueError, "MALFORMED_VARIANT"):
            variant_replace(
                column, '$.value',
                pa.array([3.0, 4.0], type=pa.float64()))
        self.assertEqual(
            GenericVariant.from_arrow_struct(column[1].as_py()).to_python(),
            {'value': 2.0},
        )

    def test_equal_length_uses_copy_on_write(self):
        column = _variants([
            {'number': 1.0, 'text': 'keep'},
            None,
            {'number': -2.0, 'text': 'also keep'},
        ])
        original = column.to_pylist()
        replacement = pa.array([-1.0, 0.0, 2.0], type=pa.float64())

        with patch.object(
                GenericVariant, 'to_python',
                side_effect=AssertionError("full decode is not allowed")):
            result = variant_replace(
                column, '$.number', replacement)

        self.assertEqual(column.to_pylist(), original)
        self.assertEqual(_decode(result), [
            {'number': -1.0, 'text': 'keep'},
            None,
            {'number': 2.0, 'text': 'also keep'},
        ])
        self.assertEqual(
            column.field('value').buffers()[1].address,
            result.field('value').buffers()[1].address,
        )
        self.assertNotEqual(
            column.field('value').buffers()[2].address,
            result.field('value').buffers()[2].address,
        )
        self.assertEqual(
            column.field('metadata').buffers()[2].address,
            result.field('metadata').buffers()[2].address,
        )
        self.assertEqual(
            column.buffers()[0].address,
            result.buffers()[0].address,
        )

    def test_sliced_input_copies_only_visible_values(self):
        base = _variants([
            {'number': float(i), 'padding': 'x' * 1000}
            for i in range(100)
        ])

        for binary_type in (pa.binary(), pa.large_binary()):
            with self.subTest(binary_type=binary_type):
                values = base.field('value').cast(binary_type)
                converted = pa.StructArray.from_arrays(
                    [values, base.field('metadata')],
                    names=['value', 'metadata'],
                )
                column = converted.slice(50, 3)

                result = variant_replace(
                    column,
                    '$.number',
                    pa.scalar(-1.0, type=pa.float64()),
                )

                expected_size = sum(
                    len(value)
                    for value in column.field('value').to_pylist()
                )
                self.assertEqual(
                    result.field('value').buffers()[2].size,
                    expected_size,
                )
                self.assertEqual(result.field('value').offset, 0)
                self.assertEqual(
                    [row['number'] for row in _decode(result)],
                    [-1.0, -1.0, -1.0],
                )

    def test_get_compute_replace_pipeline(self):
        column = pa.chunked_array([
            _variants([{'y': 1.0, 'z': -2.0}, None]),
            _variants([{'y': -3.0, 'z': 4.0}]),
        ])
        current = variant_get(column, {
            '$.y': pa.float64(),
            '$.z': pa.float64(),
        })

        result = variant_replace(column, {
            '$.y': pc.negate(current['$.y']),
            '$.z': pc.negate(current['$.z']),
        })

        self.assertIsInstance(result, pa.ChunkedArray)
        self.assertEqual(result.num_chunks, 2)
        self.assertEqual(_decode(result), [
            {'y': -1.0, 'z': 2.0}, None,
            {'y': 3.0, 'z': -4.0},
        ])

    def test_vectorized_paths_support_varying_offsets(self):
        column = _variants([
            {
                'prefix': 'x' * index,
                'items': ['y' * (64 - index), {
                    'value': float(index),
                    'other': float(-index),
                }],
            }
            for index in range(1, 64)
        ])
        paths = {
            '$.items[1].value': pa.float64(),
            '$.items[1].other': pa.float64(),
        }

        with patch(
                'pypaimon.data.variant_path._path_positions',
                side_effect=AssertionError("slow path is not allowed")):
            current = variant_get(column, paths)
            result = variant_replace(column, {
                path: pc.negate(values)
                for path, values in current.items()
            })

        decoded = _decode(result)
        self.assertEqual(
            [row['items'][1]['value'] for row in decoded],
            [float(-index) for index in range(1, 64)],
        )
        self.assertEqual(
            [row['items'][1]['other'] for row in decoded],
            [float(index) for index in range(1, 64)],
        )

    def test_vectorized_paths_support_wide_containers(self):
        rows = []
        for row in range(3):
            value = {'field_%03d' % i: i for i in range(300)}
            value['field_299'] = float(row)
            value['items'] = list(range(299)) + [float(row + 10)]
            rows.append(value)
        column = _variants(rows)
        paths = {
            '$.field_299': pa.float64(),
            '$.items[299]': pa.float64(),
        }

        with patch(
                'pypaimon.data.variant_path._path_positions',
                side_effect=AssertionError("slow path is not allowed")):
            current = variant_get(column, paths)
            result = variant_replace(column, {
                path: pc.negate(values)
                for path, values in current.items()
            })

        self.assertEqual(current['$.field_299'].to_pylist(), [0.0, 1.0, 2.0])
        self.assertEqual(
            current['$.items[299]'].to_pylist(), [10.0, 11.0, 12.0])
        decoded = _decode(result)
        self.assertEqual(
            [row['field_299'] for row in decoded], [0.0, -1.0, -2.0])
        self.assertEqual(
            [row['items'][299] for row in decoded], [-10.0, -11.0, -12.0])

    def test_scalar_and_same_length_string_replacement(self):
        column = _variants([{'text': 'aa'}, {'text': 'bb'}])

        result = variant_replace(
            column, '$.text', pa.scalar('xy', type=pa.string()))

        self.assertEqual(_decode(result), [{'text': 'xy'}, {'text': 'xy'}])
        self.assertEqual(
            column.field('value').buffers()[1].address,
            result.field('value').buffers()[1].address,
        )

    def test_decimal_replacement_preserves_arrow_value(self):
        for data_type, value, expected_exponent in (
                (pa.decimal128(10, 2), Decimal('100.00'), -2),
                (pa.decimal128(10, -2), Decimal('1E+2'), 0)):
            with self.subTest(data_type=data_type):
                column = _variants([{'value': 0}])

                result = variant_replace(
                    column,
                    '$.value',
                    pa.array([value], type=data_type),
                )

                decoded = _decode(result)[0]['value']
                self.assertEqual(decoded, Decimal('100.00'))
                self.assertEqual(
                    decoded.as_tuple().exponent, expected_exponent)

    def test_different_length_rebuilds_offsets(self):
        column = _variants([
            {
                'before': 1,
                'nested': {'items': [0, {'text': 'a'}]},
                'after': 2,
            },
            {
                'before': 3,
                'nested': {'items': [0, {'text': 'bb'}]},
                'after': 4,
            },
        ])

        result = variant_replace(
            column,
            '$.nested.items[1].text',
            pa.array(['a much longer value', 'x'], type=pa.string()),
        )

        self.assertEqual(_decode(result), [
            {
                'before': 1,
                'nested': {'items': [0, {'text': 'a much longer value'}]},
                'after': 2,
            },
            {
                'before': 3,
                'nested': {'items': [0, {'text': 'x'}]},
                'after': 4,
            },
        ])
        self.assertEqual(
            column.field('metadata').buffers()[2].address,
            result.field('metadata').buffers()[2].address,
        )

    def test_same_length_type_change_uses_fast_path(self):
        column = _variants([1000000, -1000000])

        result = variant_replace(
            column, '$', pa.array([1.25, -2.5], type=pa.float32()))

        self.assertEqual(_decode(result), [1.25, -2.5])
        self.assertEqual(
            column.field('value').buffers()[1].address,
            result.field('value').buffers()[1].address,
        )

    def test_missing_path_is_noop_or_strict_error(self):
        column = _variants([{'value': 1}, {'other': 2}])
        replacement = pa.array([10, 20], type=pa.int64())

        result = variant_replace(column, '$.value', replacement)

        self.assertEqual(_decode(result), [{'value': 10}, {'other': 2}])
        with self.assertRaisesRegex(ValueError, "path does not exist"):
            variant_replace(
                column, '$.value', replacement, strict=True)

    def test_sql_null_replacement_and_sliced_input(self):
        base = _variants([
            {'value': 0.0}, {'value': 1.0},
            {'value': 2.0}, {'value': 3.0},
        ])
        column = base.slice(1, 2)

        result = variant_replace(
            column,
            '$.value',
            pa.array([None, -2.0], type=pa.float64()),
        )

        self.assertEqual(_decode(result), [{'value': None}, {'value': -2.0}])
        self.assertEqual(result.is_valid().to_pylist(), [True, True])

    def test_rejects_invalid_arguments(self):
        column = _variants([{'value': 1.0}, {'value': 2.0}])
        cases = [
            ('value', pa.scalar(1.0), False,
             ValueError, "Invalid VARIANT path"),
            ('$.value', pa.array([1.0]), False,
             ValueError, "length must match"),
            ('$.value', 1.0, False,
             TypeError, "Arrow Scalar or Array"),
            ('$.value', pa.array([{'x': 1}, {'x': 2}]), False,
             TypeError, "Unsupported VARIANT replacement type"),
            ('$.value', pa.scalar(1.0), 'yes',
             TypeError, "strict must be a boolean"),
        ]
        for path, replacement, strict, error_type, message in cases:
            with self.subTest(path=path, replacement=replacement):
                with self.assertRaisesRegex(error_type, message):
                    variant_replace(
                        column, path, replacement, strict=strict)

        with self.assertRaisesRegex(TypeError, "must be omitted"):
            variant_get(
                column, {'$.value': pa.float64()}, pa.float64())
        with self.assertRaisesRegex(TypeError, "must be omitted"):
            variant_replace(
                column, {'$.value': pa.scalar(1.0)}, pa.scalar(2.0))
        with self.assertRaisesRegex(ValueError, "must not overlap"):
            variant_replace(column, {
                '$.value': pa.scalar(1.0),
                '$.value.child': pa.scalar(2.0),
            })


if __name__ == '__main__':
    unittest.main()
