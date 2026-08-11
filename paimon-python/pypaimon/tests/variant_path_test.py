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

import datetime
import unittest
from decimal import Decimal
from unittest.mock import patch

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from pypaimon.data._variant_binary import _primitive_header
from pypaimon.data.generic_variant import _DECIMAL4, _DOUBLE, GenericVariant
from pypaimon.data.variant_path import (
    _path_positions,
    variant_get,
    variant_replace,
)
from pypaimon.data.variant_shredding import (
    _build_array_value,
    _build_object_value,
    _encode_scalar_to_value_bytes,
)


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

    def test_get_matches_java_string_and_binary_casts(self):
        column = _variants([{
            'decimal': '1.9',
            'overflow': '2147483648',
            'truthy': 'yes',
            'falsey': '0',
            'binary': b'abc',
        }])

        self.assertEqual(
            variant_get(column, '$.decimal', pa.int32()).to_pylist(), [1])
        self.assertEqual(
            variant_get(column, '$.truthy', pa.bool_()).to_pylist(), [True])
        self.assertEqual(
            variant_get(column, '$.falsey', pa.bool_()).to_pylist(), [False])
        self.assertEqual(
            variant_get(column, '$.binary', pa.binary()).to_pylist(),
            [b'abc'],
        )
        self.assertEqual(
            variant_get(column, '$.binary', pa.string()).to_pylist(),
            ['abc'],
        )
        with self.assertRaisesRegex(ValueError, "Invalid cast"):
            variant_get(column, '$.overflow', pa.int32())

        temporal = _variants(['1', '2026-08-10'])
        self.assertEqual(
            variant_get(temporal, '$', pa.date32()).to_pylist(),
            [datetime.date(1970, 1, 2), datetime.date(2026, 8, 10)],
        )
        self.assertEqual(
            variant_get(temporal.slice(0, 1), '$', pa.timestamp('us'))
            .to_pylist(),
            [datetime.datetime(1970, 1, 1, 0, 0, 0, 1)],
        )

    def test_get_matches_java_numeric_strings(self):
        doubles = _variants([
            Decimal('100.00'), 1e20, 1e-4,
            float('inf'), float('-inf'), float('nan'),
        ])

        self.assertEqual(
            variant_get(doubles, '$', pa.string()).to_pylist(),
            ['100.00', '1.0E20', '1.0E-4',
             'Infinity', '-Infinity', 'NaN'],
        )

        floats = GenericVariant.to_arrow_array([
            GenericVariant(
                _encode_scalar_to_value_bytes(value, pa.float32()),
                b'\x01\x00',
            )
            for value in (1.2, 1e20, 1e-4)
        ])
        self.assertEqual(
            variant_get(floats, '$', pa.string()).to_pylist(),
            ['1.2', '1.0E20', '1.0E-4'],
        )

        nested = GenericVariant.to_arrow_array([
            GenericVariant(
                _build_object_value([(0, floats[0].as_py()['value'])]),
                GenericVariant.from_python({'value': 0}).metadata(),
            ),
            GenericVariant(
                _build_array_value([floats[0].as_py()['value']]),
                b'\x01\x00',
            ),
        ])
        self.assertEqual(
            variant_get(nested, '$', pa.string()).to_pylist(),
            ['{"value":1.2}', '[1.2]'],
        )
        strings = _variants(['1234567', '12345678901234'])
        self.assertEqual(
            variant_get(strings, '$', pa.string()).to_pylist(),
            ['1234567', '12345678901234'],
        )

    def test_get_matches_java_numeric_casts(self):
        column = _variants([
            {'value': 1e20},
            {'value': float('nan')},
            {'value': float('inf')},
            {'value': float('-inf')},
        ])
        self.assertEqual(
            variant_get(column, '$.value', pa.int32()).to_pylist(),
            [2147483647, 0, 2147483647, -2147483648],
        )

        invalid = _variants([
            {'value': Decimal('1.0')},
            {'value': 1.5},
        ])
        with self.assertRaisesRegex(ValueError, "Invalid cast"):
            variant_get(invalid.slice(0, 1), '$.value', pa.bool_())
        with self.assertRaisesRegex(ValueError, "Invalid cast"):
            variant_get(invalid.slice(1, 1), '$.value', pa.timestamp('us'))

        timestamp = variant_get(
            _variants([{'value': 1}]), '$.value', pa.timestamp('us'))
        self.assertEqual(
            timestamp.to_pylist(), [datetime.datetime(1970, 1, 1, 0, 0, 1)])

        nanos = variant_get(
            _variants(['1', '-1', '2026-08-10 12:34:56.123456789']),
            '$',
            pa.timestamp('ns'),
        )
        self.assertEqual(
            nanos.cast(pa.int64()).to_pylist(),
            [
                1,
                -1,
                int(np.datetime64(
                    '2026-08-10T12:34:56.123456789', 'ns').astype(np.int64)),
            ],
        )

    def test_variant_null_remains_arrow_null(self):
        column = _variants([None, {'value': None}, {'value': 1.0}])

        result = variant_get(column, '$.value', pa.float64())

        self.assertEqual(result.to_pylist(), [None, None, 1.0])

    def test_get_decimal_is_exact(self):
        expected = Decimal('12345678901234567890123456789012345678')
        column = _variants([{'value': expected}])

        result = variant_get(
            column, '$.value', pa.decimal128(38, 0))

        self.assertEqual(result.to_pylist(), [expected])

    def test_get_rejects_malformed_metadata_and_decimal(self):
        valid = GenericVariant.from_python({'value': 1.0})
        bad_metadata = bytes([2]) + valid.metadata()[1:]
        column = pa.StructArray.from_arrays([
            pa.array([valid.value()]),
            pa.array([bad_metadata]),
        ], names=['value', 'metadata'])
        with self.assertRaisesRegex(ValueError, "metadata version"):
            variant_get(column, '$.value', pa.float64())

        for scale, unscaled in ((10, 1), (0, 2147483647)):
            with self.subTest(scale=scale, unscaled=unscaled):
                value = (
                    bytes([_primitive_header(_DECIMAL4), scale])
                    + unscaled.to_bytes(4, 'little', signed=True)
                )
                malformed = GenericVariant.to_arrow_array([
                    GenericVariant(value, b'\x01\x00')])
                with self.assertRaisesRegex(
                        ValueError, "decimal precision or scale"):
                    variant_get(malformed, '$', pa.decimal128(38, 0))

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

        invalid_metadata = pa.StructArray.from_arrays(
            [
                pa.array([None], type=pa.binary()),
                pa.array([None], type=pa.string()),
            ],
            names=['value', 'metadata'],
            mask=pa.array([True]),
        )
        with self.assertRaisesRegex(TypeError, "metadata field must be binary"):
            variant_get(invalid_metadata, '$.value', pa.float64())


class TestVariantReplace(unittest.TestCase):

    def test_timestamp_replacement_is_exact(self):
        for arrow_type, value in (
                (pa.timestamp('us'),
                 datetime.datetime(9999, 12, 31, 23, 59, 59, 999999)),
                (pa.timestamp('us', tz='UTC'),
                 datetime.datetime(
                     2500, 1, 1, 0, 0, 0, 1,
                     tzinfo=datetime.timezone.utc))):
            with self.subTest(arrow_type=arrow_type):
                column = _variants([{'value': 0}])
                result = variant_replace(
                    column, '$.value', pa.scalar(value, type=arrow_type))
                self.assertEqual(_decode(result), [{'value': value}])

        column = _variants([{'value': 0}])
        for nanos in (1, -1, 1001, -1001):
            with self.subTest(nanos=nanos):
                with self.assertRaisesRegex(ValueError, "microsecond-aligned"):
                    variant_replace(
                        column,
                        '$.value',
                        pa.scalar(nanos, type=pa.timestamp('ns')),
                    )
        for nanos in (1000, -1000):
            with self.subTest(nanos=nanos):
                result = variant_replace(
                    column,
                    '$.value',
                    pa.scalar(nanos, type=pa.timestamp('ns')),
                )
                self.assertEqual(
                    _decode(result),
                    [{'value': datetime.datetime(1970, 1, 1)
                      + datetime.timedelta(microseconds=nanos // 1000)}],
                )

    def test_nullable_fast_path_does_not_devectorize_chunk(self):
        size = 50000
        column = _variants(
            [None] + [{'value': float(index)} for index in range(1, size)])
        replacement = pa.scalar(-1.0)

        with patch(
                'pypaimon.data.variant_path._path_positions',
                wraps=_path_positions,
        ) as slow_path:
            current = variant_get(column, '$.value', pa.float64())
            result = variant_replace(column, '$.value', replacement)

        self.assertEqual(current[0].as_py(), None)
        self.assertEqual(current[-1].as_py(), float(size - 1))
        self.assertIsNone(result[0].as_py())
        self.assertEqual(_decode(result.slice(size - 1, 1)),
                         [{'value': -1.0}])
        slow_path.assert_not_called()

    def test_sparse_layout_anomalies_keep_slow_path_bounded(self):
        size = 4096
        uniform = _variants([
            {'value': float(index)} for index in range(1, size)
        ])
        cases = (
            (_variants([{'extra': 1, 'value': 0.0}]), False),
            (_variants([{'other': 0.0}]), True),
        )
        for first, missing in cases:
            with self.subTest(missing=missing):
                column = pa.concat_arrays([first, uniform])
                with patch(
                        'pypaimon.data.variant_path._path_positions',
                        wraps=_path_positions,
                ) as slow_path:
                    current = variant_get(
                        column, '$.value', pa.float64())
                    self.assertLessEqual(
                        slow_path.call_count, 64)
                self.assertEqual(current[0].as_py(), None if missing else 0.0)
                self.assertEqual(current[-1].as_py(), float(size - 1))

                with patch(
                        'pypaimon.data.variant_path._path_positions',
                        wraps=_path_positions,
                ) as slow_path:
                    result = variant_replace(
                        column, '$.value', pa.scalar(-1.0))
                    self.assertLessEqual(
                        slow_path.call_count, 64)
                expected_first = {'other': 0.0} if missing else {
                    'extra': 1, 'value': -1.0,
                }
                self.assertEqual(_decode(result.slice(0, 1)),
                                 [expected_first])
                self.assertEqual(_decode(result.slice(size - 1, 1)),
                                 [{'value': -1.0}])

    def test_sparse_value_types_keep_slow_path_bounded(self):
        size = 4096
        uniform = _variants([
            {'value': float(index)} for index in range(1, size)
        ])
        for exceptional in (None, 1):
            with self.subTest(exceptional=exceptional):
                column = pa.concat_arrays([
                    _variants([{'value': exceptional}]), uniform,
                ])
                with patch(
                        'pypaimon.data.variant_path._path_positions',
                        wraps=_path_positions,
                ) as slow_path:
                    current = variant_get(
                        column, '$.value', pa.float64())
                    self.assertLessEqual(slow_path.call_count, 1)
                self.assertEqual(
                    current[0].as_py(),
                    None if exceptional is None else 1.0,
                )

                with patch(
                        'pypaimon.data.variant_path._path_positions',
                        wraps=_path_positions,
                ) as slow_path:
                    result = variant_replace(
                        column, '$.value', pa.scalar(-1.0))
                    self.assertLessEqual(slow_path.call_count, 1)
                self.assertEqual(_decode(result.slice(0, 1)),
                                 [{'value': -1.0}])
                self.assertEqual(_decode(result.slice(size - 1, 1)),
                                 [{'value': -1.0}])

        replacement = pa.array(
            [None] + [-1.0] * (len(uniform) - 1), type=pa.float64())
        with patch(
                'pypaimon.data.variant_path._path_positions',
                wraps=_path_positions,
        ) as slow_path:
            result = variant_replace(uniform, '$.value', replacement)
            self.assertLessEqual(slow_path.call_count, 1)
        self.assertEqual(_decode(result.slice(0, 1)), [{'value': None}])
        self.assertEqual(_decode(result.slice(len(uniform) - 1, 1)),
                         [{'value': -1.0}])

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

    def test_truncated_object_child_does_not_cross_sibling_boundary(self):
        valid = GenericVariant.from_python({'a': 1.0, 'b': 2.0})
        value = _build_object_value([
            (0, bytes([_primitive_header(_DOUBLE)])),
            (1, _encode_scalar_to_value_bytes(2.0, pa.float64())),
        ])
        column = GenericVariant.to_arrow_array([
            GenericVariant(value, valid.metadata())])

        with self.assertRaisesRegex(ValueError, "MALFORMED_VARIANT"):
            variant_get(column, '$.a', pa.float64())
        with self.assertRaisesRegex(ValueError, "MALFORMED_VARIANT"):
            variant_replace(column, '$.a', pa.scalar(3.0))

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
