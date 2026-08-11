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
from unittest.mock import patch

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from pypaimon.data._variant_binary import _primitive_header
from pypaimon.data.generic_variant import _DOUBLE, GenericVariant
from pypaimon.data.variant_path import (
    _path_positions,
    _rebuilt_offsets,
    variant_get,
    variant_replace,
)
from pypaimon.data.variant_shredding import (
    _build_object_value,
    _encode_scalar_to_value_bytes,
)


def _variants(values):
    return GenericVariant.to_arrow_array([
        GenericVariant.from_python(value) if value is not None else None
        for value in values
    ])


def _float_variants(values):
    metadata = b'\x01\x00'
    return GenericVariant.to_arrow_array([
        GenericVariant(
            _encode_scalar_to_value_bytes(value, pa.float32()), metadata)
        for value in values
    ])


def _decode(column):
    return [
        None if value is None
        else GenericVariant.from_arrow_struct(value).to_python()
        for value in column.to_pylist()
    ]


class TestVariantGet(unittest.TestCase):

    def test_nested_paths_and_missing_values(self):
        column = pa.chunked_array([
            _variants([{'a.b': [{'value': 1.5}]}, None]),
            _variants([{'other': 2.0}, {'a.b': [{'value': -3.5}]}]),
        ])

        result = variant_get(
            column, '$["a.b"][0].value', pa.float64())

        self.assertIsInstance(result, pa.ChunkedArray)
        self.assertEqual(result.num_chunks, 2)
        self.assertEqual(result.to_pylist(), [1.5, None, None, -3.5])

    def test_reads_float_without_full_decode(self):
        column = _float_variants([1.25, -2.5])

        with patch.object(
                GenericVariant, 'to_python',
                side_effect=AssertionError("full decode is not allowed")):
            result = variant_get(column, '$', pa.float32())

        self.assertEqual(result.to_pylist(), [1.25, -2.5])

    def test_reads_multiple_paths_in_one_pass(self):
        column = _variants([
            {'velocity': {'x': 1.0, 'y': -2.0}},
            {'velocity': {'x': 3.0, 'y': -4.0}},
        ])

        result = variant_get(column, {
            '$.velocity.x': pa.float64(),
            '$.velocity.y': pa.float64(),
        })

        self.assertEqual(result['$.velocity.x'].to_pylist(), [1.0, 3.0])
        self.assertEqual(result['$.velocity.y'].to_pylist(), [-2.0, -4.0])

    def test_requires_exact_float_type(self):
        cases = (
            (_float_variants([1.25]), pa.float64()),
            (_variants([1.25]), pa.float32()),
            (_variants([1]), pa.float64()),
        )
        for column, data_type in cases:
            with self.subTest(data_type=data_type):
                with self.assertRaisesRegex(TypeError, "does not match"):
                    variant_get(column, '$', data_type)

        with self.assertRaisesRegex(TypeError, "float32 or float64"):
            variant_get(_variants([1.0]), '$', pa.string())

    def test_variant_null_is_arrow_null(self):
        column = _variants([None, {'value': None}, {'value': 1.0}])

        result = variant_get(column, '$.value', pa.float64())

        self.assertEqual(result.to_pylist(), [None, None, 1.0])

    def test_rejects_malformed_rows(self):
        valid = GenericVariant.from_python({'value': 1.0})
        column = pa.StructArray.from_arrays([
            pa.array([valid.value()[:-8]]),
            pa.array([valid.metadata()]),
        ], names=['value', 'metadata'])
        with self.assertRaisesRegex(ValueError, "MALFORMED_VARIANT"):
            variant_get(column, '$.value', pa.float64())

        value = _build_object_value([
            (0, bytes([_primitive_header(_DOUBLE)])),
            (1, _encode_scalar_to_value_bytes(2.0, pa.float64())),
        ])
        siblings = GenericVariant.to_arrow_array([
            GenericVariant(
                value,
                GenericVariant.from_python({'a': 0, 'b': 0}).metadata(),
            )
        ])
        with self.assertRaisesRegex(ValueError, "MALFORMED_VARIANT"):
            variant_get(siblings, '$.a', pa.float64())

    def test_rejects_invalid_arguments(self):
        column = _variants([{'value': 1.0}])
        with self.assertRaisesRegex(ValueError, "Invalid VARIANT path"):
            variant_get(column, 'value', pa.float64())
        with self.assertRaisesRegex(TypeError, "PyArrow data type"):
            variant_get(column, '$.value', 'DOUBLE')
        with self.assertRaisesRegex(TypeError, "must be omitted"):
            variant_get(
                column, {'$.value': pa.float64()}, pa.float64())

        invalid_metadata = pa.StructArray.from_arrays(
            [
                pa.array([None], type=pa.binary()),
                pa.array([None], type=pa.string()),
            ],
            names=['value', 'metadata'],
            mask=pa.array([True]),
        )
        with self.assertRaisesRegex(
                TypeError, "metadata field must be binary"):
            variant_get(invalid_metadata, '$.value', pa.float64())


class TestVariantReplace(unittest.TestCase):

    def test_get_compute_replace_pipeline(self):
        column = pa.chunked_array([
            _variants([{'x': 1.0, 'y': -2.0}, None]),
            _variants([{'x': -3.0, 'y': 4.0}]),
        ])
        current = variant_get(column, {
            '$.x': pa.float64(),
            '$.y': pa.float64(),
        })

        result = variant_replace(column, {
            path: pc.negate(values)
            for path, values in current.items()
        })

        self.assertIsInstance(result, pa.ChunkedArray)
        self.assertEqual(_decode(result), [
            {'x': -1.0, 'y': 2.0}, None,
            {'x': 3.0, 'y': -4.0},
        ])

    def test_updates_four_double_paths(self):
        column = _variants([
            {'a': 1.0, 'b': 2.0, 'nested': {'c': 3.0, 'd': 4.0}},
            {'a': -1.0, 'b': -2.0, 'nested': {'c': -3.0, 'd': -4.0}},
        ])
        paths = {
            '$.a': pa.float64(),
            '$.b': pa.float64(),
            '$.nested.c': pa.float64(),
            '$.nested.d': pa.float64(),
        }

        current = variant_get(column, paths)
        result = variant_replace(column, {
            path: pc.negate(value) for path, value in current.items()
        })

        self.assertEqual(_decode(result), [
            {'a': -1.0, 'b': -2.0, 'nested': {'c': -3.0, 'd': -4.0}},
            {'a': 1.0, 'b': 2.0, 'nested': {'c': 3.0, 'd': 4.0}},
        ])

    def test_float_and_double_are_distinct(self):
        floats = _float_variants([1.0, 2.0])
        result = variant_replace(
            floats, '$', pa.array([-1.0, -2.0], type=pa.float32()))
        self.assertEqual(
            variant_get(result, '$', pa.float32()).to_pylist(),
            [-1.0, -2.0],
        )

        with self.assertRaisesRegex(TypeError, "does not match"):
            variant_replace(floats, '$', pa.scalar(1.0, type=pa.float64()))
        with self.assertRaisesRegex(TypeError, "does not match"):
            variant_replace(
                _variants([1.0]), '$', pa.scalar(1.0, type=pa.float32()))

    def test_nullable_rows_stay_vectorized(self):
        size = 4096
        column = _variants(
            [None] + [{'value': float(index)} for index in range(1, size)])

        with patch(
                'pypaimon.data.variant_path._path_positions',
                wraps=_path_positions,
        ) as slow_path:
            current = variant_get(column, '$.value', pa.float64())
            result = variant_replace(column, '$.value', pa.scalar(-1.0))

        self.assertIsNone(current[0].as_py())
        self.assertEqual(current[-1].as_py(), float(size - 1))
        self.assertIsNone(result[0].as_py())
        self.assertEqual(_decode(result.slice(size - 1, 1)),
                         [{'value': -1.0}])
        slow_path.assert_not_called()

    def test_sparse_layout_fallback_is_bounded(self):
        size = 4096
        column = pa.concat_arrays([
            _variants([{'extra': 1, 'value': 0.0}]),
            _variants([{'value': float(index)} for index in range(1, size)]),
        ])

        with patch(
                'pypaimon.data.variant_path._path_positions',
                wraps=_path_positions,
        ) as slow_path:
            current = variant_get(column, '$.value', pa.float64())
            result = variant_replace(column, '$.value', pa.scalar(-1.0))

        self.assertLessEqual(slow_path.call_count, 128)
        self.assertEqual(current[0].as_py(), 0.0)
        self.assertEqual(_decode(result.slice(0, 1)),
                         [{'extra': 1, 'value': -1.0}])

    def test_missing_path_is_noop_or_strict_error(self):
        column = _variants([
            {'other': float(index)} for index in range(4096)
        ])

        with patch(
                'pypaimon.data.variant_path._path_positions',
                wraps=_path_positions,
        ) as slow_path:
            current = variant_get(column, '$.missing', pa.float64())
            result = variant_replace(
                column, '$.missing', pa.scalar(3.0, type=pa.float64()))

        self.assertEqual(current.null_count, len(column))
        self.assertIs(result, column)
        slow_path.assert_not_called()
        with self.assertRaisesRegex(ValueError, "path does not exist"):
            variant_replace(
                column, '$.missing', pa.scalar(3.0), strict=True)

    def test_null_replacement_rebuilds_only_affected_row(self):
        column = _variants([
            {'value': 1.0, 'padding': 'x' * 1000},
            {'value': 2.0, 'padding': 'y' * 1000},
        ])

        result = variant_replace(
            column,
            '$.value',
            pa.array([None, -2.0], type=pa.float64()),
        )

        self.assertEqual(_decode(result), [
            {'value': None, 'padding': 'x' * 1000},
            {'value': -2.0, 'padding': 'y' * 1000},
        ])
        self.assertEqual(
            column.field('metadata').buffers()[2].address,
            result.field('metadata').buffers()[2].address,
        )

    def test_copy_on_write_and_sliced_input(self):
        base = _variants([
            {'value': float(index), 'padding': 'x' * 1000}
            for index in range(100)
        ])
        column = base.slice(50, 3)

        result = variant_replace(column, '$.value', pa.scalar(-1.0))

        self.assertEqual(
            [row['value'] for row in _decode(result)], [-1.0, -1.0, -1.0])
        self.assertEqual(
            result.field('value').buffers()[2].size,
            sum(len(value) for value in column.field('value').to_pylist()),
        )
        self.assertEqual(
            column.field('metadata').buffers()[2].address,
            result.field('metadata').buffers()[2].address,
        )

    def test_rejects_truncated_child_without_touching_sibling(self):
        valid = GenericVariant.from_python({'a': 1.0, 'b': 2.0})
        value = _build_object_value([
            (0, bytes([_primitive_header(_DOUBLE)])),
            (1, _encode_scalar_to_value_bytes(2.0, pa.float64())),
        ])
        column = GenericVariant.to_arrow_array([
            GenericVariant(value, valid.metadata())])
        original = column.to_pylist()

        with self.assertRaisesRegex(ValueError, "MALFORMED_VARIANT"):
            variant_replace(column, '$.a', pa.scalar(3.0))
        self.assertEqual(column.to_pylist(), original)

    def test_rebuilt_binary_offsets_reject_overflow(self):
        self.assertEqual(
            _rebuilt_offsets(np.array([2, 3]), '<i').tolist(),
            [0, 2, 5],
        )
        with self.assertRaisesRegex(ValueError, "use LargeBinary"):
            _rebuilt_offsets(np.array([(1 << 31) - 1, 1]), '<i')

    def test_rejects_invalid_arguments(self):
        column = _variants([{'value': 1.0}, {'value': 2.0}])
        cases = [
            ('value', pa.scalar(1.0), False,
             ValueError, "Invalid VARIANT path"),
            ('$.value', pa.array([1.0]), False,
             ValueError, "length must match"),
            ('$.value', 1.0, False,
             TypeError, "Arrow Scalar or Array"),
            ('$.value', pa.array([1, 2]), False,
             TypeError, "float32 or float64"),
            ('$.value', pa.scalar(1.0), 'yes',
             TypeError, "strict must be a boolean"),
        ]
        for path, replacement, strict, error_type, message in cases:
            with self.subTest(path=path, replacement=replacement):
                with self.assertRaisesRegex(error_type, message):
                    variant_replace(
                        column, path, replacement, strict=strict)

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
