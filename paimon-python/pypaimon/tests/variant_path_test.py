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

import operator
import unittest
from unittest.mock import patch

import pyarrow as pa

from pypaimon.data.generic_variant import GenericVariant
from pypaimon.data.variant_path import variant_transform
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


class TestVariantTransform(unittest.TestCase):

    def test_transform_double_paths(self):
        column = _variants([
            {
                'a_prefix': 'x',
                'angular': {'y': 1.0, 'z': -2.0},
                'linear': {'y': 3.0, 'z': -4.0},
            },
            {
                'a_prefix': 'x' * 200,
                'angular': {'y': 5.0, 'z': -6.0},
                'linear': {'y': 7.0, 'z': -8.0},
            },
        ])
        metadata = column.field('metadata').to_pylist()

        with patch.object(
                GenericVariant, 'to_python',
                side_effect=AssertionError("full decode is not allowed")):
            result = variant_transform(column, {
                '$.angular.y': operator.neg,
                '$.angular.z': operator.neg,
                '$.linear.y': operator.neg,
                '$.linear.z': operator.neg,
            })

        self.assertEqual(_decode(result), [
            {
                'a_prefix': 'x',
                'angular': {'y': -1.0, 'z': 2.0},
                'linear': {'y': -3.0, 'z': 4.0},
            },
            {
                'a_prefix': 'x' * 200,
                'angular': {'y': -5.0, 'z': 6.0},
                'linear': {'y': -7.0, 'z': 8.0},
            },
        ])
        self.assertEqual(result.field('metadata').to_pylist(), metadata)
        self.assertEqual(
            result.field('metadata').buffers()[2].address,
            column.field('metadata').buffers()[2].address,
        )

    def test_array_path_null_and_chunks(self):
        first = _variants([{'a.b': [{'value': 1.0}]}, None])
        second = _variants([{'a.b': [{'value': 2.0}]}])
        column = pa.chunked_array([first, second])

        result = variant_transform(
            column, {'$["a.b"][0].value': operator.neg})

        self.assertIsInstance(result, pa.ChunkedArray)
        self.assertEqual(result.num_chunks, 2)
        self.assertEqual(_decode(result), [
            {'a.b': [{'value': -1.0}]},
            None,
            {'a.b': [{'value': -2.0}]},
        ])

    def test_transform_float_path(self):
        column = _float_variants([1.25, -2.5])

        result = variant_transform(column, {'$': operator.neg})

        self.assertEqual(_decode(result), [-1.25, 2.5])

    def test_transform_preserves_mixed_float_and_double_types(self):
        float_value = GenericVariant.from_arrow_struct(
            _float_variants([1.25])[0].as_py())
        double_value = GenericVariant.from_python(2.5)
        column = GenericVariant.to_arrow_array([float_value, double_value])

        result = variant_transform(column, {'$': operator.neg})

        self.assertEqual(_decode(result), [-1.25, -2.5])
        self.assertEqual(
            [len(value) for value in result.field('value').to_pylist()],
            [len(float_value.value()), len(double_value.value())],
        )

    def test_float_transform_rejects_invalid_result(self):
        column = _float_variants([1.0])

        with self.assertRaisesRegex(TypeError, "must return FLOAT"):
            variant_transform(column, {'$': lambda value: 1})
        with self.assertRaisesRegex(TypeError, "must return FLOAT"):
            variant_transform(column, {'$': lambda value: 1e100})

    def test_rejects_invalid_transform(self):
        column = _variants([{'number': 1.0, 'text': 'value'}])
        cases = [
            ({'number': operator.neg}, ValueError, "Invalid VARIANT path"),
            ({'$.number': operator.neg, "$['number']": operator.neg},
             ValueError, "paths must be unique"),
            ({'$.missing': operator.neg}, ValueError, "path does not exist"),
            ({'$.text': operator.neg}, TypeError,
             "path is not FLOAT or DOUBLE"),
            ({'$.number': 'negate'}, TypeError, "must be callable"),
            ({'$.number': lambda value: 1}, TypeError,
             "must return DOUBLE"),
        ]
        for transforms, error_type, message in cases:
            with self.subTest(transforms=transforms):
                with self.assertRaisesRegex(error_type, message):
                    variant_transform(column, transforms)


if __name__ == '__main__':
    unittest.main()
