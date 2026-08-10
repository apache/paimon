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

    def test_rejects_invalid_transform(self):
        column = _variants([{'number': 1.0, 'text': 'value'}])
        cases = [
            ({'number': operator.neg}, ValueError, "Invalid VARIANT path"),
            ({'$.missing': operator.neg}, ValueError, "path does not exist"),
            ({'$.text': operator.neg}, TypeError, "path is not DOUBLE"),
            ({'$.number': 'negate'}, TypeError, "must be callable"),
            ({'$.number': lambda value: 'wrong'}, TypeError,
             "must return DOUBLE"),
        ]
        for transforms, error_type, message in cases:
            with self.subTest(transforms=transforms):
                with self.assertRaisesRegex(error_type, message):
                    variant_transform(column, transforms)


if __name__ == '__main__':
    unittest.main()
