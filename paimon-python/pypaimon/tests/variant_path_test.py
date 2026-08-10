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
import operator
from unittest.mock import patch

import pyarrow as pa

from pypaimon.data.generic_variant import GenericVariant
from pypaimon.data.variant_path import (
    variant_get,
    variant_get_many,
    variant_set,
    variant_set_many,
    variant_transform,
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


class TestVariantGet(unittest.TestCase):

    def test_get_nested_scalars(self):
        column = _variants([
            {'imu': {'velocity': {'y': 1.5}}},
            {'imu': {'velocity': {'y': -2.0}}},
            {'imu': {}},
            None,
        ])

        result = variant_get(
            column, '$.imu.velocity.y', pa.float64())

        self.assertEqual(result.to_pylist(), [1.5, -2.0, None, None])

    def test_get_quoted_key_and_array_index(self):
        column = _variants([{'a.b': [{'value': 3}, {'value': 7}]}])

        result = variant_get(
            column, '$["a.b"][1].value', pa.int64())

        self.assertEqual(result.to_pylist(), [7])

    def test_get_variant_subvalue(self):
        column = _variants([{'nested': {'value': 3}}, None])

        result = variant_get(column, '$.nested')

        self.assertEqual(_decode(result), [{'value': 3}, None])

    def test_get_preserves_chunks(self):
        first = _variants([{'value': 1}])
        second = _variants([{'value': 2}, {'value': 3}])
        column = pa.chunked_array([first, second])

        result = variant_get(column, '$.value', pa.int64())

        self.assertIsInstance(result, pa.ChunkedArray)
        self.assertEqual(result.num_chunks, 2)
        self.assertEqual(result.to_pylist(), [1, 2, 3])

    def test_get_multiple_paths(self):
        column = _variants([{
            'angular': {'y': 1.0, 'z': 2.0},
            'linear': {'y': 3.0, 'z': 4.0},
        }])

        result = variant_get_many(column, {
            '$.angular.y': pa.float64(),
            '$.angular.z': pa.float64(),
            '$.linear.y': pa.float64(),
            '$.linear.z': pa.float64(),
        })

        self.assertEqual(result['$.angular.y'].to_pylist(), [1.0])
        self.assertEqual(result['$.linear.z'].to_pylist(), [4.0])


class TestVariantSet(unittest.TestCase):

    def test_set_multiple_double_paths(self):
        column = _variants([
            {
                'angular_velocity': {'x': 1.0, 'y': 2.0, 'z': 3.0},
                'linear_acceleration': {'x': 4.0, 'y': 5.0, 'z': 6.0},
                'unchanged': 'keep',
            },
            {
                'angular_velocity': {'x': 7.0, 'y': 8.0, 'z': 9.0},
                'linear_acceleration': {'x': 10.0, 'y': 11.0, 'z': 12.0},
                'unchanged': 'also keep',
            },
        ])
        metadata_before = column.field('metadata').to_pylist()

        result = variant_set_many(column, {
            '$.angular_velocity.y': pa.array([-2.0, -8.0]),
            '$.angular_velocity.z': pa.array([-3.0, -9.0]),
            '$.linear_acceleration.y': pa.array([-5.0, -11.0]),
            '$.linear_acceleration.z': pa.array([-6.0, -12.0]),
        })

        decoded = _decode(result)
        self.assertEqual(decoded[0]['angular_velocity'],
                         {'x': 1.0, 'y': -2.0, 'z': -3.0})
        self.assertEqual(decoded[1]['linear_acceleration'],
                         {'x': 10.0, 'y': -11.0, 'z': -12.0})
        self.assertEqual(decoded[0]['unchanged'], 'keep')
        self.assertEqual(
            result.field('metadata').to_pylist(), metadata_before)

    def test_transform_multiple_double_paths(self):
        column = _variants([{
            'angular': {'y': 1.0, 'z': -2.0},
            'linear': {'y': 3.0, 'z': -4.0},
            'other': 'keep',
        }])

        result = variant_transform(column, {
            '$.angular.y': operator.neg,
            '$.angular.z': operator.neg,
            '$.linear.y': operator.neg,
            '$.linear.z': operator.neg,
        })

        self.assertEqual(_decode(result), [{
            'angular': {'y': -1.0, 'z': 2.0},
            'linear': {'y': -3.0, 'z': 4.0},
            'other': 'keep',
        }])

    def test_set_finds_path_after_variable_length_value(self):
        column = _variants([
            {'prefix': 'x', 'nested': {'value': 1.0}},
            {'prefix': 'x' * 200, 'nested': {'value': 2.0}},
        ])

        result = variant_set(
            column, '$.nested.value', pa.array([-1.0, -2.0]))

        decoded = _decode(result)
        self.assertEqual(decoded[0]['nested']['value'], -1.0)
        self.assertEqual(decoded[1]['nested']['value'], -2.0)
        self.assertEqual(decoded[1]['prefix'], 'x' * 200)

    def test_set_rebuilds_offsets_for_different_size(self):
        column = _variants([
            {'before': 'a', 'target': 'x', 'after': {'value': 7}},
        ])

        result = variant_set(column, '$.target', 'a much longer value')

        self.assertEqual(_decode(result), [{
            'before': 'a',
            'target': 'a much longer value',
            'after': {'value': 7},
        }])

    def test_set_array_element(self):
        column = _variants([{'values': [1, 2, 3]}])

        result = variant_set(column, '$.values[1]', 100000)

        self.assertEqual(_decode(result), [{'values': [1, 100000, 3]}])

    def test_set_preserves_sql_null_and_chunks(self):
        first = _variants([{'value': 1}, None])
        second = _variants([{'value': 3}])
        column = pa.chunked_array([first, second])

        result = variant_set(
            column, '$.value', pa.array([10, 20, 30]))

        self.assertIsInstance(result, pa.ChunkedArray)
        self.assertEqual(result.num_chunks, 2)
        self.assertEqual(_decode(result), [{'value': 10}, None, {'value': 30}])

    def test_set_does_not_decode_whole_variant(self):
        column = _variants([{'nested': {'value': 1.0}, 'other': [1, 2, 3]}])

        with patch.object(
                GenericVariant,
                'to_python',
                side_effect=AssertionError("full decode is not allowed")):
            result = variant_set(column, '$.nested.value', -1.0)

        self.assertEqual(_decode(result), [
            {'nested': {'value': -1.0}, 'other': [1, 2, 3]},
        ])

    def test_set_rejects_missing_and_overlapping_paths(self):
        column = _variants([{'nested': {'value': 1}}])

        with self.assertRaisesRegex(ValueError, "path does not exist"):
            variant_set(column, '$.missing', 2)
        with self.assertRaisesRegex(ValueError, "must not overlap"):
            variant_set_many(column, {
                '$.nested': 2,
                '$.nested.value': 3,
            })

    def test_set_rejects_nested_replacement(self):
        column = _variants([{'nested': {'value': 1}}])

        with self.assertRaisesRegex(TypeError, "scalar replacements"):
            variant_set(column, '$.nested', {'value': 2})


if __name__ == '__main__':
    unittest.main()
