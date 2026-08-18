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

from pypaimon.data import variant_replace, variant_set
from pypaimon.data.generic_variant import GenericVariant, _check_variant_sizes
from pypaimon.data.variant_path import (
    _apply_edits,
    _build_object_value_ordered,
    _checked_object_layout,
    _materialize_value,
    _metadata_key_ids,
    _metadata_with_keys,
    _path_positions,
    _rebuilt_offsets,
    _validate_value_field_ids,
    variant_get,
)
from pypaimon.data.variant_shredding import (
    _build_array_value,
    _build_object_value,
    _encode_scalar_to_value_bytes,
)

# Bytes built by the Java GenericVariantBuilder for
# {"angular_velocity":{"y":1.5,"z":-2.5},
#  "linear_acceleration":{"y":0.25,"z":4.0},"processed":true,"seq":7}.
_JAVA_VALUE = bytes.fromhex(
    '0204000304050019323335020201020009121c000000000000f83f1c000000000000'
    '04c0020201020009121c000000000000d03f1c0000000000001040040c07')
_JAVA_METADATA = bytes.fromhex(
    '010600101112252e31616e67756c61725f76656c6f63697479797a6c696e6561725f'
    '616363656c65726174696f6e70726f636573736564736571')
_JAVA_PYTHON_VALUE = {
    'angular_velocity': {'y': 1.5, 'z': -2.5},
    'linear_acceleration': {'y': 0.25, 'z': 4.0},
    'processed': True,
    'seq': 7,
}


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


def _sensor_rows(count, offset=0):
    return [
        {
            'angular_velocity': {
                'y': float(index + offset),
                'z': float(index + offset) + 0.5,
            },
            'linear_acceleration': {
                'y': -float(index + offset),
                'z': -float(index + offset) - 0.5,
            },
        }
        for index in range(count)
    ]


_SENSOR_PATHS = (
    '$.angular_velocity.y',
    '$.angular_velocity.z',
    '$.linear_acceleration.y',
    '$.linear_acceleration.z',
)


class TestVariantSetReplace(unittest.TestCase):

    def test_existing_paths_match_variant_replace(self):
        column = _variants(_sensor_rows(100) + [None])
        current = variant_get(
            column, {path: pa.float64() for path in _SENSOR_PATHS})
        updates = {
            path: pc.negate(values) for path, values in current.items()
        }

        self.assertTrue(
            variant_set(column, updates).equals(
                variant_replace(column, updates)))

    def test_negates_four_double_paths(self):
        rows = _sensor_rows(50)
        column = _variants(rows)
        current = variant_get(
            column, {path: pa.float64() for path in _SENSOR_PATHS})

        result = variant_set(column, {
            path: pc.negate(values) for path, values in current.items()
        })

        for row, decoded in zip(rows, _decode(result)):
            self.assertEqual(decoded, {
                'angular_velocity': {
                    'y': -row['angular_velocity']['y'],
                    'z': -row['angular_velocity']['z'],
                },
                'linear_acceleration': {
                    'y': -row['linear_acceleration']['y'],
                    'z': -row['linear_acceleration']['z'],
                },
            })

    def test_replaces_root_path(self):
        column = _variants([1.5, -2.5])

        result = variant_set(column, '$', pa.scalar(3.5, type=pa.float64()))

        self.assertEqual(
            variant_get(result, '$', pa.float64()).to_pylist(), [3.5, 3.5])


class TestVariantSetInsert(unittest.TestCase):

    def test_inserts_bool_and_string_marks(self):
        column = _variants([{'value': 1.0}, {'value': 2.0}])

        flagged = variant_set(column, '$.processed', pa.scalar(True))
        tagged = variant_set(column, '$.tag', pa.scalar('done'))

        self.assertEqual(_decode(flagged), [
            {'value': 1.0, 'processed': True},
            {'value': 2.0, 'processed': True},
        ])
        self.assertEqual(_decode(tagged), [
            {'value': 1.0, 'tag': 'done'},
            {'value': 2.0, 'tag': 'done'},
        ])

    def test_insert_extends_metadata_dictionary(self):
        column = _variants([{'value': 1.0}])

        result = variant_set(column, '$.processed', pa.scalar(True))

        metadata = result.to_pylist()[0]['metadata']
        self.assertEqual(
            _metadata_key_ids(metadata), {'value': 0, 'processed': 1})

    def test_insert_reuses_metadata_key_and_buffer(self):
        metadata = GenericVariant.from_python(
            {'value': 0, 'flag': 0}).metadata()
        key_ids = _metadata_key_ids(metadata)
        value = _build_object_value([
            (key_ids['value'],
             _encode_scalar_to_value_bytes(1.5, pa.float64())),
        ])
        column = GenericVariant.to_arrow_array(
            [GenericVariant(value, metadata)] * 100)

        result = variant_set(column, '$.flag', pa.scalar(False))

        self.assertEqual(
            _decode(result), [{'value': 1.5, 'flag': False}] * 100)
        self.assertEqual(
            result.to_pylist()[0]['metadata'], metadata)
        self.assertEqual(
            column.field('metadata').buffers()[2].address,
            result.field('metadata').buffers()[2].address,
        )

    def test_insert_into_empty_object(self):
        column = _variants([{}])

        result = variant_set(column, '$.first', pa.scalar(7, pa.int64()))

        self.assertEqual(_decode(result), [{'first': 7}])

    def test_inserts_same_key_into_two_objects(self):
        column = _variants([{'left': {}, 'right': {}}])

        result = variant_set(column, {
            '$.left.flag': pa.scalar(True),
            '$.right.flag': pa.scalar(True),
        })

        metadata = result.to_pylist()[0]['metadata']
        self.assertEqual(
            _metadata_key_ids(metadata), {'left': 0, 'right': 1, 'flag': 2})
        self.assertEqual(_decode(result), [
            {'left': {'flag': True}, 'right': {'flag': True}}])
        self.assertEqual(
            variant_get(result, '$.left.flag', pa.bool_()).to_pylist(),
            [True])

    def test_inserted_fields_stay_sorted_for_java_binary_search(self):
        payload = {'k%02d' % index: float(index) for index in range(40)}
        column = _variants([payload])

        result = variant_set(column, '$.a_mark', pa.scalar('inserted'))

        decoded = _decode(result)[0]
        expected = dict(payload)
        expected['a_mark'] = 'inserted'
        self.assertEqual(decoded, expected)
        self.assertEqual(list(decoded), sorted(decoded))

    def test_inserted_fields_stay_sorted_by_utf8(self):
        key_sup = chr(0x10000)
        key_bmp = chr(0xE000)
        payload = {key_bmp: 1.0, key_sup: 2.0}
        payload.update({'k%02d' % i: float(i) for i in range(40)})

        result = variant_set(_variants([payload]), '$.aaa', pa.scalar(3.0))

        decoded = _decode(result)[0]
        self.assertEqual(decoded[key_sup], 2.0)
        self.assertEqual(decoded[key_bmp], 1.0)
        self.assertEqual(list(decoded), sorted(
            list(payload.keys()) + ['aaa'],
            key=lambda name: name.encode('utf-8')))

    def test_mixed_rows_in_one_chunk(self):
        metadata = GenericVariant.from_python({'a': 0, 'b': 0}).metadata()
        key_ids = _metadata_key_ids(metadata)
        reversed_fields = _build_object_value_ordered([
            (key_ids['b'], _encode_scalar_to_value_bytes(2.0, pa.float64())),
            (key_ids['a'], _encode_scalar_to_value_bytes(1.0, pa.float64())),
        ])
        column = pa.concat_arrays([
            _variants([{'a': 1.0}, {'a': 1.0, 'mark': 'old'}]),
            GenericVariant.to_arrow_array(
                [GenericVariant(reversed_fields, metadata)]),
        ])

        result = variant_set(column, '$.mark', pa.scalar('new'))

        self.assertEqual(_decode(result), [
            {'a': 1.0, 'mark': 'new'},
            {'a': 1.0, 'mark': 'new'},
            {'b': 2.0, 'a': 1.0, 'mark': 'new'},
        ])

    def test_replace_and_insert_multiple_paths(self):
        column = _variants(_sensor_rows(10))
        current = variant_get(
            column, {path: pa.float64() for path in _SENSOR_PATHS})
        updates = {
            path: pc.negate(values) for path, values in current.items()
        }
        updates['$.processed'] = pa.scalar(True, type=pa.bool_())

        result = variant_set(column, updates)

        decoded = _decode(result)
        self.assertTrue(all(row['processed'] is True for row in decoded))
        self.assertEqual(
            [row['angular_velocity']['y'] for row in decoded],
            [-float(index) for index in range(10)],
        )

    def test_scalar_array_and_chunked_replacements(self):
        column = pa.chunked_array([
            _variants([{'value': 1.0}, {'value': 2.0}]),
            _variants([{'value': 3.0}]),
        ])

        result = variant_set(column, {
            '$.value': pa.chunked_array(
                [[10.0, 20.0], [30.0]], type=pa.float64()),
            '$.rank': pa.array([1, 2, 3], type=pa.int64()),
            '$.processed': pa.scalar(True),
        })

        self.assertIsInstance(result, pa.ChunkedArray)
        self.assertEqual(result.num_chunks, 2)
        self.assertEqual(_decode(result), [
            {'value': 10.0, 'rank': 1, 'processed': True},
            {'value': 20.0, 'rank': 2, 'processed': True},
            {'value': 30.0, 'rank': 3, 'processed': True},
        ])


class TestVariantSetNullSemantics(unittest.TestCase):

    def test_sql_null_rows_stay_null(self):
        column = _variants([None, {'value': 1.0}])

        result = variant_set(column, '$.processed', pa.scalar(True))

        self.assertEqual(_decode(result), [
            None, {'value': 1.0, 'processed': True},
        ])
        self.assertTrue(result.is_null()[0].as_py())

    def test_arrow_null_becomes_variant_null(self):
        column = _variants([{'value': 1.0}, {'value': 2.0}])

        result = variant_set(column, {
            '$.value': pa.array([None, -2.0], type=pa.float64()),
            '$.mark': pa.array([None, 'done'], type=pa.string()),
        })

        self.assertEqual(_decode(result), [
            {'value': None, 'mark': None},
            {'value': -2.0, 'mark': 'done'},
        ])
        self.assertEqual(result.null_count, 0)

    def test_untyped_arrow_null_becomes_variant_null(self):
        replacements = [
            pa.scalar(None),
            pa.nulls(2),
            pa.chunked_array([pa.nulls(1), pa.nulls(1)]),
        ]
        for replacement in replacements:
            with self.subTest(replacement=type(replacement).__name__):
                result = variant_set(
                    _variants([{'value': 1.0}, {'value': 2.0}]),
                    '$.value',
                    replacement,
                )
                self.assertEqual(_decode(result), [
                    {'value': None}, {'value': None},
                ])

    def test_variant_null_parent_is_not_an_object(self):
        column = _variants([{'parent': None}])

        with self.assertRaisesRegex(ValueError, "is not an object"):
            variant_set(column, '$.parent.child', pa.scalar(1.0))

    def test_missing_intermediate_parent_fails(self):
        column = _variants([{'other': 1.0}] * 100)

        with self.assertRaisesRegex(ValueError, "parent path does not"):
            variant_set(column, '$.missing.child', pa.scalar(1.0))

    def test_non_object_parent_fails(self):
        column = _variants([{'value': 1.0}] * 100)

        with self.assertRaisesRegex(ValueError, "is not an object"):
            variant_set(column, '$.value.child', pa.scalar(1.0))

    def test_replaces_array_element_of_a_different_size(self):
        column = _variants([{'items': ['aa', 'bb'], 'n': 1.0}])

        result = variant_set(column, '$.items[0]', pa.scalar('cccc'))

        self.assertEqual(
            _decode(result), [{'items': ['cccc', 'bb'], 'n': 1.0}])

    def test_array_insertion_is_not_supported(self):
        column = _variants([{'items': [1.0]}])

        with self.assertRaisesRegex(ValueError, "not supported"):
            variant_set(column, '$.items[3]', pa.scalar(1.0))
        result = variant_set(column, '$.items[0]', pa.scalar(-1.0))
        self.assertEqual(_decode(result), [{'items': [-1.0]}])


class TestVariantSetLayouts(unittest.TestCase):

    def test_sliced_input(self):
        base = _variants([
            {'value': float(index), 'padding': 'x' * 100}
            for index in range(100)
        ])
        column = base.slice(50, 3)

        result = variant_set(column, {
            '$.value': pa.scalar(-1.0),
            '$.processed': pa.scalar(True),
        })

        self.assertEqual(
            [(row['value'], row['processed']) for row in _decode(result)],
            [(-1.0, True)] * 3,
        )

    def test_large_binary_input(self):
        column = _variants([{'value': 1.0}])
        large = pa.StructArray.from_arrays(
            [
                column.field('value').cast(pa.large_binary()),
                column.field('metadata').cast(pa.large_binary()),
            ],
            names=['value', 'metadata'],
        )

        result = variant_set(large, '$.processed', pa.scalar(True))

        self.assertTrue(pa.types.is_large_binary(result.type[0].type))
        self.assertTrue(pa.types.is_large_binary(result.type[1].type))
        self.assertEqual(
            _decode(result), [{'value': 1.0, 'processed': True}])

    def test_preserves_chunk_boundaries_without_combine(self):
        column = pa.chunked_array([
            _variants([{'value': 1.0}]),
            _variants([{'value': 2.0}, {'value': 3.0}]),
        ])

        with patch(
                'pypaimon.data.variant_path._rebuilt_offsets',
                wraps=_rebuilt_offsets,
        ) as rebuilt_offsets:
            result = variant_set(column, '$.processed', pa.scalar(True))

        self.assertEqual(
            [len(chunk) for chunk in result.chunks],
            [len(chunk) for chunk in column.chunks],
        )
        # Offsets are rebuilt per chunk, never for the combined column.
        self.assertTrue(rebuilt_offsets.called)
        self.assertEqual(
            max(len(call[0][0])
                for call in rebuilt_offsets.call_args_list),
            2,
        )
        self.assertEqual(_decode(result), [
            {'value': 1.0, 'processed': True},
            {'value': 2.0, 'processed': True},
            {'value': 3.0, 'processed': True},
        ])

    def test_offset_overflow_guard_is_low_memory(self):
        lengths = np.array([(1 << 31) - 8, 16], dtype=np.int64)

        with self.assertRaisesRegex(ValueError, "use LargeBinary"):
            _rebuilt_offsets(lengths, '<i')
        self.assertEqual(
            _rebuilt_offsets(lengths, '<q')[-1], (1 << 31) + 8)

    def test_input_is_not_modified(self):
        column = _variants([{'value': 1.0}, None, {'value': 2.0}])
        original_rows = column.to_pylist()
        original_value = column.field('value').buffers()[2].to_pybytes()
        original_metadata = (
            column.field('metadata').buffers()[2].to_pybytes())

        variant_set(column, {
            '$.value': pa.scalar(-1.0),
            '$.processed': pa.scalar(True),
        })

        self.assertEqual(column.to_pylist(), original_rows)
        self.assertEqual(
            column.field('value').buffers()[2].to_pybytes(),
            original_value,
        )
        self.assertEqual(
            column.field('metadata').buffers()[2].to_pybytes(),
            original_metadata,
        )


class TestVariantSetFastPaths(unittest.TestCase):

    def test_replace_avoids_full_decode(self):
        column = _variants([{'value': float(index)} for index in range(100)])

        with patch.object(
                GenericVariant, 'to_python',
                side_effect=AssertionError("full decode is not allowed")), \
                patch.object(
                    GenericVariant, 'from_python',
                    side_effect=AssertionError(
                        "full encode is not allowed")):
            result = variant_set(column, '$.value', pa.scalar(-1.0))

        self.assertEqual(
            variant_get(result, '$.value', pa.float64()).to_pylist(),
            [-1.0] * 100,
        )

    def test_insert_avoids_full_decode(self):
        column = _variants([{'value': float(index)} for index in range(100)])

        with patch.object(
                GenericVariant, 'to_python',
                side_effect=AssertionError("full decode is not allowed")), \
                patch.object(
                    GenericVariant, 'from_python',
                    side_effect=AssertionError(
                        "full encode is not allowed")):
            result = variant_set(column, '$.processed', pa.scalar(True))

        self.assertEqual(
            variant_get(result, '$.processed', pa.bool_()).to_pylist(),
            [True] * 100,
        )

    def test_replace_fast_path_stays_vectorized(self):
        column = _variants(
            [{'value': float(index)} for index in range(4096)])

        with patch(
                'pypaimon.data.variant_path._path_positions',
                wraps=_path_positions,
        ) as slow_path, patch(
                'pypaimon.data.variant_path._apply_edits',
                wraps=_apply_edits,
        ) as rebuild:
            result = variant_set(column, '$.value', pa.scalar(-1.0))

        slow_path.assert_not_called()
        rebuild.assert_not_called()
        self.assertEqual(
            variant_get(result, '$.value', pa.float64()).to_pylist(),
            [-1.0] * 4096,
        )

    def test_insert_avoids_per_row_planning(self):
        column = _variants(
            [{'value': float(index)} for index in range(4096)])

        with patch(
                'pypaimon.data.variant_path._path_positions',
                wraps=_path_positions,
        ) as slow_path, patch(
                'pypaimon.data.variant_path._metadata_key_ids',
                wraps=_metadata_key_ids,
        ) as metadata_parse, patch(
                'pypaimon.data.variant_path._apply_edits',
                wraps=_apply_edits,
        ) as rebuild:
            result = variant_set(column, '$.processed', pa.scalar(True))

        slow_path.assert_not_called()
        rebuild.assert_not_called()
        self.assertLessEqual(metadata_parse.call_count, 2)
        self.assertEqual(
            variant_get(result, '$.processed', pa.bool_()).to_pylist(),
            [True] * 4096,
        )

    def test_insert_splices_nested_root_after_validation(self):
        column = _variants([
            {
                'nested_object': {'value': float(index)},
                'nested_array': [{'value': float(index)}],
                'other': float(index),
            }
            for index in range(100)
        ])

        with patch(
                'pypaimon.data.variant_path._validate_value_field_ids',
                wraps=_validate_value_field_ids,
        ) as subtree_validation, patch(
                'pypaimon.data.variant_path._apply_edits',
                wraps=_apply_edits,
        ) as rebuild:
            result = variant_set(column, '$.processed', pa.scalar(True))

        self.assertTrue(any(
            args[1] == 0
            for args, _ in subtree_validation.call_args_list
        ))
        self.assertEqual(subtree_validation.call_count, 1)
        rebuild.assert_not_called()
        self.assertEqual(
            variant_get(result, '$.processed', pa.bool_()).to_pylist(),
            [True] * 100,
        )

    def test_insert_batches_multiple_nested_structures(self):
        sequences = [0, 128, 32768]
        column = _variants([
            {
                'nested': {'value': float(index)},
                'sequence': sequences[index % len(sequences)],
            }
            for index in range(300)
        ])

        with patch(
                'pypaimon.data.variant_path._validate_value_field_ids',
                wraps=_validate_value_field_ids,
        ) as subtree_validation, patch(
                'pypaimon.data.variant_path._apply_edits',
                wraps=_apply_edits,
        ) as rebuild:
            result = variant_set(column, '$.processed', pa.scalar(True))

        self.assertEqual(subtree_validation.call_count, len(sequences))
        rebuild.assert_not_called()
        self.assertEqual(
            variant_get(result, '$.processed', pa.bool_()).to_pylist(),
            [True] * len(column),
        )

    def test_insert_batches_all_nested_structure_lengths(self):
        lengths = list(range(1, 13))
        column = _variants([
            {'nested': {'value': 'x' * length}}
            for length in lengths
            for _ in range(10)
        ])

        with patch(
                'pypaimon.data.variant_path._validate_value_field_ids',
                wraps=_validate_value_field_ids,
        ) as subtree_validation, patch(
                'pypaimon.data.variant_path._apply_edits',
                wraps=_apply_edits,
        ) as rebuild:
            result = variant_set(column, '$.processed', pa.scalar(True))

        self.assertEqual(subtree_validation.call_count, len(lengths))
        rebuild.assert_not_called()
        self.assertEqual(
            variant_get(result, '$.processed', pa.bool_()).to_pylist(),
            [True] * len(column),
        )

    def test_insert_validates_singleton_lengths_without_batching(self):
        lengths = list(range(1, 13))
        column = _variants([
            {'nested': {'value': 'x' * length}}
            for length in lengths
        ])

        with patch(
                'pypaimon.data.variant_path._matching_value_structures',
        ) as structure_match, patch(
                'pypaimon.data.variant_path._validate_value_field_ids',
                wraps=_validate_value_field_ids,
        ) as subtree_validation:
            result = variant_set(column, '$.processed', pa.scalar(True))

        structure_match.assert_not_called()
        self.assertEqual(subtree_validation.call_count, len(lengths))
        self.assertEqual(
            variant_get(result, '$.processed', pa.bool_()).to_pylist(),
            [True] * len(column),
        )

    def test_insert_uses_layout_after_noncanonical_first_row(self):
        metadata = GenericVariant.from_python({'a': 0, 'b': 0}).metadata()
        key_ids = _metadata_key_ids(metadata)
        a_value = _encode_scalar_to_value_bytes(1.0, pa.float64())
        b_value = _encode_scalar_to_value_bytes(2.0, pa.float64())
        noncanonical = _build_object_value_ordered([
            (key_ids['b'], b_value),
            (key_ids['a'], a_value),
        ])
        canonical = _build_object_value_ordered([
            (key_ids['a'], a_value),
            (key_ids['b'], b_value),
        ])
        column = GenericVariant.to_arrow_array([
            GenericVariant(noncanonical, metadata),
            *[GenericVariant(canonical, metadata) for _ in range(99)],
        ])

        with patch(
                'pypaimon.data.variant_path._apply_edits',
                wraps=_apply_edits,
        ) as rebuild:
            result = variant_set(column, '$.processed', pa.scalar(True))

        self.assertEqual(rebuild.call_count, 1)
        self.assertEqual(
            variant_get(result, '$.processed', pa.bool_()).to_pylist(),
            [True] * len(column),
        )

    def test_insert_validates_deep_unmodified_sibling_iteratively(self):
        metadata = GenericVariant.from_python(
            {'sibling': [], 'target': {}}).metadata()
        key_ids = _metadata_key_ids(metadata)
        sibling = _encode_scalar_to_value_bytes(1.0, pa.float64())
        for _ in range(1020):
            sibling = _build_array_value([sibling])
        root = _build_object_value([
            (key_ids['sibling'], sibling),
            (key_ids['target'], _build_object_value([])),
        ])
        column = GenericVariant.to_arrow_array([
            GenericVariant(root, metadata),
        ])

        result = variant_set(column, '$.target.new', pa.scalar(True))

        self.assertEqual(
            variant_get(result, '$.target.new', pa.bool_()).to_pylist(),
            [True],
        )

    def test_insert_rebuilds_deep_modified_path_iteratively(self):
        metadata = GenericVariant.from_python({'target': {}}).metadata()
        key_ids = _metadata_key_ids(metadata)
        target = _build_object_value([])
        for _ in range(1020):
            target = _build_array_value([target])
        root = _build_object_value([
            (key_ids['target'], target),
        ])
        column = GenericVariant.to_arrow_array([
            GenericVariant(root, metadata),
        ])
        path = '$.target' + '[0]' * 1020 + '.new'

        with patch(
                'pypaimon.data.variant_path._materialize_value',
                wraps=_materialize_value,
        ) as materialize:
            result = variant_set(column, path, pa.scalar(True))

        self.assertEqual(materialize.call_count, 1)
        self.assertEqual(
            variant_get(result, path, pa.bool_()).to_pylist(),
            [True],
        )

    def test_insert_offset_width_boundary_mixed_rows(self):
        # Rows crossing the 1-byte offset limit after the insert must be
        # rebuilt with a wider offset table inside the same plan group.
        rows = []
        for index in range(100):
            padding = 'x' * (240 if index % 3 == 0 else 10)
            rows.append({'value': float(index), 'padding': padding})
        column = _variants(rows)
        mark = 'm' * 30

        result = variant_set(column, '$.mark', pa.scalar(mark))

        for index, decoded in enumerate(_decode(result)):
            self.assertEqual(decoded, {
                'value': float(index),
                'padding': rows[index]['padding'],
                'mark': mark,
            })


class TestVariantSetErrors(unittest.TestCase):

    def test_variant_size_limit_boundary(self):
        with patch('pypaimon.data.generic_variant._SIZE_LIMIT', 64):
            _check_variant_sizes(64, 64)
            with self.assertRaisesRegex(
                    ValueError, 'VARIANT_CONSTRUCTOR_SIZE_LIMIT'):
                _check_variant_sizes(65, 64)
            with self.assertRaisesRegex(
                    ValueError, 'VARIANT_CONSTRUCTOR_SIZE_LIMIT'):
                _check_variant_sizes(64, 65)

    def test_rejects_oversized_value_and_metadata(self):
        column = _variants([{'value': 'a'}])
        with patch('pypaimon.data.generic_variant._SIZE_LIMIT', 64):
            with self.assertRaisesRegex(
                    ValueError, 'VARIANT_CONSTRUCTOR_SIZE_LIMIT'):
                variant_set(column, '$.value', pa.scalar('x' * 128))
            with self.assertRaisesRegex(
                    ValueError, 'VARIANT_CONSTRUCTOR_SIZE_LIMIT'):
                variant_set(column, '$.' + 'k' * 128, pa.scalar(True))

    def test_rejects_type_and_length_mismatches(self):
        column = _variants([{'value': 1.0}, {'value': 2.0}])
        cases = [
            ('$.value', pa.scalar('text'), TypeError, "does not match"),
            ('$.value', pa.scalar(1.0, type=pa.float32()),
             TypeError, "does not match"),
            ('$.value', pa.array([1.0]), ValueError, "length must match"),
            ('$.value', 1.0, TypeError, "Arrow Scalar or Array"),
            ('value', pa.scalar(1.0), ValueError, "Invalid VARIANT path"),
        ]
        for path, replacement, error_type, message in cases:
            with self.subTest(path=path):
                with self.assertRaisesRegex(error_type, message):
                    variant_set(column, path, replacement)

        with self.assertRaisesRegex(TypeError, "must be omitted"):
            variant_set(
                column, {'$.value': pa.scalar(1.0)}, pa.scalar(2.0))

    def test_rejects_duplicate_and_overlapping_paths(self):
        column = _variants([{'x': {'y': 1.0}}])

        with self.assertRaisesRegex(ValueError, "must not overlap"):
            variant_set(column, {
                '$.x': pa.scalar(1.0),
                "$['x']": pa.scalar(2.0),
            })
        with self.assertRaisesRegex(ValueError, "must not overlap"):
            variant_set(column, {
                '$.x': pa.scalar(1.0),
                '$.x.y': pa.scalar(2.0),
            })

    def test_rejects_malformed_metadata(self):
        valid = GenericVariant.from_python({'value': 1.0})
        column = pa.StructArray.from_arrays(
            [
                pa.array([valid.value()]),
                pa.array([valid.metadata()[:-2]]),
            ],
            names=['value', 'metadata'],
        )

        with self.assertRaisesRegex(ValueError, "MALFORMED_VARIANT"):
            variant_set(column, '$.processed', pa.scalar(True))

    def test_rejects_unknown_field_id_on_insert(self):
        metadata = GenericVariant.from_python({'value': 0}).metadata()
        orphan = _build_object_value([
            (7, _encode_scalar_to_value_bytes(1.0, pa.float64())),
        ])
        column = GenericVariant.to_arrow_array(
            [GenericVariant(orphan, metadata)])

        with self.assertRaisesRegex(ValueError, "MALFORMED_VARIANT"):
            variant_set(column, '$.processed', pa.scalar(True))

    def test_rejects_field_id_colliding_with_inserted_key(self):
        # 'processed' will be assigned id 1; a corrupt source already using
        # id 1 must be rejected rather than silently producing a duplicate.
        metadata = GenericVariant.from_python({'value': 0}).metadata()
        corrupt = _build_object_value([
            (1, _encode_scalar_to_value_bytes(2.0, pa.float64())),
        ])
        column = GenericVariant.to_arrow_array(
            [GenericVariant(corrupt, metadata)])

        with self.assertRaisesRegex(ValueError, "MALFORMED_VARIANT"):
            variant_set(column, '$.processed', pa.scalar(True))

    def test_rejects_nested_insert_exposing_invalid_sibling_field_id(self):
        metadata = GenericVariant.from_python(
            {'a': 0, 'b': 0, 'child': {}, 'sibling': {}}).metadata()
        key_ids = _metadata_key_ids(metadata)
        corrupt_sibling = _build_object_value([
            (
                len(key_ids),
                _encode_scalar_to_value_bytes(2.0, pa.float64()),
            ),
        ])
        corrupt_root = _build_object_value([
            (key_ids['child'], _build_object_value([])),
            (key_ids['sibling'], corrupt_sibling),
        ])
        column = GenericVariant.to_arrow_array([
            GenericVariant(corrupt_root, metadata),
        ])

        with self.assertRaisesRegex(ValueError, "MALFORMED_VARIANT"):
            variant_set(column, '$.child.new', pa.scalar(True))

    def test_root_splice_rejects_nested_unknown_field_id(self):
        metadata = GenericVariant.from_python({'sibling': {}}).metadata()
        key_ids = _metadata_key_ids(metadata)
        valid_sibling = _build_object_value([
            (
                key_ids['sibling'],
                _encode_scalar_to_value_bytes(1.0, pa.float64()),
            ),
        ])
        corrupt_sibling = _build_object_value([
            (
                len(key_ids),
                _encode_scalar_to_value_bytes(2.0, pa.float64()),
            ),
        ])
        corrupt_root = _build_object_value([
            (key_ids['sibling'], corrupt_sibling),
        ])
        column = GenericVariant.to_arrow_array([
            GenericVariant(_build_object_value([
                (key_ids['sibling'], valid_sibling),
            ]), metadata),
            GenericVariant(corrupt_root, metadata),
        ])

        with self.assertRaisesRegex(ValueError, "MALFORMED_VARIANT"):
            variant_set(column, '$.new', pa.scalar(True))

    def test_rejects_duplicate_source_field_id(self):
        metadata = GenericVariant.from_python({'value': 0}).metadata()
        corrupt = _build_object_value([
            (0, _encode_scalar_to_value_bytes(1.0, pa.float64())),
            (0, _encode_scalar_to_value_bytes(2.0, pa.float64())),
        ])
        column = GenericVariant.to_arrow_array(
            [GenericVariant(corrupt, metadata)])

        with self.assertRaisesRegex(ValueError, "MALFORMED_VARIANT"):
            variant_set(column, '$.value', pa.scalar(9.0))

    def test_rejects_duplicate_source_field_id_in_peer_row(self):
        metadata = GenericVariant.from_python({'a': 0, 'b': 0}).metadata()
        duplicate = _build_object_value([
            (0, _encode_scalar_to_value_bytes(1.0, pa.float64())),
            (0, _encode_scalar_to_value_bytes(2.0, pa.float64())),
        ])
        column = GenericVariant.to_arrow_array([
            GenericVariant.from_python({'a': 1.0, 'b': 2.0}),
            GenericVariant(duplicate, metadata),
        ])

        for updater in (variant_replace, variant_set):
            with self.subTest(updater=updater.__name__):
                with self.assertRaisesRegex(
                        ValueError, "MALFORMED_VARIANT"):
                    updater(column, '$.a', pa.scalar(9.0))

    def test_root_splice_rejects_duplicate_peer_offsets(self):
        valid = GenericVariant.from_python({'a': 1.0, 'b': 2.0})
        corrupt = bytearray(valid.value())
        size, id_size, id_start, _, _, _ = _checked_object_layout(
            corrupt, 0, len(corrupt))
        offset_size = ((corrupt[0] >> 2) & 0x3) + 1
        offset_start = id_start + size * id_size
        corrupt[offset_start + offset_size:
                offset_start + 2 * offset_size] = (
            0).to_bytes(offset_size, 'little')
        column = GenericVariant.to_arrow_array([
            valid, GenericVariant(bytes(corrupt), valid.metadata()),
        ])

        with self.assertRaisesRegex(ValueError, "MALFORMED_VARIANT"):
            variant_set(column, '$.new', pa.scalar(True))

    def test_root_splice_enforces_value_size_limit(self):
        variant = GenericVariant.from_python({'padding': 'x' * 100})
        column = GenericVariant.to_arrow_array([variant])

        with patch(
                'pypaimon.data.generic_variant._SIZE_LIMIT',
                len(variant.value()) + 2,
        ):
            with self.assertRaisesRegex(
                    ValueError, 'VARIANT_CONSTRUCTOR_SIZE_LIMIT'):
                variant_set(column, '$.new', pa.scalar(True))

    def test_rejects_truncated_child_offsets(self):
        valid = GenericVariant.from_python({'a': 1.0, 'b': 2.0})
        truncated = _build_object_value([
            (0, _encode_scalar_to_value_bytes(1.0, pa.float64())[:-2]),
            (1, _encode_scalar_to_value_bytes(2.0, pa.float64())),
        ])
        column = GenericVariant.to_arrow_array(
            [GenericVariant(truncated, valid.metadata())])
        original = column.to_pylist()

        with self.assertRaisesRegex(ValueError, "MALFORMED_VARIANT"):
            variant_set(column, '$.c', pa.scalar(True))
        self.assertEqual(column.to_pylist(), original)


class TestVariantSetJavaInterop(unittest.TestCase):

    def test_from_python_orders_object_fields_by_utf8(self):
        key_sup = chr(0x10000)
        key_bmp = chr(0xE000)
        payload = {'k%02d' % i: float(i) for i in range(40)}
        payload[key_sup] = 1.0
        payload[key_bmp] = 2.0

        variant = GenericVariant.from_python(payload)
        order = list(variant.to_python().keys())
        expected = sorted(
            list(payload.keys()),
            key=lambda name: name.encode('utf-8'))
        self.assertEqual(order, expected)

    def test_reads_java_generated_variant(self):
        column = GenericVariant.to_arrow_array(
            [GenericVariant(_JAVA_VALUE, _JAVA_METADATA)])

        self.assertEqual(_decode(column), [_JAVA_PYTHON_VALUE])
        self.assertEqual(
            variant_get(
                column, '$.angular_velocity.y', pa.float64()).to_pylist(),
            [1.5],
        )

    def test_updates_java_generated_variant(self):
        column = GenericVariant.to_arrow_array(
            [GenericVariant(_JAVA_VALUE, _JAVA_METADATA)])

        result = variant_set(column, {
            '$.angular_velocity.y': pa.scalar(-1.5, type=pa.float64()),
            '$.processed': pa.scalar(False),
            '$.mark': pa.scalar('py'),
        })

        expected = {
            'angular_velocity': {'y': -1.5, 'z': -2.5},
            'linear_acceleration': {'y': 0.25, 'z': 4.0},
            'processed': False,
            'seq': 7,
            'mark': 'py',
        }
        decoded = _decode(result)[0]
        self.assertEqual(decoded, expected)
        self.assertEqual(list(decoded), sorted(decoded))

    def test_produces_java_equivalent_encoding(self):
        # This update was verified to round-trip through the Java
        # GenericVariant reader (toJson/getFieldByKey, incl. binary search).
        column = _variants([{
            'angular_velocity': {'y': -1.5, 'z': 2.5},
            'linear_acceleration': {'y': -0.25, 'z': -4.0},
            'seq': 7,
        }])

        result = variant_set(column, {
            '$.angular_velocity.y': pa.scalar(1.5, type=pa.float64()),
            '$.angular_velocity.z': pa.scalar(-2.5, type=pa.float64()),
            '$.linear_acceleration.y': pa.scalar(0.25, type=pa.float64()),
            '$.linear_acceleration.z': pa.scalar(4.0, type=pa.float64()),
            '$.processed': pa.scalar(True, type=pa.bool_()),
        })

        decoded = _decode(result)[0]
        self.assertEqual(decoded, _JAVA_PYTHON_VALUE)
        java_decoded = GenericVariant(
            _JAVA_VALUE, _JAVA_METADATA).to_python()
        self.assertEqual(decoded, java_decoded)
        self.assertEqual(list(decoded), sorted(decoded))


class TestMetadataWithKeys(unittest.TestCase):

    def test_reuses_existing_keys(self):
        metadata = GenericVariant.from_python({'a': 0, 'b': 0}).metadata()

        new_metadata, key_ids, names_by_id = _metadata_with_keys(
            metadata, ('b',))

        self.assertIsNone(new_metadata)
        self.assertEqual(key_ids, {'a': 0, 'b': 1})
        self.assertEqual(names_by_id, {0: 'a', 1: 'b'})

    def test_appends_missing_keys(self):
        metadata = GenericVariant.from_python({'a': 0}).metadata()

        new_metadata, key_ids, names_by_id = _metadata_with_keys(
            metadata, ('b', 'c'))

        self.assertEqual(key_ids, {'a': 0, 'b': 1, 'c': 2})
        self.assertEqual(names_by_id, {0: 'a', 1: 'b', 2: 'c'})
        self.assertEqual(
            _metadata_key_ids(new_metadata), {'a': 0, 'b': 1, 'c': 2})


if __name__ == '__main__':
    unittest.main()
