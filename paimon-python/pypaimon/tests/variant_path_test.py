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
from pypaimon.data.generic_variant import _DOUBLE, GenericVariant
from pypaimon.data.variant_path import (
    _checked_object_layout,
    _compile_paths,
    _metadata_cache,
    _metadata_key_ids,
    _path_positions,
    _rebuilt_offsets,
    _vectorized_get_chunk,
    variant_get,
    variant_to_pylist,
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


def _typed_object(fields):
    metadata = GenericVariant.from_python({
        name: 0 for name in fields
    }).metadata()
    key_ids = _metadata_key_ids(metadata)
    value = _build_object_value([
        (key_ids[name], _encode_scalar_to_value_bytes(item, data_type))
        for name, (item, data_type) in fields.items()
    ])
    return GenericVariant.to_arrow_array([
        GenericVariant(value, metadata)])


class TestVariantToPylist(unittest.TestCase):

    def test_preserves_mixed_types_missing_null_and_literal_names(self):
        column = _variants([
            {'state.x': 1, 'child': {'count': 2}, 'nullable': None},
            {'state.x': 1.5, 'child': [3, 4]},
            {},
            None,
        ])

        result = variant_to_pylist(
            column, ['state.x', 'child', 'nullable', 'absent'])

        self.assertEqual(result, [
            {'state.x': 1, 'child': {'count': 2}, 'nullable': None},
            {'state.x': 1.5, 'child': [3, 4]},
            {},
            None,
        ])

    def test_selects_from_wide_objects_and_chunked_arrays(self):
        first = {'field.%03d' % i: float(i) for i in range(128)}
        second = {'field.%03d' % i: i for i in range(128)}
        column = pa.chunked_array([
            _variants([first]), _variants([second])])

        result = variant_to_pylist(
            column, ['field.000', 'field.127', 'field.000'])

        self.assertEqual(result, [
            {'field.000': 0.0, 'field.127': 127.0},
            {'field.000': 0, 'field.127': 127},
        ])

    def test_selects_from_same_metadata_with_different_slots(self):
        fields = {'field.%03d' % i: float(i) for i in range(128)}
        metadata = GenericVariant.from_python(fields).metadata()
        key_ids = _metadata_key_ids(metadata)
        rows = []
        for names in (list(fields), list(fields)[10:], list(fields)):
            value = _build_object_value([
                (key_ids[name], _encode_scalar_to_value_bytes(
                    fields[name], pa.float64()))
                for name in names
            ])
            rows.append(GenericVariant(value, metadata))

        result = variant_to_pylist(
            GenericVariant.to_arrow_array(rows),
            ['field.000', 'field.015'])

        self.assertEqual(result, [
            {'field.000': 0.0, 'field.015': 15.0},
            {'field.015': 15.0},
            {'field.000': 0.0, 'field.015': 15.0},
        ])

    def test_preserves_other_primitive_types(self):
        timestamp = datetime.datetime(2026, 8, 11, 1, 2, 3, 456000)
        fields = {
            'flag': (True, pa.bool_()),
            'text': ('hello', pa.string()),
            'binary': (b'abc', pa.binary()),
            'decimal': (Decimal('12.30'), pa.decimal128(4, 2)),
            'date': (datetime.date(2026, 8, 11), pa.date32()),
            'timestamp': (timestamp, pa.timestamp('us')),
        }

        result = variant_to_pylist(_typed_object(fields), fields)

        self.assertEqual(result, [
            {name: value for name, (value, _) in fields.items()}])

    def test_rejects_non_object_root_and_invalid_fields(self):
        with self.assertRaisesRegex(TypeError, "root must be an object"):
            variant_to_pylist(_variants([[1, 2]]), ['field'])
        with self.assertRaisesRegex(TypeError, "sequence of field names"):
            variant_to_pylist(_variants([{}]), 'field')
        with self.assertRaisesRegex(TypeError, "field names must be strings"):
            variant_to_pylist(_variants([{}]), [1])

    def test_rejects_truncated_object(self):
        variant = GenericVariant.from_python({'field': 123})
        truncated = GenericVariant(
            variant.value()[:-1], variant.metadata())
        with self.assertRaisesRegex(ValueError, "MALFORMED_VARIANT"):
            variant_to_pylist(
                GenericVariant.to_arrow_array([truncated]), ['field'])

    def test_rejects_truncated_selected_child_before_next_field(self):
        valid = GenericVariant.from_python({'a': 1.0, 'b': 2.0})
        key_ids = _metadata_key_ids(valid.metadata())
        value = _build_object_value([
            (key_ids['a'], bytes([_primitive_header(_DOUBLE)])),
            (key_ids['b'], _encode_scalar_to_value_bytes(2.0, pa.float64())),
        ])
        column = GenericVariant.to_arrow_array([
            GenericVariant(value, valid.metadata())])

        with self.assertRaisesRegex(ValueError, "MALFORMED_VARIANT"):
            variant_to_pylist(column, ['a'])
        self.assertEqual(variant_to_pylist(column, ['b']), [{'b': 2.0}])

    def test_rejects_truncated_nested_selected_child(self):
        valid = GenericVariant.from_python(
            {'a': {'nested': 1.0}, 'b': 2.0})
        key_ids = _metadata_key_ids(valid.metadata())
        nested = _build_object_value([
            (key_ids['nested'], bytes([_primitive_header(_DOUBLE)])),
        ])
        value = _build_object_value([
            (key_ids['a'], nested),
            (key_ids['b'], _encode_scalar_to_value_bytes(2.0, pa.float64())),
        ])
        column = GenericVariant.to_arrow_array([
            GenericVariant(value, valid.metadata())])

        with self.assertRaisesRegex(ValueError, "MALFORMED_VARIANT"):
            variant_to_pylist(column, ['a'])

    def test_selected_offsets_cross_encoded_integer_width(self):
        for count, value in ((100, None), (128, None), (5362, 1.5)):
            with self.subTest(count=count):
                name = 'field%04d' % (count - 1)
                variant = GenericVariant.from_python({
                    'field%04d' % index: value for index in range(count)
                })
                column = GenericVariant.to_arrow_array([variant])
                self.assertEqual(
                    variant_to_pylist(column, [name]), [{name: value}])


class TestVariantGet(unittest.TestCase):

    def test_medium_float_batch_keeps_vectorized_reader(self):
        column = _variants([
            {'field%03d' % i: float(i + row) for i in range(128)}
            for row in range(128)
        ])
        paths = {'$.field%03d' % i: pa.float64() for i in range(16)}
        vectorized_results = []

        def track_vectorized(*args):
            result = _vectorized_get_chunk(*args)
            vectorized_results.append(result is not None)
            return result

        with patch('pypaimon.data.variant_path._vectorized_get_chunk',
                   side_effect=track_vectorized):
            result = variant_get(column, paths)

        self.assertEqual(vectorized_results, [True])
        self.assertEqual(result['$.field000'].to_pylist(),
                         [float(row) for row in range(128)])

    def test_compile_paths_builds_trie_without_prefix_slices(self):
        class NoSlicePath(tuple):
            def __getitem__(self, item):
                if isinstance(item, slice):
                    raise AssertionError("path prefix was materialized")
                return super().__getitem__(item)

        paths = (
            NoSlicePath((('key', 'root'), ('index', 0), ('key', 'left'))),
            NoSlicePath((('key', 'root'), ('index', 0), ('key', 'right'))),
        )

        nodes, results = _compile_paths(paths)

        self.assertEqual(len(nodes), 5)
        self.assertEqual(results, (3, 4))
        self.assertEqual(nodes[3], (2, 'key', 'left'))
        self.assertEqual(nodes[4], (2, 'key', 'right'))

    def test_metadata_cache_is_bounded_and_released(self):
        column = _variants([
            {'value': float(index), 'key_%d' % index: index}
            for index in range(300)
        ])
        cache_sizes = []

        def parse_metadata(metadata):
            cache_sizes.append(len(_metadata_cache.value))
            return _metadata_key_ids(metadata)

        with patch(
                'pypaimon.data.variant_path._metadata_key_ids',
                side_effect=parse_metadata):
            result = variant_get(column, '$.value', pa.float64())

        self.assertEqual(result.to_pylist(), [float(i) for i in range(300)])
        self.assertLessEqual(max(cache_sizes), 256)
        self.assertFalse(hasattr(_metadata_cache, 'value'))

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

    def test_reads_many_flat_fields_without_repeating_vectorized_scan(self):
        fields = {'field.%03d' % i: float(i) for i in range(128)}
        paths = {'$["field.%03d"]' % i: pa.float64()
                 for i in range(16)}
        column = _variants([fields, fields])

        with patch(
                'pypaimon.data.variant_path._vectorized_get_chunk',
                side_effect=AssertionError(
                    "wide flat lookup should be shared")):
            result = variant_get(column, paths)

        for i, path in enumerate(paths):
            self.assertEqual(result[path].to_pylist(), [float(i), float(i)])

    def test_reads_many_flat_fields_with_exact_primitive_types(self):
        timestamp = datetime.datetime(2026, 8, 11, 1, 2, 3, 456000)
        fields = {
            'field.%03d' % i: (float(i), pa.float64())
            for i in range(128)
        }
        selected = {
            'flag': (True, pa.bool_()),
            'count': (123, pa.int64()),
            'text': ('hello', pa.string()),
            'binary': (b'abc', pa.binary()),
            'decimal': (Decimal('12.30'), pa.decimal128(4, 2)),
            'date': (datetime.date(2026, 8, 11), pa.date32()),
            'timestamp': (timestamp, pa.timestamp('us')),
        }
        fields.update(selected)
        paths = {'$.%s' % name: data_type
                 for name, (_, data_type) in selected.items()}
        paths['$["field.000"]'] = pa.float64()

        with patch(
                'pypaimon.data.variant_path._vectorized_get_chunk',
                side_effect=AssertionError(
                    "wide flat lookup should be shared")):
            result = variant_get(_typed_object(fields), paths)

        for name, (expected, _) in selected.items():
            self.assertEqual(result['$.%s' % name].to_pylist(), [expected])
        self.assertEqual(result['$["field.000"]'].to_pylist(), [0.0])

    def test_reads_many_flat_fields_with_complex_types(self):
        fields = {'field.%03d' % i: float(i) for i in range(128)}
        fields.update({
            'object': {'count': 2, 'flag': True},
            'array': [1, 2],
            'map': {'left': 1, 'right': 2},
        })
        paths = {'$["field.%03d"]' % i: pa.float64()
                 for i in range(5)}
        paths.update({
            '$.object': pa.struct([
                ('count', pa.int64()), ('flag', pa.bool_())]),
            '$.array': pa.list_(pa.int64()),
            '$.map': pa.map_(pa.string(), pa.int64()),
        })

        with patch(
                'pypaimon.data.variant_path._vectorized_get_chunk',
                side_effect=AssertionError(
                    "wide flat lookup should be shared")):
            result = variant_get(_variants([fields]), paths)

        self.assertEqual(result['$.object'].to_pylist(),
                         [{'count': 2, 'flag': True}])
        self.assertEqual(result['$.array'].to_pylist(), [[1, 2]])
        self.assertEqual(result['$.map'].to_pylist(),
                         [[('left', 1), ('right', 2)]])

    def test_many_flat_fields_keep_missing_null_and_mixed_metadata(self):
        fields = {'field.%03d' % i: float(i) for i in range(128)}
        paths = {'$["field.%03d"]' % i: pa.float64()
                 for i in range(15)}
        paths['$["maybe.null"]'] = pa.float64()
        column = _variants([
            {**fields, 'maybe.null': None},
            {**fields, 'field.000': None},
            fields,
            None,
        ])

        result = variant_get(column, paths)

        self.assertEqual(
            result['$["field.000"]'].to_pylist(),
            [0.0, None, 0.0, None])
        self.assertEqual(
            result['$["maybe.null"]'].to_pylist(),
            [None, None, None, None])
        self.assertEqual(
            result['$["field.014"]'].to_pylist(),
            [14.0, 14.0, 14.0, None])

    def test_many_flat_fields_allow_nonmonotonic_value_offsets(self):
        fields = {'field.%03d' % i: float(i) for i in range(128)}
        variant = GenericVariant.from_python(fields)
        value = variant.value()
        size, id_width, id_start, data_start, offsets, _ = (
            _checked_object_layout(value, 0, len(value)))
        offset_width = ((value[0] >> 2) & 0x3) + 1
        offset_start = id_start + size * id_width
        children = [value[data_start + offsets[i]:data_start + offsets[i + 1]]
                    for i in range(size)]
        reordered = bytearray(value[:data_start])
        for i in range(size):
            new_offset = sum(len(child) for child in children[i + 1:])
            start = offset_start + i * offset_width
            reordered[start:start + offset_width] = new_offset.to_bytes(
                offset_width, 'little')
        reordered.extend(b''.join(reversed(children)))
        column = GenericVariant.to_arrow_array([
            GenericVariant(bytes(reordered), variant.metadata())])
        paths = {'$["field.%03d"]' % i: pa.float64()
                 for i in range(16)}

        result = variant_get(column, paths)

        for i, path in enumerate(paths):
            self.assertEqual(result[path].to_pylist(), [float(i)])

    def test_many_flat_fields_with_three_byte_offsets(self):
        fields = {'field.%03d' % i: 'x' * 1024 for i in range(128)}
        variant = GenericVariant.from_python(fields)
        offset_width = ((variant.value()[0] >> 2) & 0x3) + 1
        self.assertEqual(offset_width, 3)
        paths = {'$["field.%03d"]' % i: pa.string()
                 for i in range(16)}

        result = variant_get(_variants([fields, fields]), paths)

        for path in paths:
            self.assertEqual(result[path].to_pylist(), ['x' * 1024] * 2)

    def test_many_flat_fields_same_metadata_different_layouts(self):
        fields = {'field.%03d' % i: float(i) for i in range(128)}
        metadata = GenericVariant.from_python(fields).metadata()
        key_ids = _metadata_key_ids(metadata)
        rows = []
        for names in (list(fields), list(fields)[10:], list(fields)):
            value = _build_object_value([
                (key_ids[name], _encode_scalar_to_value_bytes(
                    fields[name], pa.float64()))
                for name in names
            ])
            rows.append(GenericVariant(value, metadata))
        paths = {'$["field.%03d"]' % i: pa.float64()
                 for i in range(16)}

        result = variant_get(GenericVariant.to_arrow_array(rows), paths)

        self.assertEqual(result['$["field.000"]'].to_pylist(),
                         [0.0, None, 0.0])
        self.assertEqual(result['$["field.015"]'].to_pylist(),
                         [15.0, 15.0, 15.0])

    def test_many_flat_fields_reject_duplicate_id_and_offset(self):
        fields = {'field.%03d' % i: float(i) for i in range(128)}
        variant = GenericVariant.from_python(fields)
        original = variant.value()
        size, id_width, id_start, _, _, _ = (
            _checked_object_layout(original, 0, len(original)))
        offset_width = ((original[0] >> 2) & 0x3) + 1
        offset_start = id_start + size * id_width
        paths = {'$["field.%03d"]' % i: pa.float64()
                 for i in range(16)}

        duplicate_id = bytearray(original)
        duplicate_id[id_start + id_width:id_start + 2 * id_width] = (
            duplicate_id[id_start:id_start + id_width])
        with self.assertRaisesRegex(ValueError, 'duplicate object field id'):
            variant_get(GenericVariant.to_arrow_array([
                GenericVariant(bytes(duplicate_id), variant.metadata())
            ]), paths)

        duplicate_offset = bytearray(original)
        duplicate_offset[
            offset_start + offset_width:offset_start + 2 * offset_width
        ] = duplicate_offset[offset_start:offset_start + offset_width]
        with self.assertRaisesRegex(ValueError, 'invalid object offsets'):
            variant_get(GenericVariant.to_arrow_array([
                GenericVariant(bytes(duplicate_offset), variant.metadata())
            ]), paths)

    def test_requires_exact_type(self):
        cases = (
            (_float_variants([1.25]), pa.float64()),
            (_variants([1.25]), pa.float32()),
            (_variants([1]), pa.float64()),
        )
        for column, data_type in cases:
            with self.subTest(data_type=data_type):
                with self.assertRaisesRegex(TypeError, "does not match"):
                    variant_get(column, '$', data_type)

        with self.assertRaisesRegex(TypeError, "does not match"):
            variant_get(_variants([1.0]), '$', pa.string())
        with self.assertRaisesRegex(TypeError, "Unsupported exact"):
            variant_get(_variants([1]), '$', pa.uint32())

    def test_reads_all_signed_integer_widths(self):
        column = _variants([-12, 34])

        for data_type in (pa.int8(), pa.int16(), pa.int32(), pa.int64()):
            with self.subTest(data_type=data_type):
                self.assertEqual(
                    variant_get(column, '$', data_type).to_pylist(),
                    [-12, 34],
                )

    def test_reads_exact_primitive_types(self):
        timestamp = datetime.datetime(2026, 8, 11, 1, 2, 3, 456000)
        column = _typed_object({
            'flag': (True, pa.bool_()),
            'count': (123, pa.int64()),
            'text': ('hello', pa.string()),
            'binary': (b'abc', pa.binary()),
            'decimal': (Decimal('12.30'), pa.decimal128(4, 2)),
            'date': (datetime.date(2026, 8, 11), pa.date32()),
            'timestamp': (timestamp, pa.timestamp('us')),
        })
        result = variant_get(column, {
            '$.flag': pa.bool_(),
            '$.count': pa.int64(),
            '$.text': pa.string(),
            '$.binary': pa.binary(),
            '$.decimal': pa.decimal128(4, 2),
            '$.date': pa.date32(),
            '$.timestamp': pa.timestamp('us'),
        })

        self.assertEqual(
            {path: array[0].as_py() for path, array in result.items()},
            {
                '$.flag': True,
                '$.count': 123,
                '$.text': 'hello',
                '$.binary': b'abc',
                '$.decimal': Decimal('12.30'),
                '$.date': datetime.date(2026, 8, 11),
                '$.timestamp': timestamp,
            },
        )

    def test_reads_exact_complex_types(self):
        column = _variants([{
            'object': {'count': 2, 'flag': True},
            'array': [1, 2],
            'map': {'left': 1, 'right': 2},
        }])
        struct_type = pa.struct([
            ('count', pa.int64()),
            ('flag', pa.bool_()),
            ('missing', pa.string()),
        ])

        self.assertEqual(
            variant_get(column, '$.object', struct_type).to_pylist(),
            [{'count': 2, 'flag': True, 'missing': None}],
        )
        self.assertEqual(
            variant_get(
                column, '$.array', pa.list_(pa.int64())).to_pylist(),
            [[1, 2]],
        )
        self.assertEqual(
            variant_get(
                column,
                '$.map',
                pa.map_(pa.string(), pa.int64()),
            ).to_pylist(),
            [[('left', 1), ('right', 2)]],
        )

    def test_decimal_extraction_preserves_38_digits(self):
        expected = Decimal('12345678901234567890123456789012345678')
        column = _typed_object({
            'value': (expected, pa.decimal128(38, 0)),
        })

        result = variant_get(
            column, '$.value', pa.decimal128(38, 0))

        self.assertEqual(result.to_pylist(), [expected])

    def test_rejects_cross_type_casts(self):
        column = _variants([{
            'count': 123,
            'object': {'value': 1},
            'array': [1],
        }])
        for path, data_type in (
                ('$.count', pa.string()),
                ('$.object', pa.string()),
                ('$.array', pa.string()),
                ('$.array', pa.list_(pa.string()))):
            with self.subTest(path=path, data_type=data_type):
                with self.assertRaisesRegex(TypeError, "does not match"):
                    variant_get(column, path, data_type)

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

    def test_signed_integer_replacement_round_trips(self):
        column = _variants([1])

        for data_type in (pa.int8(), pa.int16(), pa.int32(), pa.int64()):
            with self.subTest(data_type=data_type):
                result = variant_replace(
                    column, '$', pa.scalar(-12, type=data_type))
                self.assertEqual(
                    variant_get(result, '$', data_type).to_pylist(), [-12])

    def test_rejects_negative_decimal_scale(self):
        data_type = pa.decimal128(3, -2)
        column = _variants([100])

        with self.assertRaisesRegex(ValueError, "non-negative"):
            variant_get(column, '$', data_type)
        with self.assertRaisesRegex(ValueError, "non-negative"):
            variant_replace(
                column, '$', pa.scalar(Decimal('1E+2'), type=data_type))
        with self.assertRaisesRegex(ValueError, "non-negative"):
            _encode_scalar_to_value_bytes(Decimal('1E+2'), data_type)
        with self.assertRaisesRegex(ValueError, "non-negative"):
            _encode_scalar_to_value_bytes(None, data_type)

    def test_replaces_exact_primitive_types(self):
        original_timestamp = datetime.datetime(2026, 8, 11)
        column = _typed_object({
            'flag': (True, pa.bool_()),
            'count': (1, pa.int64()),
            'text': ('old', pa.string()),
            'binary': (b'old', pa.binary()),
            'decimal': (Decimal('1.00'), pa.decimal128(3, 2)),
            'date': (datetime.date(2026, 8, 10), pa.date32()),
            'timestamp': (original_timestamp, pa.timestamp('us')),
        })
        new_timestamp = datetime.datetime(2026, 8, 11, 1, 2, 3, 4)

        result = variant_replace(column, {
            '$.flag': pa.scalar(False),
            '$.count': pa.scalar(2, type=pa.int64()),
            '$.text': pa.scalar('new'),
            '$.binary': pa.scalar(b'new'),
            '$.decimal': pa.scalar(
                Decimal('2.50'), type=pa.decimal128(3, 2)),
            '$.date': pa.scalar(
                datetime.date(2026, 8, 11), type=pa.date32()),
            '$.timestamp': pa.scalar(
                new_timestamp, type=pa.timestamp('us')),
        })

        self.assertEqual(_decode(result), [{
            'flag': False,
            'count': 2,
            'text': 'new',
            'binary': b'new',
            'decimal': Decimal('2.50'),
            'date': datetime.date(2026, 8, 11),
            'timestamp': new_timestamp,
        }])

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

        with self.assertRaisesRegex(TypeError, "does not match"):
            variant_replace(
                _variants([1.0]), '$', pa.scalar('1.0', type=pa.string()))

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
             TypeError, "does not match"),
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
