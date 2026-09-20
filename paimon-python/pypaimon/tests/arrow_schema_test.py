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

import unittest

import pyarrow as pa
from parameterized import parameterized

from pypaimon.schema.arrow_schema import (
    arrow_schemas_compatible, arrow_types_compatible, cast_arrow_array, prepare_arrow_input,
)
from pypaimon.read.table_read import TableRead


class ArrowSchemaTest(unittest.TestCase):
    def test_layout_compatibility_does_not_allow_value_conversion(self):
        self.assertTrue(arrow_types_compatible(pa.list_(pa.large_string()), pa.list_(pa.string())))
        for source, target in [
                (pa.int32(), pa.int64()),
                (pa.binary(), pa.large_binary()),
                (pa.large_list(pa.string()), pa.list_(pa.string())),
                (pa.list_(pa.string(), 2), pa.list_(pa.string(), 3)),
                (pa.string(), pa.binary())]:
            with self.subTest(source=source, target=target):
                self.assertFalse(arrow_types_compatible(source, target))
        # Legacy writer policies are opt-in and never apply to BLOB.
        self.assertTrue(arrow_schemas_compatible(
            pa.schema([pa.field('b', pa.binary(), nullable=False)]),
            pa.schema([('b', pa.binary(4))]), check_top_level_nullability=False, allow_binary_compatibility=True))
        self.assertFalse(arrow_schemas_compatible(
            pa.schema([('b', pa.binary())]), pa.schema([('b', pa.large_binary())]),
            allow_binary_compatibility=True))

    @parameterized.expand([(False,), (True,)])
    def test_safe_value_conversion_preserves_nested_string_layout(self, record_batch):
        source_type = pa.struct([
            pa.field('text', pa.large_string()),
            pa.field('number', pa.int64()),
        ])
        target_type = pa.struct([
            pa.field('text', pa.string(), metadata={b'description': b'label'}),
            pa.field('number', pa.int32()),
        ])
        source = pa.table({'nested': pa.array([
            {'text': '任务', 'number': 10}, None,
        ], type=source_type)})
        if record_batch:
            source = source.to_batches()[0]
        target = pa.schema([('nested', target_type)], metadata={b'contract': b'target'})
        converted = prepare_arrow_input(source, target)
        self.assertIsInstance(converted, type(source))
        self.assertEqual(converted.to_pylist(), source.to_pylist())
        self.assertEqual(converted.schema.metadata, target.metadata)
        result_type = converted.schema.field('nested').type
        self.assertEqual(result_type.field('text').type, pa.large_string())
        self.assertEqual(result_type.field('text').metadata, target_type.field('text').metadata)
        self.assertEqual(result_type.field('number').type, pa.int32())
        with self.assertRaises(pa.ArrowInvalid):
            prepare_arrow_input(pa.table({'number': [2 ** 40]}), pa.schema([('number', pa.int32())]))

    def test_unchanged_layout_reuses_buffers(self):
        source = pa.table({'text': pa.array(['a', None, '中文'], type=pa.large_string())})
        target = pa.schema([('text', pa.string())])
        self.assertIs(prepare_arrow_input(source, target), source)
        narrowed = cast_arrow_array(source['text'], pa.string())
        self.assertEqual(narrowed.type, pa.string())
        self.assertEqual(narrowed.to_pylist(), source['text'].to_pylist())

    def test_nested_nullability_validates_only_visible_values(self):
        source_type = pa.struct([pa.field('text', pa.large_string(), nullable=False)])
        target_type = pa.struct([pa.field('text', pa.string(), nullable=False)])
        target = pa.schema([('nested', target_type)])
        valid = pa.table({'nested': pa.array([None, {'text': 'ok'}], type=source_type)})
        self.assertEqual(prepare_arrow_input(valid, target, validate_nullability=True).to_pylist(), valid.to_pylist())
        invalid = pa.table({'nested': pa.array([{'text': None}], type=source_type)})
        with self.assertRaisesRegex(ValueError, 'non-nullable field nested.text'):
            prepare_arrow_input(invalid, target, validate_nullability=True)

    def test_nested_nullability_matches_projected_fields_by_name(self):
        source_type = pa.struct([
            pa.field('unused', pa.string()),
            pa.field('text', pa.large_string(), nullable=False),
        ])
        target_type = pa.struct([pa.field('text', pa.string(), nullable=False)])
        source = pa.table({'nested': pa.array([
            {'unused': None, 'text': 'ok'}, None,
        ], type=source_type)})
        result = prepare_arrow_input(source, pa.schema([('nested', target_type)]), validate_nullability=True)
        self.assertEqual(result.to_pylist(), [{'nested': {'text': 'ok'}}, {'nested': None}])
        self.assertEqual(result.schema.field('nested').type.field('text').type, pa.large_string())

    def test_missing_nested_field_reports_conversion_error(self):
        source = pa.table({'nested': pa.array([{'text': 'ok'}], type=pa.struct([('text', pa.large_string())]))})
        target = pa.schema([('nested', pa.struct([
            ('text', pa.string()), pa.field('required', pa.int32(), nullable=False),
        ]))])
        with self.assertRaises((ValueError, TypeError)):
            prepare_arrow_input(source, target, validate_nullability=True)

    def test_numeric_evolution_still_truncates_with_string_layout_change(self):
        source = pa.array([{'text': '中文', 'number': 1.9}], type=pa.struct([
            ('text', pa.large_string()), ('number', pa.float64()),
        ]))
        target = pa.struct([('text', pa.string()), ('number', pa.int32())])
        result = cast_arrow_array(source, target, safe=False)
        self.assertEqual(result.type, target)
        self.assertEqual(result.to_pylist(), [{'text': '中文', 'number': 1}])

    def test_native_output_accepts_string_layout_only(self):
        batch = pa.record_batch([pa.array(['中文', None], type=pa.large_string())], names=['value'])
        target = pa.schema([('value', pa.string())])
        result = TableRead._try_to_pad_batch_by_schema(batch, target)
        self.assertEqual(result.schema, target)
        self.assertEqual(result.to_pylist(), batch.to_pylist())
        for source_type, target_type, values in [
                (pa.binary(), pa.large_binary(), [b'blob']),
                (pa.int64(), pa.int32(), [1])]:
            with self.subTest(source=source_type, target=target_type):
                batch = pa.record_batch([pa.array(values, type=source_type)], names=['value'])
                with self.assertRaises(TypeError):
                    TableRead._try_to_pad_batch_by_schema(batch, pa.schema([('value', target_type)]))

    def test_evolved_map_read_aligns_key_and_value_layouts(self):
        from pypaimon.read.reader.data_file_batch_reader import DataFileBatchReader
        from pypaimon.schema.data_types import PyarrowFieldParser

        source = pa.array([[('任务', '值')], None, []], type=pa.map_(pa.large_string(), pa.large_string()))
        logical = PyarrowFieldParser.to_paimon_type(source.type, True)
        reader = object.__new__(DataFileBatchReader)
        result = reader._align_array_by_id(source, logical, logical)
        self.assertEqual(result.type, pa.map_(pa.string(), pa.string()))
        self.assertEqual(result.to_pylist(), source.to_pylist())

    @parameterized.expand([(False,), (True,)])
    def test_safe_conversion_enforces_top_level_not_null(self, record_batch):
        target = pa.schema([pa.field('text', pa.string(), nullable=False)])
        for values in [['ok'], [None]]:
            source = pa.table({'text': pa.array(values, type=pa.large_string())})
            if record_batch:
                source = source.to_batches()[0]
            if values[0] is None:
                with self.assertRaises((ValueError, TypeError)):
                    prepare_arrow_input(source, target)
            else:
                converted = prepare_arrow_input(source, target)
                self.assertFalse(converted.schema.field('text').nullable)
                self.assertEqual(converted.column('text').type, pa.large_string())
                self.assertEqual(converted.to_pylist(), source.to_pylist())
