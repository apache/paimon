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

import os
import shutil
import tempfile
import unittest
from unittest.mock import Mock, patch

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.read.read_builder import ReadBuilder
from pypaimon.read.stream_read_builder import StreamReadBuilder
from pypaimon.schema.data_types import AtomicType, DataField, RowType


class _ReadBuilderTestBase(unittest.TestCase):
    """Build a primary-key table whose value column is a ROW so we can
    exercise both top-level and nested projection paths against the
    actual ``ReadBuilder`` API."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', False)

        struct_type = pa.struct([
            ('latest_version', pa.int64()),
            ('latest_value', pa.string()),
        ])
        cls.pa_schema = pa.schema([
            pa.field('pk', pa.int64(), nullable=False),
            ('mv', struct_type),
            ('val', pa.string()),
            ('attrs', pa.map_(pa.string(), pa.int64())),
        ])
        schema = Schema.from_pyarrow_schema(
            cls.pa_schema, primary_keys=['pk'],
            options={'bucket': '1', 'file.format': 'parquet'})
        cls.catalog.create_table('default.rb_nested', schema, False)
        cls.table = cls.catalog.get_table('default.rb_nested')

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)


class ReadBuilderProjectionStateTest(_ReadBuilderTestBase):

    def test_variant_fields_are_validated_and_copied(self):
        table = Mock()
        table.fields = [
            DataField(0, 'id', AtomicType('INT')),
            DataField(1, 'payload', AtomicType('VARIANT')),
        ]
        paths = ['$.ratio', '$.age']
        builder = ReadBuilder(table).with_projection(
            ['id', 'payload'],
            variant_fields={
                'payload': {
                    'paths': paths,
                    'target_type': pa.float32(),
                }
            },
        )

        paths.append('$.later')
        self.assertEqual(
            ['$.ratio', '$.age'],
            builder._variant_fields['payload']['paths'])
        self.assertFalse(
            builder._variant_fields['payload']['fail_on_error'])

    def test_variant_target_type_matches_native_float32_only(self):
        table = Mock()
        table.fields = [DataField(1, 'payload', AtomicType('VARIANT'))]

        builder = ReadBuilder(table).with_projection(
            ['payload'], variant_fields={'payload': {
                'paths': ['$.x'], 'target_type': pa.float32()}})
        self.assertEqual(pa.float32(),
                         builder._variant_fields['payload']['target_type'])

        for target_type in (pa.bool_(), pa.int32(), pa.int64(),
                            pa.float64(), pa.string(), pa.binary(),
                            pa.decimal128(10, 2), pa.date32(),
                            pa.timestamp('us'),
                            pa.timestamp('us', tz='UTC'),
                            pa.large_string(), pa.large_binary(),
                            pa.binary(4),
                            pa.dictionary(pa.int8(), pa.string()),
                            pa.timestamp('s'), pa.timestamp('ms'),
                            pa.timestamp('ns'),
                            pa.timestamp('us', tz='Asia/Shanghai')):
            with self.subTest(target_type=target_type):
                with self.assertRaisesRegex(ValueError, 'must be float32'):
                    ReadBuilder(table).with_projection(
                        ['payload'], variant_fields={'payload': {
                            'paths': ['$.x'], 'target_type': target_type}})

    def test_variant_fields_require_projected_variant_column(self):
        table = Mock()
        table.fields = [
            DataField(0, 'id', AtomicType('INT')),
            DataField(1, 'payload', AtomicType('VARIANT')),
        ]

        with self.assertRaisesRegex(ValueError, 'not in the projection'):
            ReadBuilder(table).with_projection(
                ['id'],
                variant_fields={
                    'payload': {
                        'paths': ['$.ratio'],
                        'target_type': pa.float32(),
                    }
                },
            )

    def test_variant_fields_accept_literal_top_level_punctuation(self):
        table = Mock()
        table.fields = [
            DataField(0, 'id.dot', AtomicType('INT')),
            DataField(1, 'payload', AtomicType('VARIANT')),
            DataField(2, 'payload.dot', AtomicType('VARIANT')),
            DataField(3, 'payload[raw]', AtomicType('VARIANT')),
        ]
        table.options.row_tracking_enabled.return_value = False

        for projection, column in (
            (['payload.dot'], 'payload.dot'),
            (['payload[raw]'], 'payload[raw]'),
            (['id.dot', 'payload'], 'payload'),
        ):
            options = {column: {
                'paths': ['$.ratio'], 'target_type': pa.float32()}}
            with self.subTest(projection=projection):
                batch = ReadBuilder(table).with_projection(
                    projection, variant_fields=options)
                stream = StreamReadBuilder(table).with_projection(
                    projection, variant_fields=options)
                self.assertIsNone(batch._nested_paths)
                self.assertIsNone(batch._nested_name_paths())
                self.assertEqual(
                    projection, [field.name for field in batch.read_type()])
                self.assertIsNone(stream._nested_name_paths())
                self.assertEqual(
                    projection, [field.name for field in stream.read_type()])
                with patch('pypaimon.read.read_builder.TableRead') as read:
                    batch.new_read()
                    self.assertIsNone(
                        read.call_args.kwargs['nested_name_paths'])
                    self.assertIn(column,
                                  read.call_args.kwargs['variant_fields'])
                with patch('pypaimon.read.stream_read_builder.TableRead') as read:
                    stream.new_read()
                    self.assertIsNone(
                        read.call_args.kwargs['nested_name_paths'])
                    self.assertIn(column,
                                  read.call_args.kwargs['variant_fields'])

    def test_variant_fields_still_reject_nested_projection(self):
        table = Mock()
        table.fields = [
            DataField(0, 'row', RowType(True, [
                DataField(1, 'value', AtomicType('INT'))])),
            DataField(2, 'payload', AtomicType('VARIANT')),
        ]
        table.options.row_tracking_enabled.return_value = False
        options = {'payload': {
            'paths': ['$.ratio'], 'target_type': pa.float32()}}

        with self.assertRaisesRegex(ValueError, 'nested column projection'):
            ReadBuilder(table).with_projection(
                ['row.value', 'payload'], variant_fields=options)
        with self.assertRaisesRegex(ValueError, 'nested column projection'):
            StreamReadBuilder(table).with_projection(
                ['row.value', 'payload'], variant_fields=options).new_read()

    def test_no_projection_returns_full_schema(self):
        rb = self.table.new_read_builder()
        fields = rb.read_type()
        names = [f.name for f in fields]
        self.assertEqual(names, ['pk', 'mv', 'val', 'attrs'])
        # Without an explicit projection the read_type must NOT inject
        # row-tracking system columns; the raw table fields are returned
        # verbatim.
        self.assertNotIn('_ROW_ID', names)
        self.assertNotIn('_SEQUENCE_NUMBER', names)

    def test_top_level_projection_unchanged(self):
        rb = self.table.new_read_builder().with_projection(['val', 'pk'])
        names = [f.name for f in rb.read_type()]
        self.assertEqual(names, ['val', 'pk'])
        # No nested paths derived; only names are stored.
        self.assertIsNone(rb._nested_paths)

    def test_dotted_name_resolves_to_nested_path(self):
        rb = self.table.new_read_builder().with_projection(
            ['mv.latest_version', 'pk'])
        # _nested_paths is populated; user-facing names are kept on _projection
        self.assertIsNotNone(rb._nested_paths)
        self.assertEqual(rb._nested_paths, [[1, 0], [0]])
        names = [f.name for f in rb.read_type()]
        # Nested leaves get flattened to underscore-joined names.
        self.assertEqual(names, ['mv_latest_version', 'pk'])

    def test_dotted_name_unknown_top_silently_skipped(self):
        rb = self.table.new_read_builder().with_projection(
            ['nope.x', 'val'])
        # Only 'val' resolved, with no nested field actually selected.
        self.assertIsNone(rb._nested_paths)
        names = [f.name for f in rb.read_type()]
        self.assertEqual(names, ['val'])

    def test_dotted_name_unknown_subfield_silently_skipped(self):
        rb = self.table.new_read_builder().with_projection(
            ['mv.no_such_subfield', 'pk'])
        # The bad path drops out, the plain name survives.
        self.assertIsNone(rb._nested_paths)
        names = [f.name for f in rb.read_type()]
        self.assertEqual(names, ['pk'])

    def test_bracketed_map_selector_is_one_literal_key(self):
        rb = self.table.new_read_builder().with_projection(
            ["attrs['key.with.dots']", 'attrs["other"]'])

        self.assertEqual(
            [['attrs', 'key.with.dots'], ['attrs', 'other']],
            rb._nested_name_paths(),
        )
        self.assertEqual(
            ['attrs_key_with_dots', 'attrs_other'],
            [field.name for field in rb.read_type()],
        )
        self.assertEqual(
            ['attrs'], [field.name for field in rb.new_scan()._read_type])

    def test_dot_does_not_select_map_key(self):
        rb = self.table.new_read_builder().with_projection(
            ['attrs.other', 'pk'])

        self.assertEqual(['pk'], [field.name for field in rb.read_type()])


class ReadBuilderProjectionFieldIdTest(_ReadBuilderTestBase):

    def test_nested_leaves_inherit_leaf_field_id(self):
        rb = self.table.new_read_builder().with_projection(
            ['mv.latest_version', 'mv.latest_value'])
        leaf_ids = [f.id for f in rb.read_type()]
        # Look up the actual leaf IDs from the table schema for assertion
        mv_field = next(f for f in self.table.fields if f.name == 'mv')
        sub_v = next(f for f in mv_field.type.fields
                     if f.name == 'latest_version')
        sub_x = next(f for f in mv_field.type.fields
                     if f.name == 'latest_value')
        self.assertEqual(leaf_ids, [sub_v.id, sub_x.id])


class StreamReadBuilderNestedProjectionTest(_ReadBuilderTestBase):

    def test_stream_builder_matches_batch_nested_projection(self):
        projection = [
            'mv.latest_version',
            "attrs['key.with.dots']",
            'pk',
        ]
        batch = self.table.new_read_builder().with_projection(projection)
        stream = self.table.new_stream_read_builder().with_projection(projection)

        self.assertEqual(
            [field.name for field in batch.read_type()],
            [field.name for field in stream.read_type()],
        )
        self.assertEqual(
            batch._nested_name_paths(),
            stream._nested_name_paths(),
        )
        self.assertEqual(
            [field.name for field in batch.new_scan()._read_type],
            [field.name for field in stream.new_streaming_scan()._read_type],
        )

        table_read = stream.with_include_row_kind().new_read()
        self.assertEqual(batch._nested_name_paths(), table_read.nested_name_paths)
        self.assertTrue(table_read.include_row_kind)


if __name__ == '__main__':
    unittest.main()
