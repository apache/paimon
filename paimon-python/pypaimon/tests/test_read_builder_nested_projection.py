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
from pypaimon.read.table_read import _RemainingRows
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

    def test_named_variant_expressions_group_paths_and_error_policies(self):
        table = Mock()
        table.fields = [
            DataField(0, 'id', AtomicType('INT')),
            DataField(1, 'payload', AtomicType('VARIANT')),
        ]
        builder = ReadBuilder(table).with_projection(
            {'identifier': 'id',
             'ratio': "try_variant_get(payload, '$.ratio', 'float')",
             'age': "variant_get(payload, '$.age', 'float')"})

        self.assertEqual(['id', 'payload'], builder._projection)
        self.assertEqual(
            ['$.ratio', '$.age'],
            builder._variant_fields['payload']['paths'])
        self.assertEqual(
            [False, True],
            builder._variant_fields['payload']['fail_on_error'])
        self.assertEqual(
            [('identifier', 'id', None), ('ratio', 'payload', 0),
             ('age', 'payload', 1)], builder._expression_projection)

        builder.with_projection(['id'])
        self.assertIsNone(builder._variant_fields)
        self.assertIsNone(builder._expression_projection)

    def test_variant_target_type_matches_native_float32_only(self):
        table = Mock()
        table.fields = [DataField(1, 'payload', AtomicType('VARIANT'))]

        builder = ReadBuilder(table).with_projection(
            {'x': "try_variant_get(payload, '$.x', 'float')"})
        self.assertEqual(pa.float32(),
                         builder._variant_fields['payload']['target_type'])
        upper = ReadBuilder(table).with_projection(
            {'x': "TRY_VARIANT_GET(payload, '$.x', 'FLOAT')"})
        self.assertEqual([False],
                         upper._variant_fields['payload']['fail_on_error'])

        for target_type in ('double', 'int', 'string', 'timestamp', 'float32'):
            with self.subTest(target_type=target_type):
                with self.assertRaisesRegex(ValueError, 'Only float32'):
                    ReadBuilder(table).with_projection(
                        {'x': "try_variant_get(payload, '$.x', '%s')"
                         % target_type})

    def test_variant_path_rejects_java_metadata_delimiter(self):
        table = Mock()
        table.fields = [DataField(1, 'payload', AtomicType('VARIANT'))]

        with self.assertRaisesRegex(ValueError, "must not contain ';'"):
            ReadBuilder(table).with_projection(
                {'x': 'try_variant_get(payload, "$[\'a;b\']", "float")'})

    def test_variant_path_preserves_sql_escaped_quotes(self):
        table = Mock()
        table.fields = [DataField(1, 'payload', AtomicType('VARIANT'))]
        for function in ('variant_get', 'try_variant_get'):
            with self.subTest(function=function):
                builder = ReadBuilder(table).with_projection({
                    'x': "%s(payload, '$[\"it''s\"]', 'float')" % function,
                })
                self.assertEqual(
                    ['$["it\'s"]'], builder._variant_fields['payload']['paths'])
        with self.assertRaisesRegex(ValueError, 'Adjacent string literals'):
            ReadBuilder(table).with_projection({
                'x': "try_variant_get(payload, '$.it' 's', 'float')",
            })

    def test_named_projection_rejects_unknown_and_non_variant_sources(self):
        table = Mock()
        table.fields = [
            DataField(0, 'id', AtomicType('INT')),
            DataField(1, 'payload', AtomicType('VARIANT')),
        ]

        with self.assertRaisesRegex(ValueError, 'Unsupported projection'):
            ReadBuilder(table).with_projection(
                {'missing': 'not_a_column'})
        with self.assertRaisesRegex(ValueError, 'requires a VARIANT'):
            ReadBuilder(table).with_projection(
                {'x': "try_variant_get(id, '$.x', 'float')"})
        with self.assertRaisesRegex(ValueError, 'both whole and extracted'):
            ReadBuilder(table).with_projection({
                'whole': 'payload',
                'x': "try_variant_get(payload, '$.x', 'float')"})

    def test_named_projection_accepts_literal_top_level_punctuation(self):
        table = Mock()
        table.fields = [
            DataField(0, 'id.dot', AtomicType('INT')),
            DataField(1, 'payload', AtomicType('VARIANT')),
            DataField(2, 'payload.dot', AtomicType('VARIANT')),
            DataField(3, 'payload[raw]', AtomicType('VARIANT')),
        ]
        table.options.row_tracking_enabled.return_value = False

        for projection, column, sources in (
            ({'x': 'try_variant_get("payload.dot", "$.ratio", "float")'},
             'payload.dot', ['payload.dot']),
            ({'x': 'try_variant_get("payload[raw]", "$.ratio", "float")'},
             'payload[raw]', ['payload[raw]']),
            ({'id': 'id.dot',
              'x': 'try_variant_get(payload, "$.ratio", "float")'},
             'payload', ['id.dot', 'payload']),
        ):
            with self.subTest(projection=projection):
                batch = ReadBuilder(table).with_projection(projection)
                stream = StreamReadBuilder(table).with_projection(projection)
                self.assertIsNone(batch._nested_paths)
                self.assertIsNone(batch._nested_name_paths())
                self.assertEqual(
                    sources, [field.name for field in batch.read_type()])
                self.assertIsNone(stream._nested_name_paths())
                self.assertEqual(
                    sources, [field.name for field in stream.read_type()])
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

    def test_mapping_rejects_other_expressions(self):
        table = Mock()
        table.fields = [
            DataField(0, 'row', RowType(True, [
                DataField(1, 'value', AtomicType('INT'))])),
            DataField(2, 'payload', AtomicType('VARIANT')),
        ]
        table.options.row_tracking_enabled.return_value = False
        for expression in (
            'row.value', 'payload + 1',
            "try_variant_get(payload, '$.ratio', 'float') + 1",
            "try_variant_get(payload, '$.ratio', 'float', 1)",
        ):
            with self.subTest(expression=expression):
                with self.assertRaisesRegex(ValueError, 'Unsupported projection'):
                    ReadBuilder(table).with_projection({'x': expression})

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

    def test_named_ordinary_columns_keep_aliases_without_native_read(self):
        read = self.table.new_read_builder().with_projection({
            'value': 'val', 'key': 'pk'}).new_read()
        self.assertEqual(['value', 'key'], read._output_arrow_schema().names)
        batch = pa.record_batch([
            pa.array(['x']), pa.array([7], type=pa.int64()),
        ], names=['val', 'pk'])
        projected = read._project_batch_to_output(batch)
        self.assertEqual(['value', 'key'], projected.schema.names)
        self.assertEqual(['x'], projected.column('value').to_pylist())
        self.assertEqual([7], projected.column('key').to_pylist())

    def test_named_duplicate_columns_after_primary_key_merge(self):
        pa_schema = pa.schema([
            pa.field('pk', pa.int64(), nullable=False),
            pa.field('val', pa.int64()),
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema, primary_keys=['pk'], options={
                'bucket': '1', 'file.format': 'parquet',
                'read.native.enabled': 'false',
            })
        self.catalog.create_table('default.rb_named_duplicate_pk', schema, False)
        table = self.catalog.get_table('default.rb_named_duplicate_pk')
        for value in (20, 30):
            write_builder = table.new_batch_write_builder()
            writer = write_builder.new_write()
            commit = write_builder.new_commit()
            writer.write_arrow(pa.Table.from_arrays([
                pa.array([1], type=pa.int64()),
                pa.array([value], type=pa.int64()),
            ], schema=pa_schema))
            commit.commit(writer.prepare_commit())
            writer.close()
            commit.close()

        builder = table.new_read_builder().with_projection({
            'one': 'pk', 'two': 'pk', 'value': 'val',
        })
        splits = builder.new_scan().plan().splits()
        read = builder.new_read()
        expected = {'one': [1], 'two': [1], 'value': [30]}
        self.assertEqual(
            expected, read.to_arrow(splits, parallelism=1).to_pydict())
        self.assertEqual(
            expected, read.to_arrow_batch_reader(
                splits, parallelism=1).read_all().to_pydict())
        batches = read._read_one_split_to_batches(
            splits[0], read._output_arrow_schema(), _RemainingRows(None))
        self.assertEqual(expected, pa.Table.from_batches(batches).to_pydict())

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
