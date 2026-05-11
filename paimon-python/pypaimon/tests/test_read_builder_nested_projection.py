################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema


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

    def test_no_projection_returns_full_schema(self):
        rb = self.table.new_read_builder()
        fields = rb.read_type()
        names = [f.name for f in fields]
        self.assertEqual(names, ['pk', 'mv', 'val'])
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
        # Only 'val' resolved; the dot trigger still populates _nested_paths.
        self.assertEqual(rb._nested_paths, [[2]])
        names = [f.name for f in rb.read_type()]
        self.assertEqual(names, ['val'])

    def test_dotted_name_unknown_subfield_silently_skipped(self):
        rb = self.table.new_read_builder().with_projection(
            ['mv.no_such_subfield', 'pk'])
        # The bad path drops out, the plain name survives.
        self.assertEqual(rb._nested_paths, [[0]])
        names = [f.name for f in rb.read_type()]
        self.assertEqual(names, ['pk'])


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


if __name__ == '__main__':
    unittest.main()
