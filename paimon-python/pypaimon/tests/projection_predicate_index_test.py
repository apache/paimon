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
"""Regression tests for predicate index handling under with_projection.

Predicate leaves are built against the original table schema and carry an
``index`` field encoding that schema's column order. ``with_projection``
later narrows or reorders the scan ``read_type`` — without remapping, the
row-level FilterRecordReader on PK tables would index into the wrong column
of the OffsetRow it's handed, raising IndexError or returning wrong rows.

These tests guard the fix in ``rewrite_predicate_indices`` by exercising:
- PK table + filter on non-PK column + projection that includes the filter
  column (was the IndexError trigger).
- PK table + filter on non-PK column + projection that reorders columns.
- Append-only table sanity check, where the filter is pushed down at file
  level via field names (no index remapping needed) — must not regress.
"""

import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema


class ProjectionPredicateIndexTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', True)

        cls.pa_schema = pa.schema([
            pa.field('id', pa.int32(), nullable=False),
            ('name', pa.string()),
            ('value', pa.int64()),
        ])
        cls.data = pa.Table.from_pydict({
            'id': [1, 2, 3],
            'name': ['a', 'b', 'c'],
            'value': [10, 20, 30],
        }, schema=cls.pa_schema)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _populate_pk(self, table_name: str):
        schema = Schema.from_pyarrow_schema(
            self.pa_schema, primary_keys=['id'], options={'bucket': '2'},
        )
        full = 'default.{}'.format(table_name)
        self.catalog.create_table(full, schema, False)
        table = self.catalog.get_table(full)
        wb = table.new_batch_write_builder()
        w = wb.new_write()
        w.write_arrow(self.data)
        wb.new_commit().commit(w.prepare_commit())
        w.close()
        return self.catalog.get_table(full)

    def _populate_append(self, table_name: str):
        schema = Schema.from_pyarrow_schema(self.pa_schema)
        full = 'default.{}'.format(table_name)
        self.catalog.create_table(full, schema, False)
        table = self.catalog.get_table(full)
        wb = table.new_batch_write_builder()
        w = wb.new_write()
        w.write_arrow(self.data)
        wb.new_commit().commit(w.prepare_commit())
        w.close()
        return self.catalog.get_table(full)

    def _read(self, read_builder):
        scan = read_builder.new_scan()
        read = read_builder.new_read()
        return read.to_arrow(scan.plan().splits())

    def test_pk_filter_on_non_pk_with_projection_keeping_filter_column(self):
        """The OffsetRow handed to FilterRecordReader uses read_type indices.
        Before the fix this used to raise IndexError because the predicate
        leaf carried a position from the original (wider) schema.
        """
        table = self._populate_pk('test_pk_proj_filter_keep')

        pb = table.new_read_builder().new_predicate_builder()
        pred = pb.equal('value', 30)

        # Projection narrows read_type from [id, name, value] to [id, value]
        rb = table.new_read_builder().with_projection(['id', 'value']).with_filter(pred)
        actual = self._read(rb).sort_by('id')

        self.assertEqual(actual.num_rows, 1)
        self.assertEqual(actual.column_names, ['id', 'value'])
        self.assertEqual(actual.column('id').to_pylist(), [3])
        self.assertEqual(actual.column('value').to_pylist(), [30])

    def test_pk_filter_on_non_pk_with_projection_reordering_columns(self):
        """Reordering the projection (value before id) changes the column
        positions in read_type. The remapped index must follow.
        """
        table = self._populate_pk('test_pk_proj_filter_reorder')

        pb = table.new_read_builder().new_predicate_builder()
        pred = pb.greater_than('value', 15)

        rb = table.new_read_builder().with_projection(['value', 'id']).with_filter(pred)
        actual = self._read(rb).sort_by('id')

        self.assertEqual(actual.num_rows, 2)
        self.assertEqual(actual.column_names, ['value', 'id'])
        self.assertEqual(actual.column('id').to_pylist(), [2, 3])
        self.assertEqual(actual.column('value').to_pylist(), [20, 30])

    def test_pk_filter_no_projection_still_works(self):
        """Sanity: with no projection, behaviour must match the pre-fix path."""
        table = self._populate_pk('test_pk_filter_no_proj')

        pb = table.new_read_builder().new_predicate_builder()
        pred = pb.equal('value', 20)

        rb = table.new_read_builder().with_filter(pred)
        actual = self._read(rb).sort_by('id')

        self.assertEqual(actual.num_rows, 1)
        self.assertEqual(actual.column('name').to_pylist(), ['b'])
        self.assertEqual(actual.column('value').to_pylist(), [20])

    def test_pk_filter_column_outside_projection_drops_filter(self):
        """When the filter column is *not* projected we cannot evaluate the
        predicate at row level — the contract is to fall through (no
        FilterRecordReader). The reader still produces the projected columns,
        and the count must not be smaller than the unfiltered projection.
        """
        table = self._populate_pk('test_pk_filter_outside_proj')

        pb = table.new_read_builder().new_predicate_builder()
        pred = pb.equal('value', 30)

        # value is NOT in the projection — predicate cannot apply at row level
        rb = table.new_read_builder().with_projection(['id']).with_filter(pred)
        actual = self._read(rb).sort_by('id')

        # All three rows are still returned because the filter is dropped,
        # not misapplied with a stale index.
        self.assertEqual(actual.num_rows, 3)
        self.assertEqual(actual.column_names, ['id'])
        self.assertEqual(actual.column('id').to_pylist(), [1, 2, 3])

    def test_append_only_filter_with_projection_unchanged(self):
        """Append-only path uses arrow file-level pushdown by field name, not
        by index. Guard against any regression introduced by the fix.
        """
        table = self._populate_append('test_append_proj_filter')

        pb = table.new_read_builder().new_predicate_builder()
        pred = pb.equal('value', 30)

        rb = table.new_read_builder().with_projection(['id', 'value']).with_filter(pred)
        actual = self._read(rb).sort_by('id')

        self.assertEqual(actual.num_rows, 1)
        self.assertEqual(actual.column_names, ['id', 'value'])
        self.assertEqual(actual.column('id').to_pylist(), [3])


class RewritePredicateIndicesUnitTest(unittest.TestCase):
    """Direct unit tests for ``rewrite_predicate_indices`` so the helper is
    covered without going through a full table read.
    """

    def _build_predicate(self):
        from pypaimon import CatalogFactory, Schema
        tempdir = tempfile.mkdtemp()
        try:
            warehouse = os.path.join(tempdir, 'warehouse')
            catalog = CatalogFactory.create({'warehouse': warehouse})
            catalog.create_database('default', True)
            pa_schema = pa.schema([
                ('a', pa.int64()),
                ('b', pa.string()),
                ('c', pa.int64()),
            ])
            schema = Schema.from_pyarrow_schema(pa_schema)
            catalog.create_table('default.t', schema, False)
            pb = catalog.get_table('default.t').new_read_builder().new_predicate_builder()
            return pb
        finally:
            shutil.rmtree(tempdir, ignore_errors=True)

    def test_remaps_leaf_index_to_position_in_read_fields(self):
        from pypaimon.read.push_down_utils import rewrite_predicate_indices
        from pypaimon.schema.data_types import AtomicType, DataField

        pb = self._build_predicate()
        # Original schema position of 'c' is 2; after projection ['c', 'a']
        # the new position should be 0.
        leaf = pb.equal('c', 7)
        self.assertEqual(leaf.index, 2)

        read_fields = [
            DataField(0, 'c', AtomicType('BIGINT')),
            DataField(1, 'a', AtomicType('BIGINT')),
        ]
        rewritten = rewrite_predicate_indices(leaf, read_fields)
        self.assertEqual(rewritten.field, 'c')
        self.assertEqual(rewritten.index, 0)

    def test_remaps_inside_and_or(self):
        from pypaimon.read.push_down_utils import rewrite_predicate_indices
        from pypaimon.schema.data_types import AtomicType, DataField

        pb = self._build_predicate()
        p = pb.and_predicates([
            pb.equal('a', 1),
            pb.or_predicates([
                pb.equal('b', 'x'),
                pb.equal('c', 9),
            ]),
        ])

        # Project to ['c', 'b', 'a'] — every original index must change.
        read_fields = [
            DataField(0, 'c', AtomicType('BIGINT')),
            DataField(1, 'b', AtomicType('STRING')),
            DataField(2, 'a', AtomicType('BIGINT')),
        ]
        rewritten = rewrite_predicate_indices(p, read_fields)

        # Walk the tree to confirm every leaf points at the right position.
        and_top = rewritten
        eq_a = and_top.literals[0]
        self.assertEqual((eq_a.field, eq_a.index), ('a', 2))
        or_inner = and_top.literals[1]
        eq_b = or_inner.literals[0]
        eq_c = or_inner.literals[1]
        self.assertEqual((eq_b.field, eq_b.index), ('b', 1))
        self.assertEqual((eq_c.field, eq_c.index), ('c', 0))

    def test_returns_none_for_none_input(self):
        from pypaimon.read.push_down_utils import rewrite_predicate_indices

        self.assertIsNone(rewrite_predicate_indices(None, []))

    def test_raises_when_leaf_field_missing(self):
        from pypaimon.read.push_down_utils import rewrite_predicate_indices
        from pypaimon.schema.data_types import AtomicType, DataField

        pb = self._build_predicate()
        leaf = pb.equal('c', 7)

        # 'c' is not in the projection — the helper must surface a clear
        # error rather than silently dropping the leaf.
        with self.assertRaises(ValueError) as cm:
            rewrite_predicate_indices(
                leaf, [DataField(0, 'a', AtomicType('BIGINT'))]
            )
        self.assertIn('not in read fields', str(cm.exception))


if __name__ == '__main__':
    unittest.main()
