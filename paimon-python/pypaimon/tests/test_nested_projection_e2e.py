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


class _AppendOnlyNestedBase(unittest.TestCase):
    """Append-only table whose ``mv`` column is a nested struct, used to
    exercise file-level Parquet/ORC pushdown of nested projection."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', False)

        cls.pa_schema = pa.schema([
            ('id', pa.int64()),
            ('mv', pa.struct([
                ('latest_version', pa.int64()),
                ('latest_value', pa.string()),
            ])),
            ('val', pa.string()),
        ])
        cls.rows = [
            {'id': 1, 'mv': {'latest_version': 100, 'latest_value': 'a'}, 'val': 'x'},
            {'id': 2, 'mv': {'latest_version': 200, 'latest_value': 'b'}, 'val': 'y'},
            {'id': 3, 'mv': {'latest_version': 300, 'latest_value': 'c'}, 'val': 'z'},
        ]

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _create_table(self, name: str, file_format: str = 'parquet'):
        identifier = 'default.{}'.format(name)
        schema = Schema.from_pyarrow_schema(
            self.pa_schema,
            options={'bucket': '-1', 'file.format': file_format},
        )
        self.catalog.create_table(identifier, schema, False)
        table = self.catalog.get_table(identifier)
        wb = table.new_batch_write_builder()
        w = wb.new_write()
        w.write_arrow(pa.Table.from_pylist(self.rows, schema=self.pa_schema))
        wb.new_commit().commit(w.prepare_commit())
        w.close()
        return table


class AppendOnlyNestedParquetTest(_AppendOnlyNestedBase):
    """Parquet path uses PyArrow's dict-form scanner with ``ds.field(*path)``
    to push the nested column read into the engine."""

    def test_dotted_name_returns_just_the_leaf(self):
        table = self._create_table('ao_dotted_leaf')
        rb = table.new_read_builder().with_projection(['mv.latest_version'])
        got = rb.new_read().to_arrow(rb.new_scan().plan().splits()).to_pylist()
        self.assertEqual(
            got,
            [{'mv_latest_version': 100},
             {'mv_latest_version': 200},
             {'mv_latest_version': 300}])

    def test_mixed_nested_and_top_level_preserves_order(self):
        table = self._create_table('ao_mixed_order')
        rb = table.new_read_builder().with_projection(
            ['mv.latest_version', 'val', 'mv.latest_value'])
        got = rb.new_read().to_arrow(rb.new_scan().plan().splits()).to_pylist()
        self.assertEqual(
            got,
            [{'mv_latest_version': 100, 'val': 'x', 'mv_latest_value': 'a'},
             {'mv_latest_version': 200, 'val': 'y', 'mv_latest_value': 'b'},
             {'mv_latest_version': 300, 'val': 'z', 'mv_latest_value': 'c'}])

    def test_top_level_only_projection_unchanged(self):
        """A projection without dots must keep the existing top-level
        path — file-level pushdown still asks for plain column names,
        no dict-form scanner."""
        table = self._create_table('ao_top_level_unchanged')
        rb = table.new_read_builder().with_projection(['val', 'id'])
        got = rb.new_read().to_arrow(rb.new_scan().plan().splits()).to_pylist()
        self.assertEqual(
            got,
            [{'val': 'x', 'id': 1},
             {'val': 'y', 'id': 2},
             {'val': 'z', 'id': 3}])

    def test_partitioned_table_with_nested_projection(self):
        """Partition-aware reads have a separate path-mapping helper from
        the non-partitioned fast path; regress the case where it dropped
        non-nested top-level columns alongside the projected leaf."""
        identifier = 'default.ao_partitioned'
        pa_schema = pa.schema([
            ('part', pa.string()),
            ('id', pa.int64()),
            ('mv', pa.struct([
                ('latest_version', pa.int64()),
                ('latest_value', pa.string()),
            ])),
            ('val', pa.string()),
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            partition_keys=['part'],
            options={'bucket': '-1', 'file.format': 'parquet'},
        )
        self.catalog.create_table(identifier, schema, False)
        table = self.catalog.get_table(identifier)
        wb = table.new_batch_write_builder()
        w = wb.new_write()
        w.write_arrow(pa.Table.from_pylist([
            {'part': 'A', 'id': 1, 'mv': {'latest_version': 100, 'latest_value': 'a'}, 'val': 'x'},
            {'part': 'B', 'id': 2, 'mv': {'latest_version': 200, 'latest_value': 'b'}, 'val': 'y'},
        ], schema=pa_schema))
        wb.new_commit().commit(w.prepare_commit())
        w.close()

        # Mixed projection: nested leaf, a non-partition top-level column,
        # and the partition column itself.
        rb = table.new_read_builder().with_projection(
            ['part', 'mv.latest_version', 'val'])
        got = rb.new_read().to_arrow(rb.new_scan().plan().splits()).to_pylist()
        got_sorted = sorted(got, key=lambda r: r['part'])
        self.assertEqual(
            got_sorted,
            [{'part': 'A', 'mv_latest_version': 100, 'val': 'x'},
             {'part': 'B', 'mv_latest_version': 200, 'val': 'y'}])

    def test_avro_nested_projection_python_fallback(self):
        """Avro has no native nested column pruning; the reader walks
        each fastavro record dict by path and assembles the column
        client-side."""
        table = self._create_table('ao_avro_nested', file_format='avro')
        rb = table.new_read_builder().with_projection(['mv.latest_version', 'val'])
        got = rb.new_read().to_arrow(rb.new_scan().plan().splits()).to_pylist()
        self.assertEqual(
            got,
            [{'mv_latest_version': 100, 'val': 'x'},
             {'mv_latest_version': 200, 'val': 'y'},
             {'mv_latest_version': 300, 'val': 'z'}])

    def test_avro_top_level_projection_unchanged(self):
        """Top-level-only projection on Avro stays on the existing
        ``record.get(name)`` fast path."""
        table = self._create_table('ao_avro_top', file_format='avro')
        rb = table.new_read_builder().with_projection(['val', 'id'])
        got = rb.new_read().to_arrow(rb.new_scan().plan().splits()).to_pylist()
        self.assertEqual(
            got,
            [{'val': 'x', 'id': 1},
             {'val': 'y', 'id': 2},
             {'val': 'z', 'id': 3}])

    def test_pk_table_merge_split_with_nested_projection_raises(self):
        # Phase 2b lands the append-only path only; PK + nested needs an
        # outer-projection wrapper that ships in a follow-up commit. Until
        # then, the call must refuse loudly rather than silently corrupt
        # the merge function input. Two commits on the same PK force the
        # split out of the raw-convertible fast path into the merge
        # reader, which is where the guard lives.
        identifier = 'default.pk_nested_unsupported'
        schema = Schema.from_pyarrow_schema(
            self.pa_schema,
            primary_keys=['id'],
            options={'bucket': '1', 'file.format': 'parquet'},
        )
        self.catalog.create_table(identifier, schema, False)
        table = self.catalog.get_table(identifier)
        for batch in (self.rows, self.rows):  # two overlapping commits
            wb = table.new_batch_write_builder()
            w = wb.new_write()
            w.write_arrow(pa.Table.from_pylist(batch, schema=self.pa_schema))
            wb.new_commit().commit(w.prepare_commit())
            w.close()

        rb = table.new_read_builder().with_projection(['mv.latest_version'])
        splits = rb.new_scan().plan().splits()
        # ``to_arrow`` materialises the split read; the merge path is what
        # raises, so do it eagerly here rather than waiting for the first
        # batch.
        with self.assertRaises(NotImplementedError):
            rb.new_read().to_arrow(splits)


if __name__ == '__main__':
    unittest.main()
