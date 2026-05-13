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


class PrimaryKeyNestedTest(_AppendOnlyNestedBase):
    """PK tables go through the merge reader once a split is no longer
    raw-convertible (multiple overlapping commits on the same key). The
    merge function still needs full ROW sub-structures, so the read
    splits inner = full-ROW from outer = flat sub-paths via an
    OuterProjectionRecordReader."""

    def _create_pk_table(self, name: str, file_format: str = 'parquet'):
        identifier = 'default.{}'.format(name)
        schema = Schema.from_pyarrow_schema(
            self.pa_schema,
            primary_keys=['id'],
            options={'bucket': '1', 'file.format': file_format},
        )
        self.catalog.create_table(identifier, schema, False)
        table = self.catalog.get_table(identifier)
        # Two overlapping commits force the split off the raw-convertible
        # fast path into the merge reader.
        for batch in (self.rows, self.rows):
            wb = table.new_batch_write_builder()
            w = wb.new_write()
            w.write_arrow(pa.Table.from_pylist(batch, schema=self.pa_schema))
            wb.new_commit().commit(w.prepare_commit())
            w.close()
        return table

    def _read_arrow(self, table, projection):
        rb = table.new_read_builder().with_projection(projection)
        splits = rb.new_scan().plan().splits()
        return rb.new_read().to_arrow(splits)

    def test_extracts_single_nested_leaf(self):
        table = self._create_pk_table('pk_nested_single')
        arrow = self._read_arrow(table, ['mv.latest_version'])
        self.assertEqual(arrow.column_names, ['mv_latest_version'])
        versions = sorted(arrow.column('mv_latest_version').to_pylist())
        self.assertEqual(versions, [100, 200, 300])

    def test_multiple_sub_paths_under_same_struct(self):
        table = self._create_pk_table('pk_nested_double')
        arrow = self._read_arrow(
            table, ['mv.latest_version', 'mv.latest_value'])
        self.assertEqual(arrow.column_names, ['mv_latest_version', 'mv_latest_value'])
        pairs = sorted(zip(
            arrow.column('mv_latest_version').to_pylist(),
            arrow.column('mv_latest_value').to_pylist()))
        self.assertEqual(pairs, [(100, 'a'), (200, 'b'), (300, 'c')])

    def test_mixed_nested_and_top_level_preserves_order(self):
        table = self._create_pk_table('pk_nested_mixed')
        arrow = self._read_arrow(
            table, ['id', 'mv.latest_version', 'val'])
        self.assertEqual(
            arrow.column_names, ['id', 'mv_latest_version', 'val'])
        rows = sorted(zip(
            arrow.column('id').to_pylist(),
            arrow.column('mv_latest_version').to_pylist(),
            arrow.column('val').to_pylist()))
        self.assertEqual(rows, [(1, 100, 'x'), (2, 200, 'y'), (3, 300, 'z')])

    def test_avro_extracts_single_nested_leaf(self):
        # Avro PK reads resolve DataFields through ``full_fields_map`` which
        # historically only covered merge-internal aliases; without the
        # alias-safe fix, this projection would raise ``KeyError: 'id'``.
        table = self._create_pk_table('pk_avro_nested_single', file_format='avro')
        arrow = self._read_arrow(table, ['mv.latest_version'])
        self.assertEqual(arrow.column_names, ['mv_latest_version'])
        versions = sorted(arrow.column('mv_latest_version').to_pylist())
        self.assertEqual(versions, [100, 200, 300])


if __name__ == '__main__':
    unittest.main()
