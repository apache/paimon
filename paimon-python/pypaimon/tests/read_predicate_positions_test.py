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
from importlib.util import find_spec

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.common.predicate_builder import PredicateBuilder

HAS_VORTEX = find_spec('vortex') is not None

PA_SCHEMA = pa.schema([('a', pa.int32()), ('b', pa.string())])


class ReadPredicatePositionsTest(unittest.TestCase):
    """Shard ranges and _ROW_ID values are counted on the rows a format reader
    returns, so a filter must not be applied inside the format reader for
    those reads; it is applied after the rows are selected."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix='unittest_read_pred_pos_')

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _table(self, name, options):
        catalog = CatalogFactory.create(
            {'warehouse': os.path.join(self.temp_dir, 'warehouse')})
        catalog.create_database('default', True)
        ident = f'default.{name}'
        catalog.create_table(ident, Schema.from_pyarrow_schema(PA_SCHEMA, options=options), False)
        table = catalog.get_table(ident)
        wb = table.new_batch_write_builder()
        w = wb.new_write()
        w.write_arrow(pa.Table.from_pydict(
            {'a': list(range(10)), 'b': [f'row{i}' for i in range(10)]},
            schema=PA_SCHEMA))
        wb.new_commit().commit(w.prepare_commit())
        w.close()
        return table

    def _read(self, rb, scan):
        reader = rb.new_read()
        return reader.to_arrow(scan.plan().splits())

    def test_shard_with_predicate_returns_only_shard_rows(self):
        for file_format in ('parquet', 'orc', 'avro'):
            with self.subTest(file_format=file_format):
                table = self._table(
                    f'shard_pred_{file_format}', {'file.format': file_format})
                predicate = PredicateBuilder(table.schema().fields).is_in('a', [2, 8])
                rb = table.new_read_builder().with_filter(predicate)

                # The file's rows 0-4 belong to shard 0 and rows 5-9 to shard 1.
                shard0 = self._read(rb, rb.new_scan().with_shard(0, 2))
                self.assertEqual([2], shard0.column('a').to_pylist())
                shard1 = self._read(rb, rb.new_scan().with_shard(1, 2))
                self.assertEqual([8], shard1.column('a').to_pylist())

                result = self._read(rb, rb.new_scan().with_slice(0, 5))
                # row 8 lies outside the slice [0, 5)
                self.assertEqual([2], result.column('a').to_pylist())

    def test_row_tracking_predicate_keeps_physical_row_ids(self):
        for file_format in ('parquet', 'orc', 'avro'):
            with self.subTest(file_format=file_format):
                table = self._table(
                    f'rt_pred_{file_format}',
                    {'row-tracking.enabled': 'true', 'file.format': file_format})
                predicate = PredicateBuilder(table.schema().fields).greater_or_equal('a', 5)
                rb = (table.new_read_builder().with_filter(predicate)
                      .with_projection(['a', 'b', '_ROW_ID']))
                result = self._read(rb, rb.new_scan())
                self.assertEqual([5, 6, 7, 8, 9], result.column('a').to_pylist())
                self.assertEqual([5, 6, 7, 8, 9], result.column('_ROW_ID').to_pylist())

    @unittest.skipUnless(HAS_VORTEX, "vortex package not installed")
    def test_vortex_shard_predicate_keeps_row_ids(self):
        table = self._table(
            'vortex_shard', {'row-tracking.enabled': 'true', 'file.format': 'vortex'})
        predicate = PredicateBuilder(table.schema().fields).greater_or_equal('a', 3)
        rb = (table.new_read_builder().with_filter(predicate)
              .with_projection(['a', 'b', '_ROW_ID']))

        shard0 = self._read(rb, rb.new_scan().with_shard(0, 2))
        self.assertEqual([3, 4], shard0.column('a').to_pylist())
        self.assertEqual([3, 4], shard0.column('_ROW_ID').to_pylist())

        shard1 = self._read(rb, rb.new_scan().with_shard(1, 2))
        self.assertEqual([5, 6, 7, 8, 9], shard1.column('a').to_pylist())
        self.assertEqual([5, 6, 7, 8, 9], shard1.column('_ROW_ID').to_pylist())


if __name__ == '__main__':
    unittest.main()
