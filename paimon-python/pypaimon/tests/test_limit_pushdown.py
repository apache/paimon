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

"""End-to-end coverage for ``with_limit`` after row-level pushdown.

Locks the contract: ``with_limit(N)`` returns at most ``N`` rows, and
the reader actually stops at that boundary instead of reading every
split / merge output to completion and trimming at the consumer.
"""

import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema


class LimitPushdownTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog_options = {'warehouse': cls.warehouse}
        cls.catalog = CatalogFactory.create(cls.catalog_options)
        cls.catalog.create_database('default', False)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    @staticmethod
    def _ao_schema() -> pa.Schema:
        return pa.schema([
            pa.field('id', pa.int64(), nullable=False),
            ('val', pa.int64()),
            pa.field('dt', pa.string(), nullable=False),
        ])

    @staticmethod
    def _pk_schema() -> pa.Schema:
        return pa.schema([
            pa.field('id', pa.int64(), nullable=False),
            ('val', pa.int64()),
        ])

    def _create_ao_table(self, name: str):
        identifier = 'default.' + name
        schema = Schema.from_pyarrow_schema(
            self._ao_schema(),
            partition_keys=['dt'],
            options={'file.format': 'parquet'},
        )
        self.catalog.create_table(identifier, schema, False)
        return self.catalog.get_table(identifier)

    def _create_pk_table(self, name: str, *, num_buckets: int = 1):
        identifier = 'default.' + name
        schema = Schema.from_pyarrow_schema(
            self._pk_schema(),
            primary_keys=['id'],
            options={'bucket': str(num_buckets), 'file.format': 'parquet'},
        )
        self.catalog.create_table(identifier, schema, False)
        return self.catalog.get_table(identifier)

    def _write_ao_partitions(self, table, partitions):
        for dt, rows in partitions:
            wb = table.new_batch_write_builder()
            w = wb.new_write()
            data = pa.Table.from_pylist(
                [{'id': r, 'val': r * 10, 'dt': dt} for r in rows],
                schema=self._ao_schema())
            w.write_arrow(data)
            wb.new_commit().commit(w.prepare_commit())
            w.close()

    def _write_pk_snapshots(self, table, snapshots):
        for rows in snapshots:
            wb = table.new_batch_write_builder()
            w = wb.new_write()
            data = pa.Table.from_pylist(
                [{'id': i, 'val': v} for i, v in rows], schema=self._pk_schema())
            w.write_arrow(data)
            wb.new_commit().commit(w.prepare_commit())
            w.close()

    # ---- append-only -----------------------------------------------------

    def test_append_only_limit_stops_within_first_split(self):
        """With limit=3 on a partitioned append-only table, the result is
        exactly 3 rows — even though each partition split has 5 rows."""
        table = self._create_ao_table('limit_ao_within_split')
        self._write_ao_partitions(table, [
            ('p1', list(range(5))),       # 5 rows
            ('p2', list(range(5, 10))),   # 5 rows
        ])
        rb = table.new_read_builder().with_limit(3)
        result = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        self.assertEqual(result.num_rows, 3)

    def test_append_only_limit_spans_multiple_splits(self):
        """Limit larger than first split: read carries over to the next
        split until the budget is met."""
        table = self._create_ao_table('limit_ao_span_splits')
        self._write_ao_partitions(table, [
            ('p1', [1, 2]),
            ('p2', [3, 4]),
            ('p3', [5, 6]),
        ])
        rb = table.new_read_builder().with_limit(5)
        result = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        self.assertEqual(result.num_rows, 5)

    def test_append_only_limit_zero_returns_empty(self):
        table = self._create_ao_table('limit_ao_zero')
        self._write_ao_partitions(table, [('p1', [1, 2, 3])])
        rb = table.new_read_builder().with_limit(0)
        splits = rb.new_scan().plan().splits()
        result = rb.new_read().to_arrow(splits)
        self.assertEqual(result.num_rows, 0)

    def test_append_only_limit_larger_than_total(self):
        """Limit greater than the total returns the total, not the limit."""
        table = self._create_ao_table('limit_ao_oversize')
        self._write_ao_partitions(table, [('p1', [1, 2, 3])])
        rb = table.new_read_builder().with_limit(100)
        result = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        self.assertEqual(result.num_rows, 3)

    # ---- PK merge-on-read ------------------------------------------------

    def test_pk_merge_limit_stops_within_first_split(self):
        """PK + multiple snapshots forces the merge-read path. The reader
        must stop at limit rows instead of running every section to
        completion and trimming at the consumer."""
        table = self._create_pk_table('limit_pk_within_split')
        # Two snapshots over the same key range → merge path; total
        # post-merge unique rows = 20.
        self._write_pk_snapshots(table, [
            [(i, i) for i in range(20)],
            [(i, i + 1000) for i in range(0, 20, 2)],
        ])
        for limit in (1, 5, 10, 19):
            rb = table.new_read_builder().with_limit(limit)
            result = rb.new_read().to_arrow(rb.new_scan().plan().splits())
            self.assertEqual(
                result.num_rows, limit,
                "with_limit(%d) must short-circuit at the row level" % limit)

    def test_pk_merge_limit_equals_total(self):
        """Limit equal to total post-merge row count: returns everything."""
        table = self._create_pk_table('limit_pk_equals_total')
        self._write_pk_snapshots(table, [
            [(i, i) for i in range(10)],
            [(i, i + 100) for i in range(5)],
        ])
        rb = table.new_read_builder().with_limit(10)
        result = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        self.assertEqual(result.num_rows, 10)

    def test_pk_merge_limit_with_predicate(self):
        """``with_limit`` plus ``with_filter``: the filter prunes first and
        the limit caps what survives. ``val >= 1000`` matches the latest
        write of the even ``id`` rows; limit then takes the prefix."""
        table = self._create_pk_table('limit_pk_with_filter')
        self._write_pk_snapshots(table, [
            [(i, i) for i in range(20)],
            [(i, i + 1000) for i in range(0, 20, 2)],  # update evens
        ])
        rb = table.new_read_builder()
        pred = rb.new_predicate_builder().greater_or_equal('val', 1000)
        rb = rb.with_filter(pred).with_limit(3)
        result = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        self.assertEqual(result.num_rows, 3)
        for v in result.column('val').to_pylist():
            self.assertGreaterEqual(v, 1000)

    # ---- to_iterator path ------------------------------------------------

    def test_to_iterator_limit_short_circuits(self):
        table = self._create_ao_table('limit_iter')
        self._write_ao_partitions(table, [
            ('p1', list(range(50))),
            ('p2', list(range(50, 100))),
        ])
        rb = table.new_read_builder().with_limit(7)
        it = rb.new_read().to_iterator(rb.new_scan().plan().splits())
        rows = list(it)
        self.assertEqual(len(rows), 7)


if __name__ == '__main__':
    unittest.main()
