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

"""End-to-end tests for the ``partial-update`` merge engine.

Each test creates a PK table with ``merge-engine`` set to a particular
value, writes one or more batches, and reads back. Partial-update reads
must merge non-null fields across batches; ``deduplicate`` must keep
the latest row only; ``aggregation`` and ``first-row`` must raise
``NotImplementedError`` (until they are ported), since silently
treating them as deduplicate would corrupt the user's data.
"""

import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema


class PartialUpdateMergeEngineE2ETest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', True)

        cls.pa_schema = pa.schema([
            pa.field('id', pa.int64(), nullable=False),
            ('a', pa.string()),
            ('b', pa.string()),
            ('c', pa.string()),
        ])

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _create_pk_table(self, table_name, merge_engine='partial-update'):
        # bucket=1 so all rows for any PK land in the same bucket; this is
        # what forces the read path through SortMergeReader instead of the
        # raw_convertible / single-file fast path. partial-update merging
        # only happens inside SortMergeReader.
        schema = Schema.from_pyarrow_schema(
            self.pa_schema,
            primary_keys=['id'],
            options={
                'bucket': '1',
                'merge-engine': merge_engine,
            },
        )
        full = 'default.{}'.format(table_name)
        self.catalog.create_table(full, schema, False)
        return self.catalog.get_table(full)

    def _write(self, table, rows):
        wb = table.new_batch_write_builder()
        w = wb.new_write()
        c = wb.new_commit()
        try:
            w.write_arrow(pa.Table.from_pylist(rows, schema=self.pa_schema))
            c.commit(w.prepare_commit())
        finally:
            w.close()
            c.close()

    def _read(self, table):
        rb = table.new_read_builder()
        splits = rb.new_scan().plan().splits()
        if not splits:
            return []
        return sorted(
            rb.new_read().to_arrow(splits).to_pylist(),
            key=lambda r: r['id'],
        )

    # -- partial-update happy path ---------------------------------------

    def test_partial_update_two_writes_merges_non_null(self):
        """Two writes against the same PK with disjoint non-null columns
        must merge into a single row that has both columns populated.
        """
        table = self._create_pk_table('two_writes')
        self._write(table, [{'id': 1, 'a': 'A', 'b': None, 'c': None}])
        self._write(table, [{'id': 1, 'a': None, 'b': 'B', 'c': None}])

        self.assertEqual(
            self._read(table),
            [{'id': 1, 'a': 'A', 'b': 'B', 'c': None}],
        )

    def test_partial_update_three_writes_merges_left_to_right(self):
        """Three overlapping writes — each filling in a different column —
        compose into the union of non-null fields.
        """
        table = self._create_pk_table('three_writes')
        self._write(table, [{'id': 1, 'a': 'A', 'b': None, 'c': None}])
        self._write(table, [{'id': 1, 'a': None, 'b': 'B', 'c': None}])
        self._write(table, [{'id': 1, 'a': None, 'b': None, 'c': 'C'}])

        self.assertEqual(
            self._read(table),
            [{'id': 1, 'a': 'A', 'b': 'B', 'c': 'C'}],
        )

    def test_partial_update_disjoint_keys_unaffected(self):
        """Three rows with disjoint PKs must all appear unchanged in the
        output — partial-update only merges rows that share a PK.
        """
        table = self._create_pk_table('disjoint_keys')
        self._write(table, [
            {'id': 1, 'a': 'A1', 'b': None, 'c': None},
            {'id': 2, 'a': None, 'b': 'B2', 'c': None},
            {'id': 3, 'a': None, 'b': None, 'c': 'C3'},
        ])

        self.assertEqual(
            self._read(table),
            [
                {'id': 1, 'a': 'A1', 'b': None, 'c': None},
                {'id': 2, 'a': None, 'b': 'B2', 'c': None},
                {'id': 3, 'a': None, 'b': None, 'c': 'C3'},
            ],
        )

    def test_partial_update_later_value_wins_over_earlier_non_null(self):
        """When two writes both supply a non-null value for the same
        column, the later value wins (latest non-null per field).
        """
        table = self._create_pk_table('later_wins')
        self._write(table, [{'id': 1, 'a': 'old', 'b': 'keep', 'c': None}])
        self._write(table, [{'id': 1, 'a': 'new', 'b': None, 'c': 'fill'}])

        self.assertEqual(
            self._read(table),
            [{'id': 1, 'a': 'new', 'b': 'keep', 'c': 'fill'}],
        )

    def test_partial_update_later_null_does_not_clobber_earlier_value(self):
        """A later write with NULL for a column does NOT overwrite an
        earlier non-null value for that column.
        """
        table = self._create_pk_table('null_no_clobber')
        self._write(table, [{'id': 1, 'a': 'A', 'b': 'B', 'c': 'C'}])
        self._write(table, [{'id': 1, 'a': None, 'b': None, 'c': None}])

        self.assertEqual(
            self._read(table),
            [{'id': 1, 'a': 'A', 'b': 'B', 'c': 'C'}],
        )

    # -- deduplicate (regression) ----------------------------------------

    def test_deduplicate_engine_unchanged(self):
        """The default ``deduplicate`` engine must keep the latest row
        intact, including its NULLs — exactly the pre-PR behaviour.
        """
        table = self._create_pk_table('dedupe', merge_engine='deduplicate')
        self._write(table, [{'id': 1, 'a': 'old', 'b': 'old-b', 'c': 'old-c'}])
        self._write(table, [{'id': 1, 'a': 'new', 'b': None, 'c': None}])

        self.assertEqual(
            self._read(table),
            [{'id': 1, 'a': 'new', 'b': None, 'c': None}],
        )

    # -- engines we don't support yet ------------------------------------

    def test_aggregation_engine_raises_not_implemented(self):
        """Until ``aggregation`` is ported, reading an aggregation table
        must raise rather than silently produce dedupe results."""
        table = self._create_pk_table('agg_unsupported',
                                      merge_engine='aggregation')
        self._write(table, [{'id': 1, 'a': 'x', 'b': None, 'c': None}])
        self._write(table, [{'id': 1, 'a': 'y', 'b': None, 'c': None}])

        rb = table.new_read_builder()
        splits = rb.new_scan().plan().splits()
        with self.assertRaises(NotImplementedError) as cm:
            rb.new_read().to_arrow(splits)
        self.assertIn('aggregation', str(cm.exception))

    def test_first_row_engine_raises_not_implemented(self):
        """Until ``first-row`` is ported, reading a first-row table must
        raise rather than silently produce dedupe results."""
        table = self._create_pk_table('first_row_unsupported',
                                      merge_engine='first-row')
        self._write(table, [{'id': 1, 'a': 'x', 'b': None, 'c': None}])
        self._write(table, [{'id': 1, 'a': 'y', 'b': None, 'c': None}])

        rb = table.new_read_builder()
        splits = rb.new_scan().plan().splits()
        with self.assertRaises(NotImplementedError) as cm:
            rb.new_read().to_arrow(splits)
        self.assertIn('first-row', str(cm.exception))


if __name__ == '__main__':
    unittest.main()
