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

"""End-to-end tests for the ``first-row`` merge engine.

Each test creates a PK table with ``merge-engine`` set to ``first-row``,
writes one or more batches, and reads back. The first-row engine keeps
only the earliest row per primary key.
"""

import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema


class FirstRowMergeEngineE2ETest(unittest.TestCase):

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
        ])

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _create_pk_table(self, table_name, extra_options=None):
        options = {
            'bucket': '1',
            'merge-engine': 'first-row',
        }
        if extra_options:
            options.update(extra_options)
        schema = Schema.from_pyarrow_schema(
            self.pa_schema,
            primary_keys=['id'],
            options=options,
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

    def test_first_row_keeps_earliest(self):
        """Two writes with the same PK — first-row keeps the first one."""
        table = self._create_pk_table('first_row_basic')
        self._write(table, [{'id': 1, 'a': 'first', 'b': 'B1'}])
        self._write(table, [{'id': 1, 'a': 'second', 'b': 'B2'}])

        self.assertEqual(
            self._read(table),
            [{'id': 1, 'a': 'first', 'b': 'B1'}],
        )

    def test_first_row_multiple_keys(self):
        """Multiple PKs across two writes — each key keeps its first row."""
        table = self._create_pk_table('first_row_multi_key')
        self._write(table, [
            {'id': 1, 'a': 'A1', 'b': 'B1'},
            {'id': 2, 'a': 'A2', 'b': 'B2'},
        ])
        self._write(table, [
            {'id': 1, 'a': 'A1-new', 'b': 'B1-new'},
            {'id': 3, 'a': 'A3', 'b': 'B3'},
        ])

        self.assertEqual(
            self._read(table),
            [
                {'id': 1, 'a': 'A1', 'b': 'B1'},
                {'id': 2, 'a': 'A2', 'b': 'B2'},
                {'id': 3, 'a': 'A3', 'b': 'B3'},
            ],
        )

    def test_first_row_three_writes(self):
        """Three writes for the same PK — always the first one wins."""
        table = self._create_pk_table('first_row_three')
        self._write(table, [{'id': 1, 'a': 'first', 'b': None}])
        self._write(table, [{'id': 1, 'a': 'second', 'b': 'B'}])
        self._write(table, [{'id': 1, 'a': 'third', 'b': 'C'}])

        self.assertEqual(
            self._read(table),
            [{'id': 1, 'a': 'first', 'b': None}],
        )

    def test_first_row_single_write(self):
        """A single write should read back unchanged."""
        table = self._create_pk_table('first_row_single')
        self._write(table, [
            {'id': 1, 'a': 'A', 'b': 'B'},
            {'id': 2, 'a': 'C', 'b': 'D'},
        ])

        self.assertEqual(
            self._read(table),
            [
                {'id': 1, 'a': 'A', 'b': 'B'},
                {'id': 2, 'a': 'C', 'b': 'D'},
            ],
        )

    def test_first_row_intra_batch_duplicate(self):
        """A single write whose batch already contains duplicate PKs.

        The whole batch is folded in one flush, so this exercises the
        write-side fold rather than the cross-commit read merge. first-row
        must keep the first occurrence of each PK.
        """
        table = self._create_pk_table('first_row_intra_batch')
        self._write(table, [
            {'id': 1, 'a': 'first', 'b': 'B1'},
            {'id': 1, 'a': 'second', 'b': 'B2'},
            {'id': 1, 'a': 'third', 'b': 'B3'},
            {'id': 2, 'a': 'only', 'b': 'B'},
        ])

        self.assertEqual(
            self._read(table),
            [
                {'id': 1, 'a': 'first', 'b': 'B1'},
                {'id': 2, 'a': 'only', 'b': 'B'},
            ],
        )

    def test_first_row_multiple_writes_one_commit(self):
        """Several write_arrow calls committed once: the same PK across
        those writes folds in a single flush. first-row keeps the first.
        """
        table = self._create_pk_table('first_row_multi_write_one_commit')
        wb = table.new_batch_write_builder()
        w = wb.new_write()
        c = wb.new_commit()
        try:
            w.write_arrow(pa.Table.from_pylist(
                [{'id': 1, 'a': 'first', 'b': 'B1'}], schema=self.pa_schema))
            w.write_arrow(pa.Table.from_pylist(
                [{'id': 1, 'a': 'second', 'b': 'B2'}], schema=self.pa_schema))
            c.commit(w.prepare_commit())
        finally:
            w.close()
            c.close()

        self.assertEqual(
            self._read(table),
            [{'id': 1, 'a': 'first', 'b': 'B1'}],
        )


if __name__ == '__main__':
    unittest.main()
