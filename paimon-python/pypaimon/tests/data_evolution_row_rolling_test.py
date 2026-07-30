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
import uuid

import pyarrow as pa

from pypaimon import CatalogFactory, Schema


class DataEvolutionRowRollingTest(unittest.TestCase):
    """Row-count based data file rolling (target-file-row-num) for
    data-evolution append tables."""

    pa_schema = pa.schema([
        ('id', pa.int32()),
        ('name', pa.string()),
    ])
    de_options = {
        'row-tracking.enabled': 'true',
        'data-evolution.enabled': 'true',
    }

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.catalog = CatalogFactory.create(
            {'warehouse': os.path.join(cls.tempdir, 'warehouse')})
        cls.catalog.create_database('default', True)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _create(self, options):
        name = f'default.roll_{uuid.uuid4().hex[:8]}'
        self.catalog.create_table(
            name, Schema.from_pyarrow_schema(self.pa_schema, options=options),
            False)
        return self.catalog.get_table(name)

    def _rows(self, n):
        return pa.Table.from_pydict(
            {'id': list(range(n)), 'name': [f'n{i}' for i in range(n)]},
            schema=self.pa_schema)

    def _write_files(self, table, data):
        """Write one Arrow table and return the committed DataFileMeta list."""
        wb = table.new_batch_write_builder()
        tw = wb.new_write()
        tw.write_arrow(data)
        msgs = tw.prepare_commit()
        files = [f for m in msgs for f in m.new_files]
        wb.new_commit().commit(msgs)
        tw.close()
        return files

    def _read_ids(self, table):
        rb = table.new_read_builder()
        return sorted(
            rb.new_read().to_arrow(rb.new_scan().plan().splits())
            ['id'].to_pylist())

    def test_rolls_when_row_count_exceeds_limit(self):
        table = self._create({**self.de_options, 'target-file-row-num': '3'})
        files = self._write_files(table, self._rows(10))
        # 10 rows, limit 3 -> 3 full files + a 1-row remainder.
        self.assertEqual([1, 3, 3, 3], sorted(f.row_count for f in files))
        self.assertEqual(list(range(10)), self._read_ids(table))

    def test_exact_multiple_rolls_evenly(self):
        table = self._create({**self.de_options, 'target-file-row-num': '3'})
        files = self._write_files(table, self._rows(6))
        self.assertEqual([3, 3], sorted(f.row_count for f in files))
        self.assertEqual(list(range(6)), self._read_ids(table))

    def test_below_limit_is_single_file(self):
        table = self._create({**self.de_options, 'target-file-row-num': '100'})
        files = self._write_files(table, self._rows(10))
        self.assertEqual([10], [f.row_count for f in files])

    def test_unset_option_does_not_roll_by_rows(self):
        table = self._create(self.de_options)
        files = self._write_files(table, self._rows(50))
        # No row limit -> a small batch stays one file (size rolling only).
        self.assertEqual([50], [f.row_count for f in files])

    def test_oversized_row_rolls_by_itself(self):
        # Each row exceeds target-file-size: the size trigger rolls every row by
        # itself even though target-file-row-num is larger.
        table = self._create({
            **self.de_options,
            'target-file-row-num': '3',
            'target-file-size': '100 b',
        })
        big = 'x' * 500
        data = pa.Table.from_pydict(
            {'id': list(range(4)), 'name': [big] * 4}, schema=self.pa_schema)
        files = self._write_files(table, data)
        self.assertEqual([1, 1, 1, 1], [f.row_count for f in files])
        self.assertEqual(list(range(4)), self._read_ids(table))

    def test_non_de_table_still_fails_fast(self):
        table = self._create({'target-file-row-num': '3'})
        wb = table.new_batch_write_builder()
        tw = wb.new_write()
        with self.assertRaisesRegex(
                NotImplementedError, 'row-count based file rolling'):
            tw.write_arrow(self._rows(4))


if __name__ == '__main__':
    unittest.main()
