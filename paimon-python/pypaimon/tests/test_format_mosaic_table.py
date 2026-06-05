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

"""Integration tests for the Mosaic file format across table types."""

import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema


class MosaicFormatAppendOnlyTest(unittest.TestCase):
    """Test Mosaic format with append-only (non-primary-key) tables."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', True)

        cls.pa_schema = pa.schema([
            ('user_id', pa.int32()),
            ('item_id', pa.int64()),
            ('behavior', pa.string()),
            ('dt', pa.string()),
        ])

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _write_and_read(self, table, data):
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        return read_builder.new_read().to_arrow(splits)

    def test_append_only_no_partition(self):
        schema = Schema.from_pyarrow_schema(
            self.pa_schema, options={'file.format': 'mosaic'})
        self.catalog.create_table('default.ao_mosaic_no_part', schema, False)
        table = self.catalog.get_table('default.ao_mosaic_no_part')

        data = pa.Table.from_pydict({
            'user_id': [1, 2, 3],
            'item_id': [1001, 1002, 1003],
            'behavior': ['buy', 'click', 'view'],
            'dt': ['2024-01-01', '2024-01-01', '2024-01-02'],
        }, schema=self.pa_schema)

        result = self._write_and_read(table, data)
        self.assertEqual(result.num_rows, 3)
        self.assertEqual(sorted(result.column('user_id').to_pylist()), [1, 2, 3])

    def test_append_only_with_partition(self):
        schema = Schema.from_pyarrow_schema(
            self.pa_schema, partition_keys=['dt'],
            options={'file.format': 'mosaic'})
        self.catalog.create_table('default.ao_mosaic_partitioned', schema, False)
        table = self.catalog.get_table('default.ao_mosaic_partitioned')

        data = pa.Table.from_pydict({
            'user_id': [1, 2, 3, 4],
            'item_id': [1001, 1002, 1003, 1004],
            'behavior': ['buy', 'click', 'view', 'buy'],
            'dt': ['p1', 'p1', 'p2', 'p2'],
        }, schema=self.pa_schema)

        result = self._write_and_read(table, data)
        self.assertEqual(result.num_rows, 4)

    def test_append_only_multiple_commits(self):
        schema = Schema.from_pyarrow_schema(
            self.pa_schema, options={'file.format': 'mosaic'})
        self.catalog.create_table('default.ao_mosaic_multi', schema, False)
        table = self.catalog.get_table('default.ao_mosaic_multi')

        write_builder = table.new_batch_write_builder()

        for i in range(3):
            table_write = write_builder.new_write()
            table_commit = write_builder.new_commit()
            data = pa.Table.from_pydict({
                'user_id': [i * 10 + 1, i * 10 + 2],
                'item_id': [int(i * 100 + 1), int(i * 100 + 2)],
                'behavior': ['buy', 'click'],
                'dt': ['2024-01-01', '2024-01-01'],
            }, schema=self.pa_schema)
            table_write.write_arrow(data)
            table_commit.commit(table_write.prepare_commit())
            table_write.close()
            table_commit.close()

        table = self.catalog.get_table('default.ao_mosaic_multi')
        read_builder = table.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        result = read_builder.new_read().to_arrow(splits)
        self.assertEqual(result.num_rows, 6)


class MosaicFormatPrimaryKeyTest(unittest.TestCase):
    """Test Mosaic format with primary-key tables."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', True)

        cls.pa_schema = pa.schema([
            ('pk', pa.int32()),
            ('value', pa.string()),
            ('amount', pa.float64()),
        ])

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def test_primary_key_deduplicate(self):
        schema = Schema.from_pyarrow_schema(
            self.pa_schema, primary_keys=['pk'],
            options={'file.format': 'mosaic'})
        self.catalog.create_table('default.pk_mosaic_dedup', schema, False)
        table = self.catalog.get_table('default.pk_mosaic_dedup')

        write_builder = table.new_batch_write_builder()

        # First write
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data1 = pa.Table.from_pydict({
            'pk': [1, 2, 3],
            'value': ['a', 'b', 'c'],
            'amount': [1.0, 2.0, 3.0],
        }, schema=self.pa_schema)
        table_write.write_arrow(data1)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # Second write (update pk=2)
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data2 = pa.Table.from_pydict({
            'pk': [2],
            'value': ['updated'],
            'amount': [22.0],
        }, schema=self.pa_schema)
        table_write.write_arrow(data2)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        table = self.catalog.get_table('default.pk_mosaic_dedup')
        read_builder = table.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        result = read_builder.new_read().to_arrow(splits)

        self.assertEqual(result.num_rows, 3)
        result_sorted = result.sort_by('pk')
        self.assertEqual(result_sorted.column('value').to_pylist(), ['a', 'updated', 'c'])


if __name__ == '__main__':
    unittest.main()
