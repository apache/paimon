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

"""Integration tests for the ROW file format across all table types."""

import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema


class RowFormatAppendOnlyTest(unittest.TestCase):
    """Test ROW format with append-only (non-primary-key) tables."""

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
            self.pa_schema, options={'file.format': 'row'})
        self.catalog.create_table('default.ao_row_no_part', schema, False)
        table = self.catalog.get_table('default.ao_row_no_part')

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
            options={'file.format': 'row'})
        self.catalog.create_table('default.ao_row_partitioned', schema, False)
        table = self.catalog.get_table('default.ao_row_partitioned')

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
            self.pa_schema, options={'file.format': 'row'})
        self.catalog.create_table('default.ao_row_multi_commit', schema, False)
        table = self.catalog.get_table('default.ao_row_multi_commit')

        data1 = pa.Table.from_pydict({
            'user_id': [1, 2],
            'item_id': [1001, 1002],
            'behavior': ['buy', 'click'],
            'dt': ['2024-01-01', '2024-01-01'],
        }, schema=self.pa_schema)

        data2 = pa.Table.from_pydict({
            'user_id': [3, 4],
            'item_id': [1003, 1004],
            'behavior': ['view', 'buy'],
            'dt': ['2024-01-02', '2024-01-02'],
        }, schema=self.pa_schema)

        write_builder = table.new_batch_write_builder()

        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(data1)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(data2)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        result = read_builder.new_read().to_arrow(splits)
        self.assertEqual(result.num_rows, 4)
        self.assertEqual(sorted(result.column('user_id').to_pylist()), [1, 2, 3, 4])

    def test_append_only_with_nulls(self):
        schema = Schema.from_pyarrow_schema(
            self.pa_schema, options={'file.format': 'row'})
        self.catalog.create_table('default.ao_row_nulls', schema, False)
        table = self.catalog.get_table('default.ao_row_nulls')

        data = pa.Table.from_pydict({
            'user_id': [1, 2, 3],
            'item_id': [None, 1002, None],
            'behavior': ['buy', None, 'view'],
            'dt': ['2024-01-01', '2024-01-01', '2024-01-02'],
        }, schema=self.pa_schema)

        result = self._write_and_read(table, data)
        self.assertEqual(result.num_rows, 3)
        item_ids = sorted(result.column('item_id').to_pylist(), key=lambda x: (x is None, x))
        self.assertEqual(item_ids, [1002, None, None])

    def test_append_only_column_projection(self):
        """Test that reading with column projection decodes correctly."""
        schema = Schema.from_pyarrow_schema(
            self.pa_schema, options={'file.format': 'row'})
        self.catalog.create_table('default.ao_row_projection', schema, False)
        table = self.catalog.get_table('default.ao_row_projection')

        data = pa.Table.from_pydict({
            'user_id': [1, 2, 3],
            'item_id': [1001, 1002, 1003],
            'behavior': ['buy', 'click', 'view'],
            'dt': ['2024-01-01', '2024-01-01', '2024-01-02'],
        }, schema=self.pa_schema)

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # Read only (user_id, behavior) - skipping item_id and dt
        read_builder = table.new_read_builder()
        read_builder = read_builder.with_projection(['user_id', 'behavior'])
        splits = read_builder.new_scan().plan().splits()
        result = read_builder.new_read().to_arrow(splits)
        self.assertEqual(result.num_rows, 3)
        self.assertEqual(result.schema.names, ['user_id', 'behavior'])
        self.assertEqual(sorted(result.column('user_id').to_pylist()), [1, 2, 3])
        self.assertEqual(
            sorted(result.column('behavior').to_pylist()),
            ['buy', 'click', 'view'])

        # Read only (item_id, dt) - skipping user_id and behavior
        read_builder = table.new_read_builder()
        read_builder = read_builder.with_projection(['item_id', 'dt'])
        splits = read_builder.new_scan().plan().splits()
        result = read_builder.new_read().to_arrow(splits)
        self.assertEqual(result.num_rows, 3)
        self.assertEqual(result.schema.names, ['item_id', 'dt'])
        self.assertEqual(
            sorted(result.column('item_id').to_pylist()), [1001, 1002, 1003])


class RowFormatPrimaryKeyTest(unittest.TestCase):
    """Test ROW format with primary-key tables."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', True)

        cls.pa_schema = pa.schema([
            pa.field('user_id', pa.int32(), nullable=False),
            ('item_id', pa.int64()),
            ('behavior', pa.string()),
            pa.field('dt', pa.string(), nullable=False),
        ])

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def test_pk_table_basic(self):
        schema = Schema.from_pyarrow_schema(
            self.pa_schema,
            partition_keys=['dt'],
            primary_keys=['user_id', 'dt'],
            options={'bucket': '1', 'file.format': 'row'})
        self.catalog.create_table('default.pk_row_basic', schema, False)
        table = self.catalog.get_table('default.pk_row_basic')

        data = pa.Table.from_pydict({
            'user_id': [1, 2, 3],
            'item_id': [1001, 1002, 1003],
            'behavior': ['buy', 'click', 'view'],
            'dt': ['p1', 'p1', 'p2'],
        }, schema=self.pa_schema)

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        result = read_builder.new_read().to_arrow(splits)
        self.assertEqual(result.num_rows, 3)

    def test_pk_table_upsert(self):
        schema = Schema.from_pyarrow_schema(
            self.pa_schema,
            partition_keys=['dt'],
            primary_keys=['user_id', 'dt'],
            options={'bucket': '1', 'file.format': 'row'})
        self.catalog.create_table('default.pk_row_upsert', schema, False)
        table = self.catalog.get_table('default.pk_row_upsert')

        write_builder = table.new_batch_write_builder()

        # First commit
        data1 = pa.Table.from_pydict({
            'user_id': [1, 2, 3],
            'item_id': [1001, 1002, 1003],
            'behavior': ['buy', 'click', 'view'],
            'dt': ['p1', 'p1', 'p1'],
        }, schema=self.pa_schema)
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(data1)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # Second commit - update user_id=2
        data2 = pa.Table.from_pydict({
            'user_id': [2, 4],
            'item_id': [1002, 1004],
            'behavior': ['buy-updated', 'new'],
            'dt': ['p1', 'p1'],
        }, schema=self.pa_schema)
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(data2)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        result = read_builder.new_read().to_arrow(splits)
        self.assertEqual(result.num_rows, 4)
        result_dict = result.sort_by('user_id').to_pydict()
        self.assertEqual(result_dict['user_id'], [1, 2, 3, 4])
        self.assertEqual(result_dict['behavior'], ['buy', 'buy-updated', 'view', 'new'])


class RowFormatDataEvolutionTest(unittest.TestCase):
    """Test ROW format with data-evolution enabled tables."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', True)

        cls.pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('age', pa.int32()),
            ('city', pa.string()),
        ])

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def test_data_evolution_write_read(self):
        schema = Schema.from_pyarrow_schema(
            self.pa_schema,
            options={
                'file.format': 'row',
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true',
            })
        self.catalog.create_table('default.de_row_basic', schema, False)
        table = self.catalog.get_table('default.de_row_basic')

        data = pa.Table.from_pydict({
            'id': [1, 2, 3],
            'name': ['alice', 'bob', 'charlie'],
            'age': [25, 30, 35],
            'city': ['beijing', 'shanghai', 'guangzhou'],
        }, schema=self.pa_schema)

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        result = read_builder.new_read().to_arrow(splits)
        self.assertEqual(result.num_rows, 3)
        self.assertEqual(sorted(result.column('id').to_pylist()), [1, 2, 3])

    def test_data_evolution_multiple_commits(self):
        schema = Schema.from_pyarrow_schema(
            self.pa_schema,
            options={
                'file.format': 'row',
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true',
            })
        self.catalog.create_table('default.de_row_multi', schema, False)
        table = self.catalog.get_table('default.de_row_multi')

        write_builder = table.new_batch_write_builder()

        data1 = pa.Table.from_pydict({
            'id': [1, 2, 3],
            'name': ['alice', 'bob', 'charlie'],
            'age': [25, 30, 35],
            'city': ['beijing', 'shanghai', 'guangzhou'],
        }, schema=self.pa_schema)
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(data1)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        data2 = pa.Table.from_pydict({
            'id': [4, 5],
            'name': ['dave', 'eve'],
            'age': [40, 45],
            'city': ['shenzhen', 'hangzhou'],
        }, schema=self.pa_schema)
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(data2)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        result = read_builder.new_read().to_arrow(splits)
        self.assertEqual(result.num_rows, 5)
        self.assertEqual(sorted(result.column('id').to_pylist()), [1, 2, 3, 4, 5])


class RowFormatBlobTableTest(unittest.TestCase):
    """Test ROW format with blob tables (blob columns stored separately)."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', True)

        cls.pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('blob_data', pa.large_binary()),
        ])

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def test_blob_table_with_row_format(self):
        schema = Schema.from_pyarrow_schema(
            self.pa_schema,
            options={
                'file.format': 'row',
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true',
            })
        self.catalog.create_table('default.blob_row_basic', schema, False)
        table = self.catalog.get_table('default.blob_row_basic')

        data = pa.Table.from_pydict({
            'id': [1, 2, 3],
            'name': ['a', 'b', 'c'],
            'blob_data': [b'\x01\x02\x03', b'\x04\x05', b'\x06\x07\x08\x09'],
        }, schema=self.pa_schema)

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        result = read_builder.new_read().to_arrow(splits)
        self.assertEqual(result.num_rows, 3)
        self.assertEqual(sorted(result.column('id').to_pylist()), [1, 2, 3])


class RowFormatVectorTableTest(unittest.TestCase):
    """Test ROW format with vector tables (vector columns inline)."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', True)

        cls.pa_schema = pa.schema([
            ('id', pa.int64()),
            ('embed', pa.list_(pa.float32(), 3)),
        ])

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def test_vector_inline_with_row_format(self):
        """Vector stored inline in the same ROW file."""
        schema = Schema.from_pyarrow_schema(
            self.pa_schema,
            options={'file.format': 'row'})
        self.catalog.create_table('default.vec_row_inline', schema, False)
        table = self.catalog.get_table('default.vec_row_inline')

        data = pa.table({
            'id': pa.array([1, 2, 3], type=pa.int64()),
            'embed': pa.FixedSizeListArray.from_arrays(
                pa.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0], type=pa.float32()),
                3
            ),
        })

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        result = read_builder.new_read().to_arrow(splits)
        self.assertEqual(result.num_rows, 3)
        self.assertEqual(sorted(result.column('id').to_pylist()), [1, 2, 3])


if __name__ == '__main__':
    unittest.main()
