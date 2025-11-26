"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import glob
import os
import shutil

import tempfile
import unittest

from pypaimon import CatalogFactory, Schema
import pyarrow as pa


class TableWriteTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({
            'warehouse': cls.warehouse
        })
        cls.catalog.create_database('default', True)

        cls.pa_schema = pa.schema([
            ('user_id', pa.int32()),
            ('item_id', pa.int64()),
            ('behavior', pa.string()),
            ('dt', pa.string())
        ])
        cls.expected = pa.Table.from_pydict({
            'user_id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            'item_id': [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010],
            'behavior': ['a', 'b', 'c', None, 'e', 'f', 'g', 'h', 'i', 'j'],
            'dt': ['p1', 'p1', 'p2', 'p1', 'p2', 'p1', 'p2', 'p2', 'p2', 'p1']
        }, schema=cls.pa_schema)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def test_multi_prepare_commit_ao(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.catalog.create_table('default.test_append_only_parquet', schema, False)
        table = self.catalog.get_table('default.test_append_only_parquet')
        write_builder = table.new_stream_write_builder()

        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        # write 1
        data1 = {
            'user_id': [1, 2, 3, 4],
            'item_id': [1001, 1002, 1003, 1004],
            'behavior': ['a', 'b', 'c', None],
            'dt': ['p1', 'p1', 'p2', 'p1'],
        }
        pa_table = pa.Table.from_pydict(data1, schema=self.pa_schema)
        table_write.write_arrow(pa_table)
        table_write.prepare_commit(0)
        # write 2
        data2 = {
            'user_id': [5, 6, 7, 8],
            'item_id': [1005, 1006, 1007, 1008],
            'behavior': ['e', 'f', 'g', 'h'],
            'dt': ['p2', 'p1', 'p2', 'p2'],
        }
        pa_table = pa.Table.from_pydict(data2, schema=self.pa_schema)
        table_write.write_arrow(pa_table)
        table_write.prepare_commit(1)
        # write 3
        data3 = {
            'user_id': [9, 10],
            'item_id': [1009, 1010],
            'behavior': ['i', 'j'],
            'dt': ['p2', 'p1'],
        }
        pa_table = pa.Table.from_pydict(data3, schema=self.pa_schema)
        table_write.write_arrow(pa_table)
        cm = table_write.prepare_commit(2)
        # commit
        table_commit.commit(cm, 2)
        table_write.close()
        table_commit.close()
        self.assertEqual(2, table_write.file_store_write.commit_identifier)

        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        actual = table_read.to_arrow(splits).sort_by('user_id')
        self.assertEqual(self.expected, actual)

    def test_multi_prepare_commit_pk(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'], primary_keys=['user_id', 'dt'],
                                            options={'bucket': '2'})
        self.catalog.create_table('default.test_primary_key_parquet', schema, False)
        table = self.catalog.get_table('default.test_primary_key_parquet')
        write_builder = table.new_stream_write_builder()

        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        # write 1
        data1 = {
            'user_id': [1, 2, 3, 4],
            'item_id': [1001, 1002, 1003, 1004],
            'behavior': ['a', 'b', 'c', None],
            'dt': ['p1', 'p1', 'p2', 'p1'],
        }
        pa_table = pa.Table.from_pydict(data1, schema=self.pa_schema)
        table_write.write_arrow(pa_table)
        table_write.prepare_commit(0)
        # write 2
        data2 = {
            'user_id': [5, 6, 7, 8],
            'item_id': [1005, 1006, 1007, 1008],
            'behavior': ['e', 'f', 'g', 'h'],
            'dt': ['p2', 'p1', 'p2', 'p2'],
        }
        pa_table = pa.Table.from_pydict(data2, schema=self.pa_schema)
        table_write.write_arrow(pa_table)
        table_write.prepare_commit(1)
        # write 3
        data3 = {
            'user_id': [9, 10],
            'item_id': [1009, 1010],
            'behavior': ['i', 'j'],
            'dt': ['p2', 'p1'],
        }
        pa_table = pa.Table.from_pydict(data3, schema=self.pa_schema)
        table_write.write_arrow(pa_table)
        cm = table_write.prepare_commit(2)
        # commit
        table_commit.commit(cm, 2)
        table_write.close()
        table_commit.close()
        self.assertEqual(2, table_write.file_store_write.commit_identifier)

        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        actual = table_read.to_arrow(splits).sort_by('user_id')
        self.assertEqual(self.expected, actual)

    def test_postpone_read_write(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['user_id'], primary_keys=['user_id', 'dt'],
                                            options={'bucket': -2})
        self.catalog.create_table('default.test_postpone', schema, False)
        table = self.catalog.get_table('default.test_postpone')
        data = {
            'user_id': [1, 2, 3, 4],
            'item_id': [1001, 1002, 1003, 1004],
            'behavior': ['a', 'b', 'c', None],
            'dt': ['p1', 'p1', 'p2', 'p1'],
        }
        expect = pa.Table.from_pydict(data, schema=self.pa_schema)

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(expect)
        commit_messages = table_write.prepare_commit()
        table_commit.commit(commit_messages)
        table_write.close()
        table_commit.close()

        self.assertTrue(os.path.exists(self.warehouse + "/default.db/test_postpone/snapshot/LATEST"))
        self.assertTrue(os.path.exists(self.warehouse + "/default.db/test_postpone/snapshot/snapshot-1"))
        self.assertTrue(os.path.exists(self.warehouse + "/default.db/test_postpone/manifest"))
        self.assertEqual(len(glob.glob(self.warehouse + "/default.db/test_postpone/manifest/*")), 3)
        self.assertEqual(len(glob.glob(self.warehouse + "/default.db/test_postpone/user_id=2/bucket-postpone/*.avro")),
                         1)
        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        actual = table_read.to_arrow(splits)
        self.assertTrue(not actual)

    def test_data_file_prefix_postpone(self):
        """Test that generated data file names follow the expected prefix format."""
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['user_id'], primary_keys=['user_id', 'dt'],
                                            options={'bucket': -2})
        self.catalog.create_table('default.test_file_prefix_postpone', schema, False)
        table = self.catalog.get_table('default.test_file_prefix_postpone')

        # Write some data to generate files
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        data = {
            'user_id': [1, 2],
            'item_id': [1001, 1002],
            'behavior': ['a', 'b'],
            'dt': ['p1', 'p1'],
        }
        pa_table = pa.Table.from_pydict(data, schema=self.pa_schema)
        table_write.write_arrow(pa_table)

        commit_messages = table_write.prepare_commit()
        table_commit.commit(commit_messages)
        table_write.close()
        table_commit.close()

        # Find generated data files
        table_path = os.path.join(self.warehouse, 'default.db', 'test_file_prefix_postpone')
        data_files = []
        for root, dirs, files in os.walk(table_path):
            for file in files:
                if file.endswith('.parquet') or file.endswith('.avro') or file.endswith('.orc'):
                    data_files.append(file)

        # Verify at least one data file was created
        self.assertGreater(len(data_files), 0, "No data files were generated")

        # Verify file name format: {table_prefix}-u-{commit_user}-s-{random_number}-w--{uuid}-0.{format}
        # Expected pattern: data--u-{user}-s-{random}-w--{uuid}-0.{format}
        expected_pattern = r'^data--u-.+-s-\d+-w-.+-0\.avro$'

        for file_name in data_files:
            self.assertRegex(file_name, expected_pattern,
                             f"File name '{file_name}' does not match expected prefix format")

            # Additional checks for specific components
            parts = file_name.split('-')
            self.assertEqual('data', parts[0], f"File prefix should start with 'data', got '{parts[0]}'")
            self.assertEqual('u', parts[2], f"Second part should be 'u', got '{parts[2]}'")
            self.assertEqual('s', parts[8], f"Fourth part should be 's', got '{parts[8]}'")
            self.assertEqual('w', parts[10], f"Sixth part should be 'w', got '{parts[10]}'")

    def test_data_file_prefix_default(self):
        """Test that generated data file names follow the expected prefix format."""
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['user_id'])
        self.catalog.create_table('default.test_file_prefix_default', schema, False)
        table = self.catalog.get_table('default.test_file_prefix_default')

        # Write some data to generate files
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        data = {
            'user_id': [1, 2],
            'item_id': [1001, 1002],
            'behavior': ['a', 'b'],
            'dt': ['p1', 'p1'],
        }
        pa_table = pa.Table.from_pydict(data, schema=self.pa_schema)
        table_write.write_arrow(pa_table)

        commit_messages = table_write.prepare_commit()
        table_commit.commit(commit_messages)
        table_write.close()
        table_commit.close()

        # Find generated data files
        table_path = os.path.join(self.warehouse, 'default.db', 'test_file_prefix_default')
        data_files = []
        for root, dirs, files in os.walk(table_path):
            for file in files:
                if file.endswith('.parquet') or file.endswith('.avro') or file.endswith('.orc'):
                    data_files.append(file)

        # Verify at least one data file was created
        self.assertGreater(len(data_files), 0, "No data files were generated")

        expected_pattern = r'^data-.+-0\.parquet$'

        for file_name in data_files:
            self.assertRegex(file_name, expected_pattern,
                             f"File name '{file_name}' does not match expected prefix format")

            # Additional checks for specific components
            parts = file_name.split('-')
            self.assertEqual('data', parts[0], f"File prefix should start with 'data', got '{parts[0]}'")

    def test_data_file_prefix(self):
        """Test that generated data file names follow the expected prefix format."""
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['user_id'],
                                            options={'data-file.prefix': 'test_prefix'})
        self.catalog.create_table('default.test_file_prefix', schema, False)
        table = self.catalog.get_table('default.test_file_prefix')

        # Write some data to generate files
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        data = {
            'user_id': [1, 2],
            'item_id': [1001, 1002],
            'behavior': ['a', 'b'],
            'dt': ['p1', 'p1'],
        }
        pa_table = pa.Table.from_pydict(data, schema=self.pa_schema)
        table_write.write_arrow(pa_table)

        commit_messages = table_write.prepare_commit()
        table_commit.commit(commit_messages)
        table_write.close()
        table_commit.close()

        # Find generated data files
        table_path = os.path.join(self.warehouse, 'default.db', 'test_file_prefix')
        data_files = []
        for root, dirs, files in os.walk(table_path):
            for file in files:
                if file.endswith('.parquet') or file.endswith('.avro') or file.endswith('.orc'):
                    data_files.append(file)

        # Verify at least one data file was created
        self.assertGreater(len(data_files), 0, "No data files were generated")

        expected_pattern = r'^test_prefix.+-0\.parquet$'

        for file_name in data_files:
            self.assertRegex(file_name, expected_pattern,
                             f"File name '{file_name}' does not match expected prefix format")
