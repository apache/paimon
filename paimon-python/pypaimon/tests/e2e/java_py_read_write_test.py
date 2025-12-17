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
import unittest

import pandas as pd
import pyarrow as pa
from parameterized import parameterized
from pypaimon.catalog.catalog_factory import CatalogFactory
from pypaimon.schema.schema import Schema


class JavaPyReadWriteTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tempdir = os.path.abspath(".")
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({
            'warehouse': cls.warehouse
        })
        cls.catalog.create_database('default', True)

    def test_py_write_read_append_table(self):
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('category', pa.string()),
            ('value', pa.float64())
        ])

        schema = Schema.from_pyarrow_schema(
            pa_schema,
            partition_keys=['category'],
            options={'dynamic-partition-overwrite': 'false'}
        )

        self.catalog.create_table('default.mixed_test_append_tablep', schema, False)
        table = self.catalog.get_table('default.mixed_test_append_tablep')

        initial_data = pd.DataFrame({
            'id': [1, 2, 3, 4, 5, 6],
            'name': ['Apple', 'Banana', 'Carrot', 'Broccoli', 'Chicken', 'Beef'],
            'category': ['Fruit', 'Fruit', 'Vegetable', 'Vegetable', 'Meat', 'Meat'],
            'value': [1.5, 0.8, 0.6, 1.2, 5.0, 8.0]
        })
        # Write initial data
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        table_write.write_pandas(initial_data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # Verify initial data
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        initial_result = table_read.to_pandas(table_scan.plan().splits())
        print(initial_result)
        self.assertEqual(len(initial_result), 6)
        # Data order may vary due to partitioning/bucketing, so compare as sets
        expected_names = {'Apple', 'Banana', 'Carrot', 'Broccoli', 'Chicken', 'Beef'}
        actual_names = set(initial_result['name'].tolist())
        self.assertEqual(actual_names, expected_names)

    def test_read_append_table(self):
        table = self.catalog.get_table('default.mixed_test_append_tablej')
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        res = table_read.to_pandas(table_scan.plan().splits())
        print(res)

    @parameterized.expand([
        ('parquet',),
        ('lance',),
    ])
    def test_py_write_read_pk_table(self, file_format):
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('category', pa.string()),
            ('value', pa.float64())
        ])

        table_name = f'default.mixed_test_pk_tablep_{file_format}'
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            partition_keys=['category'],
            primary_keys=['id'],
            options={
                'dynamic-partition-overwrite': 'false',
                'bucket': '2',
                'file.format': file_format
            }
        )

        try:
            existing_table = self.catalog.get_table(table_name)
            table_path = self.catalog.get_table_path(existing_table.identifier)
            if self.catalog.file_io.exists(table_path):
                self.catalog.file_io.delete(table_path, recursive=True)
        except Exception:
            pass

        self.catalog.create_table(table_name, schema, False)
        table = self.catalog.get_table(table_name)

        initial_data = pd.DataFrame({
            'id': [1, 2, 3, 4, 5, 6],
            'name': ['Apple', 'Banana', 'Carrot', 'Broccoli', 'Chicken', 'Beef'],
            'category': ['Fruit', 'Fruit', 'Vegetable', 'Vegetable', 'Meat', 'Meat'],
            'value': [1.5, 0.8, 0.6, 1.2, 5.0, 8.0]
        })
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        table_write.write_pandas(initial_data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        initial_result = table_read.to_pandas(table_scan.plan().splits())
        print(f"Format: {file_format}, Result:\n{initial_result}")
        self.assertEqual(len(initial_result), 6)
        # Data order may vary due to partitioning/bucketing, so compare as sets
        expected_names = {'Apple', 'Banana', 'Carrot', 'Broccoli', 'Chicken', 'Beef'}
        actual_names = set(initial_result['name'].tolist())
        self.assertEqual(actual_names, expected_names)

    @parameterized.expand([
        ('parquet',),
        ('lance',),
    ])
    def test_read_pk_table(self, file_format):
        # For parquet, read from Java-written table (no format suffix)
        # For lance, read from Java-written table (with format suffix)
        if file_format == 'parquet':
            table_name = 'default.mixed_test_pk_tablej'
        else:
            table_name = f'default.mixed_test_pk_tablej_{file_format}'
        table = self.catalog.get_table(table_name)
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        res = table_read.to_pandas(table_scan.plan().splits())
        print(f"Format: {file_format}, Result:\n{res}")

        # Verify data
        self.assertEqual(len(res), 6)
        # Data order may vary due to partitioning/bucketing, so compare as sets
        expected_names = {'Apple', 'Banana', 'Carrot', 'Broccoli', 'Chicken', 'Beef'}
        actual_names = set(res['name'].tolist())
        self.assertEqual(actual_names, expected_names)

        # For primary key tables, verify that _VALUE_KIND is written correctly
        # by checking if we can read the raw data with system fields
        # Note: Normal read filters out system fields, so we verify through Java read
        # which explicitly reads KeyValue objects and checks valueKind
        print(f"Format: {file_format}, Python read completed. ValueKind verification should be done in Java test.")

    def test_pk_dv_read(self):
        pa_schema = pa.schema([
            pa.field('pt', pa.int32(), nullable=False),
            pa.field('a', pa.int32(), nullable=False),
            ('b', pa.int64())
        ])
        schema = Schema.from_pyarrow_schema(pa_schema,
                                            partition_keys=['pt'],
                                            primary_keys=['pt', 'a'],
                                            options={'bucket': '1'})
        self.catalog.create_table('default.test_pk_dv', schema, True)
        table = self.catalog.get_table('default.test_pk_dv')
        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        actual = table_read.to_arrow(splits).sort_by('pt')
        expected = pa.Table.from_pydict({
            'pt': [1, 2, 2],
            'a': [10, 21, 22],
            'b': [1000, 20001, 202]
        }, schema=pa_schema)
        self.assertEqual(expected, actual)

    def test_pk_dv_read_multi_batch(self):
        pa_schema = pa.schema([
            pa.field('pt', pa.int32(), nullable=False),
            pa.field('a', pa.int32(), nullable=False),
            ('b', pa.int64())
        ])
        schema = Schema.from_pyarrow_schema(pa_schema,
                                            partition_keys=['pt'],
                                            primary_keys=['pt', 'a'],
                                            options={'bucket': '1'})
        self.catalog.create_table('default.test_pk_dv_multi_batch', schema, True)
        table = self.catalog.get_table('default.test_pk_dv_multi_batch')
        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        actual = table_read.to_arrow(splits).sort_by('pt')
        expected = pa.Table.from_pydict({
            'pt': [1] * 9999,
            'a': [i * 10 for i in range(1, 10001) if i * 10 != 81930],
            'b': [i * 100 for i in range(1, 10001) if i * 10 != 81930]
        }, schema=pa_schema)
        self.assertEqual(expected, actual)

    def test_pk_dv_read_multi_batch_raw_convertable(self):
        pa_schema = pa.schema([
            pa.field('pt', pa.int32(), nullable=False),
            pa.field('a', pa.int32(), nullable=False),
            ('b', pa.int64())
        ])
        schema = Schema.from_pyarrow_schema(pa_schema,
                                            partition_keys=['pt'],
                                            primary_keys=['pt', 'a'],
                                            options={'bucket': '1'})
        self.catalog.create_table('default.test_pk_dv_raw_convertable', schema, True)
        table = self.catalog.get_table('default.test_pk_dv_raw_convertable')
        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        actual = table_read.to_arrow(splits).sort_by('pt')
        expected = pa.Table.from_pydict({
            'pt': [1] * 9999,
            'a': [i * 10 for i in range(1, 10001) if i * 10 != 81930],
            'b': [i * 100 for i in range(1, 10001) if i * 10 != 81930]
        }, schema=pa_schema)
        self.assertEqual(expected, actual)
