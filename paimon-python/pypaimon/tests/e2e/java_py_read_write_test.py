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
import sys
import unittest

import pandas as pd
import pyarrow as pa
from parameterized import parameterized
from pypaimon.catalog.catalog_factory import CatalogFactory
from pypaimon.schema.schema import Schema
from pypaimon.read.read_builder import ReadBuilder

if sys.version_info[:2] == (3, 6):
    from pypaimon.tests.py36.pyarrow_compat import table_sort_by
else:
    def table_sort_by(table: pa.Table, column_name: str, order: str = 'ascending') -> pa.Table:
        return table.sort_by([(column_name, order)])


def get_file_format_params():
    if sys.version_info[:2] == (3, 6):
        return [('parquet',), ('orc',), ('avro',)]
    else:
        return [('parquet',), ('orc',), ('avro',), ('lance',)]


class JavaPyReadWriteTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tempdir = os.path.abspath(".")
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({
            'warehouse': cls.warehouse
        })
        cls.catalog.create_database('default', True)

    @parameterized.expand(get_file_format_params())
    def test_py_write_read_append_table(self, file_format):
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('category', pa.string()),
            ('value', pa.float64()),
            ('ts', pa.timestamp('us')),
            ('ts_ltz', pa.timestamp('us', tz='UTC'))
        ])

        schema = Schema.from_pyarrow_schema(
            pa_schema,
            partition_keys=['category'],
            options={'dynamic-partition-overwrite': 'false', 'file.format': file_format}
        )

        table_name = f'default.mixed_test_append_tablep_{file_format}'
        self.catalog.create_table(table_name, schema, False)
        table = self.catalog.get_table(table_name)

        initial_data = pd.DataFrame({
            'id': [1, 2, 3, 4, 5, 6],
            'name': ['Apple', 'Banana', 'Carrot', 'Broccoli', 'Chicken', 'Beef'],
            'category': ['Fruit', 'Fruit', 'Vegetable', 'Vegetable', 'Meat', 'Meat'],
            'value': [1.5, 0.8, 0.6, 1.2, 5.0, 8.0],
            'ts': pd.to_datetime([1000000, 1000001, 1000002, 1000003, 1000004, 1000005], unit='ms'),
            'ts_ltz': pd.to_datetime([2000000, 2000001, 2000002, 2000003, 2000004, 2000005], unit='ms', utc=True)
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

    @parameterized.expand(get_file_format_params())
    def test_read_append_table(self, file_format):
        table = self.catalog.get_table('default.mixed_test_append_tablej_' + file_format)
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        res = table_read.to_pandas(table_scan.plan().splits())
        print(res)

    @parameterized.expand(get_file_format_params())
    def test_py_write_read_pk_table(self, file_format):
        # Lance format doesn't support timestamp, so exclude timestamp columns
        if file_format == 'lance':
            pa_schema = pa.schema([
                ('id', pa.int32()),
                ('name', pa.string()),
                ('category', pa.string()),
                ('value', pa.float64())
            ])
        else:
            pa_schema = pa.schema([
                ('id', pa.int32()),
                ('name', pa.string()),
                ('category', pa.string()),
                ('value', pa.float64()),
                ('ts', pa.timestamp('us')),
                ('ts_ltz', pa.timestamp('us', tz='UTC')),
                ('metadata', pa.struct([
                    pa.field('source', pa.string()),
                    pa.field('created_at', pa.int64()),
                    pa.field('location', pa.struct([
                        pa.field('city', pa.string()),
                        pa.field('country', pa.string())
                    ]))
                ]))
            ])

        table_name = f'default.mixed_test_pk_tablep_{file_format}'
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            partition_keys=['category'],
            primary_keys=['id'],
            options={
                'dynamic-partition-overwrite': 'false',
                'bucket': '4',
                'file.format': file_format,
                "orc.timestamp-ltz.legacy.type": "false"
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

        # Lance format doesn't support timestamp, so exclude timestamp columns
        if file_format == 'lance':
            initial_data = pd.DataFrame({
                'id': [1, 2, 3, 4, 5, 6],
                'name': ['Apple', 'Banana', 'Carrot', 'Broccoli', 'Chicken', 'Beef'],
                'category': ['Fruit', 'Fruit', 'Vegetable', 'Vegetable', 'Meat', 'Meat'],
                'value': [1.5, 0.8, 0.6, 1.2, 5.0, 8.0]
            })
        else:
            initial_data = pd.DataFrame({
                'id': [1, 2, 3, 4, 5, 6],
                'name': ['Apple', 'Banana', 'Carrot', 'Broccoli', 'Chicken', 'Beef'],
                'category': ['Fruit', 'Fruit', 'Vegetable', 'Vegetable', 'Meat', 'Meat'],
                'value': [1.5, 0.8, 0.6, 1.2, 5.0, 8.0],
                'ts': pd.to_datetime([1000000, 1000001, 1000002, 1000003, 1000004, 1000005], unit='ms'),
                'ts_ltz': pd.to_datetime([2000000, 2000001, 2000002, 2000003, 2000004, 2000005], unit='ms', utc=True),
                'metadata': [
                    {'source': 'store1', 'created_at': 1001, 'location': {'city': 'Beijing', 'country': 'China'}},
                    {'source': 'store1', 'created_at': 1002, 'location': {'city': 'Shanghai', 'country': 'China'}},
                    {'source': 'store2', 'created_at': 1003, 'location': {'city': 'Tokyo', 'country': 'Japan'}},
                    {'source': 'store2', 'created_at': 1004, 'location': {'city': 'Seoul', 'country': 'Korea'}},
                    {'source': 'store3', 'created_at': 1005, 'location': {'city': 'NewYork', 'country': 'USA'}},
                    {'source': 'store3', 'created_at': 1006, 'location': {'city': 'London', 'country': 'UK'}}
                ]
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

        from pypaimon.write.row_key_extractor import FixedBucketRowKeyExtractor
        expected_bucket_first_row = 2
        first_row = initial_data.head(1)
        batch = pa.RecordBatch.from_pandas(first_row, schema=pa_schema)
        extractor = FixedBucketRowKeyExtractor(table.table_schema)
        _, buckets = extractor.extract_partition_bucket_batch(batch)
        self.assertEqual(buckets[0], expected_bucket_first_row,
                         "bucket for first row (id=1) with num_buckets=4 must be %d" % expected_bucket_first_row)

    @parameterized.expand(get_file_format_params())
    def test_read_pk_table(self, file_format):
        # Skip ORC format for Python < 3.8 due to pyarrow limitation with TIMESTAMP_INSTANT
        if sys.version_info[:2] < (3, 8) and file_format == 'orc':
            self.skipTest("Skipping ORC format for Python < 3.8 (pyarrow does not support TIMESTAMP_INSTANT)")
        
        table_name = f'default.mixed_test_pk_tablej_{file_format}'
        table = self.catalog.get_table(table_name)
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        res = table_read.to_pandas(table_scan.plan().splits())
        print(f"Format: {file_format}, Result:\n{res}")

        # Verify data
        self.assertEqual(len(res), 6)
        if file_format != "lance":
            self.assertEqual(table.fields[4].type.type, "TIMESTAMP(6)")
            self.assertEqual(table.fields[5].type.type, "TIMESTAMP(6) WITH LOCAL TIME ZONE")
            from pypaimon.schema.data_types import RowType
            self.assertIsInstance(table.fields[6].type, RowType)
            metadata_fields = table.fields[6].type.fields
            self.assertEqual(len(metadata_fields), 3)
            self.assertEqual(metadata_fields[0].name, 'source')
            self.assertEqual(metadata_fields[1].name, 'created_at')
            self.assertEqual(metadata_fields[2].name, 'location')
            self.assertIsInstance(metadata_fields[2].type, RowType)
        
        # Data order may vary due to partitioning/bucketing, so compare as sets
        expected_names = {'Apple', 'Banana', 'Carrot', 'Broccoli', 'Chicken', 'Beef'}
        actual_names = set(res['name'].tolist())
        self.assertEqual(actual_names, expected_names)

        # Verify metadata column can be read and contains nested structures
        if 'metadata' in res.columns:
            self.assertFalse(res['metadata'].isnull().all())

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
        actual = table_sort_by(table_read.to_arrow(splits), 'pt')
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
        actual = table_sort_by(table_read.to_arrow(splits), 'pt')
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
        actual = table_sort_by(table_read.to_arrow(splits), 'pt')
        expected = pa.Table.from_pydict({
            'pt': [1] * 9999,
            'a': [i * 10 for i in range(1, 10001) if i * 10 != 81930],
            'b': [i * 100 for i in range(1, 10001) if i * 10 != 81930]
        }, schema=pa_schema)
        self.assertEqual(expected, actual)

    def test_read_btree_index_table(self):
        self._test_read_btree_index_generic("test_btree_index_string", "k2", pa.string())
        self._test_read_btree_index_generic("test_btree_index_int", 200, pa.int32())
        self._test_read_btree_index_generic("test_btree_index_bigint", 2000, pa.int64())
        self._test_read_btree_index_large()
        self._test_read_btree_index_null()

    def _test_read_btree_index_generic(self, table_name: str, k, k_type):
        table = self.catalog.get_table('default.' + table_name)
        read_builder: ReadBuilder = table.new_read_builder()

        # read using index
        predicate_builder = read_builder.new_predicate_builder()
        predicate = predicate_builder.equal('k', k)
        read_builder.with_filter(predicate)
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        actual = table_sort_by(table_read.to_arrow(splits), 'k')
        expected = pa.Table.from_pydict({
            'k': [k],
            'v': ["v2"]
        }, schema=pa.schema([
            ("k", k_type),
            ("v", pa.string())
        ]))
        self.assertEqual(expected, actual)

    def _test_read_btree_index_large(self):
        table = self.catalog.get_table('default.test_btree_index_large')
        read_builder: ReadBuilder = table.new_read_builder()
        predicate_builder = read_builder.new_predicate_builder()

        # read equal index
        read_builder.with_filter(predicate_builder.equal('k', 'k2'))
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        actual = table_sort_by(table_read.to_arrow(splits), 'k')
        expected = pa.Table.from_pydict({
            'k': ["k2"],
            'v': ["v2"]
        })
        self.assertEqual(expected, actual)

        # read between index
        read_builder.with_filter(predicate_builder.between('k', 'k990', 'k995'))
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        actual = table_sort_by(table_read.to_arrow(splits), 'k')
        expected = pa.Table.from_pydict({
            'k': ["k990", "k991", "k992", "k993", "k994", "k995"],
            'v': ["v990", "v991", "v992", "v993", "v994", "v995"]
        })
        self.assertEqual(expected, actual)

        # read in index
        read_builder.with_filter(predicate_builder.is_in('k', ['k990', 'k995']))
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        actual = table_sort_by(table_read.to_arrow(splits), 'k')
        expected = pa.Table.from_pydict({
            'k': ["k990", "k995"],
            'v': ["v990", "v995"]
        })
        self.assertEqual(expected, actual)

    def _test_read_btree_index_null(self):
        table = self.catalog.get_table('default.test_btree_index_null')

        # read is null index
        read_builder: ReadBuilder = table.new_read_builder()
        predicate_builder = read_builder.new_predicate_builder()
        read_builder.with_filter(predicate_builder.is_null('k'))
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        actual = table_sort_by(table_read.to_arrow(splits), 'k')
        expected = pa.Table.from_pydict({
            'k': [None, None],
            'v': ["v3", "v5"]
        }, schema=pa.schema([
            ("k", pa.string()),
            ("v", pa.string())
        ]))
        self.assertEqual(expected, actual)

        # read is not null index
        read_builder: ReadBuilder = table.new_read_builder()
        predicate_builder = read_builder.new_predicate_builder()
        read_builder.with_filter(predicate_builder.is_not_null('k'))
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        actual = table_sort_by(table_read.to_arrow(splits), 'k')
        expected = pa.Table.from_pydict({
            'k': ["k1", "k2", "k4"],
            'v': ["v1", "v2", "v4"]
        })
        self.assertEqual(expected, actual)
