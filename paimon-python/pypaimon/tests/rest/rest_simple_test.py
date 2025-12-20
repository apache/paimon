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

import pyarrow as pa

from pypaimon import Schema
from pypaimon.catalog.catalog_exception import DatabaseAlreadyExistException, TableAlreadyExistException, \
    DatabaseNotExistException, TableNotExistException
from pypaimon.tests.rest.rest_base_test import RESTBaseTest
from pypaimon.write.row_key_extractor import FixedBucketRowKeyExtractor, DynamicBucketRowKeyExtractor, \
    UnawareBucketRowKeyExtractor


class RESTSimpleTest(RESTBaseTest):
    def setUp(self):
        super().setUp()
        self.pa_schema = pa.schema([
            ('user_id', pa.int64()),
            ('item_id', pa.int64()),
            ('behavior', pa.string()),
            ('dt', pa.string()),
        ])
        self.data = {
            'user_id': [2, 4, 6, 8, 10],
            'item_id': [1001, 1002, 1003, 1004, 1005],
            'behavior': ['a', 'b', 'c', 'd', 'e'],
            'dt': ['2000-10-10', '2025-08-10', '2025-08-11', '2025-08-12', '2025-08-13']
        }
        self.expected = pa.Table.from_pydict(self.data, schema=self.pa_schema)

    def test_with_shard_ao_unaware_bucket(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.rest_catalog.create_table('default.test_with_shard_ao_unaware_bucket', schema, False)
        table = self.rest_catalog.get_table('default.test_with_shard_ao_unaware_bucket')
        write_builder = table.new_batch_write_builder()
        # first write
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data1 = {
            'user_id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
            'item_id': [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010, 1011, 1012, 1013, 1014],
            'behavior': ['a', 'b', 'c', None, 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm'],
            'dt': ['p1', 'p1', 'p2', 'p1', 'p2', 'p1', 'p2', 'p1', 'p2', 'p1', 'p2', 'p1', 'p2', 'p1'],
        }
        pa_table = pa.Table.from_pydict(data1, schema=self.pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()
        # second write
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data2 = {
            'user_id': [5, 6, 7, 8, 18],
            'item_id': [1005, 1006, 1007, 1008, 1018],
            'behavior': ['e', 'f', 'g', 'h', 'z'],
            'dt': ['p2', 'p1', 'p2', 'p2', 'p1'],
        }
        pa_table = pa.Table.from_pydict(data2, schema=self.pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().with_shard(2, 3).plan().splits()
        actual = table_read.to_arrow(splits).sort_by('user_id')
        expected = pa.Table.from_pydict({
            'user_id': [5, 7, 8, 9, 11, 13],
            'item_id': [1005, 1007, 1008, 1009, 1011, 1013],
            'behavior': ['e', 'g', 'h', 'h', 'j', 'l'],
            'dt': ['p2', 'p2', 'p2', 'p2', 'p2', 'p2'],
        }, schema=self.pa_schema)
        self.assertEqual(actual, expected)

        # Get the three actual tables
        splits1 = read_builder.new_scan().with_shard(0, 3).plan().splits()
        actual1 = table_read.to_arrow(splits1).sort_by('user_id')
        splits2 = read_builder.new_scan().with_shard(1, 3).plan().splits()
        actual2 = table_read.to_arrow(splits2).sort_by('user_id')
        splits3 = read_builder.new_scan().with_shard(2, 3).plan().splits()
        actual3 = table_read.to_arrow(splits3).sort_by('user_id')

        # Concatenate the three tables
        actual = pa.concat_tables([actual1, actual2, actual3]).sort_by('user_id')
        expected = self._read_test_table(read_builder).sort_by('user_id')
        self.assertEqual(actual, expected)

    def test_with_shard_ao_unaware_bucket_manual(self):
        """Test shard_ao_unaware_bucket with setting bucket -1 manually"""
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'],
                                            options={'bucket': '-1'})
        self.rest_catalog.create_table('default.test_with_shard_ao_unaware_bucket_manual', schema, False)
        table = self.rest_catalog.get_table('default.test_with_shard_ao_unaware_bucket_manual')
        write_builder = table.new_batch_write_builder()

        # Write data with single partition
        table_write = write_builder.new_write()
        self.assertIsInstance(table_write.row_key_extractor, UnawareBucketRowKeyExtractor)

        table_commit = write_builder.new_commit()
        data = {
            'user_id': [1, 2, 3, 4, 5, 6],
            'item_id': [1001, 1002, 1003, 1004, 1005, 1006],
            'behavior': ['a', 'b', 'c', 'd', 'e', 'f'],
            'dt': ['p1', 'p1', 'p1', 'p1', 'p1', 'p1'],
        }
        pa_table = pa.Table.from_pydict(data, schema=self.pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()

        # Test first shard (0, 2) - should get first 3 rows
        plan = read_builder.new_scan().with_shard(0, 2).plan()
        actual = table_read.to_arrow(plan.splits()).sort_by('user_id')
        expected = pa.Table.from_pydict({
            'user_id': [1, 2, 3],
            'item_id': [1001, 1002, 1003],
            'behavior': ['a', 'b', 'c'],
            'dt': ['p1', 'p1', 'p1'],
        }, schema=self.pa_schema)
        self.assertEqual(actual, expected)

        # Test second shard (1, 2) - should get last 3 rows
        plan = read_builder.new_scan().with_shard(1, 2).plan()
        actual = table_read.to_arrow(plan.splits()).sort_by('user_id')
        expected = pa.Table.from_pydict({
            'user_id': [4, 5, 6],
            'item_id': [1004, 1005, 1006],
            'behavior': ['d', 'e', 'f'],
            'dt': ['p1', 'p1', 'p1'],
        }, schema=self.pa_schema)
        self.assertEqual(actual, expected)

    def test_with_shard_ao_fixed_bucket(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'],
                                            options={'bucket': '5', 'bucket-key': 'item_id'})
        self.rest_catalog.create_table('default.test_with_slice_ao_fixed_bucket', schema, False)
        table = self.rest_catalog.get_table('default.test_with_slice_ao_fixed_bucket')
        write_builder = table.new_batch_write_builder()
        # first write
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data1 = {
            'user_id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
            'item_id': [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010, 1011, 1012, 1013, 1014],
            'behavior': ['a', 'b', 'c', None, 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm'],
            'dt': ['p1', 'p1', 'p2', 'p1', 'p2', 'p1', 'p2', 'p1', 'p2', 'p1', 'p2', 'p1', 'p2', 'p1'],
        }
        pa_table = pa.Table.from_pydict(data1, schema=self.pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()
        # second write
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data2 = {
            'user_id': [5, 6, 7, 8],
            'item_id': [1005, 1006, 1007, 1008],
            'behavior': ['e', 'f', 'g', 'h'],
            'dt': ['p2', 'p1', 'p2', 'p2'],
        }
        pa_table = pa.Table.from_pydict(data2, schema=self.pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().with_shard(0, 3).plan().splits()
        actual = table_read.to_arrow(splits).sort_by('user_id')
        expected = pa.Table.from_pydict({
            'user_id': [1, 2, 3, 5, 8, 12],
            'item_id': [1001, 1002, 1003, 1005, 1008, 1012],
            'behavior': ['a', 'b', 'c', 'd', 'g', 'k'],
            'dt': ['p1', 'p1', 'p2', 'p2', 'p1', 'p1'],
        }, schema=self.pa_schema)
        self.assertEqual(actual, expected)

        # Get the three actual tables
        splits1 = read_builder.new_scan().with_shard(0, 3).plan().splits()
        actual1 = table_read.to_arrow(splits1).sort_by('user_id')
        splits2 = read_builder.new_scan().with_shard(1, 3).plan().splits()
        actual2 = table_read.to_arrow(splits2).sort_by('user_id')
        splits3 = read_builder.new_scan().with_shard(2, 3).plan().splits()
        actual3 = table_read.to_arrow(splits3).sort_by('user_id')

        # Concatenate the three tables
        actual = pa.concat_tables([actual1, actual2, actual3]).sort_by('user_id')
        expected = self._read_test_table(read_builder).sort_by('user_id')
        self.assertEqual(actual, expected)

    def test_with_shard_single_partition(self):
        """Test sharding with single partition - tests _filter_by_shard with simple data"""
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.rest_catalog.create_table('default.test_shard_single_partition', schema, False)
        table = self.rest_catalog.get_table('default.test_shard_single_partition')
        write_builder = table.new_batch_write_builder()

        # Write data with single partition
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data = {
            'user_id': [1, 2, 3, 4, 5, 6],
            'item_id': [1001, 1002, 1003, 1004, 1005, 1006],
            'behavior': ['a', 'b', 'c', 'd', 'e', 'f'],
            'dt': ['p1', 'p1', 'p1', 'p1', 'p1', 'p1'],
        }
        pa_table = pa.Table.from_pydict(data, schema=self.pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()

        # Test first shard (0, 2) - should get first 3 rows
        plan = read_builder.new_scan().with_shard(0, 2).plan()
        actual = table_read.to_arrow(plan.splits()).sort_by('user_id')
        expected = pa.Table.from_pydict({
            'user_id': [1, 2, 3],
            'item_id': [1001, 1002, 1003],
            'behavior': ['a', 'b', 'c'],
            'dt': ['p1', 'p1', 'p1'],
        }, schema=self.pa_schema)
        self.assertEqual(actual, expected)

        # Test second shard (1, 2) - should get last 3 rows
        plan = read_builder.new_scan().with_shard(1, 2).plan()
        actual = table_read.to_arrow(plan.splits()).sort_by('user_id')
        expected = pa.Table.from_pydict({
            'user_id': [4, 5, 6],
            'item_id': [1004, 1005, 1006],
            'behavior': ['d', 'e', 'f'],
            'dt': ['p1', 'p1', 'p1'],
        }, schema=self.pa_schema)
        self.assertEqual(actual, expected)

    def test_with_shard_uneven_distribution(self):
        """Test sharding with uneven row distribution across shards"""
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.rest_catalog.create_table('default.test_shard_uneven', schema, False)
        table = self.rest_catalog.get_table('default.test_shard_uneven')
        write_builder = table.new_batch_write_builder()

        # Write data with 7 rows (not evenly divisible by 3)
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data = {
            'user_id': [1, 2, 3, 4, 5, 6, 7],
            'item_id': [1001, 1002, 1003, 1004, 1005, 1006, 1007],
            'behavior': ['a', 'b', 'c', 'd', 'e', 'f', 'g'],
            'dt': ['p1', 'p1', 'p1', 'p1', 'p1', 'p1', 'p1'],
        }
        pa_table = pa.Table.from_pydict(data, schema=self.pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()

        # Test sharding into 3 parts: 3, 2, 2 rows
        plan1 = read_builder.new_scan().with_shard(0, 3).plan()
        actual1 = table_read.to_arrow(plan1.splits()).sort_by('user_id')
        expected1 = pa.Table.from_pydict({
            'user_id': [1, 2, 3],
            'item_id': [1001, 1002, 1003],
            'behavior': ['a', 'b', 'c'],
            'dt': ['p1', 'p1', 'p1'],
        }, schema=self.pa_schema)
        self.assertEqual(actual1, expected1)

        plan2 = read_builder.new_scan().with_shard(1, 3).plan()
        actual2 = table_read.to_arrow(plan2.splits()).sort_by('user_id')
        expected2 = pa.Table.from_pydict({
            'user_id': [4, 5],
            'item_id': [1004, 1005],
            'behavior': ['d', 'e'],
            'dt': ['p1', 'p1'],
        }, schema=self.pa_schema)
        self.assertEqual(actual2, expected2)

        plan3 = read_builder.new_scan().with_shard(2, 3).plan()
        actual3 = table_read.to_arrow(plan3.splits()).sort_by('user_id')
        expected3 = pa.Table.from_pydict({
            'user_id': [6, 7],
            'item_id': [1006, 1007],
            'behavior': ['f', 'g'],
            'dt': ['p1', 'p1'],
        }, schema=self.pa_schema)
        self.assertEqual(actual3, expected3)

    def test_with_shard_single_shard(self):
        """Test sharding with only one shard - should return all data"""
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.rest_catalog.create_table('default.test_shard_single', schema, False)
        table = self.rest_catalog.get_table('default.test_shard_single')
        write_builder = table.new_batch_write_builder()

        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data = {
            'user_id': [1, 2, 3, 4],
            'item_id': [1001, 1002, 1003, 1004],
            'behavior': ['a', 'b', 'c', 'd'],
            'dt': ['p1', 'p1', 'p2', 'p2'],
        }
        pa_table = pa.Table.from_pydict(data, schema=self.pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()

        # Test single shard (0, 1) - should get all data
        plan = read_builder.new_scan().with_shard(0, 1).plan()
        actual = table_read.to_arrow(plan.splits()).sort_by('user_id')
        expected = pa.Table.from_pydict(data, schema=self.pa_schema)
        self.assertEqual(actual, expected)

    def test_with_shard_many_small_shards(self):
        """Test sharding with many small shards"""
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.rest_catalog.create_table('default.test_shard_many_small', schema, False)
        table = self.rest_catalog.get_table('default.test_shard_many_small')
        write_builder = table.new_batch_write_builder()

        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data = {
            'user_id': [1, 2, 3, 4, 5, 6],
            'item_id': [1001, 1002, 1003, 1004, 1005, 1006],
            'behavior': ['a', 'b', 'c', 'd', 'e', 'f'],
            'dt': ['p1', 'p1', 'p1', 'p1', 'p1', 'p1'],
        }
        pa_table = pa.Table.from_pydict(data, schema=self.pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()

        # Test with 6 shards (one row per shard)
        for i in range(6):
            plan = read_builder.new_scan().with_shard(i, 6).plan()
            actual = table_read.to_arrow(plan.splits())
            self.assertEqual(len(actual), 1)
            self.assertEqual(actual['user_id'][0].as_py(), i + 1)

    def test_with_shard_boundary_conditions(self):
        """Test sharding boundary conditions with edge cases"""
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.rest_catalog.create_table('default.test_shard_boundary', schema, False)
        table = self.rest_catalog.get_table('default.test_shard_boundary')
        write_builder = table.new_batch_write_builder()

        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data = {
            'user_id': [1, 2, 3, 4, 5],
            'item_id': [1001, 1002, 1003, 1004, 1005],
            'behavior': ['a', 'b', 'c', 'd', 'e'],
            'dt': ['p1', 'p1', 'p1', 'p1', 'p1'],
        }
        pa_table = pa.Table.from_pydict(data, schema=self.pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()

        # Test first shard (0, 4) - should get 1 row (5//4 +1= 2)
        plan = read_builder.new_scan().with_shard(0, 4).plan()
        actual = table_read.to_arrow(plan.splits())
        self.assertEqual(len(actual), 2)

        # Test middle shard (1, 4) - should get 1 row
        plan = read_builder.new_scan().with_shard(1, 4).plan()
        actual = table_read.to_arrow(plan.splits())
        self.assertEqual(len(actual), 1)

        # Test last shard (3, 4) - should get 1 rows (remainder goes to last shard)
        plan = read_builder.new_scan().with_shard(3, 4).plan()
        actual = table_read.to_arrow(plan.splits())
        self.assertEqual(len(actual), 1)

    def test_with_shard_large_dataset(self):
        """Test with_shard method using 50000 rows of data to verify performance and correctness"""
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'],
                                            options={'bucket': '5', 'bucket-key': 'item_id'})
        self.rest_catalog.create_table('default.test_with_shard_large_dataset', schema, False)
        table = self.rest_catalog.get_table('default.test_with_shard_large_dataset')
        write_builder = table.new_batch_write_builder()

        # Generate 50000 rows of test data
        num_rows = 50000
        batch_size = 5000  # Write in batches to avoid memory issues

        for batch_start in range(0, num_rows, batch_size):
            batch_end = min(batch_start + batch_size, num_rows)
            batch_data = {
                'user_id': list(range(batch_start + 1, batch_end + 1)),
                'item_id': [2000 + i for i in range(batch_start, batch_end)],
                'behavior': [chr(ord('a') + (i % 26)) for i in range(batch_start, batch_end)],
                'dt': [f'p{(i % 5) + 1}' for i in range(batch_start, batch_end)],
            }

            table_write = write_builder.new_write()
            table_commit = write_builder.new_commit()
            pa_table = pa.Table.from_pydict(batch_data, schema=self.pa_schema)
            table_write.write_arrow(pa_table)
            table_commit.commit(table_write.prepare_commit())
            table_write.close()
            table_commit.close()

        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()

        # Test with 6 shards
        num_shards = 6
        shard_results = []
        total_rows_from_shards = 0

        for shard_idx in range(num_shards):
            splits = read_builder.new_scan().with_shard(shard_idx, num_shards).plan().splits()
            shard_result = table_read.to_arrow(splits)
            shard_results.append(shard_result)
            shard_rows = len(shard_result) if shard_result else 0
            total_rows_from_shards += shard_rows
            print(f"Shard {shard_idx}/{num_shards}: {shard_rows} rows")

        # Verify that all shards together contain all the data
        concatenated_result = pa.concat_tables(shard_results).sort_by('user_id')

        # Read all data without sharding for comparison
        all_splits = read_builder.new_scan().plan().splits()
        all_data = table_read.to_arrow(all_splits).sort_by('user_id')

        # Verify total row count
        self.assertEqual(len(concatenated_result), len(all_data))
        self.assertEqual(len(all_data), num_rows)
        self.assertEqual(total_rows_from_shards, num_rows)

        # Verify data integrity - check first and last few rows
        self.assertEqual(concatenated_result['user_id'][0].as_py(), 1)
        self.assertEqual(concatenated_result['user_id'][-1].as_py(), num_rows)
        self.assertEqual(concatenated_result['item_id'][0].as_py(), 2000)
        self.assertEqual(concatenated_result['item_id'][-1].as_py(), 2000 + num_rows - 1)

        # Verify that concatenated result equals all data
        self.assertEqual(concatenated_result, all_data)
        # Test with different shard configurations
        # Test with 10 shards
        shard_10_results = []
        for shard_idx in range(10):
            splits = read_builder.new_scan().with_shard(shard_idx, 10).plan().splits()
            shard_result = table_read.to_arrow(splits)
            if shard_result:
                shard_10_results.append(shard_result)

        if shard_10_results:
            concatenated_10_shards = pa.concat_tables(shard_10_results).sort_by('user_id')
            self.assertEqual(len(concatenated_10_shards), num_rows)
            self.assertEqual(concatenated_10_shards, all_data)

        # Test with single shard (should return all data)
        single_shard_splits = read_builder.new_scan().with_shard(0, 1).plan().splits()
        single_shard_result = table_read.to_arrow(single_shard_splits).sort_by('user_id')
        self.assertEqual(len(single_shard_result), num_rows)
        self.assertEqual(single_shard_result, all_data)

        print(f"Successfully tested with_shard method using {num_rows} rows of data")

    def test_with_shard_large_dataset_one_commit(self):
        """Test with_shard method using 50000 rows of data to verify performance and correctness"""
        schema = Schema.from_pyarrow_schema(self.pa_schema)
        self.rest_catalog.create_table('default.test_with_shard_large_dataset', schema, False)
        table = self.rest_catalog.get_table('default.test_with_shard_large_dataset')
        write_builder = table.new_batch_write_builder()

        # Generate 50000 rows of test data
        num_rows = 50000
        batch_data = {
            'user_id': list(range(0, num_rows)),
            'item_id': [2000 + i for i in range(0, num_rows)],
            'behavior': [chr(ord('a') + (i % 26)) for i in range(0, num_rows)],
            'dt': [f'p{(i % 5) + 1}' for i in range(0, num_rows)],
        }
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        pa_table = pa.Table.from_pydict(batch_data, schema=self.pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        num_shards = 5
        shard_results = []
        total_rows_from_shards = 0
        for shard_idx in range(num_shards):
            splits = read_builder.new_scan().with_shard(shard_idx, num_shards).plan().splits()
            shard_result = table_read.to_arrow(splits)
            shard_results.append(shard_result)
            shard_rows = len(shard_result) if shard_result else 0
            total_rows_from_shards += shard_rows
            print(f"Shard {shard_idx}/{num_shards}: {shard_rows} rows")

        # Verify that all shards together contain all the data
        concatenated_result = pa.concat_tables(shard_results).sort_by('user_id')

        # Read all data without sharding for comparison
        all_splits = read_builder.new_scan().plan().splits()
        all_data = table_read.to_arrow(all_splits).sort_by('user_id')

        # Verify total row count
        self.assertEqual(len(concatenated_result), len(all_data))
        self.assertEqual(len(all_data), num_rows)
        self.assertEqual(total_rows_from_shards, num_rows)

        # Verify data integrity - check first and last few rows
        self.assertEqual(concatenated_result['user_id'][0].as_py(), 0)
        self.assertEqual(concatenated_result['user_id'][-1].as_py(), num_rows - 1)
        self.assertEqual(concatenated_result['item_id'][0].as_py(), 2000)
        self.assertEqual(concatenated_result['item_id'][-1].as_py(), 2000 + num_rows - 1)

        # Verify that concatenated result equals all data
        self.assertEqual(concatenated_result, all_data)

    def test_with_shard_parameter_validation(self):
        """Test edge cases for parameter validation"""
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.rest_catalog.create_table('default.test_shard_validation_edge', schema, False)
        table = self.rest_catalog.get_table('default.test_shard_validation_edge')

        read_builder = table.new_read_builder()
        # Test invalid case with number_of_para_subtasks = 1
        with self.assertRaises(Exception) as context:
            read_builder.new_scan().with_shard(1, 1).plan()
        self.assertEqual(str(context.exception), "idx_of_this_subtask must be less than number_of_para_subtasks")

    def test_with_shard_pk_dynamic_bucket(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['user_id'], primary_keys=['user_id', 'dt'])
        self.rest_catalog.create_table('default.test_with_shard', schema, False)
        table = self.rest_catalog.get_table('default.test_with_shard')

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        self.assertIsInstance(table_write.row_key_extractor, DynamicBucketRowKeyExtractor)

        pa_table = pa.Table.from_pydict(self.data, schema=self.pa_schema)

        with self.assertRaises(ValueError) as context:
            table_write.write_arrow(pa_table)

        self.assertEqual(str(context.exception), "Can't extract bucket from row in dynamic bucket mode")

    def test_with_shard_pk_fixed_bucket(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['user_id'], primary_keys=['user_id', 'dt'],
                                            options={'bucket': '5'})
        self.rest_catalog.create_table('default.test_with_shard', schema, False)
        table = self.rest_catalog.get_table('default.test_with_shard')

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        self.assertIsInstance(table_write.row_key_extractor, FixedBucketRowKeyExtractor)

        pa_table = pa.Table.from_pydict(self.data, schema=self.pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        splits = []
        read_builder = table.new_read_builder()
        splits.extend(read_builder.new_scan().with_shard(0, 3).plan().splits())
        splits.extend(read_builder.new_scan().with_shard(1, 3).plan().splits())
        splits.extend(read_builder.new_scan().with_shard(2, 3).plan().splits())

        table_read = read_builder.new_read()
        actual = table_read.to_arrow(splits)
        data_expected = {
            'user_id': [4, 6, 2, 10, 8],
            'item_id': [1002, 1003, 1001, 1005, 1004],
            'behavior': ['b', 'c', 'a', 'e', 'd'],
            'dt': ['2025-08-10', '2025-08-11', '2000-10-10', '2025-08-13', '2025-08-12']
        }
        expected = pa.Table.from_pydict(data_expected, schema=self.pa_schema)
        self.assertEqual(actual, expected)

    def test_with_shard_uniform_division(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.rest_catalog.create_table('default.with_shard_uniform_division', schema, False)
        table = self.rest_catalog.get_table('default.with_shard_uniform_division')
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data = {
            'user_id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
            'item_id': [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010, 1011, 1012, 1013, 1014],
            'behavior': ['a', 'b', 'c', None, 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm'],
            'dt': ['p1', 'p1', 'p2', 'p1', 'p2', 'p1', 'p2', 'p1', 'p2', 'p1', 'p2', 'p1', 'p2', 'p1'],
        }
        pa_table = pa.Table.from_pydict(data, schema=self.pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()

        # Get the three actual tables
        splits1 = read_builder.new_scan().with_shard(0, 3).plan().splits()
        actual1 = table_read.to_arrow(splits1).sort_by('user_id')
        splits2 = read_builder.new_scan().with_shard(1, 3).plan().splits()
        actual2 = table_read.to_arrow(splits2).sort_by('user_id')
        splits3 = read_builder.new_scan().with_shard(2, 3).plan().splits()
        actual3 = table_read.to_arrow(splits3).sort_by('user_id')
        self.assertEqual(5, len(actual1))
        self.assertEqual(5, len(actual2))
        self.assertEqual(4, len(actual3))
        # Concatenate the three tables
        actual = pa.concat_tables([actual1, actual2, actual3]).sort_by('user_id')
        expected = self._read_test_table(read_builder).sort_by('user_id')
        self.assertEqual(expected, actual)

    def test_create_drop_database_table(self):
        # test create database
        self.rest_catalog.create_database("db1", False)

        with self.assertRaises(DatabaseAlreadyExistException) as context:
            self.rest_catalog.create_database("db1", False)

        self.assertEqual("db1", context.exception.database)

        try:
            self.rest_catalog.create_database("db1", True)
        except DatabaseAlreadyExistException:
            self.fail("create_database with ignore_if_exists=True should not raise DatabaseAlreadyExistException")

        # test create table
        self.rest_catalog.create_table("db1.tbl1",
                                       Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt']),
                                       False)
        with self.assertRaises(TableAlreadyExistException) as context:
            self.rest_catalog.create_table("db1.tbl1",
                                           Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt']),
                                           False)
        self.assertEqual("db1.tbl1", context.exception.identifier.get_full_name())

        try:
            self.rest_catalog.create_table("db1.tbl1",
                                           Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt']),
                                           True)
        except TableAlreadyExistException:
            self.fail("create_table with ignore_if_exists=True should not raise TableAlreadyExistException")

        # test drop table
        self.rest_catalog.drop_table("db1.tbl1", False)
        with self.assertRaises(TableNotExistException) as context:
            self.rest_catalog.drop_table("db1.tbl1", False)
        self.assertEqual("db1.tbl1", context.exception.identifier.get_full_name())

        try:
            self.rest_catalog.drop_table("db1.tbl1", True)
        except TableNotExistException:
            self.fail("drop_table with ignore_if_not_exists=True should not raise TableNotExistException")

        # test drop database
        self.rest_catalog.drop_database("db1", False)
        with self.assertRaises(DatabaseNotExistException) as context:
            self.rest_catalog.drop_database("db1", False)
        self.assertEqual("db1", context.exception.database)

        try:
            self.rest_catalog.drop_database("db1", True)
        except DatabaseNotExistException:
            self.fail("drop_database with ignore_if_not_exists=True should not raise DatabaseNotExistException")
