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

import pyarrow as pa

from pypaimon import Schema
from pypaimon.tests.rest.rest_base_test import RESTBaseTest


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
        plan = read_builder.new_scan().with_shard(2, 3).plan()
        actual = table_read.to_arrow_slice(plan).sort_by('user_id')
        expected = pa.Table.from_pydict({
            'user_id': [5, 6, 7, 8, 11, 13, 18],
            'item_id': [1005, 1006, 1007, 1008, 1011, 1013, 1018],
            'behavior': ['e', 'f', 'g', 'h', 'j', 'l', 'z'],
            'dt': ['p2', 'p1', 'p2', 'p2', 'p2', 'p2', 'p1'],
        }, schema=self.pa_schema)
        self.assertEqual(actual, expected)

        # Get the three actual tables
        plan1 = read_builder.new_scan().with_shard(0, 3).plan()
        actual1 = table_read.to_arrow_slice(plan1).sort_by('user_id')
        plan2 = read_builder.new_scan().with_shard(1, 3).plan()
        actual2 = table_read.to_arrow_slice(plan2).sort_by('user_id')
        plan3 = read_builder.new_scan().with_shard(2, 3).plan()
        actual3 = table_read.to_arrow_slice(plan3).sort_by('user_id')

        # Concatenate the three tables
        actual = pa.concat_tables([actual1, actual2, actual3]).sort_by('user_id')
        expected = self._read_test_table(read_builder).sort_by('user_id')
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
        plan = read_builder.new_scan().with_shard(0, 3).plan()
        actual = table_read.to_arrow_slice(plan).sort_by('user_id')
        expected = pa.Table.from_pydict({
            'user_id': [1, 2, 3, 5, 8, 12],
            'item_id': [1001, 1002, 1003, 1005, 1008, 1012],
            'behavior': ['a', 'b', 'c', 'd', 'g', 'k'],
            'dt': ['p1', 'p1', 'p2', 'p2', 'p1', 'p1'],
        }, schema=self.pa_schema)
        self.assertEqual(actual, expected)

        # Get the three actual tables
        plan1 = read_builder.new_scan().with_shard(0, 3).plan()
        actual1 = table_read.to_arrow_slice(plan1).sort_by('user_id')
        plan2 = read_builder.new_scan().with_shard(1, 3).plan()
        actual2 = table_read.to_arrow_slice(plan2).sort_by('user_id')
        plan3 = read_builder.new_scan().with_shard(2, 3).plan()
        actual3 = table_read.to_arrow_slice(plan3).sort_by('user_id')

        # Concatenate the three tables
        actual = pa.concat_tables([actual1, actual2, actual3]).sort_by('user_id')
        expected = self._read_test_table(read_builder).sort_by('user_id')
        self.assertEqual(actual, expected)

    def test_shard_single_partition(self):
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
        actual = table_read.to_arrow_slice(plan).sort_by('user_id')
        expected = pa.Table.from_pydict({
            'user_id': [1, 2, 3],
            'item_id': [1001, 1002, 1003],
            'behavior': ['a', 'b', 'c'],
            'dt': ['p1', 'p1', 'p1'],
        }, schema=self.pa_schema)
        self.assertEqual(actual, expected)

        # Test second shard (1, 2) - should get last 3 rows
        plan = read_builder.new_scan().with_shard(1, 2).plan()
        actual = table_read.to_arrow_slice(plan).sort_by('user_id')
        expected = pa.Table.from_pydict({
            'user_id': [4, 5, 6],
            'item_id': [1004, 1005, 1006],
            'behavior': ['d', 'e', 'f'],
            'dt': ['p1', 'p1', 'p1'],
        }, schema=self.pa_schema)
        self.assertEqual(actual, expected)

    def test_shard_uneven_distribution(self):
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

        # Test sharding into 3 parts: 2, 2, 3 rows
        plan1 = read_builder.new_scan().with_shard(0, 3).plan()
        actual1 = table_read.to_arrow_slice(plan1).sort_by('user_id')
        expected1 = pa.Table.from_pydict({
            'user_id': [1, 2],
            'item_id': [1001, 1002],
            'behavior': ['a', 'b'],
            'dt': ['p1', 'p1'],
        }, schema=self.pa_schema)
        self.assertEqual(actual1, expected1)

        plan2 = read_builder.new_scan().with_shard(1, 3).plan()
        actual2 = table_read.to_arrow_slice(plan2).sort_by('user_id')
        expected2 = pa.Table.from_pydict({
            'user_id': [3, 4],
            'item_id': [1003, 1004],
            'behavior': ['c', 'd'],
            'dt': ['p1', 'p1'],
        }, schema=self.pa_schema)
        self.assertEqual(actual2, expected2)

        plan3 = read_builder.new_scan().with_shard(2, 3).plan()
        actual3 = table_read.to_arrow_slice(plan3).sort_by('user_id')
        expected3 = pa.Table.from_pydict({
            'user_id': [5, 6, 7],
            'item_id': [1005, 1006, 1007],
            'behavior': ['e', 'f', 'g'],
            'dt': ['p1', 'p1', 'p1'],
        }, schema=self.pa_schema)
        self.assertEqual(actual3, expected3)

    def test_shard_single_shard(self):
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
        actual = table_read.to_arrow_slice(plan).sort_by('user_id')
        expected = pa.Table.from_pydict(data, schema=self.pa_schema)
        self.assertEqual(actual, expected)

    def test_shard_many_small_shards(self):
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
            actual = table_read.to_arrow_slice(plan)
            self.assertEqual(len(actual), 1)
            self.assertEqual(actual['user_id'][0].as_py(), i + 1)

    def test_shard_boundary_conditions(self):
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

        # Test first shard (0, 4) - should get 1 row (5//4 = 1)
        plan = read_builder.new_scan().with_shard(0, 4).plan()
        actual = table_read.to_arrow_slice(plan)
        self.assertEqual(len(actual), 1)

        # Test middle shard (1, 4) - should get 1 row
        plan = read_builder.new_scan().with_shard(1, 4).plan()
        actual = table_read.to_arrow_slice(plan)
        self.assertEqual(len(actual), 1)

        # Test last shard (3, 4) - should get 2 rows (remainder goes to last shard)
        plan = read_builder.new_scan().with_shard(3, 4).plan()
        actual = table_read.to_arrow_slice(plan)
        self.assertEqual(len(actual), 2)

    def test_shard_parameter_validation(self):
        """Test edge cases for parameter validation"""
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.rest_catalog.create_table('default.test_shard_validation_edge', schema, False)
        table = self.rest_catalog.get_table('default.test_shard_validation_edge')

        read_builder = table.new_read_builder()
        # Test invalid case with number_of_para_subtasks = 1
        with self.assertRaises(Exception) as context:
            read_builder.new_scan().with_shard(1, 1).plan()
        self.assertEqual(str(context.exception), "idx_of_this_subtask must be less than number_of_para_subtasks")

    def test_shard_primary_key_table_not_supported(self):
        """Test that slice operation is not supported on primary key tables"""
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['user_id'], primary_keys=['user_id', 'dt'])
        self.rest_catalog.create_table('default.test_slice_pk_not_supported', schema, False)
        table = self.rest_catalog.get_table('default.test_slice_pk_not_supported')

        read_builder = table.new_read_builder()

        # Test that slice operation raises exception on primary key table
        with self.assertRaises(Exception) as context:
            read_builder.new_scan().with_shard(0, 5).plan()

        self.assertEqual(str(context.exception), "primary key table not support shard")

    def test_postpone_write(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['user_id'], primary_keys=['user_id', 'dt'],
                                            options={'bucket': -2})
        self.rest_catalog.create_table('default.test_postpone', schema, False)
        table = self.rest_catalog.get_table('default.test_postpone')

        expect = pa.Table.from_pydict(self.data, schema=self.pa_schema)

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(expect)
        commit_messages = table_write.prepare_commit()
        table_commit.commit(commit_messages)
        table_write.close()
        table_commit.close()

        self.assertTrue(os.path.exists(self.warehouse + "/default/test_postpone/snapshot/LATEST"))
        self.assertTrue(os.path.exists(self.warehouse + "/default/test_postpone/snapshot/snapshot-1"))
        self.assertTrue(os.path.exists(self.warehouse + "/default/test_postpone/manifest"))
        self.assertEqual(len(glob.glob(self.warehouse + "/default/test_postpone/manifest/*")), 3)
        self.assertEqual(len(glob.glob(self.warehouse + "/default/test_postpone/user_id=2/bucket-postpone/*.avro")), 1)

    def test_postpone_read_write(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['user_id'], primary_keys=['user_id', 'dt'],
                                            options={'bucket': -2})
        self.rest_catalog.create_table('default.test_postpone', schema, False)
        table = self.rest_catalog.get_table('default.test_postpone')

        expect = pa.Table.from_pydict(self.data, schema=self.pa_schema)

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(expect)
        commit_messages = table_write.prepare_commit()
        table_commit.commit(commit_messages)
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        actual = table_read.to_arrow(splits)
        self.assertTrue(not actual)
