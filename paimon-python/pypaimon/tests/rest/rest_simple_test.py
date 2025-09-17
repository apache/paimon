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
from pypaimon.write.row_key_extractor import (DynamicBucketRowKeyExtractor,
                                              FixedBucketRowKeyExtractor,
                                              UnawareBucketRowKeyExtractor)


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

    def test_with_slice_ao_unaware_bucket(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.rest_catalog.create_table('default.test_with_slice_ao_unaware_bucket', schema, False)
        table = self.rest_catalog.get_table('default.test_with_slice_ao_unaware_bucket')
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
        plan = read_builder.new_scan().with_slice(10, 5).plan()
        actual = table_read.to_arrow_slice(plan).sort_by('user_id')
        expected = pa.Table.from_pydict({
            'user_id': [5, 7, 9, 11, 13],
            'item_id': [1005, 1007, 1009, 1011, 1013],
            'behavior': ['e', 'f', 'h', 'j', 'l'],
            'dt': ['p2', 'p2', 'p2', 'p2', 'p2'],
        }, schema=self.pa_schema)
        self.assertEqual(actual, expected)

        # Get the three actual tables
        plan1 = read_builder.new_scan().with_slice(0, 5).plan()
        actual1 = table_read.to_arrow_slice(plan1).sort_by('user_id')
        plan2 = read_builder.new_scan().with_slice(5, 9).plan()
        actual2 = table_read.to_arrow_slice(plan2).sort_by('user_id')
        plan3 = read_builder.new_scan().with_slice(14, 100).plan()
        actual3 = table_read.to_arrow_slice(plan3).sort_by('user_id')

        # Concatenate the three tables
        actual = pa.concat_tables([actual1, actual2, actual3]).sort_by('user_id')
        expected = self._read_test_table(read_builder).sort_by('user_id')
        self.assertEqual(actual, expected)

    def test_with_slice_ao_fixed_bucket(self):
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
        plan = read_builder.new_scan().with_slice(10, 5).plan()
        actual = table_read.to_arrow_slice(plan).sort_by('user_id')
        expected = pa.Table.from_pydict({
            'user_id': [5, 6, 7, 10, 13],
            'item_id': [1005, 1006, 1007, 1010, 1013],
            'behavior': ['e', 'e', 'f', 'i', 'l'],
            'dt': ['p2', 'p1', 'p2', 'p1', 'p2'],
        }, schema=self.pa_schema)
        self.assertEqual(actual, expected)

        # Get the three actual tables
        plan1 = read_builder.new_scan().with_slice(0, 5).plan()
        actual1 = table_read.to_arrow_slice(plan1).sort_by('user_id')
        plan2 = read_builder.new_scan().with_slice(5, 9).plan()
        actual2 = table_read.to_arrow_slice(plan2).sort_by('user_id')
        plan3 = read_builder.new_scan().with_slice(14, 100).plan()
        actual3 = table_read.to_arrow_slice(plan3).sort_by('user_id')

        # Concatenate the three tables
        actual = pa.concat_tables([actual1, actual2, actual3]).sort_by('user_id')
        expected = self._read_test_table(read_builder).sort_by('user_id')
        self.assertEqual(actual, expected)

    def test_slice_from_beginning(self):
        """Test slicing from the beginning of the table"""
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.rest_catalog.create_table('default.test_slice_beginning', schema, False)
        table = self.rest_catalog.get_table('default.test_slice_beginning')
        self._write_test_table(table)

        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()

        # Test slice from start (0, 3)
        plan = read_builder.new_scan().with_slice(0, 3).plan()
        actual = table_read.to_arrow_slice(plan).sort_by('user_id')

        expected = pa.Table.from_pydict({
            'user_id': [1, 2, 4],
            'item_id': [1001, 1002, 1004],
            'behavior': ['a', 'b', None],
            'dt': ['p1', 'p1', 'p1'],
        }, schema=self.pa_schema)
        self.assertEqual(actual, expected)

    def test_slice_middle_range(self):
        """Test slicing from the middle of the table"""
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.rest_catalog.create_table('default.test_slice_middle', schema, False)
        table = self.rest_catalog.get_table('default.test_slice_middle')
        self._write_test_table(table)

        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()

        # Test slice from middle (3, 4)
        plan = read_builder.new_scan().with_slice(3, 4).plan()
        actual = table_read.to_arrow_slice(plan).sort_by('user_id')

        expected = pa.Table.from_pydict({
            'user_id': [3, 5, 7, 8],
            'item_id': [1003, 1005, 1007, 1008],
            'behavior': ['c', 'e', 'g', 'h'],
            'dt': ['p2', 'p2', 'p2', 'p2'],
        }, schema=self.pa_schema)
        self.assertEqual(actual, expected)

    def test_slice_to_end(self):
        """Test slicing to the end of the table"""
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.rest_catalog.create_table('default.test_slice_end', schema, False)
        table = self.rest_catalog.get_table('default.test_slice_end')
        self._write_test_table(table)

        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()

        # Test slice to end (6, 100) - should get remaining rows
        plan = read_builder.new_scan().with_slice(6, 100).plan()
        actual = table_read.to_arrow_slice(plan).sort_by('user_id')

        expected = pa.Table.from_pydict({
            'user_id': [6, 8],
            'item_id': [1006, 1008],
            'behavior': ['f', 'h'],
            'dt': ['p1', 'p2'],
        }, schema=self.pa_schema)
        self.assertEqual(actual, expected)

    def test_slice_single_row(self):
        """Test slicing a single row"""
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.rest_catalog.create_table('default.test_slice_single', schema, False)
        table = self.rest_catalog.get_table('default.test_slice_single')
        self._write_test_table(table)

        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()

        # Test single row slice (2, 1)
        plan = read_builder.new_scan().with_slice(2, 1).plan()
        actual = table_read.to_arrow_slice(plan).sort_by('user_id')

        expected = pa.Table.from_pydict({
            'user_id': [4],
            'item_id': [1004],
            'behavior': [None],
            'dt': ['p1'],
        }, schema=self.pa_schema)
        self.assertEqual(actual, expected)

    def test_slice_empty_result(self):
        """Test slicing beyond table bounds returns empty result"""
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.rest_catalog.create_table('default.test_slice_empty', schema, False)
        table = self.rest_catalog.get_table('default.test_slice_empty')
        self._write_test_table(table)

        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()

        # Test slice beyond bounds (100, 5)
        plan = read_builder.new_scan().with_slice(100, 5).plan()
        actual = table_read.to_arrow_slice(plan)

        # Should return empty table with correct schema
        self.assertEqual(len(actual), 0)
        self.assertEqual(actual.schema, self.pa_schema)

    def test_slice_zero_count(self):
        """Test slicing with zero count"""
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.rest_catalog.create_table('default.test_slice_zero', schema, False)
        table = self.rest_catalog.get_table('default.test_slice_zero')
        self._write_test_table(table)

        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()

        # Test zero count slice (3, 0)
        plan = read_builder.new_scan().with_slice(3, 0).plan()
        actual = table_read.to_arrow_slice(plan)

        # Should return empty table with correct schema
        self.assertEqual(len(actual), 0)
        self.assertEqual(actual.schema, self.pa_schema)

    def test_slice_multiple_consecutive(self):
        """Test multiple consecutive slices cover the entire table"""
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.rest_catalog.create_table('default.test_slice_consecutive', schema, False)
        table = self.rest_catalog.get_table('default.test_slice_consecutive')
        self._write_test_table(table)

        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()

        # Test consecutive slices that cover entire table
        plan1 = read_builder.new_scan().with_slice(0, 2).plan()
        actual1 = table_read.to_arrow_slice(plan1).sort_by('user_id')

        plan2 = read_builder.new_scan().with_slice(2, 3).plan()
        actual2 = table_read.to_arrow_slice(plan2).sort_by('user_id')

        plan3 = read_builder.new_scan().with_slice(5, 3).plan()
        actual3 = table_read.to_arrow_slice(plan3).sort_by('user_id')

        # Concatenate all slices
        actual_combined = pa.concat_tables([actual1, actual2, actual3]).sort_by('user_id')
        expected_full = self._read_test_table(read_builder).sort_by('user_id')

        self.assertEqual(actual_combined, expected_full)

    def test_slice_large_dataset_500_records(self):
        """Test slicing a large dataset with 500 records split into 3 equal parts"""
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.rest_catalog.create_table('default.test_slice_large_500', schema, False)
        table = self.rest_catalog.get_table('default.test_slice_large_500')

        # Create 500 records of test data
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        # Generate 500 records
        data_500 = {
            'user_id': list(range(1, 501)),  # 1 to 500
            'item_id': [1000 + i for i in range(1, 501)],  # 1001 to 1500
            'behavior': [chr(ord('a') + (i % 26)) for i in range(500)],  # cycling through a-z
            'dt': ['p1' if i % 2 == 0 else 'p2' for i in range(500)],  # alternating p1, p2
        }

        pa_table = pa.Table.from_pydict(data_500, schema=self.pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()

        # Split 500 records into 3 equal parts: 167, 167, 166
        # Part 1: records 0-166 (167 records)
        plan1 = read_builder.new_scan().with_slice(0, 167).plan()
        actual1 = table_read.to_arrow_slice(plan1).sort_by('user_id')

        # Part 2: records 167-333 (167 records)
        plan2 = read_builder.new_scan().with_slice(167, 167).plan()
        actual2 = table_read.to_arrow_slice(plan2).sort_by('user_id')

        # Part 3: records 334-499 (166 records)
        plan3 = read_builder.new_scan().with_slice(334, 166).plan()
        actual3 = table_read.to_arrow_slice(plan3).sort_by('user_id')

        # Verify each part has the correct number of records
        self.assertEqual(len(actual1), 167, "First part should have 167 records")
        self.assertEqual(len(actual2), 167, "Second part should have 167 records")
        self.assertEqual(len(actual3), 166, "Third part should have 166 records")

        # Concatenate all three parts and verify it matches the full dataset
        actual_combined = pa.concat_tables([actual1, actual2, actual3]).sort_by('user_id')
        expected_full = self._read_test_table(read_builder).sort_by('user_id')

        self.assertEqual(len(actual_combined), 500, "Combined result should have 500 records")
        self.assertEqual(actual_combined, expected_full, "Combined slices should match full dataset")

        # Verify no data loss or duplication
        combined_user_ids = actual_combined['user_id'].to_pylist()
        expected_user_ids = list(range(1, 501))
        self.assertEqual(sorted(combined_user_ids), expected_user_ids, "All user IDs should be present and unique")

    def test_slice_primary_key_table_not_supported(self):
        """Test that slice operation is not supported on primary key tables"""
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['user_id'], primary_keys=['user_id', 'dt'])
        self.rest_catalog.create_table('default.test_slice_pk_not_supported', schema, False)
        table = self.rest_catalog.get_table('default.test_slice_pk_not_supported')

        read_builder = table.new_read_builder()

        # Test that slice operation raises exception on primary key table
        with self.assertRaises(Exception) as context:
            read_builder.new_scan().with_slice(0, 5).plan()

        self.assertEqual(str(context.exception), "primary key table not support slice")

    def test_with_shard_ao_unaware_bucket(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['user_id'])
        self.rest_catalog.create_table('default.test_with_shard', schema, False)
        table = self.rest_catalog.get_table('default.test_with_shard')

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        self.assertIsInstance(table_write.row_key_extractor, UnawareBucketRowKeyExtractor)

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

        self.assertEqual(actual.sort_by('user_id'), self.expected)

    def test_with_shard_ao_fixed_bucket(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['user_id'],
                                            options={'bucket': '5', 'bucket-key': 'item_id'})
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
        self.assertEqual(actual.sort_by("user_id"), self.expected)

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
