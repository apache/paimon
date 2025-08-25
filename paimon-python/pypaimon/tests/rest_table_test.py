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

from pypaimon.schema.schema import Schema
from pypaimon.tests.rest_catalog_base_test import RESTCatalogBaseTest
from pypaimon.write.row_key_extractor import (DynamicBucketRowKeyExtractor,
                                              FixedBucketRowKeyExtractor,
                                              UnawareBucketRowKeyExtractor)


class RESTTableTest(RESTCatalogBaseTest):
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
