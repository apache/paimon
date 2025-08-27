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

import unittest
import pyarrow as pa

from pypaimon.schema.schema import Schema
from pypaimon.tests.rest_catalog_base_test import RESTCatalogBaseTest
from pypaimon.write.row_key_extractor import FixedBucketRowKeyExtractor, UnawareBucketRowKeyExtractor
from pypaimon.common.pyarrow_compat import table_sort_by


class RESTTableTest(RESTCatalogBaseTest):

    def setUp(self):
        super().setUp()
        self.pa_schema = pa.schema([
            ('user_id', pa.int64()),
            ('item_id', pa.int64()),
            ('behavior', pa.string()),
            ('dt', pa.string())
        ])
        self.data = {
            'user_id': [1, 2, 3, 4, 5, 6],
            'item_id': [1001, 1002, 1003, 1004, 1005, 1006],
            'behavior': ['a', 'b', 'c', None, 'e', 'f'],
            'dt': ['p1', 'p1', 'p2', 'p1', 'p2', 'p1']
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

        self.assertEqual(table_sort_by(actual, 'user_id'), self.expected)

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
        self.assertEqual(table_sort_by(actual, "user_id"), self.expected)

    def test_with_shard_pk_dynamic_bucket(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['user_id'], primary_keys=['user_id', 'dt'])
        self.rest_catalog.create_table('default.test_with_shard', schema, False)
        table = self.rest_catalog.get_table('default.test_with_shard')

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

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
        self.assertEqual(actual, self.expected)


if __name__ == '__main__':
    unittest.main()
