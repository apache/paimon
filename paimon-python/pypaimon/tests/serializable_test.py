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

import pickle
import unittest
import pyarrow as pa

from pypaimon.schema.schema import Schema
from pypaimon.tests import TestCatalogBase
from pypaimon.common.pyarrow_compat import table_sort_by


class SerializableTest(TestCatalogBase):

    def setUp(self):
        super().setUp()
        self.pa_schema = pa.schema([
            ('user_id', pa.int64()),
            ('item_id', pa.int64()),
            ('behavior', pa.string()),
            ('dt', pa.string())
        ])
        self.expected = pa.Table.from_pydict({
            'user_id': [1, 2, 3, 4, 5, 6],
            'item_id': [1001, 1002, 1003, 1004, 1005, 1006],
            'behavior': ['a', 'b', 'c', None, 'e', 'f'],
            'dt': ['p1', 'p1', 'p2', 'p1', 'p2', 'p1']
        }, schema=self.pa_schema)

    def testPickleSerializable(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema,
                                            partition_keys=['dt'],
                                            primary_keys=['user_id', 'dt'],
                                            options={'bucket': '2'})
        self.catalog.create_table('default.test_pickle_serializable', schema, False)
        table = self.catalog.get_table('default.test_pickle_serializable')
        self._write_test_table(table)
        read_builder = table.new_read_builder()

        table_read = read_builder.new_read()
        pickled_table_read = pickle.dumps(table_read)
        table_read = pickle.loads(pickled_table_read)

        splits = read_builder.new_scan().plan().splits()
        pickled_splits = pickle.dumps(splits)
        splits = pickle.loads(pickled_splits)

        actual = table_sort_by(table_read.to_arrow(splits), 'user_id')
        self.assertEqual(actual, self.expected)

    def _write_test_table(self, table):
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        pa_table = pa.Table.from_pydict({
            'user_id': [1, 2, 3, 4, 5, 6],
            'item_id': [1001, 1002, 1003, 1004, 1005, 1006],
            'behavior': ['a', 'b', 'c', None, 'e', 'f'],
            'dt': ['p1', 'p1', 'p2', 'p1', 'p2', 'p1']
        }, schema=self.pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()


if __name__ == '__main__':
    unittest.main()
