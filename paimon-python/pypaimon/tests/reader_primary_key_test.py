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

import unittest
import pyarrow as pa

from pypaimon.schema.schema import Schema
from pypaimon.tests import TestCatalogBase
from pypaimon.common.pyarrow_compat import table_sort_by


class PkReaderTest(TestCatalogBase):

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

    def testPkParquetReader(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema,
                                            partition_keys=['dt'],
                                            primary_keys=['user_id', 'dt'],
                                            options={'bucket': '2'})
        self.catalog.create_table('default.test_pk_parquet', schema, False)
        table = self.catalog.get_table('default.test_pk_parquet')
        self._write_test_table(table)

        read_builder = table.new_read_builder()
        actual = table_sort_by(self._read_test_table(read_builder), 'user_id')
        self.assertEqual(actual, self.expected)

    def testPkOrcReader(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema,
                                            partition_keys=['dt'],
                                            primary_keys=['user_id', 'dt'],
                                            options={
                                                'bucket': '1',
                                                'file.format': 'orc'
                                            })
        self.catalog.create_table('default.test_pk_orc', schema, False)
        table = self.catalog.get_table('default.test_pk_orc')
        self._write_test_table(table)

        read_builder = table.new_read_builder()
        actual: pa.Table = table_sort_by(self._read_test_table(read_builder), 'user_id')

        # when bucket=1, actual field name will contain 'not null', so skip comparing field name
        for i in range(len(actual.columns)):
            col_a = actual.column(i)
            col_b = self.expected.column(i)
            self.assertEqual(col_a.to_pylist(), col_b.to_pylist())

    def testPkAvroReader(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema,
                                            partition_keys=['dt'],
                                            primary_keys=['user_id', 'dt'],
                                            options={
                                                'bucket': '2',
                                                'file.format': 'avro'
                                            })
        self.catalog.create_table('default.test_pk_avro', schema, False)
        table = self.catalog.get_table('default.test_pk_avro')
        self._write_test_table(table)

        read_builder = table.new_read_builder()
        actual = table_sort_by(self._read_test_table(read_builder), 'user_id')
        self.assertEqual(actual, self.expected)

    def testPkReaderWithFilter(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema,
                                            partition_keys=['dt'],
                                            primary_keys=['user_id', 'dt'],
                                            options={'bucket': '2'})
        self.catalog.create_table('default.test_pk_filter', schema, False)
        table = self.catalog.get_table('default.test_pk_filter')
        self._write_test_table(table)

        predicate_builder = table.new_read_builder().new_predicate_builder()
        p1 = predicate_builder.is_in('dt', ['p1'])
        p2 = predicate_builder.between('user_id', 2, 7)
        p3 = predicate_builder.is_not_null('behavior')
        g1 = predicate_builder.and_predicates([p1, p2, p3])
        read_builder = table.new_read_builder().with_filter(g1)
        actual = table_sort_by(self._read_test_table(read_builder), 'user_id')
        expected = pa.concat_tables([
            self.expected.slice(1, 1),  # 2/b
            self.expected.slice(5, 1)   # 6/f
        ])
        self.assertEqual(actual, expected)

    def testPkReaderWithProjection(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema,
                                            partition_keys=['dt'],
                                            primary_keys=['user_id', 'dt'],
                                            options={'bucket': '2'})
        self.catalog.create_table('default.test_pk_projection', schema, False)
        table = self.catalog.get_table('default.test_pk_projection')
        self._write_test_table(table)

        read_builder = table.new_read_builder().with_projection(['dt', 'user_id', 'behavior'])
        actual = table_sort_by(self._read_test_table(read_builder), 'user_id')
        expected = self.expected.select(['dt', 'user_id', 'behavior'])
        self.assertEqual(actual, expected)

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

    def _read_test_table(self, read_builder):
        scan = read_builder.new_scan()
        read = read_builder.new_read()
        return read.to_arrow(scan.plan().splits())


if __name__ == '__main__':
    unittest.main()
