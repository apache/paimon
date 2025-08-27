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
import tempfile
import unittest

import pyarrow as pa

from pypaimon.catalog.catalog_factory import CatalogFactory
from pypaimon.schema.schema import Schema


class AoReaderTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({
            'warehouse': cls.warehouse
        })
        cls.catalog.create_database('default', False)

        cls.pa_schema = pa.schema([
            ('user_id', pa.int32()),
            ('item_id', pa.int64()),
            ('behavior', pa.string()),
            ('dt', pa.string())
        ])
        cls.expected = pa.Table.from_pydict({
            'user_id': [1, 2, 3, 4, 5, 6, 7, 8],
            'item_id': [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008],
            'behavior': ['a', 'b', 'c', None, 'e', 'f', 'g', 'h'],
            'dt': ['p1', 'p1', 'p2', 'p1', 'p2', 'p1', 'p2', 'p2'],
        }, schema=cls.pa_schema)

    def testParquetAppendOnlyReader(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.catalog.create_table('default.test_append_only_parquet', schema, False)
        table = self.catalog.get_table('default.test_append_only_parquet')
        self._write_test_table(table)

        read_builder = table.new_read_builder()
        actual = self._read_test_table(read_builder).sort_by('user_id')
        self.assertEqual(actual, self.expected)

    def testOrcAppendOnlyReader(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'], options={'file.format': 'orc'})
        self.catalog.create_table('default.test_append_only_orc', schema, False)
        table = self.catalog.get_table('default.test_append_only_orc')
        self._write_test_table(table)

        read_builder = table.new_read_builder()
        actual = self._read_test_table(read_builder).sort_by('user_id')
        self.assertEqual(actual, self.expected)

    def testAvroAppendOnlyReader(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'], options={'file.format': 'avro'})
        self.catalog.create_table('default.test_append_only_avro', schema, False)
        table = self.catalog.get_table('default.test_append_only_avro')
        self._write_test_table(table)

        read_builder = table.new_read_builder()
        actual = self._read_test_table(read_builder).sort_by('user_id')
        self.assertEqual(actual, self.expected)

    def testAppendOnlyReaderWithFilter(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.catalog.create_table('default.test_append_only_filter', schema, False)
        table = self.catalog.get_table('default.test_append_only_filter')
        self._write_test_table(table)

        predicate_builder = table.new_read_builder().new_predicate_builder()
        p1 = predicate_builder.less_than('user_id', 7)
        p2 = predicate_builder.greater_or_equal('user_id', 2)
        p3 = predicate_builder.between('user_id', 0, 6)  # [2/b, 3/c, 4/d, 5/e, 6/f] left
        p4 = predicate_builder.is_not_in('behavior', ['b', 'e'])  # [3/c, 4/d, 6/f] left
        p5 = predicate_builder.is_in('dt', ['p1'])  # exclude 3/c
        p6 = predicate_builder.is_not_null('behavior')    # exclude 4/d
        g1 = predicate_builder.and_predicates([p1, p2, p3, p4, p5, p6])
        read_builder = table.new_read_builder().with_filter(g1)
        actual = self._read_test_table(read_builder)
        expected = pa.concat_tables([
            self.expected.slice(5, 1)  # 6/f
        ])
        self.assertEqual(actual.sort_by('user_id'), expected)

        p7 = predicate_builder.startswith('behavior', 'a')
        p10 = predicate_builder.equal('item_id', 1002)
        p11 = predicate_builder.is_null('behavior')
        p9 = predicate_builder.contains('behavior', 'f')
        p8 = predicate_builder.endswith('dt', 'p2')
        g2 = predicate_builder.or_predicates([p7, p8, p9, p10, p11])
        read_builder = table.new_read_builder().with_filter(g2)
        actual = self._read_test_table(read_builder)
        self.assertEqual(actual.sort_by('user_id'), self.expected)

        g3 = predicate_builder.and_predicates([g1, g2])
        read_builder = table.new_read_builder().with_filter(g3)
        actual = self._read_test_table(read_builder)
        expected = pa.concat_tables([
            self.expected.slice(5, 1)  # 6/f
        ])
        self.assertEqual(actual.sort_by('user_id'), expected)

        # Same as java, 'not_equal' will also filter records of 'None' value
        p12 = predicate_builder.not_equal('behavior', 'f')
        read_builder = table.new_read_builder().with_filter(p12)
        actual = self._read_test_table(read_builder)
        expected = pa.concat_tables([
            # not only 6/f, but also 4/d will be filtered
            self.expected.slice(0, 1),  # 1/a
            self.expected.slice(1, 1),  # 2/b
            self.expected.slice(2, 1),  # 3/c
            self.expected.slice(4, 1),  # 5/e
            self.expected.slice(6, 1),  # 7/g
            self.expected.slice(7, 1),  # 8/h
        ])
        self.assertEqual(actual.sort_by('user_id'), expected)

    def testAppendOnlyReaderWithProjection(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.catalog.create_table('default.test_append_only_projection', schema, False)
        table = self.catalog.get_table('default.test_append_only_projection')
        self._write_test_table(table)

        read_builder = table.new_read_builder().with_projection(['dt', 'user_id'])
        actual = self._read_test_table(read_builder).sort_by('user_id')
        expected = self.expected.select(['dt', 'user_id'])
        self.assertEqual(actual, expected)

    def testAvroAppendOnlyReaderWithProjection(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'], options={'file.format': 'avro'})
        self.catalog.create_table('default.test_avro_append_only_projection', schema, False)
        table = self.catalog.get_table('default.test_avro_append_only_projection')
        self._write_test_table(table)

        read_builder = table.new_read_builder().with_projection(['dt', 'user_id'])
        actual = self._read_test_table(read_builder).sort_by('user_id')
        expected = self.expected.select(['dt', 'user_id'])
        self.assertEqual(actual, expected)

    def testAppendOnlyReaderWithLimit(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.catalog.create_table('default.test_append_only_limit', schema, False)
        table = self.catalog.get_table('default.test_append_only_limit')
        self._write_test_table(table)

        read_builder = table.new_read_builder().with_limit(1)
        actual = self._read_test_table(read_builder)
        # only records from 1st commit (1st split) will be read
        # might be split of "dt=1" or split of "dt=2"
        self.assertEqual(actual.num_rows, 4)

    def _write_test_table(self, table):
        write_builder = table.new_batch_write_builder()

        # first write
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data1 = {
            'user_id': [1, 2, 3, 4],
            'item_id': [1001, 1002, 1003, 1004],
            'behavior': ['a', 'b', 'c', None],
            'dt': ['p1', 'p1', 'p2', 'p1'],
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

    def _read_test_table(self, read_builder):
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        return table_read.to_arrow(splits)
