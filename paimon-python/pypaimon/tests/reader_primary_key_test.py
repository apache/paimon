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
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory
from pypaimon import Schema


class PkReaderTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({
            'warehouse': cls.warehouse
        })
        cls.catalog.create_database('default', False)

        cls.pa_schema = pa.schema([
            pa.field('user_id', pa.int32(), nullable=False),
            ('item_id', pa.int64()),
            ('behavior', pa.string()),
            pa.field('dt', pa.string(), nullable=False)
        ])
        cls.expected = pa.Table.from_pydict({
            'user_id': [1, 2, 3, 4, 5, 7, 8],
            'item_id': [1001, 1002, 1003, 1004, 1005, 1007, 1008],
            'behavior': ['a', 'b-new', 'c', None, 'e', 'g', 'h'],
            'dt': ['p1', 'p1', 'p2', 'p1', 'p2', 'p1', 'p2'],
        }, schema=cls.pa_schema)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def test_pk_parquet_reader(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema,
                                            partition_keys=['dt'],
                                            primary_keys=['user_id', 'dt'],
                                            options={'bucket': '2'})
        self.catalog.create_table('default.test_pk_parquet', schema, False)
        table = self.catalog.get_table('default.test_pk_parquet')
        self._write_test_table(table)

        read_builder = table.new_read_builder()
        actual = self._read_test_table(read_builder).sort_by('user_id')
        self.assertEqual(actual, self.expected)

    def test_pk_orc_reader(self):
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
        actual: pa.Table = self._read_test_table(read_builder).sort_by('user_id')

        # when bucket=1, actual field name will contain 'not null', so skip comparing field name
        for i in range(len(actual.columns)):
            col_a = actual.column(i)
            col_b = self.expected.column(i)
            self.assertEqual(col_a, col_b)

    def test_pk_avro_reader(self):
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
        actual = self._read_test_table(read_builder).sort_by('user_id')
        self.assertEqual(actual, self.expected)

    def test_pk_multi_write_once_commit(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema,
                                            partition_keys=['dt'],
                                            primary_keys=['user_id', 'dt'],
                                            options={'bucket': '2'})
        self.catalog.create_table('default.test_pk_multi', schema, False)
        table = self.catalog.get_table('default.test_pk_multi')
        write_builder = table.new_batch_write_builder()

        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data1 = {
            'user_id': [1, 2, 3, 4],
            'item_id': [1001, 1002, 1003, 1004],
            'behavior': ['a', 'b', 'c', None],
            'dt': ['p1', 'p1', 'p2', 'p1'],
        }
        pa_table1 = pa.Table.from_pydict(data1, schema=self.pa_schema)
        data2 = {
            'user_id': [5, 2, 7, 8],
            'item_id': [1005, 1002, 1007, 1008],
            'behavior': ['e', 'b-new', 'g', 'h'],
            'dt': ['p2', 'p1', 'p1', 'p2']
        }
        pa_table2 = pa.Table.from_pydict(data2, schema=self.pa_schema)

        table_write.write_arrow(pa_table1)
        table_write.write_arrow(pa_table2)

        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        actual = self._read_test_table(read_builder).sort_by('user_id')
        # Primary key deduplication keeps the latest record for each primary key
        expected = pa.Table.from_pydict({
            'user_id': [1, 2, 3, 4, 5, 7, 8],
            'item_id': [1001, 1002, 1003, 1004, 1005, 1007, 1008],
            'behavior': ['a', 'b-new', 'c', None, 'e', 'g', 'h'],
            'dt': ['p1', 'p1', 'p2', 'p1', 'p2', 'p1', 'p2'],
        }, schema=self.pa_schema)
        self.assertEqual(actual, expected)

    def test_pk_reader_with_filter(self):
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
        actual = self._read_test_table(read_builder).sort_by('user_id')
        expected = pa.concat_tables([
            self.expected.slice(1, 1),  # 2/b
            self.expected.slice(5, 1)  # 7/g
        ])
        self.assertEqual(actual, expected)

    def test_pk_reader_with_projection(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema,
                                            partition_keys=['dt'],
                                            primary_keys=['user_id', 'dt'],
                                            options={'bucket': '2'})
        self.catalog.create_table('default.test_pk_projection', schema, False)
        table = self.catalog.get_table('default.test_pk_projection')
        self._write_test_table(table)

        read_builder = table.new_read_builder().with_projection(['dt', 'user_id', 'behavior'])
        actual = self._read_test_table(read_builder).sort_by('user_id')
        expected = self.expected.select(['dt', 'user_id', 'behavior'])
        self.assertEqual(actual, expected)

    def _write_test_table(self, table):
        write_builder = table.new_batch_write_builder()

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

        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data1 = {
            'user_id': [5, 2, 7, 8],
            'item_id': [1005, 1002, 1007, 1008],
            'behavior': ['e', 'b-new', 'g', 'h'],
            'dt': ['p2', 'p1', 'p1', 'p2']
        }
        pa_table = pa.Table.from_pydict(data1, schema=self.pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

    def _read_test_table(self, read_builder):
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        return table_read.to_arrow(splits)
