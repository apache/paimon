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

import pandas as pd
import pyarrow as pa

from pypaimon.catalog.catalog_factory import CatalogFactory
from pypaimon.schema.schema import Schema


class ReaderBasicTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({
            'warehouse': cls.warehouse
        })
        cls.catalog.create_database('default', False)

        cls.pa_schema = pa.schema([
            ('user_id', pa.int64()),
            ('item_id', pa.int64()),
            ('behavior', pa.string()),
            ('dt', pa.string())
        ])
        cls.raw_data = {
            'user_id': [1, 2, 3, 4, 5],
            'item_id': [1001, 1002, 1003, 1004, 1005],
            'behavior': ['a', 'b', 'c', None, 'e'],
            'dt': ['p1', 'p1', 'p1', 'p1', 'p2'],
        }
        cls.expected = pa.Table.from_pydict(cls.raw_data, schema=cls.pa_schema)

        schema = Schema.from_pyarrow_schema(cls.pa_schema)
        cls.catalog.create_table('default.test_reader_iterator', schema, False)
        cls.table = cls.catalog.get_table('default.test_reader_iterator')
        write_builder = cls.table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(cls.expected)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def test_overwrite(self):
        pass
        # TODO: support overwrite

    def testWriteWrongSchema(self):
        self.catalog.create_table('default.test_wrong_schema',
                                  Schema.from_pyarrow_schema(self.pa_schema),
                                  False)
        table = self.catalog.get_table('default.test_wrong_schema')

        data = {
            'f0': [1, 2, 3],
            'f1': ['a', 'b', 'c'],
        }
        df = pd.DataFrame(data)
        schema = pa.schema([
            ('f0', pa.int64()),
            ('f1', pa.string())
        ])
        record_batch = pa.RecordBatch.from_pandas(df, schema)

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()

        with self.assertRaises(ValueError) as e:
            table_write.write_arrow_batch(record_batch)
        self.assertTrue(str(e.exception).startswith("Input schema isn't consistent with table schema."))

    def testReaderIterator(self):
        read_builder = self.table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        iterator = table_read.to_iterator(splits)
        result = []
        value = next(iterator, None)
        while value is not None:
            result.append(value.get_field(1))
            value = next(iterator, None)
        self.assertEqual(result, [1001, 1002, 1003, 1004, 1005])

    def testReaderDuckDB(self):
        read_builder = self.table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        duckdb_con = table_read.to_duckdb(splits, 'duckdb_table')
        actual = duckdb_con.query("SELECT * FROM duckdb_table").fetchdf()
        expect = pd.DataFrame(self.raw_data)
        pd.testing.assert_frame_equal(actual.reset_index(drop=True), expect.reset_index(drop=True))
