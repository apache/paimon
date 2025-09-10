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
from pypaimon.table.row.row_kind import RowKind

from pypaimon.table.row.generic_row import GenericRow, GenericRowSerializer, GenericRowDeserializer

from pypaimon.schema.data_types import DataField, AtomicType

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
        simple_pa_schema = pa.schema([
            ('f0', pa.int32()),
            ('f1', pa.string())
        ])
        schema = Schema.from_pyarrow_schema(simple_pa_schema, partition_keys=['f0'],
                                            options={'dynamic-partition-overwrite': 'false'})
        self.catalog.create_table('default.test_overwrite', schema, False)
        table = self.catalog.get_table('default.test_overwrite')
        read_builder = table.new_read_builder()

        # test normal write
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        df0 = pd.DataFrame({
            'f0': [1, 2],
            'f1': ['apple', 'banana'],
        })

        table_write.write_pandas(df0)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        actual_df0 = table_read.to_pandas(table_scan.plan().splits()).sort_values(by='f0')
        df0['f0'] = df0['f0'].astype('int32')
        pd.testing.assert_frame_equal(
            actual_df0.reset_index(drop=True), df0.reset_index(drop=True))

        # test partially overwrite
        write_builder = table.new_batch_write_builder().overwrite({'f0': 1})
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        df1 = pd.DataFrame({
            'f0': [1],
            'f1': ['watermelon'],
        })

        table_write.write_pandas(df1)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        actual_df1 = table_read.to_pandas(table_scan.plan().splits()).sort_values(by='f0')
        expected_df1 = pd.DataFrame({
            'f0': [1, 2],
            'f1': ['watermelon', 'banana']
        })
        expected_df1['f0'] = expected_df1['f0'].astype('int32')
        pd.testing.assert_frame_equal(
            actual_df1.reset_index(drop=True), expected_df1.reset_index(drop=True))

        # test fully overwrite
        write_builder = table.new_batch_write_builder().overwrite()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        df2 = pd.DataFrame({
            'f0': [3],
            'f1': ['Neo'],
        })

        table_write.write_pandas(df2)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        actual_df2 = table_read.to_pandas(table_scan.plan().splits())
        df2['f0'] = df2['f0'].astype('int32')
        pd.testing.assert_frame_equal(
            actual_df2.reset_index(drop=True), df2.reset_index(drop=True))

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

    def test_to_bytes_with_long_string(self):
        """Test serialization of strings longer than 7 bytes which require variable part storage."""
        # Create fields with a long string value
        fields = [
            DataField(0, "long_string", AtomicType("STRING")),
        ]

        # String longer than 7 bytes will be stored in variable part
        long_string = "This is a long string that exceeds 7 bytes"
        values = [long_string]

        binary_row = GenericRow(values, fields, RowKind.INSERT)
        serialized_bytes = GenericRowSerializer.to_bytes(binary_row)

        # Verify the last 6 bytes are 0
        # This is because the variable part data is rounded to the nearest word (8 bytes)
        # The last 6 bytes check is to ensure proper padding
        self.assertEqual(serialized_bytes[-6:], b'\x00\x00\x00\x00\x00\x00')
        self.assertEqual(serialized_bytes[20:62].decode('utf-8'), long_string)
        # Deserialize to verify
        deserialized_row = GenericRowDeserializer.from_bytes(serialized_bytes, fields)

        self.assertEqual(deserialized_row.values[0], long_string)
        self.assertEqual(deserialized_row.row_kind, RowKind.INSERT)
