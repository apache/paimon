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
import time
import unittest

import numpy as np
import pandas as pd
import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.snapshot.snapshot_manager import SnapshotManager


class AoReaderTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({
            'warehouse': cls.warehouse
        })
        cls.catalog.create_database('default', True)

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

    def test_parquet_ao_reader(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.catalog.create_table('default.test_append_only_parquet', schema, False)
        table = self.catalog.get_table('default.test_append_only_parquet')
        self._write_test_table(table)

        read_builder = table.new_read_builder()
        actual = self._read_test_table(read_builder).sort_by('user_id')
        self.assertEqual(actual, self.expected)

    def test_orc_ao_reader(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'], options={'file.format': 'orc'})
        self.catalog.create_table('default.test_append_only_orc', schema, False)
        table = self.catalog.get_table('default.test_append_only_orc')
        self._write_test_table(table)

        read_builder = table.new_read_builder()
        actual = self._read_test_table(read_builder).sort_by('user_id')
        self.assertEqual(actual, self.expected)

    def test_avro_ao_reader(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'], options={'file.format': 'avro'})
        self.catalog.create_table('default.test_append_only_avro', schema, False)
        table = self.catalog.get_table('default.test_append_only_avro')
        self._write_test_table(table)

        read_builder = table.new_read_builder()
        actual = self._read_test_table(read_builder).sort_by('user_id')
        self.assertEqual(actual, self.expected)

    def test_lance_ao_reader(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'], options={'file.format': 'lance'})
        self.catalog.create_table('default.test_append_only_lance', schema, False)
        table = self.catalog.get_table('default.test_append_only_lance')
        self._write_test_table(table)

        read_builder = table.new_read_builder()
        actual = self._read_test_table(read_builder).sort_by('user_id')
        self.assertEqual(actual, self.expected)

    def test_lance_ao_reader_with_filter(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'], options={'file.format': 'lance'})
        self.catalog.create_table('default.test_append_only_lance_filter', schema, False)
        table = self.catalog.get_table('default.test_append_only_lance_filter')
        self._write_test_table(table)

        predicate_builder = table.new_read_builder().new_predicate_builder()
        p1 = predicate_builder.less_than('user_id', 7)
        p2 = predicate_builder.greater_or_equal('user_id', 2)
        p3 = predicate_builder.between('user_id', 0, 6)  # [2/b, 3/c, 4/d, 5/e, 6/f] left
        p4 = predicate_builder.is_not_in('behavior', ['b', 'e'])  # [3/c, 4/d, 6/f] left
        p5 = predicate_builder.is_in('dt', ['p1'])  # exclude 3/c
        p6 = predicate_builder.is_not_null('behavior')  # exclude 4/d
        g1 = predicate_builder.and_predicates([p1, p2, p3, p4, p5, p6])
        read_builder = table.new_read_builder().with_filter(g1)
        actual = self._read_test_table(read_builder)
        expected = pa.concat_tables([
            self.expected.slice(5, 1)  # 6/f
        ])
        self.assertEqual(actual.sort_by('user_id'), expected)

    def test_append_only_multi_write_once_commit(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.catalog.create_table('default.test_append_only_multi_once_commit', schema, False)
        table = self.catalog.get_table('default.test_append_only_multi_once_commit')
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
            'user_id': [5, 6, 7, 8],
            'item_id': [1005, 1006, 1007, 1008],
            'behavior': ['e', 'f', 'g', 'h'],
            'dt': ['p2', 'p1', 'p2', 'p2'],
        }
        pa_table2 = pa.Table.from_pydict(data2, schema=self.pa_schema)

        table_write.write_arrow(pa_table1)
        table_write.write_arrow(pa_table2)

        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        actual = self._read_test_table(read_builder).sort_by('user_id')
        self.assertEqual(actual, self.expected)

    def test_over_1000_cols_read(self):
        num_rows = 1
        num_cols = 10
        table_name = "default.testBug"
        # Generate dynamic schema based on column count
        schema_fields = []
        for i in range(1, num_cols + 1):
            col_name = f'c{i:03d}'
            if i == 1:
                schema_fields.append((col_name, pa.string()))  # ID column
            elif i == 2:
                schema_fields.append((col_name, pa.string()))  # Name column
            elif i == 3:
                schema_fields.append((col_name, pa.string()))  # Category column (partition key)
            elif i % 4 == 0:
                schema_fields.append((col_name, pa.float64()))  # Float columns
            elif i % 4 == 1:
                schema_fields.append((col_name, pa.int32()))  # Int columns
            elif i % 4 == 2:
                schema_fields.append((col_name, pa.string()))  # String columns
            else:
                schema_fields.append((col_name, pa.int64()))  # Long columns

        pa_schema = pa.schema(schema_fields)
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            partition_keys=['c003'],  # Use c003 as partition key
        )

        # Create table
        self.catalog.create_table(table_name, schema, False)
        table = self.catalog.get_table(table_name)

        # Generate test data
        np.random.seed(42)  # For reproducible results
        categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports', 'Food', 'Toys', 'Beauty', 'Health', 'Auto']
        statuses = ['Active', 'Inactive', 'Pending', 'Completed']

        # Generate data dictionary
        test_data = {}
        for i in range(1, num_cols + 1):
            col_name = f'c{i:03d}'
            if i == 1:
                test_data[col_name] = [f'Product_{j}' for j in range(1, num_rows + 1)]
            elif i == 2:
                test_data[col_name] = [f'Product_{j}' for j in range(1, num_rows + 1)]
            elif i == 3:
                test_data[col_name] = np.random.choice(categories, num_rows)
            elif i % 4 == 0:
                test_data[col_name] = np.random.uniform(1.0, 1000.0, num_rows).round(2)
            elif i % 4 == 1:
                test_data[col_name] = np.random.randint(1, 100, num_rows)
            elif i % 4 == 2:
                test_data[col_name] = np.random.choice(statuses, num_rows)
            else:
                test_data[col_name] = np.random.randint(1640995200, 1672531200, num_rows)

        test_df = pd.DataFrame(test_data)

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        table_write.write_pandas(test_df)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        result = table_read.to_pandas(table_scan.plan().splits())
        self.assertEqual(result.to_dict(), test_df.to_dict())

    def test_ao_reader_with_filter(self):
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
        p6 = predicate_builder.is_not_null('behavior')  # exclude 4/d
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

    def test_ao_reader_with_projection(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.catalog.create_table('default.test_append_only_projection', schema, False)
        table = self.catalog.get_table('default.test_append_only_projection')
        self._write_test_table(table)

        read_builder = table.new_read_builder().with_projection(['dt', 'user_id'])
        actual = self._read_test_table(read_builder).sort_by('user_id')
        expected = self.expected.select(['dt', 'user_id'])
        self.assertEqual(actual, expected)

    def test_avro_ao_reader_with_projection(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'], options={'file.format': 'avro'})
        self.catalog.create_table('default.test_avro_append_only_projection', schema, False)
        table = self.catalog.get_table('default.test_avro_append_only_projection')
        self._write_test_table(table)

        read_builder = table.new_read_builder().with_projection(['dt', 'user_id'])
        actual = self._read_test_table(read_builder).sort_by('user_id')
        expected = self.expected.select(['dt', 'user_id'])
        self.assertEqual(actual, expected)

    def test_ao_reader_with_limit(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.catalog.create_table('default.test_append_only_limit', schema, False)
        table = self.catalog.get_table('default.test_append_only_limit')
        self._write_test_table(table)

        read_builder = table.new_read_builder().with_limit(1)
        actual = self._read_test_table(read_builder)
        # only records from 1st commit (1st split) will be read
        # might be split of "dt=1" or split of "dt=2"
        self.assertEqual(actual.num_rows, 4)

    def test_incremental_timestamp(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.catalog.create_table('default.test_incremental_parquet', schema, False)
        table = self.catalog.get_table('default.test_incremental_parquet')
        timestamp = int(time.time() * 1000)
        self._write_test_table(table)

        snapshot_manager = SnapshotManager(table)
        t1 = snapshot_manager.get_snapshot_by_id(1).time_millis
        t2 = snapshot_manager.get_snapshot_by_id(2).time_millis
        # test 1
        table = table.copy({CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP.key(): str(timestamp - 1) + ',' + str(timestamp)})
        read_builder = table.new_read_builder()
        actual = self._read_test_table(read_builder)
        self.assertEqual(len(actual), 0)
        # test 2
        table = table.copy({CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP.key(): str(timestamp) + ',' + str(t2)})
        read_builder = table.new_read_builder()
        actual = self._read_test_table(read_builder).sort_by('user_id')
        self.assertEqual(self.expected, actual)
        # test 3
        table = table.copy({CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP.key(): str(t1) + ',' + str(t2)})
        read_builder = table.new_read_builder()
        actual = self._read_test_table(read_builder).sort_by('user_id')
        expected = self.expected.slice(4, 4)
        self.assertEqual(expected, actual)

    def test_incremental_read_multi_snapshots(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.catalog.create_table('default.test_incremental_100', schema, False)
        table = self.catalog.get_table('default.test_incremental_100')

        write_builder = table.new_batch_write_builder()
        for i in range(1, 101):
            table_write = write_builder.new_write()
            table_commit = write_builder.new_commit()
            pa_table = pa.Table.from_pydict({
                'user_id': [i],
                'item_id': [1000 + i],
                'behavior': [f'snap{i}'],
                'dt': ['p1' if i % 2 == 1 else 'p2'],
            }, schema=self.pa_schema)
            table_write.write_arrow(pa_table)
            table_commit.commit(table_write.prepare_commit())
            table_write.close()
            table_commit.close()

        snapshot_manager = SnapshotManager(table)
        t10 = snapshot_manager.get_snapshot_by_id(10).time_millis
        t20 = snapshot_manager.get_snapshot_by_id(20).time_millis

        table_inc = table.copy({CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP.key(): f"{t10},{t20}"})
        read_builder = table_inc.new_read_builder()
        actual = self._read_test_table(read_builder).sort_by('user_id')

        expected = pa.Table.from_pydict({
            'user_id': list(range(11, 21)),
            'item_id': [1000 + i for i in range(11, 21)],
            'behavior': [f'snap{i}' for i in range(11, 21)],
            'dt': ['p1' if i % 2 == 1 else 'p2' for i in range(11, 21)],
        }, schema=self.pa_schema).sort_by('user_id')
        self.assertEqual(expected, actual)

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
