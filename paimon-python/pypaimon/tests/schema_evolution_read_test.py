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

import pandas
import pyarrow as pa

from pypaimon import CatalogFactory, Schema

from pypaimon.schema.schema_manager import SchemaManager
from pypaimon.schema.table_schema import TableSchema


class SchemaEvolutionReadTest(unittest.TestCase):
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

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def test_schema_evolution(self):
        # schema 0
        pa_schema = pa.schema([
            ('user_id', pa.int64()),
            ('item_id', pa.int64()),
            ('dt', pa.string())
        ])
        schema = Schema.from_pyarrow_schema(pa_schema, partition_keys=['dt'])
        self.catalog.create_table('default.test_sample', schema, False)
        table1 = self.catalog.get_table('default.test_sample')
        write_builder = table1.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data1 = {
            'user_id': [1, 2, 3, 4],
            'item_id': [1001, 1002, 1003, 1004],
            'dt': ['p1', 'p1', 'p2', 'p1'],
        }
        pa_table = pa.Table.from_pydict(data1, schema=pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # schema 1  add behavior column
        pa_schema = pa.schema([
            ('user_id', pa.int64()),
            ('item_id', pa.int64()),
            ('dt', pa.string()),
            ('behavior', pa.string())
        ])
        schema2 = Schema.from_pyarrow_schema(pa_schema, partition_keys=['dt'])
        self.catalog.create_table('default.test_schema_evolution', schema2, False)
        table2 = self.catalog.get_table('default.test_schema_evolution')
        table2.table_schema.id = 1
        write_builder = table2.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data2 = {
            'user_id': [5, 6, 7, 8],
            'item_id': [1005, 1006, 1007, 1008],
            'dt': ['p2', 'p1', 'p2', 'p2'],
            'behavior': ['e', 'f', 'g', 'h'],
        }
        pa_table = pa.Table.from_pydict(data2, schema=pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # write schema-0 and schema-1 to table2
        schema_manager = SchemaManager(table2.file_io, table2.table_path)
        schema_manager.commit(TableSchema.from_schema(schema_id=0, schema=schema))
        schema_manager.commit(TableSchema.from_schema(schema_id=1, schema=schema2))

        splits = self._scan_table(table1.new_read_builder())
        read_builder = table2.new_read_builder()
        splits2 = self._scan_table(read_builder)
        splits.extend(splits2)

        table_read = read_builder.new_read()
        actual = table_read.to_arrow(splits)
        expected = pa.Table.from_pydict({
            'user_id': [1, 2, 4, 3, 5, 7, 8, 6],
            'item_id': [1001, 1002, 1004, 1003, 1005, 1007, 1008, 1006],
            'dt': ["p1", "p1", "p1", "p2", "p2", "p2", "p2", "p1"],
            'behavior': [None, None, None, None, "e", "g", "h", "f"],
        }, schema=pa_schema)
        self.assertEqual(expected, actual)

    def test_schema_evolution_type(self):
        # schema 0
        pa_schema = pa.schema([
            ('user_id', pa.int64()),
            ('time', pa.timestamp('s')),
            ('dt', pa.string())
        ])
        schema = Schema.from_pyarrow_schema(pa_schema, partition_keys=['dt'])
        self.catalog.create_table('default.schema_evolution_type', schema, False)
        table1 = self.catalog.get_table('default.schema_evolution_type')
        write_builder = table1.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data1 = {
            'user_id': [1, 2, 3, 4],
            'time': [pandas.Timestamp("2025-01-01 00:00:00"), pandas.Timestamp("2025-01-02 00:02:00"),
                     pandas.Timestamp("2025-01-03 00:03:00"), pandas.Timestamp("2025-01-04 00:04:00")],
            'dt': ['p1', 'p1', 'p2', 'p1'],
        }
        pa_table = pa.Table.from_pydict(data1, schema=pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # schema 1  add behavior column
        pa_schema = pa.schema([
            ('user_id', pa.int8()),
            ('time', pa.timestamp('ms')),
            ('dt', pa.string()),
            ('behavior', pa.string())
        ])
        schema2 = Schema.from_pyarrow_schema(pa_schema, partition_keys=['dt'])
        self.catalog.create_table('default.schema_evolution_type2', schema2, False)
        table2 = self.catalog.get_table('default.schema_evolution_type2')
        table2.table_schema.id = 1
        write_builder = table2.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data2 = {
            'user_id': [5, 6, 7, 8],
            'time': [pandas.Timestamp("2025-01-05 00:05:00"), pandas.Timestamp("2025-01-06 00:06:00"),
                     pandas.Timestamp("2025-01-07 00:07:00"), pandas.Timestamp("2025-01-08 00:08:00")],
            'dt': ['p2', 'p1', 'p2', 'p2'],
            'behavior': ['e', 'f', 'g', 'h'],
        }
        pa_table = pa.Table.from_pydict(data2, schema=pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # write schema-0 and schema-1 to table2
        schema_manager = SchemaManager(table2.file_io, table2.table_path)
        schema_manager.commit(TableSchema.from_schema(schema_id=0, schema=schema))
        schema_manager.commit(TableSchema.from_schema(schema_id=1, schema=schema2))

        splits = self._scan_table(table1.new_read_builder())
        read_builder = table2.new_read_builder()
        splits2 = self._scan_table(read_builder)
        splits.extend(splits2)

        table_read = read_builder.new_read()
        actual = table_read.to_arrow(splits)
        expected = pa.Table.from_pydict({
            'user_id': [1, 2, 4, 3, 5, 7, 8, 6],
            'time': [pandas.Timestamp("2025-01-01 00:00:00"), pandas.Timestamp("2025-01-02 00:02:00"),
                     pandas.Timestamp("2025-01-04 00:04:00"), pandas.Timestamp("2025-01-03 00:03:00"),
                     pandas.Timestamp("2025-01-05 00:05:00"), pandas.Timestamp("2025-01-07 00:07:00"),
                     pandas.Timestamp("2025-01-08 00:08:00"), pandas.Timestamp("2025-01-06 00:06:00"), ],
            'dt': ["p1", "p1", "p1", "p2", "p2", "p2", "p2", "p1"],
            'behavior': [None, None, None, None, "e", "g", "h", "f"],
        }, schema=pa_schema)
        self.assertEqual(expected, actual)

    def test_schema_evolution_with_scan_filter(self):
        # schema 0
        pa_schema = pa.schema([
            ('user_id', pa.int64()),
            ('item_id', pa.int64()),
            ('dt', pa.string())
        ])
        options = {'metadata.stats-mode': 'full'}
        schema = Schema.from_pyarrow_schema(pa_schema, partition_keys=['dt'], options=options)
        self.catalog.create_table('default.test_schema_evolution1', schema, False)
        table1 = self.catalog.get_table('default.test_schema_evolution1')
        write_builder = table1.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data1 = {
            'user_id': [1, 2, 3, 4],
            'item_id': [1001, 1002, 1003, 1004],
            'dt': ['p1', 'p1', 'p2', 'p1'],
        }
        pa_table = pa.Table.from_pydict(data1, schema=pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # schema 1  add behavior column
        pa_schema = pa.schema([
            ('user_id', pa.int64()),
            ('item_id', pa.int64()),
            ('behavior', pa.string()),
            ('dt', pa.string())
        ])
        schema2 = Schema.from_pyarrow_schema(pa_schema, partition_keys=['dt'])
        self.catalog.create_table('default.test_schema_evolution2', schema2, False)
        table2 = self.catalog.get_table('default.test_schema_evolution2')
        table2.table_schema.id = 1
        write_builder = table2.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data2 = {
            'user_id': [5, 6, 7, 8],
            'item_id': [1005, 1006, 1007, 1008],
            'behavior': ['e', 'f', 'g', 'h'],
            'dt': ['p2', 'p1', 'p2', 'p2'],
        }
        pa_table = pa.Table.from_pydict(data2, schema=pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # write schema-0 and schema-1 to table2
        schema_manager = SchemaManager(table2.file_io, table2.table_path)
        schema_manager.commit(TableSchema.from_schema(schema_id=0, schema=schema))
        schema_manager.commit(TableSchema.from_schema(schema_id=1, schema=schema2))
        # scan filter for schema evolution
        latest_snapshot = table1.new_read_builder().new_scan().starting_scanner.snapshot_manager.get_latest_snapshot()
        table2.table_path = table1.table_path
        new_read_buidler = table2.new_read_builder()
        predicate_builder = new_read_buidler.new_predicate_builder()
        predicate = predicate_builder.less_than('user_id', 3)
        new_scan = new_read_buidler.with_filter(predicate).new_scan()
        manifest_files = new_scan.starting_scanner.manifest_list_manager.read_all(latest_snapshot)
        entries = new_scan.starting_scanner.read_manifest_entries(manifest_files)
        self.assertEqual(1, len(entries))  # verify scan filter success for schema evolution

    def test_schema_evolution_with_read_filter(self):
        # schema 0
        pa_schema = pa.schema([
            ('user_id', pa.int64()),
            ('item_id', pa.int64()),
            ('dt', pa.string())
        ])
        options = {'metadata.stats-mode': 'full'}
        schema = Schema.from_pyarrow_schema(pa_schema, partition_keys=['dt'], options=options)
        self.catalog.create_table('default.test_schema_evolution_with_filter', schema, False)
        table1 = self.catalog.get_table('default.test_schema_evolution_with_filter')
        write_builder = table1.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data1 = {
            'user_id': [1, 2, 3, 4],
            'item_id': [1001, 1002, 1003, 1004],
            'dt': ['p1', 'p1', 'p2', 'p1'],
        }
        pa_table = pa.Table.from_pydict(data1, schema=pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # schema 1  add behavior column
        pa_schema = pa.schema([
            ('user_id', pa.int64()),
            ('item_id', pa.int64()),
            ('dt', pa.string()),
            ('behavior', pa.string())
        ])
        schema2 = Schema.from_pyarrow_schema(pa_schema, partition_keys=['dt'], options=options)
        self.catalog.create_table('default.test_schema_evolution_with_filter2', schema2, False)
        table2 = self.catalog.get_table('default.test_schema_evolution_with_filter2')
        table2.table_schema.id = 1
        write_builder = table2.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data2 = {
            'user_id': [5, 6, 7, 8],
            'item_id': [1005, 1006, 1007, 1008],
            'dt': ['p2', 'p1', 'p2', 'p2'],
            'behavior': ['e', 'f', 'g', 'h'],
        }
        pa_table = pa.Table.from_pydict(data2, schema=pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # write schema-0 and schema-1 to table2
        schema_manager = SchemaManager(table2.file_io, table2.table_path)
        schema_manager.file_io.delete_quietly(table2.table_path + "/schema/schema-0")
        schema_manager.commit(TableSchema.from_schema(schema_id=0, schema=schema))
        schema_manager.commit(TableSchema.from_schema(schema_id=1, schema=schema2))

        # behavior or user_id filter
        splits = self._scan_table(table1.new_read_builder())
        read_builder = table2.new_read_builder()
        predicate_builder = read_builder.new_predicate_builder()
        ne_predicate = predicate_builder.equal('behavior', "g")
        lt_predicate = predicate_builder.less_than('user_id', 6)
        and_predicate = predicate_builder.or_predicates([ne_predicate, lt_predicate])
        splits2 = self._scan_table(read_builder.with_filter(and_predicate))
        for split in splits2:
            for file in split.files:
                file.schema_id = 1
        splits.extend(splits2)

        table_read = read_builder.new_read()
        actual = table_read.to_arrow(splits)
        expected = pa.Table.from_pydict({
            'user_id': [1, 2, 4, 3, 5, 7],
            'item_id': [1001, 1002, 1004, 1003, 1005, 1007],
            'dt': ["p1", "p1", "p1", "p2", "p2", "p2"],
            'behavior': [None, None, None, None, "e", "g"],
        }, schema=pa_schema)
        self.assertEqual(expected, actual)

        # behavior and user_id filter
        splits = self._scan_table(table1.new_read_builder())

        read_builder = table2.new_read_builder()
        predicate_builder = read_builder.new_predicate_builder()
        ne_predicate = predicate_builder.equal('behavior', "g")
        lt_predicate = predicate_builder.less_than('user_id', 8)
        and_predicate = predicate_builder.and_predicates([ne_predicate, lt_predicate])
        splits2 = self._scan_table(read_builder.with_filter(and_predicate))
        for split in splits2:
            for file in split.files:
                file.schema_id = 1
        splits.extend(splits2)

        table_read = read_builder.new_read()
        actual = table_read.to_arrow(splits)
        expected = pa.Table.from_pydict({
            'user_id': [1, 2, 4, 3, 7],
            'item_id': [1001, 1002, 1004, 1003, 1007],
            'dt': ["p1", "p1", "p1", "p2", "p2"],
            'behavior': [None, None, None, None, "g"],
        }, schema=pa_schema)
        self.assertEqual(expected, actual)

        # user_id filter
        splits = self._scan_table(table1.new_read_builder())

        read_builder = table2.new_read_builder()
        predicate_builder = read_builder.new_predicate_builder()
        predicate = predicate_builder.less_than('user_id', 6)
        splits2 = self._scan_table(read_builder.with_filter(predicate))
        self.assertEqual(1, len(splits2))
        for split in splits2:
            for file in split.files:
                file.schema_id = 1
        splits.extend(splits2)

        table_read = read_builder.new_read()
        actual = table_read.to_arrow(splits)
        expected = pa.Table.from_pydict({
            'user_id': [1, 2, 4, 3, 5],
            'item_id': [1001, 1002, 1004, 1003, 1005],
            'dt': ["p1", "p1", "p1", "p2", "p2"],
            'behavior': [None, None, None, None, "e"],
        }, schema=pa_schema)
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

    def _scan_table(self, read_builder):
        splits = read_builder.new_scan().plan().splits()
        return splits
