# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os
import shutil
import tempfile
import unittest

import pandas
import pyarrow as pa

from pypaimon import CatalogFactory, Schema

from pypaimon.schema.data_types import AtomicType
from pypaimon.schema.schema_change import SchemaChange
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

    def test_schema_evolution_type_promotion_unpartitioned(self):
        # End-to-end via public API only (create -> write -> alter column type
        # -> write -> read). A non-partitioned table whose read needs no column
        # reordering takes the reader fast path that skips partition padding and
        # index remapping. The file written before the type change keeps its old
        # physical type, so it must be aligned to the promoted type to
        # concatenate with the file written after; otherwise the read crashes
        # with an Arrow schema mismatch. This is not specific to INT -> BIGINT:
        # it applies to any type change of an existing column -- integer/float
        # widening, DECIMAL precision/scale changes, and cross-type changes.
        import decimal

        # Each case: (name, old arrow type, new arrow type, new Paimon type,
        # value written to the old-schema file, that same value as it should
        # read back under the new type, value written to the new-schema file).
        # The old write value and its expected read form differ for cross-type
        # changes, where the old file is materialized under the new type.
        cases = [
            ("smallint_to_int", pa.int16(), pa.int32(), 'INT',
             [10, 20], [10, 20], [30, 40]),
            ("int_to_bigint", pa.int32(), pa.int64(), 'BIGINT',
             [10, 20], [10, 20], [30, 40]),
            ("float_to_double", pa.float32(), pa.float64(), 'DOUBLE',
             [1.5, 2.5], [1.5, 2.5], [3.5, 4.5]),
            ("decimal_precision_up",
             pa.decimal128(10, 2), pa.decimal128(20, 2), 'DECIMAL(20, 2)',
             [decimal.Decimal('1.23'), decimal.Decimal('4.56')],
             [decimal.Decimal('1.23'), decimal.Decimal('4.56')],
             [decimal.Decimal('7.89'), decimal.Decimal('0.12')]),
            ("decimal_scale_up",
             pa.decimal128(10, 2), pa.decimal128(10, 4), 'DECIMAL(10, 4)',
             [decimal.Decimal('1.23'), decimal.Decimal('4.56')],
             [decimal.Decimal('1.2300'), decimal.Decimal('4.5600')],
             [decimal.Decimal('7.8901'), decimal.Decimal('0.1234')]),
            ("int_to_string", pa.int32(), pa.string(), 'STRING',
             [10, 20], ['10', '20'], ['a', 'b']),
            # Lossy cross-type change: DOUBLE -> INT truncates (matches Java
            # CastExecutors), so 1.2/2.8 read back as 1/2.
            ("double_to_int", pa.float64(), pa.int32(), 'INT',
             [1.2, 2.8], [1, 2], [3, 4]),
            # Lossy DECIMAL scale-down: (10,4) -> (10,2) truncates the extra
            # scale rather than raising.
            ("decimal_scale_down",
             pa.decimal128(10, 4), pa.decimal128(10, 2), 'DECIMAL(10, 2)',
             [decimal.Decimal('1.2345'), decimal.Decimal('4.5678')],
             [decimal.Decimal('1.23'), decimal.Decimal('4.56')],
             [decimal.Decimal('7.89'), decimal.Decimal('0.12')]),
        ]

        for (name, old_type, new_type, new_type_str,
             write_vals, old_read_vals, new_vals) in cases:
            with self.subTest(case=name):
                table_name = f'default.promo_{name}'
                old_schema = pa.schema([('k', pa.int64()), ('v', old_type)])
                self.catalog.create_table(
                    table_name, Schema.from_pyarrow_schema(old_schema), False)

                # Write under the original schema (file stamped schema_id 0).
                table = self.catalog.get_table(table_name)
                write_builder = table.new_batch_write_builder()
                table_write = write_builder.new_write()
                table_commit = write_builder.new_commit()
                table_write.write_arrow(pa.Table.from_pydict(
                    {'k': [1, 2], 'v': write_vals}, schema=old_schema))
                table_commit.commit(table_write.prepare_commit())
                table_write.close()
                table_commit.close()

                # Widen column v through the catalog (new schema_id 1).
                self.catalog.alter_table(
                    table_name,
                    [SchemaChange.update_column_type(
                        'v', AtomicType(new_type_str))],
                    False)

                # Write under the promoted schema (file stamped schema_id 1).
                table = self.catalog.get_table(table_name)
                new_schema = pa.schema([('k', pa.int64()), ('v', new_type)])
                write_builder = table.new_batch_write_builder()
                table_write = write_builder.new_write()
                table_commit = write_builder.new_commit()
                table_write.write_arrow(pa.Table.from_pydict(
                    {'k': [3, 4], 'v': new_vals}, schema=new_schema))
                table_commit.commit(table_write.prepare_commit())
                table_write.close()
                table_commit.close()

                # Plain full-table read spanning both schema versions.
                read_builder = table.new_read_builder()
                actual = read_builder.new_read().to_arrow(
                    self._scan_table(read_builder))
                expected = pa.Table.from_pydict(
                    {'k': [1, 2, 3, 4], 'v': old_read_vals + new_vals},
                    schema=new_schema)
                self.assertEqual(expected, actual)

    def test_schema_evolution_type_lossy_old_file_only(self):
        # Reading ONLY old-schema files after a lossy type change (no
        # newer-schema file in the splits). The output type must equal the
        # current read schema regardless of which files the read spans, and the
        # conversion must truncate to match Java CastExecutors rather than
        # raise. (A previous fix that relied on pyarrow's safe cast crashed
        # here on lossy evolutions.)
        import decimal

        cases = [
            ("scale_down",
             pa.decimal128(10, 4), pa.decimal128(10, 2), 'DECIMAL(10, 2)',
             [decimal.Decimal('1.2345'), decimal.Decimal('4.5678')],
             [decimal.Decimal('1.23'), decimal.Decimal('4.56')]),
            ("double_to_int", pa.float64(), pa.int32(), 'INT',
             [1.2, 2.8], [1, 2]),
        ]

        for name, old_type, new_type, new_type_str, write_vals, read_vals \
                in cases:
            with self.subTest(case=name):
                table_name = f'default.lossy_old_only_{name}'
                old_schema = pa.schema([('k', pa.int64()), ('v', old_type)])
                self.catalog.create_table(
                    table_name, Schema.from_pyarrow_schema(old_schema), False)

                # Write under the original schema, then change the type. No
                # write happens afterwards, so the read sees only this file.
                table = self.catalog.get_table(table_name)
                write_builder = table.new_batch_write_builder()
                table_write = write_builder.new_write()
                table_commit = write_builder.new_commit()
                table_write.write_arrow(pa.Table.from_pydict(
                    {'k': [1, 2], 'v': write_vals}, schema=old_schema))
                table_commit.commit(table_write.prepare_commit())
                table_write.close()
                table_commit.close()

                self.catalog.alter_table(
                    table_name,
                    [SchemaChange.update_column_type(
                        'v', AtomicType(new_type_str))],
                    False)

                table = self.catalog.get_table(table_name)
                new_schema = pa.schema([('k', pa.int64()), ('v', new_type)])
                read_builder = table.new_read_builder()
                actual = read_builder.new_read().to_arrow(
                    self._scan_table(read_builder))
                expected = pa.Table.from_pydict(
                    {'k': [1, 2], 'v': read_vals}, schema=new_schema)
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
        latest_snapshot = table1.new_read_builder().new_scan().file_scanner.snapshot_manager.get_latest_snapshot()
        table2.table_path = table1.table_path
        new_read_buidler = table2.new_read_builder()
        predicate_builder = new_read_buidler.new_predicate_builder()
        predicate = predicate_builder.less_than('user_id', 3)
        new_scan = new_read_buidler.with_filter(predicate).new_scan()
        manifest_files = new_scan.file_scanner.manifest_list_manager.read_all(latest_snapshot)
        entries = new_scan.file_scanner.read_manifest_entries(manifest_files)
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
