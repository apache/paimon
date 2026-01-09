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

import glob
import os
import shutil
import tempfile
import unittest
import random
from datetime import date, datetime, time
from decimal import Decimal
from unittest.mock import Mock

import pandas as pd
import pyarrow as pa
from parameterized import parameterized

from pypaimon import CatalogFactory, Schema
from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.schema.data_types import (ArrayType, AtomicType, DataField,
                                        MapType, PyarrowFieldParser)
from pypaimon.schema.table_schema import TableSchema
from pypaimon.snapshot.snapshot_manager import SnapshotManager
from pypaimon.table.row.generic_row import GenericRow, GenericRowDeserializer
from pypaimon.write.file_store_commit import FileStoreCommit


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

    def test_full_data_types(self):
        simple_pa_schema = pa.schema([
            ('f0', pa.int8()),
            ('f1', pa.int16()),
            ('f2', pa.int32()),
            ('f3', pa.int64()),
            ('f4', pa.float32()),
            ('f5', pa.float64()),
            ('f6', pa.bool_()),
            ('f7', pa.string()),
            ('f8', pa.binary()),
            ('f9', pa.binary(10)),
            ('f10', pa.decimal128(10, 2)),
            ('f11', pa.timestamp('ms')),
            ('f12', pa.date32()),
            ('f13', pa.time64('us')),
        ])
        stats_enabled = random.random() < 0.5
        options = {'metadata.stats-mode': 'full'} if stats_enabled else {}
        schema = Schema.from_pyarrow_schema(simple_pa_schema, options=options)
        self.catalog.create_table('default.test_full_data_types', schema, False)
        table = self.catalog.get_table('default.test_full_data_types')

        # to test read and write
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        expect_data = pa.Table.from_pydict({
            'f0': [-1, 2],
            'f1': [-1001, 1002],
            'f2': [-1000001, 1000002],
            'f3': [-10000000001, 10000000002],
            'f4': [-1001.05, 1002.05],
            'f5': [-1000001.05, 1000002.05],
            'f6': [False, True],
            'f7': ['Hello', 'World'],
            'f8': [b'\x01\x02\x03', b'pyarrow'],
            'f9': [b'exactly_10', b'pad'.ljust(10, b'\x00')],
            'f10': [Decimal('-987.65'), Decimal('12345.67')],
            'f11': [datetime(2000, 1, 1, 0, 0, 0, 123456), datetime(2023, 10, 27, 8, 0, 0)],
            'f12': [date(1999, 12, 31), date(2023, 1, 1)],
            'f13': [time(10, 30, 0), time(23, 59, 59, 999000)],
        }, schema=simple_pa_schema)
        table_write.write_arrow(expect_data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        splits = table_scan.plan().splits()

        # assert data file without stats
        first_file = splits[0].files[0]
        self.assertEqual(first_file.value_stats_cols, [])
        self.assertEqual(first_file.value_stats, SimpleStats.empty_stats())

        # assert equal
        actual_data = table_read.to_arrow(splits)
        self.assertEqual(actual_data, expect_data)

        # to test GenericRow ability
        latest_snapshot = SnapshotManager(table).get_latest_snapshot()
        manifest_files = table_scan.starting_scanner.manifest_list_manager.read_all(latest_snapshot)
        manifest_entries = table_scan.starting_scanner.manifest_file_manager.read(
            manifest_files[0].file_name, lambda row: table_scan.starting_scanner._filter_manifest_entry(row), False)

        # Python write does not produce value stats
        if stats_enabled:
            self.assertEqual(manifest_entries[0].file.value_stats_cols, None)
            min_value_stats = GenericRowDeserializer.from_bytes(manifest_entries[0].file.value_stats.min_values.data,
                                                                table.fields).values
            max_value_stats = GenericRowDeserializer.from_bytes(manifest_entries[0].file.value_stats.max_values.data,
                                                                table.fields).values
            expected_min_values = [col[0].as_py() for col in expect_data]
            expected_max_values = [col[1].as_py() for col in expect_data]
            self.assertEqual(min_value_stats, expected_min_values)
            self.assertEqual(max_value_stats, expected_max_values)
        else:
            self.assertEqual(manifest_entries[0].file.value_stats_cols, [])
            min_value_stats = GenericRowDeserializer.from_bytes(manifest_entries[0].file.value_stats.min_values.data,
                                                                []).values
            max_value_stats = GenericRowDeserializer.from_bytes(manifest_entries[0].file.value_stats.max_values.data,
                                                                []).values
            self.assertEqual(min_value_stats, [])
            self.assertEqual(max_value_stats, [])

    def test_write_wrong_schema(self):
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
        self.assertTrue(str(e.exception).startswith("Input schema isn't consistent with table schema and write cols."))

    def test_reader_iterator(self):
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

    def test_reader_duckDB(self):
        read_builder = self.table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        duckdb_con = table_read.to_duckdb(splits, 'duckdb_table')
        actual = duckdb_con.query("SELECT * FROM duckdb_table").fetchdf()
        expect = pd.DataFrame(self.raw_data)
        pd.testing.assert_frame_equal(actual.reset_index(drop=True), expect.reset_index(drop=True))

    def test_mixed_add_and_delete_entries_compute_stats(self):
        """Test record_count calculation with mixed ADD/DELETE entries in same partition."""
        pa_schema = pa.schema([
            ('region', pa.string()),
            ('city', pa.string())
        ])
        schema = Schema.from_pyarrow_schema(pa_schema)
        self.catalog.create_table('default.tb', schema, False)
        table = self.catalog.get_table('default.tb')
        partition_fields = [
            DataField(0, "region", AtomicType("STRING")),
            DataField(1, "city", AtomicType("STRING"))
        ]
        partition = GenericRow(['East', 'Boston'], partition_fields)

        # Create ADD entry
        from pypaimon.data.timestamp import Timestamp
        import time
        add_file_meta = Mock(spec=DataFileMeta)
        add_file_meta.row_count = 200
        add_file_meta.file_size = 2048
        add_file_meta.creation_time = Timestamp.now()
        add_file_meta.creation_time_epoch_millis = Mock(return_value=int(time.time() * 1000))

        add_entry = ManifestEntry(
            kind=0,  # ADD
            partition=partition,
            bucket=0,
            total_buckets=1,
            file=add_file_meta
        )

        # Create DELETE entry
        delete_file_meta = Mock(spec=DataFileMeta)
        delete_file_meta.row_count = 80
        delete_file_meta.file_size = 800
        delete_file_meta.creation_time = Timestamp.now()
        delete_file_meta.creation_time_epoch_millis = Mock(return_value=int(time.time() * 1000))

        delete_entry = ManifestEntry(
            kind=1,  # DELETE
            partition=partition,
            bucket=0,
            total_buckets=1,
            file=delete_file_meta
        )
        file_store_commit = FileStoreCommit(None, table, "")
        # Test the method with both entries
        statistics = file_store_commit._generate_partition_statistics([add_entry, delete_entry])

        # Verify results - should be merged into single partition statistics
        self.assertEqual(len(statistics), 1)
        stat = statistics[0]

        # Net record count: +200 + (-80) = 120
        self.assertEqual(stat.record_count, 120)
        self.assertEqual(stat.file_count, 0)
        self.assertEqual(stat.file_size_in_bytes, 1248)

    def test_delete_entries_compute_stats(self):
        """Test record_count calculation across multiple partitions."""
        pa_schema = pa.schema([
            ('region', pa.string()),
            ('city', pa.string())
        ])
        schema = Schema.from_pyarrow_schema(pa_schema)
        self.catalog.create_table('default.tb1', schema, False)
        table = self.catalog.get_table('default.tb1')
        partition_fields = [
            DataField(0, "region", AtomicType("STRING")),
            DataField(1, "city", AtomicType("STRING"))
        ]
        partition1 = GenericRow(['East', 'Boston'], partition_fields)
        from pypaimon.data.timestamp import Timestamp
        import time
        file_meta1 = Mock(spec=DataFileMeta)
        file_meta1.row_count = 150
        file_meta1.file_size = 1500
        file_meta1.creation_time = Timestamp.now()
        file_meta1.creation_time_epoch_millis = Mock(return_value=int(time.time() * 1000))

        entry1 = ManifestEntry(
            kind=0,  # ADD
            partition=partition1,
            bucket=0,
            total_buckets=1,
            file=file_meta1
        )

        # Partition 2: South/LA - DELETE operation
        partition2 = GenericRow(['South', 'LA'], partition_fields)
        file_meta2 = Mock(spec=DataFileMeta)
        file_meta2.row_count = 75
        file_meta2.file_size = 750
        file_meta2.creation_time = Timestamp.now()
        file_meta2.creation_time_epoch_millis = Mock(return_value=int(time.time() * 1000))

        entry2 = ManifestEntry(
            kind=1,  # DELETE
            partition=partition2,
            bucket=0,
            total_buckets=1,
            file=file_meta2
        )

        file_store_commit = FileStoreCommit(None, table, "")
        # Test the method with both entries
        statistics = file_store_commit._generate_partition_statistics([entry1, entry2])

        # Verify results - should have 2 separate partition statistics
        self.assertEqual(len(statistics), 2)

        # Sort by partition spec for consistent testing
        statistics.sort(key=lambda s: (s.spec.get('region', ''), s.spec.get('city', '')))

        # Check North/NY partition (ADD)
        north_stat = statistics[0]
        self.assertEqual(north_stat.record_count, 150)  # Positive for ADD
        self.assertEqual(north_stat.file_count, 1)
        self.assertEqual(north_stat.file_size_in_bytes, 1500)

        # Check South/LA partition (DELETE)
        south_stat = statistics[1]
        self.assertEqual(south_stat.record_count, -75)  # Negative for DELETE
        self.assertEqual(south_stat.file_count, -1)
        self.assertEqual(south_stat.file_size_in_bytes, -750)

    def test_value_stats_cols_param(self):
        """Test _VALUE_STATS_COLS logic in ManifestFileManager."""
        # Create a catalog and table
        catalog = CatalogFactory.create({
            "warehouse": self.warehouse
        })
        catalog.create_database("test_db", False)

        # Define schema with multiple fields
        pa_schema = pa.schema([
            ('id', pa.int64()),
            ('name', pa.string()),
            ('price', pa.float64()),
            ('category', pa.string())
        ])
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table("test_db.test_value_stats_cols", schema, False)
        table = catalog.get_table("test_db.test_value_stats_cols")

        # Create a ManifestFileManager
        manifest_manager = ManifestFileManager(table)

        # Test case 1: _VALUE_STATS_COLS is None (should use all table fields)
        self._test_value_stats_cols_case(
            manifest_manager,
            table,
            value_stats_cols=None,
            expected_fields_count=4,  # All 4 fields
            test_name="none_case"
        )

        # Test case 2: _VALUE_STATS_COLS is empty list (should use empty fields)
        self._test_value_stats_cols_case(
            manifest_manager,
            table,
            value_stats_cols=[],
            expected_fields_count=0,  # No fields
            test_name="empty_case"
        )

        # Test case 3: _VALUE_STATS_COLS has specific columns
        self._test_value_stats_cols_case(
            manifest_manager,
            table,
            value_stats_cols=['id', 'name'],
            expected_fields_count=2,  # Only 2 specified fields
            test_name="specific_case"
        )

        schema_with_stats = Schema.from_pyarrow_schema(pa_schema, options={'metadata.stats-mode': 'full'})
        catalog.create_table("test_db.test_value_stats_cols_schema_match", schema_with_stats, False)
        table_with_stats = catalog.get_table("test_db.test_value_stats_cols_schema_match")
        self._test_append_only_schema_match_case(table_with_stats, pa_schema)

    def test_primary_key_value_stats_excludes_system_fields(self):
        catalog = CatalogFactory.create({
            "warehouse": self.warehouse
        })
        catalog.create_database("test_db_system_fields", True)

        pk_pa_schema = pa.schema([
            ('id', pa.int64()),
            ('name', pa.string()),
            ('price', pa.float64()),
        ])
        pk_schema = Schema.from_pyarrow_schema(
            pk_pa_schema,
            primary_keys=['id'],
            options={'metadata.stats-mode': 'full', 'bucket': '2'}
        )
        catalog.create_table("test_db_system_fields.test_pk_value_stats_system_fields", pk_schema, False)
        pk_table = catalog.get_table("test_db_system_fields.test_pk_value_stats_system_fields")

        pk_test_data = pa.Table.from_pydict({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'price': [10.5, 20.3, 30.7],
        }, schema=pk_pa_schema)

        pk_write_builder = pk_table.new_batch_write_builder()
        pk_writer = pk_write_builder.new_write()
        pk_writer.write_arrow(pk_test_data)
        pk_commit_messages = pk_writer.prepare_commit()
        pk_commit = pk_write_builder.new_commit()
        pk_commit.commit(pk_commit_messages)
        pk_writer.close()

        pk_read_builder = pk_table.new_read_builder()
        pk_table_scan = pk_read_builder.new_scan()
        latest_snapshot = SnapshotManager(pk_table).get_latest_snapshot()
        pk_manifest_files = pk_table_scan.starting_scanner.manifest_list_manager.read_all(latest_snapshot)
        pk_manifest_entries = pk_table_scan.starting_scanner.manifest_file_manager.read(
            pk_manifest_files[0].file_name,
            lambda row: pk_table_scan.starting_scanner._filter_manifest_entry(row),
            False
        )

        self.assertGreater(len(pk_manifest_entries), 0, "Should have at least one manifest entry")
        pk_file_meta = pk_manifest_entries[0].file

        pk_table_field_names = {f.name for f in pk_table.fields}
        system_fields = {'_KEY_id', '_SEQUENCE_NUMBER', '_VALUE_KIND', '_ROW_ID'}
        pk_table_has_system_fields = bool(pk_table_field_names & system_fields)
        self.assertFalse(pk_table_has_system_fields,
                         f"table.fields should NOT contain system fields, but got: {pk_table_field_names}")

        if pk_file_meta.value_stats_cols is None:
            pk_value_stats_fields = pk_table_scan.starting_scanner.manifest_file_manager._get_value_stats_fields(
                {'_VALUE_STATS_COLS': None},
                pk_table.fields
            )
            expected_count = len(pk_value_stats_fields)
            actual_count = pk_file_meta.value_stats.min_values.arity
            self.assertEqual(actual_count, expected_count,
                             f"Field count mismatch: value_stats has {actual_count} fields, "
                             f"but table.fields has {expected_count} fields. "
                             f"This indicates value_stats contains system fields that are not in table.fields.")
        else:
            for field_name in pk_file_meta.value_stats_cols:
                is_system_field = (field_name.startswith('_KEY_') or
                                   field_name in ['_SEQUENCE_NUMBER', '_VALUE_KIND', '_ROW_ID'])
                self.assertFalse(is_system_field,
                                 f"value_stats_cols should not contain system field: {field_name}")

    def test_value_stats_empty_when_stats_disabled(self):
        catalog = CatalogFactory.create({
            "warehouse": self.warehouse
        })
        catalog.create_database("test_db_stats_disabled", True)

        pa_schema = pa.schema([
            ('id', pa.int64()),
            ('name', pa.string()),
            ('price', pa.float64()),
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            primary_keys=['id'],
            options={'metadata.stats-mode': 'none', 'bucket': '2'}  # Stats disabled
        )
        catalog.create_table("test_db_stats_disabled.test_stats_disabled", schema, False)
        table = catalog.get_table("test_db_stats_disabled.test_stats_disabled")

        test_data = pa.Table.from_pydict({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'price': [10.5, 20.3, 30.7],
        }, schema=pa_schema)

        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(test_data)
        commit_messages = writer.prepare_commit()
        commit = write_builder.new_commit()
        commit.commit(commit_messages)
        writer.close()

        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        latest_snapshot = SnapshotManager(table).get_latest_snapshot()
        manifest_files = table_scan.starting_scanner.manifest_list_manager.read_all(latest_snapshot)
        manifest_entries = table_scan.starting_scanner.manifest_file_manager.read(
            manifest_files[0].file_name,
            lambda row: table_scan.starting_scanner._filter_manifest_entry(row),
            False
        )

        self.assertGreater(len(manifest_entries), 0, "Should have at least one manifest entry")
        file_meta = manifest_entries[0].file

        self.assertEqual(
            file_meta.value_stats_cols, [],
            "value_stats_cols should be empty list [] when stats are disabled"
        )

        self.assertEqual(
            file_meta.value_stats.min_values.arity, 0,
            "value_stats.min_values should be empty (arity=0) when stats are disabled"
        )
        self.assertEqual(
            file_meta.value_stats.max_values.arity, 0,
            "value_stats.max_values should be empty (arity=0) when stats are disabled"
        )
        self.assertEqual(
            len(file_meta.value_stats.null_counts), 0,
            "value_stats.null_counts should be empty when stats are disabled"
        )

        empty_stats = SimpleStats.empty_stats()
        self.assertEqual(
            file_meta.value_stats.min_values.arity, len(empty_stats.min_values),
            "value_stats.min_values should be empty (same as SimpleStats.empty_stats()) when stats are disabled"
        )
        self.assertEqual(
            file_meta.value_stats.max_values.arity, len(empty_stats.max_values),
            "value_stats.max_values should be empty (same as SimpleStats.empty_stats()) when stats are disabled"
        )
        self.assertEqual(
            len(file_meta.value_stats.null_counts), len(empty_stats.null_counts),
            "value_stats.null_counts should be empty (same as SimpleStats.empty_stats()) when stats are disabled"
        )

    def test_types(self):
        data_fields = [
            DataField(0, "f0", AtomicType('TINYINT'), 'desc'),
            DataField(1, "f1", AtomicType('SMALLINT'), 'desc'),
            DataField(2, "f2", AtomicType('INT'), 'desc'),
            DataField(3, "f3", AtomicType('BIGINT'), 'desc'),
            DataField(4, "f4", AtomicType('FLOAT'), 'desc'),
            DataField(5, "f5", AtomicType('DOUBLE'), 'desc'),
            DataField(6, "f6", AtomicType('BOOLEAN'), 'desc'),
            DataField(7, "f7", AtomicType('STRING'), 'desc'),
            DataField(8, "f8", AtomicType('BINARY(12)'), 'desc'),
            DataField(9, "f9", AtomicType('DECIMAL(10, 6)'), 'desc'),
            DataField(10, "f10", AtomicType('BYTES'), 'desc'),
            DataField(11, "f11", AtomicType('DATE'), 'desc'),
            DataField(12, "f12", AtomicType('TIME(0)'), 'desc'),
            DataField(13, "f13", AtomicType('TIME(3)'), 'desc'),
            DataField(14, "f14", AtomicType('TIME(6)'), 'desc'),
            DataField(15, "f15", AtomicType('TIME(9)'), 'desc'),
            DataField(16, "f16", AtomicType('TIMESTAMP(0)'), 'desc'),
            DataField(17, "f17", AtomicType('TIMESTAMP(3)'), 'desc'),
            DataField(18, "f18", AtomicType('TIMESTAMP(6)'), 'desc'),
            DataField(19, "f19", AtomicType('TIMESTAMP(9)'), 'desc'),
            DataField(20, "arr", ArrayType(True, AtomicType('INT')), 'desc arr1'),
            DataField(21, "map1",
                      MapType(False, AtomicType('INT', False),
                              MapType(False, AtomicType('INT', False), AtomicType('INT', False))),
                      'desc map1'),
        ]
        table_schema = TableSchema(TableSchema.CURRENT_VERSION, len(data_fields), data_fields,
                                   max(field.id for field in data_fields),
                                   [], [], {}, "")
        pa_fields = []
        for field in table_schema.fields:
            pa_field = PyarrowFieldParser.from_paimon_field(field)
            pa_fields.append(pa_field)
        schema = Schema.from_pyarrow_schema(
            pa_schema=pa.schema(pa_fields),
            partition_keys=table_schema.partition_keys,
            primary_keys=table_schema.primary_keys,
            options=table_schema.options,
            comment=table_schema.comment
        )
        table_schema2 = TableSchema.from_schema(len(data_fields), schema)
        l1 = []
        for field in table_schema.fields:
            l1.append(field.to_dict())
        l2 = []
        for field in table_schema2.fields:
            l2.append(field.to_dict())
        self.assertEqual(l1, l2)

    @parameterized.expand([
        ('parquet',),
        ('orc',),
        ('avro',),
    ])
    def test_write(self, file_format):
        pa_schema = pa.schema([
            ('f0', pa.int32()),
            ('f1', pa.string()),
            ('f2', pa.string())
        ])
        catalog = CatalogFactory.create({
            "warehouse": self.warehouse
        })
        db_name = f"test_write_{file_format}_db"
        table_name = f"test_{file_format}_table"
        catalog.create_database(db_name, False)
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={'file.format': file_format}
        )
        catalog.create_table(f"{db_name}.{table_name}", schema, False)
        table = catalog.get_table(f"{db_name}.{table_name}")

        data = {
            'f0': [1, 2, 3],
            'f1': ['a', 'b', 'c'],
            'f2': ['X', 'Y', 'Z']
        }
        expect = pa.Table.from_pydict(data, schema=pa_schema)

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(expect)
        commit_messages = table_write.prepare_commit()
        table_commit.commit(commit_messages)
        table_write.close()
        table_commit.close()

        self._verify_file_compression(file_format, db_name, table_name, expected_rows=3)

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(expect)
        commit_messages = table_write.prepare_commit()
        table_commit.commit(commit_messages)
        table_write.close()
        table_commit.close()

        snapshot_path = os.path.join(self.warehouse, f"{db_name}.db", table_name, "snapshot", "snapshot-2")
        with open(snapshot_path, 'r', encoding='utf-8') as file:
            content = ''.join(file.readlines())
            self.assertTrue(content.__contains__('\"totalRecordCount\": 6'))
            self.assertTrue(content.__contains__('\"deltaRecordCount\": 3'))

    @parameterized.expand([
        ('parquet', 'zstd'),
        ('parquet', 'lz4'),
        ('parquet', 'snappy'),
        ('orc', 'zstd'),
        ('orc', 'lz4'),
        ('orc', 'snappy'),
        ('avro', 'zstd'),
        ('avro', 'snappy'),
    ])
    def test_write_with_compression(self, file_format, compression):
        pa_schema = pa.schema([
            ('f0', pa.int32()),
            ('f1', pa.string()),
            ('f2', pa.string())
        ])
        catalog = CatalogFactory.create({
            "warehouse": self.warehouse
        })
        db_name = f"test_write_{file_format}_{compression}_db"
        table_name = f"test_{file_format}_{compression}_table"
        catalog.create_database(db_name, False)
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={
                'file.format': file_format,
                'file.compression': compression
            }
        )
        catalog.create_table(f"{db_name}.{table_name}", schema, False)
        table = catalog.get_table(f"{db_name}.{table_name}")

        data = {
            'f0': [1, 2, 3],
            'f1': ['a', 'b', 'c'],
            'f2': ['X', 'Y', 'Z']
        }
        expect = pa.Table.from_pydict(data, schema=pa_schema)

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        try:
            table_write.write_arrow(expect)
            commit_messages = table_write.prepare_commit()
            table_commit.commit(commit_messages)
            table_write.close()
            table_commit.close()

            self._verify_file_compression_with_format(
                file_format, compression, db_name, table_name, expected_rows=3
            )
        except (ValueError, RuntimeError):
            raise

    def _verify_file_compression_with_format(
            self, file_format: str, compression: str,
            db_name: str, table_name: str, expected_rows: int = 3):
        if file_format == 'parquet':
            parquet_files = glob.glob(self.warehouse + f"/{db_name}.db/{table_name}/bucket-0/*.parquet")
            self.assertEqual(len(parquet_files), 1)
            import pyarrow.parquet as pq
            parquet_file_path = parquet_files[0]
            parquet_metadata = pq.read_metadata(parquet_file_path)
            for i in range(parquet_metadata.num_columns):
                column_metadata = parquet_metadata.row_group(0).column(i)
                actual_compression = column_metadata.compression
                compression_str = str(actual_compression).upper()
                expected_compression_upper = compression.upper()
                self.assertIn(
                    expected_compression_upper, compression_str,
                    f"Expected compression to be {compression}, but got {actual_compression}")
        elif file_format == 'orc':
            orc_files = glob.glob(self.warehouse + f"/{db_name}.db/{table_name}/bucket-0/*.orc")
            self.assertEqual(len(orc_files), 1)
            import pyarrow.orc as orc
            orc_file_path = orc_files[0]
            orc_file = orc.ORCFile(orc_file_path)
            try:
                table = orc_file.read()
                self.assertEqual(table.num_rows, expected_rows, "ORC file should contain expected rows")
            except Exception as e:
                self.fail(f"Failed to read ORC file (compression may be incorrect): {e}")
        elif file_format == 'avro':
            avro_files = glob.glob(self.warehouse + f"/{db_name}.db/{table_name}/bucket-0/*.avro")
            self.assertEqual(len(avro_files), 1)
            import fastavro
            avro_file_path = avro_files[0]
            with open(avro_file_path, 'rb') as f:
                reader = fastavro.reader(f)
                codec = reader.codec
                expected_codec_map = {
                    'zstd': 'zstandard',
                    'zstandard': 'zstandard',
                    'snappy': 'snappy',
                    'deflate': 'deflate',
                }
                expected_codec = expected_codec_map.get(
                    compression.lower(), compression.lower())
                self.assertEqual(
                    codec, expected_codec,
                    f"Expected compression codec to be '{expected_codec}', but got '{codec}'")

    def _verify_file_compression(self, file_format: str, db_name: str, table_name: str, expected_rows: int = 3):
        if file_format == 'parquet':
            parquet_files = glob.glob(self.warehouse + f"/{db_name}.db/{table_name}/bucket-0/*.parquet")
            self.assertEqual(len(parquet_files), 1)
            import pyarrow.parquet as pq
            parquet_file_path = parquet_files[0]
            parquet_metadata = pq.read_metadata(parquet_file_path)
            for i in range(parquet_metadata.num_columns):
                column_metadata = parquet_metadata.row_group(0).column(i)
                compression = column_metadata.compression
                compression_str = str(compression).upper()
                self.assertIn(
                    'ZSTD', compression_str,
                    f"Expected compression to be ZSTD , "
                    f"but got {compression}")
        elif file_format == 'orc':
            orc_files = glob.glob(self.warehouse + f"/{db_name}.db/{table_name}/bucket-0/*.orc")
            self.assertEqual(len(orc_files), 1)
            import pyarrow.orc as orc
            orc_file_path = orc_files[0]
            orc_file = orc.ORCFile(orc_file_path)
            try:
                table = orc_file.read()
                self.assertEqual(table.num_rows, expected_rows, "ORC file should contain expected rows")
            except Exception as e:
                self.fail(f"Failed to read ORC file (compression may be incorrect): {e}")
        elif file_format == 'avro':
            avro_files = glob.glob(self.warehouse + f"/{db_name}.db/{table_name}/bucket-0/*.avro")
            self.assertEqual(len(avro_files), 1)
            import fastavro
            avro_file_path = avro_files[0]
            with open(avro_file_path, 'rb') as f:
                reader = fastavro.reader(f)
                codec = reader.codec
                self.assertEqual(
                    codec, 'zstandard',
                    f"Expected compression codec to be 'zstandard', "
                    f"but got '{codec}'")

    def _test_value_stats_cols_case(self, manifest_manager, table, value_stats_cols, expected_fields_count, test_name):
        """Helper method to test a specific _VALUE_STATS_COLS case."""

        # Create test data based on expected_fields_count
        if expected_fields_count == 0:
            # Empty fields case
            test_fields = []
            min_values = []
            max_values = []
            null_counts = []
        elif expected_fields_count == 2:
            # Specific fields case (id, name)
            test_fields = [
                DataField(0, "id", AtomicType("BIGINT")),
                DataField(1, "name", AtomicType("STRING"))
            ]
            min_values = [1, "apple"]
            max_values = [100, "zebra"]
            null_counts = [0, 0]
        else:
            # All fields case
            test_fields = table.table_schema.fields
            min_values = [1, "apple", 10.5, "electronics"]
            max_values = [100, "zebra", 999.9, "toys"]
            null_counts = [0, 0, 0, 0]

        # Create BinaryRows for min/max values
        min_binary_row = GenericRow(min_values, test_fields) if test_fields else GenericRow([], [])
        max_binary_row = GenericRow(max_values, test_fields) if test_fields else GenericRow([], [])

        # Create value_stats
        value_stats = SimpleStats(
            min_values=min_binary_row,
            max_values=max_binary_row,
            null_counts=null_counts
        )

        # Create key_stats (empty for simplicity)
        empty_binary_row = GenericRow([], [])
        key_stats = SimpleStats(
            min_values=empty_binary_row,
            max_values=empty_binary_row,
            null_counts=[]
        )

        # Create DataFileMeta with value_stats_cols
        from pypaimon.data.timestamp import Timestamp
        file_meta = DataFileMeta(
            file_name=f"test-file-{test_name}.parquet",
            file_size=1024,
            row_count=100,
            min_key=empty_binary_row,
            max_key=empty_binary_row,
            key_stats=key_stats,
            value_stats=value_stats,
            min_sequence_number=1,
            max_sequence_number=100,
            schema_id=0,
            level=0,
            extra_files=[],
            creation_time=Timestamp.from_epoch_millis(1234567890),
            delete_row_count=0,
            embedded_index=None,
            file_source=None,
            value_stats_cols=value_stats_cols,  # This is the key field we're testing
            external_path=None,
            first_row_id=None,
            write_cols=None
        )

        # Create ManifestEntry
        entry = ManifestEntry(
            kind=0,  # Normal entry
            partition=empty_binary_row,
            bucket=0,
            total_buckets=1,
            file=file_meta
        )

        # Write the manifest entry
        manifest_file_name = f"manifest-test-{test_name}"
        manifest_manager.write(manifest_file_name, [entry])

        # Read the manifest entry back
        entries = manifest_manager.read(manifest_file_name, drop_stats=False)

        # Verify we have exactly one entry
        self.assertEqual(len(entries), 1)

        # Get the entry
        read_entry = entries[0]

        # Verify value_stats_cols is preserved correctly
        self.assertEqual(read_entry.file.value_stats_cols, value_stats_cols)

        # Verify value_stats structure based on the logic
        if value_stats_cols is None:
            # Should use all table fields - verify we have data for all fields
            self.assertEqual(read_entry.file.value_stats.min_values.arity, expected_fields_count)
            self.assertEqual(read_entry.file.value_stats.min_values.arity, expected_fields_count)
            self.assertEqual(len(read_entry.file.value_stats.null_counts), expected_fields_count)
        elif not value_stats_cols:  # Empty list
            # Should use empty fields - verify we have no field data
            self.assertEqual(read_entry.file.value_stats.min_values.arity, 0)
            self.assertEqual(read_entry.file.value_stats.max_values.arity, 0)
            self.assertEqual(len(read_entry.file.value_stats.null_counts), 0)
        else:
            # Should use specified fields - verify we have data for specified fields only
            self.assertEqual(read_entry.file.value_stats.min_values.arity, expected_fields_count)
            self.assertEqual(read_entry.file.value_stats.max_values.arity, expected_fields_count)
            self.assertEqual(len(read_entry.file.value_stats.null_counts), expected_fields_count)

        # Verify the actual values match what we expect
        if expected_fields_count > 0:
            self.assertEqual(
                GenericRowDeserializer.from_bytes(read_entry.file.value_stats.min_values.data, test_fields).values,
                min_values)
            self.assertEqual(
                GenericRowDeserializer.from_bytes(read_entry.file.value_stats.max_values.data, test_fields).values,
                max_values)

        self.assertEqual(read_entry.file.value_stats.null_counts, null_counts)

    def _test_append_only_schema_match_case(self, table, pa_schema):
        from pypaimon.schema.data_types import PyarrowFieldParser

        self.assertFalse(table.is_primary_key_table,
                         "Table should be append-only (no primary keys)")

        test_data = pa.Table.from_pydict({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'price': [10.5, 20.3, 30.7],
            'category': ['A', 'B', 'C']
        }, schema=pa_schema)

        data_fields_from_schema = PyarrowFieldParser.to_paimon_schema(test_data.schema)
        table_fields = table.fields

        self.assertEqual(
            len(data_fields_from_schema), len(table_fields),
            f"Field count mismatch: data.schema has {len(data_fields_from_schema)} fields, "
            f"but table.fields has {len(table_fields)} fields"
        )

        data_field_names = {field.name for field in data_fields_from_schema}
        table_field_names = {field.name for field in table_fields}
        self.assertEqual(
            data_field_names, table_field_names,
            f"Field names mismatch: data.schema has {data_field_names}, "
            f"but table.fields has {table_field_names}"
        )

    def test_primary_key_value_stats(self):
        pa_schema = pa.schema([
            ('id', pa.int64()),
            ('name', pa.string()),
            ('price', pa.float64()),
            ('category', pa.string())
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            primary_keys=['id'],
            options={'metadata.stats-mode': 'full', 'bucket': '2'}
        )
        self.catalog.create_table('default.test_pk_value_stats', schema, False)
        table = self.catalog.get_table('default.test_pk_value_stats')

        test_data = pa.Table.from_pydict({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'price': [10.5, 20.3, 30.7, 40.1, 50.9],
            'category': ['A', 'B', 'C', 'D', 'E']
        }, schema=pa_schema)

        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(test_data)
        commit_messages = writer.prepare_commit()
        commit = write_builder.new_commit()
        commit.commit(commit_messages)
        writer.close()

        # Verify that data.schema (converted to paimon schema) matches table.fields
        data_fields_from_schema = PyarrowFieldParser.to_paimon_schema(test_data.schema)
        table_fields = table.fields

        # Verify field count matches
        self.assertEqual(len(data_fields_from_schema), len(table_fields),
                         f"Field count mismatch: data.schema has {len(data_fields_from_schema)} fields, "
                         f"but table.fields has {len(table_fields)} fields")

        # Verify field names match (order may differ, but names should match)
        data_field_names = {field.name for field in data_fields_from_schema}
        table_field_names = {field.name for field in table_fields}
        self.assertEqual(data_field_names, table_field_names,
                         f"Field names mismatch: data.schema has {data_field_names}, "
                         f"but table.fields has {table_field_names}")

        # Read manifest to verify value_stats_cols is None (all fields included)
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        latest_snapshot = SnapshotManager(table).get_latest_snapshot()
        manifest_files = table_scan.starting_scanner.manifest_list_manager.read_all(latest_snapshot)
        manifest_entries = table_scan.starting_scanner.manifest_file_manager.read(
            manifest_files[0].file_name,
            lambda row: table_scan.starting_scanner._filter_manifest_entry(row),
            False
        )

        if len(manifest_entries) > 0:
            file_meta = manifest_entries[0].file
            self.assertIsNone(file_meta.value_stats_cols,
                              "value_stats_cols should be None when all table fields are included")
        self.assertGreater(len(manifest_entries), 0, "Should have at least one manifest entry")
        file_meta = manifest_entries[0].file

        key_stats = file_meta.key_stats
        self.assertIsNotNone(key_stats, "key_stats should not be None")
        self.assertGreater(key_stats.min_values.arity, 0, "key_stats should contain key fields")
        self.assertEqual(key_stats.min_values.arity, 1, "key_stats should contain exactly 1 key field (id)")

        value_stats = file_meta.value_stats
        self.assertIsNotNone(value_stats, "value_stats should not be None")
        
        if file_meta.value_stats_cols is None:
            expected_value_fields = ['name', 'price', 'category']
            self.assertGreaterEqual(value_stats.min_values.arity, len(expected_value_fields),
                                    f"value_stats should contain at least {len(expected_value_fields)} value fields")
        else:
            self.assertNotIn('id', file_meta.value_stats_cols,
                             "Key field 'id' should NOT be in value_stats_cols")
            
            expected_value_fields = ['name', 'price', 'category']
            self.assertTrue(set(expected_value_fields).issubset(set(file_meta.value_stats_cols)),
                            f"value_stats_cols should contain value fields: {expected_value_fields}, "
                            f"but got: {file_meta.value_stats_cols}")
            
            expected_arity = len(file_meta.value_stats_cols)
            self.assertEqual(value_stats.min_values.arity, expected_arity,
                             f"value_stats should contain {expected_arity} fields (matching value_stats_cols), "
                             f"but got {value_stats.min_values.arity}")
            self.assertEqual(value_stats.max_values.arity, expected_arity,
                             f"value_stats should contain {expected_arity} fields (matching value_stats_cols), "
                             f"but got {value_stats.max_values.arity}")
            self.assertEqual(len(value_stats.null_counts), expected_arity,
                             f"value_stats null_counts should have {expected_arity} elements, "
                             f"but got {len(value_stats.null_counts)}")
            
            self.assertEqual(value_stats.min_values.arity, len(file_meta.value_stats_cols),
                             f"value_stats.min_values.arity ({value_stats.min_values.arity}) must match "
                             f"value_stats_cols length ({len(file_meta.value_stats_cols)})")
            
            for field_name in file_meta.value_stats_cols:
                is_system_field = (field_name.startswith('_KEY_') or
                                   field_name in ['_SEQUENCE_NUMBER', '_VALUE_KIND', '_ROW_ID'])
                self.assertFalse(is_system_field,
                                 f"value_stats_cols should not contain system field: {field_name}")
            
            value_stats_fields = table_scan.starting_scanner.manifest_file_manager._get_value_stats_fields(
                {'_VALUE_STATS_COLS': file_meta.value_stats_cols},
                table.fields
            )
            min_value_stats = GenericRowDeserializer.from_bytes(
                value_stats.min_values.data,
                value_stats_fields
            ).values
            max_value_stats = GenericRowDeserializer.from_bytes(
                value_stats.max_values.data,
                value_stats_fields
            ).values

            self.assertEqual(len(min_value_stats), 3, "min_value_stats should have 3 values")
            self.assertEqual(len(max_value_stats), 3, "max_value_stats should have 3 values")
        
        actual_data = read_builder.new_read().to_arrow(table_scan.plan().splits())
        self.assertEqual(actual_data.num_rows, 5, "Should have 5 rows")
        actual_ids = sorted(actual_data.column('id').to_pylist())
        self.assertEqual(actual_ids, [1, 2, 3, 4, 5], "All IDs should be present")

    def test_split_target_size(self):
        """Test source.split.target-size configuration effect on split generation."""
        from pypaimon.common.options.core_options import CoreOptions

        pa_schema = pa.schema([
            ('f0', pa.int64()),
            ('f1', pa.string())
        ])

        # Test with small target_split_size (512B) - should generate more splits
        schema_small = Schema.from_pyarrow_schema(
            pa_schema,
            options={CoreOptions.SOURCE_SPLIT_TARGET_SIZE.key(): '512b'}
        )
        self.catalog.create_table('default.test_split_target_size_small', schema_small, False)
        table_small = self.catalog.get_table('default.test_split_target_size_small')

        for i in range(10):
            write_builder = table_small.new_batch_write_builder()
            table_write = write_builder.new_write()
            table_commit = write_builder.new_commit()
            data = pa.Table.from_pydict({
                'f0': list(range(i * 100, (i + 1) * 100)),
                'f1': [f'value_{j}' for j in range(i * 100, (i + 1) * 100)]
            }, schema=pa_schema)
            table_write.write_arrow(data)
            table_commit.commit(table_write.prepare_commit())
            table_write.close()
            table_commit.close()

        read_builder = table_small.new_read_builder()
        splits_small = read_builder.new_scan().plan().splits()

        schema_default = Schema.from_pyarrow_schema(pa_schema)
        self.catalog.create_table('default.test_split_target_size_default', schema_default, False)
        table_default = self.catalog.get_table('default.test_split_target_size_default')

        for i in range(10):
            write_builder = table_default.new_batch_write_builder()
            table_write = write_builder.new_write()
            table_commit = write_builder.new_commit()
            data = pa.Table.from_pydict({
                'f0': list(range(i * 100, (i + 1) * 100)),
                'f1': [f'value_{j}' for j in range(i * 100, (i + 1) * 100)]
            }, schema=pa_schema)
            table_write.write_arrow(data)
            table_commit.commit(table_write.prepare_commit())
            table_write.close()
            table_commit.close()

        # Generate splits with default target_split_size
        read_builder = table_default.new_read_builder()
        splits_default = read_builder.new_scan().plan().splits()

        self.assertGreater(
            len(splits_small), len(splits_default),
            f"Small target_split_size should generate more splits. "
            f"Got {len(splits_small)} splits with 512B vs "
            f"{len(splits_default)} splits with default")

    def test_split_open_file_cost(self):
        """Test source.split.open-file-cost configuration effect on split generation."""
        from pypaimon.common.options.core_options import CoreOptions

        pa_schema = pa.schema([
            ('f0', pa.int64()),
            ('f1', pa.string())
        ])

        # Test with large open_file_cost (64MB) - should generate more splits
        schema_large_cost = Schema.from_pyarrow_schema(
            pa_schema,
            options={
                CoreOptions.SOURCE_SPLIT_TARGET_SIZE.key(): '128mb',
                CoreOptions.SOURCE_SPLIT_OPEN_FILE_COST.key(): '64mb'
            }
        )
        self.catalog.create_table('default.test_split_open_file_cost_large', schema_large_cost, False)
        table_large_cost = self.catalog.get_table('default.test_split_open_file_cost_large')

        # Write multiple batches to create multiple files
        # Write 10 batches, each with 100 rows
        for i in range(10):
            write_builder = table_large_cost.new_batch_write_builder()
            table_write = write_builder.new_write()
            table_commit = write_builder.new_commit()
            data = pa.Table.from_pydict({
                'f0': list(range(i * 100, (i + 1) * 100)),
                'f1': [f'value_{j}' for j in range(i * 100, (i + 1) * 100)]
            }, schema=pa_schema)
            table_write.write_arrow(data)
            table_commit.commit(table_write.prepare_commit())
            table_write.close()
            table_commit.close()

        # Generate splits with large open_file_cost
        read_builder = table_large_cost.new_read_builder()
        splits_large_cost = read_builder.new_scan().plan().splits()

        # Test with default open_file_cost (4MB) - should generate fewer splits
        schema_default = Schema.from_pyarrow_schema(
            pa_schema,
            options={CoreOptions.SOURCE_SPLIT_TARGET_SIZE.key(): '128mb'}
        )
        self.catalog.create_table('default.test_split_open_file_cost_default', schema_default, False)
        table_default = self.catalog.get_table('default.test_split_open_file_cost_default')

        # Write same amount of data
        for i in range(10):
            write_builder = table_default.new_batch_write_builder()
            table_write = write_builder.new_write()
            table_commit = write_builder.new_commit()
            data = pa.Table.from_pydict({
                'f0': list(range(i * 100, (i + 1) * 100)),
                'f1': [f'value_{j}' for j in range(i * 100, (i + 1) * 100)]
            }, schema=pa_schema)
            table_write.write_arrow(data)
            table_commit.commit(table_write.prepare_commit())
            table_write.close()
            table_commit.close()

        # Generate splits with default open_file_cost
        read_builder = table_default.new_read_builder()
        splits_default = read_builder.new_scan().plan().splits()

        # With default open_file_cost (4MB), more files can be packed into each split
        self.assertGreater(
            len(splits_large_cost), len(splits_default),
            f"Large open_file_cost should generate more splits. "
            f"Got {len(splits_large_cost)} splits with 64MB cost vs "
            f"{len(splits_default)} splits with default")

    def test_create_table_with_invalid_type(self):
        """Test create_table raises ValueError when table type is not 'table'."""
        from pypaimon.common.options.core_options import CoreOptions

        pa_schema = pa.schema([
            ('f0', pa.int64()),
            ('f1', pa.string())
        ])

        # Create schema with invalid type option (not "table")
        schema_with_invalid_type = Schema.from_pyarrow_schema(
            pa_schema,
            options={CoreOptions.TYPE.key(): 'view'}  # Invalid type, should be "table"
        )

        # Attempt to create table should raise ValueError
        with self.assertRaises(ValueError) as context:
            self.catalog.create_table('default.test_invalid_type', schema_with_invalid_type, False)

        # Verify the error message contains the expected text
        self.assertIn("Table Type", str(context.exception))
