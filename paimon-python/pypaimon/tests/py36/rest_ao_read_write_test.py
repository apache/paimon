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
import logging
import time
from datetime import date
from decimal import Decimal
from unittest.mock import Mock

import numpy as np
import pandas as pd
import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.common.options import Options
from pypaimon.catalog.catalog_context import CatalogContext
from pypaimon.catalog.rest.rest_catalog import RESTCatalog
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.identifier import Identifier
from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.snapshot.snapshot_manager import SnapshotManager
from pypaimon.table.row.generic_row import (GenericRow, GenericRowDeserializer,
                                            GenericRowSerializer)
from pypaimon.table.row.row_kind import RowKind
from pypaimon.tests.py36.pyarrow_compat import table_sort_by
from pypaimon.tests.rest.rest_base_test import RESTBaseTest
from pypaimon.write.file_store_commit import FileStoreCommit


class RESTAOReadWritePy36Test(RESTBaseTest):

    def test_overwrite(self):
        simple_pa_schema = pa.schema([
            ('f0', pa.int32()),
            ('f1', pa.string())
        ])
        schema = Schema.from_pyarrow_schema(simple_pa_schema, partition_keys=['f0'],
                                            options={'dynamic-partition-overwrite': 'false'})
        self.rest_catalog.create_table('default.test_overwrite', schema, False)
        table = self.rest_catalog.get_table('default.test_overwrite')
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
            ('f11', pa.date32()),
        ])
        schema = Schema.from_pyarrow_schema(simple_pa_schema)
        self.rest_catalog.create_table('default.test_full_data_types', schema, False)
        table = self.rest_catalog.get_table('default.test_full_data_types')

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
            'f11': [date(1999, 12, 31), date(2023, 1, 1)],
        }, schema=simple_pa_schema)
        table_write.write_arrow(expect_data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        actual_data = table_read.to_arrow(table_scan.plan().splits())
        self.assertEqual(actual_data, expect_data)

        # to test GenericRow ability
        latest_snapshot = SnapshotManager(table).get_latest_snapshot()
        manifest_files = table_scan.starting_scanner.manifest_list_manager.read_all(latest_snapshot)
        manifest_entries = table_scan.starting_scanner.manifest_file_manager.read(
            manifest_files[0].file_name,
            lambda row: table_scan.starting_scanner._filter_manifest_entry(row),
            drop_stats=False)
        min_value_stats = GenericRowDeserializer.from_bytes(manifest_entries[0].file.value_stats.min_values.data,
                                                            table.fields).values
        max_value_stats = GenericRowDeserializer.from_bytes(manifest_entries[0].file.value_stats.max_values.data,
                                                            table.fields).values
        expected_min_values = [col[0].as_py() for col in expect_data]
        expected_max_values = [col[1].as_py() for col in expect_data]
        self.assertEqual(min_value_stats, expected_min_values)
        self.assertEqual(max_value_stats, expected_max_values)

    def test_mixed_add_and_delete_entries_same_partition(self):
        """Test record_count calculation with mixed ADD/DELETE entries in same partition."""
        pa_schema = pa.schema([
            ('region', pa.string()),
            ('city', pa.string())
        ])
        schema = Schema.from_pyarrow_schema(pa_schema)
        self.rest_catalog.create_table('default.tb', schema, False)
        table = self.rest_catalog.get_table('default.tb')
        partition_fields = [
            DataField(0, "region", AtomicType("STRING")),
            DataField(1, "city", AtomicType("STRING"))
        ]
        partition = GenericRow(['East', 'Boston'], partition_fields)

        # Create ADD entry
        from pypaimon.data.timestamp import Timestamp
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

    def test_multiple_partitions_with_different_operations(self):
        """Test record_count calculation across multiple partitions."""
        pa_schema = pa.schema([
            ('region', pa.string()),
            ('city', pa.string())
        ])
        schema = Schema.from_pyarrow_schema(pa_schema)
        self.rest_catalog.create_table('default.tb1', schema, False)
        table = self.rest_catalog.get_table('default.tb1')
        partition_fields = [
            DataField(0, "region", AtomicType("STRING")),
            DataField(1, "city", AtomicType("STRING"))
        ]
        partition1 = GenericRow(['East', 'Boston'], partition_fields)
        from pypaimon.data.timestamp import Timestamp
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

    def test_parquet_append_only_reader(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.rest_catalog.create_table('default.test_append_only_parquet', schema, False)
        table = self.rest_catalog.get_table('default.test_append_only_parquet')
        self._write_test_table(table)

        read_builder = table.new_read_builder()
        actual = table_sort_by(self._read_test_table(read_builder), 'user_id')
        self.assertEqual(actual, self.expected)

    def test_orc_append_only_reader(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'], options={'file.format': 'orc'})
        self.rest_catalog.create_table('default.test_append_only_orc', schema, False)
        table = self.rest_catalog.get_table('default.test_append_only_orc')
        self._write_test_table(table)

        read_builder = table.new_read_builder()
        actual = table_sort_by(self._read_test_table(read_builder), 'user_id')
        self.assertEqual(actual, self.expected)

    def test_avro_append_only_reader(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'], options={'file.format': 'avro'})
        self.rest_catalog.create_table('default.test_append_only_avro', schema, False)
        table = self.rest_catalog.get_table('default.test_append_only_avro')
        self._write_test_table(table)

        read_builder = table.new_read_builder()
        actual = table_sort_by(self._read_test_table(read_builder), 'user_id')
        self.assertEqual(actual, self.expected)

    def test_append_only_multi_write_once_commit(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.rest_catalog.create_table('default.test_append_only_multi_once_commit', schema, False)
        table = self.rest_catalog.get_table('default.test_append_only_multi_once_commit')
        write_builder = table.new_batch_write_builder()

        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data1 = {
            'user_id': [1, 2, 3, 4],
            'item_id': [1001, 1002, 1003, 1004],
            'behavior': ['a', 'b', 'c', None],
            'dt': ['p1', 'p1', 'p2', 'p1'],
            'long-dt': ['2024-10-10', '2024-10-10', '2024-10-10', '2024-01-01'],
        }
        pa_table1 = pa.Table.from_pydict(data1, schema=self.pa_schema)
        data2 = {
            'user_id': [5, 6, 7, 8],
            'item_id': [1005, 1006, 1007, 1008],
            'behavior': ['e', 'f', 'g', 'h'],
            'dt': ['p2', 'p1', 'p2', 'p2'],
            'long-dt': ['2024-10-10', '2025-01-23', 'abcdefghijklmnopk', '2025-08-08'],
        }
        pa_table2 = pa.Table.from_pydict(data2, schema=self.pa_schema)

        table_write.write_arrow(pa_table1)
        table_write.write_arrow(pa_table2)

        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        actual = table_sort_by(self._read_test_table(read_builder), 'user_id')
        self.assertEqual(actual, self.expected)

    def test_over1000cols_read(self):
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
        self.rest_catalog.create_table(table_name, schema, False)
        table = self.rest_catalog.get_table(table_name)

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

    def test_append_only_reader_with_filter(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.rest_catalog.create_table('default.test_append_only_filter', schema, False)
        table = self.rest_catalog.get_table('default.test_append_only_filter')
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
        self.assertEqual(table_sort_by(actual, 'user_id'), expected)

        p7 = predicate_builder.startswith('behavior', 'a')
        p8 = predicate_builder.equal('item_id', 1002)
        p9 = predicate_builder.is_null('behavior')
        g2 = predicate_builder.or_predicates([p7, p8, p9])
        read_builder = table.new_read_builder().with_filter(g2)
        actual = self._read_test_table(read_builder)
        expected = pa.concat_tables([
            self.expected.slice(0, 1),  # 1/a
            self.expected.slice(1, 1),  # 2/b
            self.expected.slice(3, 1),  # 5/e
        ])
        self.assertEqual(table_sort_by(actual, 'user_id'), expected)

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
        self.assertEqual(table_sort_by(actual, 'user_id'), expected)

    def test_append_only_reader_with_projection(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.rest_catalog.create_table('default.test_append_only_projection', schema, False)
        table = self.rest_catalog.get_table('default.test_append_only_projection')
        self._write_test_table(table)

        read_builder = table.new_read_builder().with_projection(['dt', 'user_id'])
        actual = table_sort_by(self._read_test_table(read_builder), 'user_id')
        expected = self.expected.select(['dt', 'user_id'])
        self.assertEqual(actual, expected)

    def test_avro_append_only_reader_with_projection(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'], options={'file.format': 'avro'})
        self.rest_catalog.create_table('default.test_avro_append_only_projection', schema, False)
        table = self.rest_catalog.get_table('default.test_avro_append_only_projection')
        self._write_test_table(table)

        read_builder = table.new_read_builder().with_projection(['dt', 'user_id'])
        actual = table_sort_by(self._read_test_table(read_builder), 'user_id')
        expected = self.expected.select(['dt', 'user_id'])
        self.assertEqual(actual, expected)

    def test_append_only_reader_with_limit(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.rest_catalog.create_table('default.test_append_only_limit', schema, False)
        table = self.rest_catalog.get_table('default.test_append_only_limit')
        self._write_test_table(table)

        read_builder = table.new_read_builder().with_limit(1)
        actual = self._read_test_table(read_builder)
        # only records from 1st commit (1st split) will be read
        # might be split of "dt=1" or split of "dt=2"
        self.assertEqual(actual.num_rows, 4)

    def test_write_wrong_schema(self):
        self.rest_catalog.create_table('default.test_wrong_schema',
                                       Schema.from_pyarrow_schema(self.pa_schema),
                                       False)
        table = self.rest_catalog.get_table('default.test_wrong_schema')

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

    def test_write_wide_table_large_data(self):
        logging.basicConfig(level=logging.INFO)
        catalog = CatalogFactory.create(self.options)

        # Build table structure: 200 data columns + 1 partition column
        # Create PyArrow schema
        pa_fields = []

        # Create 200 data columns f0 to f199
        for i in range(200):
            pa_fields.append(pa.field(f"f{i}", pa.string(), metadata={"description": f"Column f{i}"}))

        # Add partition column dt
        pa_fields.append(pa.field("dt", pa.string(), metadata={"description": "Partition column dt"}))

        # Create PyArrow schema
        pa_schema = pa.schema(pa_fields)

        # Convert to Paimon Schema and specify partition key
        schema = Schema.from_pyarrow_schema(pa_schema, partition_keys=["dt"])

        # Create table
        table_identifier = Identifier.create("default", "wide_table_200cols")
        try:
            # If table already exists, drop it first
            try:
                catalog.get_table(table_identifier)
                catalog.drop_table(table_identifier)
                print(f"Dropped existing table {table_identifier}")
            except Exception:
                # Table does not exist, continue creating
                pass

            # Create new table
            catalog.create_table(
                identifier=table_identifier,
                schema=schema,
                ignore_if_exists=False
            )

            print(
                f"Successfully created table {table_identifier} with {len(pa_fields) - 1} "
                f"data columns and 1 partition column")
            print(
                f"Table schema: {len([f for f in pa_fields if f.name != 'dt'])} data columns (f0-f199) + dt partition")

        except Exception as e:
            print(f"Error creating table: {e}")
            raise e
        import random

        table_identifier = Identifier.create("default", "wide_table_200cols")
        table = catalog.get_table(table_identifier)

        total_rows = 500000  # rows of data
        batch_size = 100000  # 100,000 rows per batch
        commit_batches = total_rows // batch_size

        for commit_batch in range(commit_batches):
            start_idx = commit_batch * batch_size
            end_idx = start_idx + batch_size

            print(f"Processing batch {commit_batch + 1}/{commit_batches} ({start_idx:,} - {end_idx:,})...")
            # Generate data for current batch - generate data for all 200 columns
            data = {}
            # Generate data for f0-f199
            for i in range(200):
                if i == 0:
                    data[f"f{i}"] = [f'value_{j}' for j in range(start_idx, end_idx)]
                elif i == 1:
                    data[f"f{i}"] = [random.choice(['A', 'B', 'C', 'D', 'E']) for _ in range(batch_size)]
                elif i == 2:
                    data[f"f{i}"] = [f'detail_{random.randint(1, 1000)}' for _ in range(batch_size)]
                elif i == 3:
                    data[f"f{i}"] = [f'id_{j:06d}' for j in range(start_idx, end_idx)]
                else:
                    # Generate random string data for other columns
                    data[f"f{i}"] = [f'col{i}_val_{random.randint(1, 10000)}' for _ in range(batch_size)]

            # Add partition column data
            data['dt'] = ['2025-09-01' for _ in range(batch_size)]
            # Convert dictionary to PyArrow RecordBatch
            arrow_batch = pa.RecordBatch.from_pydict(data)
            # Create new write and commit objects for each commit batch
            write_builder = table.new_batch_write_builder()
            table_write = write_builder.new_write()
            table_commit = write_builder.new_commit()

            try:
                # Write current batch data
                table_write.write_arrow_batch(arrow_batch)
                print("Batch data write completed, committing...")
                # Commit current batch
                commit_messages = table_write.prepare_commit()
                table_commit.commit(commit_messages)
                print(f"Batch {commit_batch + 1} committed successfully! Written {end_idx:,} rows of data")

            finally:
                # Ensure resource cleanup
                table_write.close()
                table_commit.close()

        print(
            f"All data writing completed! "
            f"Total written {total_rows:,} rows of data to 200-column wide table in {commit_batches} commits")
        rest_catalog = RESTCatalog(CatalogContext.create_from_options(Options(self.options)))
        table = rest_catalog.get_table('default.wide_table_200cols')
        predicate_builder = table.new_read_builder().new_predicate_builder()
        read_builder = (table.new_read_builder()
                        .with_projection(['f0', 'f1'])
                        .with_filter(predicate=predicate_builder.equal("dt", "2025-09-01")))
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        self.assertEqual(table_read.to_arrow(splits).num_rows, total_rows)

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

    def test_value_stats_cols_logic(self):
        """Test _VALUE_STATS_COLS logic in ManifestFileManager."""
        self.rest_catalog.create_database("test_db", False)

        # Define schema with multiple fields
        pa_schema = pa.schema([
            ('id', pa.int64()),
            ('name', pa.string()),
            ('price', pa.float64()),
            ('category', pa.string())
        ])
        schema = Schema.from_pyarrow_schema(pa_schema)
        self.rest_catalog.create_table("test_db.test_value_stats_cols", schema, False)
        table = self.rest_catalog.get_table("test_db.test_value_stats_cols")

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

    def test_incremental_timestamp(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.rest_catalog.create_table('default.test_incremental_parquet', schema, False)
        table = self.rest_catalog.get_table('default.test_incremental_parquet')
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
        actual = table_sort_by(self._read_test_table(read_builder), 'user_id')
        self.assertEqual(self.expected, actual)
        # test 3
        table = table.copy({CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP.key(): str(t1) + ',' + str(t2)})
        read_builder = table.new_read_builder()
        actual = table_sort_by(self._read_test_table(read_builder), 'user_id')
        expected = self.expected.slice(4, 4)
        self.assertEqual(expected, actual)

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
