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
import sys
import tempfile
import time
import unittest

import numpy as np
import pandas as pd
import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.write.file_store_commit import RetryResult


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

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

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

    def test_plan_snapshot_id_for_empty_and_non_empty_scan(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.catalog.create_table('default.test_plan_snapshot_id', schema, False)
        table = self.catalog.get_table('default.test_plan_snapshot_id')

        empty_plan = table.new_read_builder().new_scan().plan()
        self.assertIsNone(empty_plan.snapshot_id)
        self.assertEqual(len(empty_plan.splits()), 0)

        self._write_test_table(table)

        plan = table.new_read_builder().new_scan().plan()
        self.assertEqual(plan.snapshot_id, 2)
        self.assertGreater(len(plan.splits()), 0)

    def test_incremental_timestamp_empty_range_keeps_end_snapshot_id(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.catalog.create_table('default.test_incremental_empty_range_snapshot', schema, False)
        table = self.catalog.get_table('default.test_incremental_empty_range_snapshot')

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        pa_table = pa.Table.from_pydict({
            'user_id': [1],
            'item_id': [1001],
            'behavior': ['a'],
            'dt': ['p1'],
        }, schema=self.pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        snapshot_manager = table.snapshot_manager()
        snapshot = snapshot_manager.get_latest_snapshot()
        table_inc = table.copy({
            CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP.key():
                "{},{}".format(snapshot.time_millis, snapshot.time_millis + 1)
        })

        plan = table_inc.new_read_builder().new_scan().plan()
        self.assertEqual(plan.snapshot_id, snapshot.id)
        self.assertEqual(len(plan.splits()), 0)

    @unittest.skipIf(sys.version_info < (3, 11), "vortex-data requires Python >= 3.11")
    def test_vortex_ao_reader(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'], options={'file.format': 'vortex'})
        self.catalog.create_table('default.test_append_only_vortex', schema, False)
        table = self.catalog.get_table('default.test_append_only_vortex')
        self._write_test_table(table)

        read_builder = table.new_read_builder()
        actual = self._read_test_table(read_builder).sort_by('user_id')
        self.assertEqual(actual, self.expected)

    @unittest.skipIf(sys.version_info < (3, 11), "vortex-data requires Python >= 3.11")
    def test_vortex_ao_reader_with_filter(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'], options={'file.format': 'vortex'})
        self.catalog.create_table('default.test_append_only_vortex_filter', schema, False)
        table = self.catalog.get_table('default.test_append_only_vortex_filter')
        self._write_test_table(table)

        predicate_builder = table.new_read_builder().new_predicate_builder()
        p1 = predicate_builder.less_than('user_id', 7)
        p2 = predicate_builder.greater_or_equal('user_id', 2)
        p3 = predicate_builder.between('user_id', 0, 6)  # [2/b, 3/c, 4/d, 5/e, 6/f] left
        p6 = predicate_builder.is_not_null('behavior')  # exclude 4/d -> [2/b, 3/c, 5/e, 6/f]
        g1 = predicate_builder.and_predicates([p1, p2, p3, p6])
        read_builder = table.new_read_builder().with_filter(g1)
        actual = self._read_test_table(read_builder)
        expected = pa.concat_tables([
            self.expected.slice(1, 2),  # 2/b, 3/c
            self.expected.slice(4, 2),  # 5/e, 6/f
        ])
        self.assertEqual(actual.sort_by('user_id'), expected)

        # OR predicates with startswith, endswith, contains, equal, is_null
        p7 = predicate_builder.startswith('behavior', 'a')
        p10 = predicate_builder.equal('item_id', 1002)
        p11 = predicate_builder.is_null('behavior')
        p9 = predicate_builder.contains('behavior', 'f')
        p8 = predicate_builder.endswith('dt', 'p2')
        g2 = predicate_builder.or_predicates([p7, p8, p9, p10, p11])
        read_builder = table.new_read_builder().with_filter(g2)
        actual = self._read_test_table(read_builder)
        self.assertEqual(actual.sort_by('user_id'), self.expected)

        # Combined AND + OR
        g3 = predicate_builder.and_predicates([g1, g2])
        read_builder = table.new_read_builder().with_filter(g3)
        actual = self._read_test_table(read_builder)
        expected = pa.concat_tables([
            self.expected.slice(1, 2),  # 2/b, 3/c
            self.expected.slice(4, 2),  # 5/e, 6/f
        ])
        self.assertEqual(actual.sort_by('user_id'), expected)

        # not_equal also filters None values
        p12 = predicate_builder.not_equal('behavior', 'f')
        read_builder = table.new_read_builder().with_filter(p12)
        actual = self._read_test_table(read_builder)
        expected = pa.concat_tables([
            self.expected.slice(0, 1),  # 1/a
            self.expected.slice(1, 1),  # 2/b
            self.expected.slice(2, 1),  # 3/c
            self.expected.slice(4, 1),  # 5/e
            self.expected.slice(6, 1),  # 7/g
            self.expected.slice(7, 1),  # 8/h
        ])
        self.assertEqual(actual.sort_by('user_id'), expected)

    @unittest.skipIf(sys.version_info < (3, 11), "vortex-data requires Python >= 3.11")
    def test_vortex_ao_reader_with_projection(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'], options={'file.format': 'vortex'})
        self.catalog.create_table('default.test_vortex_append_only_projection', schema, False)
        table = self.catalog.get_table('default.test_vortex_append_only_projection')
        self._write_test_table(table)

        read_builder = table.new_read_builder().with_projection(['dt', 'user_id'])
        actual = self._read_test_table(read_builder).sort_by('user_id')
        expected = self.expected.select(['dt', 'user_id'])
        self.assertEqual(actual, expected)

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

    def test_lance_sliced_split_row_range_pushdown(self):
        """
        SlicedSplit with Lance format calls read_range() instead of read_all(),
        reading only the requested row slice from disk rather than the full file.
        """
        import unittest.mock as mock
        try:
            import lance as _lance
            import lance.file  # ensure submodule is loaded before write_lance uses it
        except ImportError:
            self.skipTest("lance not installed")

        schema = Schema.from_pyarrow_schema(
            pa.schema([('id', pa.int64()), ('value', pa.string())]),
            options={'file.format': 'lance'})
        self.catalog.create_table('default.test_lance_sliced_split', schema, False)
        table = self.catalog.get_table('default.test_lance_sliced_split')

        # Write 1000 rows in a single shot so they land in one file
        n = 1000
        pa_table = pa.table({'id': list(range(n)), 'value': [f'v{i}' for i in range(n)]})
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        scan = table.new_read_builder().new_scan()
        splits = scan.plan().splits()
        self.assertEqual(len(splits), 1, "Expected single split (no partition, single bucket)")
        data_split = splits[0]
        self.assertEqual(len(data_split.files), 1, "Expected single file in split")

        file_name = data_split.files[0].file_name
        slice_start, slice_end = 200, 700  # request 500 of 1000 rows

        from pypaimon.read.sliced_split import SlicedSplit
        sliced = SlicedSplit(data_split, {file_name: (slice_start, slice_end)})

        # Spy on lance.file.LanceFileReader to verify read_range is used
        OrigReader = _lance.file.LanceFileReader
        read_all_calls = []
        read_range_calls = []

        class SpyLanceFileReader:
            def __init__(self, *args, **kwargs):
                self._r = OrigReader(*args, **kwargs)

            def read_all(self, *args, **kwargs):
                read_all_calls.append(())
                return self._r.read_all(*args, **kwargs)

            def read_range(self, start, count, **kwargs):
                read_range_calls.append((start, count))
                return self._r.read_range(start, count, **kwargs)

            def __getattr__(self, name):
                return getattr(self._r, name)

        table_read = table.new_read_builder().new_read()
        with mock.patch.object(_lance.file, 'LanceFileReader', SpyLanceFileReader):
            result = table_read.to_arrow([sliced])

        # Verify that read_range was used, not read_all
        self.assertEqual(len(read_all_calls), 0,
                         "read_all() must not be called when SlicedSplit row range is available")
        self.assertEqual(len(read_range_calls), 1)
        actual_start, actual_count = read_range_calls[0]
        self.assertEqual(actual_start, slice_start)
        self.assertEqual(actual_count, slice_end - slice_start,
                         f"read_range should request exactly {slice_end - slice_start} rows, "
                         f"not the full {n} rows of the file")

        # Verify functional correctness: correct rows returned
        self.assertEqual(result.num_rows, slice_end - slice_start)
        result_ids = sorted(result.column('id').to_pylist())
        self.assertEqual(result_ids, list(range(slice_start, slice_end)))

    def test_lance_indexed_split_take_rows_pushdown(self):
        """
        IndexedSplit row ranges (from ANN global index results) are converted to
        local file indices and pushed down to lance.file.LanceFileReader.take_rows(),
        so only the matched rows are physically read instead of the full file.
        """
        import unittest.mock as mock
        try:
            import lance as _lance
            import lance.file
        except ImportError:
            self.skipTest("lance not installed")

        schema = Schema.from_pyarrow_schema(
            pa.schema([('id', pa.int64()), ('value', pa.string())]),
            options={'file.format': 'lance',
                     'row-tracking.enabled': 'true',
                     'data-evolution.enabled': 'true'})
        self.catalog.create_table('default.test_lance_indexed_split', schema, False)
        table = self.catalog.get_table('default.test_lance_indexed_split')

        n = 1000
        pa_table = pa.table({'id': list(range(n)), 'value': [f'v{i}' for i in range(n)]})
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        splits = table.new_read_builder().new_scan().plan().splits()
        self.assertEqual(len(splits), 1)
        data_split = splits[0]
        self.assertEqual(len(data_split.files), 1)

        file_meta = data_split.files[0]
        self.assertIsNotNone(file_meta.first_row_id, "row tracking must assign first_row_id")
        first_row_id = file_meta.first_row_id

        # Simulate ANN result: 3 scattered global row IDs
        from pypaimon.globalindex.indexed_split import IndexedSplit
        from pypaimon.utils.range import Range
        target_global_ids = [first_row_id + 50, first_row_id + 300, first_row_id + 700]
        row_ranges = [Range(g, g) for g in target_global_ids]
        indexed = IndexedSplit(data_split, row_ranges)

        OrigReader = _lance.file.LanceFileReader
        take_rows_calls = []
        read_all_calls = []

        class SpyLanceFileReader:
            def __init__(self, *args, **kwargs):
                self._r = OrigReader(*args, **kwargs)

            def take_rows(self, indices, **kwargs):
                take_rows_calls.append(list(indices))
                return self._r.take_rows(indices, **kwargs)

            def read_all(self, *args, **kwargs):
                read_all_calls.append(())
                return self._r.read_all(*args, **kwargs)

            def read_range(self, *args, **kwargs):
                return self._r.read_range(*args, **kwargs)

            def __getattr__(self, name):
                return getattr(self._r, name)

        table_read = table.new_read_builder().new_read()
        with mock.patch.object(_lance.file, 'LanceFileReader', SpyLanceFileReader):
            result = table_read.to_arrow([indexed])

        self.assertEqual(len(read_all_calls), 0,
                         "read_all() must not be called when IndexedSplit row ranges are available")
        self.assertEqual(len(take_rows_calls), 1)
        # local indices = global_ids - first_row_id
        expected_local = [g - first_row_id for g in target_global_ids]
        self.assertEqual(sorted(take_rows_calls[0]), sorted(expected_local),
                         f"take_rows should request local indices {expected_local}, "
                         f"not read the full {n} rows")

        self.assertEqual(result.num_rows, len(target_global_ids))
        result_ids = sorted(result.column('id').to_pylist())
        self.assertEqual(result_ids, [50, 300, 700])

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

    def test_commit_retry_filter(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.catalog.create_table('default.test_commit_retry_filter', schema, False)
        table = self.catalog.get_table('default.test_commit_retry_filter')
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

        messages = table_write.prepare_commit()
        table_commit.commit(messages)
        table_write.close()

        snapshot_manager = table.snapshot_manager()
        latest_snapshot = snapshot_manager.get_latest_snapshot()
        commit_entries = []
        for msg in messages:
            partition = GenericRow(list(msg.partition), table.partition_keys_fields)
            for file in msg.new_files:
                commit_entries.append(ManifestEntry(
                    kind=0,
                    partition=partition,
                    bucket=msg.bucket,
                    total_buckets=table.total_buckets,
                    file=file
                ))
        # mock retry
        success = table_commit.file_store_commit._try_commit_once(
            RetryResult(None),
            "APPEND",
            commit_entries,
            BATCH_COMMIT_IDENTIFIER,
            latest_snapshot)
        self.assertTrue(success.is_success())
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

        snapshot_manager = table.snapshot_manager()
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

        snapshot_manager = table.snapshot_manager()
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

    def test_concurrent_writes_with_retry(self):
        """Test concurrent writes to verify retry mechanism works correctly."""
        import threading

        # Run the test 10 times to verify stability
        iter_num = 5
        for test_iteration in range(iter_num):
            # Create a unique table for each iteration
            table_name = f'default.test_concurrent_writes_{test_iteration}'
            schema = Schema.from_pyarrow_schema(self.pa_schema)
            self.catalog.create_table(table_name, schema, False)
            table = self.catalog.get_table(table_name)

            write_results = []
            write_errors = []

            def write_data(thread_id, start_user_id):
                """Write data in a separate thread."""
                try:
                    threading.current_thread().name = f"Iter{test_iteration}-Thread-{thread_id}"
                    write_builder = table.new_batch_write_builder()
                    table_write = write_builder.new_write()
                    table_commit = write_builder.new_commit()

                    # Create unique data for this thread
                    data = {
                        'user_id': list(range(start_user_id, start_user_id + 5)),
                        'item_id': [1000 + i for i in range(start_user_id, start_user_id + 5)],
                        'behavior': [f'thread{thread_id}_{i}' for i in range(5)],
                        'dt': ['p1' if i % 2 == 0 else 'p2' for i in range(5)],
                    }
                    pa_table = pa.Table.from_pydict(data, schema=self.pa_schema)

                    table_write.write_arrow(pa_table)
                    commit_messages = table_write.prepare_commit()

                    table_commit.commit(commit_messages)
                    table_write.close()
                    table_commit.close()

                    write_results.append({
                        'thread_id': thread_id,
                        'start_user_id': start_user_id,
                        'success': True
                    })
                except Exception as e:
                    write_errors.append({
                        'thread_id': thread_id,
                        'error': str(e)
                    })

            # Create and start multiple threads
            threads = []
            num_threads = 10
            for i in range(num_threads):
                thread = threading.Thread(
                    target=write_data,
                    args=(i, i * 10)
                )
                threads.append(thread)
                thread.start()

            # Wait for all threads to complete
            for thread in threads:
                thread.join()

            # Verify all writes succeeded (retry mechanism should handle conflicts)
            self.assertEqual(num_threads, len(write_results),
                             f"Iteration {test_iteration}: Expected {num_threads} successful writes, "
                             f"got {len(write_results)}. Errors: {write_errors}")
            self.assertEqual(0, len(write_errors),
                             f"Iteration {test_iteration}: Expected no errors, but got: {write_errors}")

            read_builder = table.new_read_builder()
            actual = self._read_test_table(read_builder).sort_by('user_id')

            # Verify data rows
            self.assertEqual(num_threads * 5, actual.num_rows,
                             f"Iteration {test_iteration}: Expected {num_threads * 5} rows")

            # Verify user_id
            user_ids = actual.column('user_id').to_pylist()
            expected_user_ids = []
            for i in range(num_threads):
                expected_user_ids.extend(range(i * 10, i * 10 + 5))
            expected_user_ids.sort()

            self.assertEqual(user_ids, expected_user_ids,
                             f"Iteration {test_iteration}: User IDs mismatch")

            # Verify snapshot count (should have num_threads snapshots)
            snapshot_manager = table.snapshot_manager()
            latest_snapshot = snapshot_manager.get_latest_snapshot()
            self.assertIsNotNone(latest_snapshot,
                                 f"Iteration {test_iteration}: Latest snapshot should not be None")
            self.assertEqual(latest_snapshot.id, num_threads,
                             f"Iteration {test_iteration}: Expected snapshot ID {num_threads}, "
                             f"got {latest_snapshot.id}")

            print(f"✓ Iteration {test_iteration + 1}/{iter_num} completed successfully")

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
