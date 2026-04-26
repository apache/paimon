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

import datetime
import os
import sys
import unittest

import pandas as pd
import pyarrow as pa
from parameterized import parameterized
from pypaimon.catalog.catalog_factory import CatalogFactory
from pypaimon.schema.schema import Schema
from pypaimon.read.read_builder import ReadBuilder

if sys.version_info[:2] == (3, 6):
    from pypaimon.tests.py36.pyarrow_compat import table_sort_by
else:
    def table_sort_by(table: pa.Table, column_name: str, order: str = 'ascending') -> pa.Table:
        return table.sort_by([(column_name, order)])


def get_file_format_params():
    if sys.version_info[:2] == (3, 6):
        return [('parquet',), ('orc',), ('avro',)]
    else:
        return [('parquet',), ('orc',), ('avro',), ('lance',)]


class JavaPyReadWriteTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tempdir = os.path.abspath(".")
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({
            'warehouse': cls.warehouse
        })
        cls.catalog.create_database('default', True)

    @parameterized.expand(get_file_format_params())
    def test_py_write_read_append_table(self, file_format):
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('category', pa.string()),
            ('value', pa.float64()),
            ('ts', pa.timestamp('us')),
            ('ts_ltz', pa.timestamp('us', tz='UTC')),
            ('t', pa.time32('ms'))
        ])

        schema = Schema.from_pyarrow_schema(
            pa_schema,
            partition_keys=['category'],
            options={'dynamic-partition-overwrite': 'false', 'file.format': file_format}
        )

        table_name = f'default.mixed_test_append_tablep_{file_format}'
        self.catalog.create_table(table_name, schema, False)
        table = self.catalog.get_table(table_name)

        initial_data = pd.DataFrame({
            'id': [1, 2, 3, 4, 5, 6],
            'name': ['Apple', 'Banana', 'Carrot', 'Broccoli', 'Chicken', 'Beef'],
            'category': ['Fruit', 'Fruit', 'Vegetable', 'Vegetable', 'Meat', 'Meat'],
            'value': [1.5, 0.8, 0.6, 1.2, 5.0, 8.0],
            'ts': pd.to_datetime([1000000, 1000001, 1000002, 1000003, 1000004, 1000005], unit='ms'),
            'ts_ltz': pd.to_datetime([2000000, 2000001, 2000002, 2000003, 2000004, 2000005], unit='ms', utc=True),
            't': [datetime.time(0, 0, 1), datetime.time(0, 0, 2), datetime.time(0, 0, 3),
                  datetime.time(0, 0, 4), datetime.time(0, 0, 5), datetime.time(0, 0, 6)]
        })
        # Write initial data
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        table_write.write_pandas(initial_data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # Verify initial data
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        initial_result = table_read.to_pandas(table_scan.plan().splits())
        print(initial_result)
        self.assertEqual(len(initial_result), 6)
        # Data order may vary due to partitioning/bucketing, so compare as sets
        expected_names = {'Apple', 'Banana', 'Carrot', 'Broccoli', 'Chicken', 'Beef'}
        actual_names = set(initial_result['name'].tolist())
        self.assertEqual(actual_names, expected_names)

    @parameterized.expand(get_file_format_params())
    def test_read_append_table(self, file_format):
        table = self.catalog.get_table('default.mixed_test_append_tablej_' + file_format)
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        res = table_read.to_pandas(table_scan.plan().splits())
        print(res)

    @parameterized.expand(get_file_format_params())
    def test_py_write_read_pk_table(self, file_format):
        # Lance format doesn't support timestamp, so exclude timestamp columns
        if file_format == 'lance':
            pa_schema = pa.schema([
                ('id', pa.int32()),
                ('name', pa.string()),
                ('category', pa.string()),
                ('value', pa.float64())
            ])
        else:
            pa_schema = pa.schema([
                ('id', pa.int32()),
                ('name', pa.string()),
                ('category', pa.string()),
                ('value', pa.float64()),
                ('ts', pa.timestamp('us')),
                ('ts_ltz', pa.timestamp('us', tz='UTC')),
                ('t', pa.time32('ms')),
                ('metadata', pa.struct([
                    pa.field('source', pa.string()),
                    pa.field('created_at', pa.int64()),
                    pa.field('location', pa.struct([
                        pa.field('city', pa.string()),
                        pa.field('country', pa.string())
                    ]))
                ]))
            ])

        table_name = f'default.mixed_test_pk_tablep_{file_format}'
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            partition_keys=['category'],
            primary_keys=['id'],
            options={
                'dynamic-partition-overwrite': 'false',
                'bucket': '4',
                'file.format': file_format,
                "orc.timestamp-ltz.legacy.type": "false"
            }
        )

        try:
            existing_table = self.catalog.get_table(table_name)
            table_path = self.catalog.get_table_path(existing_table.identifier)
            if self.catalog.file_io.exists(table_path):
                self.catalog.file_io.delete(table_path, recursive=True)
        except Exception:
            pass

        self.catalog.create_table(table_name, schema, False)
        table = self.catalog.get_table(table_name)

        # Lance format doesn't support timestamp, so exclude timestamp columns
        if file_format == 'lance':
            initial_data = pd.DataFrame({
                'id': [1, 2, 3, 4, 5, 6],
                'name': ['Apple', 'Banana', 'Carrot', 'Broccoli', 'Chicken', 'Beef'],
                'category': ['Fruit', 'Fruit', 'Vegetable', 'Vegetable', 'Meat', 'Meat'],
                'value': [1.5, 0.8, 0.6, 1.2, 5.0, 8.0]
            })
        else:
            initial_data = pd.DataFrame({
                'id': [1, 2, 3, 4, 5, 6],
                'name': ['Apple', 'Banana', 'Carrot', 'Broccoli', 'Chicken', 'Beef'],
                'category': ['Fruit', 'Fruit', 'Vegetable', 'Vegetable', 'Meat', 'Meat'],
                'value': [1.5, 0.8, 0.6, 1.2, 5.0, 8.0],
                'ts': pd.to_datetime([1000000, 1000001, 1000002, 1000003, 1000004, 1000005], unit='ms'),
                'ts_ltz': pd.to_datetime([2000000, 2000001, 2000002, 2000003, 2000004, 2000005], unit='ms', utc=True),
                't': [datetime.time(0, 0, 1), datetime.time(0, 0, 2), datetime.time(0, 0, 3),
                      datetime.time(0, 0, 4), datetime.time(0, 0, 5), datetime.time(0, 0, 6)],
                'metadata': [
                    {'source': 'store1', 'created_at': 1001, 'location': {'city': 'Beijing', 'country': 'China'}},
                    {'source': 'store1', 'created_at': 1002, 'location': {'city': 'Shanghai', 'country': 'China'}},
                    {'source': 'store2', 'created_at': 1003, 'location': {'city': 'Tokyo', 'country': 'Japan'}},
                    {'source': 'store2', 'created_at': 1004, 'location': {'city': 'Seoul', 'country': 'Korea'}},
                    {'source': 'store3', 'created_at': 1005, 'location': {'city': 'NewYork', 'country': 'USA'}},
                    {'source': 'store3', 'created_at': 1006, 'location': {'city': 'London', 'country': 'UK'}}
                ]
            })
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        table_write.write_pandas(initial_data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        result = table_read.to_pandas(table_scan.plan().splits())
        print(f"Format: {file_format}, Result:\n{result}")
        self.assertEqual(initial_data.to_dict(), result.to_dict())

        from pypaimon.write.row_key_extractor import FixedBucketRowKeyExtractor
        expected_bucket_first_row = 2
        first_row = initial_data.head(1)
        batch = pa.RecordBatch.from_pandas(first_row, schema=pa_schema)
        extractor = FixedBucketRowKeyExtractor(table.table_schema)
        _, buckets = extractor.extract_partition_bucket_batch(batch)
        self.assertEqual(buckets[0], expected_bucket_first_row,
                         "bucket for first row (id=1) with num_buckets=4 must be %d" % expected_bucket_first_row)

    @parameterized.expand(get_file_format_params())
    def test_read_pk_table(self, file_format):
        # Skip ORC format for Python < 3.8 due to pyarrow limitation with TIMESTAMP_INSTANT
        if sys.version_info[:2] < (3, 8) and file_format == 'orc':
            self.skipTest("Skipping ORC format for Python < 3.8 (pyarrow does not support TIMESTAMP_INSTANT)")
        
        table_name = f'default.mixed_test_pk_tablej_{file_format}'
        table = self.catalog.get_table(table_name)
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        res = table_read.to_pandas(table_scan.plan().splits())
        print(f"Format: {file_format}, Result:\n{res}")

        # Verify data
        self.assertEqual(len(res), 7)
        if file_format != "lance":
            self.assertEqual(table.fields[4].type.type, "TIMESTAMP(6)")
            self.assertEqual(table.fields[5].type.type, "TIMESTAMP(6) WITH LOCAL TIME ZONE")
            self.assertEqual(table.fields[6].type.type, "TIME(0)")
            self.assertEqual(table.fields[7].type.type, "BINARY(20)")
            from pypaimon.schema.data_types import RowType
            self.assertIsInstance(table.fields[8].type, RowType)
            metadata_fields = table.fields[8].type.fields
            self.assertEqual(len(metadata_fields), 3)
            self.assertEqual(metadata_fields[0].name, 'source')
            self.assertEqual(metadata_fields[1].name, 'created_at')
            self.assertEqual(metadata_fields[2].name, 'location')
            self.assertIsInstance(metadata_fields[2].type, RowType)
        
        # Data order may vary due to partitioning/bucketing, so compare as sets
        expected_names = {'Apple', 'Banana', 'Carrot', 'Broccoli', 'Chicken', 'Beef', 'Tofu'}
        actual_names = set(res['name'].tolist())
        self.assertEqual(actual_names, expected_names)

        # Verify null partition value (default partition) is readable
        tofu_row = res[res['name'] == 'Tofu']
        self.assertEqual(len(tofu_row), 1)
        self.assertTrue(pd.isna(tofu_row['category'].iloc[0]))

        if file_format != "lance" and 'bin_data' in res.columns:
            apple_row = res[res['name'] == 'Apple']
            self.assertEqual(apple_row['bin_data'].iloc[0], b'apple_bin_data')
            carrot_row = res[res['name'] == 'Carrot']
            self.assertEqual(carrot_row['bin_data'].iloc[0], b'carrot')

        # Verify metadata column can be read and contains nested structures
        if 'metadata' in res.columns:
            self.assertFalse(res['metadata'].isnull().all())

        # For primary key tables, verify that _VALUE_KIND is written correctly
        # by checking if we can read the raw data with system fields
        # Note: Normal read filters out system fields, so we verify through Java read
        # which explicitly reads KeyValue objects and checks valueKind
        print(f"Format: {file_format}, Python read completed. ValueKind verification should be done in Java test.")

    def test_pk_dv_read(self):
        pa_schema = pa.schema([
            pa.field('pt', pa.int32(), nullable=False),
            pa.field('a', pa.int32(), nullable=False),
            ('b', pa.int64())
        ])
        schema = Schema.from_pyarrow_schema(pa_schema,
                                            partition_keys=['pt'],
                                            primary_keys=['pt', 'a'],
                                            options={'bucket': '1'})
        self.catalog.create_table('default.test_pk_dv', schema, True)
        table = self.catalog.get_table('default.test_pk_dv')
        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        actual = table_sort_by(table_read.to_arrow(splits), 'pt')
        expected = pa.Table.from_pydict({
            'pt': [1, 2, 2],
            'a': [10, 21, 22],
            'b': [1000, 20001, 202]
        }, schema=pa_schema)
        self.assertEqual(expected, actual)

    def test_pk_dv_read_multi_batch(self):
        pa_schema = pa.schema([
            pa.field('pt', pa.int32(), nullable=False),
            pa.field('a', pa.int32(), nullable=False),
            ('b', pa.int64())
        ])
        schema = Schema.from_pyarrow_schema(pa_schema,
                                            partition_keys=['pt'],
                                            primary_keys=['pt', 'a'],
                                            options={'bucket': '1'})
        self.catalog.create_table('default.test_pk_dv_multi_batch', schema, True)
        table = self.catalog.get_table('default.test_pk_dv_multi_batch')
        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        actual = table_sort_by(table_read.to_arrow(splits), 'pt')
        expected = pa.Table.from_pydict({
            'pt': [1] * 9999,
            'a': [i * 10 for i in range(1, 10001) if i * 10 != 81930],
            'b': [i * 100 for i in range(1, 10001) if i * 10 != 81930]
        }, schema=pa_schema)
        self.assertEqual(expected, actual)

    def test_pk_dv_read_multi_batch_with_value_filter(self):
        """Test that DV-enabled PK table filters files by value stats during scan."""
        pa_schema = pa.schema([
            pa.field('pt', pa.int32(), nullable=False),
            pa.field('a', pa.int32(), nullable=False),
            ('b', pa.int64())
        ])
        schema = Schema.from_pyarrow_schema(pa_schema,
                                            partition_keys=['pt'],
                                            primary_keys=['pt', 'a'],
                                            options={'bucket': '1'})
        self.catalog.create_table('default.test_pk_dv_multi_batch', schema, True)
        table = self.catalog.get_table('default.test_pk_dv_multi_batch')

        # Unfiltered scan: count total files
        rb_all = table.new_read_builder()
        all_splits = rb_all.new_scan().plan().splits()
        all_files_count = sum(len(s.files) for s in all_splits)

        # Filtered scan: b < 500 should only match a few rows (b in {100,200,300,400})
        # and prune files whose value stats don't overlap
        predicate_builder = table.new_read_builder().new_predicate_builder()
        pred = predicate_builder.less_than('b', 500)
        rb_filtered = table.new_read_builder().with_filter(pred)
        filtered_splits = rb_filtered.new_scan().plan().splits()
        filtered_files_count = sum(len(s.files) for s in filtered_splits)
        filtered_result = table_sort_by(
            rb_filtered.new_read().to_arrow(filtered_splits), 'a')

        # Verify correctness: rows with b < 500 are a=10,b=100 / a=20,b=200 / a=30,b=300 / a=40,b=400
        expected = pa.Table.from_pydict({
            'pt': [1, 1, 1, 1],
            'a': [10, 20, 30, 40],
            'b': [100, 200, 300, 400]
        }, schema=pa_schema)
        self.assertEqual(expected, filtered_result)

        # Verify file pruning: filtered scan should read fewer files
        self.assertLess(
            filtered_files_count, all_files_count,
            f"DV value filter should prune files: filtered={filtered_files_count}, all={all_files_count}")

    def test_pk_dv_read_multi_batch_raw_convertable(self):
        pa_schema = pa.schema([
            pa.field('pt', pa.int32(), nullable=False),
            pa.field('a', pa.int32(), nullable=False),
            ('b', pa.int64())
        ])
        schema = Schema.from_pyarrow_schema(pa_schema,
                                            partition_keys=['pt'],
                                            primary_keys=['pt', 'a'],
                                            options={'bucket': '1'})
        self.catalog.create_table('default.test_pk_dv_raw_convertable', schema, True)
        table = self.catalog.get_table('default.test_pk_dv_raw_convertable')
        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        actual = table_sort_by(table_read.to_arrow(splits), 'pt')
        expected = pa.Table.from_pydict({
            'pt': [1] * 9999,
            'a': [i * 10 for i in range(1, 10001) if i * 10 != 81930],
            'b': [i * 100 for i in range(1, 10001) if i * 10 != 81930]
        }, schema=pa_schema)
        self.assertEqual(expected, actual)

    def test_read_btree_index_table(self):
        self._test_read_btree_index_generic("test_btree_index_string", "k2", pa.string())
        self._test_read_btree_index_generic("test_btree_index_int", 200, pa.int32())
        self._test_read_btree_index_generic("test_btree_index_bigint", 2000, pa.int64())
        self._test_read_btree_index_large()
        self._test_read_btree_index_null()
        self._test_index_manifest_inherited_after_write()

    def _test_read_btree_index_generic(self, table_name: str, k, k_type):
        table = self.catalog.get_table('default.' + table_name)
        read_builder: ReadBuilder = table.new_read_builder()

        # read using index
        predicate_builder = read_builder.new_predicate_builder()
        predicate = predicate_builder.equal('k', k)
        read_builder.with_filter(predicate)
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        actual = table_sort_by(table_read.to_arrow(splits), 'k')
        expected = pa.Table.from_pydict({
            'k': [k],
            'v': ["v2"]
        }, schema=pa.schema([
            ("k", k_type),
            ("v", pa.string())
        ]))
        self.assertEqual(expected, actual)

    def _test_read_btree_index_large(self):
        table = self.catalog.get_table('default.test_btree_index_large')
        read_builder: ReadBuilder = table.new_read_builder()
        predicate_builder = read_builder.new_predicate_builder()

        # read equal index
        read_builder.with_filter(predicate_builder.equal('k', 'k2'))
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        actual = table_sort_by(table_read.to_arrow(splits), 'k')
        expected = pa.Table.from_pydict({
            'k': ["k2"],
            'v': ["v2"]
        })
        self.assertEqual(expected, actual)

        # read between index
        read_builder.with_filter(predicate_builder.between('k', 'k990', 'k995'))
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        actual = table_sort_by(table_read.to_arrow(splits), 'k')
        expected = pa.Table.from_pydict({
            'k': ["k990", "k991", "k992", "k993", "k994", "k995"],
            'v': ["v990", "v991", "v992", "v993", "v994", "v995"]
        })
        self.assertEqual(expected, actual)

        # read in index
        read_builder.with_filter(predicate_builder.is_in('k', ['k990', 'k995']))
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        actual = table_sort_by(table_read.to_arrow(splits), 'k')
        expected = pa.Table.from_pydict({
            'k': ["k990", "k995"],
            'v': ["v990", "v995"]
        })
        self.assertEqual(expected, actual)

        # read is_not_null index (full scan across all data blocks)
        read_builder.with_filter(predicate_builder.is_not_null('k'))
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        actual = table_read.to_arrow(splits)
        self.assertEqual(len(actual), 2000)

    def _test_read_btree_index_null(self):
        table = self.catalog.get_table('default.test_btree_index_null')

        # read is null index
        read_builder: ReadBuilder = table.new_read_builder()
        predicate_builder = read_builder.new_predicate_builder()
        read_builder.with_filter(predicate_builder.is_null('k'))
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        actual = table_sort_by(table_read.to_arrow(splits), 'k')
        expected = pa.Table.from_pydict({
            'k': [None, None],
            'v': ["v3", "v5"]
        }, schema=pa.schema([
            ("k", pa.string()),
            ("v", pa.string())
        ]))
        self.assertEqual(expected, actual)

        # read is not null index
        read_builder: ReadBuilder = table.new_read_builder()
        predicate_builder = read_builder.new_predicate_builder()
        read_builder.with_filter(predicate_builder.is_not_null('k'))
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        actual = table_sort_by(table_read.to_arrow(splits), 'k')
        expected = pa.Table.from_pydict({
            'k': ["k1", "k2", "k4"],
            'v': ["v1", "v2", "v4"]
        })
        self.assertEqual(expected, actual)

    def _test_index_manifest_inherited_after_write(self):
        table = self.catalog.get_table('default.test_btree_index_string')

        snapshot_before = table.snapshot_manager().get_latest_snapshot()
        self.assertIsNotNone(snapshot_before.index_manifest,
                             "Index manifest should exist before Python write")

        write_builder = table.new_batch_write_builder()
        write = write_builder.new_write()
        commit = write_builder.new_commit()
        data = pa.table({'k': ['k4'], 'v': ['v4']})
        write.write_arrow(data)
        commit.commit(write.prepare_commit())
        write.close()
        commit.close()

        snapshot_after = table.snapshot_manager().get_latest_snapshot()
        self.assertGreater(snapshot_after.id, snapshot_before.id)
        self.assertIsNotNone(
            snapshot_after.index_manifest,
            "index_manifest lost after Python data write - indexes become invisible"
        )

    @parameterized.expand([('json',), ('csv',)])
    def test_read_compressed_text_append_table(self, file_format):
        table = self.catalog.get_table(
            f'default.mixed_test_append_tablej_{file_format}_gz')
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        splits = table_scan.plan().splits()
        with self.assertRaises(NotImplementedError) as ctx:
            table_read.to_arrow(splits)
        self.assertIn(file_format, str(ctx.exception))
        self.assertIn("not yet supported", str(ctx.exception))

    def test_read_tantivy_full_text_index(self):
        """Test reading a Tantivy full-text index built by Java."""
        table = self.catalog.get_table('default.test_tantivy_fulltext')

        # Use FullTextSearchBuilder to search
        builder = table.new_full_text_search_builder()
        builder.with_text_column('content')
        builder.with_query_text('paimon')
        builder.with_limit(10)

        result = builder.execute_local()
        # Row 0, 2, 4 mention "paimon"
        row_ids = sorted(list(result.results()))
        print(f"Tantivy full-text search for 'paimon': row_ids={row_ids}")
        self.assertEqual(row_ids, [0, 2, 4])

        # Read matching rows using withGlobalIndexResult
        read_builder = table.new_read_builder()
        scan = read_builder.new_scan().with_global_index_result(result)
        plan = scan.plan()
        table_read = read_builder.new_read()
        pa_table = table_read.to_arrow(plan.splits())
        pa_table = table_sort_by(pa_table, 'id')
        self.assertEqual(pa_table.num_rows, 3)
        ids = pa_table.column('id').to_pylist()
        self.assertEqual(ids, [0, 2, 4])

        # Search for "tantivy" - only row 1
        builder2 = table.new_full_text_search_builder()
        builder2.with_text_column('content')
        builder2.with_query_text('tantivy')
        builder2.with_limit(10)

        result2 = builder2.execute_local()
        row_ids2 = sorted(list(result2.results()))
        print(f"Tantivy full-text search for 'tantivy': row_ids={row_ids2}")
        self.assertEqual(row_ids2, [1])

        # Read matching rows
        read_builder2 = table.new_read_builder()
        scan2 = read_builder2.new_scan().with_global_index_result(result2)
        plan2 = scan2.plan()
        pa_table2 = read_builder2.new_read().to_arrow(plan2.splits())
        self.assertEqual(pa_table2.num_rows, 1)
        self.assertEqual(pa_table2.column('id').to_pylist(), [1])

        # Search for "full-text search" - rows 1, 3
        builder3 = table.new_full_text_search_builder()
        builder3.with_text_column('content')
        builder3.with_query_text('full-text search')
        builder3.with_limit(10)

        result3 = builder3.execute_local()
        row_ids3 = sorted(list(result3.results()))
        print(f"Tantivy full-text search for 'full-text search': row_ids={row_ids3}")
        self.assertIn(1, row_ids3)
        self.assertIn(3, row_ids3)

        # Read matching rows
        read_builder3 = table.new_read_builder()
        scan3 = read_builder3.new_scan().with_global_index_result(result3)
        plan3 = scan3.plan()
        pa_table3 = read_builder3.new_read().to_arrow(plan3.splits())
        pa_table3 = table_sort_by(pa_table3, 'id')
        ids3 = pa_table3.column('id').to_pylist()
        self.assertIn(1, ids3)
        self.assertIn(3, ids3)

    def test_read_lumina_vector_index(self):
        """Test reading a Lumina vector index built by Java."""
        table = self.catalog.get_table('default.test_lumina_vector')

        # Use VectorSearchBuilder to search
        # Java wrote 6 vectors: [1,0,0,0], [0.9,0.1,0,0], [0,1,0,0],
        #                        [0,0,1,0], [0,0,0,1], [0.95,0.05,0,0]
        # Query with [1,0,0,0] - nearest by L2 should be row 0, 5, 1
        builder = table.new_vector_search_builder()
        builder.with_vector_column('embedding')
        builder.with_query_vector([1.0, 0.0, 0.0, 0.0])
        builder.with_limit(3)

        result = builder.execute_local()
        row_ids = sorted(list(result.results()))
        print(f"Lumina vector search for [1,0,0,0]: row_ids={row_ids}")
        self.assertIn(0, row_ids)  # exact match
        self.assertEqual(len(row_ids), 3)

        # Read matching rows using withGlobalIndexResult
        read_builder = table.new_read_builder()
        scan = read_builder.new_scan().with_global_index_result(result)
        plan = scan.plan()
        table_read = read_builder.new_read()
        pa_table = table_read.to_arrow(plan.splits())
        pa_table = table_sort_by(pa_table, 'id')
        self.assertEqual(pa_table.num_rows, 3)
        ids = pa_table.column('id').to_pylist()
        print(f"Lumina vector search matched rows: ids={ids}")
        self.assertIn(0, ids)

    def test_read_lumina_vector_with_btree_filter(self):
        """Vector search + btree scalar pre-filter, using a table that Java
        populated with both a Lumina vector index on `embedding` and a BTree
        global index on `id` (JavaPyLuminaE2ETest.testLuminaVectorWithBTreeIndexWrite)."""
        from pypaimon.common.predicate_builder import PredicateBuilder

        table = self.catalog.get_table('default.test_lumina_vector_btree_filter')

        # Baseline search — same 6 vectors as test_read_lumina_vector_index,
        # no filter, top 6 covers the whole table.
        baseline = (table.new_vector_search_builder()
                    .with_vector_column('embedding')
                    .with_query_vector([1.0, 0.0, 0.0, 0.0])
                    .with_limit(6)
                    .execute_local())
        baseline_ids = sorted(list(baseline.results()))
        print(f"Baseline vector search ids={baseline_ids}")
        self.assertEqual(baseline_ids, [0, 1, 2, 3, 4, 5])

        # Filtered search — id >= 3 should restrict vector search to rows
        # {3,4,5}, so top_k results must be a subset of that.
        pb = PredicateBuilder(table.fields)
        filter_pred = pb.greater_or_equal('id', 3)
        filtered = (table.new_vector_search_builder()
                    .with_vector_column('embedding')
                    .with_query_vector([1.0, 0.0, 0.0, 0.0])
                    .with_limit(6)
                    .with_filter(filter_pred)
                    .execute_local())
        filtered_ids = sorted(list(filtered.results()))
        print(f"Filtered (id >= 3) vector search ids={filtered_ids}")
        self.assertEqual(filtered_ids, [3, 4, 5])

        # Narrower filter — id in {5} — only row 5 should survive.
        filter_eq = pb.equal('id', 5)
        eq_result = (table.new_vector_search_builder()
                     .with_vector_column('embedding')
                     .with_query_vector([1.0, 0.0, 0.0, 0.0])
                     .with_limit(6)
                     .with_filter(filter_eq)
                     .execute_local())
        eq_ids = sorted(list(eq_result.results()))
        print(f"Filtered (id == 5) vector search ids={eq_ids}")
        self.assertEqual(eq_ids, [5])

    def test_read_blob_after_alter_and_compact(self):
        table = self.catalog.get_table('default.blob_alter_compact_test')
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        splits = table_scan.plan().splits()
        result = table_read.to_arrow(splits)
        self.assertEqual(result.num_rows, 200)

    def test_compact_conflict_shard_update(self):
        """
        1. Java writes 5 base files (testCompactConflictWriteBase)
        2. pypaimon ShardTableUpdator scans table, prepares evolution
        3. Java runs compact (testCompactConflictRunCompact)
        4. pypaimon commits stale evolution -> conflict detected, raises RuntimeError
        """
        import subprocess

        table = self.catalog.get_table('default.compact_conflict_test')

        # Step 2: pypaimon shard update - scan and prepare commit
        wb = table.new_batch_write_builder()
        update = wb.new_update()
        update.with_read_projection(['f0'])
        update.with_update_type(['f2'])
        upd = update.new_shard_updator(shard_num=0, total_shard_count=3)
        print(f"Shard 0 row_ranges: {[(r[1].from_, r[1].to) for r in upd.row_ranges]}")

        reader = upd.arrow_reader()
        import pyarrow as pa
        rows_read = 0
        for batch in iter(reader.read_next_batch, None):
            n = batch.num_rows
            rows_read += n
            upd.update_by_arrow_batch(
                pa.RecordBatch.from_pydict(
                    {'f2': [f'evo_{i}' for i in range(n)]},
                    schema=pa.schema([('f2', pa.string())])
                )
            )
        print(f"Shard update read {rows_read} rows")
        stale_commit_msgs = upd.prepare_commit()

        # Step 3: Java compact (compact happening between scan and commit)
        project_root = os.path.join(self.tempdir, '..', '..', '..', '..')
        result = subprocess.run(
            ['mvn', 'test',
             '-pl', 'paimon-core',
             '-Dtest=org.apache.paimon.JavaPyE2ETest#testCompactConflictRunCompact',
             '-Drun.e2e.tests=true',
             '-Dsurefire.failIfNoSpecifiedTests=false',
             '-q'],
            cwd=os.path.abspath(project_root),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=True, timeout=120
        )
        self.assertEqual(result.returncode, 0,
                         f"Java compact failed:\n{result.stdout}\n{result.stderr}")
        print("Java compact completed")

        # Step 4: pypaimon commits stale evolution -> conflict detected
        tc = wb.new_commit()
        with self.assertRaises(RuntimeError) as ctx:
            tc.commit(stale_commit_msgs)
        self.assertIn("conflicts", str(ctx.exception))
        tc.close()
        print(f"Conflict detected as expected: {ctx.exception}")

    @parameterized.expand(get_file_format_params())
    def test_read_data_evolution_table(self, file_format):
        """Read data evolution tables written by Java and verify merged results."""
        table = self.catalog.get_table(f'default.data_evolution_test_{file_format}')
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        splits = table_scan.plan().splits()
        result = table_read.to_arrow(splits)
        result = table_sort_by(result, 'f0')
        self.assertEqual(result.num_rows, 5)
        for i in range(5):
            self.assertEqual(result.column('f0')[i].as_py(), i)
            self.assertEqual(result.column('f1')[i].as_py(), f'a{i}')
            self.assertEqual(result.column('f2')[i].as_py(), f'b{i}')

    @parameterized.expand(get_file_format_params())
    def test_py_write_data_evolution_table(self, file_format):
        """Python writes data evolution tables for Java to read."""
        table_name = f'default.data_evolution_test_py_{file_format}'
        simple_pa_schema = pa.schema([
            ('f0', pa.int32()),
            ('f1', pa.utf8()),
            ('f2', pa.utf8()),
        ])
        schema = Schema.from_pyarrow_schema(simple_pa_schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'file.format': file_format,
        })
        self.catalog.create_table(table_name, schema, True)
        table = self.catalog.get_table(table_name)

        # Write (f0, f1) columns
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write().with_write_type(['f0', 'f1'])
        table_commit = write_builder.new_commit()
        data0 = pa.Table.from_pydict({
            'f0': list(range(5)),
            'f1': [f'a{i}' for i in range(5)],
        }, schema=pa.schema([('f0', pa.int32()), ('f1', pa.utf8())]))
        table_write.write_arrow(data0)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # Write (f2) column with first_row_id
        table_write = write_builder.new_write().with_write_type(['f2'])
        table_commit = write_builder.new_commit()
        data1 = pa.Table.from_pydict({
            'f2': [f'b{i}' for i in range(5)],
        }, schema=pa.schema([('f2', pa.utf8())]))
        table_write.write_arrow(data1)
        cmts = table_write.prepare_commit()
        cmts[0].new_files[0].first_row_id = 0
        table_commit.commit(cmts)
        table_write.close()
        table_commit.close()

        # Verify read-back
        read_builder = table.new_read_builder()
        result = read_builder.new_read().to_arrow(read_builder.new_scan().plan().splits())
        result = table_sort_by(result, 'f0')
        self.assertEqual(result.num_rows, 5)
        for i in range(5):
            self.assertEqual(result.column('f0')[i].as_py(), i)
            self.assertEqual(result.column('f1')[i].as_py(), f'a{i}')
            self.assertEqual(result.column('f2')[i].as_py(), f'b{i}')
