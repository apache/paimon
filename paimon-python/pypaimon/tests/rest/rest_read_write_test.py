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

import pandas as pd
import pyarrow as pa
import unittest

from pypaimon.common.options import Options
from pypaimon.catalog.catalog_context import CatalogContext
from pypaimon import CatalogFactory
from pypaimon.catalog.rest.rest_catalog import RESTCatalog
from pypaimon.common.identifier import Identifier
from pypaimon import Schema
from pypaimon.tests.rest.rest_base_test import RESTBaseTest

import ray


class RESTTableReadWriteTest(RESTBaseTest):

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

    def test_parquet_ao_reader(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.rest_catalog.create_table('default.test_append_only_parquet', schema, False)
        table = self.rest_catalog.get_table('default.test_append_only_parquet')
        self._write_test_table(table)

        read_builder = table.new_read_builder()
        actual = self._read_test_table(read_builder).sort_by('user_id')
        self.assertEqual(actual, self.expected)

    def test_orc_ao_reader(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'], options={'file.format': 'orc'})
        self.rest_catalog.create_table('default.test_append_only_orc', schema, False)
        table = self.rest_catalog.get_table('default.test_append_only_orc')
        self._write_test_table(table)

        read_builder = table.new_read_builder()
        actual = self._read_test_table(read_builder).sort_by('user_id')
        self.assertEqual(actual, self.expected)

    def test_avro_ao_reader(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'], options={'file.format': 'avro'})
        self.rest_catalog.create_table('default.test_append_only_avro', schema, False)
        table = self.rest_catalog.get_table('default.test_append_only_avro')
        self._write_test_table(table)

        read_builder = table.new_read_builder()
        actual = self._read_test_table(read_builder).sort_by('user_id')
        self.assertEqual(actual, self.expected)

    def test_lance_ao_reader(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'], options={'file.format': 'lance'})
        self.rest_catalog.create_table('default.test_append_only_lance', schema, False)
        table = self.rest_catalog.get_table('default.test_append_only_lance')
        self._write_test_table(table)

        read_builder = table.new_read_builder()
        actual = self._read_test_table(read_builder).sort_by('user_id')
        self.assertEqual(actual, self.expected)

    def test_ao_reader_with_filter(self):
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
        self.rest_catalog.create_table('default.test_append_only_projection', schema, False)
        table = self.rest_catalog.get_table('default.test_append_only_projection')
        self._write_test_table(table)

        read_builder = table.new_read_builder().with_projection(['dt', 'user_id'])
        actual = self._read_test_table(read_builder).sort_by('user_id')
        expected = self.expected.select(['dt', 'user_id'])
        self.assertEqual(actual, expected)

    def test_avro_ao_reader_with_projection(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'], options={'file.format': 'avro'})
        self.rest_catalog.create_table('default.test_avro_append_only_projection', schema, False)
        table = self.rest_catalog.get_table('default.test_avro_append_only_projection')
        self._write_test_table(table)

        read_builder = table.new_read_builder().with_projection(['dt', 'user_id'])
        actual = self._read_test_table(read_builder).sort_by('user_id')
        expected = self.expected.select(['dt', 'user_id'])
        self.assertEqual(actual, expected)

    def test_ao_reader_with_limit(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.rest_catalog.create_table('default.test_append_only_limit', schema, False)
        table = self.rest_catalog.get_table('default.test_append_only_limit')
        self._write_test_table(table)

        read_builder = table.new_read_builder().with_limit(1)
        actual = self._read_test_table(read_builder)
        # only records from 1st commit (1st split) will be read
        # might be split of "dt=1" or split of "dt=2"
        self.assertEqual(actual.num_rows, 4)

    def test_pk_parquet_reader(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema,
                                            partition_keys=['dt'],
                                            primary_keys=['user_id', 'dt'],
                                            options={'bucket': '2'})
        self.rest_catalog.create_table('default.test_pk_parquet', schema, False)
        table = self.rest_catalog.get_table('default.test_pk_parquet')
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
        self.rest_catalog.create_table('default.test_pk_orc', schema, False)
        table = self.rest_catalog.get_table('default.test_pk_orc')
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
        self.rest_catalog.create_table('default.test_pk_avro', schema, False)
        table = self.rest_catalog.get_table('default.test_pk_avro')
        self._write_test_table(table)

        read_builder = table.new_read_builder()
        actual = self._read_test_table(read_builder).sort_by('user_id')
        self.assertEqual(actual, self.expected)

    def test_pk_lance_reader(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema,
                                            partition_keys=['dt'],
                                            primary_keys=['user_id', 'dt'],
                                            options={
                                                'bucket': '2',
                                                'file.format': 'lance'
                                            })
        self.rest_catalog.drop_table('default.test_pk_lance', True)
        self.rest_catalog.create_table('default.test_pk_lance', schema, False)
        table = self.rest_catalog.get_table('default.test_pk_lance')
        # Use table's schema for writing to ensure schema consistency
        from pypaimon.schema.data_types import PyarrowFieldParser
        table_pa_schema = PyarrowFieldParser.from_paimon_schema(table.table_schema.fields)
        self._write_test_table_with_schema(table, table_pa_schema)

        read_builder = table.new_read_builder()
        actual = self._read_test_table(read_builder).sort_by('user_id')
        self.assertEqual(actual, self.expected)

    def test_lance_ao_reader_with_filter(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'], options={'file.format': 'lance'})
        self.rest_catalog.create_table('default.test_append_only_lance_filter', schema, False)
        table = self.rest_catalog.get_table('default.test_append_only_lance_filter')
        self._write_test_table(table)

        predicate_builder = table.new_read_builder().new_predicate_builder()
        p1 = predicate_builder.less_than('user_id', 7)
        p2 = predicate_builder.greater_or_equal('user_id', 2)
        p3 = predicate_builder.between('user_id', 0, 6)
        p4 = predicate_builder.is_not_in('behavior', ['b', 'e'])
        p5 = predicate_builder.is_in('dt', ['p1'])
        p6 = predicate_builder.is_not_null('behavior')
        g1 = predicate_builder.and_predicates([p1, p2, p3, p4, p5, p6])
        read_builder = table.new_read_builder().with_filter(g1)
        actual = self._read_test_table(read_builder)
        expected = pa.concat_tables([
            self.expected.slice(5, 1)  # 6/f
        ])
        self.assertEqual(actual.sort_by('user_id'), expected)

    def test_pk_lance_reader_with_filter(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema,
                                            partition_keys=['dt'],
                                            primary_keys=['user_id', 'dt'],
                                            options={
                                                'bucket': '2',
                                                'file.format': 'lance'
                                            })
        self.rest_catalog.create_table('default.test_pk_lance_filter', schema, False)
        table = self.rest_catalog.get_table('default.test_pk_lance_filter')
        from pypaimon.schema.data_types import PyarrowFieldParser
        table_pa_schema = PyarrowFieldParser.from_paimon_schema(table.table_schema.fields)
        self._write_test_table_with_schema(table, table_pa_schema)

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

    @unittest.skip("does not support dynamic bucket in dummy rest server")
    def test_pk_lance_reader_no_bucket(self):
        """Test Lance format with PrimaryKey table without specifying bucket."""
        schema = Schema.from_pyarrow_schema(self.pa_schema,
                                            partition_keys=['dt'],
                                            primary_keys=['user_id', 'dt'],
                                            options={'file.format': 'lance'})
        self.rest_catalog.drop_table('default.test_pk_lance_no_bucket', True)
        self.rest_catalog.create_table('default.test_pk_lance_no_bucket', schema, False)
        table = self.rest_catalog.get_table('default.test_pk_lance_no_bucket')
        from pypaimon.schema.data_types import PyarrowFieldParser
        table_pa_schema = PyarrowFieldParser.from_paimon_schema(table.table_schema.fields)
        self._write_test_table_with_schema(table, table_pa_schema)

        read_builder = table.new_read_builder()
        actual = self._read_test_table(read_builder).sort_by('user_id')
        self.assertEqual(actual, self.expected)

    def test_pk_reader_with_filter(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema,
                                            partition_keys=['dt'],
                                            primary_keys=['user_id', 'dt'],
                                            options={'bucket': '2'})
        self.rest_catalog.create_table('default.test_pk_filter', schema, False)
        table = self.rest_catalog.get_table('default.test_pk_filter')
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
        self.rest_catalog.create_table('default.test_pk_projection', schema, False)
        table = self.rest_catalog.get_table('default.test_pk_projection')
        self._write_test_table(table)

        read_builder = table.new_read_builder().with_projection(['dt', 'user_id', 'behavior'])
        actual = self._read_test_table(read_builder).sort_by('user_id')
        expected = self.expected.select(['dt', 'user_id', 'behavior'])
        self.assertEqual(actual, expected)

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
        self.assertEqual(result, [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008])

    def test_reader_duckdb(self):
        read_builder = self.table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        duckdb_con = table_read.to_duckdb(splits, 'duckdb_table')
        actual = duckdb_con.query("SELECT * FROM duckdb_table").fetchdf()
        expect = pd.DataFrame(self.raw_data)
        pd.testing.assert_frame_equal(actual.reset_index(drop=True), expect.reset_index(drop=True))

    def test_reader_ray_data(self):
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True, num_cpus=2)

        read_builder = self.table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()

        ray_dataset = table_read.to_ray(splits, parallelism=2)

        self.assertIsNotNone(ray_dataset, "Ray dataset should not be None")
        self.assertEqual(ray_dataset.count(), 8, "Should have 8 rows")

        df = ray_dataset.to_pandas()
        expect = pd.DataFrame(self.raw_data)
        pd.testing.assert_frame_equal(df.sort_values(by='user_id').reset_index(drop=True),
                                      expect.sort_values(by='user_id').reset_index(drop=True))

    def test_ray_data_write_and_read(self):
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True, num_cpus=2)

        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('value', pa.int64()),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema)
        self.rest_catalog.create_table('default.test_ray_write_read', schema, False)
        table = self.rest_catalog.get_table('default.test_ray_write_read')

        test_data = pa.Table.from_pydict({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'value': [100, 200, 300],
        }, schema=pa_schema)

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(test_data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()

        ray_dataset = table_read.to_ray(splits, parallelism=2)

        self.assertIsNotNone(ray_dataset, "Ray dataset should not be None")
        self.assertEqual(ray_dataset.count(), 3, "Should have 3 rows")

        df = ray_dataset.to_pandas()
        expected_df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'value': [100, 200, 300],
        })
        expected_df['id'] = expected_df['id'].astype('int32')
        pd.testing.assert_frame_equal(df.sort_values(by='id').reset_index(drop=True),
                                      expected_df.sort_values(by='id').reset_index(drop=True))

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
