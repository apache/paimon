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
#  limitations under the License.
################################################################################

import os
import tempfile
import unittest
import shutil

import pyarrow as pa
import ray

from pypaimon import CatalogFactory, Schema
from pypaimon.common.options.core_options import CoreOptions


class RayDataTest(unittest.TestCase):
    """Tests for Ray Data integration with PyPaimon."""

    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({
            'warehouse': cls.warehouse
        })
        cls.catalog.create_database('default', True)

        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True, num_cpus=2)

        try:
            from ray.data import DataContext
            context = DataContext.get_current()
            if hasattr(context, 'shuffle_strategy'):
                try:
                    from ray.data._internal.execution.interfaces.execution_options import ShuffleStrategy
                    context.shuffle_strategy = ShuffleStrategy.SORT_SHUFFLE_PUSH_BASED
                except (ImportError, AttributeError):
                    pass
            if hasattr(context, 'use_polars_sort'):
                context.use_polars_sort = True
        except (ImportError, AttributeError):
            pass

    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        try:
            if ray.is_initialized():
                ray.shutdown()
        except Exception:
            pass
        try:
            shutil.rmtree(cls.tempdir)
        except OSError:
            pass

    def setUp(self):
        """Set up test method."""
        pass

    def test_basic_ray_data_read(self):
        """Test basic Ray Data read from PyPaimon table."""
        # Create schema
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('value', pa.int64()),
        ])

        schema = Schema.from_pyarrow_schema(pa_schema)
        self.catalog.create_table('default.test_ray_basic', schema, False)
        table = self.catalog.get_table('default.test_ray_basic')

        # Write test data
        test_data = pa.Table.from_pydict({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'value': [100, 200, 300, 400, 500],
        }, schema=pa_schema)

        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(test_data)
        commit_messages = writer.prepare_commit()
        commit = write_builder.new_commit()
        commit.commit(commit_messages)
        writer.close()

        # Read using Ray Data
        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        table_scan = read_builder.new_scan()
        splits = table_scan.plan().splits()

        ray_dataset = table_read.to_ray(splits, parallelism=2)

        # Verify Ray dataset
        self.assertIsNotNone(ray_dataset, "Ray dataset should not be None")
        self.assertEqual(ray_dataset.count(), 5, "Should have 5 rows")

        # Test basic operations
        sample_data = ray_dataset.take(3)
        self.assertEqual(len(sample_data), 3, "Should have 3 sample rows")

        # Convert to pandas for verification
        df = ray_dataset.to_pandas()
        self.assertEqual(len(df), 5, "DataFrame should have 5 rows")
        # Sort by id to ensure order-independent comparison
        df_sorted = df.sort_values(by='id').reset_index(drop=True)
        self.assertEqual(list(df_sorted['id']), [1, 2, 3, 4, 5], "ID column should match")
        self.assertEqual(
            list(df_sorted['name']),
            ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            "Name column should match"
        )

    def test_ray_data_with_predicate(self):
        """Test Ray Data read with predicate filtering."""
        # Create schema
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('category', pa.string()),
            ('amount', pa.int64()),
        ])

        schema = Schema.from_pyarrow_schema(pa_schema)
        self.catalog.create_table('default.test_ray_predicate', schema, False)
        table = self.catalog.get_table('default.test_ray_predicate')

        # Write test data
        test_data = pa.Table.from_pydict({
            'id': [1, 2, 3, 4, 5],
            'category': ['A', 'B', 'A', 'C', 'B'],
            'amount': [100, 200, 150, 300, 250],
        }, schema=pa_schema)

        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(test_data)
        commit_messages = writer.prepare_commit()
        commit = write_builder.new_commit()
        commit.commit(commit_messages)
        writer.close()

        # Read with predicate
        read_builder = table.new_read_builder()
        predicate_builder = read_builder.new_predicate_builder()
        predicate = predicate_builder.equal('category', 'A')
        read_builder = read_builder.with_filter(predicate)

        table_read = read_builder.new_read()
        table_scan = read_builder.new_scan()
        splits = table_scan.plan().splits()

        ray_dataset = table_read.to_ray(splits, parallelism=2)

        # Verify filtered results
        self.assertEqual(ray_dataset.count(), 2, "Should have 2 rows after filtering")
        df = ray_dataset.to_pandas()
        self.assertEqual(set(df['category'].tolist()), {'A'}, "All rows should have category='A'")
        self.assertEqual(set(df['id'].tolist()), {1, 3}, "Should have IDs 1 and 3")

    def test_ray_data_with_projection(self):
        """Test Ray Data read with column projection."""
        # Create schema
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('email', pa.string()),
            ('age', pa.int32()),
        ])

        schema = Schema.from_pyarrow_schema(pa_schema)
        self.catalog.create_table('default.test_ray_projection', schema, False)
        table = self.catalog.get_table('default.test_ray_projection')

        # Write test data
        test_data = pa.Table.from_pydict({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com'],
            'age': [25, 30, 35],
        }, schema=pa_schema)

        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(test_data)
        commit_messages = writer.prepare_commit()
        commit = write_builder.new_commit()
        commit.commit(commit_messages)
        writer.close()

        # Read with projection
        read_builder = table.new_read_builder()
        read_builder = read_builder.with_projection(['id', 'name'])

        table_read = read_builder.new_read()
        table_scan = read_builder.new_scan()
        splits = table_scan.plan().splits()

        ray_dataset = table_read.to_ray(splits, parallelism=2)

        # Verify projection
        self.assertEqual(ray_dataset.count(), 3, "Should have 3 rows")
        df = ray_dataset.to_pandas()
        self.assertEqual(set(df.columns), {'id', 'name'}, "Should only have id and name columns")
        self.assertFalse('email' in df.columns, "Should not have email column")
        self.assertFalse('age' in df.columns, "Should not have age column")

    def test_ray_data_map_operation(self):
        """Test Ray Data map operations after reading from PyPaimon."""
        # Create schema
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('value', pa.int64()),
        ])

        schema = Schema.from_pyarrow_schema(pa_schema)
        self.catalog.create_table('default.test_ray_map', schema, False)
        table = self.catalog.get_table('default.test_ray_map')

        # Write test data
        test_data = pa.Table.from_pydict({
            'id': [1, 2, 3, 4, 5],
            'value': [10, 20, 30, 40, 50],
        }, schema=pa_schema)

        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(test_data)
        commit_messages = writer.prepare_commit()
        commit = write_builder.new_commit()
        commit.commit(commit_messages)
        writer.close()

        # Read using Ray Data
        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        table_scan = read_builder.new_scan()
        splits = table_scan.plan().splits()

        ray_dataset = table_read.to_ray(splits, parallelism=2)

        # Apply map operation (double the value)
        def double_value(row):
            row['value'] = row['value'] * 2
            return row

        mapped_dataset = ray_dataset.map(double_value)

        # Verify mapped results
        df = mapped_dataset.to_pandas()
        # Sort by id to ensure order-independent comparison
        df_sorted = df.sort_values(by='id').reset_index(drop=True)
        self.assertEqual(list(df_sorted['value']), [20, 40, 60, 80, 100], "Values should be doubled")

    def test_ray_data_filter_operation(self):
        """Test Ray Data filter operations after reading from PyPaimon."""
        # Create schema
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('score', pa.int64()),
        ])

        schema = Schema.from_pyarrow_schema(pa_schema)
        self.catalog.create_table('default.test_ray_filter', schema, False)
        table = self.catalog.get_table('default.test_ray_filter')

        # Write test data
        test_data = pa.Table.from_pydict({
            'id': [1, 2, 3, 4, 5],
            'score': [60, 70, 80, 90, 100],
        }, schema=pa_schema)

        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(test_data)
        commit_messages = writer.prepare_commit()
        commit = write_builder.new_commit()
        commit.commit(commit_messages)
        writer.close()

        # Read using Ray Data
        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        table_scan = read_builder.new_scan()
        splits = table_scan.plan().splits()

        ray_dataset = table_read.to_ray(splits, parallelism=2)

        # Apply filter operation (score >= 80)
        filtered_dataset = ray_dataset.filter(lambda row: row['score'] >= 80)

        # Verify filtered results
        df = filtered_dataset.to_pandas()
        self.assertEqual(len(df), 3, "Should have 3 rows with score >= 80")
        self.assertEqual(set(df['id'].tolist()), {3, 4, 5}, "Should have IDs 3, 4, 5")
        self.assertEqual(set(df['score'].tolist()), {80, 90, 100}, "Should have scores 80, 90, 100")

    def test_ray_data_distributed_vs_simple(self):
        """Test that both distributed and simple reading modes work correctly."""
        # Create schema
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('value', pa.int64()),
        ])

        schema = Schema.from_pyarrow_schema(pa_schema)
        self.catalog.create_table('default.test_ray_modes', schema, False)
        table = self.catalog.get_table('default.test_ray_modes')

        # Write test data
        test_data = pa.Table.from_pydict({
            'id': [1, 2, 3],
            'value': [10, 20, 30],
        }, schema=pa_schema)

        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(test_data)
        commit_messages = writer.prepare_commit()
        commit = write_builder.new_commit()
        commit.commit(commit_messages)
        writer.close()

        # Read using distributed mode
        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        table_scan = read_builder.new_scan()
        splits = table_scan.plan().splits()

        ray_dataset_distributed = table_read.to_ray(splits, parallelism=2)
        ray_dataset_simple = table_read.to_ray(splits, parallelism=1)

        # Both should produce the same results
        self.assertEqual(ray_dataset_distributed.count(), 3, "Distributed mode should have 3 rows")
        self.assertEqual(ray_dataset_simple.count(), 3, "Simple mode should have 3 rows")

        df_distributed = ray_dataset_distributed.to_pandas()
        df_simple = ray_dataset_simple.to_pandas()

        # Sort both dataframes by id to ensure order-independent comparison
        df_distributed_sorted = df_distributed.sort_values(by='id').reset_index(drop=True)
        df_simple_sorted = df_simple.sort_values(by='id').reset_index(drop=True)

        self.assertEqual(list(df_distributed_sorted['id']), list(df_simple_sorted['id']), "IDs should match")
        self.assertEqual(list(df_distributed_sorted['value']), list(df_simple_sorted['value']), "Values should match")

    def test_ray_data_primary_key_basic(self):
        """Test Ray Data read from PrimaryKey table."""
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('value', pa.int64()),
        ])

        schema = Schema.from_pyarrow_schema(pa_schema, primary_keys=['id'], options={'bucket': '2'})
        self.catalog.create_table('default.test_ray_pk_basic', schema, False)
        table = self.catalog.get_table('default.test_ray_pk_basic')

        test_data = pa.Table.from_pydict({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'value': [100, 200, 300, 400, 500],
        }, schema=pa_schema)

        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(test_data)
        commit_messages = writer.prepare_commit()
        commit = write_builder.new_commit()
        commit.commit(commit_messages)
        writer.close()

        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        table_scan = read_builder.new_scan()
        splits = table_scan.plan().splits()

        ray_dataset = table_read.to_ray(splits, parallelism=1)
        self.assertIsNotNone(ray_dataset, "Ray dataset should not be None")
        self.assertEqual(ray_dataset.count(), 5, "Should have 5 rows")

        df = ray_dataset.to_pandas()
        self.assertEqual(len(df), 5, "DataFrame should have 5 rows")
        df_sorted = df.sort_values(by='id').reset_index(drop=True)
        self.assertEqual(list(df_sorted['id']), [1, 2, 3, 4, 5], "ID column should match")
        self.assertEqual(
            list(df_sorted['name']),
            ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            "Name column should match"
        )

    def test_ray_data_primary_key_update(self):
        """Test Ray Data read from PrimaryKey table with updates (upsert behavior)."""
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('value', pa.int64()),
        ])

        schema = Schema.from_pyarrow_schema(pa_schema, primary_keys=['id'], options={'bucket': '2'})
        self.catalog.create_table('default.test_ray_pk_update', schema, False)
        table = self.catalog.get_table('default.test_ray_pk_update')

        initial_data = pa.Table.from_pydict({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'value': [100, 200, 300],
        }, schema=pa_schema)

        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(initial_data)
        commit_messages = writer.prepare_commit()
        commit = write_builder.new_commit()
        commit.commit(commit_messages)
        writer.close()

        updated_data = pa.Table.from_pydict({
            'id': [1, 2, 4],  # id=1,2 updated, id=4 new
            'name': ['Alice-Updated', 'Bob-Updated', 'David'],
            'value': [150, 250, 400],
        }, schema=pa_schema)

        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(updated_data)
        commit_messages = writer.prepare_commit()
        commit = write_builder.new_commit()
        commit.commit(commit_messages)
        writer.close()

        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        table_scan = read_builder.new_scan()
        splits = table_scan.plan().splits()

        ray_dataset = table_read.to_ray(splits, parallelism=2)

        self.assertIsNotNone(ray_dataset, "Ray dataset should not be None")
        self.assertEqual(ray_dataset.count(), 4, "Should have 4 rows after upsert")

        df = ray_dataset.to_pandas()
        df_sorted = df.sort_values(by='id').reset_index(drop=True)
        self.assertEqual(list(df_sorted['id']), [1, 2, 3, 4], "ID column should match")
        self.assertEqual(
            list(df_sorted['name']),
            ['Alice-Updated', 'Bob-Updated', 'Charlie', 'David'],
            "Name column should reflect updates"
        )
        self.assertEqual(list(df_sorted['value']), [150, 250, 300, 400], "Value column should reflect updates")

    def test_ray_data_primary_key_with_predicate(self):
        """Test Ray Data read from PrimaryKey table with predicate filtering."""
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('category', pa.string()),
            ('amount', pa.int64()),
            ('dt', pa.string()),
        ])

        schema = Schema.from_pyarrow_schema(
            pa_schema,
            primary_keys=['id', 'dt'],
            partition_keys=['dt'],
            options={'bucket': '2'}
        )
        self.catalog.create_table('default.test_ray_pk_predicate', schema, False)
        table = self.catalog.get_table('default.test_ray_pk_predicate')

        test_data = pa.Table.from_pydict({
            'id': [1, 2, 3, 4, 5],
            'category': ['A', 'B', 'A', 'C', 'B'],
            'amount': [100, 200, 150, 300, 250],
            'dt': ['2024-01-01', '2024-01-01', '2024-01-02', '2024-01-02', '2024-01-03'],
        }, schema=pa_schema)

        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(test_data)
        commit_messages = writer.prepare_commit()
        commit = write_builder.new_commit()
        commit.commit(commit_messages)
        writer.close()

        read_builder = table.new_read_builder()
        predicate_builder = read_builder.new_predicate_builder()
        predicate = predicate_builder.equal('category', 'A')
        read_builder = read_builder.with_filter(predicate)

        table_read = read_builder.new_read()
        table_scan = read_builder.new_scan()
        splits = table_scan.plan().splits()

        ray_dataset = table_read.to_ray(splits, parallelism=1)

        # Verify filtered results
        self.assertEqual(ray_dataset.count(), 2, "Should have 2 rows after filtering")
        df = ray_dataset.to_pandas()
        self.assertEqual(set(df['category'].tolist()), {'A'}, "All rows should have category='A'")
        self.assertEqual(set(df['id'].tolist()), {1, 3}, "Should have IDs 1 and 3")

        # Read with predicate on partition
        read_builder = table.new_read_builder()
        predicate_builder = read_builder.new_predicate_builder()
        predicate = predicate_builder.equal('dt', '2024-01-01')
        read_builder = read_builder.with_filter(predicate)

        table_read = read_builder.new_read()
        table_scan = read_builder.new_scan()
        splits = table_scan.plan().splits()

        ray_dataset = table_read.to_ray(splits, parallelism=1)

        # Verify filtered results by partition
        self.assertEqual(ray_dataset.count(), 2, "Should have 2 rows in partition 2024-01-01")
        df = ray_dataset.to_pandas()
        self.assertEqual(set(df['dt'].tolist()), {'2024-01-01'}, "All rows should be in partition 2024-01-01")

    def test_ray_data_primary_key_multiple_splits_same_bucket(self):
        """Test Ray Data read from PrimaryKey table with small target_split_size."""

        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('value', pa.int64()),
        ])

        schema = Schema.from_pyarrow_schema(
            pa_schema,
            primary_keys=['id'],
            options={
                'bucket': '2',
                CoreOptions.SOURCE_SPLIT_TARGET_SIZE.key(): '1b'
            }
        )
        self.catalog.create_table('default.test_ray_pk_multi_split', schema, False)
        table = self.catalog.get_table('default.test_ray_pk_multi_split')

        initial_data = pa.Table.from_pydict({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'value': [100, 200, 300],
        }, schema=pa_schema)

        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(initial_data)
        commit_messages = writer.prepare_commit()
        commit = write_builder.new_commit()
        commit.commit(commit_messages)
        writer.close()

        updated_data = pa.Table.from_pydict({
            'id': [1, 2, 4],
            'name': ['Alice-Updated', 'Bob-Updated', 'David'],
            'value': [150, 250, 400],
        }, schema=pa_schema)

        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(updated_data)
        commit_messages = writer.prepare_commit()
        commit = write_builder.new_commit()
        commit.commit(commit_messages)
        writer.close()

        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        table_scan = read_builder.new_scan()
        splits = table_scan.plan().splits()

        ray_dataset = table_read.to_ray(splits, parallelism=2)

        self.assertIsNotNone(ray_dataset, "Ray dataset should not be None")
        self.assertEqual(ray_dataset.count(), 4, "Should have 4 rows after upsert")

        df = ray_dataset.to_pandas()
        df_sorted = df.sort_values(by='id').reset_index(drop=True)
        self.assertEqual(list(df_sorted['id']), [1, 2, 3, 4], "ID column should match")
        self.assertEqual(
            list(df_sorted['name']),
            ['Alice-Updated', 'Bob-Updated', 'Charlie', 'David'],
            "Name column should reflect updates"
        )
        self.assertEqual(list(df_sorted['value']), [150, 250, 300, 400], "Value column should reflect updates")

    def test_ray_data_invalid_parallelism(self):
        """Test that invalid parallelism values raise ValueError."""
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
        ])

        schema = Schema.from_pyarrow_schema(pa_schema)
        self.catalog.create_table('default.test_ray_invalid_parallelism', schema, False)
        table = self.catalog.get_table('default.test_ray_invalid_parallelism')

        # Write some data
        data = pa.Table.from_pydict({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
        }, schema=pa_schema)

        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(data)
        commit_messages = writer.prepare_commit()
        commit = write_builder.new_commit()
        commit.commit(commit_messages)
        writer.close()

        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        table_scan = read_builder.new_scan()
        splits = table_scan.plan().splits()

        # Test with parallelism = 0
        with self.assertRaises(ValueError) as context:
            table_read.to_ray(splits, parallelism=0)
        self.assertIn("parallelism must be at least 1", str(context.exception))

        # Test with parallelism < 0
        with self.assertRaises(ValueError) as context:
            table_read.to_ray(splits, parallelism=-1)
        self.assertIn("parallelism must be at least 1", str(context.exception))

        # Test with parallelism = -10
        with self.assertRaises(ValueError) as context:
            table_read.to_ray(splits, parallelism=-10)
        self.assertIn("parallelism must be at least 1", str(context.exception))


if __name__ == '__main__':
    unittest.main()
