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
import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema


class TableUpdateTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({
            'warehouse': cls.warehouse
        })
        cls.catalog.create_database('default', True)

        # Define table schema for testing
        cls.pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('age', pa.int32()),
            ('city', pa.string()),
        ])

        # Define options for data evolution
        cls.table_options = {
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true'
        }

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _create_table(self):
        """Helper method to create a table with initial data."""
        # Generate unique table name for each test
        import uuid
        table_name = f'test_data_evolution_{uuid.uuid4().hex[:8]}'
        schema = Schema.from_pyarrow_schema(self.pa_schema, options=self.table_options)
        self.catalog.create_table(f'default.{table_name}', schema, False)
        table = self.catalog.get_table(f'default.{table_name}')

        # Write batch-1
        write_builder = table.new_batch_write_builder()

        initial_data = pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'age': [25, 30],
            'city': ['NYC', 'LA']
        }, schema=self.pa_schema)

        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(initial_data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # Write batch-2
        following_data = pa.Table.from_pydict({
            'id': [3, 4, 5],
            'name': ['Charlie', 'David', 'Eve'],
            'age': [35, 40, 45],
            'city': ['Chicago', 'Houston', 'Phoenix']
        }, schema=self.pa_schema)

        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(following_data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        return table

    def test_update_existing_column(self):
        """Test updating an existing column using data evolution."""
        # Create table with initial data
        table = self._create_table()

        # Create data evolution table update
        write_builder = table.new_batch_write_builder()
        batch_write = write_builder.new_write()

        # Prepare update data (sorted by row_id)
        update_data = pa.Table.from_pydict({
            '_ROW_ID': [1, 0, 2, 3, 4],
            'age': [31, 26, 36, 39, 42]
        })

        # Update the age column
        write_builder = table.new_batch_write_builder()
        table_update = write_builder.new_update().with_update_type(['age'])
        commit_messages = table_update.update_by_arrow_with_row_id(update_data)

        # Commit the changes
        table_commit = write_builder.new_commit()
        table_commit.commit(commit_messages)
        table_commit.close()
        batch_write.close()

        # Verify the updated data
        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        result = table_read.to_arrow(splits)

        # Check that ages were updated for rows 0-2
        ages = result['age'].to_pylist()
        expected_ages = [26, 31, 36, 39, 42]
        self.assertEqual(ages, expected_ages)

    def test_update_multiple_columns(self):
        """Test updating multiple columns at once."""
        # Create table with initial data
        table = self._create_table()

        # Create data evolution table update
        write_builder = table.new_batch_write_builder()
        batch_write = write_builder.new_write()

        # Prepare update data (sorted by row_id)
        update_data = pa.Table.from_pydict({
            '_ROW_ID': [1, 0, 2, 3, 4],
            'age': [31, 26, 36, 39, 42],
            'city': ['Los Angeles', 'New York', 'Chicago', 'Phoenix', 'Houston']
        })

        # Update multiple columns
        write_builder = table.new_batch_write_builder()
        table_update = write_builder.new_update().with_update_type(['age', 'city'])
        commit_messages = table_update.update_by_arrow_with_row_id(update_data)

        # Commit the changes
        table_commit = write_builder.new_commit()
        table_commit.commit(commit_messages)
        table_commit.close()
        batch_write.close()

        # Verify the updated data
        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        result = table_read.to_arrow(splits)

        # Check that both age and city were updated for rows 0-2
        ages = result['age'].to_pylist()
        cities = result['city'].to_pylist()

        expected_ages = [26, 31, 36, 39, 42]
        expected_cities = ['New York', 'Los Angeles', 'Chicago', 'Phoenix', 'Houston']

        self.assertEqual(ages, expected_ages)
        self.assertEqual(cities, expected_cities)

    def test_nonexistent_column(self):
        """Test that updating a non-existent column raises an error."""
        table = self._create_table()

        # Try to update a non-existent column
        update_data = pa.Table.from_pydict({
            '_ROW_ID': [0, 1, 2, 3, 4],
            'nonexistent_column': [100, 200, 300, 400, 500]
        })

        # Should raise ValueError
        with self.assertRaises(ValueError) as context:
            write_builder = table.new_batch_write_builder()
            table_update = write_builder.new_update().with_update_type(['nonexistent_column'])
            table_update.update_by_arrow_with_row_id(update_data)

        self.assertIn('not in table schema', str(context.exception))

    def test_missing_row_id_column(self):
        """Test that missing row_id column raises an error."""
        table = self._create_table()

        # Create data evolution table update
        write_builder = table.new_batch_write_builder()
        batch_write = write_builder.new_write()

        # Prepare update data without row_id column
        update_data = pa.Table.from_pydict({
            'age': [26, 27, 28, 29, 30]
        })

        # Should raise ValueError
        with self.assertRaises(ValueError) as context:
            write_builder = table.new_batch_write_builder()
            table_update = write_builder.new_update().with_update_type(['age'])
            table_update.update_by_arrow_with_row_id(update_data)

        self.assertIn("Input data must contain _ROW_ID column", str(context.exception))
        batch_write.close()

    def test_partitioned_table_update(self):
        """Test updating columns in a partitioned table."""
        # Create partitioned table
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['city'], options=self.table_options)
        self.catalog.create_table('default.test_partitioned_evolution', schema, False)
        table = self.catalog.get_table('default.test_partitioned_evolution')

        # Write initial data
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        initial_data = pa.Table.from_pydict({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'age': [25, 30, 35, 40, 45],
            'city': ['NYC', 'NYC', 'LA', 'LA', 'Chicago']
        }, schema=self.pa_schema)

        table_write.write_arrow(initial_data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # Create data evolution table update
        write_builder = table.new_batch_write_builder()
        table_update = write_builder.new_update().with_update_type(['age'])

        # Update ages
        update_data = pa.Table.from_pydict({
            '_ROW_ID': [1, 0, 2, 3, 4],
            'age': [31, 26, 36, 41, 46]
        })

        commit_messages = table_update.update_by_arrow_with_row_id(update_data)

        # Commit the changes
        table_commit = write_builder.new_commit()
        table_commit.commit(commit_messages)
        table_commit.close()

        # Verify the updated data
        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        result = table_read.to_arrow(splits)

        # Check ages were updated
        ages = result['age'].to_pylist()
        expected_ages = [26, 31, 36, 41, 46]
        self.assertEqual(ages, expected_ages)

    def test_multiple_calls(self):
        """Test multiple calls to update_columns, each updating a single column."""
        # Create table with initial data
        table = self._create_table()

        # First update: Update age column
        write_builder = table.new_batch_write_builder()
        table_update = write_builder.new_update().with_update_type(['age'])

        update_age_data = pa.Table.from_pydict({
            '_ROW_ID': [1, 0, 2, 3, 4],
            'age': [31, 26, 36, 41, 46]
        })

        commit_messages = table_update.update_by_arrow_with_row_id(update_age_data)
        table_commit = write_builder.new_commit()
        table_commit.commit(commit_messages)
        table_commit.close()

        # Second update: Update city column
        update_city_data = pa.Table.from_pydict({
            '_ROW_ID': [1, 0, 2, 3, 4],
            'city': ['Los Angeles', 'New York', 'Chicago', 'Phoenix', 'Houston']
        })
        table_update.with_update_type(['city'])
        commit_messages = table_update.update_by_arrow_with_row_id(update_city_data)
        table_commit = write_builder.new_commit()
        table_commit.commit(commit_messages)
        table_commit.close()

        # Verify both columns were updated correctly
        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        result = table_read.to_arrow(splits)

        ages = result['age'].to_pylist()
        cities = result['city'].to_pylist()

        expected_ages = [26, 31, 36, 41, 46]
        expected_cities = ['New York', 'Los Angeles', 'Chicago', 'Phoenix', 'Houston']

        self.assertEqual(ages, expected_ages, "Age column was not updated correctly")
        self.assertEqual(cities, expected_cities, "City column was not updated correctly")

    def test_update_partial_file(self):
        """Test updating an existing column using data evolution."""
        # Create table with initial data
        table = self._create_table()

        # Create data evolution table update
        write_builder = table.new_batch_write_builder()
        batch_write = write_builder.new_write()

        # Prepare update data (sorted by row_id)
        update_data = pa.Table.from_pydict({
            '_ROW_ID': [1],
            'age': [31]
        })

        # Update the age column
        write_builder = table.new_batch_write_builder()
        table_update = write_builder.new_update().with_update_type(['age'])
        commit_messages = table_update.update_by_arrow_with_row_id(update_data)

        # Commit the changes
        table_commit = write_builder.new_commit()
        table_commit.commit(commit_messages)
        table_commit.close()
        batch_write.close()

        # Verify the updated data
        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        result = table_read.to_arrow(splits)

        # Check that ages were updated for rows 0-2
        ages = result['age'].to_pylist()
        expected_ages = [25, 31, 35, 40, 45]
        self.assertEqual(ages, expected_ages)

    def test_partial_rows_update_single_file(self):
        """Test updating only some rows within a single file."""
        # Create table with initial data
        table = self._create_table()

        # Create data evolution table update
        write_builder = table.new_batch_write_builder()
        table_update = write_builder.new_update().with_update_type(['age'])

        # Update only row 0 in the first file (which contains rows 0-1)
        update_data = pa.Table.from_pydict({
            '_ROW_ID': [0],
            'age': [100]
        })

        commit_messages = table_update.update_by_arrow_with_row_id(update_data)

        # Commit the changes
        table_commit = write_builder.new_commit()
        table_commit.commit(commit_messages)
        table_commit.close()

        # Verify the updated data
        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        result = table_read.to_arrow(splits)

        # Check that only row 0 was updated, others remain unchanged
        ages = result['age'].to_pylist()
        expected_ages = [100, 30, 35, 40, 45]  # Only first row updated
        self.assertEqual(ages, expected_ages)

    def test_partial_rows_update_multiple_files(self):
        """Test updating partial rows across multiple files."""
        # Create table with initial data
        table = self._create_table()

        # Create data evolution table update
        write_builder = table.new_batch_write_builder()
        table_update = write_builder.new_update().with_update_type(['age'])

        # Update row 1 from first file and row 2 from second file
        update_data = pa.Table.from_pydict({
            '_ROW_ID': [1, 2],
            'age': [200, 300]
        })

        commit_messages = table_update.update_by_arrow_with_row_id(update_data)

        # Commit the changes
        table_commit = write_builder.new_commit()
        table_commit.commit(commit_messages)
        table_commit.close()

        # Verify the updated data
        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        result = table_read.to_arrow(splits)

        # Check that only specified rows were updated
        ages = result['age'].to_pylist()
        expected_ages = [25, 200, 300, 40, 45]  # Rows 1 and 2 updated
        self.assertEqual(ages, expected_ages)

    def test_partial_rows_non_consecutive(self):
        """Test updating non-consecutive rows."""
        # Create table with initial data
        table = self._create_table()

        # Create data evolution table update
        write_builder = table.new_batch_write_builder()
        table_update = write_builder.new_update().with_update_type(['age'])

        # Update rows 0, 2, 4 (non-consecutive)
        update_data = pa.Table.from_pydict({
            '_ROW_ID': [0, 2, 4],
            'age': [100, 300, 500]
        })

        commit_messages = table_update.update_by_arrow_with_row_id(update_data)

        # Commit the changes
        table_commit = write_builder.new_commit()
        table_commit.commit(commit_messages)
        table_commit.close()

        # Verify the updated data
        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        result = table_read.to_arrow(splits)

        # Check that only specified rows were updated
        ages = result['age'].to_pylist()
        expected_ages = [100, 30, 300, 40, 500]  # Rows 0, 2, 4 updated
        self.assertEqual(ages, expected_ages)

    def test_update_preserves_other_columns(self):
        """Test that updating one column preserves other columns."""
        # Create table with initial data
        table = self._create_table()

        # Create data evolution table update
        write_builder = table.new_batch_write_builder()
        table_update = write_builder.new_update().with_update_type(['age'])

        # Update only row 1
        update_data = pa.Table.from_pydict({
            '_ROW_ID': [1],
            'age': [999]
        })

        commit_messages = table_update.update_by_arrow_with_row_id(update_data)

        # Commit the changes
        table_commit = write_builder.new_commit()
        table_commit.commit(commit_messages)
        table_commit.close()

        # Verify the updated data
        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        result = table_read.to_arrow(splits)

        # Check that age was updated for row 1
        ages = result['age'].to_pylist()
        expected_ages = [25, 999, 35, 40, 45]
        self.assertEqual(ages, expected_ages)

        # Check that other columns remain unchanged
        names = result['name'].to_pylist()
        expected_names = ['Alice', 'Bob', 'Charlie', 'David', 'Eve']
        self.assertEqual(names, expected_names)

        cities = result['city'].to_pylist()
        expected_cities = ['NYC', 'LA', 'Chicago', 'Houston', 'Phoenix']
        self.assertEqual(cities, expected_cities)

    def test_sequential_partial_updates(self):
        """Test multiple sequential partial updates on the same table."""
        # Create table with initial data
        table = self._create_table()

        # First update: Update row 0
        write_builder = table.new_batch_write_builder()
        table_update = write_builder.new_update().with_update_type(['age'])

        update_data_1 = pa.Table.from_pydict({
            '_ROW_ID': [0],
            'age': [100]
        })

        commit_messages = table_update.update_by_arrow_with_row_id(update_data_1)
        table_commit = write_builder.new_commit()
        table_commit.commit(commit_messages)
        table_commit.close()

        # Second update: Update row 2
        write_builder = table.new_batch_write_builder()
        table_update = write_builder.new_update().with_update_type(['age'])

        update_data_2 = pa.Table.from_pydict({
            '_ROW_ID': [2],
            'age': [300]
        })

        commit_messages = table_update.update_by_arrow_with_row_id(update_data_2)
        table_commit = write_builder.new_commit()
        table_commit.commit(commit_messages)
        table_commit.close()

        # Third update: Update row 4
        write_builder = table.new_batch_write_builder()
        table_update = write_builder.new_update().with_update_type(['age'])

        update_data_3 = pa.Table.from_pydict({
            '_ROW_ID': [4],
            'age': [500]
        })

        commit_messages = table_update.update_by_arrow_with_row_id(update_data_3)
        table_commit = write_builder.new_commit()
        table_commit.commit(commit_messages)
        table_commit.close()

        # Verify the final data
        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        result = table_read.to_arrow(splits)

        # Check that all updates were applied correctly
        ages = result['age'].to_pylist()
        expected_ages = [100, 30, 300, 40, 500]
        self.assertEqual(expected_ages, ages)

    def test_row_id_out_of_range(self):
        """Test that row_id out of valid range raises an error."""
        # Create table with initial data
        table = self._create_table()

        # Create data evolution table update
        write_builder = table.new_batch_write_builder()
        table_update = write_builder.new_update().with_update_type(['age'])

        # Prepare update data with row_id out of range (table has 5 rows: 0-4)
        update_data = pa.Table.from_pydict({
            '_ROW_ID': [0, 10],  # 10 is out of range
            'age': [26, 100]
        })

        # Should raise ValueError for row_id out of range
        with self.assertRaises(ValueError) as context:
            table_update.update_by_arrow_with_row_id(update_data)

        self.assertIn("out of valid range", str(context.exception))

    def test_negative_row_id(self):
        """Test that negative row_id raises an error."""
        # Create table with initial data
        table = self._create_table()

        # Create data evolution table update
        write_builder = table.new_batch_write_builder()
        table_update = write_builder.new_update().with_update_type(['age'])

        # Prepare update data with negative row_id
        update_data = pa.Table.from_pydict({
            '_ROW_ID': [-1, 0],
            'age': [100, 26]
        })

        # Should raise ValueError for negative row_id
        with self.assertRaises(ValueError) as context:
            table_update.update_by_arrow_with_row_id(update_data)

        self.assertIn("out of valid range", str(context.exception))

    def test_duplicate_row_id(self):
        """Test that duplicate _ROW_ID values raise an error."""
        table = self._create_table()

        write_builder = table.new_batch_write_builder()
        table_update = write_builder.new_update().with_update_type(['age'])

        update_data = pa.Table.from_pydict({
            '_ROW_ID': [0, 0, 1],
            'age': [100, 200, 300]
        })

        with self.assertRaises(ValueError) as context:
            table_update.update_by_arrow_with_row_id(update_data)

        self.assertIn("duplicate _ROW_ID values", str(context.exception))

    def test_large_table_partial_column_updates(self):
        """Test partial column updates on a large table with 4 columns.

        This test covers:
        1. Update first column (id), update 1 row, verify result
        2. Update first and second columns (id, name), update 2 rows, verify result
        3. Update second column (name), update 1 row, verify result
        4. Update third column (age), verify result
        """
        import uuid

        # Create table with 4 columns and 20 rows in 2 files
        table_name = f'test_large_table_{uuid.uuid4().hex[:8]}'
        schema = Schema.from_pyarrow_schema(self.pa_schema, options=self.table_options)
        self.catalog.create_table(f'default.{table_name}', schema, False)
        table = self.catalog.get_table(f'default.{table_name}')

        # Write batch-1
        num_row = 1000
        write_builder = table.new_batch_write_builder()
        batch1_data = pa.Table.from_pydict({
            'id': list(range(num_row)),
            'name': [f'Name_{i}' for i in range(num_row)],
            'age': [20 + i for i in range(num_row)],
            'city': [f'City_{i}' for i in range(num_row)]
        }, schema=self.pa_schema)

        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(batch1_data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # Write batch-2
        batch2_data = pa.Table.from_pydict({
            'id': list(range(num_row, num_row * 2)),
            'name': [f'Name_{i}' for i in range(num_row, num_row * 2)],
            'age': [20 + num_row + i for i in range(num_row)],
            'city': [f'City_{i}' for i in range(num_row, num_row * 2)]
        }, schema=self.pa_schema)

        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(batch2_data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # --- Test 1: Update first column (id), update 1 row ---
        write_builder = table.new_batch_write_builder()
        table_update = write_builder.new_update().with_update_type(['id'])

        update_data = pa.Table.from_pydict({
            '_ROW_ID': [5],
            'id': [999]
        })

        commit_messages = table_update.update_by_arrow_with_row_id(update_data)
        table_commit = write_builder.new_commit()
        table_commit.commit(commit_messages)
        table_commit.close()

        # Verify Test 1
        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        result = table_read.to_arrow(splits)

        ids = result['id'].to_pylist()
        expected_ids = list(range(num_row * 2))
        expected_ids[5] = 999
        self.assertEqual(expected_ids, ids, "Test 1 failed: Update first column for 1 row")

        # --- Test 2: Update first and second columns (id, name), update 2 rows ---
        write_builder = table.new_batch_write_builder()
        table_update = write_builder.new_update().with_update_type(['id', 'name'])

        update_data = pa.Table.from_pydict({
            '_ROW_ID': [3, 15],
            'id': [888, 777],
            'name': ['Updated_Name_3', 'Updated_Name_15']
        })

        commit_messages = table_update.update_by_arrow_with_row_id(update_data)
        table_commit = write_builder.new_commit()
        table_commit.commit(commit_messages)
        table_commit.close()

        # Verify Test 2
        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        result = table_read.to_arrow(splits)

        ids = result['id'].to_pylist()
        expected_ids[3] = 888
        expected_ids[15] = 777
        self.assertEqual(expected_ids, ids, "Test 2 failed: Update id column for 2 rows")

        names = result['name'].to_pylist()
        expected_names = [f'Name_{i}' for i in range(num_row * 2)]
        expected_names[3] = 'Updated_Name_3'
        expected_names[15] = 'Updated_Name_15'
        self.assertEqual(expected_names, names, "Test 2 failed: Update name column for 2 rows")

        # --- Test 3: Update second column (name), update 1 row ---
        write_builder = table.new_batch_write_builder()
        table_update = write_builder.new_update().with_update_type(['name'])

        update_data = pa.Table.from_pydict({
            '_ROW_ID': [12],
            'name': ['NewName_12']
        })

        commit_messages = table_update.update_by_arrow_with_row_id(update_data)
        table_commit = write_builder.new_commit()
        table_commit.commit(commit_messages)
        table_commit.close()

        # Verify Test 3
        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        result = table_read.to_arrow(splits)

        names = result['name'].to_pylist()
        expected_names[12] = 'NewName_12'
        self.assertEqual(expected_names, names, "Test 3 failed: Update name column for 1 row")

        # --- Test 4: Update third column (age), update multiple rows across both files ---
        write_builder = table.new_batch_write_builder()
        table_update = write_builder.new_update().with_update_type(['age'])

        update_data = pa.Table.from_pydict({
            '_ROW_ID': [0, 5, 10, 15],
            'age': [100, 105, 110, 115]
        })

        commit_messages = table_update.update_by_arrow_with_row_id(update_data)
        table_commit = write_builder.new_commit()
        table_commit.commit(commit_messages)
        table_commit.close()

        # Verify Test 4
        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        result = table_read.to_arrow(splits)

        ages = result['age'].to_pylist()
        expected_ages = [20 + i for i in range(num_row)] + [20 + num_row + i for i in range(num_row)]
        expected_ages[0] = 100
        expected_ages[5] = 105
        expected_ages[10] = 110
        expected_ages[15] = 115
        self.assertEqual(expected_ages, ages, "Test 4 failed: Update age column for multiple rows")

        # Verify other columns remain unchanged after all updates
        cities = result['city'].to_pylist()
        expected_cities = [f'City_{i}' for i in range(num_row * 2)]
        self.assertEqual(expected_cities, cities, "City column should remain unchanged")

    def test_update_partial_rows_across_two_files(self):
        """Test updating partial rows across two data files in a single update operation.

        This test creates a table with 2 commits (2 data files), then performs a single update
        that modifies partial rows from both files simultaneously.
        """
        import uuid

        # Create table
        table_name = f'test_two_files_{uuid.uuid4().hex[:8]}'
        schema = Schema.from_pyarrow_schema(self.pa_schema, options=self.table_options)
        self.catalog.create_table(f'default.{table_name}', schema, False)
        table = self.catalog.get_table(f'default.{table_name}')

        # Commit 1: Write first batch of data (row_id 0-4)
        write_builder = table.new_batch_write_builder()
        batch1_data = pa.Table.from_pydict({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'age': [20, 25, 30, 35, 40],
            'city': ['NYC', 'LA', 'Chicago', 'Houston', 'Phoenix']
        }, schema=self.pa_schema)

        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(batch1_data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # Commit 2: Write second batch of data (row_id 5-9)
        batch2_data = pa.Table.from_pydict({
            'id': [6, 7, 8, 9, 10],
            'name': ['Frank', 'Grace', 'Henry', 'Ivy', 'Jack'],
            'age': [45, 50, 55, 60, 65],
            'city': ['Seattle', 'Boston', 'Denver', 'Miami', 'Atlanta']
        }, schema=self.pa_schema)

        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(batch2_data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # Single update: Update partial rows from both files
        # Update rows 1, 3 from file 1 and rows 6, 8 from file 2
        write_builder = table.new_batch_write_builder()
        table_update = write_builder.new_update().with_update_type(['age', 'name'])

        update_data = pa.Table.from_pydict({
            '_ROW_ID': [1, 3, 6, 8],
            'age': [100, 200, 300, 400],
            'name': ['Updated_Bob', 'Updated_David', 'Updated_Grace', 'Updated_Ivy']
        })

        commit_messages = table_update.update_by_arrow_with_row_id(update_data)
        table_commit = write_builder.new_commit()
        table_commit.commit(commit_messages)
        table_commit.close()

        # Verify the updated data
        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        result = table_read.to_arrow(splits)

        # Verify ages: rows 1, 3, 6, 8 should be updated
        ages = result['age'].to_pylist()
        expected_ages = [20, 100, 30, 200, 40, 45, 300, 55, 400, 65]
        self.assertEqual(expected_ages, ages, "Ages not updated correctly across two files")

        # Verify names: rows 1, 3, 6, 8 should be updated
        names = result['name'].to_pylist()
        expected_names = ['Alice', 'Updated_Bob', 'Charlie', 'Updated_David', 'Eve',
                          'Frank', 'Updated_Grace', 'Henry', 'Updated_Ivy', 'Jack']
        self.assertEqual(expected_names, names, "Names not updated correctly across two files")

        # Verify other columns remain unchanged
        ids = result['id'].to_pylist()
        expected_ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        self.assertEqual(expected_ids, ids, "IDs should remain unchanged")

        cities = result['city'].to_pylist()
        expected_cities = ['NYC', 'LA', 'Chicago', 'Houston', 'Phoenix',
                           'Seattle', 'Boston', 'Denver', 'Miami', 'Atlanta']
        self.assertEqual(expected_cities, cities, "Cities should remain unchanged")


if __name__ == '__main__':
    unittest.main()
