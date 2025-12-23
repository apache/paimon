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

    def test_wrong_total_row_count(self):
        """Test that wrong total row count raises an error."""
        # Create table with initial data
        table = self._create_table()

        # Create data evolution table update
        write_builder = table.new_batch_write_builder()
        table_update = write_builder.new_update().with_update_type(['age'])

        # Prepare update data with wrong row count (only 3 rows instead of 5)
        update_data = pa.Table.from_pydict({
            '_ROW_ID': [0, 1, 2],
            'age': [26, 31, 36]
        })

        # Should raise ValueError for total row count mismatch
        with self.assertRaises(ValueError) as context:
            table_update.update_by_arrow_with_row_id(update_data)

        self.assertIn("does not match table total row count", str(context.exception))

    def test_wrong_first_row_id_row_count(self):
        """Test that wrong row count for a first_row_id raises an error."""
        # Create table with initial data
        table = self._create_table()

        # Create data evolution table update
        write_builder = table.new_batch_write_builder()
        table_update = write_builder.new_update().with_update_type(['age'])

        # Prepare update data with duplicate row_id (violates monotonically increasing)
        update_data = pa.Table.from_pydict({
            '_ROW_ID': [0, 1, 1, 4, 5],
            'age': [26, 31, 36, 37, 38]
        })

        # Should raise ValueError for row ID validation
        with self.assertRaises(ValueError) as context:
            table_update.update_by_arrow_with_row_id(update_data)

        self.assertIn("Row IDs are not monotonically increasing", str(context.exception))

if __name__ == '__main__':
    unittest.main()
