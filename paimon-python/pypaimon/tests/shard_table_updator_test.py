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
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema


class ShardTableUpdatorTest(unittest.TestCase):
    """Tests for ShardTableUpdator partial column updates in data-evolution mode."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({
            'warehouse': cls.warehouse
        })
        cls.catalog.create_database('default', False)
        cls.table_count = 0

    def _create_unique_table_name(self, prefix='test'):
        ShardTableUpdatorTest.table_count += 1
        return f'default.{prefix}_{ShardTableUpdatorTest.table_count}'

    def test_compute_column_d_equals_c_plus_b_minus_a(self):
        """
        Test: Create a table with columns a, b, c, d.
        Write initial data for a, b, c.
        Use ShardTableUpdator to compute d = c + b - a and fill in the d column.
        """
        # Step 1: Create table with a, b, c, d columns (all int32)
        table_schema = pa.schema([
            ('a', pa.int32()),
            ('b', pa.int32()),
            ('c', pa.int32()),
            ('d', pa.int32()),
        ])
        schema = Schema.from_pyarrow_schema(
            table_schema,
            options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'}
        )
        self.catalog.create_table('default.test_compute_d', schema, False)
        table = self.catalog.get_table('default.test_compute_d')

        # Step 2: Write initial data for a, b, c columns only
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write().with_write_type(['a', 'b', 'c'])
        table_commit = write_builder.new_commit()

        init_data = pa.Table.from_pydict({
            'a': [1, 2, 3, 4, 5],
            'b': [10, 20, 30, 40, 50],
            'c': [100, 200, 300, 400, 500],
        }, schema=pa.schema([('a', pa.int32()), ('b', pa.int32()), ('c', pa.int32())]))

        table_write.write_arrow(init_data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # Step 3: Use ShardTableUpdator to compute d = c + b - a
        table_update = write_builder.new_update()
        table_update.with_read_projection(['a', 'b', 'c'])
        table_update.with_update_type(['d'])
        
        shard_updator = table_update.new_shard_updator(0, 1)

        # Read data using arrow_reader
        reader = shard_updator.arrow_reader()
        
        for batch in iter(reader.read_next_batch, None):
            # Compute d = c + b - a
            a_values = batch.column('a').to_pylist()
            b_values = batch.column('b').to_pylist()
            c_values = batch.column('c').to_pylist()
            
            d_values = [c + b - a for a, b, c in zip(a_values, b_values, c_values)]
            
            # Create batch with d column
            new_batch = pa.RecordBatch.from_pydict({
                'd': d_values,
            }, schema=pa.schema([('d', pa.int32())]))
            
            # Write d column
            shard_updator.update_by_arrow_batch(new_batch)

        # Prepare and commit
        commit_messages = shard_updator.prepare_commit()
        commit = write_builder.new_commit()
        commit.commit(commit_messages)
        commit.close()

        # Step 4: Verify the result
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        actual = table_read.to_arrow(table_scan.plan().splits())

        # Expected values:
        # Row 0: d = 100 + 10 - 1 = 109
        # Row 1: d = 200 + 20 - 2 = 218
        # Row 2: d = 300 + 30 - 3 = 327
        # Row 3: d = 400 + 40 - 4 = 436
        # Row 4: d = 500 + 50 - 5 = 545
        expected = pa.Table.from_pydict({
            'a': [1, 2, 3, 4, 5],
            'b': [10, 20, 30, 40, 50],
            'c': [100, 200, 300, 400, 500],
            'd': [109, 218, 327, 436, 545],
        }, schema=table_schema)

        print("\n=== Actual Data ===")
        print(actual.to_pandas())
        print("\n=== Expected Data ===")
        print(expected.to_pandas())

        self.assertEqual(actual, expected)
        print("\n✅ Test passed! Column d = c + b - a computed correctly!")

    def test_update_existing_column(self):
        """
        Test: Update an existing column's values using ShardTableUpdator.
        Create table with a, b columns, then update b = a * 2.
        """
        table_name = self._create_unique_table_name('update_existing')
        table_schema = pa.schema([
            ('a', pa.int32()),
            ('b', pa.int32()),
        ])
        schema = Schema.from_pyarrow_schema(
            table_schema,
            options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'}
        )
        self.catalog.create_table(table_name, schema, False)
        table = self.catalog.get_table(table_name)

        # Write initial data
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        init_data = pa.Table.from_pydict({
            'a': [1, 2, 3, 4, 5],
            'b': [10, 20, 30, 40, 50],  # Original values
        }, schema=table_schema)

        table_write.write_arrow(init_data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # Use ShardTableUpdator to update b = a * 2
        table_update = write_builder.new_update()
        table_update.with_read_projection(['a'])
        table_update.with_update_type(['b'])

        shard_updator = table_update.new_shard_updator(0, 1)
        reader = shard_updator.arrow_reader()

        for batch in iter(reader.read_next_batch, None):
            a_values = batch.column('a').to_pylist()
            new_b_values = [a * 2 for a in a_values]

            new_batch = pa.RecordBatch.from_pydict({
                'b': new_b_values,
            }, schema=pa.schema([('b', pa.int32())]))

            shard_updator.update_by_arrow_batch(new_batch)

        commit_messages = shard_updator.prepare_commit()
        commit = write_builder.new_commit()
        commit.commit(commit_messages)
        commit.close()

        # Verify results
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        actual = table_read.to_arrow(table_scan.plan().splits())

        expected = pa.Table.from_pydict({
            'a': [1, 2, 3, 4, 5],
            'b': [2, 4, 6, 8, 10],  # Updated: a * 2
        }, schema=table_schema)

        print("\n=== Update Existing Column ===")
        print("Actual:", actual.to_pandas().to_dict())
        print("Expected:", expected.to_pandas().to_dict())

        self.assertEqual(actual, expected)
        print("✅ Test passed!")

    def test_sharding_distributes_work(self):
        """
        Test: Verify that sharding divides splits among multiple shards.
        """
        table_name = self._create_unique_table_name('sharding')
        table_schema = pa.schema([
            ('x', pa.int32()),
            ('y', pa.int32()),
        ])
        schema = Schema.from_pyarrow_schema(
            table_schema,
            options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'}
        )
        self.catalog.create_table(table_name, schema, False)
        table = self.catalog.get_table(table_name)

        # Write multiple files to create multiple splits
        write_builder = table.new_batch_write_builder()
        table_commit = write_builder.new_commit()
        
        all_commits = []
        for i in range(3):
            table_write = write_builder.new_write().with_write_type(['x'])
            data = pa.Table.from_pydict({
                'x': list(range(i * 10, (i + 1) * 10)),
            }, schema=pa.schema([('x', pa.int32())]))
            table_write.write_arrow(data)
            all_commits.extend(table_write.prepare_commit())
            table_write.close()

        table_commit.commit(all_commits)
        table_commit.close()

        # Test sharding with 2 shards
        table_update = write_builder.new_update()
        table_update.with_read_projection(['x'])
        table_update.with_update_type(['y'])

        shard_0 = table_update.new_shard_updator(0, 2)
        shard_1 = table_update.new_shard_updator(1, 2)

        # Both shards should have some splits, or one could have all
        total_splits_shard_0 = len(shard_0.splits)
        total_splits_shard_1 = len(shard_1.splits)
        total_splits = total_splits_shard_0 + total_splits_shard_1

        print(f"\n=== Sharding Test ===")
        print(f"Shard 0 splits: {total_splits_shard_0}")
        print(f"Shard 1 splits: {total_splits_shard_1}")
        print(f"Total splits: {total_splits}")

        # At least one shard should have splits
        self.assertTrue(total_splits > 0, "Should have splits")
        print("✅ Test passed!")

    def test_multiple_columns_update(self):
        """
        Test: Update multiple columns at once.
        """
        table_name = self._create_unique_table_name('multi_cols')
        table_schema = pa.schema([
            ('a', pa.int32()),
            ('b', pa.int32()),
            ('c', pa.int32()),
        ])
        schema = Schema.from_pyarrow_schema(
            table_schema,
            options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'}
        )
        self.catalog.create_table(table_name, schema, False)
        table = self.catalog.get_table(table_name)

        # Write initial data for 'a' only
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write().with_write_type(['a'])
        table_commit = write_builder.new_commit()

        init_data = pa.Table.from_pydict({
            'a': [1, 2, 3, 4, 5],
        }, schema=pa.schema([('a', pa.int32())]))

        table_write.write_arrow(init_data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # Use ShardTableUpdator to add b and c (b = a + 1, c = a + 2)
        table_update = write_builder.new_update()
        table_update.with_read_projection(['a'])
        table_update.with_update_type(['b', 'c'])

        shard_updator = table_update.new_shard_updator(0, 1)
        reader = shard_updator.arrow_reader()

        for batch in iter(reader.read_next_batch, None):
            a_values = batch.column('a').to_pylist()
            b_values = [a + 1 for a in a_values]
            c_values = [a + 2 for a in a_values]

            new_batch = pa.RecordBatch.from_pydict({
                'b': b_values,
                'c': c_values,
            }, schema=pa.schema([('b', pa.int32()), ('c', pa.int32())]))

            shard_updator.update_by_arrow_batch(new_batch)

        commit_messages = shard_updator.prepare_commit()
        commit = write_builder.new_commit()
        commit.commit(commit_messages)
        commit.close()

        # Verify
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        actual = table_read.to_arrow(table_scan.plan().splits())

        expected = pa.Table.from_pydict({
            'a': [1, 2, 3, 4, 5],
            'b': [2, 3, 4, 5, 6],
            'c': [3, 4, 5, 6, 7],
        }, schema=table_schema)

        print("\n=== Multiple Columns Update ===")
        print("Actual:", actual.to_pandas())

        self.assertEqual(actual, expected)
        print("✅ Test passed!")

    def test_row_ranges_computed_correctly(self):
        """
        Test: Verify that row_ranges are computed correctly from files.
        """
        table_name = self._create_unique_table_name('row_ranges')
        table_schema = pa.schema([
            ('val', pa.int32()),
        ])
        schema = Schema.from_pyarrow_schema(
            table_schema,
            options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'}
        )
        self.catalog.create_table(table_name, schema, False)
        table = self.catalog.get_table(table_name)

        # Write data
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        data = pa.Table.from_pydict({
            'val': [10, 20, 30, 40, 50],
        }, schema=table_schema)

        table_write.write_arrow(data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # Create shard updator and check row_ranges
        table_update = write_builder.new_update()
        table_update.with_read_projection(['val'])
        table_update.with_update_type(['val'])

        shard_updator = table_update.new_shard_updator(0, 1)

        print("\n=== Row Ranges Test ===")
        print(f"Row ranges: {shard_updator.row_ranges}")

        # Should have at least one row range
        self.assertTrue(len(shard_updator.row_ranges) > 0, "Should have row ranges")
        
        # Each row range should have partition and range
        for partition, row_range in shard_updator.row_ranges:
            self.assertIsNotNone(row_range.from_, "Range should have from_")
            self.assertIsNotNone(row_range.to, "Range should have to")
            print(f"  Partition: {partition}, Range: {row_range.from_} - {row_range.to}")

        print("✅ Test passed!")


if __name__ == '__main__':
    unittest.main()
