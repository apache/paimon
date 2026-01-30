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
        name = self._create_unique_table_name()
        self.catalog.create_table(name, schema, False)
        table = self.catalog.get_table(name)

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

    def test_compute_column_d_equals_c_plus_b_minus_a2(self):
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
        name = self._create_unique_table_name()
        self.catalog.create_table(name, schema, False)
        table = self.catalog.get_table(name)

        # Step 2: Write initial data for a, b, c columns only
        for i in range(1000):
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

        for i in range(10):
            d_all_values = []
            shard_updator = table_update.new_shard_updator(i, 10)

            # Read data using arrow_reader
            reader = shard_updator.arrow_reader()

            for batch in iter(reader.read_next_batch, None):
                # Compute d = c + b - a
                a_values = batch.column('a').to_pylist()
                b_values = batch.column('b').to_pylist()
                c_values = batch.column('c').to_pylist()

                d_values = [c + b - a for a, b, c in zip(a_values, b_values, c_values)]
                d_all_values.extend(d_values)

            # Concatenate all computed values and update once for this shard
            new_batch = pa.RecordBatch.from_pydict(
                {'d': d_all_values},
                schema=pa.schema([('d', pa.int32())]),
            )
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
            'a': [1, 2, 3, 4, 5] * 1000,
            'b': [10, 20, 30, 40, 50] * 1000,
            'c': [100, 200, 300, 400, 500] * 1000,
            'd': [109, 218, 327, 436, 545] * 1000,
        }, schema=table_schema)

        print("\n=== Actual Data ===")
        print(actual.to_pandas())
        print("\n=== Expected Data ===")
        print(expected.to_pandas())

        self.assertEqual(actual, expected)
        print("\n✅ Test passed! Column d = c + b - a computed correctly!")

    def test_compute_column_with_existing_column(self):
        table_schema = pa.schema([
            ('a', pa.int32()),
            ('b', pa.int32()),
            ('c', pa.int32()),
        ])
        schema = Schema.from_pyarrow_schema(
            table_schema,
            options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'}
        )
        name = self._create_unique_table_name()
        self.catalog.create_table(name, schema, False)
        table = self.catalog.get_table(name)

        # Step 2: Write initial data for a, b, c columns only
        for i in range(1000):
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
        table_update.with_read_projection(['a', 'b'])
        table_update.with_update_type(['c'])

        for i in range(10):
            shard_updator = table_update.new_shard_updator(i, 10)

            # Read data using arrow_reader
            reader = shard_updator.arrow_reader()

            for batch in iter(reader.read_next_batch, None):
                a_values = batch.column('a').to_pylist()
                b_values = batch.column('b').to_pylist()

                c_values = [b - a for a, b in zip(a_values, b_values)]

                new_batch = pa.RecordBatch.from_pydict({
                    'c': c_values,
                }, schema=pa.schema([('c', pa.int32())]))

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

        expected = pa.Table.from_pydict({
            'a': [1, 2, 3, 4, 5] * 1000,
            'b': [10, 20, 30, 40, 50] * 1000,
            'c': [9, 18, 27, 36, 45] * 1000,
        }, schema=table_schema)

        print("\n=== Actual Data ===")
        print(actual.to_pandas())
        print("\n=== Expected Data ===")
        print(expected.to_pandas())

        self.assertEqual(actual, expected)
        print("\n✅ Test passed! Column d = c + b - a computed correctly!")

if __name__ == '__main__':
    unittest.main()
