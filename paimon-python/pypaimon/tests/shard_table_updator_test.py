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
        table_update.with_read_projection(['a', 'b', 'c', '_ROW_ID'])
        table_update.with_update_type(['d'])
        
        shard_updator = table_update.new_shard_updator(0, 1)

        # Read data using arrow_reader
        reader = shard_updator.arrow_reader()

        for batch in iter(reader.read_next_batch, None):
            # Compute d = c + b - a
            a_values = batch.column('a').to_pylist()
            b_values = batch.column('b').to_pylist()
            c_values = batch.column('c').to_pylist()
            row_id_values = batch.column('_ROW_ID').to_pylist()
            self.assertEqual(
                row_id_values,
                list(range(len(a_values))),
                '_ROW_ID should be [0, 1, 2, ...] for sequential rows',
            )

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

    def test_partial_shard_update_full_read_schema_unified(self):
        table_schema = pa.schema([
            ('a', pa.int32()),
            ('b', pa.int32()),
            ('c', pa.int32()),
            ('d', pa.int32()),
        ])
        schema = Schema.from_pyarrow_schema(
            table_schema,
            options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'},
        )
        name = self._create_unique_table_name()
        self.catalog.create_table(name, schema, False)
        table = self.catalog.get_table(name)

        # Two commits => two files (two first_row_id ranges)
        for start, end in [(1, 10), (10, 20)]:
            wb = table.new_batch_write_builder()
            tw = wb.new_write().with_write_type(['a', 'b', 'c'])
            tc = wb.new_commit()
            data = pa.Table.from_pydict({
                'a': list(range(start, end + 1)),
                'b': [i * 10 for i in range(start, end + 1)],
                'c': [i * 100 for i in range(start, end + 1)],
            }, schema=pa.schema([
                ('a', pa.int32()), ('b', pa.int32()), ('c', pa.int32()),
            ]))
            tw.write_arrow(data)
            tc.commit(tw.prepare_commit())
            tw.close()
            tc.close()

        # Only shard 0 runs => only first file gets d
        wb = table.new_batch_write_builder()
        upd = wb.new_update()
        upd.with_read_projection(['a', 'b', 'c'])
        upd.with_update_type(['d'])
        shard0 = upd.new_shard_updator(0, 2)
        reader = shard0.arrow_reader()
        for batch in iter(reader.read_next_batch, None):
            a_ = batch.column('a').to_pylist()
            b_ = batch.column('b').to_pylist()
            c_ = batch.column('c').to_pylist()
            d_ = [c + b - a for a, b, c in zip(a_, b_, c_)]
            shard0.update_by_arrow_batch(pa.RecordBatch.from_pydict(
                {'d': d_}, schema=pa.schema([('d', pa.int32())]),
            ))
        tc = wb.new_commit()
        tc.commit(shard0.prepare_commit())
        tc.close()

        rb = table.new_read_builder()
        tr = rb.new_read()
        actual = tr.to_arrow(rb.new_scan().plan().splits())
        self.assertEqual(actual.num_rows, 21)
        d_col = actual.column('d')
        # First 10 rows (shard 0): d = c+b-a
        for i in range(10):
            self.assertEqual(d_col[i].as_py(), (i + 1) * 100 + (i + 1) * 10 - (i + 1))
        # Rows 10-20 (shard 1 not run): d is null
        for i in range(10, 21):
            self.assertIsNone(d_col[i].as_py())

    def test_with_shard_read_after_partial_shard_update(self):
        table_schema = pa.schema([
            ('a', pa.int32()),
            ('b', pa.int32()),
            ('c', pa.int32()),
            ('d', pa.int32()),
        ])
        schema = Schema.from_pyarrow_schema(
            table_schema,
            options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'},
        )
        name = self._create_unique_table_name()
        self.catalog.create_table(name, schema, False)
        table = self.catalog.get_table(name)

        for start, end in [(1, 10), (10, 20)]:
            wb = table.new_batch_write_builder()
            tw = wb.new_write().with_write_type(['a', 'b', 'c'])
            tc = wb.new_commit()
            data = pa.Table.from_pydict({
                'a': list(range(start, end + 1)),
                'b': [i * 10 for i in range(start, end + 1)],
                'c': [i * 100 for i in range(start, end + 1)],
            }, schema=pa.schema([
                ('a', pa.int32()), ('b', pa.int32()), ('c', pa.int32()),
            ]))
            tw.write_arrow(data)
            tc.commit(tw.prepare_commit())
            tw.close()
            tc.close()

        wb = table.new_batch_write_builder()
        upd = wb.new_update()
        upd.with_read_projection(['a', 'b', 'c'])
        upd.with_update_type(['d'])
        shard0 = upd.new_shard_updator(0, 2)
        reader = shard0.arrow_reader()
        for batch in iter(reader.read_next_batch, None):
            a_ = batch.column('a').to_pylist()
            b_ = batch.column('b').to_pylist()
            c_ = batch.column('c').to_pylist()
            d_ = [c + b - a for a, b, c in zip(a_, b_, c_)]
            shard0.update_by_arrow_batch(pa.RecordBatch.from_pydict(
                {'d': d_}, schema=pa.schema([('d', pa.int32())]),
            ))
        tc = wb.new_commit()
        tc.commit(shard0.prepare_commit())
        tc.close()

        rb = table.new_read_builder()
        tr = rb.new_read()

        splits_0 = rb.new_scan().with_shard(0, 2).plan().splits()
        result_0 = tr.to_arrow(splits_0)
        self.assertEqual(result_0.num_rows, 10)
        d_col_0 = result_0.column('d')
        for i in range(10):
            self.assertEqual(
                d_col_0[i].as_py(),
                (i + 1) * 100 + (i + 1) * 10 - (i + 1),
                "Shard 0 row %d: d should be c+b-a" % i,
            )

        splits_1 = rb.new_scan().with_shard(1, 2).plan().splits()
        result_1 = tr.to_arrow(splits_1)
        self.assertEqual(result_1.num_rows, 11)
        d_col_1 = result_1.column('d')
        for i in range(11):
            self.assertIsNone(d_col_1[i].as_py(), "Shard 1 row %d: d should be null" % i)

        full_splits = rb.new_scan().plan().splits()
        full_result = tr.to_arrow(full_splits)
        self.assertEqual(
            result_0.num_rows + result_1.num_rows,
            full_result.num_rows,
            "Shard 0 + Shard 1 row count should equal full scan (21)",
        )

        rb_filter = table.new_read_builder()
        rb_filter.with_projection(['a', 'b', 'c', 'd', '_ROW_ID'])
        pb = rb_filter.new_predicate_builder()
        pred_row_id = pb.is_in('_ROW_ID', [0, 1, 2, 3, 4])
        rb_filter.with_filter(pred_row_id)
        tr_filter = rb_filter.new_read()
        splits_row_id = rb_filter.new_scan().plan().splits()
        result_row_id = tr_filter.to_arrow(splits_row_id)
        self.assertEqual(result_row_id.num_rows, 5, "Filter _ROW_ID in [0..4] should return 5 rows")
        a_col = result_row_id.column('a')
        d_col_r = result_row_id.column('d')
        for i in range(5):
            self.assertEqual(a_col[i].as_py(), i + 1)
            self.assertEqual(
                d_col_r[i].as_py(),
                (i + 1) * 100 + (i + 1) * 10 - (i + 1),
                "Filter-by-_row_id row %d: d should be c+b-a" % i,
            )

        rb_slice = table.new_read_builder()
        tr_slice = rb_slice.new_read()
        slice_0 = rb_slice.new_scan().with_slice(0, 10).plan().splits()
        result_slice_0 = tr_slice.to_arrow(slice_0)
        self.assertEqual(result_slice_0.num_rows, 10, "with_slice(0, 10) should return 10 rows")
        d_s0 = result_slice_0.column('d')
        for i in range(10):
            self.assertEqual(
                d_s0[i].as_py(),
                (i + 1) * 100 + (i + 1) * 10 - (i + 1),
                "Slice [0,10) row %d: d should be c+b-a" % i,
            )
        slice_1 = rb_slice.new_scan().with_slice(10, 21).plan().splits()
        result_slice_1 = tr_slice.to_arrow(slice_1)
        self.assertEqual(result_slice_1.num_rows, 11, "with_slice(10, 21) should return 11 rows")
        d_s1 = result_slice_1.column('d')
        for i in range(11):
            self.assertIsNone(d_s1[i].as_py(), "Slice [10,21) row %d: d should be null" % i)

        cross_slice = rb_slice.new_scan().with_slice(5, 16).plan().splits()
        result_cross = tr_slice.to_arrow(cross_slice)
        self.assertEqual(
            result_cross.num_rows, 11,
            "Cross-shard with_slice(5, 16) should return 11 rows (5 from file1 + 6 from file2)",
        )
        a_cross = result_cross.column('a')
        d_cross = result_cross.column('d')
        for i in range(5):
            self.assertEqual(a_cross[i].as_py(), 6 + i)
            self.assertEqual(
                d_cross[i].as_py(),
                (6 + i) * 100 + (6 + i) * 10 - (6 + i),
                "Cross-shard slice row %d (from file1): d should be c+b-a" % i,
            )
        for i in range(5, 11):
            self.assertEqual(a_cross[i].as_py(), 10 + (i - 5))
            self.assertIsNone(d_cross[i].as_py(), "Cross-shard slice row %d (from file2): d null" % i)

        rb_col = table.new_read_builder()
        rb_col.with_projection(['a', 'b', 'c', 'd'])
        pb_col = rb_col.new_predicate_builder()
        pred_d = pb_col.is_in('d', [109, 218])  # d = c+b-a for a=1,2
        rb_col.with_filter(pred_d)
        tr_col = rb_col.new_read()
        splits_d = rb_col.new_scan().plan().splits()
        result_d = tr_col.to_arrow(splits_d)
        self.assertEqual(result_d.num_rows, 2, "Filter d in [109, 218] should return 2 rows")
        a_d = result_d.column('a')
        d_d = result_d.column('d')
        self.assertEqual(a_d[0].as_py(), 1)
        self.assertEqual(d_d[0].as_py(), 109)
        self.assertEqual(a_d[1].as_py(), 2)
        self.assertEqual(d_d[1].as_py(), 218)

    def test_read_projection(self):
        table_schema = pa.schema([
            ('a', pa.int32()),
            ('b', pa.int32()),
            ('c', pa.int32()),
        ])
        schema = Schema.from_pyarrow_schema(
            table_schema,
            options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'}
        )
        name = self._create_unique_table_name('read_proj')
        self.catalog.create_table(name, schema, False)
        table = self.catalog.get_table(name)

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write().with_write_type(['a', 'b', 'c'])
        table_commit = write_builder.new_commit()
        init_data = pa.Table.from_pydict(
            {'a': [1, 2, 3], 'b': [10, 20, 30], 'c': [100, 200, 300]},
            schema=pa.schema([('a', pa.int32()), ('b', pa.int32()), ('c', pa.int32())])
        )
        table_write.write_arrow(init_data)
        cmts = table_write.prepare_commit()
        for cmt in cmts:
            for nf in cmt.new_files:
                nf.first_row_id = 0
        table_commit.commit(cmts)
        table_write.close()
        table_commit.close()

        table_update = write_builder.new_update()
        table_update.with_read_projection(['a', 'b', 'c'])
        table_update.with_update_type(['a'])
        shard_updator = table_update.new_shard_updator(0, 1)
        reader = shard_updator.arrow_reader()

        batch = reader.read_next_batch()
        self.assertIsNotNone(batch, "Should have at least one batch")
        actual_columns = set(batch.schema.names)

        expected_columns = {'a', 'b', 'c'}
        self.assertEqual(
            actual_columns,
            expected_columns,
            "with_read_projection(['a','b','c']) should return only a,b,c; "
            "got %s. _ROW_ID and _SEQUENCE_NUMBER should NOT be returned when not in projection."
            % actual_columns
        )


if __name__ == '__main__':
    unittest.main()
