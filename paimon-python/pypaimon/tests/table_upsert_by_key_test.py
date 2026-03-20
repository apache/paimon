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
import uuid

import pyarrow as pa

from pypaimon import CatalogFactory, Schema


class TableUpsertByKeyTest(unittest.TestCase):
    """Tests for TableUpsertByKey (upsert_by_arrow_with_key)."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({
            'warehouse': cls.warehouse
        })
        cls.catalog.create_database('default', True)

        # Schema for non-partitioned table
        cls.pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('age', pa.int32()),
            ('city', pa.string()),
        ])

        # Schema for partitioned table
        cls.partitioned_pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('age', pa.int32()),
            ('region', pa.string()),
        ])

        cls.table_options = {
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
        }

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def _unique_name(self, prefix='upsert'):
        return f'default.{prefix}_{uuid.uuid4().hex[:8]}'

    def _create_table(self, pa_schema=None, partition_keys=None):
        """Create a table and return the table object."""
        pa_schema = pa_schema or self.pa_schema
        name = self._unique_name()
        if partition_keys:
            schema = Schema.from_pyarrow_schema(
                pa_schema, partition_keys=partition_keys, options=self.table_options
            )
        else:
            schema = Schema.from_pyarrow_schema(pa_schema, options=self.table_options)
        self.catalog.create_table(name, schema, False)
        return self.catalog.get_table(name)

    def _write(self, table, data):
        """Write an Arrow table and commit."""
        wb = table.new_batch_write_builder()
        tw = wb.new_write()
        tc = wb.new_commit()
        tw.write_arrow(data)
        tc.commit(tw.prepare_commit())
        tw.close()
        tc.close()

    def _read_all(self, table):
        """Read the full table and return as Arrow table."""
        rb = table.new_read_builder()
        tr = rb.new_read()
        splits = rb.new_scan().plan().splits()
        return tr.to_arrow(splits)

    def _upsert(self, table, data, upsert_keys, update_cols=None):
        """Run upsert_by_arrow_with_key and commit."""
        wb = table.new_batch_write_builder()
        tu = wb.new_update()
        if update_cols:
            tu.with_update_type(update_cols)
        msgs = tu.upsert_by_arrow_with_key(data, upsert_keys)
        tc = wb.new_commit()
        tc.commit(msgs)
        tc.close()

    # ==================================================================
    # Basic upsert tests (non-partitioned)
    # ==================================================================

    def test_all_new_rows(self):
        """Upsert into an empty table – every row is new."""
        table = self._create_table()
        data = pa.Table.from_pydict({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Carol'],
            'age': [25, 30, 35],
            'city': ['NYC', 'LA', 'Chicago'],
        }, schema=self.pa_schema)

        self._upsert(table, data, upsert_keys=['id'])

        result = self._read_all(table)
        self.assertEqual(result.num_rows, 3)
        self.assertEqual(sorted(result['id'].to_pylist()), [1, 2, 3])

    def test_all_matched_rows(self):
        """Upsert where every row matches an existing key – pure update."""
        table = self._create_table()
        initial = pa.Table.from_pydict({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Carol'],
            'age': [25, 30, 35],
            'city': ['NYC', 'LA', 'Chicago'],
        }, schema=self.pa_schema)
        self._write(table, initial)

        update = pa.Table.from_pydict({
            'id': [1, 2, 3],
            'name': ['Alice2', 'Bob2', 'Carol2'],
            'age': [26, 31, 36],
            'city': ['NYC2', 'LA2', 'Chicago2'],
        }, schema=self.pa_schema)
        self._upsert(table, update, upsert_keys=['id'])

        result = self._read_all(table)
        self.assertEqual(result.num_rows, 3)
        names = sorted(zip(result['id'].to_pylist(), result['name'].to_pylist()))
        self.assertEqual(names, [(1, 'Alice2'), (2, 'Bob2'), (3, 'Carol2')])

    def test_mixed_update_and_append(self):
        """Upsert where some rows match and some are new."""
        table = self._create_table()
        initial = pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'age': [25, 30],
            'city': ['NYC', 'LA'],
        }, schema=self.pa_schema)
        self._write(table, initial)

        upsert_data = pa.Table.from_pydict({
            'id': [2, 3],
            'name': ['Bob_new', 'Carol'],
            'age': [31, 35],
            'city': ['LA_new', 'Chicago'],
        }, schema=self.pa_schema)
        self._upsert(table, upsert_data, upsert_keys=['id'])

        result = self._read_all(table)
        self.assertEqual(result.num_rows, 3)
        rows = sorted(
            zip(result['id'].to_pylist(), result['name'].to_pylist()),
            key=lambda x: x[0],
        )
        self.assertEqual(rows, [(1, 'Alice'), (2, 'Bob_new'), (3, 'Carol')])

    def test_upsert_preserves_untouched_columns(self):
        """Columns not listed in update_cols remain unchanged after upsert."""
        table = self._create_table()
        initial = pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'age': [25, 30],
            'city': ['NYC', 'LA'],
        }, schema=self.pa_schema)
        self._write(table, initial)

        # Only update 'age' for matched row (id=1)
        upsert_data = pa.Table.from_pydict({
            'id': [1],
            'name': ['_ignored_'],
            'age': [99],
            'city': ['_ignored_'],
        }, schema=self.pa_schema)
        self._upsert(table, upsert_data, upsert_keys=['id'], update_cols=['age'])

        result = self._read_all(table)
        row = {k: result[k].to_pylist() for k in result.column_names}
        idx = row['id'].index(1)
        self.assertEqual(row['age'][idx], 99)
        self.assertEqual(row['name'][idx], 'Alice')  # unchanged
        self.assertEqual(row['city'][idx], 'NYC')  # unchanged

    # ==================================================================
    # Composite key tests
    # ==================================================================

    def test_composite_key_upsert(self):
        """Upsert using two columns as composite key."""
        table = self._create_table()
        initial = pa.Table.from_pydict({
            'id': [1, 1, 2],
            'name': ['Alice', 'Alice', 'Bob'],
            'age': [25, 30, 35],
            'city': ['NYC', 'LA', 'Chicago'],
        }, schema=self.pa_schema)
        self._write(table, initial)

        # (id=1, name='Alice') matches two existing rows with different cities;
        # since keys contain (id, name) the match is unique per key column combo.
        # But our data has (1, 'Alice') twice in initial → validation should still
        # pass because we check input data, not table data.
        upsert_data = pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['Alice', 'Carol'],
            'age': [99, 40],
            'city': ['Updated', 'Dallas'],
        }, schema=self.pa_schema)
        self._upsert(table, upsert_data, upsert_keys=['id', 'name'])

        result = self._read_all(table)
        # 3 original + 1 new (2, Carol) = 4 rows; (1, Alice) matched the first occurrence
        self.assertEqual(result.num_rows, 4)
        ids = sorted(zip(
            result['id'].to_pylist(),
            result['name'].to_pylist(),
            result['city'].to_pylist(),
        ))
        # (1,'Alice','LA') row should remain; (1,'Alice','NYC')-keyed row updated; (2,'Bob') unchanged; (2,'Carol') new
        self.assertIn((2, 'Carol', 'Dallas'), ids)

    # ==================================================================
    # Sequential upsert tests
    # ==================================================================

    def test_sequential_upserts(self):
        """Two sequential upserts: second upsert sees results of the first."""
        table = self._create_table()
        self._write(table, pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'age': [25, 30],
            'city': ['NYC', 'LA'],
        }, schema=self.pa_schema))

        # First upsert: update id=1, add id=3
        self._upsert(table, pa.Table.from_pydict({
            'id': [1, 3],
            'name': ['Alice_v2', 'Carol'],
            'age': [26, 35],
            'city': ['NYC', 'Chicago'],
        }, schema=self.pa_schema), upsert_keys=['id'])

        # Second upsert: update id=3 (just inserted), add id=4
        self._upsert(table, pa.Table.from_pydict({
            'id': [3, 4],
            'name': ['Carol_v2', 'Dave'],
            'age': [36, 40],
            'city': ['Houston', 'Phoenix'],
        }, schema=self.pa_schema), upsert_keys=['id'])

        result = self._read_all(table)
        self.assertEqual(result.num_rows, 4)
        rows = {r: v for r, v in zip(result['id'].to_pylist(), result['name'].to_pylist())}
        self.assertEqual(rows[1], 'Alice_v2')
        self.assertEqual(rows[3], 'Carol_v2')
        self.assertEqual(rows[4], 'Dave')

    # ==================================================================
    # Partitioned table tests
    # ==================================================================

    def test_partitioned_table_upsert(self):
        """Upsert on a partitioned table with data in multiple partitions."""
        table = self._create_table(
            pa_schema=self.partitioned_pa_schema,
            partition_keys=['region'],
        )

        # Write initial data across two partitions
        initial = pa.Table.from_pydict({
            'id': [1, 2, 3, 4],
            'name': ['Alice', 'Bob', 'Carol', 'Dave'],
            'age': [25, 30, 35, 40],
            'region': ['US', 'US', 'EU', 'EU'],
        }, schema=self.partitioned_pa_schema)
        self._write(table, initial)

        # Upsert: update one row per partition, add one new
        upsert_data = pa.Table.from_pydict({
            'id': [1, 3, 5],
            'name': ['Alice_v2', 'Carol_v2', 'Eve'],
            'age': [26, 36, 45],
            'region': ['US', 'EU', 'EU'],
        }, schema=self.partitioned_pa_schema)
        # upsert_keys=['id'] only – partition key 'region' is auto-stripped
        self._upsert(table, upsert_data, upsert_keys=['id'])

        result = self._read_all(table)
        self.assertEqual(result.num_rows, 5)
        rows = sorted(zip(
            result['id'].to_pylist(),
            result['name'].to_pylist(),
            result['region'].to_pylist(),
        ))
        self.assertIn((1, 'Alice_v2', 'US'), rows)
        self.assertIn((2, 'Bob', 'US'), rows)
        self.assertIn((3, 'Carol_v2', 'EU'), rows)
        self.assertIn((4, 'Dave', 'EU'), rows)
        self.assertIn((5, 'Eve', 'EU'), rows)

    def test_partitioned_upsert_single_partition(self):
        """Upsert targeting only one partition does not affect the other."""
        table = self._create_table(
            pa_schema=self.partitioned_pa_schema,
            partition_keys=['region'],
        )

        initial = pa.Table.from_pydict({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Carol'],
            'age': [25, 30, 35],
            'region': ['US', 'US', 'EU'],
        }, schema=self.partitioned_pa_schema)
        self._write(table, initial)

        # Only touch US partition
        upsert_data = pa.Table.from_pydict({
            'id': [1, 4],
            'name': ['Alice_v2', 'Dave'],
            'age': [26, 40],
            'region': ['US', 'US'],
        }, schema=self.partitioned_pa_schema)
        self._upsert(table, upsert_data, upsert_keys=['id'])

        result = self._read_all(table)
        self.assertEqual(result.num_rows, 4)
        eu_rows = [
            (i, n) for i, n, r in zip(
                result['id'].to_pylist(),
                result['name'].to_pylist(),
                result['region'].to_pylist(),
            ) if r == 'EU'
        ]
        # EU partition unchanged
        self.assertEqual(eu_rows, [(3, 'Carol')])

    def test_partitioned_all_new_rows(self):
        """Upsert into an empty partitioned table – all new."""
        table = self._create_table(
            pa_schema=self.partitioned_pa_schema,
            partition_keys=['region'],
        )

        data = pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'age': [25, 30],
            'region': ['US', 'EU'],
        }, schema=self.partitioned_pa_schema)
        self._upsert(table, data, upsert_keys=['id'])

        result = self._read_all(table)
        self.assertEqual(result.num_rows, 2)

    # ==================================================================
    # Multiple data files
    # ==================================================================

    def test_upsert_across_multiple_data_files(self):
        """Upsert matches rows that live in different data files (commits)."""
        table = self._create_table()
        # First commit
        self._write(table, pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'age': [25, 30],
            'city': ['NYC', 'LA'],
        }, schema=self.pa_schema))
        # Second commit
        self._write(table, pa.Table.from_pydict({
            'id': [3, 4],
            'name': ['Carol', 'Dave'],
            'age': [35, 40],
            'city': ['Chicago', 'Houston'],
        }, schema=self.pa_schema))

        # Upsert hits both files: update id=2 (file1) and id=3 (file2), add id=5
        upsert_data = pa.Table.from_pydict({
            'id': [2, 3, 5],
            'name': ['Bob_v2', 'Carol_v2', 'Eve'],
            'age': [31, 36, 45],
            'city': ['LA2', 'Chicago2', 'Phoenix'],
        }, schema=self.pa_schema)
        self._upsert(table, upsert_data, upsert_keys=['id'])

        result = self._read_all(table)
        self.assertEqual(result.num_rows, 5)
        rows = {r: v for r, v in zip(result['id'].to_pylist(), result['name'].to_pylist())}
        self.assertEqual(rows[1], 'Alice')
        self.assertEqual(rows[2], 'Bob_v2')
        self.assertEqual(rows[3], 'Carol_v2')
        self.assertEqual(rows[4], 'Dave')
        self.assertEqual(rows[5], 'Eve')

    # ==================================================================
    # Validation / error tests
    # ==================================================================

    def test_empty_upsert_keys_raises(self):
        """Empty upsert_keys list should raise ValueError."""
        table = self._create_table()
        self._write(table, pa.Table.from_pydict({
            'id': [1], 'name': ['A'], 'age': [1], 'city': ['X'],
        }, schema=self.pa_schema))

        data = pa.Table.from_pydict({
            'id': [1], 'name': ['B'], 'age': [2], 'city': ['Y'],
        }, schema=self.pa_schema)

        with self.assertRaises(ValueError) as ctx:
            wb = table.new_batch_write_builder()
            tu = wb.new_update()
            tu.upsert_by_arrow_with_key(data, upsert_keys=[])
        self.assertIn('must not be empty', str(ctx.exception))

    def test_upsert_key_not_in_schema_raises(self):
        """upsert_key not present in table schema should raise ValueError."""
        table = self._create_table()
        self._write(table, pa.Table.from_pydict({
            'id': [1], 'name': ['A'], 'age': [1], 'city': ['X'],
        }, schema=self.pa_schema))

        data = pa.Table.from_pydict({
            'id': [1], 'name': ['B'], 'age': [2], 'city': ['Y'],
        }, schema=self.pa_schema)

        with self.assertRaises(ValueError) as ctx:
            wb = table.new_batch_write_builder()
            tu = wb.new_update()
            tu.upsert_by_arrow_with_key(data, upsert_keys=['nonexistent'])
        self.assertIn('not in table schema', str(ctx.exception))

    def test_upsert_key_not_in_data_raises(self):
        """upsert_key not present in input data should raise ValueError."""
        table = self._create_table()
        self._write(table, pa.Table.from_pydict({
            'id': [1], 'name': ['A'], 'age': [1], 'city': ['X'],
        }, schema=self.pa_schema))

        # Input data missing 'city' column but upsert_keys=['city']
        data = pa.Table.from_pydict({
            'id': [1], 'name': ['B'], 'age': [2],
        })

        with self.assertRaises(ValueError) as ctx:
            wb = table.new_batch_write_builder()
            tu = wb.new_update()
            tu.upsert_by_arrow_with_key(data, upsert_keys=['city'])
        self.assertIn('not in input data', str(ctx.exception))

    def test_empty_data_raises(self):
        """Empty input data should raise ValueError."""
        table = self._create_table()
        data = pa.Table.from_pydict({
            'id': pa.array([], type=pa.int32()),
            'name': pa.array([], type=pa.string()),
            'age': pa.array([], type=pa.int32()),
            'city': pa.array([], type=pa.string()),
        })

        with self.assertRaises(ValueError) as ctx:
            wb = table.new_batch_write_builder()
            tu = wb.new_update()
            tu.upsert_by_arrow_with_key(data, upsert_keys=['id'])
        self.assertIn('empty', str(ctx.exception))

    def test_duplicate_keys_in_input_raises(self):
        """Duplicate composite keys in input data should raise ValueError."""
        table = self._create_table()
        data = pa.Table.from_pydict({
            'id': [1, 1],
            'name': ['A', 'B'],
            'age': [25, 30],
            'city': ['NYC', 'LA'],
        }, schema=self.pa_schema)

        with self.assertRaises(ValueError) as ctx:
            wb = table.new_batch_write_builder()
            tu = wb.new_update()
            tu.upsert_by_arrow_with_key(data, upsert_keys=['id'])
        self.assertIn('duplicate', str(ctx.exception).lower())

    def test_partitioned_table_missing_partition_col_in_data_raises(self):
        """Input data missing partition column should raise ValueError."""
        table = self._create_table(
            pa_schema=self.partitioned_pa_schema,
            partition_keys=['region'],
        )

        # Input data does NOT include the 'region' partition column
        data = pa.Table.from_pydict({
            'id': [1], 'name': ['A'], 'age': [25],
        })

        with self.assertRaises(ValueError) as ctx:
            wb = table.new_batch_write_builder()
            tu = wb.new_update()
            tu.upsert_by_arrow_with_key(data, upsert_keys=['id'])
        self.assertIn('partition key', str(ctx.exception).lower())

    def test_same_key_in_different_partitions(self):
        """Same upsert key value in different partitions is valid."""
        table = self._create_table(
            pa_schema=self.partitioned_pa_schema,
            partition_keys=['region'],
        )

        initial = pa.Table.from_pydict({
            'id': [1, 1],
            'name': ['Alice_US', 'Alice_EU'],
            'age': [25, 30],
            'region': ['US', 'EU'],
        }, schema=self.partitioned_pa_schema)
        self._write(table, initial)

        # Upsert id=1 in both partitions (same key, different partitions)
        upsert_data = pa.Table.from_pydict({
            'id': [1, 1],
            'name': ['Alice_US_v2', 'Alice_EU_v2'],
            'age': [26, 31],
            'region': ['US', 'EU'],
        }, schema=self.partitioned_pa_schema)
        self._upsert(table, upsert_data, upsert_keys=['id'])

        result = self._read_all(table)
        self.assertEqual(result.num_rows, 2)
        rows = sorted(zip(
            result['id'].to_pylist(),
            result['name'].to_pylist(),
            result['region'].to_pylist(),
        ), key=lambda x: x[2])
        self.assertEqual(rows[0], (1, 'Alice_EU_v2', 'EU'))
        self.assertEqual(rows[1], (1, 'Alice_US_v2', 'US'))

    def test_invalid_update_cols_raises(self):
        """update_cols referencing non-existent column should raise."""
        table = self._create_table()
        data = pa.Table.from_pydict({
            'id': [1], 'name': ['A'], 'age': [25], 'city': ['NYC'],
        }, schema=self.pa_schema)

        with self.assertRaises(ValueError):
            wb = table.new_batch_write_builder()
            tu = wb.new_update().with_update_type(['nonexistent_col'])
            tu.upsert_by_arrow_with_key(data, upsert_keys=['id'])

    # ==================================================================
    # update_cols (partial column update) tests
    # ==================================================================

    def test_update_cols_partial_update(self):
        """update_cols limits which columns are updated for matched rows."""
        table = self._create_table()
        initial = pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'age': [25, 30],
            'city': ['NYC', 'LA'],
        }, schema=self.pa_schema)
        self._write(table, initial)

        upsert_data = pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['_X_', '_Y_'],
            'age': [99, 88],
            'city': ['_X_', '_Y_'],
        }, schema=self.pa_schema)
        # Only update 'age'
        self._upsert(table, upsert_data, upsert_keys=['id'], update_cols=['age'])

        result = self._read_all(table)
        rows = sorted(zip(
            result['id'].to_pylist(),
            result['name'].to_pylist(),
            result['age'].to_pylist(),
            result['city'].to_pylist(),
        ))
        # name and city should be unchanged; age updated
        self.assertEqual(rows[0], (1, 'Alice', 99, 'NYC'))
        self.assertEqual(rows[1], (2, 'Bob', 88, 'LA'))

    # ==================================================================
    # Large data test
    # ==================================================================

    def test_large_table_upsert(self):
        """Upsert on a table with many rows spread across multiple commits."""
        table = self._create_table()
        n = 200

        # Write 200 rows in 2 commits
        self._write(table, pa.Table.from_pydict({
            'id': list(range(n // 2)),
            'name': [f'Name_{i}' for i in range(n // 2)],
            'age': [20 + i for i in range(n // 2)],
            'city': [f'City_{i}' for i in range(n // 2)],
        }, schema=self.pa_schema))
        self._write(table, pa.Table.from_pydict({
            'id': list(range(n // 2, n)),
            'name': [f'Name_{i}' for i in range(n // 2, n)],
            'age': [20 + i for i in range(n // 2, n)],
            'city': [f'City_{i}' for i in range(n // 2, n)],
        }, schema=self.pa_schema))

        # Upsert: update every 10th row, add 10 new rows
        update_ids = list(range(0, n, 10))
        new_ids = list(range(n, n + 10))
        all_ids = update_ids + new_ids

        upsert_data = pa.Table.from_pydict({
            'id': all_ids,
            'name': [f'Upserted_{i}' for i in all_ids],
            'age': [1000 + i for i in all_ids],
            'city': [f'UCity_{i}' for i in all_ids],
        }, schema=self.pa_schema)
        self._upsert(table, upsert_data, upsert_keys=['id'])

        result = self._read_all(table)
        self.assertEqual(result.num_rows, n + 10)

        result_map = {
            rid: rname
            for rid, rname in zip(result['id'].to_pylist(), result['name'].to_pylist())
        }
        # Verify updated rows
        for uid in update_ids:
            self.assertEqual(result_map[uid], f'Upserted_{uid}')
        # Verify new rows
        for nid in new_ids:
            self.assertEqual(result_map[nid], f'Upserted_{nid}')
        # Verify untouched rows
        for i in range(n):
            if i not in update_ids:
                self.assertEqual(result_map[i], f'Name_{i}')

    def test_non_data_evolution_table_raises(self):
        """Upsert on a table without data-evolution enabled should raise."""
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
        ])
        name = self._unique_name()
        # No data-evolution option
        schema = Schema.from_pyarrow_schema(pa_schema)
        self.catalog.create_table(name, schema, False)
        table = self.catalog.get_table(name)

        data = pa.Table.from_pydict({'id': [1], 'name': ['A']}, schema=pa_schema)
        with self.assertRaises(ValueError) as ctx:
            wb = table.new_batch_write_builder()
            tu = wb.new_update()
            tu.upsert_by_arrow_with_key(data, upsert_keys=['id'])
        self.assertIn('data-evolution.enabled', str(ctx.exception))

    def test_partitioned_update_cols_with_new_rows(self):
        """When update_cols is set, matched rows update only those columns,
        but new rows are appended with ALL columns via the standard write path."""
        table = self._create_table(
            pa_schema=self.partitioned_pa_schema,
            partition_keys=['region'],
        )

        initial = pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'age': [25, 30],
            'region': ['US', 'EU'],
        }, schema=self.partitioned_pa_schema)
        self._write(table, initial)

        # Upsert with update_cols=['age'] -- the partition key 'region' is NOT
        # in update_cols.  id=1 matches (update), id=3 is new (append).
        upsert_data = pa.Table.from_pydict({
            'id': [1, 3],
            'name': ['Alice_v2', 'Carol'],
            'age': [99, 50],
            'region': ['US', 'US'],
        }, schema=self.partitioned_pa_schema)
        self._upsert(table, upsert_data, upsert_keys=['id'],
                     update_cols=['age'])

        result = self._read_all(table)
        # 2 original + 1 new = 3
        self.assertEqual(result.num_rows, 3)

        ids = result['id'].to_pylist()
        names = result['name'].to_pylist()
        ages = result['age'].to_pylist()
        regions = result['region'].to_pylist()

        # id=1 was updated: only age changed
        idx1 = ids.index(1)
        self.assertEqual(ages[idx1], 99)
        self.assertEqual(names[idx1], 'Alice')  # unchanged
        self.assertEqual(regions[idx1], 'US')

        # id=2 was untouched
        idx2 = ids.index(2)
        self.assertEqual(ages[idx2], 30)
        self.assertEqual(regions[idx2], 'EU')

        # id=3 is new — all columns written (not partial)
        idx3 = ids.index(3)
        self.assertEqual(ages[idx3], 50)
        self.assertEqual(names[idx3], 'Carol')
        self.assertEqual(regions[idx3], 'US')


if __name__ == '__main__':
    unittest.main()
