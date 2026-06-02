# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import unittest

import pyarrow as pa

from pypaimon.tests.data_evolution_test_helpers import (
    BatchModeMixin,
    DataEvolutionTestBase,
    StreamModeMixin,
)


# ======================================================================
# Shared base for batch & stream upsert-by-key tests
# ======================================================================

class _TableUpsertByKeyTestBase(DataEvolutionTestBase):
    """Shared tests for ``TableUpdate.upsert_by_arrow_with_key``.

    Concrete subclasses must inherit from :class:`unittest.TestCase` AND one
    of :class:`_BatchModeMixin` / :class:`_StreamModeMixin`, which add the
    operation-specific primitive ``_apply_upsert(tu, data, upsert_keys, cid)``
    on top of the framework primitives provided by
    :class:`DataEvolutionTestBase`/:class:`BatchModeMixin`/:class:`StreamModeMixin`.
    """

    # Partitioned variant of the base schema (city → region).
    partitioned_pa_schema = pa.schema([
        ('id', pa.int32()),
        ('name', pa.string()),
        ('age', pa.int32()),
        ('region', pa.string()),
    ])

    # ------------------------------------------------------------------
    # Operation-specific primitive (overridden by mixins)
    # ------------------------------------------------------------------

    def _apply_upsert(self, table_update, data, upsert_keys, cid):
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Helpers built on the primitives
    # ------------------------------------------------------------------

    def _upsert(self, table, data, upsert_keys, update_cols=None):
        """End-to-end upsert + commit."""
        wb = self._make_write_builder(table)
        tu = wb.new_update()
        if update_cols:
            tu.with_update_type(update_cols)
        cid = self._next_commit_id()
        msgs = self._apply_upsert(tu, data, upsert_keys, cid)
        tc = wb.new_commit()
        self._apply_commit(tc, msgs, cid)
        tc.close()
        return msgs

    # ==================================================================
    # Basic upsert tests (non-partitioned)
    # ==================================================================

    def test_all_new_rows(self):
        """Upsert into an empty table – every row is appended."""
        table = self._create_table()
        data = pa.Table.from_pydict({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Carol'],
            'age': [25, 30, 35],
            'city': ['NYC', 'LA', 'Chicago'],
        }, schema=self.pa_schema)

        self._upsert(table, data, upsert_keys=['id'])

        result = self._read_all(table)
        self.assertEqual(3, result.num_rows)
        self.assertEqual([1, 2, 3], sorted(result['id'].to_pylist()))

    def test_all_matched_rows(self):
        """Upsert where every row matches existing keys – pure update path."""
        table = self._create_table()
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Carol'],
            'age': [25, 30, 35],
            'city': ['NYC', 'LA', 'Chicago'],
        }, schema=self.pa_schema))

        self._upsert(table, pa.Table.from_pydict({
            'id': [1, 2, 3],
            'name': ['Alice2', 'Bob2', 'Carol2'],
            'age': [26, 31, 36],
            'city': ['NYC2', 'LA2', 'Chicago2'],
        }, schema=self.pa_schema), upsert_keys=['id'])

        result = self._read_all(table)
        self.assertEqual(3, result.num_rows)
        names = sorted(zip(result['id'].to_pylist(), result['name'].to_pylist()))
        self.assertEqual([(1, 'Alice2'), (2, 'Bob2'), (3, 'Carol2')], names)

    def test_mixed_update_and_append(self):
        """Upsert with some matched + some new rows."""
        table = self._create_table()
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'age': [25, 30],
            'city': ['NYC', 'LA'],
        }, schema=self.pa_schema))

        self._upsert(table, pa.Table.from_pydict({
            'id': [2, 3],
            'name': ['Bob_new', 'Carol'],
            'age': [31, 35],
            'city': ['LA_new', 'Chicago'],
        }, schema=self.pa_schema), upsert_keys=['id'])

        result = self._read_all(table)
        self.assertEqual(3, result.num_rows)
        rows = sorted(
            zip(result['id'].to_pylist(), result['name'].to_pylist()),
            key=lambda x: x[0],
        )
        self.assertEqual([(1, 'Alice'), (2, 'Bob_new'), (3, 'Carol')], rows)

    def test_composite_key_upsert(self):
        """Upsert with a multi-column composite key."""
        table = self._create_table()
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1, 1, 2],
            'name': ['Alice', 'Alice', 'Bob'],
            'age': [25, 30, 35],
            'city': ['NYC', 'LA', 'Chicago'],
        }, schema=self.pa_schema))

        # (id, name) = (1, Alice) appears twice in the table → matches the
        # first occurrence; (2, Carol) is new.
        self._upsert(table, pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['Alice', 'Carol'],
            'age': [99, 40],
            'city': ['Updated', 'Dallas'],
        }, schema=self.pa_schema), upsert_keys=['id', 'name'])

        result = self._read_all(table)
        self.assertEqual(4, result.num_rows)
        rows = sorted(zip(
            result['id'].to_pylist(),
            result['name'].to_pylist(),
            result['city'].to_pylist(),
        ))
        self.assertIn((2, 'Carol', 'Dallas'), rows)

    def test_sequential_upserts(self):
        """A second upsert sees the rows inserted by the first."""
        table = self._create_table()
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'age': [25, 30],
            'city': ['NYC', 'LA'],
        }, schema=self.pa_schema))

        # First upsert: update id=1, insert id=3
        self._upsert(table, pa.Table.from_pydict({
            'id': [1, 3],
            'name': ['Alice_v2', 'Carol'],
            'age': [26, 35],
            'city': ['NYC', 'Chicago'],
        }, schema=self.pa_schema), upsert_keys=['id'])

        # Second upsert: update id=3 (just inserted), insert id=4
        self._upsert(table, pa.Table.from_pydict({
            'id': [3, 4],
            'name': ['Carol_v2', 'Dave'],
            'age': [36, 40],
            'city': ['Houston', 'Phoenix'],
        }, schema=self.pa_schema), upsert_keys=['id'])

        result = self._read_all(table)
        self.assertEqual(4, result.num_rows)
        rows = {r: v for r, v in zip(
            result['id'].to_pylist(), result['name'].to_pylist()
        )}
        self.assertEqual('Alice_v2', rows[1])
        self.assertEqual('Carol_v2', rows[3])
        self.assertEqual('Dave',     rows[4])

    def test_upsert_across_multiple_data_files(self):
        """Upsert hits rows that live in different snapshots/files."""
        table = self._create_table()
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'age': [25, 30],
            'city': ['NYC', 'LA'],
        }, schema=self.pa_schema))
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [3, 4],
            'name': ['Carol', 'Dave'],
            'age': [35, 40],
            'city': ['Chicago', 'Houston'],
        }, schema=self.pa_schema))

        self._upsert(table, pa.Table.from_pydict({
            'id': [2, 3, 5],
            'name': ['Bob_v2', 'Carol_v2', 'Eve'],
            'age': [31, 36, 45],
            'city': ['LA2', 'Chicago2', 'Phoenix'],
        }, schema=self.pa_schema), upsert_keys=['id'])

        result = self._read_all(table)
        self.assertEqual(5, result.num_rows)
        rows = {r: v for r, v in zip(
            result['id'].to_pylist(), result['name'].to_pylist()
        )}
        self.assertEqual('Alice',    rows[1])
        self.assertEqual('Bob_v2',   rows[2])
        self.assertEqual('Carol_v2', rows[3])
        self.assertEqual('Dave',     rows[4])
        self.assertEqual('Eve',      rows[5])

    def test_large_table_upsert(self):
        """Upsert that touches a wide selection of rows in a 200-row table."""
        table = self._create_table()
        n = 200

        for half in (0, 1):
            rng = range(half * n // 2, (half + 1) * n // 2)
            self._write_arrow(table, pa.Table.from_pydict({
                'id':   list(rng),
                'name': [f'Name_{i}' for i in rng],
                'age':  [20 + i for i in rng],
                'city': [f'City_{i}' for i in rng],
            }, schema=self.pa_schema))

        update_ids = list(range(0, n, 10))
        new_ids = list(range(n, n + 10))
        all_ids = update_ids + new_ids

        self._upsert(table, pa.Table.from_pydict({
            'id':   all_ids,
            'name': [f'Upserted_{i}' for i in all_ids],
            'age':  [1000 + i for i in all_ids],
            'city': [f'UCity_{i}' for i in all_ids],
        }, schema=self.pa_schema), upsert_keys=['id'])

        result = self._read_all(table)
        self.assertEqual(n + 10, result.num_rows)

        m = {r: nm for r, nm in zip(
            result['id'].to_pylist(), result['name'].to_pylist()
        )}
        for uid in update_ids:
            self.assertEqual(f'Upserted_{uid}', m[uid])
        for nid in new_ids:
            self.assertEqual(f'Upserted_{nid}', m[nid])
        for i in range(n):
            if i not in update_ids:
                self.assertEqual(f'Name_{i}', m[i])

    # ==================================================================
    # Partitioned table tests
    # ==================================================================

    def test_partitioned_table_upsert(self):
        """Upsert touching multiple partitions: match in each + 1 new row."""
        table = self._create_table(
            pa_schema=self.partitioned_pa_schema,
            partition_keys=['region'],
        )
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1, 2, 3, 4],
            'name': ['Alice', 'Bob', 'Carol', 'Dave'],
            'age': [25, 30, 35, 40],
            'region': ['US', 'US', 'EU', 'EU'],
        }, schema=self.partitioned_pa_schema))

        # Partition key 'region' is auto-stripped from upsert keys
        self._upsert(table, pa.Table.from_pydict({
            'id': [1, 3, 5],
            'name': ['Alice_v2', 'Carol_v2', 'Eve'],
            'age': [26, 36, 45],
            'region': ['US', 'EU', 'EU'],
        }, schema=self.partitioned_pa_schema), upsert_keys=['id'])

        result = self._read_all(table)
        self.assertEqual(5, result.num_rows)
        rows = sorted(zip(
            result['id'].to_pylist(),
            result['name'].to_pylist(),
            result['region'].to_pylist(),
        ))
        for expected in [(1, 'Alice_v2', 'US'), (2, 'Bob', 'US'),
                         (3, 'Carol_v2', 'EU'), (4, 'Dave', 'EU'),
                         (5, 'Eve', 'EU')]:
            self.assertIn(expected, rows)

    def test_partitioned_upsert_single_partition_leaves_others_unchanged(self):
        """Touching only one partition does not affect any other partition."""
        table = self._create_table(
            pa_schema=self.partitioned_pa_schema,
            partition_keys=['region'],
        )
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Carol'],
            'age': [25, 30, 35],
            'region': ['US', 'US', 'EU'],
        }, schema=self.partitioned_pa_schema))

        self._upsert(table, pa.Table.from_pydict({
            'id': [1, 4],
            'name': ['Alice_v2', 'Dave'],
            'age': [26, 40],
            'region': ['US', 'US'],
        }, schema=self.partitioned_pa_schema), upsert_keys=['id'])

        result = self._read_all(table)
        self.assertEqual(4, result.num_rows)
        eu_rows = [
            (i, n) for i, n, r in zip(
                result['id'].to_pylist(),
                result['name'].to_pylist(),
                result['region'].to_pylist(),
            ) if r == 'EU'
        ]
        self.assertEqual([(3, 'Carol')], eu_rows)

    def test_partitioned_all_new_rows(self):
        """Upsert into an empty partitioned table – pure append per partition."""
        table = self._create_table(
            pa_schema=self.partitioned_pa_schema,
            partition_keys=['region'],
        )
        self._upsert(table, pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'age': [25, 30],
            'region': ['US', 'EU'],
        }, schema=self.partitioned_pa_schema), upsert_keys=['id'])

        self.assertEqual(2, self._read_all(table).num_rows)

    def test_same_key_in_different_partitions(self):
        """The same upsert-key value in different partitions stays distinct."""
        table = self._create_table(
            pa_schema=self.partitioned_pa_schema,
            partition_keys=['region'],
        )
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1, 1],
            'name': ['Alice_US', 'Alice_EU'],
            'age': [25, 30],
            'region': ['US', 'EU'],
        }, schema=self.partitioned_pa_schema))

        self._upsert(table, pa.Table.from_pydict({
            'id': [1, 1],
            'name': ['Alice_US_v2', 'Alice_EU_v2'],
            'age': [26, 31],
            'region': ['US', 'EU'],
        }, schema=self.partitioned_pa_schema), upsert_keys=['id'])

        result = self._read_all(table)
        self.assertEqual(2, result.num_rows)
        rows = sorted(zip(
            result['id'].to_pylist(),
            result['name'].to_pylist(),
            result['region'].to_pylist(),
        ), key=lambda x: x[2])
        self.assertEqual((1, 'Alice_EU_v2', 'EU'), rows[0])
        self.assertEqual((1, 'Alice_US_v2', 'US'), rows[1])

    def test_partitioned_update_cols_with_new_rows(self):
        """``update_cols`` only constrains matched rows; new rows still get
        every column written via the regular write path."""
        table = self._create_table(
            pa_schema=self.partitioned_pa_schema,
            partition_keys=['region'],
        )
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'age': [25, 30],
            'region': ['US', 'EU'],
        }, schema=self.partitioned_pa_schema))

        upsert_data = pa.Table.from_pydict({
            'id': [1, 3],
            'name': ['Alice_v2', 'Carol'],
            'age': [99, 50],
            'region': ['US', 'US'],
        }, schema=self.partitioned_pa_schema)
        self._upsert(table, upsert_data,
                     upsert_keys=['id'], update_cols=['age'])

        result = self._read_all(table)
        self.assertEqual(3, result.num_rows)

        ids = result['id'].to_pylist()
        names = result['name'].to_pylist()
        ages = result['age'].to_pylist()
        regions = result['region'].to_pylist()

        # id=1 was matched: only 'age' should change
        idx1 = ids.index(1)
        self.assertEqual(99,      ages[idx1])
        self.assertEqual('Alice', names[idx1])
        self.assertEqual('US',    regions[idx1])

        # id=2 untouched
        idx2 = ids.index(2)
        self.assertEqual(30,   ages[idx2])
        self.assertEqual('EU', regions[idx2])

        # id=3 is new — all columns from the input land in the table
        idx3 = ids.index(3)
        self.assertEqual(50,      ages[idx3])
        self.assertEqual('Carol', names[idx3])
        self.assertEqual('US',    regions[idx3])

    # ==================================================================
    # update_cols partial update (non-partitioned)
    # ==================================================================

    def test_update_cols_partial_update(self):
        """``update_cols`` limits which columns are touched for matched rows."""
        table = self._create_table()
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'age': [25, 30],
            'city': ['NYC', 'LA'],
        }, schema=self.pa_schema))

        self._upsert(table, pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['_X_', '_Y_'],
            'age': [99, 88],
            'city': ['_X_', '_Y_'],
        }, schema=self.pa_schema), upsert_keys=['id'], update_cols=['age'])

        result = self._read_all(table)
        rows = sorted(zip(
            result['id'].to_pylist(),
            result['name'].to_pylist(),
            result['age'].to_pylist(),
            result['city'].to_pylist(),
        ))
        self.assertEqual((1, 'Alice', 99, 'NYC'), rows[0])
        self.assertEqual((2, 'Bob',   88, 'LA'),  rows[1])

    # ==================================================================
    # Duplicate-key dedup tests — parametrised
    # ==================================================================

    def test_duplicate_keys_in_input_keeps_last(self):
        """Duplicate keys in input always keep the *last* occurrence."""
        cases = [
            ('empty_table_non_partitioned', False, {
                'data': pa.Table.from_pydict({
                    'id': [1, 1, 1, 2],
                    'name': ['A1', 'A2', 'A3', 'B'],
                    'age': [10, 20, 30, 40],
                    'city': ['X1', 'X2', 'X3', 'Y'],
                }, schema=self.pa_schema),
                'expected_rows': {
                    1: ('A3', 30, 'X3'),
                    2: ('B',  40, 'Y'),
                },
                'expected_num_rows': 2,
            }),
            ('with_existing_rows_non_partitioned', True, {
                'data': pa.Table.from_pydict({
                    'id': [1, 1],
                    'name': ['A_first', 'A_last'],
                    'age': [90, 91],
                    'city': ['X', 'Y'],
                }, schema=self.pa_schema),
                'expected_rows': {
                    1: ('A_last', 91, 'Y'),
                    2: ('Bob',    30, 'LA'),   # untouched
                },
                'expected_num_rows': 2,
            }),
        ]

        for name, prefill, spec in cases:
            with self.subTest(case=name):
                table = self._create_table()
                if prefill:
                    self._write_arrow(table, pa.Table.from_pydict({
                        'id': [1, 2],
                        'name': ['Alice', 'Bob'],
                        'age': [25, 30],
                        'city': ['NYC', 'LA'],
                    }, schema=self.pa_schema))

                self._upsert(table, spec['data'], upsert_keys=['id'])
                result = self._read_all(table)
                self.assertEqual(spec['expected_num_rows'], result.num_rows)
                actual = {r: (n, a, c) for r, n, a, c in zip(
                    result['id'].to_pylist(),
                    result['name'].to_pylist(),
                    result['age'].to_pylist(),
                    result['city'].to_pylist(),
                )}
                for k, v in spec['expected_rows'].items():
                    self.assertEqual(v, actual[k])

    def test_duplicate_keys_in_input_partitioned_keeps_last(self):
        """Duplicate keys in a partitioned table keep the last *per partition*."""
        table = self._create_table(
            pa_schema=self.partitioned_pa_schema,
            partition_keys=['region'],
        )
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'age': [25, 30],
            'region': ['US', 'EU'],
        }, schema=self.partitioned_pa_schema))

        self._upsert(table, pa.Table.from_pydict({
            'id': [1, 1, 2, 2],
            'name': ['A_first', 'A_last', 'B_first', 'B_last'],
            'age': [50, 51, 60, 61],
            'region': ['US', 'US', 'EU', 'EU'],
        }, schema=self.partitioned_pa_schema), upsert_keys=['id'])

        result = self._read_all(table)
        self.assertEqual(2, result.num_rows)
        rows = {(r, reg): (n, a) for r, n, a, reg in zip(
            result['id'].to_pylist(),
            result['name'].to_pylist(),
            result['age'].to_pylist(),
            result['region'].to_pylist(),
        )}
        self.assertEqual(('A_last', 51), rows[(1, 'US')])
        self.assertEqual(('B_last', 61), rows[(2, 'EU')])

    # ==================================================================
    # Validation tests
    # ==================================================================

    def _seed_simple_table(self):
        """A 1-row non-partitioned table used by most validation tests."""
        table = self._create_table()
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1], 'name': ['A'], 'age': [1], 'city': ['X'],
        }, schema=self.pa_schema))
        return table

    def _expect_upsert_value_error(self, table, data, upsert_keys,
                                   update_cols=None, expected_substring=''):
        """Build update + invoke upsert (no commit) and assert it raises."""
        with self.assertRaises(ValueError) as ctx:
            wb = self._make_write_builder(table)
            tu = wb.new_update()
            if update_cols:
                tu.with_update_type(update_cols)
            self._apply_upsert(tu, data, upsert_keys, self._next_commit_id())
        if expected_substring:
            self.assertIn(expected_substring, str(ctx.exception))

    def test_empty_upsert_keys_raises(self):
        table = self._seed_simple_table()
        data = pa.Table.from_pydict({
            'id': [1], 'name': ['B'], 'age': [2], 'city': ['Y'],
        }, schema=self.pa_schema)
        self._expect_upsert_value_error(
            table, data, upsert_keys=[], expected_substring='must not be empty'
        )

    def test_upsert_key_not_in_schema_raises(self):
        table = self._seed_simple_table()
        data = pa.Table.from_pydict({
            'id': [1], 'name': ['B'], 'age': [2], 'city': ['Y'],
        }, schema=self.pa_schema)
        self._expect_upsert_value_error(
            table, data, upsert_keys=['nonexistent'],
            expected_substring='not in table schema',
        )

    def test_upsert_key_not_in_data_raises(self):
        table = self._seed_simple_table()
        # 'city' is missing from the input
        data = pa.Table.from_pydict({
            'id': [1], 'name': ['B'], 'age': [2],
        })
        self._expect_upsert_value_error(
            table, data, upsert_keys=['city'],
            expected_substring='not in input data',
        )

    def test_empty_data_raises(self):
        table = self._create_table()
        empty = pa.Table.from_pydict({
            'id':   pa.array([], type=pa.int32()),
            'name': pa.array([], type=pa.string()),
            'age':  pa.array([], type=pa.int32()),
            'city': pa.array([], type=pa.string()),
        })
        self._expect_upsert_value_error(
            table, empty, upsert_keys=['id'], expected_substring='empty'
        )

    def test_invalid_update_cols_raises(self):
        table = self._create_table()
        # ``with_update_type`` raises eagerly before upsert is called
        with self.assertRaises(ValueError):
            wb = self._make_write_builder(table)
            wb.new_update().with_update_type(['nonexistent_col'])

    def test_partitioned_missing_partition_col_in_data_raises(self):
        table = self._create_table(
            pa_schema=self.partitioned_pa_schema,
            partition_keys=['region'],
        )
        # Input data does NOT contain the 'region' partition column
        data = pa.Table.from_pydict({
            'id': [1], 'name': ['A'], 'age': [25],
        })
        with self.assertRaises(ValueError) as ctx:
            wb = self._make_write_builder(table)
            tu = wb.new_update()
            self._apply_upsert(tu, data, ['id'], self._next_commit_id())
        self.assertIn('partition key', str(ctx.exception).lower())

    def test_non_data_evolution_table_raises(self):
        """Upsert on a table without data-evolution enabled is rejected."""
        plain = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
        ])
        # No data-evolution / row-tracking options
        table = self._create_table(pa_schema=plain, options={})
        data = pa.Table.from_pydict({'id': [1], 'name': ['A']}, schema=plain)
        with self.assertRaises(ValueError) as ctx:
            wb = self._make_write_builder(table)
            tu = wb.new_update()
            self._apply_upsert(tu, data, ['id'], self._next_commit_id())
        self.assertIn('data-evolution.enabled', str(ctx.exception))


# ======================================================================
# Mode-specific mixins (add the ``upsert_by_arrow_with_key`` primitive)
# ======================================================================

class _BatchModeMixin(BatchModeMixin):
    def _apply_upsert(self, table_update, data, upsert_keys, cid):
        return table_update.upsert_by_arrow_with_key(data, upsert_keys)


class _StreamModeMixin(StreamModeMixin):
    def _apply_upsert(self, table_update, data, upsert_keys, cid):
        return table_update.upsert_by_arrow_with_key(data, upsert_keys, cid)


# ======================================================================
# Concrete test classes
# ======================================================================

class TableUpsertByKeyBatchTest(
    _BatchModeMixin, _TableUpsertByKeyTestBase, unittest.TestCase
):
    """All shared upsert tests under batch (``BatchWriteBuilder``) semantics."""


class TableUpsertByKeyStreamTest(
    _StreamModeMixin, _TableUpsertByKeyTestBase, unittest.TestCase
):
    """All shared upsert tests under stream (``StreamWriteBuilder``) semantics,
    plus stream-only multi-commit scenarios."""

    # ------------------------------------------------------------------
    # Stream-only helpers
    # ------------------------------------------------------------------

    def _stream_commit_upserts_by_key(
            self, tu, tc, commit_ids, tables, upsert_keys):
        """One upsert + commit per ``(cid, arrow_table)`` pair.

        Reuses the same ``StreamTableUpdate`` instance ``tu`` across commits.
        """
        for cid, data in zip(commit_ids, tables):
            msgs = tu.upsert_by_arrow_with_key(data, upsert_keys, cid)
            tc.commit(msgs, cid)

    # ------------------------------------------------------------------
    # Stream-only tests
    # ------------------------------------------------------------------

    def test_stream_multi_upsert_on_one_write_builder(self):
        """A single ``StreamWriteBuilder`` drives many upsert+commit cycles
        with reused ``tu`` and ``tc`` instances. Each commit produces its own
        snapshot tagged with the caller-supplied ``commit_identifier`` under
        a stable ``commit_user`` — the core contract distinguishing stream
        from batch mode.

        Parameterised over both contiguous and sparse identifier sequences
        to catch any accidental coupling between ``commit_identifier`` and
        ``snapshot_id`` ordering.
        """
        upserts = [
            # First upsert: all new
            pa.Table.from_pydict({
                'id': [1, 2],
                'name': ['Alice', 'Bob'],
                'age': [25, 30],
                'city': ['NYC', 'LA'],
            }, schema=self.pa_schema),
            # Second upsert: update id=1 + append id=3
            pa.Table.from_pydict({
                'id': [1, 3],
                'name': ['Alice_v2', 'Carol'],
                'age': [26, 35],
                'city': ['NYC2', 'Chicago'],
            }, schema=self.pa_schema),
        ]
        expected_by_id = {
            1: ('Alice_v2', 26, 'NYC2'),
            2: ('Bob', 30, 'LA'),
            3: ('Carol', 35, 'Chicago'),
        }

        for case_name, commit_ids in [
            ('contiguous', [1, 2]),
            ('sparse',     [42, 1000]),
        ]:
            with self.subTest(case=case_name):
                table = self._create_table()
                wb, tc, base_snapshot_id = self._stream_commit_session(table)
                tu = wb.new_update()
                self._stream_commit_upserts_by_key(
                    tu, tc, commit_ids, upserts, ['id'],
                )
                tc.close()

                result = self._read_all(table)
                self.assertEqual(3, result.num_rows)
                actual = {
                    i: (n, a, c) for i, n, a, c in zip(
                        result['id'].to_pylist(),
                        result['name'].to_pylist(),
                        result['age'].to_pylist(),
                        result['city'].to_pylist(),
                    )
                }
                self.assertEqual(expected_by_id, actual)
                self._assert_stream_builder_snapshots(
                    table, wb, base_snapshot_id, commit_ids,
                )

    def test_stream_commit_same_tc_different_update_projections(self):
        """``StreamTableCommit`` may carry commits from distinct
        :class:`StreamTableUpdate` instances — e.g. full upsert followed by a
        projection-limited upsert on the same ``tc``.
        """
        table = self._create_table()
        wb, tc, base_snapshot_id = self._stream_commit_session(table)

        tu1 = wb.new_update()
        msgs1 = tu1.upsert_by_arrow_with_key(
            pa.Table.from_pydict({
                'id': [1],
                'name': ['Alice'],
                'age': [25],
                'city': ['NYC'],
            }, schema=self.pa_schema),
            ['id'],
            1,
        )
        tc.commit(msgs1, 1)

        tu2 = wb.new_update().with_update_type(['city'])
        msgs2 = tu2.upsert_by_arrow_with_key(
            pa.Table.from_pydict({
                'id': [1],
                'name': ['ShouldIgnore'],
                'age': [99],
                'city': ['Boston'],
            }, schema=self.pa_schema),
            ['id'],
            2,
        )
        tc.commit(msgs2, 2)
        tc.close()

        result = self._read_all(table)
        self.assertEqual(1, result.num_rows)
        self.assertEqual(1, result['id'].to_pylist()[0])
        self.assertEqual('Alice', result['name'].to_pylist()[0])
        self.assertEqual(25, result['age'].to_pylist()[0])
        self.assertEqual('Boston', result['city'].to_pylist()[0])
        self._assert_stream_builder_snapshots(
            table, wb, base_snapshot_id, [1, 2],
        )

    def test_stream_interleaved_write_and_upsert_on_same_builder(self):
        """A single stream builder may perform an initial write followed by
        an upsert on the same ``tc``, each tagged with its own
        ``commit_identifier`` that propagates to its own snapshot.
        """
        table = self._create_table()
        wb, tc, base_snapshot_id = self._stream_commit_session(table)
        tw = wb.new_write()
        tu = wb.new_update()

        # Phase 1: initial write
        tw.write_arrow(pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'age': [25, 30],
            'city': ['NYC', 'LA'],
        }, schema=self.pa_schema))
        tc.commit(tw.prepare_commit(1), 1)
        tw.close()

        # Phase 2: upsert overlapping + new rows
        msgs = tu.upsert_by_arrow_with_key(
            pa.Table.from_pydict({
                'id': [2, 3],
                'name': ['Bob_v2', 'Carol'],
                'age': [31, 35],
                'city': ['LA2', 'Chicago'],
            }, schema=self.pa_schema),
            ['id'],
            2,
        )
        tc.commit(msgs, 2)
        tc.close()

        result = self._read_all(table)
        self.assertEqual(3, result.num_rows)
        actual = {
            i: (n, a, c) for i, n, a, c in zip(
                result['id'].to_pylist(),
                result['name'].to_pylist(),
                result['age'].to_pylist(),
                result['city'].to_pylist(),
            )
        }
        self.assertEqual({
            1: ('Alice', 25, 'NYC'),
            2: ('Bob_v2', 31, 'LA2'),
            3: ('Carol', 35, 'Chicago'),
        }, actual)
        self._assert_stream_builder_snapshots(
            table, wb, base_snapshot_id, [1, 2],
        )


if __name__ == '__main__':
    unittest.main()
