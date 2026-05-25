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

import random
import string
import threading
import unittest

import pyarrow as pa

from pypaimon.tests.data_evolution_test_helpers import (
    BatchModeMixin,
    DataEvolutionTestBase,
    StreamModeMixin,
)


# ======================================================================
# Shared base for batch & stream table-update tests
# ======================================================================

class _TableUpdateTestBase(DataEvolutionTestBase):
    """Shared tests for ``TableUpdate.update_by_arrow_with_row_id``.

    Concrete subclasses must inherit from :class:`unittest.TestCase` AND one
    of :class:`_BatchModeMixin` / :class:`_StreamModeMixin`, which add the
    operation-specific primitive ``_apply_update(tu, data, cid)`` on top of
    the framework primitives provided by
    :class:`DataEvolutionTestBase`/:class:`BatchModeMixin`/:class:`StreamModeMixin`.
    """

    # ------------------------------------------------------------------
    # Operation-specific primitive (overridden by mixins)
    # ------------------------------------------------------------------

    def _apply_update(self, table_update, data, cid):
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Helpers built on the primitives
    # ------------------------------------------------------------------

    def _create_seeded_table(self, partition_keys=None):
        """Create the canonical 5-row / 2-file table used by most tests.

        Layout (row_id → row):
            0: (1, Alice,   25, NYC)
            1: (2, Bob,     30, LA)
            2: (3, Charlie, 35, Chicago)
            3: (4, David,   40, Houston)
            4: (5, Eve,     45, Phoenix)
        """
        table = self._create_table(partition_keys=partition_keys)
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'age': [25, 30],
            'city': ['NYC', 'LA'],
        }, schema=self.pa_schema))
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [3, 4, 5],
            'name': ['Charlie', 'David', 'Eve'],
            'age': [35, 40, 45],
            'city': ['Chicago', 'Houston', 'Phoenix'],
        }, schema=self.pa_schema))
        return table

    def _do_update(self, table, data, columns):
        """End-to-end ``update_by_arrow_with_row_id`` + commit. Returns the
        commit messages so callers can inspect produced files."""
        wb = self._make_write_builder(table)
        tu = wb.new_update().with_update_type(columns)
        cid = self._next_commit_id()
        msgs = self._apply_update(tu, data, cid)
        tc = wb.new_commit()
        self._apply_commit(tc, msgs, cid)
        tc.close()
        return msgs

    # ==================================================================
    # Shared tests (run under both batch and stream modes)
    # ==================================================================

    def test_update_existing_column(self):
        """Update a single column across both files (row_ids unordered)."""
        table = self._create_seeded_table()
        self._do_update(table, pa.Table.from_pydict({
            '_ROW_ID': [1, 0, 2, 3, 4],
            'age': [31, 26, 36, 39, 42],
        }), ['age'])
        self.assertEqual(
            [26, 31, 36, 39, 42],
            self._read_all(table)['age'].to_pylist(),
        )

    def test_update_multiple_columns(self):
        """Update ``age`` + ``city`` together; other columns are untouched."""
        table = self._create_seeded_table()
        self._do_update(table, pa.Table.from_pydict({
            '_ROW_ID': [1, 0, 2, 3, 4],
            'age': [31, 26, 36, 39, 42],
            'city': ['Los Angeles', 'New York', 'Chicago', 'Phoenix', 'Houston'],
        }), ['age', 'city'])

        result = self._read_all(table)
        self.assertEqual([26, 31, 36, 39, 42], result['age'].to_pylist())
        self.assertEqual(
            ['New York', 'Los Angeles', 'Chicago', 'Phoenix', 'Houston'],
            result['city'].to_pylist(),
        )

    def test_partitioned_table_update(self):
        """Updates work on a partitioned table the same as a flat one."""
        table = self._create_table(partition_keys=['city'])
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'age': [25, 30, 35, 40, 45],
            'city': ['NYC', 'NYC', 'LA', 'LA', 'Chicago'],
        }, schema=self.pa_schema))

        self._do_update(table, pa.Table.from_pydict({
            '_ROW_ID': [1, 0, 2, 3, 4],
            'age': [31, 26, 36, 41, 46],
        }), ['age'])

        self.assertEqual(
            [26, 31, 36, 41, 46],
            self._read_all(table)['age'].to_pylist(),
        )

    def test_update_preserves_other_columns(self):
        """Updating ``age`` leaves all other columns untouched."""
        table = self._create_seeded_table()
        self._do_update(table, pa.Table.from_pydict({
            '_ROW_ID': [1],
            'age': [999],
        }), ['age'])

        result = self._read_all(table)
        self.assertEqual([25, 999, 35, 40, 45], result['age'].to_pylist())
        self.assertEqual(
            ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            result['name'].to_pylist(),
        )
        self.assertEqual(
            ['NYC', 'LA', 'Chicago', 'Houston', 'Phoenix'],
            result['city'].to_pylist(),
        )

    def test_partial_row_updates(self):
        """All partial-row patterns share one parameterised test."""
        cases = [
            # name, row_ids, ages, expected ages after update
            ('single_first_file',  [0],       [100],            [100, 30,  35,  40, 45]),
            ('single_second_file', [1],       [31],             [25,  31,  35,  40, 45]),
            ('one_per_file',       [1, 2],    [200, 300],       [25,  200, 300, 40, 45]),
            ('non_consecutive',    [0, 2, 4], [100, 300, 500],  [100, 30,  300, 40, 500]),
        ]
        for name, row_ids, ages, expected in cases:
            with self.subTest(case=name):
                table = self._create_seeded_table()
                self._do_update(table, pa.Table.from_pydict({
                    '_ROW_ID': row_ids,
                    'age': ages,
                }), ['age'])
                self.assertEqual(
                    expected,
                    self._read_all(table)['age'].to_pylist(),
                )

    def test_multiple_sequential_single_column_updates(self):
        """Two sequential updates on different single columns compose."""
        table = self._create_seeded_table()
        self._do_update(table, pa.Table.from_pydict({
            '_ROW_ID': [1, 0, 2, 3, 4],
            'age': [31, 26, 36, 41, 46],
        }), ['age'])
        self._do_update(table, pa.Table.from_pydict({
            '_ROW_ID': [1, 0, 2, 3, 4],
            'city': ['Los Angeles', 'New York', 'Chicago', 'Phoenix', 'Houston'],
        }), ['city'])

        result = self._read_all(table)
        self.assertEqual([26, 31, 36, 41, 46], result['age'].to_pylist())
        self.assertEqual(
            ['New York', 'Los Angeles', 'Chicago', 'Phoenix', 'Houston'],
            result['city'].to_pylist(),
        )

    def test_sequential_partial_updates(self):
        """Sequential single-row updates accumulate correctly."""
        table = self._create_seeded_table()
        for row_id, age in [(0, 100), (2, 300), (4, 500)]:
            self._do_update(table, pa.Table.from_pydict({
                '_ROW_ID': [row_id],
                'age': [age],
            }), ['age'])
        self.assertEqual(
            [100, 30, 300, 40, 500],
            self._read_all(table)['age'].to_pylist(),
        )

    def test_update_partial_rows_across_two_files(self):
        """A single update modifies partial rows from both data files."""
        table = self._create_table()
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'age': [20, 25, 30, 35, 40],
            'city': ['NYC', 'LA', 'Chicago', 'Houston', 'Phoenix'],
        }, schema=self.pa_schema))
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [6, 7, 8, 9, 10],
            'name': ['Frank', 'Grace', 'Henry', 'Ivy', 'Jack'],
            'age': [45, 50, 55, 60, 65],
            'city': ['Seattle', 'Boston', 'Denver', 'Miami', 'Atlanta'],
        }, schema=self.pa_schema))

        self._do_update(table, pa.Table.from_pydict({
            '_ROW_ID': [1, 3, 6, 8],
            'age': [100, 200, 300, 400],
            'name': ['Updated_Bob', 'Updated_David', 'Updated_Grace', 'Updated_Ivy'],
        }), ['age', 'name'])

        result = self._read_all(table)
        self.assertEqual(
            [20, 100, 30, 200, 40, 45, 300, 55, 400, 65],
            result['age'].to_pylist(),
        )
        self.assertEqual(
            ['Alice', 'Updated_Bob', 'Charlie', 'Updated_David', 'Eve',
             'Frank', 'Updated_Grace', 'Henry', 'Updated_Ivy', 'Jack'],
            result['name'].to_pylist(),
        )
        # Untouched columns
        self.assertEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                         result['id'].to_pylist())
        self.assertEqual(
            ['NYC', 'LA', 'Chicago', 'Houston', 'Phoenix',
             'Seattle', 'Boston', 'Denver', 'Miami', 'Atlanta'],
            result['city'].to_pylist(),
        )

    def test_large_table_partial_column_updates(self):
        """4-step update sequence on a 2000-row / 2-file table.

        Covers: single-column / multi-column / both-files updates while
        verifying that untouched columns stay intact.
        """
        num_row = 1000
        table = self._create_table()

        # 2 commits, num_row rows each
        self._write_arrow(table, pa.Table.from_pydict({
            'id': list(range(num_row)),
            'name': [f'Name_{i}' for i in range(num_row)],
            'age': [20 + i for i in range(num_row)],
            'city': [f'City_{i}' for i in range(num_row)],
        }, schema=self.pa_schema))
        self._write_arrow(table, pa.Table.from_pydict({
            'id': list(range(num_row, num_row * 2)),
            'name': [f'Name_{i}' for i in range(num_row, num_row * 2)],
            'age': [20 + num_row + i for i in range(num_row)],
            'city': [f'City_{i}' for i in range(num_row, num_row * 2)],
        }, schema=self.pa_schema))

        expected_ids = list(range(num_row * 2))
        expected_names = [f'Name_{i}' for i in range(num_row * 2)]
        expected_ages = ([20 + i for i in range(num_row)]
                         + [20 + num_row + i for i in range(num_row)])

        # 1) update id col, 1 row
        self._do_update(table, pa.Table.from_pydict({
            '_ROW_ID': [5], 'id': [999],
        }), ['id'])
        expected_ids[5] = 999

        # 2) update id+name cols, 2 rows
        self._do_update(table, pa.Table.from_pydict({
            '_ROW_ID': [3, 15],
            'id': [888, 777],
            'name': ['Updated_Name_3', 'Updated_Name_15'],
        }), ['id', 'name'])
        expected_ids[3], expected_ids[15] = 888, 777
        expected_names[3] = 'Updated_Name_3'
        expected_names[15] = 'Updated_Name_15'

        # 3) update name col, 1 row
        self._do_update(table, pa.Table.from_pydict({
            '_ROW_ID': [12], 'name': ['NewName_12'],
        }), ['name'])
        expected_names[12] = 'NewName_12'

        # 4) update age col, 4 rows across both files
        self._do_update(table, pa.Table.from_pydict({
            '_ROW_ID': [0, 5, 10, 15],
            'age': [100, 105, 110, 115],
        }), ['age'])
        for r, a in [(0, 100), (5, 105), (10, 110), (15, 115)]:
            expected_ages[r] = a

        result = self._read_all(table)
        self.assertEqual(expected_ids, result['id'].to_pylist())
        self.assertEqual(expected_names, result['name'].to_pylist())
        self.assertEqual(expected_ages, result['age'].to_pylist())
        # city was never touched
        self.assertEqual(
            [f'City_{i}' for i in range(num_row * 2)],
            result['city'].to_pylist(),
        )

    def test_update_with_large_file(self):
        """Even with a tiny ``target-file-size`` the update produces one
        output file per first_row_id group (rolling is disabled internally)."""
        from pypaimon.schema.schema_change import SetOption

        N = 5000
        schema = pa.schema([('id', pa.int64()), ('name', pa.string())])
        opts = {
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'write-only': 'true',
        }
        table = self._create_table(pa_schema=schema, options=opts)
        table_identifier = table.identifier

        self._write_arrow(table, pa.table({
            'id': list(range(N)),
            'name': [''.join(random.choices(string.ascii_letters, k=200))
                     for _ in range(N)],
        }))

        self.catalog.alter_table(
            table_identifier, [SetOption('target-file-size', '10kb')]
        )
        table = self.catalog.get_table(table_identifier)

        msgs = self._do_update(table, pa.table({
            '_ROW_ID': pa.array(list(range(N)), type=pa.int64()),
            'name': [''.join(random.choices(string.ascii_letters, k=200))
                     for _ in range(N)],
        }), ['name'])

        all_files = [f for m in msgs for f in m.new_files]
        self.assertEqual(1, len(all_files),
                         "Update should produce exactly one file per group")
        self.assertEqual(0, all_files[0].first_row_id)
        self.assertEqual(N, all_files[0].row_count)

    # ------------------------------------------------------------------
    # Validation tests
    # ------------------------------------------------------------------

    def test_nonexistent_column_raises(self):
        table = self._create_seeded_table()
        bad = pa.Table.from_pydict({
            '_ROW_ID': [0, 1, 2, 3, 4],
            'nonexistent_column': [100, 200, 300, 400, 500],
        })
        with self.assertRaises(ValueError) as ctx:
            wb = self._make_write_builder(table)
            tu = wb.new_update().with_update_type(['nonexistent_column'])
            self._apply_update(tu, bad, self._next_commit_id())
        self.assertIn('not in table schema', str(ctx.exception))

    def test_missing_row_id_column_raises(self):
        table = self._create_seeded_table()
        bad = pa.Table.from_pydict({'age': [26, 27, 28, 29, 30]})
        with self.assertRaises(ValueError) as ctx:
            wb = self._make_write_builder(table)
            tu = wb.new_update().with_update_type(['age'])
            self._apply_update(tu, bad, self._next_commit_id())
        self.assertIn('_ROW_ID column', str(ctx.exception))

    def test_invalid_row_id_raises(self):
        """row_id outside [0, total_row_count) (both directions) raises."""
        table = self._create_seeded_table()
        cases = [
            ('out_of_range_high', [0, 10], [26, 100]),
            ('negative',          [-1, 0], [100, 26]),
        ]
        for name, row_ids, ages in cases:
            with self.subTest(case=name):
                wb = self._make_write_builder(table)
                tu = wb.new_update().with_update_type(['age'])
                bad = pa.Table.from_pydict({'_ROW_ID': row_ids, 'age': ages})
                with self.assertRaises(ValueError) as ctx:
                    self._apply_update(tu, bad, self._next_commit_id())
                self.assertIn('out of valid range', str(ctx.exception))

    def test_duplicate_row_id_raises(self):
        table = self._create_seeded_table()
        wb = self._make_write_builder(table)
        tu = wb.new_update().with_update_type(['age'])
        with self.assertRaises(ValueError) as ctx:
            self._apply_update(
                tu,
                pa.Table.from_pydict({
                    '_ROW_ID': [0, 0, 1],
                    'age': [100, 200, 300],
                }),
                self._next_commit_id(),
            )
        self.assertIn('duplicate _ROW_ID', str(ctx.exception))

    # ------------------------------------------------------------------
    # Concurrency tests
    # ------------------------------------------------------------------

    def _run_concurrent_updates(self, table, thread_specs, max_retries):
        """Run a batch of concurrent updates with conflict-retry; return the
        commit order (``thread_index`` of the winning commit appended last)."""
        errors = []
        completion_order = []
        lock = threading.Lock()
        retry_marker = (
            "multiple 'MERGE INTO' operations have encountered conflicts"
        )

        def worker(idx, spec):
            for _ in range(max_retries):
                try:
                    self._do_update(table, pa.Table.from_pydict({
                        '_ROW_ID': spec['row_ids'],
                        'age': spec['ages'],
                    }), ['age'])
                    with lock:
                        completion_order.append(idx)
                    return
                except Exception as e:
                    if retry_marker not in str(e):
                        errors.append(f"Thread-{idx} unexpected error: {e}")
                        return
            errors.append(f"Thread-{idx} did not succeed in {max_retries} retries")

        threads = [threading.Thread(target=worker, args=(i, s))
                   for i, s in enumerate(thread_specs)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=120)

        self.assertEqual([], errors)
        self.assertEqual(len(thread_specs), len(completion_order),
                         "Not all threads committed successfully")
        return completion_order

    def test_concurrent_updates_disjoint_rows(self):
        """Disjoint per-thread updates all materialise into the final state."""
        table = self._create_seeded_table()
        specs = [
            {'row_ids': [0, 1], 'ages': [100, 200]},
            {'row_ids': [2, 3], 'ages': [300, 400]},
            {'row_ids': [4],    'ages': [500]},
        ]
        self._run_concurrent_updates(table, specs, max_retries=20)
        self.assertEqual(
            [100, 200, 300, 400, 500],
            self._read_all(table)['age'].to_pylist(),
        )

    def test_concurrent_updates_overlapping_rows_last_writer_wins(self):
        """When threads compete on the same rows, the final commit wins."""
        table = self._create_seeded_table()
        specs = [
            {'row_ids': [0, 1, 2], 'ages': [101, 201, 301]},
            {'row_ids': [0, 1, 2], 'ages': [102, 202, 302]},
            {'row_ids': [0, 1, 2], 'ages': [103, 203, 303]},
        ]
        completion_order = self._run_concurrent_updates(
            table, specs, max_retries=30
        )
        winner = specs[completion_order[-1]]['ages']
        ages = self._read_all(table)['age'].to_pylist()
        self.assertEqual(winner, ages[:3])
        # Rows 3 & 4 must remain at seed values
        self.assertEqual([40, 45], ages[3:])


# ======================================================================
# Mode-specific mixins (add the ``update_by_arrow_with_row_id`` primitive)
# ======================================================================

class _BatchModeMixin(BatchModeMixin):
    def _apply_update(self, table_update, data, cid):
        return table_update.update_by_arrow_with_row_id(data)


class _StreamModeMixin(StreamModeMixin):
    def _apply_update(self, table_update, data, cid):
        return table_update.update_by_arrow_with_row_id(data, cid)


# ======================================================================
# Concrete test classes
# ======================================================================

class TableUpdateBatchTest(_BatchModeMixin, _TableUpdateTestBase, unittest.TestCase):
    """All shared update tests under batch (``BatchWriteBuilder``) semantics."""


class TableUpdateStreamTest(_StreamModeMixin, _TableUpdateTestBase, unittest.TestCase):
    """All shared update tests under stream (``StreamWriteBuilder``) semantics,
    plus stream-only multi-commit scenarios that are impossible to express
    under :class:`BatchTableCommit` (which forbids re-committing).
    """

    # ------------------------------------------------------------------
    # Stream-only helpers
    # ------------------------------------------------------------------

    def _stream_commit_age_updates_by_row_id(self, tu, tc, commit_ids, row_age_pairs):
        """Apply one ``age`` update per ``commit_id``; ``tu`` is already
        ``with_update_type(['age'])``."""
        for cid, (row_id, age) in zip(commit_ids, row_age_pairs):
            msgs = tu.update_by_arrow_with_row_id(
                pa.Table.from_pydict({'_ROW_ID': [row_id], 'age': [age]}),
                cid,
            )
            tc.commit(msgs, cid)

    # ------------------------------------------------------------------
    # Stream-only tests
    # ------------------------------------------------------------------

    def test_stream_multi_commit_on_one_write_builder(self):
        """A single ``StreamWriteBuilder`` drives many update+commit cycles
        with reused ``tu`` and ``tc`` instances. Each commit produces its own
        snapshot tagged with the caller-supplied ``commit_identifier`` under
        a stable ``commit_user`` — the core contract distinguishing stream
        from batch mode.

        Parameterised over both contiguous and sparse identifier sequences
        to catch any accidental coupling between ``commit_identifier`` and
        ``snapshot_id`` ordering.
        """
        # (row_id, age) for each of the 3 commits — same triplet for both runs.
        per_commit_rows = [(0, 100), (2, 300), (4, 500)]
        expected_ages = [100, 30, 300, 40, 500]

        for case_name, commit_ids in [
            ('contiguous', [1, 2, 3]),
            ('sparse',     [42, 100, 1000]),
        ]:
            with self.subTest(case=case_name):
                table = self._create_seeded_table()
                wb, tc, base_snapshot_id = self._stream_commit_session(table)
                tu = wb.new_update().with_update_type(['age'])
                self._stream_commit_age_updates_by_row_id(
                    tu, tc, commit_ids, per_commit_rows,
                )
                tc.close()

                result = self._read_all(table)
                self.assertEqual(expected_ages, result['age'].to_pylist())
                self.assertEqual([1, 2, 3, 4, 5], result['id'].to_pylist())
                self.assertEqual(
                    ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
                    result['name'].to_pylist(),
                )
                self.assertEqual(
                    ['NYC', 'LA', 'Chicago', 'Houston', 'Phoenix'],
                    result['city'].to_pylist(),
                )
                self._assert_stream_builder_snapshots(
                    table, wb, base_snapshot_id, commit_ids,
                )

    def test_stream_commit_instance_accepts_messages_from_different_updates(self):
        """``StreamTableCommit`` may be committed many times, and each
        commit may carry messages from a *different* ``StreamTableUpdate``
        instance (different ``update_cols``). The one-shot restriction
        that applies to :class:`BatchTableCommit` does not apply here.
        """
        table = self._create_seeded_table()
        wb, tc, base_snapshot_id = self._stream_commit_session(table)

        # First update: only 'age' projection, fresh tu.
        tu1 = wb.new_update().with_update_type(['age'])
        msgs1 = tu1.update_by_arrow_with_row_id(
            pa.Table.from_pydict({'_ROW_ID': [0], 'age': [111]}), 1,
        )
        tc.commit(msgs1, 1)

        # Second update: different 'city' projection, fresh tu, same tc.
        tu2 = wb.new_update().with_update_type(['city'])
        msgs2 = tu2.update_by_arrow_with_row_id(
            pa.Table.from_pydict({'_ROW_ID': [0], 'city': ['Beijing']}), 2,
        )
        tc.commit(msgs2, 2)
        tc.close()

        result = self._read_all(table)
        self.assertEqual([111, 30, 35, 40, 45], result['age'].to_pylist())
        self.assertEqual(
            ['Beijing', 'LA', 'Chicago', 'Houston', 'Phoenix'],
            result['city'].to_pylist(),
        )
        self.assertEqual([1, 2, 3, 4, 5], result['id'].to_pylist())
        self.assertEqual(
            ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            result['name'].to_pylist(),
        )
        self._assert_stream_builder_snapshots(
            table, wb, base_snapshot_id, [1, 2],
        )

    def test_stream_interleaved_write_and_update_on_same_builder(self):
        """A single stream builder may perform an initial write followed by
        an update on the same ``tc``, each tagged with its own
        ``commit_identifier`` that propagates to its own snapshot.
        """
        table = self._create_table()
        wb, tc, base_snapshot_id = self._stream_commit_session(table)
        tw = wb.new_write()

        tw.write_arrow(pa.Table.from_pydict({
            'id': [1, 2, 3],
            'name': ['A', 'B', 'C'],
            'age': [10, 20, 30],
            'city': ['X', 'Y', 'Z'],
        }, schema=self.pa_schema))
        tc.commit(tw.prepare_commit(1), 1)
        tw.close()

        tu = wb.new_update().with_update_type(['age'])
        msgs = tu.update_by_arrow_with_row_id(
            pa.Table.from_pydict({'_ROW_ID': [0, 2], 'age': [111, 333]}), 2,
        )
        tc.commit(msgs, 2)
        tc.close()

        result = self._read_all(table)
        self.assertEqual([111, 20, 333], result['age'].to_pylist())
        self.assertEqual([1, 2, 3], result['id'].to_pylist())
        self.assertEqual(['A', 'B', 'C'], result['name'].to_pylist())
        self.assertEqual(['X', 'Y', 'Z'], result['city'].to_pylist())
        self._assert_stream_builder_snapshots(
            table, wb, base_snapshot_id, [1, 2],
        )


if __name__ == '__main__':
    unittest.main()
