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

"""Regression test for overwrite-commit conflict retries.

Reproduces the behaviour that makes a single OVERWRITE commit slow under
concurrent writers: on every retry, pypaimon re-runs *two* full manifest
scans of the changed partitions instead of reusing the previous attempt's
work and reading only the incremental changes (as the Java side does).

The test deterministically forces ``K`` commit conflicts (each one advances
the latest snapshot via an external append to an unrelated partition, then
makes our CAS fail) and counts how many times the two full scans run:

  * ``_generate_overwrite_entries``            (computes the DELETE set)
  * ``read_all_entries_from_changed_partitions`` (feeds conflict detection)

Current behaviour:  each is called ``K + 1`` times (once per attempt).
Target behaviour:   each is called exactly once; retries read only the
                    incremental delta.
"""

import os
import shutil
import tempfile
import unittest

import pandas as pd
import pyarrow as pa

from pypaimon import CatalogFactory, Schema


class OverwriteCommitConflictTest(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="ow_conflict_")
        self.warehouse = os.path.join(self.temp_dir, 'wh')
        self.catalog = CatalogFactory.create({"warehouse": self.warehouse})
        self.catalog.create_database("test_db", True)

        pa_schema = pa.schema([('f0', pa.int32()), ('f1', pa.string())])
        # Static overwrite (dynamic-partition-overwrite=false) so the overwrite
        # is scoped to the explicit partition f0=1.
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            partition_keys=['f0'],
            options={'dynamic-partition-overwrite': 'false'},
        )
        self.catalog.create_table('test_db.t', schema, False)
        self.table = self.catalog.get_table('test_db.t')

        # Seed base data: partition f0=1 is the overwrite target, f0=2 untouched.
        self._append(pd.DataFrame({'f0': [1, 1, 2], 'f1': ['a', 'b', 'c']}))

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _append(self, df):
        """A normal (non-overwrite) append commit on a fresh write builder.

        Used both to seed data and, during the test, as the "concurrent
        writer" that advances the latest snapshot between retries.
        """
        wb = self.table.new_batch_write_builder()
        w = wb.new_write()
        c = wb.new_commit()
        w.write_pandas(df)
        c.commit(w.prepare_commit())
        w.close()
        c.close()

    def test_full_scan_runs_once_not_once_per_retry(self):
        K = 3  # force 3 conflicts; the 4th attempt wins

        # Build the overwrite of partition f0=1 (do not commit yet).
        wb = self.table.new_batch_write_builder().overwrite({'f0': 1})
        w = wb.new_write()
        c = wb.new_commit()
        w.write_pandas(pd.DataFrame({'f0': [1], 'f1': ['new']}))
        messages = w.prepare_commit()

        fsc = c.file_store_commit

        # --- spies -----------------------------------------------------------
        # scan2_conflict: full conflict-detection scan (this PR / #4286 makes it
        #                 run once, then reuse + incremental on retries).
        # incremental:    the incremental read used on retries (new path).
        # scan1_delete:   full overwrite-DELETE scan (out of scope here; still
        #                 once-per-attempt until #7894 is ported).
        counts = {'scan1_delete': 0, 'scan2_conflict': 0, 'incremental': 0}
        orig_scan1 = fsc._generate_overwrite_entries
        orig_scan2 = fsc.commit_scanner.read_all_entries_from_changed_partitions
        orig_incr = fsc.commit_scanner.read_incremental_changes

        def spy_scan1(*a, **k):
            counts['scan1_delete'] += 1
            return orig_scan1(*a, **k)

        def spy_scan2(*a, **k):
            counts['scan2_conflict'] += 1
            return orig_scan2(*a, **k)

        def spy_incr(*a, **k):
            counts['incremental'] += 1
            return orig_incr(*a, **k)

        fsc._generate_overwrite_entries = spy_scan1
        fsc.commit_scanner.read_all_entries_from_changed_partitions = spy_scan2
        fsc.commit_scanner.read_incremental_changes = spy_incr

        # --- inject K CAS conflicts ------------------------------------------
        # Each forced failure first advances the latest snapshot via an append
        # to an unrelated partition (f0=99), so retries face a genuinely newer
        # snapshot (and thus a real incremental delta), then fails our CAS.
        orig_cas = fsc.snapshot_commit.commit
        cas = {'fails': 0}

        def patched_cas(snapshot, statistics):
            if snapshot.commit_kind == "OVERWRITE" and cas['fails'] < K:
                cas['fails'] += 1
                self._append(pd.DataFrame({'f0': [99], 'f1': [f'x{cas["fails"]}']}))
                return False
            return orig_cas(snapshot, statistics)

        fsc.snapshot_commit.commit = patched_cas

        # --- run the overwrite commit ----------------------------------------
        c.commit(messages)
        c.close()

        # Harness sanity: we really did force K conflicts and then converged.
        self.assertEqual(cas['fails'], K, "expected exactly K forced conflicts")

        print(f"\n[overwrite-conflict] K={K} conflicts -> "
              f"scan2_conflict={counts['scan2_conflict']}, "
              f"incremental={counts['incremental']}, "
              f"scan1_delete={counts['scan1_delete']} (PR2 territory)")

        # #4286: the full conflict-detection scan runs once; every retry reuses
        # the previous base entries and only reads the incremental changes.
        self.assertEqual(
            counts['scan2_conflict'], 1,
            f"read_all_entries_from_changed_partitions ran "
            f"{counts['scan2_conflict']}x; should run once and read incremental "
            f"on retry")
        self.assertEqual(
            counts['incremental'], K,
            f"read_incremental_changes ran {counts['incremental']}x; should run "
            f"once per retry (= K = {K})")

        # Sanity: the overwrite still produced the correct table (f0=1 overwritten,
        # f0=2 preserved, f0=99 appended by the simulated concurrent writer).
        read_builder = self.table.new_read_builder()
        actual = read_builder.new_read().to_pandas(
            read_builder.new_scan().plan().splits()).sort_values(
            by=['f0', 'f1']).reset_index(drop=True)
        self.assertEqual(sorted(actual[actual['f0'] == 1]['f1'].tolist()), ['new'])
        self.assertEqual(sorted(actual[actual['f0'] == 2]['f1'].tolist()), ['c'])
        self.assertEqual(len(actual[actual['f0'] == 99]), K)


if __name__ == '__main__':
    unittest.main()
