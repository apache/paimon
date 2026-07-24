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

"""Conflict-detection base scan is reused across commit retries.

The first attempt full-scans the changed partitions; later retries reuse that
base and read only the incremental changes since (read_incremental_changes).
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
        # Static overwrite, scoped to the explicit partition f0=1.
        schema = Schema.from_pyarrow_schema(
            pa_schema, partition_keys=['f0'],
            options={'dynamic-partition-overwrite': 'false'})
        self.catalog.create_table('test_db.t', schema, False)
        self.table = self.catalog.get_table('test_db.t')

        # f0=1 is the overwrite target, f0=2 untouched.
        self._append(pd.DataFrame({'f0': [1, 1, 2], 'f1': ['a', 'b', 'c']}))

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _append(self, df):
        wb = self.table.new_batch_write_builder()
        w = wb.new_write()
        c = wb.new_commit()
        w.write_pandas(df)
        c.commit(w.prepare_commit())
        w.close()
        c.close()

    def _overwrite_target(self, f1_val):
        wb = self.table.new_batch_write_builder().overwrite({'f0': 1})
        w = wb.new_write()
        c = wb.new_commit()
        w.write_pandas(pd.DataFrame({'f0': [1], 'f1': [f1_val]}))
        c.commit(w.prepare_commit())
        w.close()
        c.close()

    def _compact_target(self, f1_val):
        # pypaimon has no compact API; produce a COMPACT-kind snapshot by running
        # an overwrite of f0=1 but labelling the snapshot COMPACT.
        wb = self.table.new_batch_write_builder().overwrite({'f0': 1})
        w = wb.new_write()
        tc = wb.new_commit()
        w.write_pandas(pd.DataFrame({'f0': [1], 'f1': [f1_val]}))
        cfsc = tc.file_store_commit
        orig_try = cfsc._try_commit
        cfsc._try_commit = lambda commit_kind, *a, **k: orig_try("COMPACT", *a, **k)
        tc.commit(w.prepare_commit())
        w.close()
        tc.close()

    def test_conflict_scan_runs_once_not_once_per_retry(self):
        K = 3

        wb = self.table.new_batch_write_builder().overwrite({'f0': 1})
        w = wb.new_write()
        c = wb.new_commit()
        w.write_pandas(pd.DataFrame({'f0': [1], 'f1': ['new']}))
        messages = w.prepare_commit()

        fsc = c.file_store_commit
        counts = {'full_scan': 0, 'incremental': 0}
        orig_full = fsc.commit_scanner.read_all_entries_from_changed_partitions
        orig_incr = fsc.commit_scanner.read_incremental_changes

        def spy_full(*a, **k):
            counts['full_scan'] += 1
            return orig_full(*a, **k)

        def spy_incr(*a, **k):
            counts['incremental'] += 1
            return orig_incr(*a, **k)

        fsc.commit_scanner.read_all_entries_from_changed_partitions = spy_full
        fsc.commit_scanner.read_incremental_changes = spy_incr

        orig_cas = fsc.snapshot_commit.commit
        cas = {'fails': 0}

        def patched_cas(snapshot, statistics):
            # Each conflict appends to an unrelated partition (f0=99), advancing
            # latest, then fails our CAS.
            if snapshot.commit_kind == "OVERWRITE" and cas['fails'] < K:
                cas['fails'] += 1
                self._append(pd.DataFrame({'f0': [99], 'f1': [f'x{cas["fails"]}']}))
                return False
            return orig_cas(snapshot, statistics)

        fsc.snapshot_commit.commit = patched_cas

        c.commit(messages)
        c.close()

        self.assertEqual(cas['fails'], K, "expected exactly K forced conflicts")
        self.assertEqual(counts['full_scan'], 1)   # once, then incremental on retry
        self.assertEqual(counts['incremental'], K)

        read_builder = self.table.new_read_builder()
        actual = read_builder.new_read().to_pandas(
            read_builder.new_scan().plan().splits())
        self.assertEqual(sorted(actual[actual['f0'] == 1]['f1'].tolist()), ['new'])
        self.assertEqual(sorted(actual[actual['f0'] == 2]['f1'].tolist()), ['c'])
        self.assertEqual(len(actual[actual['f0'] == 99]), K)

    def test_incremental_merge_when_concurrent_append_hits_target_partition(self):
        # Concurrent appends hit the target partition, so the incremental read is
        # non-empty and must be merged into the reused base.
        K = 3

        wb = self.table.new_batch_write_builder().overwrite({'f0': 1})
        w = wb.new_write()
        c = wb.new_commit()
        w.write_pandas(pd.DataFrame({'f0': [1], 'f1': ['new']}))
        messages = w.prepare_commit()

        fsc = c.file_store_commit
        full_scans = {'n': 0}
        incr_lengths = []
        captured = []
        orig_full = fsc.commit_scanner.read_all_entries_from_changed_partitions
        orig_incr = fsc.commit_scanner.read_incremental_changes
        orig_check = fsc.conflict_detection.check_conflicts

        def spy_full(*a, **k):
            full_scans['n'] += 1
            return orig_full(*a, **k)

        def spy_incr(*a, **k):
            r = orig_incr(*a, **k)
            incr_lengths.append(None if r is None else len(r))
            return r

        def spy_check(latest_snapshot, base_entries, delta_entries, *a, **k):
            captured.append((latest_snapshot, list(base_entries), list(delta_entries)))
            return orig_check(latest_snapshot, base_entries, delta_entries, *a, **k)

        fsc.commit_scanner.read_all_entries_from_changed_partitions = spy_full
        fsc.commit_scanner.read_incremental_changes = spy_incr
        fsc.conflict_detection.check_conflicts = spy_check

        orig_cas = fsc.snapshot_commit.commit
        cas = {'fails': 0}

        def patched_cas(snapshot, statistics):
            if snapshot.commit_kind == "OVERWRITE" and cas['fails'] < K:
                cas['fails'] += 1
                self._append(pd.DataFrame({'f0': [1], 'f1': [f'y{cas["fails"]}']}))
                return False
            return orig_cas(snapshot, statistics)

        fsc.snapshot_commit.commit = patched_cas

        c.commit(messages)
        c.close()

        self.assertEqual(cas['fails'], K, "expected exactly K forced conflicts")
        self.assertEqual(full_scans['n'], 1)
        self.assertEqual(len(incr_lengths), K)
        self.assertTrue(all(incr_lengths),
                        f"expected non-empty incremental on every retry, got {incr_lengths}")

        # The incremental-merged base must equal a fresh full scan.
        last_snapshot, merged_base, last_delta = captured[-1]
        full = orig_full(last_snapshot, last_delta)
        self.assertEqual(
            set(e.identifier() for e in merged_base),
            set(e.identifier() for e in full))

        read_builder = self.table.new_read_builder()
        actual = read_builder.new_read().to_pandas(
            read_builder.new_scan().plan().splits())
        self.assertEqual(sorted(actual[actual['f0'] == 1]['f1'].tolist()), ['new'])
        self.assertEqual(sorted(actual[actual['f0'] == 2]['f1'].tolist()), ['c'])

    def test_falls_back_to_full_scan_when_intermediate_snapshot_missing(self):
        # A missing intermediate snapshot -> read_incremental_changes returns None
        # and the retry falls back to a full scan.
        K = 1
        missing_id = self.table.snapshot_manager().get_latest_snapshot().id + 1

        wb = self.table.new_batch_write_builder().overwrite({'f0': 1})
        w = wb.new_write()
        c = wb.new_commit()
        w.write_pandas(pd.DataFrame({'f0': [1], 'f1': ['new']}))
        messages = w.prepare_commit()

        fsc = c.file_store_commit
        full_scans = {'n': 0}
        incr_results = []
        orig_full = fsc.commit_scanner.read_all_entries_from_changed_partitions
        orig_incr = fsc.commit_scanner.read_incremental_changes

        def spy_full(*a, **k):
            full_scans['n'] += 1
            return orig_full(*a, **k)

        def spy_incr(*a, **k):
            r = orig_incr(*a, **k)
            incr_results.append(r)
            return r

        fsc.commit_scanner.read_all_entries_from_changed_partitions = spy_full
        fsc.commit_scanner.read_incremental_changes = spy_incr

        # Only the scanner's lookups see missing_id as absent; the commit's own
        # manager (bound earlier) is untouched.
        real_mgr = fsc.commit_scanner.table.snapshot_manager()

        class _Wrap:
            def __getattr__(self, name):
                return getattr(real_mgr, name)

            def get_snapshot_by_id(self, i):
                return None if i == missing_id else real_mgr.get_snapshot_by_id(i)

        fsc.commit_scanner.table.snapshot_manager = lambda: _Wrap()

        orig_cas = fsc.snapshot_commit.commit
        cas = {'fails': 0}

        def patched_cas(snapshot, statistics):
            if snapshot.commit_kind == "OVERWRITE" and cas['fails'] < K:
                cas['fails'] += 1
                self._append(pd.DataFrame({'f0': [99], 'f1': ['x']}))
                return False
            return orig_cas(snapshot, statistics)

        fsc.snapshot_commit.commit = patched_cas

        c.commit(messages)
        c.close()

        self.assertEqual(cas['fails'], K, "expected exactly K forced conflicts")
        self.assertIn(None, incr_results)          # incremental bailed on missing
        self.assertEqual(full_scans['n'], 2)       # first attempt + fallback

    def test_retry_base_matches_full_scan_across_overwrite_snapshot(self):
        self._assert_merge_equals_full_scan(self._overwrite_target)

    def test_incremental_merge_across_compact_snapshot(self):
        self._assert_merge_equals_full_scan(self._compact_target)

    def _assert_merge_equals_full_scan(self, concurrent_fn):
        # A non-APPEND snapshot lands between retries. OVERWRITE falls back to
        # a full scan; COMPACT can still merge deltas. Either base must be fresh.
        K = 2

        wb = self.table.new_batch_write_builder().overwrite({'f0': 1})
        w = wb.new_write()
        c = wb.new_commit()
        w.write_pandas(pd.DataFrame({'f0': [1], 'f1': ['new']}))
        messages = w.prepare_commit()

        fsc = c.file_store_commit
        captured = []
        orig_check = fsc.conflict_detection.check_conflicts
        orig_full = fsc.commit_scanner.read_all_entries_from_changed_partitions

        def spy_check(latest_snapshot, base_entries, delta_entries, *a, **k):
            captured.append((latest_snapshot, list(base_entries), list(delta_entries)))
            return orig_check(latest_snapshot, base_entries, delta_entries, *a, **k)

        fsc.conflict_detection.check_conflicts = spy_check

        orig_cas = fsc.snapshot_commit.commit
        cas = {'fails': 0}

        def patched_cas(snapshot, statistics):
            if snapshot.commit_kind == "OVERWRITE" and cas['fails'] < K:
                cas['fails'] += 1
                concurrent_fn(f'z{cas["fails"]}')
                return False
            return orig_cas(snapshot, statistics)

        fsc.snapshot_commit.commit = patched_cas

        c.commit(messages)
        c.close()

        self.assertEqual(cas['fails'], K, "expected exactly K forced conflicts")

        last_snapshot, merged_base, last_delta = captured[-1]
        full = orig_full(last_snapshot, last_delta)
        self.assertEqual(
            set(e.identifier() for e in merged_base),
            set(e.identifier() for e in full))

        read_builder = self.table.new_read_builder()
        actual = read_builder.new_read().to_pandas(
            read_builder.new_scan().plan().splits())
        self.assertEqual(sorted(actual[actual['f0'] == 1]['f1'].tolist()), ['new'])


if __name__ == '__main__':
    unittest.main()
