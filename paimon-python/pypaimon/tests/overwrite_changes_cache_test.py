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

"""Regression test for caching of OVERWRITE changes across commit retries.

On every OVERWRITE retry, pypaimon re-scanned the full target partitions to
recompute the files to delete. Under concurrent writers this made each retry as
expensive as the first attempt. OverwriteChangesProvider caches the existing
files of the target partitions and, on retry, reuses them when the snapshots in
between are all APPEND and have not touched the target partitions (verified by a
cheap DELTA probe), instead of a full re-scan.

The test deterministically forces ``K`` conflicts, each advancing the latest
snapshot with an append to an unrelated partition, and asserts the full scan
runs once (not ``K + 1``) and the cache is advanced by a delta probe per retry.
"""

import os
import shutil
import tempfile
import unittest

import pandas as pd
import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.write.commit.overwrite_changes_provider import OverwriteChangesProvider


class OverwriteChangesCacheTest(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="ow_cache_")
        self.warehouse = os.path.join(self.temp_dir, 'wh')
        self.catalog = CatalogFactory.create({"warehouse": self.warehouse})
        self.catalog.create_database("test_db", True)

        pa_schema = pa.schema([('f0', pa.int32()), ('f1', pa.string())])
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            partition_keys=['f0'],
            options={'dynamic-partition-overwrite': 'false'},
        )
        self.catalog.create_table('test_db.t', schema, False)
        self.table = self.catalog.get_table('test_db.t')

        # Seed: f0=1 is the overwrite target, f0=2 untouched.
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

    def test_overwrite_scan_runs_once_not_once_per_retry(self):
        K = 3  # force 3 conflicts; the 4th attempt wins

        wb = self.table.new_batch_write_builder().overwrite({'f0': 1})
        w = wb.new_write()
        c = wb.new_commit()
        w.write_pandas(pd.DataFrame({'f0': [1], 'f1': ['new']}))
        messages = w.prepare_commit()

        fsc = c.file_store_commit

        # --- count provider full scans and delta probes (class-level spies) ---
        counts = {'full_scan': 0, 'probe': 0}
        orig_full = OverwriteChangesProvider._full_scan
        orig_probe = OverwriteChangesProvider._delta_touches_target

        def spy_full(self, *a, **k):
            counts['full_scan'] += 1
            return orig_full(self, *a, **k)

        def spy_probe(self, *a, **k):
            counts['probe'] += 1
            return orig_probe(self, *a, **k)

        # --- inject K CAS conflicts; each advances latest via an APPEND to an
        #     unrelated partition (f0=99) so the delta probe finds the target
        #     (f0=1) untouched and the cache is reused.
        orig_cas = fsc.snapshot_commit.commit
        cas = {'fails': 0}

        def patched_cas(snapshot, statistics):
            if snapshot.commit_kind == "OVERWRITE" and cas['fails'] < K:
                cas['fails'] += 1
                self._append(pd.DataFrame({'f0': [99], 'f1': [f'x{cas["fails"]}']}))
                return False
            return orig_cas(snapshot, statistics)

        fsc.snapshot_commit.commit = patched_cas
        OverwriteChangesProvider._full_scan = spy_full
        OverwriteChangesProvider._delta_touches_target = spy_probe
        try:
            c.commit(messages)
            c.close()
        finally:
            OverwriteChangesProvider._full_scan = orig_full
            OverwriteChangesProvider._delta_touches_target = orig_probe

        # Harness sanity: we really did force K conflicts and then converged.
        self.assertEqual(cas['fails'], K, "expected exactly K forced conflicts")

        print(f"\n[overwrite-cache] K={K} conflicts -> "
              f"full_scan={counts['full_scan']}, probe={counts['probe']} "
              f"(target: full_scan=1, probe=K)")

        # #7894: the full overwrite scan runs once; each retry reuses the cache
        # after a cheap delta probe of the (unrelated) intervening append.
        self.assertEqual(
            counts['full_scan'], 1,
            f"full overwrite scan ran {counts['full_scan']}x; should run once and "
            f"reuse the cache on retries")
        self.assertEqual(
            counts['probe'], K,
            f"delta probe ran {counts['probe']}x; should probe once per retry "
            f"(= K = {K})")

        # Sanity: f0=1 overwritten, f0=2 preserved, f0=99 appended K times.
        read_builder = self.table.new_read_builder()
        actual = read_builder.new_read().to_pandas(
            read_builder.new_scan().plan().splits())
        self.assertEqual(sorted(actual[actual['f0'] == 1]['f1'].tolist()), ['new'])
        self.assertEqual(sorted(actual[actual['f0'] == 2]['f1'].tolist()), ['c'])
        self.assertEqual(len(actual[actual['f0'] == 99]), K)

    def test_cache_rebuilt_when_concurrent_append_hits_target_partition(self):
        # Concurrent appends hit the overwrite target; probe sees it touched -> rebuild.
        K = 3

        wb = self.table.new_batch_write_builder().overwrite({'f0': 1})
        w = wb.new_write()
        c = wb.new_commit()
        w.write_pandas(pd.DataFrame({'f0': [1], 'f1': ['new']}))
        messages = w.prepare_commit()

        fsc = c.file_store_commit

        counts = {'full_scan': 0, 'probe': 0}
        orig_full = OverwriteChangesProvider._full_scan
        orig_probe = OverwriteChangesProvider._delta_touches_target

        def spy_full(self, *a, **k):
            counts['full_scan'] += 1
            return orig_full(self, *a, **k)

        def spy_probe(self, *a, **k):
            counts['probe'] += 1
            return orig_probe(self, *a, **k)

        orig_cas = fsc.snapshot_commit.commit
        cas = {'fails': 0}

        def patched_cas(snapshot, statistics):
            if snapshot.commit_kind == "OVERWRITE" and cas['fails'] < K:
                cas['fails'] += 1
                self._append(pd.DataFrame({'f0': [1], 'f1': [f'y{cas["fails"]}']}))
                return False
            return orig_cas(snapshot, statistics)

        fsc.snapshot_commit.commit = patched_cas
        OverwriteChangesProvider._full_scan = spy_full
        OverwriteChangesProvider._delta_touches_target = spy_probe
        try:
            c.commit(messages)
            c.close()
        finally:
            OverwriteChangesProvider._full_scan = orig_full
            OverwriteChangesProvider._delta_touches_target = orig_probe

        self.assertEqual(cas['fails'], K, "expected exactly K forced conflicts")

        # Target touched each retry => cache rebuilds; full scan runs every attempt.
        self.assertEqual(counts['full_scan'], K + 1,
                         f"full scan ran {counts['full_scan']}x; cache must rebuild "
                         f"when the target partition is touched")
        self.assertEqual(counts['probe'], K,
                         f"delta probe ran {counts['probe']}x; once per retry (= K)")

        # Overwrite wins: f0=1 is just 'new', f0=2 untouched.
        read_builder = self.table.new_read_builder()
        actual = read_builder.new_read().to_pandas(
            read_builder.new_scan().plan().splits())
        self.assertEqual(sorted(actual[actual['f0'] == 1]['f1'].tolist()), ['new'])
        self.assertEqual(sorted(actual[actual['f0'] == 2]['f1'].tolist()), ['c'])


if __name__ == '__main__':
    unittest.main()
