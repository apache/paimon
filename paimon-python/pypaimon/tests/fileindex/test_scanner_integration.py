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

"""Scanner-level integration tests for bloom-filter file index pushdown.

These tests build a bloom blob with pypaimon's own BloomFilter64 (over keys
10/20/30), attach it to a real manifest entry, and drive the actual scan path,
verifying that:

* a value absent from the bloom prunes the file (fewer/zero splits),
* a value present in the bloom keeps the file,
* ``file-index.read.enabled=false`` disables pruning,
* the explain counters / "File index pruning" funnel reflect what happened.

This covers the scanner *wiring* (pure Python); cross-language byte
compatibility with the Java writer is covered separately by the e2e test
(JavaPyE2ETest#testBloomFilterIndexWrite) and the hash unit tests in
test_java_compat.py.

The value stats on the entry are deliberately widened to span the queried
values, so the stats stage never prunes and the bloom filter is the sole
decider — otherwise these would not actually exercise the file-index stage.
"""

import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.read.scanner.file_scanner import FileScanner
from pypaimon.table.row.generic_row import GenericRow

from pypaimon.tests.fileindex.bloom_blob_builder import build_bloom_blob


class BloomScannerIntegrationTest(unittest.TestCase):

    BLOOM_KEYS = [10, 20, 30]   # keys baked into the bloom blob
    ABSENT_KEY = 99             # not in the bloom

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.warehouse = os.path.join(self.tempdir, "warehouse")
        self.catalog = CatalogFactory.create({"warehouse": self.warehouse})
        self.catalog.create_database("default", False)
        self.pa_schema = pa.schema([("a", pa.int64())])
        self.blob = build_bloom_blob("a", "BIGINT", self.BLOOM_KEYS)

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def _create_table(self, name, options=None):
        schema = Schema.from_pyarrow_schema(self.pa_schema, options=options or {})
        self.catalog.create_table(f"default.{name}", schema, False)
        table = self.catalog.get_table(f"default.{name}")
        # Write the keys that are actually in the bloom so the data file is real.
        data = pa.Table.from_pydict({"a": list(self.BLOOM_KEYS)}, schema=self.pa_schema)
        wb = table.new_batch_write_builder()
        w = wb.new_write()
        c = wb.new_commit()
        w.write_arrow(data)
        c.commit(w.prepare_commit())
        w.close()
        c.close()
        return table

    def _attach_bloom(self, table):
        """Attach the locally-built bloom blob to the single data file's manifest
        entry, and widen value stats so the stats stage never prunes."""
        fs = FileScanner(table, lambda: ([], None))
        snapshot = fs.snapshot_manager.get_latest_snapshot()
        manifest_files = fs.manifest_list_manager.read_all(snapshot)
        entries = fs.manifest_file_manager.read(manifest_files[0].file_name)
        self.assertEqual(len(entries), 1)
        for entry in entries:
            entry.file.embedded_index = self.blob
            # Span [0, 1000] so any queried value passes the stats stage and the
            # bloom filter is the only thing that can prune.
            entry.file.value_stats_cols = ["a"]
            entry.file.value_stats = SimpleStats(
                GenericRow([0], [table.fields[0]]),
                GenericRow([1000], [table.fields[0]]),
                [0],
            )
        fs.manifest_file_manager.write(manifest_files[0].file_name, entries)

    def _scan_splits(self, table, value):
        rb = table.new_read_builder()
        pred = rb.new_predicate_builder().equal("a", value)
        return rb.with_filter(pred).new_scan().plan().splits()

    def test_absent_value_prunes_file(self):
        table = self._create_table("t_absent")
        self._attach_bloom(table)
        # 99 is not in the bloom -> the only file is pruned -> no splits.
        splits = self._scan_splits(table, self.ABSENT_KEY)
        self.assertEqual(len(splits), 0)

    def test_present_value_keeps_file(self):
        table = self._create_table("t_present")
        self._attach_bloom(table)
        # 10 is in the bloom -> file is kept.
        splits = self._scan_splits(table, self.BLOOM_KEYS[0])
        self.assertGreater(len(splits), 0)

    def test_disabled_does_not_prune(self):
        table = self._create_table("t_disabled", options={"file-index.read.enabled": "false"})
        self._attach_bloom(table)
        # With the feature off, even an absent value must not prune via bloom.
        splits = self._scan_splits(table, self.ABSENT_KEY)
        self.assertGreater(len(splits), 0)

    def test_explain_counters_reflect_pruning(self):
        table = self._create_table("t_explain")
        self._attach_bloom(table)
        rb = table.new_read_builder()
        pred = rb.new_predicate_builder().equal("a", self.ABSENT_KEY)
        rb = rb.with_filter(pred)
        scan = rb.new_scan()
        _, stats = scan.file_scanner.scan_with_stats()
        # One entry survived stats, the bloom was applied to it, and it did not
        # survive the file-index stage.
        self.assertEqual(stats.entries_after_stats, 1)
        self.assertEqual(stats.entries_file_index_applied, 1)
        self.assertEqual(stats.entries_after_file_index, 0)
        self.assertEqual(stats.file_index_fail_open_count, 0)

    def test_explain_counters_present_value(self):
        table = self._create_table("t_explain_present")
        self._attach_bloom(table)
        rb = table.new_read_builder()
        pred = rb.new_predicate_builder().equal("a", self.BLOOM_KEYS[0])
        rb = rb.with_filter(pred)
        scan = rb.new_scan()
        _, stats = scan.file_scanner.scan_with_stats()
        self.assertEqual(stats.entries_after_stats, 1)
        self.assertEqual(stats.entries_file_index_applied, 1)
        self.assertEqual(stats.entries_after_file_index, 1)


if __name__ == "__main__":
    unittest.main()
