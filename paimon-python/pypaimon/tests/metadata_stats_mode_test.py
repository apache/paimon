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

import os
import shutil
import tempfile
import unittest

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.table.row.generic_row import GenericRowDeserializer
from pypaimon.write.writer import stats_mode


class MetadataStatsModeUnitTest(unittest.TestCase):
    """Parity with Java ``*SimpleColStatsCollector`` (values cross-checked
    against ``SimpleColStatsCollectorTest``)."""

    def test_parse_stats_mode(self):
        self.assertEqual(stats_mode.parse_stats_mode("none"), ("none", None))
        self.assertEqual(stats_mode.parse_stats_mode("counts"), ("counts", None))
        self.assertEqual(stats_mode.parse_stats_mode("full"), ("full", None))
        # case-insensitive, like Java's toUpperCase()
        self.assertEqual(
            stats_mode.parse_stats_mode("TRUNCATE(16)"), ("truncate", 16))
        for bad in ("aatruncate(10)", "truncate(10.1)", "truncate(0)", "x"):
            with self.assertRaises(ValueError):
                stats_mode.parse_stats_mode(bad)

    def test_truncate_string_increments_last_code_point(self):
        # From Java testTruncateTwoChar: an astral (4-byte) code point plus a
        # trailing char, truncated to 1 code point. min keeps the emoji, max
        # bumps it by one code point. Python str is code-point indexed, so
        # this matches Java's BinaryString.substring / appendCodePoint.
        emoji = "\U0001F918"       # U+1F918
        self.assertEqual(
            stats_mode.convert_col_stats(
                "truncate", 1, emoji + "a", emoji + "b", 0),
            (emoji, "\U0001F919", 0))

    def test_truncate_short_string_is_unchanged(self):
        # Original already fits the length -> returned unchanged (no bump).
        self.assertEqual(
            stats_mode.convert_col_stats("truncate", 16, "ab", "ab", 3),
            ("ab", "ab", 3))

    def test_truncate_bytes_min_max(self):
        # From Java testTruncateBinaryMinMax.
        self.assertEqual(
            stats_mode.convert_col_stats(
                "truncate", 2, b"\x01\x02\x03", b"\x01\x02\x03", 0),
            (b"\x01\x02", b"\x01\x03", 0))

    def test_truncate_bytes_all_ff_drops_min_and_max(self):
        # From Java testTruncateBinaryFail: no sound upper bound -> both dropped.
        self.assertEqual(
            stats_mode.convert_col_stats(
                "truncate", 2, b"\xff\xff\x07", b"\xff\xff\x07", 4),
            (None, None, 4))

    def test_counts_keeps_only_null_count(self):
        self.assertEqual(
            stats_mode.convert_col_stats("counts", None, "a", "z", 7),
            (None, None, 7))

    def test_none_drops_everything_and_full_passes_through(self):
        self.assertEqual(
            stats_mode.convert_col_stats("none", None, "a", "z", 7),
            (None, None, None))
        self.assertEqual(
            stats_mode.convert_col_stats("full", None, "a", "z", 7),
            ("a", "z", 7))


# placeholder-e2e
@pytest.mark.python_write
class MetadataStatsModeE2ETest(unittest.TestCase):
    """End to end: metadata.stats-mode must control the value stats pypaimon
    records in the manifest, so a Paimon/Spark/Flink reader can data-skip.

    Marked ``python_write``: metadata.stats-mode shapes the *Python* writer's
    value stats. The native writer does not implement the option (it records
    full value stats, i.e. value_stats_cols=None), which is a safe superset
    but not what these assertions pin down -- so keep them on the Python
    writer lane rather than the Rust one the Native CI job forces on.
    """

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, "warehouse")
        cls.catalog = CatalogFactory.create({"warehouse": cls.warehouse})
        cls.catalog.create_database("default", True)
        cls.pa_schema = pa.schema([
            ("id", pa.int32(), False),
            ("name", pa.string()),
        ])

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _write_and_read_file_stats(self, table_name, mode):
        options = {} if mode is None else {"metadata.stats-mode": mode}
        schema = Schema.from_pyarrow_schema(self.pa_schema, options=options)
        self.catalog.create_table("default." + table_name, schema, False)
        table = self.catalog.get_table("default." + table_name)

        wb = table.new_batch_write_builder()
        w = wb.new_write()
        c = wb.new_commit()
        try:
            w.write_arrow(pa.Table.from_pylist([
                {"id": 10, "name": "apple"},
                {"id": 20, "name": "banana"},
                {"id": 30, "name": None},
            ], schema=self.pa_schema))
            c.commit(w.prepare_commit())
        finally:
            w.close()
            c.close()

        scan = table.new_read_builder().new_scan()
        fs = scan.file_scanner
        latest = table.snapshot_manager().get_latest_snapshot()
        manifest_files = fs.manifest_list_manager.read_all(latest)
        entries = fs.manifest_file_manager.read(
            manifest_files[0].file_name,
            lambda row: fs._filter_manifest_entry(row), False)
        return table, entries[0].file

    @staticmethod
    def _min_max(table, file):
        vs = file.value_stats
        fields = table.fields if file.value_stats_cols is None else []
        mn = GenericRowDeserializer.from_bytes(vs.min_values.data, fields).values
        mx = GenericRowDeserializer.from_bytes(vs.max_values.data, fields).values
        return mn, mx, list(vs.null_counts)

    def test_none_records_no_value_stats(self):
        table, file = self._write_and_read_file_stats("stats_none", "none")
        self.assertEqual(file.value_stats_cols, [])
        mn, mx, _ = self._min_max(table, file)
        self.assertEqual((mn, mx), ([], []))

    def test_default_is_none(self):
        # pypaimon's default stays none (Java's is truncate(16)); unset behaves
        # exactly like an explicit none, so existing tables are untouched.
        table, file = self._write_and_read_file_stats("stats_default", None)
        self.assertEqual(file.value_stats_cols, [])

    def test_counts_records_only_null_counts(self):
        table, file = self._write_and_read_file_stats("stats_counts", "counts")
        self.assertIsNone(file.value_stats_cols)
        mn, mx, null_counts = self._min_max(table, file)
        self.assertEqual(mn, [None, None])
        self.assertEqual(mx, [None, None])
        self.assertEqual(null_counts, [0, 1])   # id has no null, name has one

    def test_full_records_full_min_max(self):
        table, file = self._write_and_read_file_stats("stats_full", "full")
        self.assertIsNone(file.value_stats_cols)
        mn, mx, null_counts = self._min_max(table, file)
        self.assertEqual(mn, [10, "apple"])
        self.assertEqual(mx, [30, "banana"])
        self.assertEqual(null_counts, [0, 1])

    def test_truncate_truncates_string_min_max(self):
        table, file = self._write_and_read_file_stats("stats_trunc", "truncate(3)")
        self.assertIsNone(file.value_stats_cols)
        mn, mx, null_counts = self._min_max(table, file)
        # int is not truncated; string min "apple"->"app", max "banana"->"bao"
        # ("ban" bumped at the last code point so it stays >= "banana").
        self.assertEqual(mn, [10, "app"])
        self.assertEqual(mx, [30, "bao"])
        self.assertEqual(null_counts, [0, 1])


if __name__ == "__main__":
    unittest.main()
