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

from pypaimon.stats.col_stats import ColStats
from pypaimon.stats.statistics import Statistics
from pypaimon.stats.stats_file_handler import StatsFileHandler


class TestColStatsSerialization(unittest.TestCase):

    def test_roundtrip(self):
        cs = ColStats(col_id=3, distinct_count=100, min="1", max="999",
                      null_count=5, avg_len=8, max_len=12)
        d = cs.to_dict()
        restored = ColStats.from_dict(d)
        self.assertEqual(cs, restored)

    def test_optional_fields(self):
        cs = ColStats(col_id=1, null_count=10)
        d = cs.to_dict()
        self.assertNotIn("distinctCount", d)
        self.assertNotIn("min", d)
        restored = ColStats.from_dict(d)
        self.assertIsNone(restored.distinct_count)
        self.assertEqual(restored.null_count, 10)


class TestStatisticsSerialization(unittest.TestCase):

    def test_roundtrip(self):
        stats = Statistics(
            snapshot_id=5,
            schema_id=0,
            merged_record_count=1000,
            merged_record_size=4096,
            col_stats={
                "id": ColStats(col_id=0, distinct_count=1000, min="1", max="1000", null_count=0),
                "name": ColStats(col_id=1, distinct_count=500, null_count=3, avg_len=10, max_len=50),
            }
        )
        json_str = stats.to_json()
        restored = Statistics.from_json(json_str)
        self.assertEqual(restored.snapshot_id, 5)
        self.assertEqual(restored.merged_record_count, 1000)
        self.assertEqual(len(restored.col_stats), 2)
        self.assertEqual(restored.col_stats["id"].distinct_count, 1000)

    def test_java_compatible_keys(self):
        stats = Statistics(snapshot_id=1, schema_id=0, merged_record_count=10)
        d = stats.to_dict()
        self.assertIn("snapshotId", d)
        self.assertIn("schemaId", d)
        self.assertIn("mergedRecordCount", d)

    def test_empty_col_stats_always_emitted(self):
        stats = Statistics(snapshot_id=1, schema_id=0)
        d = stats.to_dict()
        self.assertIn("colStats", d)
        self.assertEqual(d["colStats"], {})


class TestStatsFileHandler(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.table_path = os.path.join(self.temp_dir, "test_table")
        os.makedirs(os.path.join(self.table_path, "statistics"))

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def test_write_and_read(self):
        from pypaimon.filesystem.local_file_io import LocalFileIO
        file_io = LocalFileIO()
        handler = StatsFileHandler(file_io, self.table_path)

        stats = Statistics(
            snapshot_id=1, schema_id=0, merged_record_count=42,
            col_stats={"x": ColStats(col_id=0, min="0", max="100", null_count=0)}
        )
        file_name = handler.write_stats(stats)
        self.assertTrue(file_name.startswith("statistics-"))

        from pypaimon.snapshot.snapshot import Snapshot
        mock_snapshot = Snapshot(
            version=3, id=1, schema_id=0,
            base_manifest_list="", delta_manifest_list="",
            total_record_count=42, delta_record_count=42,
            commit_user="test", commit_identifier=0,
            commit_kind="ANALYZE", time_millis=0,
            statistics=file_name,
        )
        read_back = handler.read_stats(mock_snapshot)
        self.assertEqual(read_back.merged_record_count, 42)
        self.assertEqual(read_back.col_stats["x"].max, "100")


if __name__ == '__main__':
    unittest.main()
