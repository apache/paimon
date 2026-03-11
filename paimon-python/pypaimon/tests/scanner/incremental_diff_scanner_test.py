################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
"""Tests for IncrementalDiffScanner."""

import unittest
from unittest.mock import Mock, patch


def _create_mock_table():
    table = Mock()
    table.is_primary_key_table = False
    table.options = Mock()
    table.options.source_split_target_size.return_value = 128 * 1024 * 1024
    table.options.source_split_open_file_cost.return_value = 4 * 1024 * 1024
    table.options.scan_manifest_parallelism.return_value = 8
    table.partition_keys = []
    return table


def _create_mock_entry(partition_values, bucket, filename):
    entry = Mock()
    entry.partition = Mock()
    entry.partition.values = partition_values
    entry.bucket = bucket
    entry.total_buckets = 1
    entry.file = Mock()
    entry.file.file_name = filename
    entry.file.file_size = 1024
    entry.file.row_count = 100
    entry.kind = 0  # ADD
    return entry


def _create_mock_snapshot(snapshot_id, commit_kind="APPEND",
                          base_manifest="base", delta_manifest="delta"):
    snapshot = Mock()
    snapshot.id = snapshot_id
    snapshot.commit_kind = commit_kind
    snapshot.base_manifest_list = f"{base_manifest}-{snapshot_id}"
    snapshot.delta_manifest_list = f"{delta_manifest}-{snapshot_id}"
    return snapshot


class IncrementalDiffScannerTest(unittest.TestCase):

    @patch('pypaimon.read.scanner.incremental_diff_scanner.ManifestListManager')
    @patch('pypaimon.read.scanner.incremental_diff_scanner.ManifestFileManager')
    def test_diff_returns_only_added_files(self, MockManifestFileManager, MockManifestListManager):
        """Files in end but not in start should be returned."""
        from pypaimon.read.scanner.incremental_diff_scanner import \
            IncrementalDiffScanner

        table = _create_mock_table()

        start_entries = [
            _create_mock_entry([], 0, "file1.parquet"),
            _create_mock_entry([], 0, "file2.parquet"),
        ]
        end_entries = [
            _create_mock_entry([], 0, "file1.parquet"),
            _create_mock_entry([], 0, "file2.parquet"),
            _create_mock_entry([], 0, "file3.parquet"),
            _create_mock_entry([], 0, "file4.parquet"),
        ]

        mock_mlm = MockManifestListManager.return_value
        mock_mfm = MockManifestFileManager.return_value

        mock_mlm.read_base.side_effect = lambda s: [Mock(file_name=f"manifest-{s.id}")]
        mock_mfm.read_entries_parallel.side_effect = [start_entries, end_entries]

        scanner = IncrementalDiffScanner(table)
        start_snapshot = _create_mock_snapshot(1)
        end_snapshot = _create_mock_snapshot(5)

        added_entries = scanner.compute_diff(start_snapshot, end_snapshot)

        added_filenames = {e.file.file_name for e in added_entries}
        self.assertEqual(added_filenames, {"file3.parquet", "file4.parquet"})

    @patch('pypaimon.read.scanner.incremental_diff_scanner.ManifestListManager')
    @patch('pypaimon.read.scanner.incremental_diff_scanner.ManifestFileManager')
    def test_diff_handles_empty_start(self, MockManifestFileManager, MockManifestListManager):
        """When start is empty, all end files should be returned."""
        from pypaimon.read.scanner.incremental_diff_scanner import \
            IncrementalDiffScanner

        table = _create_mock_table()

        start_entries = []
        end_entries = [
            _create_mock_entry([], 0, "file1.parquet"),
            _create_mock_entry([], 0, "file2.parquet"),
        ]

        mock_mlm = MockManifestListManager.return_value
        mock_mfm = MockManifestFileManager.return_value
        mock_mlm.read_base.side_effect = lambda s: [Mock(file_name=f"manifest-{s.id}")]
        mock_mfm.read_entries_parallel.side_effect = [start_entries, end_entries]

        scanner = IncrementalDiffScanner(table)
        start_snapshot = _create_mock_snapshot(0)
        end_snapshot = _create_mock_snapshot(5)

        added_entries = scanner.compute_diff(start_snapshot, end_snapshot)

        added_filenames = {e.file.file_name for e in added_entries}
        self.assertEqual(added_filenames, {"file1.parquet", "file2.parquet"})

    @patch('pypaimon.read.scanner.incremental_diff_scanner.ManifestListManager')
    @patch('pypaimon.read.scanner.incremental_diff_scanner.ManifestFileManager')
    def test_diff_handles_same_snapshots(self, MockManifestFileManager, MockManifestListManager):
        """When start == end (same files), diff should return empty."""
        from pypaimon.read.scanner.incremental_diff_scanner import \
            IncrementalDiffScanner

        table = _create_mock_table()

        entries = [
            _create_mock_entry([], 0, "file1.parquet"),
            _create_mock_entry([], 0, "file2.parquet"),
        ]

        mock_mlm = MockManifestListManager.return_value
        mock_mfm = MockManifestFileManager.return_value
        mock_mlm.read_base.side_effect = lambda s: [Mock(file_name=f"manifest-{s.id}")]
        mock_mfm.read_entries_parallel.side_effect = [entries, entries]

        scanner = IncrementalDiffScanner(table)
        snapshot = _create_mock_snapshot(5)

        added_entries = scanner.compute_diff(snapshot, snapshot)

        self.assertEqual(len(added_entries), 0)

    @patch('pypaimon.read.scanner.incremental_diff_scanner.ManifestListManager')
    @patch('pypaimon.read.scanner.incremental_diff_scanner.ManifestFileManager')
    def test_diff_groups_by_partition_bucket(self, MockManifestFileManager, MockManifestListManager):
        """Diff should be computed per (partition, bucket)."""
        from pypaimon.read.scanner.incremental_diff_scanner import \
            IncrementalDiffScanner

        table = _create_mock_table()

        start_entries = [
            _create_mock_entry([1], 0, "file1.parquet"),  # p1, bucket 0
            _create_mock_entry([1], 1, "file3.parquet"),  # p1, bucket 1
        ]
        end_entries = [
            _create_mock_entry([1], 0, "file1.parquet"),  # p1, bucket 0
            _create_mock_entry([1], 0, "file2.parquet"),  # p1, bucket 0 - NEW
            _create_mock_entry([1], 1, "file3.parquet"),  # p1, bucket 1
        ]

        mock_mlm = MockManifestListManager.return_value
        mock_mfm = MockManifestFileManager.return_value
        mock_mlm.read_base.side_effect = lambda s: [Mock(file_name=f"manifest-{s.id}")]
        mock_mfm.read_entries_parallel.side_effect = [start_entries, end_entries]

        scanner = IncrementalDiffScanner(table)
        start_snapshot = _create_mock_snapshot(1)
        end_snapshot = _create_mock_snapshot(5)

        added_entries = scanner.compute_diff(start_snapshot, end_snapshot)

        self.assertEqual(len(added_entries), 1)
        self.assertEqual(added_entries[0].file.file_name, "file2.parquet")

    @patch('pypaimon.read.scanner.incremental_diff_scanner.ManifestListManager')
    @patch('pypaimon.read.scanner.incremental_diff_scanner.ManifestFileManager')
    def test_diff_handles_file_deletions(self, MockManifestFileManager, MockManifestListManager):
        """Files in start but not in end (deleted) should NOT be returned."""
        from pypaimon.read.scanner.incremental_diff_scanner import \
            IncrementalDiffScanner

        table = _create_mock_table()

        start_entries = [
            _create_mock_entry([], 0, "file1.parquet"),
            _create_mock_entry([], 0, "file2.parquet"),
            _create_mock_entry([], 0, "file3.parquet"),
        ]
        end_entries = [
            _create_mock_entry([], 0, "file1.parquet"),
            _create_mock_entry([], 0, "file2.parquet"),
        ]

        mock_mlm = MockManifestListManager.return_value
        mock_mfm = MockManifestFileManager.return_value
        mock_mlm.read_base.side_effect = lambda s: [Mock(file_name=f"manifest-{s.id}")]
        mock_mfm.read_entries_parallel.side_effect = [start_entries, end_entries]

        scanner = IncrementalDiffScanner(table)
        start_snapshot = _create_mock_snapshot(1)
        end_snapshot = _create_mock_snapshot(5)

        added_entries = scanner.compute_diff(start_snapshot, end_snapshot)

        self.assertEqual(len(added_entries), 0)

    @patch('pypaimon.read.scanner.incremental_diff_scanner.ManifestListManager')
    @patch('pypaimon.read.scanner.incremental_diff_scanner.ManifestFileManager')
    def test_diff_creates_correct_splits(self, MockManifestFileManager, MockManifestListManager):
        """scan() should return a Plan with correct DataSplits."""
        from pypaimon.read.plan import Plan
        from pypaimon.read.scanner.incremental_diff_scanner import \
            IncrementalDiffScanner

        table = _create_mock_table()

        start_entries = []
        end_entries = [
            _create_mock_entry([], 0, "file1.parquet"),
        ]

        mock_mlm = MockManifestListManager.return_value
        mock_mfm = MockManifestFileManager.return_value
        mock_mlm.read_base.side_effect = lambda s: [Mock(file_name=f"manifest-{s.id}")]
        mock_mfm.read_entries_parallel.side_effect = [start_entries, end_entries]

        scanner = IncrementalDiffScanner(table)
        start_snapshot = _create_mock_snapshot(0)
        end_snapshot = _create_mock_snapshot(5)

        plan = scanner.scan(start_snapshot, end_snapshot)

        self.assertIsInstance(plan, Plan)
        splits = plan.splits()
        self.assertGreater(len(splits), 0)

    @patch('pypaimon.read.scanner.incremental_diff_scanner.ManifestListManager')
    @patch('pypaimon.read.scanner.incremental_diff_scanner.ManifestFileManager')
    def test_entry_key_uniqueness(self, MockManifestFileManager, MockManifestListManager):
        """Entry key should uniquely identify a file by (partition, bucket, filename)."""
        from pypaimon.read.scanner.incremental_diff_scanner import \
            IncrementalDiffScanner

        table = _create_mock_table()

        scanner = IncrementalDiffScanner(table)

        # Same file in different partitions should have different keys
        entry1 = _create_mock_entry([1], 0, "file.parquet")
        entry2 = _create_mock_entry([2], 0, "file.parquet")
        self.assertNotEqual(scanner._entry_key(entry1), scanner._entry_key(entry2))

        # Same file in different buckets should have different keys
        entry3 = _create_mock_entry([1], 0, "file.parquet")
        entry4 = _create_mock_entry([1], 1, "file.parquet")
        self.assertNotEqual(scanner._entry_key(entry3), scanner._entry_key(entry4))

        # Same partition/bucket/filename should have same key
        entry5 = _create_mock_entry([1], 0, "file.parquet")
        entry6 = _create_mock_entry([1], 0, "file.parquet")
        self.assertEqual(scanner._entry_key(entry5), scanner._entry_key(entry6))


class IncrementalDiffIntegrationTest(unittest.TestCase):
    """Integration tests comparing diff vs delta approaches."""

    @patch('pypaimon.read.scanner.incremental_diff_scanner.ManifestListManager')
    @patch('pypaimon.read.scanner.incremental_diff_scanner.ManifestFileManager')
    def test_diff_equals_delta_results(self, MockManifestFileManager, MockManifestListManager):
        """Diff and delta approaches should return the same added files."""
        from pypaimon.read.scanner.incremental_diff_scanner import \
            IncrementalDiffScanner

        table = _create_mock_table()

        base_entries_1 = [
            _create_mock_entry([], 0, "file1.parquet"),
            _create_mock_entry([], 0, "file2.parquet"),
        ]

        base_entries_5 = [
            _create_mock_entry([], 0, "file1.parquet"),
            _create_mock_entry([], 0, "file2.parquet"),
            _create_mock_entry([], 0, "file3.parquet"),
            _create_mock_entry([], 0, "file4.parquet"),
            _create_mock_entry([], 0, "file5.parquet"),
        ]

        # Delta manifests for each snapshot
        delta_2 = [_create_mock_entry([], 0, "file3.parquet")]
        delta_3 = []  # COMPACT - no new files
        delta_4 = [_create_mock_entry([], 0, "file4.parquet")]
        delta_5 = [_create_mock_entry([], 0, "file5.parquet")]

        mock_mlm = MockManifestListManager.return_value
        mock_mfm = MockManifestFileManager.return_value

        def mock_read_all(snapshot):
            if snapshot.id == 1:
                return [Mock(file_name="all-manifest-1")]
            elif snapshot.id == 5:
                return [Mock(file_name="all-manifest-5")]
            return []

        mock_mlm.read_all.side_effect = mock_read_all

        def mock_read_entries(manifests, *args, **kwargs):
            if manifests and manifests[0].file_name == "all-manifest-1":
                return base_entries_1
            elif manifests and manifests[0].file_name == "all-manifest-5":
                return base_entries_5
            return []

        mock_mfm.read_entries_parallel.side_effect = mock_read_entries

        scanner = IncrementalDiffScanner(table)
        start_snapshot = _create_mock_snapshot(1)
        end_snapshot = _create_mock_snapshot(5)

        diff_entries = scanner.compute_diff(start_snapshot, end_snapshot)
        diff_filenames = {e.file.file_name for e in diff_entries}

        delta_filenames = {
            e.file.file_name for entries in [delta_2, delta_3, delta_4, delta_5]
            for e in entries
        }

        self.assertEqual(diff_filenames, delta_filenames)
        self.assertEqual(diff_filenames, {"file3.parquet", "file4.parquet", "file5.parquet"})

    @patch('pypaimon.read.scanner.incremental_diff_scanner.ManifestListManager')
    @patch('pypaimon.read.scanner.incremental_diff_scanner.ManifestFileManager')
    def test_diff_handles_compaction_correctly(self, MockManifestFileManager, MockManifestListManager):
        """Compaction merging file1+file2 into file3 should return only file3."""
        from pypaimon.read.scanner.incremental_diff_scanner import \
            IncrementalDiffScanner

        table = _create_mock_table()

        base_entries_1 = [
            _create_mock_entry([], 0, "file1.parquet"),
            _create_mock_entry([], 0, "file2.parquet"),
        ]

        base_entries_5 = [
            _create_mock_entry([], 0, "file3.parquet"),  # Compacted result
        ]

        mock_mlm = MockManifestListManager.return_value
        mock_mfm = MockManifestFileManager.return_value

        def mock_read_all(snapshot):
            if snapshot.id == 1:
                return [Mock(file_name="all-manifest-1")]
            elif snapshot.id == 5:
                return [Mock(file_name="all-manifest-5")]
            return []

        mock_mlm.read_all.side_effect = mock_read_all

        def mock_read_entries(manifests, *args, **kwargs):
            if manifests and manifests[0].file_name == "all-manifest-1":
                return base_entries_1
            elif manifests and manifests[0].file_name == "all-manifest-5":
                return base_entries_5
            return []

        mock_mfm.read_entries_parallel.side_effect = mock_read_entries

        scanner = IncrementalDiffScanner(table)
        start_snapshot = _create_mock_snapshot(1)
        end_snapshot = _create_mock_snapshot(5)

        diff_entries = scanner.compute_diff(start_snapshot, end_snapshot)
        diff_filenames = {e.file.file_name for e in diff_entries}

        self.assertEqual(diff_filenames, {"file3.parquet"})


if __name__ == '__main__':
    unittest.main()
