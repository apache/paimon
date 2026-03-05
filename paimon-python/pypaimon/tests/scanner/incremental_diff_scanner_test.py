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
"""
TDD tests for IncrementalDiffScanner.

These tests define the expected behavior BEFORE implementation.
Run these tests first - they should fail. Then implement the scanner
to make them pass.
"""

import unittest
from unittest.mock import Mock, patch


class IncrementalDiffScannerTest(unittest.TestCase):
    """TDD tests for IncrementalDiffScanner - write these FIRST."""

    def _create_mock_table(self):
        """Helper to create a mock table with all necessary options."""
        table = Mock()
        table.is_primary_key_table = False
        table.options = Mock()
        table.options.source_split_target_size.return_value = 128 * 1024 * 1024
        table.options.source_split_open_file_cost.return_value = 4 * 1024 * 1024
        table.options.scan_manifest_parallelism.return_value = 8
        table.partition_keys = []
        return table

    def _create_mock_entry(self, partition_values, bucket, filename):
        """Helper to create a mock ManifestEntry."""
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

    def _create_mock_snapshot(self, snapshot_id, base_manifest="base", delta_manifest="delta"):
        """Helper to create a mock Snapshot."""
        snapshot = Mock()
        snapshot.id = snapshot_id
        snapshot.base_manifest_list = f"{base_manifest}-{snapshot_id}"
        snapshot.delta_manifest_list = f"{delta_manifest}-{snapshot_id}"
        return snapshot

    @patch('pypaimon.read.scanner.incremental_diff_scanner.ManifestListManager')
    @patch('pypaimon.read.scanner.incremental_diff_scanner.ManifestFileManager')
    def test_diff_returns_only_added_files(self, MockManifestFileManager, MockManifestListManager):
        """Files in end but not in start should be returned."""
        from pypaimon.read.scanner.incremental_diff_scanner import \
            IncrementalDiffScanner

        table = self._create_mock_table()

        # Setup: start has file1, file2; end has file1, file2, file3, file4
        start_entries = [
            self._create_mock_entry([], 0, "file1.parquet"),
            self._create_mock_entry([], 0, "file2.parquet"),
        ]
        end_entries = [
            self._create_mock_entry([], 0, "file1.parquet"),
            self._create_mock_entry([], 0, "file2.parquet"),
            self._create_mock_entry([], 0, "file3.parquet"),
            self._create_mock_entry([], 0, "file4.parquet"),
        ]

        mock_mlm = MockManifestListManager.return_value
        mock_mfm = MockManifestFileManager.return_value

        # Mock read_base to return different manifests for each snapshot
        mock_mlm.read_base.side_effect = lambda s: [Mock(file_name=f"manifest-{s.id}")]
        mock_mfm.read_entries_parallel.side_effect = [start_entries, end_entries]

        scanner = IncrementalDiffScanner(table)
        start_snapshot = self._create_mock_snapshot(1)
        end_snapshot = self._create_mock_snapshot(5)

        added_entries = scanner.compute_diff(start_snapshot, end_snapshot)

        # Should only return file3 and file4 (new files)
        added_filenames = {e.file.file_name for e in added_entries}
        self.assertEqual(added_filenames, {"file3.parquet", "file4.parquet"})

    @patch('pypaimon.read.scanner.incremental_diff_scanner.ManifestListManager')
    @patch('pypaimon.read.scanner.incremental_diff_scanner.ManifestFileManager')
    def test_diff_handles_empty_start(self, MockManifestFileManager, MockManifestListManager):
        """When start is empty, all end files should be returned."""
        from pypaimon.read.scanner.incremental_diff_scanner import \
            IncrementalDiffScanner

        table = self._create_mock_table()

        # Setup: start is empty, end has file1, file2
        start_entries = []
        end_entries = [
            self._create_mock_entry([], 0, "file1.parquet"),
            self._create_mock_entry([], 0, "file2.parquet"),
        ]

        mock_mlm = MockManifestListManager.return_value
        mock_mfm = MockManifestFileManager.return_value
        mock_mlm.read_base.side_effect = lambda s: [Mock(file_name=f"manifest-{s.id}")]
        mock_mfm.read_entries_parallel.side_effect = [start_entries, end_entries]

        scanner = IncrementalDiffScanner(table)
        start_snapshot = self._create_mock_snapshot(0)
        end_snapshot = self._create_mock_snapshot(5)

        added_entries = scanner.compute_diff(start_snapshot, end_snapshot)

        # Should return all end files
        added_filenames = {e.file.file_name for e in added_entries}
        self.assertEqual(added_filenames, {"file1.parquet", "file2.parquet"})

    @patch('pypaimon.read.scanner.incremental_diff_scanner.ManifestListManager')
    @patch('pypaimon.read.scanner.incremental_diff_scanner.ManifestFileManager')
    def test_diff_handles_same_snapshots(self, MockManifestFileManager, MockManifestListManager):
        """When start == end (same files), diff should return empty."""
        from pypaimon.read.scanner.incremental_diff_scanner import \
            IncrementalDiffScanner

        table = self._create_mock_table()

        # Setup: both have same files
        entries = [
            self._create_mock_entry([], 0, "file1.parquet"),
            self._create_mock_entry([], 0, "file2.parquet"),
        ]

        mock_mlm = MockManifestListManager.return_value
        mock_mfm = MockManifestFileManager.return_value
        mock_mlm.read_base.side_effect = lambda s: [Mock(file_name=f"manifest-{s.id}")]
        mock_mfm.read_entries_parallel.side_effect = [entries, entries]

        scanner = IncrementalDiffScanner(table)
        snapshot = self._create_mock_snapshot(5)

        added_entries = scanner.compute_diff(snapshot, snapshot)

        # Should return empty (no new files)
        self.assertEqual(len(added_entries), 0)

    @patch('pypaimon.read.scanner.incremental_diff_scanner.ManifestListManager')
    @patch('pypaimon.read.scanner.incremental_diff_scanner.ManifestFileManager')
    def test_diff_groups_by_partition_bucket(self, MockManifestFileManager, MockManifestListManager):
        """Diff should be computed per (partition, bucket)."""
        from pypaimon.read.scanner.incremental_diff_scanner import \
            IncrementalDiffScanner

        table = self._create_mock_table()

        # Setup: partition p1/bucket 0 has file1 in both, file2 only in end
        #        partition p1/bucket 1 has file3 in both (no change)
        start_entries = [
            self._create_mock_entry([1], 0, "file1.parquet"),  # p1, bucket 0
            self._create_mock_entry([1], 1, "file3.parquet"),  # p1, bucket 1
        ]
        end_entries = [
            self._create_mock_entry([1], 0, "file1.parquet"),  # p1, bucket 0
            self._create_mock_entry([1], 0, "file2.parquet"),  # p1, bucket 0 - NEW
            self._create_mock_entry([1], 1, "file3.parquet"),  # p1, bucket 1
        ]

        mock_mlm = MockManifestListManager.return_value
        mock_mfm = MockManifestFileManager.return_value
        mock_mlm.read_base.side_effect = lambda s: [Mock(file_name=f"manifest-{s.id}")]
        mock_mfm.read_entries_parallel.side_effect = [start_entries, end_entries]

        scanner = IncrementalDiffScanner(table)
        start_snapshot = self._create_mock_snapshot(1)
        end_snapshot = self._create_mock_snapshot(5)

        added_entries = scanner.compute_diff(start_snapshot, end_snapshot)

        # Should only return file2 (from p1, bucket 0)
        self.assertEqual(len(added_entries), 1)
        self.assertEqual(added_entries[0].file.file_name, "file2.parquet")

    @patch('pypaimon.read.scanner.incremental_diff_scanner.ManifestListManager')
    @patch('pypaimon.read.scanner.incremental_diff_scanner.ManifestFileManager')
    def test_diff_handles_file_deletions(self, MockManifestFileManager, MockManifestListManager):
        """Files in start but not in end (deleted) should NOT be returned."""
        from pypaimon.read.scanner.incremental_diff_scanner import \
            IncrementalDiffScanner

        table = self._create_mock_table()

        # Setup: start has file1, file2, file3; end has file1, file2 (file3 was compacted away)
        start_entries = [
            self._create_mock_entry([], 0, "file1.parquet"),
            self._create_mock_entry([], 0, "file2.parquet"),
            self._create_mock_entry([], 0, "file3.parquet"),
        ]
        end_entries = [
            self._create_mock_entry([], 0, "file1.parquet"),
            self._create_mock_entry([], 0, "file2.parquet"),
        ]

        mock_mlm = MockManifestListManager.return_value
        mock_mfm = MockManifestFileManager.return_value
        mock_mlm.read_base.side_effect = lambda s: [Mock(file_name=f"manifest-{s.id}")]
        mock_mfm.read_entries_parallel.side_effect = [start_entries, end_entries]

        scanner = IncrementalDiffScanner(table)
        start_snapshot = self._create_mock_snapshot(1)
        end_snapshot = self._create_mock_snapshot(5)

        added_entries = scanner.compute_diff(start_snapshot, end_snapshot)

        # Should return empty (no NEW files, only deletions)
        self.assertEqual(len(added_entries), 0)

    @patch('pypaimon.read.scanner.incremental_diff_scanner.ManifestListManager')
    @patch('pypaimon.read.scanner.incremental_diff_scanner.ManifestFileManager')
    def test_diff_creates_correct_splits(self, MockManifestFileManager, MockManifestListManager):
        """scan() should return a Plan with correct DataSplits."""
        from pypaimon.read.plan import Plan
        from pypaimon.read.scanner.incremental_diff_scanner import \
            IncrementalDiffScanner

        table = self._create_mock_table()

        # Setup: start empty, end has file1
        start_entries = []
        end_entries = [
            self._create_mock_entry([], 0, "file1.parquet"),
        ]

        mock_mlm = MockManifestListManager.return_value
        mock_mfm = MockManifestFileManager.return_value
        mock_mlm.read_base.side_effect = lambda s: [Mock(file_name=f"manifest-{s.id}")]
        mock_mfm.read_entries_parallel.side_effect = [start_entries, end_entries]

        scanner = IncrementalDiffScanner(table)
        start_snapshot = self._create_mock_snapshot(0)
        end_snapshot = self._create_mock_snapshot(5)

        plan = scanner.scan(start_snapshot, end_snapshot)

        # Should return a Plan
        self.assertIsInstance(plan, Plan)
        # Plan should have splits
        splits = plan.splits()
        self.assertGreater(len(splits), 0)

    @patch('pypaimon.read.scanner.incremental_diff_scanner.ManifestListManager')
    @patch('pypaimon.read.scanner.incremental_diff_scanner.ManifestFileManager')
    def test_entry_key_uniqueness(self, MockManifestFileManager, MockManifestListManager):
        """Entry key should uniquely identify a file by (partition, bucket, filename)."""
        from pypaimon.read.scanner.incremental_diff_scanner import \
            IncrementalDiffScanner

        table = self._create_mock_table()

        scanner = IncrementalDiffScanner(table)

        # Same file in different partitions should have different keys
        entry1 = self._create_mock_entry([1], 0, "file.parquet")
        entry2 = self._create_mock_entry([2], 0, "file.parquet")
        self.assertNotEqual(scanner._entry_key(entry1), scanner._entry_key(entry2))

        # Same file in different buckets should have different keys
        entry3 = self._create_mock_entry([1], 0, "file.parquet")
        entry4 = self._create_mock_entry([1], 1, "file.parquet")
        self.assertNotEqual(scanner._entry_key(entry3), scanner._entry_key(entry4))

        # Same partition/bucket/filename should have same key
        entry5 = self._create_mock_entry([1], 0, "file.parquet")
        entry6 = self._create_mock_entry([1], 0, "file.parquet")
        self.assertEqual(scanner._entry_key(entry5), scanner._entry_key(entry6))


class IncrementalDiffIntegrationTest(unittest.TestCase):
    """Integration tests comparing diff vs delta approaches."""

    def _create_mock_table(self):
        """Helper to create a mock table with all necessary options."""
        table = Mock()
        table.is_primary_key_table = False
        table.options = Mock()
        table.options.source_split_target_size.return_value = 128 * 1024 * 1024
        table.options.source_split_open_file_cost.return_value = 4 * 1024 * 1024
        table.options.scan_manifest_parallelism.return_value = 8
        table.partition_keys = []
        return table

    def _create_mock_entry(self, partition_values, bucket, filename):
        """Helper to create a mock ManifestEntry."""
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

    def _create_mock_snapshot(self, snapshot_id, commit_kind="APPEND"):
        """Helper to create a mock Snapshot."""
        snapshot = Mock()
        snapshot.id = snapshot_id
        snapshot.commit_kind = commit_kind
        snapshot.base_manifest_list = f"base-manifest-list-{snapshot_id}"
        snapshot.delta_manifest_list = f"delta-manifest-list-{snapshot_id}"
        return snapshot

    @patch('pypaimon.read.scanner.incremental_diff_scanner.ManifestListManager')
    @patch('pypaimon.read.scanner.incremental_diff_scanner.ManifestFileManager')
    def test_diff_equals_delta_results(self, MockManifestFileManager, MockManifestListManager):
        """
        Diff and delta approaches should return the same files.

        Scenario:
        - Snapshot 1: file1, file2 (base state)
        - Snapshot 2: adds file3 (delta: file3)
        - Snapshot 3: COMPACT - no new files (delta: empty)
        - Snapshot 4: adds file4 (delta: file4)
        - Snapshot 5: adds file5 (delta: file5)

        Diff approach: end_files(5) - start_files(1) = file3, file4, file5
        Delta approach: sum of deltas from 2,3,4,5 = file3 + empty + file4 + file5
        Both should equal: {file3, file4, file5}
        """
        from pypaimon.read.scanner.incremental_diff_scanner import \
            IncrementalDiffScanner

        table = self._create_mock_table()

        # Base state at snapshot 1
        base_entries_1 = [
            self._create_mock_entry([], 0, "file1.parquet"),
            self._create_mock_entry([], 0, "file2.parquet"),
        ]

        # Base state at snapshot 5 (cumulative)
        base_entries_5 = [
            self._create_mock_entry([], 0, "file1.parquet"),
            self._create_mock_entry([], 0, "file2.parquet"),
            self._create_mock_entry([], 0, "file3.parquet"),
            self._create_mock_entry([], 0, "file4.parquet"),
            self._create_mock_entry([], 0, "file5.parquet"),
        ]

        # Delta manifests for each snapshot
        delta_2 = [self._create_mock_entry([], 0, "file3.parquet")]
        delta_3 = []  # COMPACT - no new files
        delta_4 = [self._create_mock_entry([], 0, "file4.parquet")]
        delta_5 = [self._create_mock_entry([], 0, "file5.parquet")]

        mock_mlm = MockManifestListManager.return_value
        mock_mfm = MockManifestFileManager.return_value

        # Mock read_all for diff approach (reads both base + delta to get full state)
        def mock_read_all(snapshot):
            if snapshot.id == 1:
                return [Mock(file_name="all-manifest-1")]
            elif snapshot.id == 5:
                return [Mock(file_name="all-manifest-5")]
            return []

        mock_mlm.read_all.side_effect = mock_read_all

        # Mock read_entries_parallel for manifest files
        def mock_read_entries(manifests, *args, **kwargs):
            if manifests and manifests[0].file_name == "all-manifest-1":
                return base_entries_1
            elif manifests and manifests[0].file_name == "all-manifest-5":
                return base_entries_5
            return []

        mock_mfm.read_entries_parallel.side_effect = mock_read_entries

        # Test diff approach
        scanner = IncrementalDiffScanner(table)
        start_snapshot = self._create_mock_snapshot(1)
        end_snapshot = self._create_mock_snapshot(5)

        diff_entries = scanner.compute_diff(start_snapshot, end_snapshot)
        diff_filenames = {e.file.file_name for e in diff_entries}

        # Delta approach would sum: file3 + file4 + file5
        delta_filenames = {
            e.file.file_name for entries in [delta_2, delta_3, delta_4, delta_5]
            for e in entries
        }

        # Both approaches should return the same files
        self.assertEqual(diff_filenames, delta_filenames)
        self.assertEqual(diff_filenames, {"file3.parquet", "file4.parquet", "file5.parquet"})

    @patch('pypaimon.read.scanner.incremental_diff_scanner.ManifestListManager')
    @patch('pypaimon.read.scanner.incremental_diff_scanner.ManifestFileManager')
    def test_diff_handles_compaction_correctly(self, MockManifestFileManager, MockManifestListManager):
        """
        Diff approach should correctly handle compaction scenarios.

        When compaction merges file1+file2 into file3:
        - Snapshot 1: file1, file2
        - Snapshot 5: file3 (compacted)

        Diff should return file3 (new file), not file1 or file2.
        This is different from delta approach which would show:
        - DELETE file1, DELETE file2, ADD file3

        For streaming catch-up (append-only), we only want NEW files.
        """
        from pypaimon.read.scanner.incremental_diff_scanner import \
            IncrementalDiffScanner

        table = self._create_mock_table()

        # Base state at snapshot 1
        base_entries_1 = [
            self._create_mock_entry([], 0, "file1.parquet"),
            self._create_mock_entry([], 0, "file2.parquet"),
        ]

        # Base state at snapshot 5 (after compaction)
        base_entries_5 = [
            self._create_mock_entry([], 0, "file3.parquet"),  # Compacted result
        ]

        mock_mlm = MockManifestListManager.return_value
        mock_mfm = MockManifestFileManager.return_value

        # Mock read_all (reads both base + delta to get full state)
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
        start_snapshot = self._create_mock_snapshot(1)
        end_snapshot = self._create_mock_snapshot(5)

        diff_entries = scanner.compute_diff(start_snapshot, end_snapshot)
        diff_filenames = {e.file.file_name for e in diff_entries}

        # Should return file3 (new compacted file), not file1/file2
        self.assertEqual(diff_filenames, {"file3.parquet"})


if __name__ == '__main__':
    unittest.main()
