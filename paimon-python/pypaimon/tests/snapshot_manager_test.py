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
"""
Tests for SnapshotManager batch lookahead functionality.
"""

import unittest
from unittest.mock import Mock


def _create_mock_snapshot(snapshot_id: int, commit_kind: str = "APPEND"):
    """Helper to create a mock snapshot."""
    from pypaimon.snapshot.snapshot import Snapshot
    snapshot = Mock(spec=Snapshot)
    snapshot.id = snapshot_id
    snapshot.commit_kind = commit_kind
    snapshot.time_millis = 1000000 + snapshot_id
    return snapshot


class SnapshotManagerTest(unittest.TestCase):
    """Tests for SnapshotManager batch lookahead methods."""

    def test_find_next_scannable_returns_first_matching(self):
        """find_next_scannable should return the first snapshot that passes should_scan."""
        from pypaimon.snapshot.snapshot_manager import SnapshotManager

        table = Mock()
        table.table_path = "/tmp/test_table"
        table.file_io = Mock()
        table.file_io.exists_batch.return_value = {
            "/tmp/test_table/snapshot/snapshot-5": True,
            "/tmp/test_table/snapshot/snapshot-6": True,
            "/tmp/test_table/snapshot/snapshot-7": True,
        }
        table.catalog_environment = Mock()
        table.catalog_environment.snapshot_loader.return_value = None

        # Create mock snapshots with different commit kinds
        snapshots = {
            5: _create_mock_snapshot(5, "COMPACT"),
            6: _create_mock_snapshot(6, "COMPACT"),
            7: _create_mock_snapshot(7, "APPEND"),
        }

        manager = SnapshotManager(table)

        # Mock get_snapshot_by_id to return our test snapshots
        def mock_get_snapshot(sid):
            return snapshots.get(sid)

        manager.get_snapshot_by_id = mock_get_snapshot

        # should_scan only accepts APPEND commits
        def should_scan(snapshot):
            return snapshot.commit_kind == "APPEND"

        result, next_id, skipped_count = manager.find_next_scannable(5, should_scan, lookahead_size=5)

        self.assertEqual(result.id, 7)  # First APPEND snapshot
        self.assertEqual(next_id, 8)    # Next ID to check
        self.assertEqual(skipped_count, 2)  # Skipped snapshots 5 and 6

    def test_find_next_scannable_returns_none_when_no_snapshot_exists(self):
        """find_next_scannable should return None when no snapshot exists at start_id."""
        from pypaimon.snapshot.snapshot_manager import SnapshotManager

        table = Mock()
        table.table_path = "/tmp/test_table"
        table.file_io = Mock()
        # All paths return False (no files exist)
        table.file_io.exists_batch.return_value = {}
        table.catalog_environment = Mock()
        table.catalog_environment.snapshot_loader.return_value = None

        manager = SnapshotManager(table)

        def should_scan(snapshot):
            return True

        result, next_id, skipped_count = manager.find_next_scannable(5, should_scan, lookahead_size=5)

        self.assertIsNone(result)
        self.assertEqual(next_id, 5)  # Still at start_id
        self.assertEqual(skipped_count, 0)

    def test_find_next_scannable_continues_when_all_skipped(self):
        """When all lookahead snapshots are skipped, next_id should be start+lookahead."""
        from pypaimon.snapshot.snapshot_manager import SnapshotManager

        table = Mock()
        table.table_path = "/tmp/test_table"
        table.file_io = Mock()

        # All 3 snapshots exist but are COMPACT (will be skipped)
        table.file_io.exists_batch.return_value = {
            "/tmp/test_table/snapshot/snapshot-5": True,
            "/tmp/test_table/snapshot/snapshot-6": True,
            "/tmp/test_table/snapshot/snapshot-7": True,
        }
        table.catalog_environment = Mock()
        table.catalog_environment.snapshot_loader.return_value = None

        snapshots = {
            5: _create_mock_snapshot(5, "COMPACT"),
            6: _create_mock_snapshot(6, "COMPACT"),
            7: _create_mock_snapshot(7, "COMPACT"),
        }

        manager = SnapshotManager(table)

        def mock_get_snapshot(sid):
            return snapshots.get(sid)

        manager.get_snapshot_by_id = mock_get_snapshot

        def should_scan(snapshot):
            return snapshot.commit_kind == "APPEND"

        result, next_id, skipped_count = manager.find_next_scannable(5, should_scan, lookahead_size=3)

        self.assertIsNone(result)  # No APPEND found
        self.assertEqual(next_id, 8)  # 5 + 3 = 8, continue from here
        self.assertEqual(skipped_count, 3)  # All 3 were skipped


if __name__ == '__main__':
    unittest.main()
