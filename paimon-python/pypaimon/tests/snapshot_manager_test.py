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

"""
Tests for SnapshotManager batch lookahead functionality.
"""

import unittest
from unittest.mock import Mock, patch


def _create_mock_snapshot(snapshot_id: int, commit_kind: str = "APPEND"):
    """Helper to create a mock snapshot."""
    from pypaimon.snapshot.snapshot import Snapshot
    snapshot = Mock(spec=Snapshot)
    snapshot.id = snapshot_id
    snapshot.commit_kind = commit_kind
    snapshot.time_millis = 1000000 + snapshot_id
    return snapshot


def _create_mock_snapshot_with_time(snapshot_id: int, time_millis: int):
    snapshot = _create_mock_snapshot(snapshot_id)
    snapshot.time_millis = time_millis
    return snapshot


def _build_manager(file_io):
    from pypaimon.snapshot.snapshot_manager import SnapshotManager
    return SnapshotManager(file_io, "/tmp/test_table")


class SnapshotManagerTest(unittest.TestCase):
    """Tests for SnapshotManager batch lookahead methods."""

    def test_find_next_scannable_returns_first_matching(self):
        """find_next_scannable should return the first snapshot that passes should_scan."""
        file_io = Mock()
        file_io.exists_batch.return_value = {
            "/tmp/test_table/snapshot/snapshot-5": True,
            "/tmp/test_table/snapshot/snapshot-6": True,
            "/tmp/test_table/snapshot/snapshot-7": True,
        }

        snapshots = {
            5: _create_mock_snapshot(5, "COMPACT"),
            6: _create_mock_snapshot(6, "COMPACT"),
            7: _create_mock_snapshot(7, "APPEND"),
        }

        manager = _build_manager(file_io)
        manager.get_snapshot_by_id = lambda sid: snapshots.get(sid)

        def should_scan(snapshot):
            return snapshot.commit_kind == "APPEND"

        result, next_id, skipped_count = manager.find_next_scannable(5, should_scan, lookahead_size=5)

        self.assertEqual(result.id, 7)
        self.assertEqual(next_id, 8)
        self.assertEqual(skipped_count, 2)

    def test_find_next_scannable_returns_none_when_no_snapshot_exists(self):
        """find_next_scannable should return None when no snapshot exists at start_id."""
        file_io = Mock()
        file_io.exists_batch.return_value = {}

        manager = _build_manager(file_io)

        def should_scan(snapshot):
            return True

        result, next_id, skipped_count = manager.find_next_scannable(5, should_scan, lookahead_size=5)

        self.assertIsNone(result)
        self.assertEqual(next_id, 5)
        self.assertEqual(skipped_count, 0)

    def test_find_next_scannable_continues_when_all_skipped(self):
        """When all lookahead snapshots are skipped, next_id should be start+lookahead."""
        file_io = Mock()
        file_io.exists_batch.return_value = {
            "/tmp/test_table/snapshot/snapshot-5": True,
            "/tmp/test_table/snapshot/snapshot-6": True,
            "/tmp/test_table/snapshot/snapshot-7": True,
        }

        snapshots = {
            5: _create_mock_snapshot(5, "COMPACT"),
            6: _create_mock_snapshot(6, "COMPACT"),
            7: _create_mock_snapshot(7, "COMPACT"),
        }

        manager = _build_manager(file_io)
        manager.get_snapshot_by_id = lambda sid: snapshots.get(sid)

        def should_scan(snapshot):
            return snapshot.commit_kind == "APPEND"

        result, next_id, skipped_count = manager.find_next_scannable(5, should_scan, lookahead_size=3)

        self.assertIsNone(result)
        self.assertEqual(next_id, 8)
        self.assertEqual(skipped_count, 3)

    def test_earlier_or_equal_time_mills_skips_missing_earliest_snapshot(self):
        """earlier_or_equal_time_mills should retry if earliest snapshot disappeared."""
        file_io = Mock()
        file_io.exists.return_value = True
        file_io.read_file_utf8.return_value = "1"

        snapshots = {
            2: _create_mock_snapshot_with_time(2, 2000),
            3: _create_mock_snapshot_with_time(3, 3000),
        }

        manager = _build_manager(file_io)
        manager.get_latest_snapshot = lambda: snapshots[3]
        manager.get_snapshot_by_id = lambda sid: snapshots.get(sid)

        result = manager.earlier_or_equal_time_mills(2500)

        self.assertEqual(result.id, 2)

    def test_try_get_earliest_snapshot_retries_beyond_one_hundred_missing_snapshots(self):
        file_io = Mock()
        file_io.exists.return_value = True
        file_io.read_file_utf8.return_value = "1"

        snapshot = _create_mock_snapshot_with_time(105, 105000)

        manager = _build_manager(file_io)
        manager.get_snapshot_by_id = lambda sid: snapshot if sid == 105 else None

        result = manager.try_get_earliest_snapshot()

        self.assertEqual(result.id, 105)

    def test_try_get_earliest_snapshot_throws_when_retry_exhausted(self):
        file_io = Mock()
        file_io.exists.return_value = True
        file_io.read_file_utf8.return_value = "1"

        manager = _build_manager(file_io)
        manager.get_snapshot_by_id = lambda sid: None

        with self.assertRaisesRegex(RuntimeError, "Cannot find earliest snapshot"):
            manager.try_get_earliest_snapshot()


def _file_info(path):
    info = Mock()
    info.path = path
    return info


class SnapshotManagerFilesystemHintTest(unittest.TestCase):
    """Tests for _get_latest_snapshot_from_filesystem hint reconciliation."""

    SNAP_DIR = "/tmp/test_table/snapshot"
    LATEST = "/tmp/test_table/snapshot/LATEST"

    def _snap(self, n):
        return "{}/snapshot-{}".format(self.SNAP_DIR, n)

    def test_ignores_stale_latest_hint(self):
        # LATEST still points to snapshot 1 but snapshot 2 was committed (a
        # lagging _commit_latest_hint); the newer snapshot must win.
        present = {self.SNAP_DIR, self.LATEST, self._snap(1), self._snap(2)}
        file_io = Mock()
        file_io.exists.side_effect = lambda p: p in present
        file_io.read_file_utf8.side_effect = (
            lambda p: "1" if p == self.LATEST else "content-" + p.rsplit("-", 1)[-1])
        file_io.list_status.return_value = [
            _file_info(self._snap(1)), _file_info(self._snap(2))]
        manager = _build_manager(file_io)
        with patch(
                'pypaimon.snapshot.snapshot_manager.JSON.from_json',
                side_effect=lambda content, cls: content):
            result = manager._get_latest_snapshot_from_filesystem()
        self.assertEqual(result, "content-2")

    def test_fails_closed_when_hint_unreadable_and_listing_empty(self):
        # LATEST exists (the table is non-empty) but cannot be read, and the
        # listing came back empty (e.g. a swallowed PermissionError). Returning
        # None here would let the immutable-option guard fail open.
        present = {self.SNAP_DIR, self.LATEST}
        file_io = Mock()
        file_io.exists.side_effect = lambda p: p in present

        def _read(p):
            if p == self.LATEST:
                raise IOError("permission denied")
            raise AssertionError("unexpected read of " + p)

        file_io.read_file_utf8.side_effect = _read
        file_io.list_status.return_value = []
        manager = _build_manager(file_io)
        with self.assertRaises(RuntimeError):
            manager._get_latest_snapshot_from_filesystem()

    def test_returns_none_when_table_genuinely_empty(self):
        # No LATEST and no snapshot files: a real empty table, not an error.
        present = {self.SNAP_DIR}
        file_io = Mock()
        file_io.exists.side_effect = lambda p: p in present
        file_io.list_status.return_value = []
        manager = _build_manager(file_io)
        self.assertIsNone(manager._get_latest_snapshot_from_filesystem())


if __name__ == '__main__':
    unittest.main()
