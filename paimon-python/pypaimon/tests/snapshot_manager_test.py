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
from contextlib import contextmanager, ExitStack
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from pypaimon.snapshot.snapshot_manager import (
    LATEST_SNAPSHOT_RELOCATE_ATTEMPTS, SnapshotNotResolvable)


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

    def test_rest_snapshot_response_can_be_empty(self):
        from pypaimon.api.api_response import GetTableSnapshotResponse
        from pypaimon.common.json_util import JSON
        for payload in ('{}', '{"snapshot": null}'):
            self.assertIsNone(JSON.from_json(payload, GetTableSnapshotResponse).get_snapshot())

    def test_only_unsupported_rest_snapshot_loader_falls_back(self):
        from pypaimon.api.rest_exception import NotImplementedException, RESTException
        from pypaimon.snapshot.snapshot_loader import SnapshotLoader
        from pypaimon.snapshot.snapshot_manager import SnapshotManager
        catalog_loader = Mock()
        catalog_loader.load.return_value.load_snapshot.side_effect = NotImplementedError('unsupported')
        manager = SnapshotManager(Mock(), '/table', snapshot_loader=SnapshotLoader(catalog_loader, Mock()))
        snapshot = _create_mock_snapshot(2)
        manager._get_latest_snapshot_from_filesystem = Mock(return_value=snapshot)
        self.assertIs(manager.get_latest_snapshot(), snapshot)
        manager._get_latest_snapshot_from_filesystem.assert_called_once()
        manager._get_latest_snapshot_from_filesystem.reset_mock()
        for error in (RESTException('unavailable'), NotImplementedException('unavailable')):
            catalog_loader.load.return_value.load_snapshot.side_effect = error
            with self.assertRaisesRegex(RuntimeError, 'unavailable'):
                manager.get_latest_snapshot()
            manager._get_latest_snapshot_from_filesystem.assert_not_called()

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

    def test_relocates_when_resolved_snapshot_expires_before_the_read(self):
        # The snapshot picked by the resolution is expired before the read that
        # follows it: another writer committed snapshot 2 and retention dropped
        # snapshot 1. The replacement is readable, so this is a recoverable
        # race -- the vanished file must not reach the caller.
        present = {self.SNAP_DIR, self.LATEST, self._snap(1)}
        file_io = Mock()
        file_io.exists.side_effect = lambda p: p in present

        def _read(p):
            if p == self.LATEST:
                return "1"
            if p == self._snap(1):
                # Reading the resolved candidate is what races the cleanup:
                # snapshot 1 goes away and snapshot 2 takes its place.
                present.discard(self._snap(1))
                present.add(self._snap(2))
                raise FileNotFoundError(p)
            return "content-" + p.rsplit("-", 1)[-1]

        file_io.read_file_utf8.side_effect = _read
        file_io.list_status.side_effect = lambda _: [
            _file_info(p) for p in sorted(present) if "snapshot-" in p]
        manager = _build_manager(file_io)
        with patch(
                'pypaimon.snapshot.snapshot_manager.JSON.from_json',
                side_effect=lambda content, cls: content):
            result = manager._get_latest_snapshot_from_filesystem()
        self.assertEqual(result, "content-2")

    def test_never_reports_empty_when_every_candidate_expires(self):
        # Pathological case: each resolved candidate is gone by the time it is
        # read. The table is not empty, so this must fail closed rather than
        # return None, which a caller would read as "nothing was committed".
        present = {self.SNAP_DIR, self.LATEST, self._snap(1)}
        counter = [1]
        file_io = Mock()
        file_io.exists.side_effect = lambda p: p in present

        def _read(p):
            if p == self.LATEST:
                return str(counter[0])
            # Every candidate vanishes as it is read, replaced by a newer one.
            present.discard(self._snap(counter[0]))
            counter[0] += 1
            present.add(self._snap(counter[0]))
            raise FileNotFoundError(p)

        file_io.read_file_utf8.side_effect = _read
        file_io.list_status.side_effect = lambda _: [
            _file_info(p) for p in sorted(present) if "snapshot-" in p]
        manager = _build_manager(file_io)
        from pypaimon.snapshot.snapshot_manager import SnapshotNotResolvable
        with self.assertRaises(SnapshotNotResolvable):
            manager._get_latest_snapshot_from_filesystem()

    def test_a_read_error_that_retrying_cannot_fix_is_not_retried(self):
        # Only a vanished file is a race. A permission error must surface as
        # itself rather than be re-read as if the snapshot had merely moved.
        present = {self.SNAP_DIR, self.LATEST, self._snap(1)}
        reads = []
        file_io = Mock()
        file_io.exists.side_effect = lambda p: p in present

        def _read(p):
            if p == self.LATEST:
                return "1"
            reads.append(p)
            raise PermissionError(p)

        file_io.read_file_utf8.side_effect = _read
        file_io.list_status.return_value = [_file_info(self._snap(1))]
        manager = _build_manager(file_io)
        with self.assertRaises(PermissionError):
            manager._get_latest_snapshot_from_filesystem()
        self.assertEqual(len(reads), 1, "a permission error must not be retried")


@pytest.mark.parametrize('evidence', ['snapshot', 'hint', 'corrupt-hint', 'unreadable-hint'])
@pytest.mark.parametrize('recover', [True, False], ids=['eventual-recovery', 'exhausted'])
def test_preserves_snapshot_evidence_after_empty_listing(evidence, recover):
    file_io = Mock()
    manager = _build_manager(file_io)
    first_snapshot = manager.get_snapshot_path(1)
    latest_snapshot = manager.get_snapshot_path(3)
    present = {manager.snapshot_dir}
    if evidence != 'snapshot':
        present.add(manager.latest_file)
    file_io.exists.side_effect = lambda path: path in present
    read_error = (FileNotFoundError(first_snapshot) if evidence == 'snapshot'
                  else PermissionError('Cannot read LATEST') if evidence == 'unreadable-hint'
                  else None)

    def read_file(path):
        if path == manager.latest_file:
            present.remove(path)
            if read_error is not None:
                raise read_error
            return 'invalid' if evidence == 'corrupt-hint' else '1'
        if path == first_snapshot:
            raise read_error
        assert path == latest_snapshot
        return 'snapshot-3'

    file_io.read_file_utf8.side_effect = read_file
    attempts = LATEST_SNAPSHOT_RELOCATE_ATTEMPTS + 1
    listings = [[_file_info(first_snapshot)] if evidence == 'snapshot' else []]
    listings.extend([[] for attempt in range(attempts - 1)])
    if recover:
        listings[-1] = [_file_info(latest_snapshot)]
    file_io.list_status.side_effect = listings

    with patch('pypaimon.snapshot.snapshot_manager.JSON.from_json',
               side_effect=lambda content, cls: content):
        if recover:
            assert manager.get_latest_snapshot() == 'snapshot-3'
        else:
            with pytest.raises(SnapshotNotResolvable) as raised:
                manager.get_latest_snapshot()
            assert manager.snapshot_dir in str(raised.value)
            assert str(attempts) in str(raised.value)
            assert raised.value.__cause__ is read_error
            if read_error is not None:
                assert str(read_error) in str(raised.value)
    assert file_io.list_status.call_count == attempts


@pytest.mark.parametrize('layout', ['missing-directory', 'missing-hint', 'empty-hint'])
def test_genuine_empty_snapshot_resolution(layout):
    file_io = Mock()
    manager = _build_manager(file_io)
    present = set() if layout == 'missing-directory' else {manager.snapshot_dir}
    if layout == 'empty-hint':
        present.add(manager.latest_file)
    file_io.exists.side_effect = lambda path: path in present
    file_io.read_file_utf8.return_value = ' \n'
    file_io.list_status.return_value = []
    assert manager.get_latest_snapshot() is None
    assert file_io.list_status.call_count == (0 if layout == 'missing-directory' else 1)


@pytest.mark.parametrize('failure', ['permission', 'io', 'parse'])
def test_snapshot_read_errors_after_relocation_are_not_retried(failure):
    file_io = Mock()
    manager = _build_manager(file_io)
    file_io.exists.side_effect = lambda path: path == manager.snapshot_dir
    file_io.list_status.side_effect = [
        [_file_info(manager.get_snapshot_path(1))], [],
        [_file_info(manager.get_snapshot_path(3))]]
    read_error = {'permission': PermissionError('denied'), 'io': OSError('read failed')}
    file_io.read_file_utf8.side_effect = [
        FileNotFoundError('expired snapshot'), read_error.get(failure, '{')]
    with pytest.raises(type(read_error[failure]) if failure != 'parse' else ValueError) as raised:
        manager.get_latest_snapshot()
    if failure != 'parse':
        assert raised.value is read_error[failure]
    assert file_io.read_file_utf8.call_count == 2
    assert file_io.list_status.call_count == 3


@contextmanager
def _prepared_snapshot_append(table, value):
    import pyarrow as pa

    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    committer = builder.new_commit()
    try:
        writer.write_arrow(pa.table({'value': pa.array([value], type=pa.int64())}))
        messages = writer.prepare_commit()
        yield lambda: committer.commit(messages)
    finally:
        writer.close()
        committer.close()


def _append_without_latest_hint(table, value):
    with _prepared_snapshot_append(table, value) as commit:
        commit()
    latest_file = Path(table.snapshot_manager().latest_file)
    if latest_file.exists():
        latest_file.unlink()


def _snapshot_table_values(table):
    builder = table.new_read_builder()
    splits = builder.new_scan().plan().splits()
    return sorted(builder.new_read().to_arrow(splits)['value'].to_pylist())


@pytest.fixture
def snapshot_race_table(tmp_path):
    import pyarrow as pa
    from pypaimon import CatalogFactory, Schema

    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = Schema.from_pyarrow_schema(pa.schema([('value', pa.int64())]), options={
        'read.native.enabled': 'false', 'write.native.enabled': 'false',
        'commit.native.enabled': 'false', 'scan.native-plan.enabled': 'false'})
    catalog.create_table('default.relocation', schema, False)
    table = catalog.get_table('default.relocation')
    _append_without_latest_hint(table, 1)
    return catalog, table


@pytest.mark.parametrize('operation', ['read', 'append', 'alter'])
def test_snapshot_evidence_survives_real_retention_races(snapshot_race_table, operation):
    """Publish real batches while retention races a read and the retry's stat calls."""
    from pypaimon.filesystem.local_file_io import LocalFileIO
    from pypaimon.schema.schema_change import SchemaChange

    catalog, table = snapshot_race_table
    manager = table.snapshot_manager()
    snapshot_dir = Path(manager.snapshot_dir)
    phase = ['await-first-read']
    events = []
    actual_read = LocalFileIO.read_file_utf8
    actual_iterdir = Path.iterdir

    def interleaved_read(file_io, path):
        if phase[0] == 'await-first-read' and path == manager.get_snapshot_path(1):
            phase[0] = 'publishing-second'
            _append_without_latest_hint(table, 2)
            Path(path).unlink()
            phase[0] = 'await-retry-listing'
            events.append('expired-before-read')
        return actual_read(file_io, path)

    def interleaved_iterdir(directory):
        captured = list(actual_iterdir(directory))
        if directory == snapshot_dir and phase[0] == 'await-retry-listing':
            assert captured == [snapshot_dir / 'snapshot-2']
            phase[0] = 'publishing-third'
            _append_without_latest_hint(table, 3)
            captured[0].unlink()
            phase[0] = 'finished'
            events.append('expired-before-stat')
        return iter(captured)

    with ExitStack() as stack:
        if operation == 'append':
            commit = stack.enter_context(_prepared_snapshot_append(table, 4))
        stack.enter_context(patch.object(LocalFileIO, 'read_file_utf8', interleaved_read))
        stack.enter_context(patch.object(Path, 'iterdir', interleaved_iterdir))
        if operation == 'read':
            assert _snapshot_table_values(table) == [1, 2, 3]
        elif operation == 'append':
            commit()
        else:
            with pytest.raises(RuntimeError, match=r'row-tracking\.enabled') as rejected:
                catalog.alter_table('default.relocation', [
                    SchemaChange.set_option('row-tracking.enabled', 'true')])
            assert isinstance(rejected.value.__cause__, ValueError)

    assert events == ['expired-before-read', 'expired-before-stat']
    assert _snapshot_table_values(table) == ([1, 2, 3, 4] if operation == 'append' else [1, 2, 3])
    assert manager.get_latest_snapshot().id == (4 if operation == 'append' else 3)
    assert catalog.get_table('default.relocation').table_schema.options.get('row-tracking.enabled') is None


if __name__ == '__main__':
    unittest.main()
