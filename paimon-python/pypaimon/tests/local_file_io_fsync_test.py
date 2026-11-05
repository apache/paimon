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

"""Durability tests for LocalFileIO fsync (#10739)."""

import errno
import os
import stat
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.filesystem.local_file_io import LocalFileIO, _fsync_directory


class LocalFileIOFsyncTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self._tmpdir.name).resolve()
        self.io = LocalFileIO()

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_try_to_write_atomic_fsyncs_temp_before_rename_then_parent_dir(self):
        target = self.root / "snapshot-1"
        content = '{"id":1}'
        events = []
        open_paths = {}
        real_open, real_os_open = open, os.open
        real_fsync, real_close = os.fsync, os.close
        real_rename = Path.rename
        renamed = []

        def tracking_open(file, mode="r", *a, **k):
            f = real_open(file, mode, *a, **k)
            open_paths[f.fileno()] = ("file", str(Path(file).resolve()))
            return f

        def tracking_os_open(path, flags, *a, **k):
            fd = real_os_open(path, flags, *a, **k)
            p = Path(path)
            open_paths[fd] = ("dir" if p.is_dir() else "file", str(p.resolve()))
            return fd

        def tracking_fsync(fd):
            kind, path = open_paths.get(fd, ("unknown", "fd:%s" % fd))
            events.append((kind, path))
            return real_fsync(fd)

        def tracking_close(fd):
            try:
                return real_close(fd)
            finally:
                open_paths.pop(fd, None)

        def tracking_rename(self_path, dst):
            # File data must already be fsynced before publish.
            self.assertTrue(
                any(k == "file" for k, _ in events),
                "file fsync missing before rename: %s" % (events,),
            )
            renamed.append((str(self_path), str(dst)))
            return real_rename(self_path, dst)

        with mock.patch("builtins.open", side_effect=tracking_open), \
                mock.patch("os.open", side_effect=tracking_os_open), \
                mock.patch("os.fsync", side_effect=tracking_fsync), \
                mock.patch("os.close", side_effect=tracking_close), \
                mock.patch.object(Path, "rename", tracking_rename):
            ok = self.io.try_to_write_atomic(str(target), content)

        self.assertTrue(ok)
        self.assertEqual(target.read_text(encoding="utf-8"), content)
        self.assertEqual(len(renamed), 1)
        file_idxs = [i for i, (k, _) in enumerate(events) if k == "file"]
        dir_idxs = [i for i, (k, p) in enumerate(events)
                    if k == "dir" and p == str(self.root)]
        self.assertTrue(file_idxs, events)
        self.assertTrue(dir_idxs, events)
        self.assertLess(file_idxs[0], dir_idxs[0])

    def test_try_to_write_atomic_fsync_failure_does_not_publish_target(self):
        target = self.root / "snap-fail"
        real_fsync = os.fsync
        n = {"c": 0}

        def boom(fd):
            # First fsync in try_to_write_atomic is the temp file after write.
            n["c"] += 1
            if n["c"] == 1:
                raise OSError(5, "simulated fsync failure")
            return real_fsync(fd)

        with mock.patch("os.fsync", side_effect=boom):
            with self.assertRaises(OSError) as raised:
                self.io.try_to_write_atomic(str(target), '{"id":9}')

        self.assertEqual(raised.exception.errno, errno.EIO)
        self.assertEqual(raised.exception.__cause__.errno, errno.EIO)
        self.assertFalse(target.exists())
        self.assertEqual(list(self.root.iterdir()), [])

    def test_directory_sync_propagates_storage_errors_and_closes_fd(self):
        for error in (errno.EIO, errno.ENOSPC, errno.EACCES, errno.EBADF):
            with self.subTest(errno=error):
                with mock.patch("os.fsync", side_effect=OSError(error, "sync failed")), \
                        mock.patch("os.close", wraps=os.close) as close:
                    with self.assertRaises(OSError) as raised:
                        _fsync_directory(self.root)
                self.assertEqual(raised.exception.errno, error)
                close.assert_called_once()

    def test_directory_open_propagates_storage_errors(self):
        for error in (errno.EIO, errno.ENOSPC, errno.ENOENT):
            with self.subTest(errno=error):
                with mock.patch("os.open", side_effect=OSError(error, "open failed")):
                    with self.assertRaises(OSError) as raised:
                        _fsync_directory(self.root)
                self.assertEqual(raised.exception.errno, error)

    def test_directory_sync_ignores_only_unsupported_operations(self):
        for error in (errno.EINVAL, errno.ENOTSUP, errno.ENOSYS):
            with self.subTest(errno=error):
                with mock.patch("os.fsync", side_effect=OSError(error, "unsupported")), \
                        mock.patch("os.close", wraps=os.close) as close:
                    _fsync_directory(self.root)
                close.assert_called_once()

    def test_directory_open_unsupported_and_windows_access_denied(self):
        for error in (errno.ENOTSUP, errno.ENOSYS):
            with self.subTest(errno=error):
                with mock.patch("os.open", side_effect=OSError(error, "unsupported")):
                    _fsync_directory(self.root)
        with mock.patch("os.name", "nt"), \
                mock.patch("os.open", side_effect=PermissionError(errno.EACCES, "directory")):
            _fsync_directory(self.root)
        with mock.patch("os.name", "posix"), \
                mock.patch("os.open", side_effect=PermissionError(errno.EACCES, "denied")):
            with self.assertRaises(OSError):
                _fsync_directory(self.root)

    def test_atomic_directory_sync_error_preserves_published_target(self):
        target = self.root / "snapshot-1"
        real_fsync = os.fsync

        def fail_directory(fd):
            if stat.S_ISDIR(os.fstat(fd).st_mode):
                raise OSError(errno.EIO, "directory sync failed")
            real_fsync(fd)

        with mock.patch("os.fsync", side_effect=fail_directory):
            with self.assertRaises(OSError) as raised:
                self.io.try_to_write_atomic(str(target), "published")
        self.assertEqual(raised.exception.errno, errno.EIO)
        self.assertEqual(raised.exception.__cause__.errno, errno.EIO)
        self.assertEqual(target.read_text(), "published")
        self.assertEqual(list(self.root.iterdir()), [target])

    def test_atomic_contention_still_returns_false_and_cleans_temp(self):
        target = self.root / "schema-0"
        target.write_text("existing")
        self.assertFalse(self.io.try_to_write_atomic(str(target), "contender"))
        self.assertEqual(target.read_text(), "existing")
        self.assertEqual(list(self.root.iterdir()), [target])

    def test_output_stream_write_close_fsync_order_and_api(self):
        path = self.root / "data.bin"
        events = []
        open_paths = {}
        real_open, real_os_open = open, os.open
        real_fsync, real_close = os.fsync, os.close

        def tracking_open(file, mode="r", *a, **k):
            f = real_open(file, mode, *a, **k)
            open_paths[f.fileno()] = ("file", str(Path(file).resolve()))
            return f

        def tracking_os_open(path_, flags, *a, **k):
            fd = real_os_open(path_, flags, *a, **k)
            p = Path(path_)
            open_paths[fd] = ("dir" if p.is_dir() else "file", str(p.resolve()))
            return fd

        def tracking_fsync(fd):
            kind, p = open_paths.get(fd, ("unknown", "fd:%s" % fd))
            events.append((kind, p))
            return real_fsync(fd)

        def tracking_close(fd):
            try:
                return real_close(fd)
            finally:
                open_paths.pop(fd, None)

        with mock.patch("builtins.open", side_effect=tracking_open), \
                mock.patch("os.open", side_effect=tracking_os_open), \
                mock.patch("os.fsync", side_effect=tracking_fsync), \
                mock.patch("os.close", side_effect=tracking_close):
            with self.io.new_output_stream(str(path)) as stream:
                self.assertTrue(callable(stream.write))
                self.assertTrue(callable(stream.flush))
                written = stream.write(b"payload-bytes")
                self.assertEqual(written, len(b"payload-bytes"))
                stream.flush()
                before_close = len(events)
            # close() of context manager must have fsynced the file
            after = events[before_close:]
            kinds = [k for k, _ in after]
            self.assertIn("file", kinds, after)
            if "dir" in kinds:
                self.assertLess(kinds.index("file"), kinds.index("dir"), after)

        self.assertEqual(path.read_bytes(), b"payload-bytes")

    def test_output_stream_fsync_failure_raises_after_close(self):
        path = self.root / "ctx.bin"

        def boom(fd):
            raise OSError(5, "simulated fsync failure")

        with mock.patch("os.fsync", side_effect=boom):
            with self.assertRaises(OSError):
                with self.io.new_output_stream(str(path)) as stream:
                    stream.write(b"x")
        self.assertTrue(stream.closed)

    def test_output_stream_directory_error_raises_after_close(self):
        path = self.root / "data.bin"
        real_fsync = os.fsync

        def fail_directory(fd):
            if stat.S_ISDIR(os.fstat(fd).st_mode):
                raise OSError(errno.ENOSPC, "directory sync failed")
            real_fsync(fd)

        with mock.patch("os.fsync", side_effect=fail_directory):
            with self.assertRaises(OSError) as raised:
                with self.io.new_output_stream(str(path)) as stream:
                    stream.write(b"payload")
        self.assertEqual(raised.exception.errno, errno.ENOSPC)
        self.assertTrue(stream.closed)
        self.assertEqual(path.read_bytes(), b"payload")

    def _catalog_and_schema(self):
        catalog = CatalogFactory.create({"warehouse": str(self.root)})
        catalog.create_database("default", False)
        schema = Schema.from_pyarrow_schema(pa.schema([("id", pa.int64())]), options={
            "write.native.enabled": "false", "commit.native.enabled": "false",
            "read.native.enabled": "false", "scan.native-plan.enabled": "false",
            "commit.max-retries": "2", "commit.min-retry-wait": "0 ms",
        })
        return catalog, schema

    def test_catalog_create_propagates_file_sync_failure_without_retry(self):
        catalog, schema = self._catalog_and_schema()
        attempts = []

        def fail_file(fd):
            if stat.S_ISREG(os.fstat(fd).st_mode):
                attempts.append(fd)
                # Bound the regression on the old implementation's infinite loop.
                if len(attempts) > 3:
                    raise AssertionError("schema creation retried a storage failure")
                raise OSError(errno.EIO, "file sync failed")

        with mock.patch("os.fsync", side_effect=fail_file):
            with self.assertRaises(RuntimeError) as raised:
                catalog.create_table("default.t", schema, False)
        self.assertIsInstance(raised.exception.__cause__, OSError)
        self.assertEqual(raised.exception.__cause__.errno, errno.EIO)
        self.assertEqual(len(attempts), 1)
        self.assertEqual(list(self.root.rglob("schema-0")), [])
        self.assertEqual(list(self.root.rglob("*.tmp")), [])

    def test_catalog_commit_rejects_directory_sync_failure(self):
        catalog, schema = self._catalog_and_schema()
        catalog.create_table("default.t", schema, False)
        table = catalog.get_table("default.t")
        builder = table.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        try:
            writer.write_arrow(pa.table({"id": [1]}))
            messages = writer.prepare_commit()
            real_fsync = os.fsync
            failures = []

            def fail_directory(fd):
                if stat.S_ISDIR(os.fstat(fd).st_mode):
                    failures.append(fd)
                    raise OSError(errno.EIO, "directory sync failed")
                real_fsync(fd)

            with mock.patch("os.fsync", side_effect=fail_directory):
                with self.assertRaises(RuntimeError):
                    commit.commit(messages)
            self.assertTrue(failures)
            self.assertIsNone(table.snapshot_manager().get_latest_snapshot())
        finally:
            writer.close()
            commit.close()

    def test_catalog_snapshot_sync_failure_does_not_ack_or_delete_files(self):
        catalog, schema = self._catalog_and_schema()
        catalog.create_table("default.t", schema, False)
        table = catalog.get_table("default.t")
        builder = table.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        try:
            writer.write_arrow(pa.table({"id": [1]}))
            messages = writer.prepare_commit()
            real_open, real_fsync = os.open, os.fsync
            snapshot_fds = set()
            failures = []

            def open_directory(path, flags, *args, **kwargs):
                fd = real_open(path, flags, *args, **kwargs)
                if Path(path).name == "snapshot":
                    snapshot_fds.add(fd)
                else:
                    snapshot_fds.discard(fd)
                return fd

            def fail_snapshot(fd):
                if fd in snapshot_fds and stat.S_ISDIR(os.fstat(fd).st_mode):
                    failures.append(fd)
                    raise OSError(errno.EIO, "snapshot directory sync failed")
                real_fsync(fd)

            with mock.patch("os.open", side_effect=open_directory), \
                    mock.patch("os.fsync", side_effect=fail_snapshot):
                with self.assertRaises(OSError) as raised:
                    commit.commit(messages)
            self.assertEqual(raised.exception.errno, errno.EIO)
            self.assertEqual(raised.exception.__cause__.errno, errno.EIO)
            self.assertEqual(len(failures), 1)
            self.assertEqual(table.snapshot_manager().get_latest_snapshot().id, 1)
            # The rename was visible, so abort must preserve its data and manifests.
            writer.abort()
            writer.close()
            reader = table.new_read_builder()
            actual = reader.new_read().to_arrow(reader.new_scan().plan().splits())
            self.assertEqual(actual.to_pylist(), [{"id": 1}])
        finally:
            writer.close()
            commit.close()

    def test_catalog_commit_with_unsupported_directory_sync(self):
        catalog, schema = self._catalog_and_schema()
        real_fsync = os.fsync

        def unsupported_directory(fd):
            if stat.S_ISDIR(os.fstat(fd).st_mode):
                raise OSError(errno.EINVAL, "directory fsync unsupported")
            real_fsync(fd)

        with mock.patch("os.fsync", side_effect=unsupported_directory):
            catalog.create_table("default.t", schema, False)
            table = catalog.get_table("default.t")
            builder = table.new_batch_write_builder()
            writer, commit = builder.new_write(), builder.new_commit()
            try:
                writer.write_arrow(pa.table({"id": [1]}))
                commit.commit(writer.prepare_commit())
            finally:
                writer.close()
                commit.close()
        self.assertEqual(table.snapshot_manager().get_latest_snapshot().id, 1)
        reader = table.new_read_builder()
        actual = reader.new_read().to_arrow(reader.new_scan().plan().splits())
        self.assertEqual(actual.to_pylist(), [{"id": 1}])

    def test_write_parquet_uses_fsyncing_output_stream(self):
        path = self.root / "t.parquet"
        table = pa.table({"id": pa.array([1, 2], pa.int64())})
        calls = []
        real_fsync = os.fsync

        def track(fd):
            calls.append(fd)
            return real_fsync(fd)

        with mock.patch("os.fsync", side_effect=track):
            self.io.write_parquet(str(path), table, compression="none")

        self.assertTrue(path.exists() and path.stat().st_size > 0)
        self.assertGreaterEqual(len(calls), 1)


if __name__ == "__main__":
    unittest.main()
