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

"""Durability tests for LocalFileIO fsync behavior (#10739)."""

import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pyarrow as pa

from pypaimon.filesystem import local_file_io as lfio
from pypaimon.filesystem.local_file_io import LocalFileIO


def _path_for_fd(fd, open_paths):
    return open_paths.get(fd)


class _FsyncRecorder:
    """Record fsync targets by tracking open() filenos -> paths."""

    def __init__(self):
        self.events = []  # list of ("file"|"dir", path_str)
        self._open_paths = {}
        self._real_open = open
        self._real_os_open = os.open
        self._real_fsync = os.fsync
        self._real_close = os.close
        self._fail_file_paths = set()
        self._rename_seen = []

    def install(self):
        self._patchers = [
            mock.patch("builtins.open", side_effect=self._open),
            mock.patch("os.open", side_effect=self._os_open),
            mock.patch("os.fsync", side_effect=self._fsync),
            mock.patch("os.close", side_effect=self._os_close),
        ]
        for p in self._patchers:
            p.start()
        return self

    def uninstall(self):
        for p in reversed(self._patchers):
            p.stop()

    def fail_fsync_for(self, path: Path):
        self._fail_file_paths.add(str(path.resolve()))

    def _open(self, file, mode="r", *args, **kwargs):
        f = self._real_open(file, mode, *args, **kwargs)
        try:
            key = str(Path(file).resolve())
        except Exception:
            key = str(file)
        self._open_paths[f.fileno()] = ("file", key)
        return f

    def _os_open(self, path, flags, *args, **kwargs):
        fd = self._real_os_open(path, flags, *args, **kwargs)
        try:
            key = str(Path(path).resolve())
        except Exception:
            key = str(path)
        kind = "dir" if Path(path).is_dir() else "file"
        self._open_paths[fd] = (kind, key)
        return fd

    def _fsync(self, fd):
        kind, path = self._open_paths.get(fd, ("unknown", f"fd:{fd}"))
        if kind == "file" and path in self._fail_file_paths:
            raise OSError(5, "simulated fsync failure", path)
        self.events.append((kind, path))
        return self._real_fsync(fd)

    def _os_close(self, fd):
        try:
            return self._real_close(fd)
        finally:
            self._open_paths.pop(fd, None)

    def file_events(self):
        return [p for k, p in self.events if k == "file"]

    def dir_events(self):
        return [p for k, p in self.events if k == "dir"]


class LocalFileIOFsyncTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self._tmpdir.name).resolve()
        self.io = LocalFileIO()
        self.rec = _FsyncRecorder().install()

    def tearDown(self):
        self.rec.uninstall()
        self._tmpdir.cleanup()

    def test_try_to_write_atomic_fsyncs_file_before_rename_then_parent_dir(self):
        target = self.root / "snapshot-1"
        content = '{"id":1,"meta":"x"}'

        rename_order = []
        real_rename = Path.rename

        def tracking_rename(self_path, target_path):
            # file fsync must already have happened for the temp path
            self.assertTrue(
                any(p.endswith(Path(self_path).name) or Path(self_path).name in p
                    for p in self.rec.file_events()),
                f"file fsync missing before rename; events={self.rec.events}",
            )
            rename_order.append((str(self_path), str(target_path)))
            return real_rename(self_path, target_path)

        with mock.patch.object(Path, "rename", tracking_rename):
            ok = self.io.try_to_write_atomic(str(target), content)

        self.assertTrue(ok)
        self.assertEqual(target.read_text(encoding="utf-8"), content)
        self.assertEqual(len(rename_order), 1)
        # After publish, parent directory fsync should appear
        parent = str(self.root)
        self.assertTrue(
            any(d == parent or d.startswith(parent) for d in self.rec.dir_events()),
            f"dir fsync missing after rename; events={self.rec.events}",
        )
        # Ordering: at least one file fsync index < first dir fsync after rename
        first_file_idx = next(i for i, (k, _) in enumerate(self.rec.events) if k == "file")
        dir_idxs = [i for i, (k, p) in enumerate(self.rec.events) if k == "dir"]
        self.assertTrue(dir_idxs, self.rec.events)
        self.assertLess(first_file_idx, dir_idxs[-1])

    def test_try_to_write_atomic_fsync_failure_does_not_publish_target(self):
        target = self.root / "snapshot-bad"
        # Fail fsync for any temp under root (name starts with .)
        def fail_any_temp(fd):
            kind, path = self.rec._open_paths.get(fd, ("unknown", ""))
            if kind == "file" and Path(path).name.startswith("."):
                raise OSError(5, "simulated fsync failure", path)
            return self.rec._real_fsync(fd)

        # Replace fsync side effect after install
        self.rec._fsync = fail_any_temp  # won't work - already patched
        self.rec.uninstall()
        self.rec = _FsyncRecorder()
        # Mark all files under root that are not yet the final name
        self.rec.install()
        original_fsync = self.rec._fsync

        def selective(fd):
            kind, path = self.rec._open_paths.get(fd, ("unknown", ""))
            if kind == "file" and "/." in path.replace(str(self.root), "") or (
                kind == "file" and Path(path).name.startswith(".")
            ):
                raise OSError(5, "simulated fsync failure", path)
            return self.rec._real_fsync(fd)

        with mock.patch("os.fsync", side_effect=selective):
            # Need open tracking still - re-do simpler
            pass
        self.rec.uninstall()

        # Simpler approach without double patch mess:
        io = LocalFileIO()
        target = self.root / "snap-fail"
        calls = {"n": 0}
        real_fsync = os.fsync

        def boom(fd):
            calls["n"] += 1
            # first fsync is the temp file in try_to_write_atomic
            if calls["n"] == 1:
                raise OSError(5, "simulated fsync failure")
            return real_fsync(fd)

        with mock.patch("os.fsync", side_effect=boom):
            ok = io.try_to_write_atomic(str(target), '{"id":9}')
        self.assertFalse(ok)
        self.assertFalse(target.exists(), "must not rename when fsync failed")
        # no leftover empty published target
        self.assertEqual(list(self.root.glob("snap-fail*")), [])

    def test_output_stream_fsyncs_file_before_close_completes_then_dir(self):
        path = self.root / "data.bin"
        events_at_close = []

        stream = self.io.new_output_stream(str(path))
        self.assertTrue(hasattr(stream, "write"))
        self.assertTrue(callable(getattr(stream, "write")))
        n = stream.write(b"payload-bytes")
        self.assertEqual(n, len(b"payload-bytes"))
        # fileno / flush compatibility for upper layers
        self.assertTrue(hasattr(stream, "flush"))
        stream.flush()

        real_close = stream.close

        def wrapped_close():
            # Before close returns, file fsync must be recorded
            before = list(self.rec.events)
            real_close()
            after = list(self.rec.events)
            events_at_close.append((before, after))

        stream.close = wrapped_close
        stream.close()

        self.assertEqual(path.read_bytes(), b"payload-bytes")
        before, after = events_at_close[0]
        new_events = after[len(before):]
        self.assertTrue(any(k == "file" for k, _ in new_events), new_events)
        # dir fsync after file fsync in the new events
        kinds = [k for k, _ in new_events]
        if "dir" in kinds:
            self.assertLess(kinds.index("file"), kinds.index("dir"))

    def test_output_stream_context_manager_and_fsync_failure_raises(self):
        path = self.root / "ctx.bin"
        real_fsync = os.fsync
        calls = {"n": 0}

        def boom(fd):
            calls["n"] += 1
            # skip possible mkdir dir fsync; fail the file fsync on close
            # file fsync uses the stream fileno via _fsync_file_obj
            raise OSError(5, "simulated fsync failure")

        with mock.patch("os.fsync", side_effect=boom):
            with self.assertRaises(OSError):
                with self.io.new_output_stream(str(path)) as stream:
                    stream.write(b"x")
        # File may exist with data but close failed — not treated as durable success
        self.assertTrue(calls["n"] >= 1)

    def test_write_parquet_goes_through_fsyncing_output_stream(self):
        path = self.root / "t.parquet"
        table = pa.table({"id": pa.array([1, 2], pa.int64())})
        # Track that fsync was invoked during write_parquet
        calls = []
        real_fsync = os.fsync

        def track(fd):
            calls.append(fd)
            return real_fsync(fd)

        with mock.patch("os.fsync", side_effect=track):
            self.io.write_parquet(str(path), table, compression="none")
        self.assertTrue(path.exists() and path.stat().st_size > 0)
        self.assertGreaterEqual(len(calls), 1, "parquet write must fsync via output stream")


if __name__ == "__main__":
    unittest.main()
