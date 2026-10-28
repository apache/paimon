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

import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pyarrow as pa

from pypaimon.filesystem.local_file_io import LocalFileIO


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
            ok = self.io.try_to_write_atomic(str(target), '{"id":9}')

        self.assertFalse(ok)
        self.assertFalse(target.exists())
        self.assertEqual(list(self.root.glob("snap-fail*")), [])

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
        real_fsync = os.fsync

        def boom(fd):
            raise OSError(5, "simulated fsync failure")

        with mock.patch("os.fsync", side_effect=boom):
            with self.assertRaises(OSError):
                with self.io.new_output_stream(str(path)) as stream:
                    stream.write(b"x")

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
