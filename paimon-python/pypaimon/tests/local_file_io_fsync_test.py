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

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from pypaimon.filesystem.local_file_io import LocalFileIO


class LocalFileIOFsyncTest(unittest.TestCase):
    """Durability: LocalFileIO must fsync file data before publishing names (#10739)."""

    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self._tmpdir.name)
        self.io = LocalFileIO()

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_try_to_write_atomic_fsyncs_file_before_rename(self):
        target = self.root / "snapshot-1"
        fsynced = []

        real_fsync = os.fsync

        def tracking_fsync(fd):
            fsynced.append(fd)
            return real_fsync(fd)

        with mock.patch("os.fsync", side_effect=tracking_fsync):
            ok = self.io.try_to_write_atomic(str(target), '{"version":1}')
        self.assertTrue(ok)
        self.assertTrue(target.exists())
        self.assertEqual(target.read_text(encoding="utf-8"), '{"version":1}')
        # At least the temp file data must have been fsynced (fd may be file or dir).
        self.assertGreaterEqual(len(fsynced), 1)

    def test_output_stream_close_fsyncs(self):
        path = self.root / "data.bin"
        fsynced = []
        real_fsync = os.fsync

        def tracking_fsync(fd):
            fsynced.append(fd)
            return real_fsync(fd)

        with mock.patch("os.fsync", side_effect=tracking_fsync):
            stream = self.io.new_output_stream(str(path))
            stream.write(b"abc")
            stream.close()
        self.assertEqual(path.read_bytes(), b"abc")
        self.assertGreaterEqual(len(fsynced), 1)

    def test_truncated_newest_snapshot_breaks_naive_readers_without_durability(self):
        """Documents the failure mode #10739 describes (empty newest snapshot)."""
        snap_dir = self.root / "snapshot"
        snap_dir.mkdir()
        (snap_dir / "snapshot-1").write_text('{"id":1}', encoding="utf-8")
        newest = snap_dir / "snapshot-2"
        newest.write_text('{"id":2}', encoding="utf-8")
        # Simulate power loss after rename of unsynced temp: name exists, size 0.
        os.truncate(newest, 0)
        self.assertEqual(newest.stat().st_size, 0)
        with self.assertRaises(json.JSONDecodeError):
            json.loads(newest.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
