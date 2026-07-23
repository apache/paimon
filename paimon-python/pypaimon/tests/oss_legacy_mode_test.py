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

"""Unit tests for the OSS bucket-in-endpoint mode (PyArrow < 16) of PyArrowFileIO.

See ``PyArrowFileIO._legacy_oss_mode`` for why bucket-level operations
must be guarded in this mode. No real OSS access is required.
"""

import unittest
from unittest import mock

import pyarrow.fs as pafs

from pypaimon.common.options import Options
from pypaimon.common.options.config import OssOptions
from pypaimon.filesystem.pyarrow_file_io import PyArrowFileIO


TABLE_PATH = "oss://test-bucket/db-uuid.db/tbl-uuid"


def _file_info(path, file_type):
    return pafs.FileInfo(path, file_type)


def _probe_response(status_code, body):
    response = mock.MagicMock(status_code=status_code)
    response.iter_content.return_value = iter([body])
    return response


class OssLegacyModeTest(unittest.TestCase):
    """Behavior of PyArrowFileIO when OSS runs on PyArrow < 16."""

    def _new_file_io(self, legacy):
        options = Options({
            OssOptions.OSS_ACCESS_KEY_ID.key(): "ak",
            OssOptions.OSS_ACCESS_KEY_SECRET.key(): "sk",
            OssOptions.OSS_ENDPOINT.key(): "oss-cn-test.example.com",
            OssOptions.OSS_REGION.key(): "cn-test",
            OssOptions.OSS_IMPL.key(): "legacy",
        })
        with mock.patch.object(
                PyArrowFileIO, "_initialize_oss_fs", return_value=mock.Mock()):
            file_io = PyArrowFileIO("oss://test-bucket/", options)
        # _legacy_oss_mode() keys off the bucket-in-endpoint flag (PyArrow < 16).
        file_io._oss_bucket_in_endpoint = legacy
        file_io.filesystem = mock.Mock()
        return file_io

    def test_legacy_mkdirs_skips_create_dir(self):
        """create_dir would CreateBucket and corrupt the parent directory."""
        file_io = self._new_file_io(legacy=True)
        file_io.filesystem.get_file_info.return_value = [
            _file_info("db-uuid.db/tbl-uuid", pafs.FileType.NotFound)]

        with mock.patch("requests.get") as get:
            get.return_value = mock.MagicMock(status_code=403)
            self.assertTrue(file_io.mkdirs(TABLE_PATH))
            get.assert_called_once_with(
                "https://test-bucket.oss-cn-test.example.com/",
                timeout=5, allow_redirects=False, stream=True)

        file_io.filesystem.create_dir.assert_not_called()

    def test_legacy_mkdirs_raises_when_bucket_missing(self):
        """mkdirs must not report success for a missing real bucket."""
        file_io = self._new_file_io(legacy=True)
        file_io.filesystem.get_file_info.return_value = [
            _file_info("db-uuid.db/tbl-uuid", pafs.FileType.NotFound)]

        with mock.patch("requests.get") as get:
            get.return_value = _probe_response(
                404, b"<Error><Code>NoSuchBucket</Code></Error>")
            with self.assertRaises(OSError) as ctx:
                file_io.mkdirs(TABLE_PATH)
        self.assertIn("does not exist", str(ctx.exception))
        file_io.filesystem.create_dir.assert_not_called()

    def test_legacy_mkdirs_allows_bare_404_from_custom_endpoint(self):
        """A 404 without the OSS NoSuchBucket body must not reject the bucket."""
        file_io = self._new_file_io(legacy=True)
        file_io.filesystem.get_file_info.return_value = [
            _file_info("db-uuid.db/tbl-uuid", pafs.FileType.NotFound)]

        with mock.patch("requests.get") as get:
            get.return_value = _probe_response(404, b"not found")
            self.assertTrue(file_io.mkdirs(TABLE_PATH))
            # The indeterminate probe is cached; no repeat per mkdirs.
            self.assertTrue(file_io.mkdirs(TABLE_PATH))
            self.assertEqual(get.call_count, 1)

    def test_legacy_mkdirs_fails_open_and_caches_on_transport_error(self):
        """A probe transport failure must neither fail mkdirs nor repeat
        the probe (and its timeout) on every subsequent write."""
        file_io = self._new_file_io(legacy=True)
        file_io.filesystem.get_file_info.return_value = [
            _file_info("db-uuid.db/tbl-uuid", pafs.FileType.NotFound)]

        import requests
        with mock.patch("requests.get") as get:
            get.side_effect = requests.ConnectionError("boom")
            self.assertTrue(file_io.mkdirs(TABLE_PATH))
            self.assertTrue(file_io.mkdirs(TABLE_PATH))
            self.assertEqual(get.call_count, 1)

    def test_legacy_mkdirs_fails_open_on_plain_oserror(self):
        """Requests-only setup errors (e.g. a broken REQUESTS_CA_BUNDLE)
        raise plain OSError; they must not abort legacy writes."""
        file_io = self._new_file_io(legacy=True)
        file_io.filesystem.get_file_info.return_value = [
            _file_info("db-uuid.db/tbl-uuid", pafs.FileType.NotFound)]

        with mock.patch("requests.get") as get:
            get.side_effect = OSError(
                "Could not find a suitable TLS CA certificate bundle")
            self.assertTrue(file_io.mkdirs(TABLE_PATH))
            self.assertTrue(file_io.mkdirs(TABLE_PATH))
            self.assertEqual(get.call_count, 1)

    def test_legacy_mkdirs_probe_is_serialized_across_threads(self):
        """Concurrent writers must share one probe and all observe its
        published result."""
        import threading
        import time

        for body, expect_error in [
                (b"<Error><Code>NoSuchBucket</Code></Error>", True),
                (b"not found", False)]:
            file_io = self._new_file_io(legacy=True)
            file_io.filesystem.get_file_info.return_value = [
                _file_info("db-uuid.db/tbl-uuid", pafs.FileType.NotFound)]

            def slow_get(*args, **kwargs):
                time.sleep(0.05)
                return _probe_response(404, body)

            results = []

            def call_mkdirs():
                try:
                    results.append(file_io.mkdirs(TABLE_PATH))
                except OSError:
                    results.append("raised")

            with mock.patch("requests.get", side_effect=slow_get) as get:
                threads = [threading.Thread(target=call_mkdirs) for _ in range(4)]
                for t in threads:
                    t.start()
                for t in threads:
                    t.join()
                self.assertEqual(get.call_count, 1)
            expected = "raised" if expect_error else True
            self.assertEqual(results, [expected] * 4)

    def test_legacy_mkdirs_missing_bucket_keeps_raising_without_reprobe(self):
        file_io = self._new_file_io(legacy=True)
        file_io.filesystem.get_file_info.return_value = [
            _file_info("db-uuid.db/tbl-uuid", pafs.FileType.NotFound)]

        with mock.patch("requests.get") as get:
            get.return_value = _probe_response(
                404, b"<Error><Code>NoSuchBucket</Code></Error>")
            for _ in range(2):
                with self.assertRaises(OSError):
                    file_io.mkdirs(TABLE_PATH)
            self.assertEqual(get.call_count, 1)

    def test_legacy_mkdirs_still_rejects_file_conflict(self):
        file_io = self._new_file_io(legacy=True)
        file_io.filesystem.get_file_info.return_value = [
            _file_info("db-uuid.db/tbl-uuid", pafs.FileType.File)]

        with self.assertRaises(FileExistsError):
            file_io.mkdirs(TABLE_PATH)
        file_io.filesystem.create_dir.assert_not_called()

    def test_modern_mkdirs_still_creates_dir(self):
        file_io = self._new_file_io(legacy=False)
        file_io.filesystem.get_file_info.return_value = [
            _file_info("test-bucket/db-uuid.db/tbl-uuid", pafs.FileType.NotFound)]

        self.assertTrue(file_io.mkdirs(TABLE_PATH))

        file_io.filesystem.create_dir.assert_called_once()

    def test_file_io_pickle_roundtrip_recreates_lock(self):
        """The probe lock must not break pickling (FileIO travels to Ray or
        multiprocessing workers); probe state is carried over."""
        import pickle

        options = Options({
            OssOptions.OSS_ACCESS_KEY_ID.key(): "ak",
            OssOptions.OSS_ACCESS_KEY_SECRET.key(): "sk",
            OssOptions.OSS_ENDPOINT.key(): "oss-cn-test.example.com",
            OssOptions.OSS_REGION.key(): "cn-test",
            OssOptions.OSS_IMPL.key(): "legacy",
        })
        file_io = PyArrowFileIO("oss://test-bucket/wh", options)
        file_io._legacy_bucket_checked = True
        file_io._legacy_bucket_error = "OSS bucket 'test-bucket' does not exist"

        restored = pickle.loads(pickle.dumps(file_io))

        self.assertIsNotNone(restored._legacy_bucket_lock)
        # Cached probe verdict survives; no re-probe in the worker.
        with mock.patch("requests.get") as get:
            with self.assertRaises(OSError):
                restored._check_legacy_bucket_exists()
            get.assert_not_called()

    def test_legacy_exists_true_for_plain_object(self):
        file_io = self._new_file_io(legacy=True)
        file_io.filesystem.get_file_info.side_effect = lambda paths: [
            _file_info(paths[0], pafs.FileType.File)]

        self.assertTrue(file_io.exists(TABLE_PATH + "/data-1.parquet"))

    def test_legacy_list_status_raises_actionable_error(self):
        """Fail fast instead of the misleading raw NoSuchKey selector error."""
        file_io = self._new_file_io(legacy=True)

        with self.assertRaises(RuntimeError) as ctx:
            file_io.list_status(TABLE_PATH)
        self.assertIn("pyarrow >= 16", str(ctx.exception))
        file_io.filesystem.get_file_info.assert_not_called()

    def test_modern_list_status_uses_selector(self):
        file_io = self._new_file_io(legacy=False)
        file_io.filesystem.get_file_info.return_value = []

        self.assertEqual(file_io.list_status(TABLE_PATH), [])
        file_io.filesystem.get_file_info.assert_called_once()


if __name__ == "__main__":
    unittest.main()
