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

"""Unit tests for PyArrow-backed S3-compatible storage.

No real OSS access is required.
"""

import multiprocessing
import os
import pickle
import socketserver
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest import mock
from urllib.parse import parse_qs, unquote, urlsplit
from xml.sax.saxutils import escape

import pyarrow
import pyarrow.fs as pafs
from packaging.version import parse

from pypaimon.common.options import Options
from pypaimon.common.options.config import OssOptions, S3Options
from pypaimon.filesystem.pyarrow_file_io import (
    LegacyOssDirectoryListingError,
    PyArrowFileIO,
)


TABLE_PATH = "oss://test-bucket/db-uuid.db/tbl-uuid"


class _ThreadingHTTPServer(socketserver.ThreadingMixIn, HTTPServer):
    daemon_threads = True


class _DeleteRequestHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        self.server.requests.append((self.command, self.path))
        query = parse_qs(urlsplit(self.path).query)
        prefix = query.get("prefix", [""])[0]
        max_keys = int(query.get("max-keys", ["1000"])[0])
        keys = sorted(
            key for key in self.server.objects if key.startswith(prefix)
        )[:max_keys]
        body = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
            '<IsTruncated>false</IsTruncated>{}</ListBucketResult>'.format(
                "".join(
                    "<Contents><Key>{}</Key><Size>1</Size></Contents>".format(
                        escape(key))
                    for key in keys)))
        if (keys == [self.server.prefix]
                and not self.server.late_object_added):
            self.server.objects.add(self.server.prefix + "late.parquet")
            self.server.late_object_added = True
        first = self.server.prefix + "first.parquet"
        if first in keys and not self.server.missing_object_removed:
            self.server.objects.discard(first)
            self.server.missing_object_removed = True
        encoded = body.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/xml")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def do_DELETE(self):
        self.server.requests.append((self.command, self.path))
        path = unquote(urlsplit(self.path).path).lstrip("/")
        _, key = path.split("/", 1)
        self.server.objects.discard(key)
        self.send_response(204)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _unexpected(self):
        self.server.requests.append((self.command, self.path))
        self.send_response(501)
        self.send_header("Content-Length", "0")
        self.end_headers()

    do_HEAD = _unexpected
    do_POST = _unexpected
    do_PUT = _unexpected

    def log_message(self, *args):
        pass


def _file_info(path, file_type):
    return pafs.FileInfo(path, file_type)


def _set_listed_keys(file_io, *passes):
    file_io._s3_delete_client.list_objects_v2.side_effect = [
        {"Contents": [{"Key": key} for key in keys]}
        for keys in passes
    ] + [{"Contents": []}]


def _probe_response(status_code, body):
    response = mock.MagicMock(status_code=status_code)
    response.iter_content.return_value = iter([body])
    return response


def _restore_s3_file_io(connection):
    os.environ.pop("AWS_REQUEST_CHECKSUM_CALCULATION", None)
    connection.send("ready")
    payload = connection.recv_bytes()
    with mock.patch("pyarrow.fs.S3FileSystem", return_value=object()) as s3:
        restored = pickle.loads(payload)
    connection.send((
        os.environ.get("AWS_REQUEST_CHECKSUM_CALCULATION"),
        s3.call_count,
        restored.filesystem is s3.return_value,
    ))
    connection.close()


class OssLegacyModeTest(unittest.TestCase):
    """Behavior of legacy PyArrow S3FileSystem access to OSS."""

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
        file_io._s3_delete_client = mock.Mock()
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

    def test_modern_mkdirs_creates_directory(self):
        file_io = self._new_file_io(legacy=False)
        file_io.filesystem.get_file_info.return_value = [
            _file_info("test-bucket/db-uuid.db/tbl-uuid", pafs.FileType.NotFound)]

        self.assertTrue(file_io.mkdirs(TABLE_PATH))

        file_io.filesystem.create_dir.assert_called_once_with(
            file_io.to_filesystem_path(TABLE_PATH), recursive=True)

    def test_oss_initialization_disables_optional_checksum_trailers(self):
        options = Options({
            OssOptions.OSS_ACCESS_KEY_ID.key(): "ak",
            OssOptions.OSS_ACCESS_KEY_SECRET.key(): "sk",
            OssOptions.OSS_ENDPOINT.key(): "oss-cn-test.example.com",
            OssOptions.OSS_REGION.key(): "cn-test",
            OssOptions.OSS_IMPL.key(): "legacy",
        })
        with mock.patch.dict("os.environ", {}, clear=True), \
                mock.patch("pyarrow.fs.S3FileSystem", return_value=mock.Mock()):
            PyArrowFileIO("oss://test-bucket/", options)
            self.assertEqual(
                "WHEN_REQUIRED",
                os.environ["AWS_REQUEST_CHECKSUM_CALCULATION"])

    def test_pyarrow_22_recursive_delete_uses_concurrent_individual_objects(self):
        file_io = self._new_file_io(legacy=False)
        file_io._pyarrow_gte_22 = True
        directory = file_io.to_filesystem_path(TABLE_PATH)
        data_dir = directory.rstrip("/") + "/data"
        data_file = directory.rstrip("/") + "/data/data.parquet"
        file_io.filesystem.get_file_info.return_value = [
            _file_info(directory, pafs.FileType.Directory)]
        _set_listed_keys(
            file_io, [data_dir.lstrip("/") + "/", data_file.lstrip("/")], [])

        self.assertTrue(file_io.delete(TABLE_PATH, recursive=True))

        calls = file_io._s3_delete_client.delete_object.call_args_list
        self.assertCountEqual([
            mock.call(
                Bucket="test-bucket",
                Key="db-uuid.db/tbl-uuid/data/data.parquet"),
            mock.call(
                Bucket="test-bucket",
                Key="db-uuid.db/tbl-uuid/data/"),
        ], calls[:-1])
        self.assertEqual(
            mock.call(
                Bucket="test-bucket", Key="db-uuid.db/tbl-uuid/"),
            calls[-1])
        file_io.filesystem.delete_file.assert_not_called()
        file_io.filesystem.delete_dir_contents.assert_not_called()
        file_io.filesystem.delete_dir.assert_not_called()

    def test_pyarrow_22_recursive_delete_rechecks_late_objects(self):
        file_io = self._new_file_io(legacy=False)
        file_io._pyarrow_gte_22 = True
        directory = file_io.to_filesystem_path(TABLE_PATH)
        first = directory.rstrip("/") + "/first.parquet"
        late = directory.rstrip("/") + "/late.parquet"
        file_io.filesystem.get_file_info.return_value = [
            _file_info(directory, pafs.FileType.Directory)]
        _set_listed_keys(file_io, [first.lstrip("/")], [late.lstrip("/")], [])

        self.assertTrue(file_io.delete(TABLE_PATH, recursive=True))

        self.assertEqual(
            [
                mock.call(
                    Bucket="test-bucket",
                    Key="db-uuid.db/tbl-uuid/first.parquet"),
                mock.call(
                    Bucket="test-bucket",
                    Key="db-uuid.db/tbl-uuid/late.parquet"),
                mock.call(
                    Bucket="test-bucket",
                    Key="db-uuid.db/tbl-uuid/"),
            ],
            file_io._s3_delete_client.delete_object.call_args_list,
        )

    def test_pre_pyarrow_22_recursive_delete_keeps_native_batch(self):
        file_io = self._new_file_io(legacy=False)
        file_io._pyarrow_gte_22 = False
        directory = file_io.to_filesystem_path(TABLE_PATH)
        file_io.filesystem.get_file_info.side_effect = [
            [_file_info(directory, pafs.FileType.Directory)],
        ]

        self.assertTrue(file_io.delete(TABLE_PATH, recursive=True))

        file_io.filesystem.delete_dir_contents.assert_called_once_with(directory)
        file_io.filesystem.delete_dir.assert_called_once_with(directory)
        file_io.filesystem.delete_file.assert_not_called()

    def test_modern_non_recursive_delete_removes_empty_directory(self):
        file_io = self._new_file_io(legacy=False)
        file_io._pyarrow_gte_22 = True
        directory = file_io.to_filesystem_path(TABLE_PATH)
        file_io.filesystem.get_file_info.side_effect = [
            [_file_info(directory, pafs.FileType.Directory)],
            [],
        ]

        self.assertTrue(file_io.delete(TABLE_PATH))

        file_io.filesystem.delete_file.assert_not_called()
        file_io.filesystem.delete_dir.assert_not_called()
        file_io._s3_delete_client.delete_object.assert_called_once_with(
            Bucket="test-bucket", Key="db-uuid.db/tbl-uuid/")

    def test_modern_non_recursive_delete_keeps_bucket_root(self):
        file_io = self._new_file_io(legacy=False)
        file_io._pyarrow_gte_22 = True
        file_io.filesystem.get_file_info.side_effect = [
            [_file_info("/", pafs.FileType.Directory)],
            [],
        ]

        self.assertTrue(file_io.delete("oss://test-bucket/"))

        file_io._s3_delete_client.delete_object.assert_not_called()

    def test_modern_non_recursive_delete_rejects_non_empty_directory(self):
        file_io = self._new_file_io(legacy=False)
        directory = file_io.to_filesystem_path(TABLE_PATH)
        data_file = directory.rstrip("/") + "/data.parquet"
        file_io.filesystem.get_file_info.side_effect = [
            [_file_info(directory, pafs.FileType.Directory)],
            [_file_info(data_file, pafs.FileType.File)],
        ]

        with self.assertRaisesRegex(OSError, "is not empty"):
            file_io.delete(TABLE_PATH)

        file_io.filesystem.delete_file.assert_not_called()

    def test_file_io_pickle_roundtrip_recreates_lock(self):
        """The probe lock must not break pickling (FileIO travels to Ray or
        multiprocessing workers); probe state is carried over."""
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

    def test_pickle_recreates_oss_client_with_worker_checksum_setting(self):
        context = multiprocessing.get_context("spawn")
        parent, child = context.Pipe()
        process = context.Process(target=_restore_s3_file_io, args=(child,))
        process.start()
        child.close()
        try:
            self.assertTrue(parent.poll(15))
            self.assertEqual("ready", parent.recv())

            file_io = self._new_file_io(legacy=False)
            file_io.filesystem = pafs.LocalFileSystem()
            parent.send_bytes(pickle.dumps(file_io))

            self.assertTrue(parent.poll(15))
            self.assertEqual(("WHEN_REQUIRED", 1, True), parent.recv())
        finally:
            parent.close()
            process.join(15)
            if process.is_alive():
                process.terminate()
                process.join()
        self.assertEqual(0, process.exitcode)

    def test_legacy_exists_true_for_plain_object(self):
        file_io = self._new_file_io(legacy=True)
        file_io.filesystem.get_file_info.side_effect = lambda paths: [
            _file_info(paths[0], pafs.FileType.File)]

        self.assertTrue(file_io.exists(TABLE_PATH + "/data-1.parquet"))

    def test_legacy_list_status_raises_actionable_error(self):
        """Fail fast instead of the misleading raw NoSuchKey selector error."""
        file_io = self._new_file_io(legacy=True)

        with self.assertRaises(LegacyOssDirectoryListingError) as ctx:
            file_io.list_status(TABLE_PATH)
        self.assertIn("pyarrow >= 16", str(ctx.exception))
        file_io.filesystem.get_file_info.assert_not_called()

    def test_modern_list_status_uses_selector(self):
        file_io = self._new_file_io(legacy=False)
        file_io.filesystem.get_file_info.return_value = []

        self.assertEqual(file_io.list_status(TABLE_PATH), [])
        file_io.filesystem.get_file_info.assert_called_once()


class CustomS3EndpointTest(unittest.TestCase):
    def _new_file_io(self, scheme="s3"):
        options = Options({
            S3Options.S3_ACCESS_KEY_ID.key(): "ak",
            S3Options.S3_ACCESS_KEY_SECRET.key(): "sk",
            S3Options.S3_ENDPOINT.key(): "http://minio:9000",
            S3Options.S3_REGION.key(): "us-east-1",
        })
        with mock.patch.object(
                PyArrowFileIO, "_initialize_s3_fs", return_value=mock.Mock()):
            file_io = PyArrowFileIO(
                "{}://test-bucket/warehouse".format(scheme), options)
        file_io.filesystem = mock.Mock()
        file_io._s3_delete_client = mock.Mock()
        return file_io

    def test_initialization_configures_all_s3_schemes(self):
        options = Options({
            S3Options.S3_ENDPOINT.key(): "http://minio:9000",
        })
        for scheme in ("s3", "s3a", "s3n"):
            with self.subTest(scheme=scheme), \
                    mock.patch.dict("os.environ", {}, clear=True), \
                    mock.patch("pyarrow.fs.S3FileSystem", return_value=mock.Mock()):
                PyArrowFileIO(
                    "{}://test-bucket/warehouse".format(scheme), options)
                self.assertEqual(
                    "WHEN_REQUIRED",
                    os.environ["AWS_REQUEST_CHECKSUM_CALCULATION"])

    def test_native_s3_does_not_change_checksum_setting(self):
        with mock.patch.dict("os.environ", {}, clear=True), \
                mock.patch("pyarrow.fs.S3FileSystem", return_value=mock.Mock()):
            PyArrowFileIO("s3://test-bucket/warehouse", Options({}))
            self.assertNotIn(
                "AWS_REQUEST_CHECKSUM_CALCULATION", os.environ)

    def test_pyarrow_22_recursive_delete_uses_individual_objects(self):
        for scheme in ("s3", "s3a", "s3n"):
            with self.subTest(scheme=scheme):
                file_io = self._new_file_io(scheme)
                file_io._pyarrow_gte_22 = True
                path = "{}://test-bucket/table".format(scheme)
                directory = file_io.to_filesystem_path(path)
                data_file = directory + "/data.parquet"
                file_io.filesystem.get_file_info.return_value = [
                    _file_info(directory, pafs.FileType.Directory)]
                _set_listed_keys(file_io, [data_file.lstrip("/")], [])

                self.assertTrue(file_io.delete(path, recursive=True))

                file_io.filesystem.delete_file.assert_not_called()
                file_io.filesystem.delete_dir_contents.assert_not_called()
                file_io._s3_delete_client.delete_object.assert_has_calls([
                    mock.call(Bucket="test-bucket", Key="table/data.parquet"),
                    mock.call(Bucket="test-bucket", Key="table/"),
                ])

    def test_batch_delete_can_be_enabled_for_compatible_endpoint(self):
        file_io = self._new_file_io()
        file_io.properties.set(
            S3Options.S3_DELETE_BATCH_ENABLED, "true")
        file_io._pyarrow_gte_22 = True
        directory = "test-bucket/table"
        file_io.filesystem.get_file_info.return_value = [
            _file_info(directory, pafs.FileType.Directory),
        ]
        file_io.to_filesystem_path = mock.Mock(return_value=directory)

        self.assertTrue(file_io.delete("s3://test-bucket/table", recursive=True))

        file_io.filesystem.delete_dir_contents.assert_called_once_with(directory)
        file_io.filesystem.delete_dir.assert_called_once_with(directory)
        file_io._s3_delete_client.delete_object.assert_not_called()

    def test_recursive_delete_uses_bucket_from_target_uri(self):
        file_io = self._new_file_io()
        file_io._pyarrow_gte_22 = True
        file_io.to_filesystem_path = mock.Mock(
            return_value="target-bucket/table")
        file_io.filesystem.get_file_info.return_value = [
            _file_info("target-bucket/table", pafs.FileType.Directory)]
        _set_listed_keys(file_io, ["table/data.parquet"], [])

        self.assertTrue(file_io.delete(
            "s3://target-bucket/table", recursive=True))

        self.assertEqual([
            mock.call(Bucket="target-bucket", Key="table/data.parquet"),
            mock.call(Bucket="target-bucket", Key="table/"),
        ], file_io._s3_delete_client.delete_object.call_args_list)

    def test_pre_pyarrow_22_cross_bucket_delete_keeps_native_path(self):
        file_io = self._new_file_io()
        file_io._pyarrow_gte_22 = False
        file_io.to_filesystem_path = mock.Mock(
            return_value="target-bucket/table")
        file_io.filesystem.get_file_info.return_value = [
            _file_info("target-bucket/table", pafs.FileType.Directory)]

        self.assertTrue(file_io.delete(
            "s3://target-bucket/table", recursive=True))

        file_io.filesystem.delete_dir_contents.assert_called_once_with(
            "target-bucket/table")
        file_io.filesystem.delete_dir.assert_called_once_with(
            "target-bucket/table")
        file_io._s3_delete_client.delete_object.assert_not_called()

    def test_non_recursive_delete_uses_bucket_from_target_uri(self):
        file_io = self._new_file_io()
        file_io._pyarrow_gte_22 = True
        file_io.to_filesystem_path = mock.Mock(
            return_value="target-bucket/table")
        file_io.filesystem.get_file_info.side_effect = [
            [_file_info("target-bucket/table", pafs.FileType.Directory)],
            [],
        ]

        self.assertTrue(file_io.delete("s3://target-bucket/table"))

        file_io._s3_delete_client.delete_object.assert_called_once_with(
            Bucket="target-bucket", Key="table/")

    @unittest.skipUnless(
        parse(pyarrow.__version__) >= parse("22.0.0"),
        "requires PyArrow 22+ and boto3",
    )
    def test_recursive_delete_uses_only_object_delete_requests_during_races(self):
        server = _ThreadingHTTPServer(
            ("127.0.0.1", 0), _DeleteRequestHandler)
        server.requests = []
        server.prefix = "ta/ble/"
        decoy = "ta/ble-other/keep.parquet"
        server.objects = {
            server.prefix, server.prefix + "first.parquet", decoy}
        server.late_object_added = False
        server.missing_object_removed = False
        server_thread = threading.Thread(target=server.serve_forever)
        server_thread.start()
        try:
            options = Options({
                S3Options.S3_ACCESS_KEY_ID.key(): "ak",
                S3Options.S3_ACCESS_KEY_SECRET.key(): "sk",
                S3Options.S3_ENDPOINT.key():
                    "http://127.0.0.1:{}".format(server.server_port),
                S3Options.S3_REGION.key(): "us-east-1",
                "fs.s3.path.style.access": "true",
            })
            with mock.patch.object(
                    PyArrowFileIO, "_initialize_s3_fs", return_value=mock.Mock()), \
                    mock.patch.dict(os.environ, {
                        "NO_PROXY": "127.0.0.1,localhost",
                        "no_proxy": "127.0.0.1,localhost",
                    }):
                file_io = PyArrowFileIO(
                    "s3://source-bucket/warehouse", options)
                file_io.filesystem = mock.Mock()
                file_io.filesystem.get_file_info.return_value = [
                    _file_info("/ta/ble", pafs.FileType.Directory)]

                self.assertTrue(file_io.delete(
                    "s3://target-bucket/ta//ble", recursive=True))
                file_io._s3_delete_client.close()

            self.assertTrue(server.late_object_added)
            self.assertTrue(server.missing_object_removed)
            self.assertEqual({decoy}, server.objects)
            self.assertEqual(
                {"GET", "DELETE"},
                {method for method, _ in server.requests})
            self.assertEqual([
                "/target-bucket/ta/ble/first.parquet",
                "/target-bucket/ta/ble/",
                "/target-bucket/ta/ble/late.parquet",
                "/target-bucket/ta/ble/",
            ], [path for method, path in server.requests
                if method == "DELETE"])
        finally:
            server.shutdown()
            server.server_close()
            server_thread.join()

    def test_pickle_recreates_client_with_worker_checksum_setting(self):
        context = multiprocessing.get_context("spawn")
        parent, child = context.Pipe()
        process = context.Process(target=_restore_s3_file_io, args=(child,))
        process.start()
        child.close()
        try:
            self.assertTrue(parent.poll(15))
            self.assertEqual("ready", parent.recv())

            file_io = self._new_file_io()
            file_io.filesystem = pafs.LocalFileSystem()
            parent.send_bytes(pickle.dumps(file_io))

            self.assertTrue(parent.poll(15))
            self.assertEqual(("WHEN_REQUIRED", 1, True), parent.recv())
        finally:
            parent.close()
            process.join(15)
            if process.is_alive():
                process.terminate()
                process.join()
        self.assertEqual(0, process.exitcode)


if __name__ == "__main__":
    unittest.main()
