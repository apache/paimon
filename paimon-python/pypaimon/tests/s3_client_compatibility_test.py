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

"""Unit tests for PyArrow-backed OSS and S3-compatible storage.

No real OSS access is required.
"""

import multiprocessing
import os
import pickle
import unittest
import threading
from itertools import product
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from unittest import mock

import pyarrow
import pyarrow.fs as pafs
from packaging.version import parse

from pypaimon.common.options import Options
from pypaimon.common.options.config import OssOptions, S3Options
from pypaimon.filesystem.pyarrow_file_io import PyArrowFileIO


def _restore_s3_file_io(connection):
    connection.send("ready")
    payload = connection.recv_bytes()
    client = object()
    settings = []

    def create_client(**kwargs):
        settings.append(os.environ.get("AWS_REQUEST_CHECKSUM_CALCULATION"))
        return client

    with mock.patch("pyarrow.fs.S3FileSystem", side_effect=create_client) as s3:
        restored = pickle.loads(payload)
    connection.send((
        os.environ.get("AWS_REQUEST_CHECKSUM_CALCULATION"),
        settings,
        s3.call_count,
        restored.filesystem is client,
    ))
    connection.close()


def _write_in_worker(connection):
    connection.send("ready")
    try:
        file_io = pickle.loads(connection.recv_bytes())
        with file_io.filesystem.open_output_stream("test-bucket/file") as stream:
            stream.write(b"x")
        connection.send("written")
    except Exception as error:
        connection.send(repr(error))
    finally:
        connection.close()


class _HTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True


class _UploadHandler(BaseHTTPRequestHandler):
    def _respond(self, body=b""):
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("ETag", '"etag"')
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)
        self.close_connection = True

    def do_POST(self):
        if "uploads" in self.path:
            self._respond(b"<InitiateMultipartUploadResult><Bucket>test-bucket</Bucket>"
                          b"<Key>file</Key><UploadId>test-id</UploadId>"
                          b"</InitiateMultipartUploadResult>")
        else:
            self._respond(b"<CompleteMultipartUploadResult><Bucket>test-bucket</Bucket>"
                          b"<Key>file</Key><ETag>etag</ETag></CompleteMultipartUploadResult>")

    def do_PUT(self):
        self.server.put_headers.append(dict(self.headers))
        self._respond()

    def log_message(self, *args):
        pass


class S3ClientCompatibilityTest(unittest.TestCase):
    def _new_file_io(self, scheme="s3"):
        if scheme == "oss":
            options = Options({
                OssOptions.OSS_IMPL.key(): "legacy",
                OssOptions.OSS_ENDPOINT.key(): "oss-cn-test.example.com",
                OssOptions.OSS_ACCESS_KEY_ID.key(): "ak",
                OssOptions.OSS_ACCESS_KEY_SECRET.key(): "sk",
            })
        else:
            options = Options({S3Options.S3_ENDPOINT.key(): "http://minio:9000"})
        with mock.patch("pyarrow.fs.S3FileSystem", return_value=pafs.LocalFileSystem()):
            file_io = PyArrowFileIO(scheme + "://test-bucket/", options)
        return file_io

    def test_environment_unchanged_after_client_creation_failure(self):
        for previous in (None, "WHEN_SUPPORTED"):
            with self.subTest(previous=previous), mock.patch.dict(os.environ, {}, clear=True):
                if previous is not None:
                    os.environ["AWS_REQUEST_CHECKSUM_CALCULATION"] = previous
                with mock.patch("pyarrow.fs.S3FileSystem", side_effect=RuntimeError("failed")):
                    with self.assertRaisesRegex(RuntimeError, "failed"):
                        self._new_file_io_failure()
                self.assertEqual(previous, os.environ.get("AWS_REQUEST_CHECKSUM_CALCULATION"))

    def _new_file_io_failure(self):
        PyArrowFileIO("s3://test-bucket/", Options({"fs.s3.endpoint": "http://minio:9000"}))

    @mock.patch.dict(os.environ, {"AWS_REQUEST_CHECKSUM_CALCULATION": "WHEN_REQUIRED"})
    def test_pickle_rebuilds_client_in_started_worker(self):
        methods = [name for name in ("spawn", "fork")
                   if name in multiprocessing.get_all_start_methods()]
        for scheme, method in product(("oss", "s3", "s3a", "s3n"), methods):
            with self.subTest(scheme=scheme, method=method):
                context = multiprocessing.get_context(method)
                parent, child = context.Pipe()
                process = context.Process(target=_restore_s3_file_io, args=(child,))
                process.start()
                child.close()
                try:
                    self.assertTrue(parent.poll(20))
                    self.assertEqual("ready", parent.recv())
                    file_io = self._new_file_io(scheme)
                    self.assertNotIn("filesystem", file_io.__getstate__())
                    parent.send_bytes(pickle.dumps(file_io))
                    self.assertTrue(parent.poll(20))
                    self.assertEqual(("WHEN_REQUIRED", ["WHEN_REQUIRED"], 1, True), parent.recv())
                finally:
                    parent.close()
                    process.join(20)
                    if process.is_alive():
                        process.terminate()
                        process.join()
                self.assertEqual(0, process.exitcode)

    def test_oss_initialization_preserves_process_setting(self):
        options = Options({
            OssOptions.OSS_ACCESS_KEY_ID.key(): "ak",
            OssOptions.OSS_ACCESS_KEY_SECRET.key(): "sk",
            OssOptions.OSS_ENDPOINT.key(): "oss-cn-test.example.com",
            OssOptions.OSS_REGION.key(): "cn-test",
            OssOptions.OSS_IMPL.key(): "legacy",
        })
        settings = []

        def create_client(**kwargs):
            settings.append(os.environ.get("AWS_REQUEST_CHECKSUM_CALCULATION"))
            return mock.Mock()

        with mock.patch.dict("os.environ", {
                "AWS_REQUEST_CHECKSUM_CALCULATION": "WHEN_SUPPORTED"}, clear=True), \
                mock.patch("pyarrow.fs.S3FileSystem", side_effect=create_client):
            PyArrowFileIO("oss://test-bucket/", options)
            self.assertEqual(
                "WHEN_SUPPORTED",
                os.environ["AWS_REQUEST_CHECKSUM_CALCULATION"])
        self.assertEqual(["WHEN_SUPPORTED"], settings)

    def test_initialization_preserves_environment_for_all_s3_schemes(self):
        options = Options({
            S3Options.S3_ENDPOINT.key(): "http://minio:9000",
        })
        for scheme in ("s3", "s3a", "s3n"):
            settings = []

            def create_client(**kwargs):
                settings.append(os.environ.get("AWS_REQUEST_CHECKSUM_CALCULATION"))
                return mock.Mock()

            with self.subTest(scheme=scheme), \
                    mock.patch.dict("os.environ", {}, clear=True), \
                    mock.patch("pyarrow.fs.S3FileSystem", side_effect=create_client):
                PyArrowFileIO(
                    "{}://test-bucket/warehouse".format(scheme), options)
                self.assertNotIn("AWS_REQUEST_CHECKSUM_CALCULATION", os.environ)
            self.assertEqual([None], settings)

    def test_native_s3_does_not_change_checksum_setting(self):
        with mock.patch.dict("os.environ", {}, clear=True), \
                mock.patch("pyarrow.fs.S3FileSystem", return_value=mock.Mock()):
            PyArrowFileIO("s3://test-bucket/warehouse", Options({}))
            self.assertNotIn(
                "AWS_REQUEST_CHECKSUM_CALCULATION", os.environ)

    def test_worker_recomputes_pyarrow_version(self):
        state = self._new_file_io().__getstate__()
        state.update({
            "_pyarrow_gte_8": False,
            "_pyarrow_gte_16": False,
            "_oss_bucket_in_endpoint": True,
        })
        restored = object.__new__(PyArrowFileIO)
        with mock.patch.object(
                PyArrowFileIO, "_initialize_s3_fs", return_value=object()):
            restored.__setstate__(state)

        version = parse(pyarrow.__version__)
        self.assertEqual(version >= parse("8.0.0"), restored._pyarrow_gte_8)
        self.assertEqual(version >= parse("16.0.0"), restored._pyarrow_gte_16)
        self.assertEqual(version < parse("16.0.0"), restored._oss_bucket_in_endpoint)

    def test_worker_accepts_old_non_oss_pickle(self):
        state = self._new_file_io().__dict__.copy()
        for key in ("_legacy_bucket_lock", "_is_s3",
                    "_s3_endpoint"):
            state.pop(key)
        state["filesystem"] = mock.Mock(spec=pafs.S3FileSystem)
        restored = object.__new__(PyArrowFileIO)
        with mock.patch.object(
                PyArrowFileIO, "_initialize_s3_fs", return_value=object()
        ) as initialize:
            restored.__setstate__(state)
        self.assertTrue(restored._is_s3)
        self.assertEqual("http://minio:9000", restored._s3_endpoint)
        initialize.assert_called_once()

        state["filesystem"] = pafs.LocalFileSystem()
        restored = object.__new__(PyArrowFileIO)
        restored.__setstate__(state)
        self.assertFalse(restored._is_s3)
        self.assertIsNone(restored._s3_endpoint)

    @unittest.skipUnless(
        parse(pyarrow.__version__) >= parse("22.0.0"),
        "requires PyArrow 22+ optional request checksums",
    )
    def test_explicit_process_setting_applies_to_parent_and_worker(self):
        server = _HTTPServer(
            ("127.0.0.1", 0), _UploadHandler)
        server.requests = []
        server.put_headers = []
        server.bucket_objects = {"test-bucket": set()}
        server_thread = threading.Thread(target=server.serve_forever)
        server_thread.start()
        try:
            endpoint = "http://127.0.0.1:{}".format(server.server_port)
            options = Options({
                S3Options.S3_ACCESS_KEY_ID.key(): "ak",
                S3Options.S3_ACCESS_KEY_SECRET.key(): "sk",
                S3Options.S3_ENDPOINT.key(): endpoint,
                S3Options.S3_REGION.key(): "us-east-1",
                "fs.s3.path.style.access": "true",
            })
            with mock.patch.dict(os.environ, {
                    "AWS_REQUEST_CHECKSUM_CALCULATION": "WHEN_REQUIRED",
                    "NO_PROXY": "127.0.0.1,localhost",
                    "no_proxy": "127.0.0.1,localhost",
            }):
                native = pafs.S3FileSystem(
                    access_key="ak", secret_key="sk", region="us-east-1",
                    endpoint_override=endpoint)
                compatible = PyArrowFileIO("s3://test-bucket/", options)
                self.assertEqual("WHEN_REQUIRED",
                                 os.environ["AWS_REQUEST_CHECKSUM_CALCULATION"])
                with native.open_output_stream("test-bucket/file") as stream:
                    stream.write(b"x")
                with compatible.filesystem.open_output_stream(
                        "test-bucket/file") as stream:
                    stream.write(b"x")

            context = multiprocessing.get_context("spawn")
            parent, child = context.Pipe()
            process = context.Process(target=_write_in_worker, args=(child,))
            with mock.patch.dict(os.environ, {"AWS_REQUEST_CHECKSUM_CALCULATION": "WHEN_REQUIRED"}):
                process.start()
            child.close()
            try:
                self.assertTrue(parent.poll(20))
                self.assertEqual("ready", parent.recv())
                parent.send_bytes(pickle.dumps(compatible))
                self.assertTrue(parent.poll(20))
                self.assertEqual("written", parent.recv())
            finally:
                parent.close()
                process.join(20)
                if process.is_alive():
                    process.terminate()
                    process.join()
            self.assertEqual(0, process.exitcode)
            self.assertEqual(3, len(server.put_headers))
            for headers in server.put_headers:
                self.assertNotIn("x-amz-trailer", {key.lower() for key in headers})
        finally:
            server.shutdown()
            server.server_close()
            server_thread.join()
