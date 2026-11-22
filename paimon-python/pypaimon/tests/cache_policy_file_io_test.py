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

import os
import pickle
import signal
import socket
import tempfile
import threading
import types
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
from urllib.parse import urlparse

import pyarrow.fs as pafs

from pypaimon.common.file_io import FileIO
from pypaimon.common.options import Options
from pypaimon.common.options.config import CatalogOptions, OssOptions
from pypaimon.filesystem.cache_policy_file_io import CachePolicyFileIO, _is_connection_failure


TABLE_ROOT = "oss://test-bucket/database/table"
META_PATH = TABLE_ROOT + "/snapshot/snapshot-1"
MANIFEST_PATH = TABLE_ROOT + "/manifest/manifest-a1b2c3d4-0"
DATA_PATH = TABLE_ROOT + "/bucket-0/data-a1b2c3d4-0.orc"
OTHER_DATA_PATH = TABLE_ROOT + "/bucket-1/data-a1b2c3d4-0.orc"
LATEST_PATH = TABLE_ROOT + "/snapshot/LATEST"


class _RecordingFileSystem:
    def __init__(self, name):
        self.name = name
        self.calls = []

    def normalize_path(self, path):
        self.calls.append(("normalize_path", path))
        return path

    def get_file_info(self, value):
        self.calls.append(("get_file_info", value))
        if isinstance(value, pafs.FileSelector):
            return [(self.name, value.base_dir)]
        return [(self.name, path) for path in value]

    def create_dir(self, path, recursive=True):
        self.calls.append(("create_dir", path, recursive))

    def delete_dir(self, path):
        self.calls.append(("delete_dir", path))

    def delete_dir_contents(self, path, **kwargs):
        self.calls.append(("delete_dir_contents", path, kwargs))

    def delete_file(self, path):
        self.calls.append(("delete_file", path))

    def move(self, src, dest):
        self.calls.append(("move", src, dest))

    def copy_file(self, src, dest):
        self.calls.append(("copy_file", src, dest))

    def open_input_stream(self, path):
        self.calls.append(("open_input_stream", path))
        return self.name, "open_input_stream", path

    def open_input_file(self, path):
        self.calls.append(("open_input_file", path))
        return self.name, "open_input_file", path

    def open_output_stream(self, path, **kwargs):
        self.calls.append(("open_output_stream", path, kwargs))
        return self.name, "open_output_stream", path

    def open_append_stream(self, path, **kwargs):
        self.calls.append(("open_append_stream", path, kwargs))
        return self.name, "open_append_stream", path


class _RecordingFileIO(FileIO):
    def __init__(self, name, options):
        self.name = name
        self.properties = options
        self.filesystem = _RecordingFileSystem(name)
        self.calls = []

    def _record(self, operation, *paths):
        self.calls.append((operation,) + paths)
        return (self.name, operation) + paths

    def new_input_stream(self, path: str):
        return self._record("read", path)

    def new_output_stream(self, path: str):
        return self._record("write", path)

    def get_file_status(self, path: str):
        return self._record("status", path)

    def list_status(self, path: str):
        return self._record("list", path)

    def exists(self, path: str) -> bool:
        self._record("exists", path)
        return True

    def exists_batch(self, paths):
        self._record("exists_batch", *paths)
        return {path: True for path in paths}

    def delete(self, path: str, recursive: bool = False) -> bool:
        self._record("delete", path, recursive)
        return True

    def mkdirs(self, path: str) -> bool:
        self._record("mkdirs", path)
        return True

    def rename(self, src: str, dst: str) -> bool:
        self._record("rename", src, dst)
        return True

    def get_file_size(self, path: str) -> int:
        self._record("size", path)
        return 1

    def is_dir(self, path: str) -> bool:
        self._record("is_dir", path)
        return False

    def to_filesystem_path(self, path: str) -> str:
        parsed = urlparse(path)
        if not parsed.scheme:
            return path
        if self.properties.get(OssOptions.OSS_IMPL) == "jindo":
            return parsed.path.lstrip("/")
        path_part = parsed.path.lstrip("/")
        return parsed.netloc + "/" + path_part if path_part else parsed.netloc

    def write_parquet(self, path: str, *args, **kwargs):
        return self._record("write_parquet", path)

    def close(self):
        self._record("close")


class _RecordingFactory:
    def __init__(self):
        self.calls = []

    def __call__(self, path, options):
        endpoint = options.get(OssOptions.OSS_ENDPOINT)
        impl = options.get(OssOptions.OSS_IMPL)
        name = "{}:{}".format(impl, endpoint)
        file_io = _RecordingFileIO(name, options)
        self.calls.append((path, options, file_io))
        return file_io


class _LocalArrowFileIO(_RecordingFileIO):
    def __init__(self, name, options, root):
        super().__init__(name, options)
        self.filesystem = pafs.SubTreeFileSystem(root, pafs.LocalFileSystem())

    def to_filesystem_path(self, path: str) -> str:
        parsed = urlparse(path)
        return parsed.path.lstrip("/") if parsed.scheme else path


def _options(**overrides):
    values = {
        CatalogOptions.IO_CACHE_POLICY.key(): "meta,read,write",
        CatalogOptions.IO_CACHE_ORIGIN_ENDPOINT.key(): "origin-endpoint",
        CatalogOptions.IO_CACHE_WHITELIST.key(): "*",
        CatalogOptions.IO_CACHE_WHITELIST_PATH.key(): "*",
        OssOptions.OSS_ENDPOINT.key(): "cache-endpoint",
        OssOptions.OSS_IMPL.key(): "jindo",
    }
    values.update(overrides)
    return Options(values)


class CachePolicyFileIOTest(unittest.TestCase):

    def test_equal_endpoints_use_plain_file_io_without_cache_probe(self):
        for impl in ("jindo", "legacy"):
            for origin in (None, "", " ", "cache-endpoint"):
                with self.subTest(impl=impl, origin=origin):
                    options = _options(**{
                        OssOptions.OSS_IMPL.key(): impl,
                        CatalogOptions.IO_CACHE_ORIGIN_ENDPOINT.key(): origin})
                    factory = _RecordingFactory()
                    io = CachePolicyFileIO.create(TABLE_ROOT, options, factory)
                    self.assertIsInstance(io, _RecordingFileIO)
                    self.assertNotIsInstance(io, CachePolicyFileIO)
                    io.new_input_stream(DATA_PATH)
                    io.new_output_stream(DATA_PATH)
                    io.get_file_status(DATA_PATH)
                    self.assertEqual(1, len(factory.calls))
                    self.assertEqual([("read", DATA_PATH), ("write", DATA_PATH),
                                      ("status", DATA_PATH)], io.calls)
                    self.assertEqual("cache-endpoint", io.properties.get(OssOptions.OSS_ENDPOINT))

    def test_vortex_native_options_follow_operation_path_and_disabled_state(self):
        from pypaimon.filesystem.vortex_utils import to_vortex_specified

        path = TABLE_ROOT + "/bucket-0/data-a.vortex"
        for policy, operation, expected in (
                ("meta", "read", "origin-endpoint"),
                ("read", "write", "origin-endpoint"),
                ("read", "read", "cache-endpoint"),
                ("write", "write", "cache-endpoint")):
            io = CachePolicyFileIO.create(
                TABLE_ROOT, _options(**{"io-cache.policy": policy}), _RecordingFactory())
            _, storage = to_vortex_specified(io, path, operation)
            self.assertIn(expected, storage["endpoint"])

        factory = _RecordingFactory()
        io = CachePolicyFileIO.create(
            TABLE_ROOT, _options(**{CatalogOptions.IO_CACHE_WHITELIST_PATH.key(): "bucket-1"}), factory)
        self.assertIn("origin-endpoint", to_vortex_specified(io, path)[1]["endpoint"])
        self.assertEqual(1, len(factory.calls))

        io = CachePolicyFileIO.create(TABLE_ROOT, _options(), factory)
        io.new_input_stream(path)
        with patch.object(factory.calls[-1][2], "new_input_stream", side_effect=ConnectionRefusedError("refused")), \
                self.assertLogs("pypaimon.filesystem.cache_policy_file_io", level="WARNING"):
            io.new_input_stream(path)
        for operation in ("read", "write"):
            self.assertIn("origin-endpoint", to_vortex_specified(io, path, operation)[1]["endpoint"])

    def test_vortex_writer_selects_write_options(self):
        from unittest.mock import MagicMock
        from pypaimon.execution.python.write.format_file_writer import write_vortex_file

        fake = types.ModuleType("vortex")
        fake.array = MagicMock(return_value="array")
        fake.store = types.SimpleNamespace(from_url=MagicMock())
        io = CachePolicyFileIO.create(
            TABLE_ROOT, _options(**{"io-cache.policy": "read"}), _RecordingFactory())
        with patch.dict("sys.modules", {"vortex": fake}):
            write_vortex_file(io, TABLE_ROOT + "/bucket-0/data-a.vortex", [])
        self.assertIn("origin-endpoint", fake.store.from_url.call_args.kwargs["endpoint"])
        fake.store.from_url.return_value.write.assert_called_once_with("array")

    def test_vortex_options_resolve_policy_inside_local_block_cache(self):
        from pypaimon.filesystem.caching_file_io import CachingFileIO
        from pypaimon.filesystem.vortex_utils import to_vortex_specified

        policy = CachePolicyFileIO.create(
            TABLE_ROOT, _options(**{"io-cache.policy": "meta"}), _RecordingFactory())
        wrapped = CachingFileIO(policy, None)
        self.assertIn("origin-endpoint", to_vortex_specified(
            wrapped, TABLE_ROOT + "/bucket-0/data-a.vortex")[1]["endpoint"])

    def test_arrow_format_reader_with_resolving_delegates(self):
        import pyarrow as pa
        import pyarrow.orc as orc
        import pyarrow.parquet as pq
        from pypaimon.execution.python.read.reader.format_pyarrow_reader import FormatPyArrowReader
        from pypaimon.schema.data_types import DataField, AtomicType

        data = pa.table({"id": pa.array([1, 2, 3], type=pa.int64())})
        with tempfile.TemporaryDirectory() as root:
            for fmt, write in (("parquet", pq.write_table), ("orc", orc.write_table)):
                path = os.path.join(root, "bucket-0", "data-a." + fmt)
                os.makedirs(os.path.dirname(path), exist_ok=True)
                write(data, path)
                for policy in ("meta", "read"):
                    options = _options(**{
                        "resolving-file-io.enabled": "true", "io-cache.policy": policy})
                    uri = "file://" + path
                    with self.subTest(format=fmt, policy=policy):
                        io = CachePolicyFileIO.create("file://" + root, options)
                        reader = FormatPyArrowReader(io, fmt, uri, [DataField(0, "id", AtomicType("BIGINT"))], None)
                        try:
                            batches = []
                            while True:
                                batch = reader.read_arrow_batch()
                                if batch is None:
                                    break
                                batches.append(batch)
                            self.assertEqual(data.to_pylist(), pa.Table.from_batches(batches).to_pylist())
                            info = io.filesystem.get_file_info([uri])[0]
                            self.assertEqual(uri, info.path)
                            self.assertEqual(pafs.FileType.File, info.type)
                            selector = pafs.FileSelector("file://" + os.path.dirname(path))
                            self.assertIn(uri, [entry.path for entry in io.filesystem.get_file_info(selector)])
                        finally:
                            reader.close()
                            io.close()

    @unittest.skipUnless(hasattr(os, "fork"), "requires fork")
    def test_fork_during_cache_initialization_recreates_lock_and_backends(self):
        parent_pid = os.getpid()
        entered, release = threading.Event(), threading.Event()
        factory = _RecordingFactory()

        def blocking_factory(path, options):
            if os.getpid() == parent_pid and options.get(OssOptions.OSS_ENDPOINT) == "cache-endpoint":
                entered.set()
                if not release.wait(10):
                    raise RuntimeError("parent initialization was not released")
            return factory(path, options)

        io = CachePolicyFileIO.create(TABLE_ROOT, _options(), blocking_factory)
        from pypaimon.catalog.rest.rest_token_file_io import RESTTokenFileIO
        key = ("cache-policy-fork", "test")
        RESTTokenFileIO._FILE_IO_CACHE[key] = io
        self.addCleanup(RESTTokenFileIO._FILE_IO_CACHE.pop, key, None)
        origin = io._origin_file_io
        worker = threading.Thread(target=io.new_input_stream, args=(DATA_PATH,))
        worker.start()
        self.assertTrue(entered.wait(5))
        child = os.fork()
        if child == 0:
            signal.alarm(8)
            try:
                retained = RESTTokenFileIO._FILE_IO_CACHE[key]
                self.assertIs(io, retained)
                self.assertIn("cache-endpoint", retained.new_input_stream(DATA_PATH)[0])
                self.assertIn("origin-endpoint", io.new_input_stream(LATEST_PATH)[0])
                self.assertIsNot(origin, io._origin_file_io)
                os._exit(0)
            except BaseException:
                os._exit(1)
        release.set()
        worker.join(5)
        _, status = os.waitpid(child, 0)
        self.assertFalse(worker.is_alive())
        self.assertTrue(os.WIFEXITED(status), status)
        self.assertEqual(0, os.WEXITSTATUS(status))
        io.close()

    def test_transport_classifier_preserves_logical_errors(self):
        for error in (ConnectionRefusedError("refused"), socket.gaierror("DNS"),
                      TimeoutError("timeout"),
                      OSError(b"open file reader failed: Failed to connect Http service: http://cache"),
                      OSError("AWS Error NETWORK_CONNECTION during HeadObject operation: "
                              "curlCode: 28, Timeout was reached")):
            self.assertTrue(_is_connection_failure(error), repr(error))
        for error in (FileNotFoundError("missing"), PermissionError("denied"),
                      FileExistsError("exists"), InterruptedError("cancelled"),
                      OSError("read failed"), OSError("timeout"),
                      OSError("ErrorCode : 133, No such file"),
                      OSError("AWS Error ACCESS_DENIED: HTTP 403")):
            self.assertFalse(_is_connection_failure(error), repr(error))

    def test_unreachable_cache_is_probed_once_and_warns_once(self):
        factory = _RecordingFactory()
        file_io = CachePolicyFileIO.create(TABLE_ROOT, _options(), factory)
        with patch.object(_RecordingFileIO, "exists", side_effect=ConnectionRefusedError("refused")), \
                self.assertLogs("pypaimon.filesystem.cache_policy_file_io", level="WARNING") as logs:
            with ThreadPoolExecutor(max_workers=4) as executor:
                results = list(executor.map(file_io.new_input_stream, [DATA_PATH] * 16))
            self.assertTrue(all("origin-endpoint" in result[0] for result in results))
            self.assertIn("origin-endpoint", file_io.new_output_stream(DATA_PATH)[0])
        self.assertEqual(2, len(factory.calls))
        self.assertEqual(1, len(logs.output))
        self.assertIn("CACHE_POLICY_FALLBACK", logs.output[0])
        restored = pickle.loads(pickle.dumps(file_io))
        self.assertIn("origin-endpoint", restored.new_input_stream(DATA_PATH)[0])
        self.assertEqual(2, len(restored._file_io_factory.calls))

    def test_operation_failure_retries_origin_and_batch_handler_obeys_disabled_state(self):
        factory = _RecordingFactory()
        file_io = CachePolicyFileIO.create(TABLE_ROOT, _options(), factory)
        file_io.new_input_stream(DATA_PATH)
        cache = factory.calls[1][2]
        with patch.object(cache, "exists_batch", side_effect=ConnectionResetError("reset")) as batch, \
                self.assertLogs("pypaimon.filesystem.cache_policy_file_io", level="WARNING"):
            self.assertEqual({DATA_PATH: True, LATEST_PATH: True},
                             file_io.exists_batch([DATA_PATH, LATEST_PATH]))
            self.assertIn("origin-endpoint", file_io.new_input_stream(DATA_PATH)[0])
            infos = file_io._filesystem_handler.get_file_info([DATA_PATH, LATEST_PATH])
            self.assertTrue(all("origin-endpoint" in info[0] for info in infos))
            batch.assert_called_once()

    def test_arrow_handler_operation_failure_retries_origin(self):
        for method, args in (("open_input_file", (DATA_PATH,)),
                             ("open_output_stream", (DATA_PATH, None)),
                             ("get_file_info", ([DATA_PATH, LATEST_PATH],))):
            factory = _RecordingFactory()
            file_io = CachePolicyFileIO.create(TABLE_ROOT, _options(), factory)
            file_io.new_input_stream(DATA_PATH)
            cache_fs = factory.calls[1][2].filesystem
            with patch.object(cache_fs, method, side_effect=TimeoutError("timeout")), \
                    self.assertLogs("pypaimon.filesystem.cache_policy_file_io", level="WARNING"):
                result = getattr(file_io._filesystem_handler, method)(*args)
                self.assertIn("origin-endpoint", str(result))
            self.assertIn("origin-endpoint", file_io.new_input_stream(DATA_PATH)[0])

    def test_missing_permission_and_generic_errors_do_not_disable_cache(self):
        for error in (FileNotFoundError("missing"), PermissionError("denied"),
                      OSError("read failed")):
            factory = _RecordingFactory()
            file_io = CachePolicyFileIO.create(TABLE_ROOT, _options(), factory)
            file_io.new_input_stream(DATA_PATH)
            with patch.object(factory.calls[1][2], "new_input_stream", side_effect=error):
                with self.assertRaises(type(error)) as caught:
                    file_io.new_input_stream(DATA_PATH)
                self.assertIs(error, caught.exception)
            self.assertIn("cache-endpoint", file_io.new_input_stream(DATA_PATH)[0])
            self.assertNotIn(("read", DATA_PATH), factory.calls[0][2].calls)

    def test_origin_failure_retains_cache_failure_as_context(self):
        factory = _RecordingFactory()
        file_io = CachePolicyFileIO.create(TABLE_ROOT, _options(), factory)
        file_io.new_input_stream(DATA_PATH)
        cache_error = ConnectionRefusedError("refused")
        origin_error = FileNotFoundError("origin missing")
        with patch.object(factory.calls[1][2], "new_input_stream", side_effect=cache_error), \
                patch.object(factory.calls[0][2], "new_input_stream", side_effect=origin_error), \
                self.assertLogs("pypaimon.filesystem.cache_policy_file_io", level="WARNING"):
            with self.assertRaises(FileNotFoundError) as caught:
                file_io.new_input_stream(DATA_PATH)
        self.assertIs(origin_error, caught.exception)
        self.assertIs(cache_error, caught.exception.__context__)

    def test_real_native_arrow_unreachable_endpoint_falls_back(self):
        self._assert_real_backend_fallback("legacy")

    def test_real_pyjindo_unreachable_endpoint_falls_back(self):
        from pypaimon.filesystem.pyarrow_file_io import JINDO_AVAILABLE
        if not JINDO_AVAILABLE:
            self.skipTest("pyjindosdk is not installed")
        self._assert_real_backend_fallback("jindo")

    def _assert_real_backend_fallback(self, impl):
        from pypaimon.filesystem.pyarrow_file_io import PyArrowFileIO

        class FastFileIO(PyArrowFileIO):
            def _create_s3_retry_config(self, **kwargs):
                return super()._create_s3_retry_config(
                    max_attempts=1, request_timeout=1, connect_timeout=1)

        for first in ("new_input_stream", "new_output_stream", "get_file_status"):
            options = _options(**{
                OssOptions.OSS_IMPL.key(): impl,
                OssOptions.OSS_ENDPOINT.key(): "127.0.0.1:1",
                "fs.oss.accessKeyId": "probe-id",
                "fs.oss.accessKeySecret": "probe-secret",
                "fs.oss.region": "cn-hangzhou",
                "fs.oss.connection.timeout.millisecond": "500",
                "fs.oss.timeout.millisecond": "1000",
                "fs.oss.retry.count": "0",
            })
            origin_options = CachePolicyFileIO._origin_options(options)
            origin = _RecordingFileIO("origin", origin_options)
            cache = FastFileIO(TABLE_ROOT, options)
            self.assertEqual(impl == "jindo", cache._use_jindo)
            file_io = CachePolicyFileIO(
                TABLE_ROOT, options, origin, options, lambda path, opts: cache)
            with self.assertLogs("pypaimon.filesystem.cache_policy_file_io", level="WARNING") as logs:
                self.assertEqual("origin", getattr(file_io, first)(DATA_PATH)[0])
                self.assertEqual("origin", file_io.new_input_stream(DATA_PATH)[0])
                self.assertEqual("origin", file_io.new_output_stream(DATA_PATH)[0])
            self.assertEqual(1, len(logs.output))
            file_io.close()

    def test_cache_uses_standard_endpoint_and_origin_is_only_a_fallback(self):
        factory = _RecordingFactory()
        options = _options(**{"io-cache.endpoint": "unused-cache"})
        file_io = CachePolicyFileIO.create(TABLE_ROOT, options, factory)
        self.assertIn("cache-endpoint", file_io.new_input_stream(DATA_PATH)[0])
        self.assertIn("origin-endpoint", file_io.new_input_stream(LATEST_PATH)[0])
        self.assertEqual("cache-endpoint", options.get(OssOptions.OSS_ENDPOINT))

        options.to_map().pop(CatalogOptions.IO_CACHE_ORIGIN_ENDPOINT.key())
        file_io = CachePolicyFileIO.create(TABLE_ROOT, options, _RecordingFactory())
        self.assertIn("cache-endpoint", file_io.new_input_stream(LATEST_PATH)[0])

    def test_routes_jindo_and_native_file_io_above_concrete_implementation(self):
        for impl in ("jindo", "legacy"):
            with self.subTest(impl=impl):
                factory = _RecordingFactory()
                file_io = CachePolicyFileIO.create(
                    TABLE_ROOT,
                    _options(**{OssOptions.OSS_IMPL.key(): impl}),
                    factory,
                )

                self.assertIsInstance(file_io, CachePolicyFileIO)
                self.assertEqual("origin-endpoint", factory.calls[0][1].get(
                    OssOptions.OSS_ENDPOINT))
                self.assertEqual(impl, factory.calls[0][1].get(OssOptions.OSS_IMPL))

                self.assertEqual(
                    ("{}:cache-endpoint".format(impl), "read", DATA_PATH),
                    file_io.new_input_stream(DATA_PATH),
                )
                self.assertEqual(2, len(factory.calls))
                self.assertEqual("cache-endpoint", factory.calls[1][1].get(
                    OssOptions.OSS_ENDPOINT))
                self.assertEqual(impl, factory.calls[1][1].get(OssOptions.OSS_IMPL))

    def test_policy_operation_routing(self):
        factory = _RecordingFactory()
        file_io = CachePolicyFileIO.create(TABLE_ROOT, _options(), factory)

        self.assertIn("cache-endpoint", file_io.new_input_stream(DATA_PATH)[0])
        self.assertIn("cache-endpoint", file_io.new_output_stream(DATA_PATH)[0])
        self.assertIn("cache-endpoint", file_io.get_file_status(DATA_PATH)[0])
        self.assertTrue(file_io.exists(DATA_PATH))
        self.assertIn("origin-endpoint", file_io.list_status(TABLE_ROOT)[0])
        self.assertTrue(file_io.delete(DATA_PATH))
        self.assertTrue(file_io.mkdirs(TABLE_ROOT + "/new-dir"))
        self.assertTrue(file_io.rename(DATA_PATH, OTHER_DATA_PATH))

        origin_calls = factory.calls[0][2].calls
        self.assertIn(("list", TABLE_ROOT), origin_calls)
        self.assertIn(("delete", DATA_PATH, False), origin_calls)
        self.assertIn(("mkdirs", TABLE_ROOT + "/new-dir"), origin_calls)
        self.assertIn(("rename", DATA_PATH, OTHER_DATA_PATH), origin_calls)

    def test_debug_log_contains_path_route_and_endpoint(self):
        file_io = CachePolicyFileIO.create(
            TABLE_ROOT, _options(), _RecordingFactory())

        with self.assertLogs(
                "pypaimon.filesystem.cache_policy_file_io", level="DEBUG") as logs:
            file_io.new_input_stream(DATA_PATH)
            file_io.delete(META_PATH)

        output = "\n".join(logs.output)
        self.assertIn("CACHE_POLICY_ROUTE operation=read route=cache", output)
        self.assertIn("endpoint=cache-endpoint", output)
        self.assertIn("file_type=STRUCTURED_DATA", output)
        self.assertIn("path=" + DATA_PATH, output)
        self.assertIn("CACHE_POLICY_ROUTE operation=delete route=origin", output)
        self.assertIn("endpoint=origin-endpoint", output)

    def test_individual_policy_tags(self):
        cases = [
            ("meta", (True, False, False)),
            ("read", (False, True, False)),
            ("write", (False, False, True)),
            ("meta,read,write", (True, True, True)),
        ]
        for policy, expected in cases:
            with self.subTest(policy=policy):
                file_io = CachePolicyFileIO.create(
                    TABLE_ROOT,
                    _options(**{CatalogOptions.IO_CACHE_POLICY.key(): policy}),
                    _RecordingFactory(),
                )
                actual = (
                    "cache-endpoint" in file_io.get_file_status(DATA_PATH)[0],
                    "cache-endpoint" in file_io.new_input_stream(DATA_PATH)[0],
                    "cache-endpoint" in file_io.new_output_stream(DATA_PATH)[0],
                )
                self.assertEqual(expected, actual)

    def test_file_type_path_whitelist_and_mutable_files(self):
        factory = _RecordingFactory()
        file_io = CachePolicyFileIO.create(
            TABLE_ROOT,
            _options(**{
                CatalogOptions.IO_CACHE_WHITELIST.key(): "meta,data",
                CatalogOptions.IO_CACHE_WHITELIST_PATH.key(): "manifest,bucket-0",
            }),
            factory,
        )

        self.assertTrue(file_io.should_cache(MANIFEST_PATH))
        self.assertTrue(file_io.should_cache(DATA_PATH))
        self.assertFalse(file_io.should_cache(META_PATH))
        self.assertFalse(file_io.should_cache(OTHER_DATA_PATH))
        self.assertFalse(file_io.should_cache(LATEST_PATH))

    def test_native_filesystem_bucket_prefix_is_not_part_of_policy_path(self):
        factory = _RecordingFactory()
        file_io = CachePolicyFileIO.create(
            TABLE_ROOT,
            _options(**{
                OssOptions.OSS_IMPL.key(): "legacy",
                CatalogOptions.IO_CACHE_WHITELIST_PATH.key(): "test-bucket",
            }),
            factory,
        )

        converted_path = "test-bucket/database/table/bucket-0/data.orc"
        self.assertFalse(file_io.should_cache(converted_path))

        file_io = CachePolicyFileIO.create(
            TABLE_ROOT,
            _options(**{
                OssOptions.OSS_IMPL.key(): "legacy",
                CatalogOptions.IO_CACHE_WHITELIST_PATH.key(): "bucket-0",
            }),
            _RecordingFactory(),
        )
        self.assertTrue(file_io.should_cache(converted_path))

    def test_default_and_invalid_whitelists(self):
        options = _options()
        options.to_map().pop(CatalogOptions.IO_CACHE_WHITELIST.key())
        options.to_map().pop(CatalogOptions.IO_CACHE_WHITELIST_PATH.key())
        file_io = CachePolicyFileIO.create(
            TABLE_ROOT, options, _RecordingFactory())
        self.assertTrue(file_io.should_cache(META_PATH))
        self.assertTrue(file_io.should_cache(DATA_PATH))

        for whitelist in ("", "manifest,unknown"):
            with self.subTest(whitelist=whitelist):
                file_io = CachePolicyFileIO.create(
                    TABLE_ROOT,
                    _options(**{
                        CatalogOptions.IO_CACHE_WHITELIST.key(): whitelist,
                    }),
                    _RecordingFactory(),
                )
                self.assertFalse(file_io.should_cache(META_PATH))
                self.assertFalse(file_io.should_cache(DATA_PATH))

        file_io = CachePolicyFileIO.create(
            TABLE_ROOT,
            _options(**{CatalogOptions.IO_CACHE_WHITELIST_PATH.key(): ""}),
            _RecordingFactory(),
        )
        self.assertFalse(file_io.should_cache(DATA_PATH))

    def test_filesystem_routes_direct_pyarrow_access(self):
        factory = _RecordingFactory()
        file_io = CachePolicyFileIO.create(
            TABLE_ROOT,
            _options(**{CatalogOptions.IO_CACHE_WHITELIST.key(): "data"}),
            factory,
        )
        handler = file_io._filesystem_handler

        self.assertIsInstance(file_io.filesystem, pafs.PyFileSystem)
        self.assertIn("cache-endpoint", handler.open_input_file(DATA_PATH)[0])
        self.assertIn("origin-endpoint", handler.open_input_file(META_PATH)[0])
        self.assertIn("cache-endpoint", handler.open_output_stream(DATA_PATH, None)[0])

        infos = handler.get_file_info([META_PATH, DATA_PATH, OTHER_DATA_PATH])
        self.assertIn("origin-endpoint", infos[0][0])
        self.assertIn("cache-endpoint", infos[1][0])
        self.assertIn("cache-endpoint", infos[2][0])

    def test_pyarrow_filesystem_invokes_policy_handler(self):
        with tempfile.TemporaryDirectory() as origin_dir, \
                tempfile.TemporaryDirectory() as cache_dir:
            relative_path = "bucket-0/data.orc"
            for root, content in ((origin_dir, b"origin"), (cache_dir, b"cache")):
                full_path = os.path.join(root, relative_path)
                os.makedirs(os.path.dirname(full_path))
                with open(full_path, "wb") as output:
                    output.write(content)

            options = _options(**{
                CatalogOptions.IO_CACHE_POLICY.key(): "read",
                CatalogOptions.IO_CACHE_WHITELIST.key(): "data",
            })
            origin_file_io = _LocalArrowFileIO("origin", options, origin_dir)
            cache_file_io = _LocalArrowFileIO("cache", options, cache_dir)
            file_io = CachePolicyFileIO(
                TABLE_ROOT,
                options,
                origin_file_io,
                options,
                lambda path, cache_options: cache_file_io,
            )

            with file_io.filesystem.open_input_file(relative_path) as input_file:
                self.assertEqual(b"cache", input_file.read())

    def test_missing_or_none_policy_disables_wrapper(self):
        for policy in (None, "none", "none,read"):
            with self.subTest(policy=policy):
                factory = _RecordingFactory()
                file_io = CachePolicyFileIO.create(
                    TABLE_ROOT,
                    _options(**{CatalogOptions.IO_CACHE_POLICY.key(): policy}),
                    factory,
                )
                self.assertNotIsInstance(file_io, CachePolicyFileIO)
                self.assertEqual(1, len(factory.calls))
                self.assertEqual(
                    "origin-endpoint", factory.calls[0][1].get(OssOptions.OSS_ENDPOINT))

    def test_policy_does_not_require_vpc_validation(self):
        file_io = CachePolicyFileIO.create(TABLE_ROOT, _options(), _RecordingFactory())
        self.assertIsInstance(file_io, CachePolicyFileIO)
        file_io = CachePolicyFileIO.create(
            TABLE_ROOT,
            _options(**{
                "io-cache.vpc-validated": False,
                "io-cache.ignore-vpc-validate": False,
            }),
            _RecordingFactory(),
        )
        self.assertIn("cache-endpoint", file_io.new_input_stream(DATA_PATH)[0])

    def test_serialization_restores_policy_filesystem(self):
        file_io = CachePolicyFileIO.create(
            TABLE_ROOT, _options(), _RecordingFactory())
        restored = pickle.loads(pickle.dumps(file_io))

        self.assertTrue(restored.should_cache(DATA_PATH))
        self.assertIsInstance(restored.filesystem, pafs.PyFileSystem)
        self.assertIn(
            "cache-endpoint",
            restored._filesystem_handler.open_input_file(DATA_PATH)[0],
        )


if __name__ == '__main__':
    unittest.main()
