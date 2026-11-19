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
import tempfile
import unittest
from urllib.parse import urlparse

import pyarrow.fs as pafs

from pypaimon.common.file_io import FileIO
from pypaimon.common.options import Options
from pypaimon.common.options.config import CatalogOptions, OssOptions
from pypaimon.filesystem.cache_policy_file_io import CachePolicyFileIO


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
        CatalogOptions.IO_CACHE_VPC_VALIDATED.key(): True,
        CatalogOptions.IO_CACHE_ORIGIN_ENDPOINT.key(): "origin-endpoint",
        CatalogOptions.IO_CACHE_WHITELIST.key(): "*",
        CatalogOptions.IO_CACHE_WHITELIST_PATH.key(): "*",
        OssOptions.OSS_ENDPOINT.key(): "cache-endpoint",
        OssOptions.OSS_IMPL.key(): "jindo",
    }
    values.update(overrides)
    return Options(values)


class CachePolicyFileIOTest(unittest.TestCase):

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

    def test_vpc_validation_and_none_policy_disable_wrapper(self):
        factory = _RecordingFactory()
        file_io = CachePolicyFileIO.create(
            TABLE_ROOT,
            _options(**{CatalogOptions.IO_CACHE_VPC_VALIDATED.key(): False}),
            factory,
        )
        self.assertNotIsInstance(file_io, CachePolicyFileIO)
        self.assertEqual(1, len(factory.calls))
        self.assertEqual(
            "origin-endpoint",
            factory.calls[0][1].get(OssOptions.OSS_ENDPOINT),
        )

        factory = _RecordingFactory()
        options = _options()
        options.to_map().pop(CatalogOptions.IO_CACHE_VPC_VALIDATED.key())
        file_io = CachePolicyFileIO.create(TABLE_ROOT, options, factory)
        self.assertNotIsInstance(file_io, CachePolicyFileIO)

        factory = _RecordingFactory()
        file_io = CachePolicyFileIO.create(
            TABLE_ROOT,
            _options(**{
                CatalogOptions.IO_CACHE_VPC_VALIDATED.key(): False,
                "io-cache.ignore-vpc-validate": True,
            }),
            factory,
        )
        self.assertNotIsInstance(file_io, CachePolicyFileIO)

        factory = _RecordingFactory()
        file_io = CachePolicyFileIO.create(
            TABLE_ROOT,
            _options(**{CatalogOptions.IO_CACHE_POLICY.key(): "none"}),
            factory,
        )
        self.assertNotIsInstance(file_io, CachePolicyFileIO)

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
