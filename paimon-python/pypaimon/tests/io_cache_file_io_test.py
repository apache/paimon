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

"""Tests for IoCacheRoutingFileIO and its pyarrow filesystem."""

import contextlib
import gzip
import inspect
import io
import os
import shutil
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pytest
from pyarrow._fs import FileSystemHandler

from pypaimon.common.file_io import FileIO
from pypaimon.filesystem.caching_file_io import (CachingFileIO,
                                                 LocalMemoryCacheManager)
from pypaimon.filesystem.io_cache_file_io import IoCacheRoutingFileIO
from pypaimon.filesystem.io_cache_routing import IoCacheRouting
from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.read.reader.format_avro_reader import FormatAvroReader
from pypaimon.read.reader.format_pyarrow_reader import FormatPyArrowReader
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.table.row.blob import BlobDescriptor
from pypaimon.utils.file_type import FileType

TABLE = "oss://bkt/db1.db/t1"
UUID = "8b1f7c2e-3a4d-4e5f-9a0b-1c2d3e4f5a6b"
PARTITION = TABLE + "/dt=1/bucket-0"
DATA = PARTITION + "/data-" + UUID + "-0.parquet"
OTHER_DATA = PARTITION + "/data-" + UUID + "-1.parquet"
UNKNOWN = PARTITION + "/part-" + UUID + "-0.parquet"
MANIFEST = TABLE + "/manifest/manifest-" + UUID + "-0"
SNAPSHOT = TABLE + "/snapshot/snapshot-1"
LATEST = TABLE + "/snapshot/LATEST"


def _routing(policy="meta,read", whitelist="*", **overrides):
    """Manifests go to the accel target, data files to the cluster target."""
    options = {
        "fs.oss.endpoint": "http://accel.example.com",
        "io-cache.enabled": "true",
        "io-cache.origin.endpoint": "https://origin.example.com",
        "io-cache.policy": policy,
        "io-cache.whitelist": whitelist,
        "io-cache.targets": "accel,cluster",
        "io-cache.target.accel.endpoint": "http://accel.example.com",
        "io-cache.target.cluster.endpoint": "http://10.0.0.1:8080",
        "io-cache.routes": "meta=accel;data=cluster",
    }
    options.update(overrides)
    return IoCacheRouting.create(options)


class _DirFileIO(LocalFileIO):
    """LocalFileIO that keeps oss://bucket/key under root/bucket/key, standing in for one endpoint."""

    def __init__(self, root):
        super().__init__()
        self.root = root
        self.error = None
        self.attempts = 0
        self.closed = False

    def _to_file(self, path):
        parsed = urlparse(path)
        if parsed.scheme != "oss":
            return super()._to_file(path)
        return Path(self.root, parsed.netloc, parsed.path.lstrip("/"))

    def _check(self):
        self.attempts += 1
        if self.error is not None:
            raise self.error

    def new_input_stream(self, path):
        self._check()
        return super().new_input_stream(path)

    def new_output_stream(self, path):
        self._check()
        return super().new_output_stream(path)

    def get_file_status(self, path):
        self._check()
        return super().get_file_status(path)

    def exists(self, path):
        self._check()
        return super().exists(path)

    def to_filesystem_path(self, path):
        # Only the routing filesystem calls this, right before it calls the filesystem.
        self._check()
        return super().to_filesystem_path(path)

    def close(self):
        self.closed = True


class IoCacheRoutingFileIOTest(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="io_cache_file_io_test_")
        self.origin = _DirFileIO(os.path.join(self.temp_dir, "origin"))
        self.accel = _DirFileIO(os.path.join(self.temp_dir, "accel"))
        self.cluster = _DirFileIO(os.path.join(self.temp_dir, "cluster"))
        self.created = []
        self.creation_error = None
        self.file_io = self._file_io(_routing())

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _create_target(self, name):
        self.created.append(name)
        if self.creation_error is not None:
            raise self.creation_error
        return {"accel": self.accel, "cluster": self.cluster}[name]

    def _file_io(self, routing):
        return IoCacheRoutingFileIO(routing, self.origin, self._create_target)

    @staticmethod
    def _put(file_io, path, content):
        file_io._to_file(path).parent.mkdir(parents=True, exist_ok=True)
        file_io._to_file(path).write_bytes(content)

    def _put_all(self, *paths):
        for path in paths:
            for name, file_io in (("origin", self.origin), ("accel", self.accel), ("cluster", self.cluster)):
                self._put(file_io, path, name.encode())

    @staticmethod
    def _content(file_io, path):
        file = file_io._to_file(path)
        return file.read_bytes() if file.exists() else None

    def _attempts(self):
        return self.origin.attempts, self.accel.attempts, self.cluster.attempts

    def test_reads_follow_the_route(self):
        self._put_all(DATA, MANIFEST, LATEST, UNKNOWN, SNAPSHOT)
        self.assertEqual(b"cluster", self.file_io.new_input_stream(DATA).read())
        self.assertEqual("cluster", self.file_io.read_file_utf8(DATA))
        self.assertEqual(b"lus", self.file_io.read_file_range(DATA, 1, 3))
        self.assertEqual(b"accel", self.file_io.new_input_stream(MANIFEST).read())
        for path in (LATEST, UNKNOWN, SNAPSHOT):
            with self.subTest(path=path):
                self._put(self.origin, path, b"origin")
                self.assertEqual(b"origin", self.file_io.new_input_stream(path).read())

    def test_single_target_takes_every_routed_type(self):
        self._put_all(DATA, MANIFEST)
        routing = IoCacheRouting.create({
            "io-cache.enabled": "true",
            "io-cache.endpoint": "http://cluster.example.com",
            "io-cache.policy": "read",
        })
        file_io = IoCacheRoutingFileIO(routing, self.origin, lambda name: {"default": self.cluster}[name])
        self.assertEqual(b"cluster", file_io.new_input_stream(DATA).read())
        self.assertEqual(b"cluster", file_io.filesystem.open_input_file(MANIFEST).read())
        self.assertEqual(6, file_io.get_file_size(DATA))

    def test_targets_are_created_on_first_use(self):
        self._put_all(DATA, MANIFEST, LATEST)
        self.file_io.new_input_stream(LATEST)
        self.assertEqual([], self.created)
        self.file_io.new_input_stream(DATA)
        self.file_io.get_file_status(DATA)
        self.assertEqual(["cluster"], self.created)
        self.file_io.new_input_stream(MANIFEST)
        self.assertEqual(["cluster", "accel"], self.created)

    def test_reads_without_read_policy_use_origin(self):
        self._put_all(DATA)
        file_io = self._file_io(_routing(policy="meta"))
        self.assertEqual(b"origin", file_io.new_input_stream(DATA).read())
        self.assertEqual(7, file_io.get_file_size(DATA))
        self.assertEqual(b"origin", self._file_io(_routing(whitelist="meta")).new_input_stream(DATA).read())

    def test_meta_follows_the_route(self):
        self._put(self.cluster, DATA, b"cached!")
        self._put(self.accel, MANIFEST, b"m")
        self.assertEqual(7, self.file_io.get_file_size(DATA))
        self.assertEqual(1, self.file_io.get_file_status(MANIFEST).size)
        with self.assertRaises(FileNotFoundError):
            self._file_io(_routing(policy="read")).get_file_size(DATA)

    def test_exists_follows_the_route(self):
        self._put(self.cluster, DATA, b"cached!")
        self._put(self.accel, MANIFEST, b"m")
        self._put(self.origin, OTHER_DATA, b"origin")
        self._put(self.origin, SNAPSHOT, b"origin")
        self._put(self.origin, TABLE + "/manifest/manifest-list-" + UUID + "-0", b"origin")
        # Without the exists token the origin, which has neither file, answers.
        self.assertFalse(self.file_io.exists(DATA))
        self.file_io = self._file_io(_routing(policy="meta,read,exists"))
        self.assertTrue(self.file_io.exists(DATA))
        # OTHER_DATA is routed to the cluster, which does not have it
        self.assertFalse(self.file_io.exists(OTHER_DATA))
        self.assertTrue(self.file_io.exists(SNAPSHOT))
        self.assertEqual({MANIFEST: True, DATA: True, SNAPSHOT: True, OTHER_DATA: False},
                         self.file_io.exists_batch([MANIFEST, DATA, SNAPSHOT, OTHER_DATA]))
        self.assertTrue(self._file_io(_routing(policy="read")).exists(OTHER_DATA))
        self.assertTrue(self.file_io.is_dir(PARTITION))
        self.assertTrue(self.file_io.is_dir(TABLE + "/manifest"))

    def test_listing_and_mutations_use_origin(self):
        self._put_all(DATA, SNAPSHOT)
        listed = self.file_io.list_status(PARTITION)
        self.assertEqual([str(self.origin._to_file(DATA))], [status.path for status in listed])

        self.assertTrue(self.file_io.rename(DATA, OTHER_DATA))
        self.assertIsNone(self._content(self.origin, DATA))
        self.assertEqual(b"origin", self._content(self.origin, OTHER_DATA))
        self.assertEqual(b"cluster", self._content(self.cluster, DATA))

        self.file_io.copy_file(OTHER_DATA, DATA)
        self.assertEqual(b"origin", self._content(self.origin, DATA))
        self.assertIsNone(self._content(self.cluster, OTHER_DATA))

        self.assertTrue(self.file_io.delete(DATA))
        self.assertIsNone(self._content(self.origin, DATA))
        self.assertEqual(b"cluster", self._content(self.cluster, DATA))

        self.assertTrue(self.file_io.mkdirs(TABLE + "/new-dir"))
        self.assertTrue(self.origin._to_file(TABLE + "/new-dir").is_dir())
        self.assertFalse(self.cluster._to_file(TABLE + "/new-dir").exists())

        self.assertTrue(self.file_io.try_to_write_atomic(TABLE + "/snapshot/snapshot-2", "{}"))
        self.assertEqual(b"{}", self._content(self.origin, TABLE + "/snapshot/snapshot-2"))
        self.assertEqual([], self.created)

    def test_writes_use_origin_without_write_policy(self):
        file_io = self._file_io(_routing(policy="read,meta"))
        with file_io.new_output_stream(DATA) as out:
            out.write(b"written")
        file_io.write_file(MANIFEST, "3")
        self.assertEqual(b"written", self._content(self.origin, DATA))
        self.assertEqual(b"3", self._content(self.origin, MANIFEST))
        self.assertEqual([], self.created)

    def test_writes_follow_the_route_with_write_policy(self):
        table = pa.table({"a": [1, 2]})
        file_io = self._file_io(_routing(policy="read,meta,write"))
        with file_io.new_output_stream(DATA) as out:
            out.write(b"written")
        file_io.write_parquet(OTHER_DATA, table)
        file_io.write_file(MANIFEST, "3")
        file_io.write_file(SNAPSHOT, "{}")
        self.assertEqual(b"written", self._content(self.cluster, DATA))
        self.assertEqual(table, pq.ParquetFile(str(self.cluster._to_file(OTHER_DATA))).read())
        self.assertEqual(b"3", self._content(self.accel, MANIFEST))
        self.assertEqual(b"{}", self._content(self.origin, SNAPSHOT))
        # write_file checks existence on origin, not on the target
        self._put(self.origin, OTHER_DATA, b"origin")
        # unknown names are written on origin
        with file_io.filesystem.open_output_stream(UNKNOWN) as out:
            out.write(b"streamed")
        self.assertEqual(b"streamed", self._content(self.origin, UNKNOWN))
        with self.assertRaises(FileExistsError):
            file_io.write_file(OTHER_DATA, "again")

    def test_filesystem_reads_follow_the_route(self):
        for file_io, value in ((self.origin, 1), (self.cluster, 2)):
            sink = pa.BufferOutputStream()
            pq.write_table(pa.table({"a": [value]}), sink)
            self._put(file_io, DATA, sink.getvalue().to_pybytes())
        filesystem = self.file_io.filesystem
        self.assertIs(filesystem, self.file_io.filesystem)
        path = self.file_io.to_filesystem_path(DATA)
        self.assertEqual([2], pq.read_table(path, filesystem=filesystem).column("a").to_pylist())
        with filesystem.open_input_file(path) as f:
            self.assertEqual([2], pq.read_table(f).column("a").to_pylist())
        origin_only = self._file_io(_routing(policy="meta"))
        self.assertEqual([1], pq.read_table(DATA, filesystem=origin_only.filesystem).column("a").to_pylist())

    def test_readers_use_the_routing_filesystem(self):
        for file_io, value in ((self.origin, 1), (self.accel, 2), (self.cluster, 3)):
            file_io.write_parquet(DATA, pa.table({"a": pa.array([value], pa.int64())}))
            file_io.write_avro(MANIFEST, pa.table({"a": pa.array([value], pa.int64())}))
        fields = [DataField(0, "a", AtomicType("BIGINT"))]
        size = self.cluster._to_file(DATA).stat().st_size

        reader = FormatPyArrowReader(self.file_io, "parquet", DATA, fields, None, file_size=size)
        self.assertEqual([3], reader.read_arrow_batch().column(0).to_pylist())
        reader = FormatAvroReader(self.file_io, MANIFEST, ["a"], fields, None)
        self.assertEqual([2], reader.read_arrow_batch().column(0).to_pylist())
        reader = FormatPyArrowReader(self._file_io(_routing(policy="meta")), "parquet", DATA, fields, None)
        self.assertEqual([1], reader.read_arrow_batch().column(0).to_pylist())

    def test_filesystem_file_info_follows_the_route(self):
        self._put(self.cluster, DATA, b"cached!")
        self._put(self.accel, MANIFEST, b"m")
        self._put(self.origin, LATEST, b"1")
        infos = self.file_io.filesystem.get_file_info([LATEST, DATA, MANIFEST, OTHER_DATA])
        self.assertEqual([pafs.FileType.File, pafs.FileType.File, pafs.FileType.File, pafs.FileType.NotFound],
                         [info.type for info in infos])
        self.assertEqual([1, 7, 1], [info.size for info in infos[:3]])
        self.assertEqual([LATEST, DATA, MANIFEST, OTHER_DATA], [info.path for info in infos])

    def test_directory_dataset_keeps_oss_paths_and_routes_reads(self):
        for file_io, value in ((self.origin, 1), (self.cluster, 2)):
            file_io.write_parquet(DATA, pa.table({"a": [value]}))
        dataset = ds.dataset(PARTITION, format="parquet", filesystem=self.file_io.filesystem)
        self.assertEqual([2], dataset.to_table().column("a").to_pylist())

    def test_filesystem_mutations_use_origin(self):
        self._put_all(DATA)
        filesystem = self.file_io.filesystem
        selected = filesystem.get_file_info(pafs.FileSelector(PARTITION))
        self.assertEqual([DATA], [info.path for info in selected])
        filesystem.copy_file(DATA, OTHER_DATA)
        self.assertEqual(b"origin", self._content(self.origin, OTHER_DATA))
        filesystem.delete_file(OTHER_DATA)
        filesystem.move(DATA, OTHER_DATA)
        self.assertEqual(b"origin", self._content(self.origin, OTHER_DATA))
        self.assertEqual(b"cluster", self._content(self.cluster, DATA))
        filesystem.create_dir(TABLE + "/new-dir")
        self.assertTrue(self.origin._to_file(TABLE + "/new-dir").is_dir())
        filesystem.delete_dir(TABLE + "/new-dir")
        filesystem.delete_dir_contents(PARTITION)
        self.assertEqual([], list(self.origin._to_file(PARTITION).iterdir()))
        self.assertEqual(b"cluster", self._content(self.cluster, DATA))
        self.assertEqual([], self.created)

    def test_filesystem_streams_are_compressed_once(self):
        path = PARTITION + "/data-" + UUID + "-0.json.gz"
        self.origin._to_file(path).parent.mkdir(parents=True)
        with self.file_io.filesystem.open_output_stream(path) as out:
            out.write(b"payload")
        self.assertEqual(b"payload", gzip.decompress(self._content(self.origin, path)))
        self._put(self.cluster, path, self._content(self.origin, path))
        self.origin._to_file(path).unlink()
        with self.file_io.filesystem.open_input_stream(path) as stream:
            self.assertEqual(b"payload", stream.read())

    def test_target_errors_are_raised(self):
        self._put(self.origin, DATA, b"origin")
        with self.assertRaises(FileNotFoundError):
            self.file_io.new_input_stream(DATA)
        with self.assertRaises(FileNotFoundError):
            self.file_io.get_file_status(DATA)
        with self.assertRaises(FileNotFoundError):
            self.file_io.filesystem.open_input_file(DATA)
        with self.assertRaises(FileNotFoundError):
            self.file_io.filesystem.open_input_stream(DATA)
        # Target errors propagate without retrying the request on origin.
        for error in (OSError("[E1010]HTTP Status: 503 Error Code: SlowDown"), ConnectionRefusedError("refused"),
                      TimeoutError("timed out"), OSError("[E1010]HTTP Status: 403 Error Code: AccessDenied")):
            self.cluster.error = error
            with self.assertRaises(type(error)):
                self.file_io.new_input_stream(DATA)
            with self.assertRaises(type(error)):
                self.file_io.filesystem.open_input_file(DATA)
        self.assertEqual(0, self.origin.attempts)

    def test_target_that_cannot_be_created_raises(self):
        self._put_all(DATA, MANIFEST)
        self.creation_error = RuntimeError("cannot connect")
        with self.assertRaisesRegex(RuntimeError, "cannot connect"):
            self.file_io.new_input_stream(DATA)
        self.creation_error = None
        self.assertEqual(b"cluster", self.file_io.new_input_stream(DATA).read())
        self.assertEqual(["cluster", "cluster"], self.created)

    def test_register_file_size_reaches_the_routed_filesystem(self):
        origin, targets = _RecordingFileIO(), {}

        def create(name):
            targets[name] = _RecordingFileIO()
            return targets[name]

        file_io = IoCacheRoutingFileIO(_routing(), origin, create)
        handler = file_io.filesystem.handler
        handler.register_file_size(DATA, 7)
        handler.register_file_size(LATEST, 1)
        self.assertEqual(["register_file_size"], origin.calls)
        self.assertEqual({"cluster": ["register_file_size"]}, {name: io.calls for name, io in targets.items()})

    def test_close_closes_created_targets(self):
        self._put_all(DATA)
        self.file_io.new_input_stream(DATA)
        self.file_io.close()
        self.assertEqual((True, False, True), (self.origin.closed, self.accel.closed, self.cluster.closed))

    def test_local_block_cache_on_top(self):
        self._put_all(DATA)
        caching = CachingFileIO(self.file_io, LocalMemoryCacheManager(1 << 20), {FileType.DATA})
        self.assertEqual(b"cluster", caching.new_input_stream(DATA).read())
        self.assertIs(self.file_io.filesystem, caching.filesystem)
        self.assertEqual(DATA, caching.to_filesystem_path(DATA))

    def test_unrouted_attributes_come_from_origin(self):
        self.assertIs(self.origin.properties, self.file_io.properties)
        self.assertIs(self.origin.uri_reader_factory, self.file_io.uri_reader_factory)
        self.assertFalse(hasattr(self.file_io, "file_io"))


class _RecordingHandler(FileSystemHandler):

    def __init__(self, calls):
        self.calls = calls

    def _record(self, name, result=None):
        self.calls.append(name)
        return result

    def __eq__(self, other):
        return self is other

    def __ne__(self, other):
        return self is not other

    def get_type_name(self):
        return "recording"

    def normalize_path(self, path):
        return path

    def register_file_size(self, path, file_size):
        self._record("register_file_size")

    def get_file_info(self, paths):
        return self._record("get_file_info", [pafs.FileInfo(path, pafs.FileType.File, size=0) for path in paths])

    def get_file_info_selector(self, selector):
        return self._record("get_file_info_selector", [])

    def create_dir(self, path, recursive):
        self._record("create_dir")

    def delete_dir(self, path):
        self._record("delete_dir")

    def delete_dir_contents(self, path, missing_dir_ok=False):
        self._record("delete_dir_contents")

    def delete_root_dir_contents(self):
        self._record("delete_root_dir_contents")

    def delete_file(self, path):
        self._record("delete_file")

    def move(self, src, dest):
        self._record("move")

    def copy_file(self, src, dest):
        self._record("copy_file")

    def open_input_stream(self, path):
        return self._record("open_input_stream", pa.BufferReader(b""))

    def open_input_file(self, path):
        return self._record("open_input_file", pa.BufferReader(b""))

    def open_output_stream(self, path, metadata):
        return self._record("open_output_stream", pa.BufferOutputStream())

    def open_append_stream(self, path, metadata):
        return self._record("open_append_stream", pa.BufferOutputStream())


class _RecordingFileIO(FileIO):
    """Records every call; the routing tests only check which FileIO was called."""

    def __init__(self):
        self.calls = []
        self.filesystem = pafs.PyFileSystem(_RecordingHandler(self.calls))

    def _record(self, name, result=None):
        self.calls.append(name)
        return result

    def new_input_stream(self, path):
        return self._record("new_input_stream", io.BytesIO(b""))

    def new_output_stream(self, path):
        return self._record("new_output_stream", io.BytesIO())

    def get_file_status(self, path):
        return self._record("get_file_status", pafs.FileInfo(path, pafs.FileType.File, size=0))

    def list_status(self, path):
        return self._record("list_status", [])

    def exists(self, path):
        return self._record("exists", True)

    def exists_batch(self, paths):
        return self._record("exists_batch", {path: True for path in paths})

    def delete(self, path, recursive=False):
        return self._record("delete", True)

    def mkdirs(self, path):
        return self._record("mkdirs", True)

    def rename(self, src, dst):
        return self._record("rename", True)

    def copy_file(self, source_path, target_path, overwrite=False):
        self._record("copy_file")

    def try_to_write_atomic(self, path, content):
        return self._record("try_to_write_atomic", True)

    def create_blob_presigned_url(self, table_root, descriptor, validity):
        return self._record("create_blob_presigned_url", "https://signed")

    def write_parquet(self, path, data, compression='zstd', zstd_level=1, **kwargs):
        self._record("write_parquet")

    def to_filesystem_path(self, path):
        return path


# The FileIO and filesystem calls of each routing operation; pypaimon has no two-phase writer.
OP_CALLS = {
    "read": {
        "new_input_stream": lambda f, p: f.new_input_stream(p),
        "read_file_range": lambda f, p: f.read_file_range(p, 0, 1),
        "fs.open_input_file": lambda f, p: f.filesystem.open_input_file(p),
        "fs.open_input_stream": lambda f, p: f.filesystem.open_input_stream(p),
    },
    "meta": {
        "get_file_status": lambda f, p: f.get_file_status(p),
        "get_file_size": lambda f, p: f.get_file_size(p),
        "fs.get_file_info": lambda f, p: f.filesystem.get_file_info([p]),
    },
    "exists": {
        "exists": lambda f, p: f.exists(p),
        "exists_batch": lambda f, p: f.exists_batch([p]),
    },
    "write": {
        "new_output_stream": lambda f, p: f.new_output_stream(p),
        "write_parquet": lambda f, p: f.write_parquet(p, None),
        "fs.open_output_stream": lambda f, p: f.filesystem.open_output_stream(p),
        "fs.open_append_stream": lambda f, p: f.filesystem.open_append_stream(p),
    },
    "list": {
        "list_status": lambda f, p: f.list_status(p),
        "fs.get_file_info_selector": lambda f, p: f.filesystem.get_file_info(pafs.FileSelector(p)),
    },
    "delete": {
        "delete": lambda f, p: f.delete(p),
        "fs.delete_file": lambda f, p: f.filesystem.delete_file(p),
        "fs.delete_dir": lambda f, p: f.filesystem.delete_dir(p),
    },
    "rename": {
        "rename": lambda f, p: f.rename(p, p + "-renamed"),
        "fs.move": lambda f, p: f.filesystem.move(p, p + "-renamed"),
    },
    "mkdirs": {
        "mkdirs": lambda f, p: f.mkdirs(p),
        "fs.create_dir": lambda f, p: f.filesystem.create_dir(p),
    },
    "copy": {
        "copy_file": lambda f, p: f.copy_file(p, p + "-copy"),
        "fs.copy_file": lambda f, p: f.filesystem.copy_file(p, p + "-copy"),
    },
    "atomic-write": {
        "try_to_write_atomic": lambda f, p: f.try_to_write_atomic(p, "{}"),
    },
    "two-phase-write": {},
    "presign": {
        "create_blob_presigned_url": lambda f, p: f.create_blob_presigned_url(
            TABLE, BlobDescriptor(p, 0, 1), timedelta(minutes=1)),
    },
}

# Inherited FileIO methods that only read through new_input_stream, or do no I/O.
INHERITED_READS = {
    "read_blobs_concurrent", "read_file_range", "read_file_utf8", "read_overwritten_file_utf8",
    "read_ranges_coalesced", "read_ranges_coalesced_views",
}
NO_IO = {"get", "parse_location"}


def test_every_file_io_method_is_classified():
    """A FileIO method the routing FileIO neither overrides nor lists here could reach a cache."""
    public = {name for name, value in inspect.getmembers(FileIO)
              if not name.startswith("_") and callable(value)}
    overridden = {name for name in public if name in IoCacheRoutingFileIO.__dict__}
    assert public - overridden - INHERITED_READS - NO_IO == set()
    assert not overridden & INHERITED_READS


@pytest.mark.parametrize("call", [
    lambda f: f.write_file(DATA, "x"),
    lambda f: f.overwrite_file_utf8(DATA, "x"),
    lambda f: f.delete_quietly(DATA),
    lambda f: f.delete_files_quietly([DATA]),
    lambda f: f.check_or_mkdirs(PARTITION),
    lambda f: f.copy_files(PARTITION, PARTITION + "-copy"),
])
def test_write_and_delete_helpers_use_origin(call):
    routing = _routing()
    file_ios = {key: _RecordingFileIO() for key in ("origin", "accel", "cluster")}
    # the recorder says every path exists, so the existence checks may raise
    with contextlib.suppress(FileExistsError, ValueError):
        call(IoCacheRoutingFileIO(routing, file_ios["origin"], file_ios.__getitem__))
    assert file_ios["origin"].calls
    assert not file_ios["accel"].calls and not file_ios["cluster"].calls


FILE_IO_CALLS = [(op, name, call) for op, calls in OP_CALLS.items() for name, call in calls.items()]


@pytest.mark.parametrize("op,name,call", FILE_IO_CALLS,
                         ids=["{} / {}".format(op, name) for op, name, _ in FILE_IO_CALLS])
@pytest.mark.parametrize("policy", ["meta,read", "meta,read,write", "meta,read,exists"])
def test_each_file_io_call_reaches_its_endpoint(policy, op, name, call):
    tokens = set(policy.split(","))
    routed = {"read", "meta"} | ({"write", "two-phase-write"} if "write" in tokens else set()) | (tokens & {"exists"})
    routing = _routing(policy=policy)
    for path, target in ((DATA, "cluster"), (MANIFEST, "accel"), (SNAPSHOT, "origin"), (UNKNOWN, "origin")):
        file_ios = {key: _RecordingFileIO() for key in ("origin", "accel", "cluster")}
        call(IoCacheRoutingFileIO(routing, file_ios["origin"], file_ios.__getitem__), path)
        called = {key for key, recorder in file_ios.items() if recorder.calls}
        assert called == {target if op in routed else "origin"}, path


if __name__ == "__main__":
    unittest.main()
