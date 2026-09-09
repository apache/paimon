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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import io
import os
import shutil
import sys
import tempfile
import unittest
import warnings
from dataclasses import fields
from functools import partial
from pathlib import Path
from unittest.mock import Mock, patch

import numpy as np
import pyarrow as pa
import pyarrow.fs as pafs

import pypaimon.multimodal as pmm
from pypaimon.common.options import Options
from pypaimon.filesystem.pyarrow_file_io import LegacyOssDirectoryListingError
from pypaimon.multimodal.hdf5 import (
    _Hdf5SourceFileIO,
    _normalize_source_path,
    _qualified_status_path,
    _strict_arrow_table,
    load_from_hdf5,
)

try:
    import h5py
except ImportError:
    h5py = None


_OPTIONS = {
    "file.format": "parquet",
    "vector.file.format": "parquet",
    "blob-as-descriptor": "false",
}


class Hdf5RuntimeContractTest(unittest.TestCase):

    def test_hdf5_load_requires_python_38_or_newer(self):
        with patch(
                "pypaimon.multimodal.hdf5.sys.version_info", (3, 7)):
            with self.assertRaisesRegex(RuntimeError, "Python 3.8 or newer"):
                load_from_hdf5(Mock(), [], transform=lambda h5, source: ())

    def test_hdf5_source_io_decodes_only_its_external_uri(self):
        backend = Mock()
        backend.to_filesystem_path.return_value = (
            "bucket/episode%23copy%3F1.h5")
        backend.get_file_status.return_value = "status"
        resolver = Mock()
        resolver._get_fileio.return_value = backend

        with patch(
                "pypaimon.multimodal.source_utils.ResolvingFileIO",
                return_value=resolver):
            source_io = _Hdf5SourceFileIO(Options({}))
            self.assertEqual(
                "status",
                source_io.get_file_status(
                    "s3://bucket/episode%23copy%3F1.h5"),
            )

        backend.get_file_status.assert_called_once_with(
            "bucket/episode#copy?1.h5")


class _WriterProxy:

    def __init__(self, delegate, write_error=None):
        self.delegate = delegate
        self.write_error = write_error
        self.abort_count = 0
        self.close_count = 0

    def write_arrow(self, table):
        if self.write_error is not None:
            raise self.write_error
        return self.delegate.write_arrow(table)

    def prepare_commit(self):
        return self.delegate.prepare_commit()

    def abort(self):
        self.abort_count += 1
        return self.delegate.abort()

    def close(self):
        self.close_count += 1
        return self.delegate.close()


class _CommitterProxy:

    def __init__(self, delegate, commit_error=None, raise_after_commit=False):
        self.delegate = delegate
        self.commit_error = commit_error
        self.raise_after_commit = raise_after_commit
        self.commit_count = 0
        self.close_count = 0

    def add_commit_callback(self, callback):
        return self.delegate.add_commit_callback(callback)

    def commit(self, messages):
        self.commit_count += 1
        if self.commit_error is not None and not self.raise_after_commit:
            raise self.commit_error
        result = self.delegate.commit(messages)
        if self.commit_error is not None:
            raise self.commit_error
        return result

    def close(self):
        self.close_count += 1
        return self.delegate.close()


class _WriteBuilderProxy:

    def __init__(self, writer, committer):
        self.writer = writer
        self.committer = committer

    def new_write(self):
        return self.writer

    def new_commit(self):
        return self.committer


class _TrackedSourceStream(io.BytesIO):

    def __init__(self, data, source_file_io):
        super().__init__(data)
        self.source_file_io = source_file_io

    def seekable(self):
        return self.source_file_io.seekable

    def close(self):
        if not self.closed:
            self.source_file_io.closed_stream_count += 1
        super().close()


class _RemoteSourceFileIO:

    def __init__(
            self,
            objects=None,
            directories=None,
            seekable=True,
            native_stream=False):
        self.objects = {
            self._filesystem_path(path): value
            for path, value in dict(objects or {}).items()
        }
        self.directories = {
            self._filesystem_path(path): [
                self._filesystem_path(child) for child in children]
            for path, children in dict(directories or {}).items()
        }
        self.seekable = seekable
        self.native_stream = native_stream
        self.opened_paths = []
        self.streams = []
        self.closed_stream_count = 0
        self.close_count = 0
        self.list_error = None

    @staticmethod
    def _filesystem_path(path):
        from urllib.parse import urlparse
        parsed = urlparse(path)
        return "%s%s" % (parsed.netloc, parsed.path)

    def _status(self, path):
        if path in self.objects:
            file_type = pafs.FileType.File
        elif path in self.directories:
            file_type = pafs.FileType.Directory
        else:
            raise FileNotFoundError(path)
        return pafs.FileInfo(path, file_type)

    def _get_fileio(self, path):
        return self

    def to_filesystem_path(self, path):
        return self._filesystem_path(path)

    def get_file_status(self, path):
        return self._status(path)

    def list_status(self, path):
        if self.list_error is not None:
            raise self.list_error
        return [self._status(child) for child in self.directories[path]]

    def new_input_stream(self, path):
        self.opened_paths.append(path)
        if self.native_stream:
            stream = pa.BufferReader(self.objects[path])
        else:
            stream = _TrackedSourceStream(self.objects[path], self)
        self.streams.append(stream)
        return stream

    def close(self):
        self.close_count += 1


@unittest.skipIf(h5py is None, "h5py is not installed")
class MultimodalHdf5Test(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="pypaimon_hdf5_")
        self.source_dir = Path(self.temp_dir) / "source"
        self.warehouse = os.path.join(self.temp_dir, "warehouse")
        self.source_dir.mkdir()
        self.conn = pmm.connect(options={"warehouse": self.warehouse})

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _write_source(self, relative_path, offset=0):
        path = self.source_dir / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        with h5py.File(path, "w") as h5:
            h5.create_dataset(
                "values",
                data=np.arange(
                    offset, offset + 12, dtype=np.float32).reshape(4, 3),
            )
            images = h5.create_dataset(
                "images", (4,), dtype=h5py.vlen_dtype(np.dtype("uint8")))
            for index in range(4):
                jpeg = (b"\xff\xd8\xff" +
                        ("%s-%d" % (path.name, index)).encode("utf-8") +
                        b"\xff\xd9")
                images[index] = np.frombuffer(
                    jpeg, dtype=np.uint8)
        return path

    @staticmethod
    def _schema():
        return pa.schema([
            pa.field("source", pa.string(), nullable=False),
            pa.field("frame_index", pa.int32(), nullable=False),
            pa.field("score", pa.float64()),
            pa.field("value", pa.list_(pa.float32(), 3), nullable=False),
            pa.field("image", pa.large_binary()),
        ])

    def _create_table(self, name="frames"):
        return self.conn.create_table(
            name, schema=self._schema(), options=_OPTIONS)

    def _load(self, table, paths, *, transform, source_options=None):
        with patch.object(self.conn, "get_table", return_value=table):
            return self.conn.load_from_hdf5(
                table.identifier,
                paths,
                transform=transform,
                source_options=source_options,
            )

    @staticmethod
    def _transform(h5, source):
        values = np.asarray(h5["values"][:], dtype=np.float32)
        images = [
            np.asarray(value, dtype=np.uint8).tobytes()
            for value in h5["images"][:]
        ]
        for begin in range(0, len(values), 2):
            end = min(begin + 2, len(values))
            # Deliberately let Arrow infer int64, list<double>, and binary.
            # HDF5 loading may safely cast after validating exact columns.
            yield pa.Table.from_pydict({
                "source": [source.name] * (end - begin),
                "frame_index": list(range(begin, end)),
                "score": [None if index == 0 else float(index)
                          for index in range(begin, end)],
                "value": values[begin:end].tolist(),
                "image": images[begin:end],
            })

    def _instrument_write(
            self,
            table,
            *,
            write_error=None,
            commit_error=None,
            raise_after_commit=False):
        builder = table.raw_table.new_batch_write_builder()
        writer = _WriterProxy(builder.new_write(), write_error=write_error)
        committer = _CommitterProxy(
            builder.new_commit(),
            commit_error=commit_error,
            raise_after_commit=raise_after_commit,
        )
        proxy = _WriteBuilderProxy(writer, committer)
        return proxy, writer, committer

    def test_load_from_hdf5_streams_multiple_files_in_one_snapshot(self):
        first = self._write_source("a.hdf5", 0)
        self._write_source("nested/b.h5", 100)
        table = self._create_table()
        table.add(pa.Table.from_pydict({
            "source": ["seed"],
            "frame_index": [-1],
            "score": [None],
            "value": [[-1.0, -1.0, -1.0]],
            "image": [b"seed"],
        }, schema=self._schema()))
        before_snapshot = table.raw_table.snapshot_manager().get_latest_snapshot()
        seen = []
        seen_paths = []

        def transform(h5, source):
            seen.append(source.name)
            seen_paths.append(source.path)
            yield from self._transform(h5, source)

        result = self._load(
            table,
            [self.source_dir, first, first.resolve()],
            transform=transform,
        )

        self.assertEqual(2, result.file_count)
        self.assertEqual(4, result.batch_count)
        self.assertEqual(8, result.row_count)
        self.assertEqual(before_snapshot.id + 1, result.snapshot_id)
        self.assertEqual(
            result.snapshot_id,
            table.raw_table.snapshot_manager().get_latest_snapshot().id,
        )
        self.assertEqual(["a.hdf5", "b.h5"], seen)
        self.assertTrue(all(path.startswith("file://") for path in seen_paths))
        rows = table.scan().select([
            "source", "frame_index", "score", "value", "image",
        ]).to_arrow().to_pylist()
        self.assertEqual(9, len(rows))
        self.assertEqual({"seed", "a.hdf5", "b.h5"}, {
            row["source"] for row in rows
        })
        self.assertTrue(any(row["score"] is None for row in rows))
        self.assertTrue(all(
            row["image"] is None or isinstance(row["image"], bytes)
            for row in rows
        ))
        self.assertTrue(all(
            row["image"].startswith(b"\xff\xd8\xff")
            and row["image"].endswith(b"\xff\xd9")
            for row in rows if row["source"] != "seed"
        ))
        self.assertEqual(3, len(rows[-1]["value"]))

    def test_load_from_hdf5_accepts_paths_and_reappends(self):
        first = self._write_source("a.hdf5", 0)
        self._write_source("nested/b.h5", 100)
        table = self._create_table()

        single = self._load(table, first, transform=self._transform)
        duplicate_list = self._load(
            table,
            [first, first.resolve(), first.resolve().as_uri()],
            transform=self._transform,
        )
        directory = self._load(
            table, self.source_dir, transform=self._transform)

        self.assertEqual((1, 2, 4), (
            single.file_count, single.batch_count, single.row_count))
        self.assertEqual((1, 2, 4), (
            duplicate_list.file_count,
            duplicate_list.batch_count,
            duplicate_list.row_count,
        ))
        self.assertEqual((2, 4, 8), (
            directory.file_count, directory.batch_count, directory.row_count))
        self.assertEqual(16, table.scan().to_arrow().num_rows)
        self.assertEqual(
            ["file_count", "batch_count", "row_count", "snapshot_id"],
            [field.name for field in fields(type(directory))],
        )
        self.assertEqual(["path"], [
            field.name for field in fields(pmm.Hdf5File)
        ])

    def test_hdf5_file_exposes_decoded_local_path(self):
        source = self._write_source("episode 中文.h5")
        table = self._create_table("local_context")
        seen = []

        def transform(h5, context):
            seen.append(context)
            yield from self._transform(h5, context)

        self._load(table, source, transform=transform)

        self.assertEqual(source.resolve(), seen[0].local_path)
        self.assertEqual("episode 中文.h5", seen[0].name)
        self.assertEqual("episode 中文", seen[0].stem)
        self.assertIsNone(
            pmm.Hdf5File("s3://source-bucket/episode.h5").local_path)

        expected = source.resolve()
        context = pmm.Hdf5File(expected.as_uri())
        with patch.object(Path, "resolve", side_effect=AssertionError):
            self.assertEqual(expected, context.local_path)

    def test_windows_drive_paths_are_normalized_as_file_uris(self):
        path = r"C:\data set\episode.h5"
        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            self.assertEqual(
                "file:///C:/data%20set/episode.h5",
                _normalize_source_path(path),
            )

    def test_qualified_status_path_covers_fileio_path_shapes(self):
        cases = (
            ("s3://bucket/root", "s3://bucket/root/a.h5",
             "s3://bucket/root/a.h5"),
            ("file:///tmp/root", "/tmp/root/a.h5",
             Path("/tmp/root/a.h5").resolve().as_uri()),
            ("file:///C:/root", r"C:\root\a.h5",
             "file:///C:/root/a.h5"),
            ("hdfs://namenode:8020/root", "/root/a.h5",
             "hdfs://namenode:8020/root/a.h5"),
            ("viewfs://cluster/root", "root/a.h5",
             "viewfs://cluster/root/a.h5"),
            ("s3://bucket/root", "bucket/root/a.h5",
             "s3://bucket/root/a.h5"),
            ("s3://bucket/root", "root/a.h5",
             "s3://bucket/root/a.h5"),
            ("oss://bucket/root", "bucket/root/a.h5",
             "oss://bucket/root/a.h5"),
            ("gs://bucket/root", "root/a.h5",
             "gs://bucket/root/a.h5"),
            ("s3://bucket/root", "bucket/root/episode#backup.h5",
             "s3://bucket/root/episode%23backup.h5"),
            ("hdfs://namenode/root", "/root/episode?copy.hdf5",
             "hdfs://namenode/root/episode%3Fcopy.hdf5"),
            ("s3://bucket/root", "bucket/root/episode%23copy.h5",
             "s3://bucket/root/episode%2523copy.h5"),
        )
        for parent, status_path, expected in cases:
            with self.subTest(parent=parent, status_path=status_path):
                status = pafs.FileInfo(status_path, pafs.FileType.File)
                self.assertEqual(
                    expected, _qualified_status_path(parent, status))

    def test_load_from_hdf5_reads_recursive_remote_sources(self):
        root = "s3://source-bucket/episodes"
        first = root + "/a.hdf5"
        nested = root + "/nested"
        second = nested + "/b.h5"
        source_file_io = _RemoteSourceFileIO(
            objects={
                first: self._write_source("remote-a.hdf5", 0).read_bytes(),
                second: self._write_source("remote-b.h5", 100).read_bytes(),
            },
            directories={
                root: [nested, first],
                nested: [second],
            },
            native_stream=True,
        )
        source_options = {
            "fs.s3.endpoint": "https://source.example.com",
            "fs.s3.access-key": "source-key",
        }
        table = self._create_table("remote_frames")
        seen = []

        def transform(h5, source):
            seen.append((source.path, source.name, source.stem))
            yield from self._transform(h5, source)

        with patch(
                "pypaimon.multimodal.source_utils.ResolvingFileIO",
                return_value=source_file_io) as resolving_file_io:
            result = self._load(
                table,
                [root, first],
                transform=transform,
                source_options=source_options,
            )

        self.assertEqual((2, 4, 8), (
            result.file_count, result.batch_count, result.row_count))
        self.assertEqual([
            "source-bucket/episodes/a.hdf5",
            "source-bucket/episodes/nested/b.h5",
        ], source_file_io.opened_paths)
        self.assertEqual([
            (first, "a.hdf5", "a"),
            (second, "b.h5", "b"),
        ], seen)
        self.assertTrue(all(stream.closed for stream in source_file_io.streams))
        self.assertEqual(1, source_file_io.close_count)
        actual_options = resolving_file_io.call_args.args[0].to_map()
        self.assertEqual(source_options, actual_options)
        self.assertEqual(
            result.snapshot_id,
            table.raw_table.snapshot_manager().get_latest_snapshot().id,
        )

    def test_non_seekable_remote_stream_aborts_and_closes_resources(self):
        source = "s3://source-bucket/episode.h5"
        source_file_io = _RemoteSourceFileIO(
            objects={
                source: self._write_source("remote.h5").read_bytes(),
            },
            seekable=False,
        )
        table = self._create_table("non_seekable")
        proxy, writer, committer = self._instrument_write(table)

        with patch(
                "pypaimon.multimodal.source_utils.ResolvingFileIO",
                return_value=source_file_io), patch.object(
                    table.raw_table,
                    "new_batch_write_builder",
                    return_value=proxy):
            with self.assertRaisesRegex(ValueError, "seekable"):
                self._load(table, source, transform=self._transform)

        self.assertEqual(1, writer.abort_count)
        self.assertEqual(1, writer.close_count)
        self.assertEqual(1, committer.close_count)
        self.assertEqual(1, source_file_io.closed_stream_count)
        self.assertEqual(1, source_file_io.close_count)
        self.assertIsNone(
            table.raw_table.snapshot_manager().get_latest_snapshot())

    def test_legacy_oss_directory_error_recommends_explicit_files(self):
        source = "oss://source-bucket/episodes"
        source_file_io = _RemoteSourceFileIO()
        source_file_io.list_error = LegacyOssDirectoryListingError(
            "Listing OSS directories is not supported with PyArrow < 16")
        table = self._create_table("legacy_oss")
        new_builder = Mock()

        with patch(
                "pypaimon.multimodal.source_utils.ResolvingFileIO",
                return_value=source_file_io), patch.object(
                    table.raw_table,
                    "new_batch_write_builder",
                    new_builder):
            with self.assertRaisesRegex(
                    ValueError, "pass explicit HDF5 file paths"):
                self._load(table, source, transform=self._transform)

        new_builder.assert_not_called()
        self.assertEqual(1, source_file_io.close_count)

    def test_load_from_hdf5_rejects_non_arrow_and_empty_output(self):
        source = self._write_source("episode.h5")
        cases = (
            (lambda h5, info: {"frame_index": [0]}, "Arrow data"),
            (lambda h5, info: iter(()), "produced no rows"),
        )
        for index, (transform, message) in enumerate(cases):
            table = self._create_table("invalid_output_%d" % index)
            with self.subTest(message=message):
                with self.assertRaisesRegex(ValueError, message):
                    self._load(table, source, transform=transform)
                self.assertIsNone(
                    table.raw_table.snapshot_manager().get_latest_snapshot())

    def test_each_discovered_file_must_produce_rows(self):
        valid = self._write_source("valid.h5")
        empty = self._write_source("empty.h5")
        table = self._create_table("per_source_rows")
        proxy, writer, committer = self._instrument_write(table)

        def transform(h5, source):
            if source.name == "empty.h5":
                return iter(())
            return self._transform(h5, source)

        with patch.object(
                table.raw_table,
                "new_batch_write_builder",
                return_value=proxy):
            with self.assertRaisesRegex(
                    ValueError, "empty.h5.*produced no rows"):
                self._load(table, [valid, empty], transform=transform)

        self.assertEqual(1, writer.abort_count)
        self.assertEqual(0, committer.commit_count)
        self.assertIsNone(
            table.raw_table.snapshot_manager().get_latest_snapshot())

    def test_strict_schema_rejects_nested_non_nullable_values(self):
        source = pmm.Hdf5File("file:///tmp/episode.h5")
        cases = (
            (
                pa.field(
                    "items",
                    pa.list_(pa.field("item", pa.int32(), nullable=False)),
                ),
                [[1, None]],
                "items.item",
            ),
            (
                pa.field(
                    "vector",
                    pa.list_(
                        pa.field("item", pa.float32(), nullable=False), 2),
                ),
                [[1.0, None]],
                "vector.item",
            ),
            (
                pa.field(
                    "attributes",
                    pa.map_(
                        pa.string(),
                        pa.field("value", pa.int32(), nullable=False),
                    ),
                ),
                [[("key", None)]],
                "attributes.value",
            ),
        )

        for field, values, path in cases:
            schema = pa.schema([field])
            batch = pa.Table.from_arrays(
                [pa.array(values, type=field.type)], schema=schema)
            with self.subTest(path=path):
                with self.assertRaisesRegex(ValueError, path):
                    _strict_arrow_table(batch, schema, source, 0)

    def test_orc_load_rejects_null_array_elements_and_map_values(self):
        source = self._write_source("nested_nulls.h5")
        cases = (
            (
                "orc_array_null",
                pa.field(
                    "nested",
                    pa.list_(pa.field("item", pa.int32(), nullable=False)),
                ),
                [[1, None]],
            ),
            (
                "orc_map_null",
                pa.field(
                    "nested",
                    pa.map_(
                        pa.string(),
                        pa.field("value", pa.int32(), nullable=False),
                    ),
                ),
                [[("key", None)]],
            ),
        )
        for name, nested_field, values in cases:
            schema = pa.schema([
                pa.field("id", pa.int32(), nullable=False),
                nested_field,
            ])
            table = self.conn.create_table(
                name,
                schema=schema,
                options={
                    "file.format": "orc",
                    "blob-as-descriptor": "false",
                },
            )

            def transform(h5, context, field=nested_field, data=values):
                return pa.Table.from_arrays(
                    [pa.array([1], type=pa.int32()),
                     pa.array(data, type=field.type)],
                    names=["id", "nested"],
                )

            with self.subTest(name=name):
                with self.assertRaisesRegex(ValueError, "nested"):
                    self._load(table, source, transform=transform)
                self.assertIsNone(
                    table.raw_table.snapshot_manager().get_latest_snapshot())

    def test_nested_nullability_checks_only_logical_sliced_map_values(self):
        item_field = pa.field("value", pa.int32(), nullable=False)
        map_type = pa.map_(pa.string(), item_field)
        schema = pa.schema([pa.field("attributes", map_type)])
        values = pa.array(
            [[("outside", None)], [("inside", 1)]], type=map_type)
        sliced = pa.Table.from_arrays(
            [values.slice(1, 1)], schema=schema)

        result = _strict_arrow_table(
            sliced, schema, pmm.Hdf5File("file:///tmp/episode.h5"), 0)

        self.assertEqual(
            [[("inside", 1)]], result.column("attributes").to_pylist())

    def test_nested_nullability_ignores_entries_hidden_by_null_maps(self):
        item_field = pa.field("value", pa.int32(), nullable=False)
        map_type = pa.map_(pa.string(), item_field)
        schema = pa.schema([pa.field("attributes", map_type)])
        offsets = pa.array([0, 1, 2], type=pa.int32())
        entries = pa.StructArray.from_arrays(
            [
                pa.array(["hidden", "visible"]),
                pa.array([None, 1], type=pa.int32()),
            ],
            fields=[map_type.key_field, map_type.item_field],
        )
        validity = pa.array([False, True], type=pa.bool_()).buffers()[1]
        values = pa.MapArray.from_buffers(
            map_type,
            2,
            [validity, offsets.buffers()[1]],
            children=[entries],
        )
        table = pa.Table.from_arrays([values], schema=schema)

        result = _strict_arrow_table(
            table, schema, pmm.Hdf5File("file:///tmp/episode.h5"), 0)

        self.assertEqual(
            [None, [("visible", 1)]],
            result.column("attributes").to_pylist(),
        )

    def test_load_from_hdf5_rejects_invalid_schemas(self):
        source = self._write_source("episode.hdf5")
        good = {
            "source": ["episode.hdf5"],
            "frame_index": [0],
            "score": [None],
            "value": [[1.0, 2.0, 3.0]],
            "image": [b"jpeg"],
        }
        cases = []
        missing = dict(good)
        missing.pop("score")
        cases.append((missing, "missing columns"))
        extra = dict(good, unexpected=[1])
        cases.append((extra, "unexpected columns"))
        incompatible = dict(good, frame_index=["not-an-int"])
        cases.append((incompatible, "cannot be converted"))
        invalid_vector = dict(good, value=[[1.0, 2.0]])
        cases.append((invalid_vector, "cannot be converted"))
        null_non_nullable = dict(good, source=[None])
        cases.append((null_non_nullable, "cannot be converted"))

        for index, (data, message) in enumerate(cases):
            table = self._create_table("strict_%d" % index)

            def transform(h5, info, batch=data):
                yield pa.Table.from_pydict(batch)

            with self.subTest(message=message):
                with self.assertRaisesRegex(ValueError, message):
                    self._load(table, source, transform=transform)
                self.assertIsNone(
                    table.raw_table.snapshot_manager().get_latest_snapshot())

    def test_hdfs_source_rejects_explicit_keytab_for_any_target(self):
        table = self._create_table("kerberos_isolation")

        with patch.object(
                self.conn, "get_table", return_value=table), patch(
                    "pypaimon.multimodal.source_utils.ResolvingFileIO") as resolving:
            with self.assertRaisesRegex(
                    ValueError, "process-isolated"):
                self.conn.load_from_hdf5(
                    table.identifier,
                    "hdfs://source-ns/episode.h5",
                    transform=self._transform,
                    source_options={
                        "security.kerberos.login.principal": "source@REALM",
                        "security.kerberos.login.keytab": "/source.keytab",
                    },
                )

        resolving.assert_not_called()

    def test_precommit_failures_abort_and_close_all_open_resources(self):
        valid_source = self._write_source("valid.h5")
        invalid_source = self.source_dir / "invalid.h5"
        invalid_source.write_bytes(b"not hdf5")

        def transform_failure(h5, source):
            yield next(self._transform(h5, source))
            raise RuntimeError("transform failed")

        def schema_failure(h5, source, resource_state):
            try:
                yield pa.Table.from_pydict({"missing": [1]})
                raise AssertionError("generator should have been closed")
            finally:
                resource_state["generator_closed"] = True

        cases = (
            (invalid_source, self._transform, None, OSError),
            (valid_source, transform_failure, None, RuntimeError),
            (valid_source, schema_failure, None, ValueError),
            (valid_source, self._transform, RuntimeError("write failed"), RuntimeError),
        )
        for index, (source, transform, write_error, error_type) in enumerate(cases):
            table = self._create_table("failure_%d" % index)
            proxy, writer, committer = self._instrument_write(
                table, write_error=write_error)
            resource_state = {}
            expects_generator_close = transform is schema_failure
            if expects_generator_close:
                transform = partial(
                    schema_failure, resource_state=resource_state)

            def tracked_transform(h5, info, delegate=transform):
                resource_state["h5"] = h5
                return delegate(h5, info)

            with self.subTest(error=error_type.__name__):
                with patch.object(
                        table.raw_table,
                        "new_batch_write_builder",
                        return_value=proxy):
                    with self.assertRaises(error_type):
                        self._load(
                            table, source, transform=tracked_transform)
                self.assertEqual(1, writer.abort_count)
                self.assertEqual(1, writer.close_count)
                self.assertEqual(1, committer.close_count)
                self.assertEqual(0, committer.commit_count)
                self.assertIsNone(
                    table.raw_table.snapshot_manager().get_latest_snapshot())
                if "h5" in resource_state:
                    self.assertFalse(resource_state["h5"].id.valid)
                if expects_generator_close:
                    self.assertTrue(resource_state.get("generator_closed"))

    def test_empty_discovery_is_a_noop_without_creating_a_writer(self):
        table = self._create_table()
        new_builder = Mock()

        with patch.object(
                table.raw_table,
                "new_batch_write_builder",
                new_builder), patch.dict(sys.modules, {"h5py": None}):
            for paths in ([], self.source_dir):
                with self.subTest(paths=paths):
                    result = self._load(
                        table, paths, transform=self._transform)
                    self.assertEqual(
                        (0, 0, 0, None),
                        (
                            result.file_count,
                            result.batch_count,
                            result.row_count,
                            result.snapshot_id,
                        ),
                    )

        new_builder.assert_not_called()
        self.assertIsNone(
            table.raw_table.snapshot_manager().get_latest_snapshot())

    def test_discovery_failure_happens_before_writer_creation(self):
        table = self._create_table()
        new_builder = Mock()
        missing = self.source_dir / "missing.h5"
        with patch.object(
                table.raw_table,
                "new_batch_write_builder",
                new_builder):
            with self.assertRaisesRegex(
                    ValueError,
                    "HDF5 path does not exist: %s" % missing):
                self._load(table, missing, transform=self._transform)
        new_builder.assert_not_called()

    def test_existing_file_with_unsupported_suffix_reports_suffix_only(self):
        source = self.source_dir / "episode.txt"
        source.write_text("not hdf5", encoding="utf-8")
        table = self._create_table("unsupported_suffix")

        with self.assertRaisesRegex(
                ValueError,
                "HDF5 file has unsupported suffix: %s" % source):
            self._load(table, source, transform=self._transform)

    def test_commit_exception_is_not_retried_or_aborted(self):
        source = self._write_source("episode.h5")
        table = self._create_table()
        commit_error = RuntimeError("unknown commit result")
        proxy, writer, committer = self._instrument_write(
            table,
            commit_error=commit_error,
            raise_after_commit=True,
        )

        with patch.object(
                table.raw_table,
                "new_batch_write_builder",
                return_value=proxy):
            with self.assertRaisesRegex(RuntimeError, "unknown commit result"):
                self._load(table, source, transform=self._transform)

        self.assertEqual(1, committer.commit_count)
        self.assertEqual(0, writer.abort_count)
        self.assertEqual(1, writer.close_count)
        self.assertEqual(1, committer.close_count)
        self.assertEqual(4, table.scan().to_arrow().num_rows)
        self.assertEqual(
            1, table.raw_table.snapshot_manager().get_latest_snapshot().id)

    def test_hdf5_api_is_connection_only_and_has_no_managed_provenance(self):
        table = self._create_table()
        self.assertTrue(hasattr(self.conn, "load_from_hdf5"))
        self.assertFalse(hasattr(table, "append_hdf5"))
        self.assertFalse(hasattr(table, "from_hdf5"))
        self.assertFalse(hasattr(pmm, "HDF5_SOURCE_PATH_COLUMN"))
        self.assertFalse(hasattr(pmm, "HDF5_SOURCE_SHA256_COLUMN"))
        self.assertFalse(hasattr(pmm, "Hdf5SourceDriftError"))


if __name__ == "__main__":
    unittest.main()
