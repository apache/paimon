# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import gc
import os
import tempfile
import time
import unittest
import weakref
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
from fsspec.implementations.local import LocalFileSystem as FsspecLocalFileSystem

from pypaimon.common.options import Options
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.read.reader import format_pyarrow_reader as reader_module
from pypaimon.read.reader.format_pyarrow_reader import FormatPyArrowReader
from pypaimon.schema.data_types import AtomicType, DataField


DEFAULT_CACHE_SIZE = 50 * 1024 * 1024


class _CountingInputFile:
    def __init__(self, wrapped, file_system):
        self._wrapped = wrapped
        self._file_system = file_system

    def read(self, size=-1):
        offset = self._wrapped.tell()
        data = self._wrapped.read(size)
        self._file_system.reads.append((offset, len(data)))
        return data

    def readinto(self, buffer):
        offset = self._wrapped.tell()
        size = self._wrapped.readinto(buffer)
        self._file_system.reads.append((offset, size))
        return size

    def __getattr__(self, name):
        return getattr(self._wrapped, name)


class _CountingLocalFileSystem(FsspecLocalFileSystem):
    def __init__(self):
        super().__init__()
        self.opens = 0
        self.reads = []

    def _open(self, path, mode="rb", **kwargs):
        wrapped = super()._open(path, mode=mode, **kwargs)
        if "r" not in mode:
            return wrapped
        self.opens += 1
        return _CountingInputFile(wrapped, self)

    def reset_counts(self):
        self.opens = 0
        self.reads = []


class FileFormatMetadataCacheTest(unittest.TestCase):
    def setUp(self):
        reader_module._reset_file_format_dataset_cache()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.file_io = LocalFileIO(self.temp_dir.name, Options({}))
        self.paths = []
        for index in range(3):
            path = os.path.join(self.temp_dir.name, "data-{}.parquet".format(index))
            pq.write_table(
                pa.table({"value": list(range(index * 10, index * 10 + 10))}),
                path,
                row_group_size=2,
            )
            self.paths.append(path)

    def tearDown(self):
        reader_module._reset_file_format_dataset_cache()
        self.temp_dir.cleanup()

    @staticmethod
    def _options(max_size="50 mb"):
        return CoreOptions(Options({
            "file-format.metadata-cache.max-size": max_size,
        }))

    def _read(self, path, options, file_io=None):
        reader = FormatPyArrowReader(
            file_io or self.file_io,
            "parquet",
            path,
            [DataField(0, "value", AtomicType("BIGINT"))],
            None,
            options=options,
        )
        values = []
        try:
            while True:
                batch = reader.read_arrow_batch()
                if batch is None:
                    return values
                values.extend(batch.column(0).to_pylist())
        finally:
            reader.close()

    def test_enabled_by_default(self):
        options = CoreOptions(Options({}))
        self.assertEqual(
            DEFAULT_CACHE_SIZE,
            options.file_format_metadata_cache_max_size().get_bytes())

        original = reader_module.ds.dataset
        with patch.object(reader_module.ds, "dataset", wraps=original) as dataset:
            self._read(self.paths[0], options)
            self._read(self.paths[0], options)
        self.assertEqual(1, dataset.call_count)

    def test_zero_size_bypasses_and_removes_entry(self):
        enabled = self._options()
        disabled = self._options("0 b")
        original = reader_module.ds.dataset
        with patch.object(reader_module.ds, "dataset", wraps=original) as dataset:
            self._read(self.paths[0], enabled)
            self._read(self.paths[0], disabled)
            self._read(self.paths[0], enabled)
        self.assertEqual(3, dataset.call_count)

    def test_zero_size_keeps_other_entries(self):
        enabled = self._options()
        disabled = self._options("0 b")
        original = reader_module.ds.dataset
        with patch.object(reader_module.ds, "dataset", wraps=original) as dataset:
            self._read(self.paths[0], enabled)
            self._read(self.paths[1], enabled)
            self._read(self.paths[0], disabled)
            self._read(self.paths[1], enabled)
        self.assertEqual(3, dataset.call_count)

    def test_reuses_dataset(self):
        options = self._options()
        original = reader_module.ds.dataset
        with patch.object(reader_module.ds, "dataset", wraps=original) as dataset:
            first = self._read(self.paths[0], options)
            second = self._read(self.paths[0], options)

        self.assertEqual(list(range(10)), first)
        self.assertEqual(first, second)
        self.assertEqual(1, dataset.call_count)

    def test_repeated_scan_skips_footer_io(self):
        path = os.path.join(self.temp_dir.name, "footer-io.parquet")
        pq.write_table(
            pa.table({
                "value": list(range(10000)),
                "payload": ["x" * 100] * 10000,
            }),
            path,
            row_group_size=100,
            compression="none",
        )
        counting = _CountingLocalFileSystem()
        file_io = LocalFileIO(self.temp_dir.name, Options({}))
        file_io.filesystem = pafs.PyFileSystem(pafs.FSSpecHandler(counting))

        uncached = self._read(path, self._options("0 b"), file_io)
        uncached_opens = counting.opens
        uncached_reads = len(counting.reads)

        counting.reset_counts()
        reader_module._reset_file_format_dataset_cache()
        self._read(path, self._options(), file_io)
        counting.reset_counts()
        cached = self._read(path, self._options(), file_io)

        self.assertEqual(uncached, cached)
        self.assertLess(counting.opens, uncached_opens)
        self.assertLess(len(counting.reads), uncached_reads)

    def test_evicts_least_recently_used_entry_by_estimated_size(self):
        cache = reader_module._FileFormatDatasetCache(10)
        first_key = (None, "parquet", "first")
        second_key = (None, "parquet", "second")
        third_key = (None, "parquet", "third")

        cache.get_or_load(first_key, lambda: "first", lambda _: 4)
        cache.get_or_load(second_key, lambda: "second", lambda _: 4)
        cache.get_or_load(first_key, lambda: "unused", lambda _: 4)
        cache.get_or_load(third_key, lambda: "third", lambda _: 4)

        self.assertEqual([first_key, third_key], list(cache._entries.keys()))
        self.assertEqual(8, cache.estimated_size)

    def test_does_not_retain_entry_larger_than_size_limit(self):
        cache = reader_module._FileFormatDatasetCache(5)
        loads = []
        key = (None, "parquet", "large")

        def load():
            loads.append(True)
            return "large"

        self.assertEqual(
            "large", cache.get_or_load(key, load, lambda _: 6))
        self.assertEqual(
            "large", cache.get_or_load(key, load, lambda _: 6))
        self.assertEqual(2, len(loads))
        self.assertEqual(0, len(cache._entries))
        self.assertEqual(0, cache.estimated_size)

    def test_does_not_retain_entry_without_size_estimate(self):
        cache = reader_module._FileFormatDatasetCache(10)
        key = (None, "unknown", "data")
        loads = []

        def load():
            loads.append(True)
            return "unknown"

        self.assertEqual(
            "unknown", cache.get_or_load(key, load, lambda _: None))
        self.assertEqual(
            "unknown", cache.get_or_load(key, load, lambda _: None))
        self.assertEqual(2, len(loads))
        self.assertEqual(0, len(cache._entries))

    def test_estimates_serialized_parquet_footer_size(self):
        dataset = reader_module.ds.dataset(self.paths[0], format="parquet")
        expected = sum(
            fragment.metadata.serialized_size
            for fragment in dataset.get_fragments()
        )
        self.assertGreater(expected, 0)
        self.assertEqual(
            expected,
            reader_module._estimate_file_format_dataset_size(
                dataset, "parquet"))

    def test_process_cache_uses_largest_requested_capacity(self):
        cache = reader_module._file_format_dataset_cache(3 * 1024 * 1024)
        same_cache = reader_module._file_format_dataset_cache(1024 * 1024)

        self.assertIs(cache, same_cache)
        self.assertEqual(3 * 1024 * 1024, cache.max_size)

    def test_shares_cache_across_file_io_with_same_filesystem(self):
        other_file_io = LocalFileIO(self.temp_dir.name, Options({}))
        other_file_io.filesystem = self.file_io.filesystem

        original = reader_module.ds.dataset
        with patch.object(reader_module.ds, "dataset", wraps=original) as dataset:
            reader_module._file_format_dataset(
                self.file_io, "parquet", self.paths[0], DEFAULT_CACHE_SIZE)
            reader_module._file_format_dataset(
                other_file_io, "parquet", self.paths[0], DEFAULT_CACHE_SIZE)

        self.assertEqual(1, dataset.call_count)

    def test_does_not_share_across_filesystems(self):
        other_file_io = LocalFileIO(self.temp_dir.name, Options({}))
        original = reader_module.ds.dataset
        with patch.object(reader_module.ds, "dataset", wraps=original) as dataset:
            reader_module._file_format_dataset(
                self.file_io, "parquet", self.paths[0], DEFAULT_CACHE_SIZE)
            reader_module._file_format_dataset(
                other_file_io, "parquet", self.paths[0], DEFAULT_CACHE_SIZE)
        self.assertEqual(2, dataset.call_count)

    def test_does_not_share_across_file_formats(self):
        parquet_dataset = object()
        orc_dataset = object()
        with patch.object(
                reader_module.ds, "dataset",
                side_effect=[parquet_dataset, orc_dataset]) as dataset:
            with patch.object(
                    reader_module, "_estimate_file_format_dataset_size",
                    return_value=1):
                first = reader_module._file_format_dataset(
                    self.file_io, "parquet", self.paths[0], DEFAULT_CACHE_SIZE)
                second = reader_module._file_format_dataset(
                    self.file_io, "orc", self.paths[0], DEFAULT_CACHE_SIZE)

        self.assertIs(parquet_dataset, first)
        self.assertIs(orc_dataset, second)
        self.assertEqual(2, dataset.call_count)

    def test_cache_key_retains_filesystem_wrapper(self):
        root = pafs.LocalFileSystem()
        filesystem = pafs.SubTreeFileSystem(self.temp_dir.name, root)
        filesystem_ref = weakref.ref(filesystem)
        file_io = LocalFileIO(self.temp_dir.name, Options({}))
        file_io.filesystem = filesystem

        reader_module._file_format_dataset(
            file_io, "parquet", os.path.basename(self.paths[0]),
            DEFAULT_CACHE_SIZE)
        file_io.filesystem = root
        del filesystem
        gc.collect()

        self.assertIsNotNone(filesystem_ref())

    def test_filesystem_hash_collision_does_not_share_dataset(self):
        first_dir = tempfile.TemporaryDirectory()
        second_dir = tempfile.TemporaryDirectory()
        try:
            file_name = "same.parquet"
            pq.write_table(
                pa.table({"value": [1]}), os.path.join(first_dir.name, file_name))
            pq.write_table(
                pa.table({"value": [2]}), os.path.join(second_dir.name, file_name))
            file_io = LocalFileIO(first_dir.name, Options({}))
            first_filesystem = pafs.SubTreeFileSystem(
                first_dir.name, pafs.LocalFileSystem())
            second_filesystem = pafs.SubTreeFileSystem(
                second_dir.name, pafs.LocalFileSystem())

            with patch.object(
                    reader_module._FilesystemIdentity, "__hash__", return_value=1):
                file_io.filesystem = first_filesystem
                first = reader_module._file_format_dataset(
                    file_io, "parquet", file_name,
                    DEFAULT_CACHE_SIZE).to_table()
                file_io.filesystem = second_filesystem
                second = reader_module._file_format_dataset(
                    file_io, "parquet", file_name,
                    DEFAULT_CACHE_SIZE).to_table()

            self.assertEqual([1], first.column("value").to_pylist())
            self.assertEqual([2], second.column("value").to_pylist())
        finally:
            first_dir.cleanup()
            second_dir.cleanup()

    def test_resets_after_process_change(self):
        parent_cache = reader_module._file_format_dataset_cache(DEFAULT_CACHE_SIZE)
        with patch.object(reader_module.os, "getpid", return_value=os.getpid() + 1):
            child_cache = reader_module._file_format_dataset_cache(DEFAULT_CACHE_SIZE)
        self.assertIsNot(parent_cache, child_cache)

    def test_coalesces_concurrent_loads(self):
        original = reader_module.ds.dataset

        def delayed_dataset(*args, **kwargs):
            time.sleep(0.05)
            return original(*args, **kwargs)

        with patch.object(
                reader_module.ds, "dataset", side_effect=delayed_dataset) as dataset:
            with ThreadPoolExecutor(max_workers=8) as executor:
                results = list(executor.map(
                    lambda _: self._read(self.paths[0], self._options()),
                    range(8),
                ))

        self.assertEqual(1, dataset.call_count)
        self.assertTrue(all(value == list(range(10)) for value in results))


if __name__ == "__main__":
    unittest.main()
