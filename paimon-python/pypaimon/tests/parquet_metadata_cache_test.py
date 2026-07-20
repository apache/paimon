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


class ParquetMetadataCacheTest(unittest.TestCase):
    def setUp(self):
        reader_module._reset_parquet_dataset_cache()
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
        reader_module._reset_parquet_dataset_cache()
        self.temp_dir.cleanup()

    @staticmethod
    def _options(enabled, max_entries=256):
        return CoreOptions(Options({
            "parquet.metadata-cache-enabled": enabled,
            "parquet.metadata-cache-max-entries": max_entries,
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

    def test_disabled_by_default(self):
        options = CoreOptions(Options({}))
        self.assertFalse(options.parquet_metadata_cache_enabled())
        self.assertEqual(256, options.parquet_metadata_cache_max_entries())

        original = reader_module.ds.dataset
        with patch.object(reader_module.ds, "dataset", wraps=original) as dataset:
            self._read(self.paths[0], options)
            self._read(self.paths[0], options)
        self.assertEqual(2, dataset.call_count)

    def test_reuses_dataset(self):
        options = self._options(True)
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

        uncached = self._read(path, self._options(False), file_io)
        uncached_opens = counting.opens
        uncached_reads = len(counting.reads)

        counting.reset_counts()
        reader_module._reset_parquet_dataset_cache()
        self._read(path, self._options(True), file_io)
        counting.reset_counts()
        cached = self._read(path, self._options(True), file_io)

        self.assertEqual(uncached, cached)
        self.assertLess(counting.opens, uncached_opens)
        self.assertLess(len(counting.reads), uncached_reads)

    def test_evicts_least_recently_used_entry(self):
        options = self._options(True, max_entries=2)
        original = reader_module.ds.dataset
        with patch.object(reader_module.ds, "dataset", wraps=original) as dataset:
            for path in self.paths:
                self._read(path, options)
            self._read(self.paths[0], options)
        self.assertEqual(4, dataset.call_count)

    def test_cache_capacity_does_not_shrink(self):
        original = reader_module.ds.dataset
        with patch.object(reader_module.ds, "dataset", wraps=original) as dataset:
            for path in self.paths:
                self._read(path, self._options(True, max_entries=3))
            self._read(self.paths[2], self._options(True, max_entries=1))
            self._read(self.paths[0], self._options(True, max_entries=1))
        self.assertEqual(3, dataset.call_count)
        self.assertEqual(
            3, reader_module._parquet_dataset_cache(self.file_io, 1).max_entries)

    def test_cache_capacity_is_isolated_by_file_io(self):
        other_file_io = LocalFileIO(self.temp_dir.name, Options({}))

        first_cache = reader_module._parquet_dataset_cache(self.file_io, 3)
        second_cache = reader_module._parquet_dataset_cache(other_file_io, 1)

        self.assertIsNot(first_cache, second_cache)
        self.assertEqual(3, first_cache.max_entries)
        self.assertEqual(1, second_cache.max_entries)

    def test_does_not_share_across_filesystems(self):
        other_file_io = LocalFileIO(self.temp_dir.name, Options({}))
        original = reader_module.ds.dataset
        with patch.object(reader_module.ds, "dataset", wraps=original) as dataset:
            reader_module._parquet_dataset(
                self.file_io, self.paths[0], True, 256)
            reader_module._parquet_dataset(
                other_file_io, self.paths[0], True, 256)
        self.assertEqual(2, dataset.call_count)

    def test_cache_key_retains_filesystem_wrapper(self):
        root = pafs.LocalFileSystem()
        filesystem = pafs.SubTreeFileSystem(self.temp_dir.name, root)
        filesystem_ref = weakref.ref(filesystem)
        file_io = LocalFileIO(self.temp_dir.name, Options({}))
        file_io.filesystem = filesystem

        reader_module._parquet_dataset(
            file_io, os.path.basename(self.paths[0]), True, 256)
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
                first = reader_module._parquet_dataset(
                    file_io, file_name, True, 256).to_table()
                file_io.filesystem = second_filesystem
                second = reader_module._parquet_dataset(
                    file_io, file_name, True, 256).to_table()

            self.assertEqual([1], first.column("value").to_pylist())
            self.assertEqual([2], second.column("value").to_pylist())
        finally:
            first_dir.cleanup()
            second_dir.cleanup()

    def test_resets_after_process_change(self):
        parent_cache = reader_module._parquet_dataset_cache(self.file_io, 256)
        with patch.object(reader_module.os, "getpid", return_value=os.getpid() + 1):
            child_cache = reader_module._parquet_dataset_cache(self.file_io, 256)
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
                    lambda _: self._read(self.paths[0], self._options(True)),
                    range(8),
                ))

        self.assertEqual(1, dataset.call_count)
        self.assertTrue(all(value == list(range(10)) for value in results))


if __name__ == "__main__":
    unittest.main()
