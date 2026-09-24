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
import threading
import time
import unittest
import weakref
from concurrent.futures import Future, ThreadPoolExecutor
from unittest.mock import patch

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
from fsspec.implementations.local import LocalFileSystem as FsspecLocalFileSystem

from pypaimon.common.options import Options
from pypaimon.common.options.config import CatalogOptions
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


class _CountingFileSystemHandler(pafs.FSSpecHandler):
    """Count Arrow calls, not backend HTTP requests."""

    def __init__(self):
        super().__init__(FsspecLocalFileSystem(skip_instance_cache=True))
        self.calls = []

    def get_file_info(self, paths):
        self.calls.append("get_file_info")
        return super().get_file_info(paths)

    def open_input_file(self, path):
        self.calls.append("open_input_file")
        return super().open_input_file(path)

    def register_file_size(self, path, file_size):
        self.calls.append(("register_file_size", path, file_size))


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

    def _file_io(self, max_size="50 mb"):
        return LocalFileIO(self.temp_dir.name, Options({
            "file-format.metadata-cache.max-size": max_size,
        }))

    def _read(self, path, file_io=None, options=None):
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
        self.assertEqual(
            DEFAULT_CACHE_SIZE,
            self.file_io.properties.get(
                CatalogOptions.FILE_FORMAT_METADATA_CACHE_MAX_SIZE).get_bytes())

        original = reader_module.ds.FileSystemDataset
        with patch.object(reader_module.ds, "FileSystemDataset", wraps=original) as dataset:
            self._read(self.paths[0])
            self._read(self.paths[0])
        self.assertEqual(1, dataset.call_count)

    def test_zero_size_bypasses_and_removes_entry(self):
        enabled = self._file_io()
        disabled = self._file_io("0 b")
        original = reader_module.ds.FileSystemDataset
        with patch.object(reader_module.ds, "FileSystemDataset", wraps=original) as dataset:
            self._read(self.paths[0], enabled)
            self._read(self.paths[0], disabled)
            self._read(self.paths[0], enabled)
        self.assertEqual(3, dataset.call_count)

    def test_zero_size_clears_other_entries(self):
        enabled = self._file_io()
        disabled = self._file_io("0 b")
        original = reader_module.ds.FileSystemDataset
        with patch.object(reader_module.ds, "FileSystemDataset", wraps=original) as dataset:
            self._read(self.paths[0], enabled)
            self._read(self.paths[1], enabled)
            self._read(self.paths[0], disabled)
            self._read(self.paths[1], enabled)
        self.assertEqual(4, dataset.call_count)

    def test_reuses_dataset(self):
        file_io = self._file_io()
        original = reader_module.ds.FileSystemDataset
        with patch.object(reader_module.ds, "FileSystemDataset", wraps=original) as dataset:
            first = self._read(self.paths[0], file_io)
            second = self._read(self.paths[0], file_io)

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
        file_io = self._file_io("0 b")
        file_io.filesystem = pafs.PyFileSystem(pafs.FSSpecHandler(counting))

        uncached = self._read(path, file_io)
        uncached_opens = counting.opens
        uncached_reads = len(counting.reads)

        counting.reset_counts()
        reader_module._reset_file_format_dataset_cache()
        file_io.properties.set(
            CatalogOptions.FILE_FORMAT_METADATA_CACHE_MAX_SIZE, "50 mb")
        self._read(path, file_io)
        counting.reset_counts()
        cached = self._read(path, file_io)

        self.assertEqual(uncached, cached)
        self.assertLess(counting.opens, uncached_opens)
        self.assertLess(len(counting.reads), uncached_reads)

    def test_single_file_request_counts(self):
        for max_size, expected_opens in [
                (DEFAULT_CACHE_SIZE, [2, 1]), (0, [2, 2]), (1, [2, 2])]:
            with self.subTest(cache_max_size=max_size):
                reader_module._reset_file_format_dataset_cache()
                handler = _CountingFileSystemHandler()
                self.file_io.filesystem = pafs.PyFileSystem(handler)
                results = []
                for opens in expected_opens:
                    handler.calls.clear()
                    dataset = reader_module._file_format_dataset(
                        self.file_io, "parquet", self.paths[0], max_size)
                    results.append(dataset.to_table().to_pydict())
                    self.assertEqual(0, handler.calls.count("get_file_info"))
                    self.assertEqual(opens, handler.calls.count("open_input_file"))
                self.assertEqual({"value": list(range(10))}, results[0])
                self.assertEqual(results[0], results[1])

    def test_forwards_known_file_size(self):
        parquet_format = unittest.mock.Mock()
        fragment = unittest.mock.Mock(physical_schema=pa.schema([]))
        parquet_format.make_fragment.return_value = fragment
        with patch.object(
                reader_module.ds, "ParquetFileFormat",
                return_value=parquet_format), patch.object(
                    reader_module.ds, "FileSystemDataset",
                    return_value=unittest.mock.sentinel.dataset):
            dataset = reader_module._file_format_dataset(
                self.file_io, "parquet", self.paths[0], 0, 123)
        self.assertIs(unittest.mock.sentinel.dataset, dataset)
        expected_options = (
            {} if reader_module._pyarrow_lt_7()
            else {"file_size": 123})
        parquet_format.make_fragment.assert_called_once_with(
            self.paths[0], filesystem=self.file_io.filesystem,
            **expected_options)

        handler = _CountingFileSystemHandler()
        self.file_io.filesystem = pafs.PyFileSystem(handler)
        file_size = os.path.getsize(self.paths[0])

        dataset = reader_module._file_format_dataset(
            self.file_io, "parquet", self.paths[0], 0, file_size)

        self.assertEqual({"value": list(range(10))},
                         dataset.to_table().to_pydict())
        self.assertIn(
            ("register_file_size", self.paths[0], file_size), handler.calls)

    def test_old_pyarrow_retries_fragment_without_file_size(self):
        parquet_format = unittest.mock.Mock()
        fragment = unittest.mock.Mock(physical_schema=pa.schema([]))
        parquet_format.make_fragment.side_effect = [
            TypeError("make_fragment() got an unexpected keyword argument 'file_size'"),
            fragment,
        ]
        with patch.object(reader_module, "_pyarrow_lt_7", return_value=False), \
                patch.object(reader_module.ds, "ParquetFileFormat", return_value=parquet_format), \
                patch.object(reader_module.ds, "FileSystemDataset",
                             return_value=unittest.mock.sentinel.dataset):
            dataset = reader_module._file_format_dataset(
                self.file_io, "parquet", self.paths[0], 0, 123)
        self.assertIs(unittest.mock.sentinel.dataset, dataset)
        self.assertEqual([
            unittest.mock.call(self.paths[0], filesystem=self.file_io.filesystem, file_size=123),
            unittest.mock.call(self.paths[0], filesystem=self.file_io.filesystem),
        ], parquet_format.make_fragment.call_args_list)

    def test_fragment_metadata_is_reused_without_io(self):
        handler = _CountingFileSystemHandler()
        self.file_io.filesystem = pafs.PyFileSystem(handler)
        dataset = reader_module._file_format_dataset(
            self.file_io, "parquet", self.paths[0], 0)
        self.assertEqual(["open_input_file"], handler.calls)
        handler.calls.clear()
        for _ in range(2):
            fragment = next(dataset.get_fragments())
            self.assertEqual(dataset.schema, fragment.physical_schema)
            self.assertEqual(10, fragment.metadata.num_rows)
            self.assertEqual(5, len(fragment.split_by_row_group()))
            self.assertGreater(
                reader_module._estimate_file_format_dataset_size(
                    dataset, "parquet"), 0)
        self.assertEqual([], handler.calls)
        self.assertEqual(list(range(10)), dataset.to_table()[0].to_pylist())
        self.assertEqual(["open_input_file"], handler.calls)

    def test_concurrent_scans_load_footer_once(self):
        handler = _CountingFileSystemHandler()
        self.file_io.filesystem = pafs.PyFileSystem(handler)
        barrier = threading.Barrier(8)

        def read(_):
            barrier.wait(timeout=10)
            return self._read(self.paths[0])

        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(read, range(8)))
        self.assertTrue(all(result == list(range(10)) for result in results))
        self.assertEqual(0, handler.calls.count("get_file_info"))
        self.assertEqual(9, handler.calls.count("open_input_file"))

    def test_missing_and_corrupt_files_fail_and_can_retry(self):
        for max_size in [0, DEFAULT_CACHE_SIZE]:
            for corrupt in [False, True]:
                with self.subTest(cache_max_size=max_size, corrupt=corrupt):
                    reader_module._reset_file_format_dataset_cache()
                    path = os.path.join(self.temp_dir.name, "invalid.parquet")
                    if corrupt:
                        with open(path, "wb") as output:
                            output.write(b"not a parquet file")
                    elif os.path.exists(path):
                        os.remove(path)
                    with self.assertRaises((OSError, pa.ArrowInvalid)):
                        reader_module._file_format_dataset(
                            self.file_io, "parquet", path, max_size).to_table()
                    pq.write_table(pa.table({"value": [42]}), path)
                    self.assertEqual(
                        [42], reader_module._file_format_dataset(
                            self.file_io, "parquet", path, max_size
                        ).to_table()[0].to_pylist())

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

    def test_evicts_least_recently_used_entry_by_entry_count(self):
        cache = reader_module._FileFormatDatasetCache(
            1024, max_entries=2)
        first_key = (None, "parquet", "first")
        second_key = (None, "parquet", "second")
        third_key = (None, "parquet", "third")

        cache.get_or_load(first_key, lambda: "first", lambda _: 1)
        cache.get_or_load(second_key, lambda: "second", lambda _: 1)
        cache.get_or_load(third_key, lambda: "third", lambda _: 1)

        self.assertEqual(
            [second_key, third_key], list(cache._entries.keys()))
        self.assertEqual(2, cache.estimated_size)

    def test_does_not_retain_entry_larger_than_size_limit(self):
        cache = reader_module._FileFormatDatasetCache(5)
        loads = []
        small_key = (None, "parquet", "small")
        key = (None, "parquet", "large")
        cache.get_or_load(small_key, lambda: "small", lambda _: 4)

        def load():
            loads.append(True)
            return "large"

        self.assertEqual(
            "large", cache.get_or_load(key, load, lambda _: 6))
        self.assertEqual(
            "large", cache.get_or_load(key, load, lambda _: 6))
        self.assertEqual(2, len(loads))
        self.assertEqual([small_key], list(cache._entries))
        self.assertEqual(4, cache.estimated_size)

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

    def test_coalesces_load_while_uncached_result_completes(self):
        cache = reader_module._FileFormatDatasetCache(10)
        key = (None, "unknown", "data")
        setting_result = threading.Event()
        waiter_started = threading.Event()
        release_result = threading.Event()
        loads = []
        future_count = []

        def new_future():
            future = Future()
            future_count.append(future)
            if len(future_count) == 1:
                original_set_result = future.set_result
                original_result = future.result

                def delayed_set_result(result):
                    setting_result.set()
                    release_result.wait()
                    original_set_result(result)

                def observed_result(*args, **kwargs):
                    waiter_started.set()
                    return original_result(*args, **kwargs)

                future.set_result = delayed_set_result
                future.result = observed_result
            return future

        def load():
            loads.append(True)
            return "unknown"

        with patch.object(reader_module, "Future", side_effect=new_future):
            with ThreadPoolExecutor(max_workers=2) as executor:
                first = executor.submit(
                    cache.get_or_load, key, load, lambda _: None)
                self.assertTrue(setting_result.wait(1))
                second = executor.submit(
                    cache.get_or_load, key, load, lambda _: None)
                waited = waiter_started.wait(1)
                release_result.set()

                self.assertTrue(waited)
                self.assertEqual("unknown", first.result())
                self.assertEqual("unknown", second.result())
        self.assertEqual(1, len(loads))

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

    def test_cache_entry_estimate_has_conservative_floor(self):
        dataset = reader_module.ds.dataset(
            self.paths[0], format="parquet")
        key = (
            reader_module._FilesystemIdentity(self.file_io.filesystem),
            "parquet",
            self.paths[0],
        )

        estimated = reader_module._estimate_file_format_cache_entry_size(
            key, dataset, "parquet")

        self.assertGreaterEqual(
            estimated,
            reader_module._FILE_FORMAT_METADATA_CACHE_MIN_ENTRY_SIZE)

    def test_process_cache_can_shrink_requested_capacity(self):
        cache = reader_module._file_format_dataset_cache(10)
        cache.get_or_load(("first", "parquet", "first"),
                          lambda: "first", lambda _: 4)
        cache.get_or_load(("second", "parquet", "second"),
                          lambda: "second", lambda _: 4)
        same_cache = reader_module._file_format_dataset_cache(5)

        self.assertIs(cache, same_cache)
        self.assertEqual(5, cache.max_size)
        self.assertEqual(
            [("second", "parquet", "second")],
            list(cache._entries.keys()))
        self.assertEqual(4, cache.estimated_size)

    def test_table_option_does_not_configure_process_cache(self):
        table_options = CoreOptions(Options({
            "file-format.metadata-cache.max-size": "0 b",
        }))
        original = reader_module.ds.FileSystemDataset
        with patch.object(reader_module.ds, "FileSystemDataset", wraps=original) as dataset:
            self._read(self.paths[0], options=table_options)
            self._read(self.paths[0], options=table_options)
        self.assertEqual(1, dataset.call_count)

    def test_shares_cache_across_file_io_with_same_filesystem(self):
        other_file_io = LocalFileIO(self.temp_dir.name, Options({}))
        other_file_io.filesystem = self.file_io.filesystem

        original = reader_module.ds.FileSystemDataset
        with patch.object(reader_module.ds, "FileSystemDataset", wraps=original) as dataset:
            reader_module._file_format_dataset(
                self.file_io, "parquet", self.paths[0], DEFAULT_CACHE_SIZE)
            reader_module._file_format_dataset(
                other_file_io, "parquet", self.paths[0], DEFAULT_CACHE_SIZE)

        self.assertEqual(1, dataset.call_count)

    def test_does_not_share_across_filesystems(self):
        other_file_io = LocalFileIO(self.temp_dir.name, Options({}))
        original = reader_module.ds.FileSystemDataset
        with patch.object(reader_module.ds, "FileSystemDataset", wraps=original) as dataset:
            reader_module._file_format_dataset(
                self.file_io, "parquet", self.paths[0], DEFAULT_CACHE_SIZE)
            reader_module._file_format_dataset(
                other_file_io, "parquet", self.paths[0], DEFAULT_CACHE_SIZE)
        self.assertEqual(2, dataset.call_count)

    def test_does_not_share_across_file_formats(self):
        parquet_dataset = object()
        orc_dataset = object()
        with patch.object(
                reader_module.ds, "FileSystemDataset",
                return_value=parquet_dataset) as parquet_loader, patch.object(
                    reader_module.ds, "dataset",
                    return_value=orc_dataset) as orc_loader:
            with patch.object(
                    reader_module, "_estimate_file_format_dataset_size",
                    return_value=1):
                first = reader_module._file_format_dataset(
                    self.file_io, "parquet", self.paths[0], DEFAULT_CACHE_SIZE)
                second = reader_module._file_format_dataset(
                    self.file_io, "orc", self.paths[0], DEFAULT_CACHE_SIZE)

        self.assertIs(parquet_dataset, first)
        self.assertIs(orc_dataset, second)
        self.assertEqual(1, parquet_loader.call_count)
        orc_loader.assert_called_once_with(
            self.paths[0], format="orc", filesystem=self.file_io.filesystem)

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
        original = reader_module.ds.FileSystemDataset

        def delayed_dataset(*args, **kwargs):
            time.sleep(0.05)
            return original(*args, **kwargs)

        with patch.object(
                reader_module.ds, "FileSystemDataset", side_effect=delayed_dataset) as dataset:
            with ThreadPoolExecutor(max_workers=8) as executor:
                results = list(executor.map(
                    lambda _: self._read(self.paths[0]),
                    range(8),
                ))

        self.assertEqual(1, dataset.call_count)
        self.assertTrue(all(value == list(range(10)) for value in results))


if __name__ == "__main__":
    unittest.main()
