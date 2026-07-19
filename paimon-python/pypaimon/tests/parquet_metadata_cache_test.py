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

import os
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from pypaimon.common.options import Options
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.read.reader import format_pyarrow_reader as reader_module
from pypaimon.read.reader.format_pyarrow_reader import FormatPyArrowReader
from pypaimon.schema.data_types import AtomicType, DataField


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
    def _options(enabled, size=256):
        return CoreOptions(Options({
            "parquet.metadata-cache-enabled": enabled,
            "parquet.metadata-cache-size": size,
        }))

    def _read(self, path, options):
        reader = FormatPyArrowReader(
            self.file_io,
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
        self.assertEqual(256, options.parquet_metadata_cache_size())

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

    def test_evicts_least_recently_used_entry(self):
        options = self._options(True, size=2)
        original = reader_module.ds.dataset
        with patch.object(reader_module.ds, "dataset", wraps=original) as dataset:
            for path in self.paths:
                self._read(path, options)
            self._read(self.paths[0], options)
        self.assertEqual(4, dataset.call_count)

    def test_resizes_existing_cache(self):
        original = reader_module.ds.dataset
        with patch.object(reader_module.ds, "dataset", wraps=original) as dataset:
            for path in self.paths:
                self._read(path, self._options(True, size=3))
            self._read(self.paths[2], self._options(True, size=1))
            self._read(self.paths[0], self._options(True, size=1))
        self.assertEqual(4, dataset.call_count)

    def test_does_not_share_across_filesystems(self):
        other_file_io = LocalFileIO(self.temp_dir.name, Options({}))
        original = reader_module.ds.dataset
        with patch.object(reader_module.ds, "dataset", wraps=original) as dataset:
            reader_module._parquet_dataset(
                self.file_io, self.paths[0], True, 256)
            reader_module._parquet_dataset(
                other_file_io, self.paths[0], True, 256)
        self.assertEqual(2, dataset.call_count)

    def test_resets_after_process_change(self):
        parent_cache = reader_module._global_parquet_dataset_cache(256)
        with patch.object(reader_module.os, "getpid", return_value=os.getpid() + 1):
            child_cache = reader_module._global_parquet_dataset_cache(256)
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
