"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import tempfile
import unittest

import pyarrow as pa

from pypaimon.catalog.filesystem_catalog import FileSystemCatalog
from pypaimon.common.identifier import Identifier
from pypaimon.common.options import Options
from pypaimon.common.options.config import CatalogOptions
from pypaimon.data.timestamp import Timestamp
from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.manifest.schema.manifest_file_meta import ManifestFileMeta
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.schema.schema import Schema
from pypaimon.table.row.generic_row import GenericRow


class ManifestFileCacheTest(unittest.TestCase):
    """Tests for ManifestFileManager caching."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.catalog = FileSystemCatalog(
            Options({CatalogOptions.WAREHOUSE.key(): cls.tempdir})
        )
        cls.catalog.create_database('default', False)

    def setUp(self):
        # Clean up any existing table
        try:
            table_identifier = Identifier.from_string('default.cache_test')
            table_path = self.catalog.get_table_path(table_identifier)
            if self.catalog.file_io.exists(table_path):
                self.catalog.file_io.delete(table_path, recursive=True)
        except Exception:
            pass

        # Create test table
        pa_schema = pa.schema([('id', pa.int32()), ('value', pa.string())])
        schema = Schema.from_pyarrow_schema(pa_schema)
        self.catalog.create_table('default.cache_test', schema, False)
        self.table = self.catalog.get_table('default.cache_test')

    def _create_file_meta(self, file_name):
        """Helper to create DataFileMeta with common defaults."""
        return DataFileMeta(
            file_name=file_name,
            file_size=1024,
            row_count=100,
            min_key=GenericRow([], []),
            max_key=GenericRow([], []),
            key_stats=SimpleStats(
                min_values=GenericRow([], []),
                max_values=GenericRow([], []),
                null_counts=[]
            ),
            value_stats=SimpleStats(
                min_values=GenericRow([], []),
                max_values=GenericRow([], []),
                null_counts=[]
            ),
            min_sequence_number=1,
            max_sequence_number=100,
            schema_id=0,
            level=0,
            extra_files=[],
            creation_time=Timestamp.from_epoch_millis(1234567890),
            delete_row_count=0,
            embedded_index=None,
            file_source=None,
            value_stats_cols=None,
            external_path=None,
            first_row_id=None,
            write_cols=None
        )

    def _create_manifest_entry(self, file_name, bucket=0):
        """Helper to create a ManifestEntry."""
        return ManifestEntry(
            kind=0,  # ADD
            partition=GenericRow([], []),
            bucket=bucket,
            total_buckets=1,
            file=self._create_file_meta(file_name)
        )

    def test_second_read_uses_cache(self):
        """Reading the same manifest file twice should hit the cache."""
        manager = ManifestFileManager(self.table, cache_max_size=10)

        # Write a manifest file
        entry = self._create_manifest_entry("data-1.parquet")
        manager.write("test-manifest.avro", [entry])

        # First read - cache miss
        result1 = manager.read("test-manifest.avro")
        self.assertEqual(manager._cache_misses, 1)
        self.assertEqual(manager._cache_hits, 0)

        # Second read - cache hit
        result2 = manager.read("test-manifest.avro")
        self.assertEqual(manager._cache_misses, 1)
        self.assertEqual(manager._cache_hits, 1)

        # Results should be equivalent
        self.assertEqual(len(result1), len(result2))
        self.assertEqual(result1[0].file.file_name, result2[0].file.file_name)

    def test_cache_disabled_when_max_size_zero(self):
        """Cache should be disabled when max_size=0."""
        manager = ManifestFileManager(self.table, cache_max_size=0)

        # Write a manifest file
        entry = self._create_manifest_entry("data-1.parquet")
        manager.write("test-manifest.avro", [entry])

        # Read twice - both should be misses (no caching)
        manager.read("test-manifest.avro")
        manager.read("test-manifest.avro")

        self.assertEqual(manager._cache_misses, 2)
        self.assertEqual(manager._cache_hits, 0)

    def test_cache_evicts_oldest_when_full(self):
        """Cache should evict oldest entries when full."""
        manager = ManifestFileManager(self.table, cache_max_size=2)

        # Write 3 manifest files
        for i in range(3):
            entry = self._create_manifest_entry(f"data-{i}.parquet")
            manager.write(f"manifest-{i}.avro", [entry])

        # Read all 3 files
        manager.read("manifest-0.avro")  # miss
        manager.read("manifest-1.avro")  # miss
        manager.read("manifest-2.avro")  # miss, evicts manifest-0

        self.assertEqual(manager._cache_misses, 3)

        # manifest-0 should have been evicted, so reading it again is a miss
        manager.read("manifest-0.avro")
        self.assertEqual(manager._cache_misses, 4)

        # manifest-2 should still be cached
        manager.read("manifest-2.avro")
        self.assertEqual(manager._cache_hits, 1)

    def test_filter_applied_on_cache_hit(self):
        """Filter should be applied even on cache hits."""
        manager = ManifestFileManager(self.table, cache_max_size=10)

        # Write manifest with entries for different buckets
        entries = [
            self._create_manifest_entry("data-1.parquet", bucket=0),
            self._create_manifest_entry("data-2.parquet", bucket=1),
            self._create_manifest_entry("data-3.parquet", bucket=0),
        ]
        manager.write("test-manifest.avro", entries)

        # First read without filter
        result_all = manager.read("test-manifest.avro")
        self.assertEqual(len(result_all), 3)

        # Second read with filter - should use cache but apply filter
        bucket_filter = lambda e: e.bucket == 0
        result_filtered = manager.read("test-manifest.avro",
                                       manifest_entry_filter=bucket_filter)
        self.assertEqual(len(result_filtered), 2)
        self.assertEqual(manager._cache_hits, 1)

    def test_default_cache_size(self):
        """Default cache size should be 100."""
        manager = ManifestFileManager(self.table)
        self.assertIsNotNone(manager._cache)
        self.assertEqual(manager._cache.maxsize, 100)


class ManifestListCacheTest(unittest.TestCase):
    """Tests for ManifestListManager caching."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.catalog = FileSystemCatalog(
            Options({CatalogOptions.WAREHOUSE.key(): cls.tempdir})
        )
        cls.catalog.create_database('default', False)

    def setUp(self):
        # Clean up any existing table
        try:
            table_identifier = Identifier.from_string('default.list_cache_test')
            table_path = self.catalog.get_table_path(table_identifier)
            if self.catalog.file_io.exists(table_path):
                self.catalog.file_io.delete(table_path, recursive=True)
        except Exception:
            pass

        # Create test table
        pa_schema = pa.schema([('id', pa.int32()), ('value', pa.string())])
        schema = Schema.from_pyarrow_schema(pa_schema)
        self.catalog.create_table('default.list_cache_test', schema, False)
        self.table = self.catalog.get_table('default.list_cache_test')

    def _create_manifest_file_meta(self, file_name):
        """Helper to create ManifestFileMeta."""
        return ManifestFileMeta(
            file_name=file_name,
            file_size=1024,
            num_added_files=1,
            num_deleted_files=0,
            partition_stats=SimpleStats.empty_stats(),
            schema_id=0
        )

    def test_second_read_uses_cache(self):
        """Reading the same manifest list twice should hit the cache."""
        manager = ManifestListManager(self.table, cache_max_size=10)

        # Write a manifest list
        meta = self._create_manifest_file_meta("manifest-1.avro")
        manager.write("manifest-list-1", [meta])

        # First read - cache miss
        result1 = manager.read("manifest-list-1")
        self.assertEqual(manager._cache_misses, 1)
        self.assertEqual(manager._cache_hits, 0)

        # Second read - cache hit
        result2 = manager.read("manifest-list-1")
        self.assertEqual(manager._cache_misses, 1)
        self.assertEqual(manager._cache_hits, 1)

        # Results should be equivalent
        self.assertEqual(len(result1), len(result2))

    def test_cache_disabled_when_max_size_zero(self):
        """Cache should be disabled when max_size=0."""
        manager = ManifestListManager(self.table, cache_max_size=0)

        # Write a manifest list
        meta = self._create_manifest_file_meta("manifest-1.avro")
        manager.write("manifest-list-1", [meta])

        # Read twice - both should be misses
        manager.read("manifest-list-1")
        manager.read("manifest-list-1")

        self.assertEqual(manager._cache_misses, 2)
        self.assertEqual(manager._cache_hits, 0)

    def test_default_cache_size(self):
        """Default cache size should be 50."""
        manager = ManifestListManager(self.table)
        self.assertIsNotNone(manager._cache)
        self.assertEqual(manager._cache.maxsize, 50)

    def test_read_base_returns_only_base_manifest(self):
        """read_base() should only read base_manifest_list, not delta."""
        from pypaimon.snapshot.snapshot import Snapshot

        manager = ManifestListManager(self.table, cache_max_size=10)

        # Write base and delta manifest lists
        base_meta = self._create_manifest_file_meta("manifest-base.avro")
        delta_meta = self._create_manifest_file_meta("manifest-delta.avro")
        manager.write("base-manifest-list", [base_meta])
        manager.write("delta-manifest-list", [delta_meta])

        # Create mock snapshot with both base and delta
        snapshot = Snapshot(
            version=3,
            id=1,
            schema_id=0,
            base_manifest_list="base-manifest-list",
            delta_manifest_list="delta-manifest-list",
            commit_user="test",
            commit_identifier=1,
            commit_kind="APPEND",
            time_millis=1234567890,
            total_record_count=100,
            delta_record_count=10,
        )

        # read_base should only return base manifests
        result = manager.read_base(snapshot)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].file_name, "manifest-base.avro")

    def test_read_base_uses_cache(self):
        """read_base() should use LRU cache."""
        from pypaimon.snapshot.snapshot import Snapshot

        manager = ManifestListManager(self.table, cache_max_size=10)

        # Write base manifest list
        base_meta = self._create_manifest_file_meta("manifest-base.avro")
        manager.write("base-manifest-list", [base_meta])

        # Create mock snapshot
        snapshot = Snapshot(
            version=3,
            id=1,
            schema_id=0,
            base_manifest_list="base-manifest-list",
            delta_manifest_list="delta-manifest-list",
            commit_user="test",
            commit_identifier=1,
            commit_kind="APPEND",
            time_millis=1234567890,
            total_record_count=100,
            delta_record_count=10,
        )

        # First read - cache miss
        manager.read_base(snapshot)
        self.assertEqual(manager._cache_misses, 1)
        self.assertEqual(manager._cache_hits, 0)

        # Second read - cache hit
        manager.read_base(snapshot)
        self.assertEqual(manager._cache_misses, 1)
        self.assertEqual(manager._cache_hits, 1)


if __name__ == '__main__':
    unittest.main()
