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
from contextlib import contextmanager
from unittest.mock import patch

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

_EMPTY_ROW = GenericRow([], [])
_EMPTY_STATS = SimpleStats(min_values=_EMPTY_ROW, max_values=_EMPTY_ROW, null_counts=[])


class _ManifestCacheSetup(unittest.TestCase):
    """Shared setup for manifest cache tests.

    Subclasses must set _table_name and implement _make_manager / _write_one.
    """

    _table_name: str

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.catalog = FileSystemCatalog(
            Options({CatalogOptions.WAREHOUSE.key(): cls.tempdir})
        )
        cls.catalog.create_database('default', False)

    def setUp(self):
        table_id = f'default.{self._table_name}'
        try:
            table_identifier = Identifier.from_string(table_id)
            table_path = self.catalog.get_table_path(table_identifier)
            if self.catalog.file_io.exists(table_path):
                self.catalog.file_io.delete(table_path, recursive=True)
        except Exception:
            pass

        pa_schema = pa.schema([('id', pa.int32()), ('value', pa.string())])
        schema = Schema.from_pyarrow_schema(pa_schema)
        self.catalog.create_table(table_id, schema, False)
        self.table = self.catalog.get_table(table_id)

    def _make_manager(self, cache_max_size=10):
        raise NotImplementedError

    def _write_one(self, manager, name):
        """Write a single item that can be read back by manager.read(name)."""
        raise NotImplementedError

    def _warm_side_caches(self, manager, name):
        """Read one item to warm any side caches (e.g. schema cache), then
        clear the LRU cache so subsequent reads start fresh for counting."""
        manager.read(name)
        manager._cache.clear()


@contextmanager
def _spy_io(manager):
    """Spy on manager.file_io.new_input_stream to count real IO calls."""
    original = manager.file_io.new_input_stream
    with patch.object(manager.file_io, 'new_input_stream', wraps=original) as spy:
        yield spy


class _CacheBehaviourMixin:
    """Shared cache-behaviour tests. Mixed into concrete TestCase subclasses."""

    def test_second_read_uses_cache(self):
        manager = self._make_manager(cache_max_size=10)
        self._write_one(manager, "test-item")
        self._warm_side_caches(manager, "test-item")

        with _spy_io(manager) as spy:
            manager.read("test-item")
            manager.read("test-item")
            self.assertEqual(spy.call_count, 1)

    def test_cache_disabled_when_max_size_zero(self):
        manager = self._make_manager(cache_max_size=0)
        self._write_one(manager, "test-item")
        self._warm_side_caches(manager, "test-item")

        with _spy_io(manager) as spy:
            manager.read("test-item")
            manager.read("test-item")
            self.assertEqual(spy.call_count, 2)


class ManifestFileCacheTest(_CacheBehaviourMixin, _ManifestCacheSetup):
    """Tests for ManifestFileManager caching."""

    _table_name = 'cache_test'

    def _make_manager(self, cache_max_size=10):
        return ManifestFileManager(self.table, cache_max_size=cache_max_size)

    def _write_one(self, manager, name):
        entry = ManifestEntry(
            kind=0,
            partition=_EMPTY_ROW,
            bucket=0,
            total_buckets=1,
            file=DataFileMeta(
                file_name="data.parquet", file_size=1024, row_count=100,
                min_key=_EMPTY_ROW, max_key=_EMPTY_ROW,
                key_stats=_EMPTY_STATS, value_stats=_EMPTY_STATS,
                min_sequence_number=1, max_sequence_number=100,
                schema_id=0, level=0, extra_files=[],
                creation_time=Timestamp.from_epoch_millis(0),
                delete_row_count=0, embedded_index=None, file_source=None,
                value_stats_cols=None, external_path=None,
                first_row_id=None, write_cols=None,
            ),
        )
        manager.write(name, [entry])

    def _create_manifest_entry(self, file_name, bucket=0):
        entry = ManifestEntry(
            kind=0,
            partition=_EMPTY_ROW,
            bucket=bucket,
            total_buckets=1,
            file=DataFileMeta(
                file_name=file_name, file_size=1024, row_count=100,
                min_key=_EMPTY_ROW, max_key=_EMPTY_ROW,
                key_stats=_EMPTY_STATS, value_stats=_EMPTY_STATS,
                min_sequence_number=1, max_sequence_number=100,
                schema_id=0, level=0, extra_files=[],
                creation_time=Timestamp.from_epoch_millis(0),
                delete_row_count=0, embedded_index=None, file_source=None,
                value_stats_cols=None, external_path=None,
                first_row_id=None, write_cols=None,
            ),
        )
        return entry

    def test_cache_evicts_oldest_when_full(self):
        manager = self._make_manager(cache_max_size=2)

        for i in range(3):
            entry = self._create_manifest_entry(f"data-{i}.parquet")
            manager.write(f"manifest-{i}.avro", [entry])

        # Warm schema cache so only manifest IO is counted
        self._warm_side_caches(manager, "manifest-0.avro")

        with _spy_io(manager) as spy:
            manager.read("manifest-0.avro")
            manager.read("manifest-1.avro")
            manager.read("manifest-2.avro")  # evicts manifest-0
            self.assertEqual(spy.call_count, 3)

            manager.read("manifest-0.avro")  # re-read after eviction
            self.assertEqual(spy.call_count, 4)

            manager.read("manifest-2.avro")  # still cached
            self.assertEqual(spy.call_count, 4)

    def test_filter_applied_on_cache_hit(self):
        manager = self._make_manager(cache_max_size=10)

        entries = [
            self._create_manifest_entry("data-1.parquet", bucket=0),
            self._create_manifest_entry("data-2.parquet", bucket=1),
            self._create_manifest_entry("data-3.parquet", bucket=0),
        ]
        manager.write("test-manifest.avro", entries)
        self._warm_side_caches(manager, "test-manifest.avro")

        with _spy_io(manager) as spy:
            result_all = manager.read("test-manifest.avro")
            self.assertEqual(len(result_all), 3)

            result_filtered = manager.read(
                "test-manifest.avro", manifest_entry_filter=lambda e: e.bucket == 0)
            self.assertEqual(len(result_filtered), 2)
            self.assertEqual(spy.call_count, 1)


class ManifestListCacheTest(_CacheBehaviourMixin, _ManifestCacheSetup):
    """Tests for ManifestListManager caching."""

    _table_name = 'list_cache_test'

    def _make_manager(self, cache_max_size=10):
        return ManifestListManager(self.table, cache_max_size=cache_max_size)

    def _write_one(self, manager, name):
        meta = ManifestFileMeta(
            file_name="manifest.avro", file_size=1024,
            num_added_files=1, num_deleted_files=0,
            partition_stats=SimpleStats.empty_stats(), schema_id=0,
        )
        manager.write(name, [meta])

    def _make_snapshot(self, base_manifest_list, delta_manifest_list="delta-manifest-list"):
        from pypaimon.snapshot.snapshot import Snapshot
        return Snapshot(
            version=3, id=1, schema_id=0,
            base_manifest_list=base_manifest_list,
            delta_manifest_list=delta_manifest_list,
            commit_user="test", commit_identifier=1, commit_kind="APPEND",
            time_millis=1234567890, total_record_count=100, delta_record_count=10,
        )

    def test_read_base_returns_only_base_manifest(self):
        manager = self._make_manager(cache_max_size=10)

        base_meta = ManifestFileMeta(
            file_name="manifest-base.avro", file_size=1024,
            num_added_files=1, num_deleted_files=0,
            partition_stats=SimpleStats.empty_stats(), schema_id=0,
        )
        delta_meta = ManifestFileMeta(
            file_name="manifest-delta.avro", file_size=1024,
            num_added_files=1, num_deleted_files=0,
            partition_stats=SimpleStats.empty_stats(), schema_id=0,
        )
        manager.write("base-manifest-list", [base_meta])
        manager.write("delta-manifest-list", [delta_meta])

        snapshot = self._make_snapshot("base-manifest-list", "delta-manifest-list")
        result = manager.read_base(snapshot)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].file_name, "manifest-base.avro")

    def test_read_base_uses_cache(self):
        manager = self._make_manager(cache_max_size=10)
        self._write_one(manager, "base-manifest-list")
        snapshot = self._make_snapshot("base-manifest-list")

        with _spy_io(manager) as spy:
            manager.read_base(snapshot)
            manager.read_base(snapshot)
            self.assertEqual(spy.call_count, 1)


if __name__ == '__main__':
    unittest.main()
