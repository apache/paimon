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

_EMPTY_ROW = GenericRow([], [])
_EMPTY_STATS = SimpleStats(min_values=_EMPTY_ROW, max_values=_EMPTY_ROW, null_counts=[])


class _ManifestManagerSetup(unittest.TestCase):
    """Shared setup for manifest manager tests.

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

    def _make_manager(self):
        raise NotImplementedError

    def _write_one(self, manager, name):
        """Write a single item that can be read back by manager.read(name)."""
        raise NotImplementedError


class ManifestFileManagerTest(_ManifestManagerSetup):
    """Tests for ManifestFileManager."""

    _table_name = 'manager_test'

    def _make_manager(self):
        return ManifestFileManager(self.table)

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

    def test_filter_applied_after_read(self):
        manager = self._make_manager()

        entries = [
            self._create_manifest_entry("data-1.parquet", bucket=0),
            self._create_manifest_entry("data-2.parquet", bucket=1),
            self._create_manifest_entry("data-3.parquet", bucket=0),
        ]
        manager.write("test-manifest.avro", entries)

        result_all = manager.read("test-manifest.avro")
        self.assertEqual(len(result_all), 3)

        result_filtered = manager.read(
            "test-manifest.avro", manifest_entry_filter=lambda e: e.bucket == 0)
        self.assertEqual(len(result_filtered), 2)


class ManifestListManagerTest(_ManifestManagerSetup):
    """Tests for ManifestListManager."""

    _table_name = 'list_manager_test'

    def _make_manager(self):
        return ManifestListManager(self.table)

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
        manager = self._make_manager()

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


if __name__ == '__main__':
    unittest.main()
