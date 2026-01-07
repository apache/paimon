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

from pypaimon.catalog.filesystem_catalog import FileSystemCatalog
from pypaimon.common.identifier import Identifier
from pypaimon.common.options import Options
from pypaimon.common.options.config import CatalogOptions
from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.manifest.schema.manifest_file_meta import ManifestFileMeta
from pypaimon.schema.schema import Schema
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.manifest.schema.simple_stats import SimpleStats
import pyarrow as pa


class ManifestEntryIdentifierTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.catalog = FileSystemCatalog(Options({CatalogOptions.WAREHOUSE.key(): cls.tempdir}))
        cls.catalog.create_database('default', False)

    def setUp(self):
        try:
            table_identifier = Identifier.from_string('default.test_table')
            table_path = self.catalog.get_table_path(table_identifier)
            if self.catalog.file_io.exists(table_path):
                self.catalog.file_io.delete(table_path, recursive=True)
        except Exception:
            pass

        pa_schema = pa.schema([('id', pa.int32()), ('value', pa.string())])
        schema = Schema.from_pyarrow_schema(pa_schema)
        self.catalog.create_table('default.test_table', schema, False)
        self.table = self.catalog.get_table('default.test_table')
        self.manifest_file_manager = ManifestFileManager(self.table)

    def _create_file_meta(self, file_name, level=0, extra_files=None, external_path=None):
        """Helper to create DataFileMeta with common defaults."""
        from pypaimon.data.timestamp import Timestamp
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
            level=level,
            extra_files=extra_files or [],
            creation_time=Timestamp.from_epoch_millis(1234567890),
            delete_row_count=0,
            embedded_index=None,
            file_source=None,
            value_stats_cols=None,
            external_path=external_path,
            first_row_id=None,
            write_cols=None
        )

    def test_add_delete_matching_same_file(self):
        """
        Core test: ADD -> DELETE should result in empty entries.

        This is the main scenario that was broken:
        - Manifest 1: ADD entry for "data-1.parquet"
        - Manifest 2: DELETE entry for "data-1.parquet" (same identifier)
        - Expected: Both entries removed, final_entries should be empty
        - Bug: If identifier was incomplete, DELETE wouldn't match ADD,
               ADD would remain, but file is gone -> empty reads
        """
        partition = GenericRow([], [])

        add_entry = ManifestEntry(
            kind=0,  # ADD
            partition=partition,
            bucket=0,
            total_buckets=1,
            file=self._create_file_meta("data-1.parquet", level=0)
        )

        delete_entry = ManifestEntry(
            kind=1,  # DELETE
            partition=partition,
            bucket=0,
            total_buckets=1,
            file=self._create_file_meta("data-1.parquet", level=0)  # Same identifier
        )

        manifest_file_1 = ManifestFileMeta(
            file_name="manifest-1.avro",
            file_size=1024,
            num_added_files=1,
            num_deleted_files=0,
            partition_stats=SimpleStats.empty_stats(),
            schema_id=0
        )

        manifest_file_2 = ManifestFileMeta(
            file_name="manifest-2.avro",
            file_size=1024,
            num_added_files=0,
            num_deleted_files=1,
            partition_stats=SimpleStats.empty_stats(),
            schema_id=0
        )

        self.manifest_file_manager.write(manifest_file_1.file_name, [add_entry])
        self.manifest_file_manager.write(manifest_file_2.file_name, [delete_entry])

        final_entries = self.manifest_file_manager.read_entries_parallel(
            [manifest_file_1, manifest_file_2])

        # With correct identifier matching, ADD and DELETE should cancel out
        self.assertEqual(
            len(final_entries), 0,
            "ADD and DELETE entries with same identifier should both be removed")

    def test_add_delete_different_levels(self):
        """
        Test that entries with different levels are NOT matched.

        Same file_name but different level -> different files -> should NOT cancel out.
        """
        partition = GenericRow([], [])

        add_entry = ManifestEntry(
            kind=0,
            partition=partition,
            bucket=0,
            total_buckets=1,
            file=self._create_file_meta("data-1.parquet", level=0)
        )

        delete_entry = ManifestEntry(
            kind=1,
            partition=partition,
            bucket=0,
            total_buckets=1,
            file=self._create_file_meta("data-1.parquet", level=1)  # Different level!
        )

        manifest_file_1 = ManifestFileMeta(
            file_name="manifest-1.avro",
            file_size=1024,
            num_added_files=1,
            num_deleted_files=0,
            partition_stats=SimpleStats.empty_stats(),
            schema_id=0
        )

        manifest_file_2 = ManifestFileMeta(
            file_name="manifest-2.avro",
            file_size=1024,
            num_added_files=0,
            num_deleted_files=1,
            partition_stats=SimpleStats.empty_stats(),
            schema_id=0
        )

        self.manifest_file_manager.write(manifest_file_1.file_name, [add_entry])
        self.manifest_file_manager.write(manifest_file_2.file_name, [delete_entry])

        final_entries = self.manifest_file_manager.read_entries_parallel(
            [manifest_file_1, manifest_file_2])

        # Different levels -> different identifiers -> should NOT match
        self.assertEqual(len(final_entries), 1, "Different levels should NOT match")
        self.assertEqual(final_entries[0].file.level, 0, "ADD entry with level=0 should remain")

    def test_add_delete_different_extra_files(self):
        """
        Test that entries with different extra_files are NOT matched.
        """
        partition = GenericRow([], [])

        add_entry = ManifestEntry(
            kind=0,
            partition=partition,
            bucket=0,
            total_buckets=1,
            file=self._create_file_meta("data-1.parquet", extra_files=["index.idx"])
        )

        delete_entry = ManifestEntry(
            kind=1,
            partition=partition,
            bucket=0,
            total_buckets=1,
            file=self._create_file_meta("data-1.parquet", extra_files=[])  # Different!
        )

        manifest_file_1 = ManifestFileMeta(
            file_name="manifest-1.avro",
            file_size=1024,
            num_added_files=1,
            num_deleted_files=0,
            partition_stats=SimpleStats.empty_stats(),
            schema_id=0
        )

        manifest_file_2 = ManifestFileMeta(
            file_name="manifest-2.avro",
            file_size=1024,
            num_added_files=0,
            num_deleted_files=1,
            partition_stats=SimpleStats.empty_stats(),
            schema_id=0
        )

        self.manifest_file_manager.write(manifest_file_1.file_name, [add_entry])
        self.manifest_file_manager.write(manifest_file_2.file_name, [delete_entry])

        final_entries = self.manifest_file_manager.read_entries_parallel(
            [manifest_file_1, manifest_file_2])

        # Different extra_files -> different identifiers -> should NOT match
        self.assertEqual(len(final_entries), 1, "Different extra_files should NOT match")
        self.assertEqual(final_entries[0].file.extra_files, ["index.idx"])


if __name__ == '__main__':
    unittest.main()
