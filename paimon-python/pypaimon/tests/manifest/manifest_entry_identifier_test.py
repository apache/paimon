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
import shutil
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

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

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

        # Different levels -> different identifiers -> both preserved
        self.assertEqual(len(final_entries), 2, "Different levels should NOT match")
        kinds = {e.kind for e in final_entries}
        self.assertEqual(kinds, {0, 1}, "Both ADD and DELETE should remain")

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

        # Different extra_files -> different identifiers -> both preserved
        self.assertEqual(len(final_entries), 2, "Different extra_files should NOT match")
        kinds = {e.kind for e in final_entries}
        self.assertEqual(kinds, {0, 1}, "Both ADD and DELETE should remain")

    def test_delete_then_add_same_identifier(self):
        """DELETE followed by ADD of the same identifier (compaction scenario).

        - Manifest 1: ADD(file_A)
        - Manifest 2: DELETE(file_A), ADD(file_A) (re-added after compaction)
        - Expected: file_A exists (the re-ADD survives)
        - Old bug: global DELETE set filtered ALL matching ADDs regardless of order.
        """
        partition = GenericRow([], [])

        add_entry_v1 = ManifestEntry(
            kind=0, partition=partition, bucket=0, total_buckets=1,
            file=self._create_file_meta("data-1.parquet", level=0)
        )
        delete_entry = ManifestEntry(
            kind=1, partition=partition, bucket=0, total_buckets=1,
            file=self._create_file_meta("data-1.parquet", level=0)
        )
        add_entry_v2 = ManifestEntry(
            kind=0, partition=partition, bucket=0, total_buckets=1,
            file=self._create_file_meta("data-1.parquet", level=0)
        )

        manifest_file_1 = ManifestFileMeta(
            file_name="manifest-1.avro", file_size=1024,
            num_added_files=1, num_deleted_files=0,
            partition_stats=SimpleStats.empty_stats(), schema_id=0
        )
        manifest_file_2 = ManifestFileMeta(
            file_name="manifest-2.avro", file_size=1024,
            num_added_files=1, num_deleted_files=1,
            partition_stats=SimpleStats.empty_stats(), schema_id=0
        )

        self.manifest_file_manager.write(manifest_file_1.file_name, [add_entry_v1])
        self.manifest_file_manager.write(manifest_file_2.file_name, [delete_entry, add_entry_v2])

        final_entries = self.manifest_file_manager.read_entries_parallel(
            [manifest_file_1, manifest_file_2])

        # ADD(v1) cancelled by DELETE, then ADD(v2) re-added -> 1 entry
        self.assertEqual(len(final_entries), 1,
                         "Re-added file after DELETE should survive")
        self.assertEqual(final_entries[0].kind, 0, "Surviving entry should be ADD")

    def test_unmatched_delete_preserved(self):
        """A DELETE without a matching ADD should be preserved in the output."""
        partition = GenericRow([], [])

        delete_entry = ManifestEntry(
            kind=1, partition=partition, bucket=0, total_buckets=1,
            file=self._create_file_meta("data-orphan.parquet", level=0)
        )

        manifest_file = ManifestFileMeta(
            file_name="manifest-1.avro", file_size=1024,
            num_added_files=0, num_deleted_files=1,
            partition_stats=SimpleStats.empty_stats(), schema_id=0
        )

        self.manifest_file_manager.write(manifest_file.file_name, [delete_entry])

        final_entries = self.manifest_file_manager.read_entries_parallel([manifest_file])

        self.assertEqual(len(final_entries), 1, "Unmatched DELETE should be preserved")
        self.assertEqual(final_entries[0].kind, 1)


class MergeEntriesUnitTest(unittest.TestCase):
    """Unit tests for FileEntry.merge_entries without Avro I/O."""

    def _create_file_meta(self, file_name, level=0):
        from pypaimon.data.timestamp import Timestamp
        return DataFileMeta(
            file_name=file_name, file_size=1024, row_count=100,
            min_key=GenericRow([], []), max_key=GenericRow([], []),
            key_stats=SimpleStats.empty_stats(), value_stats=SimpleStats.empty_stats(),
            min_sequence_number=1, max_sequence_number=100, schema_id=0,
            level=level, extra_files=[],
            creation_time=Timestamp.from_epoch_millis(0), delete_row_count=0,
            embedded_index=None, file_source=None, value_stats_cols=None,
            external_path=None, first_row_id=None, write_cols=None
        )

    def _entry(self, kind, partition_values, bucket, file_name, level=0):
        from pypaimon.schema.data_types import DataField, AtomicType
        fields = [DataField(0, 'pt', AtomicType('STRING'))] if partition_values else []
        partition = GenericRow(partition_values, fields)
        return ManifestEntry(
            kind=kind, partition=partition, bucket=bucket, total_buckets=1,
            file=self._create_file_meta(file_name, level=level)
        )

    def test_different_partitions_not_matched(self):
        """Same file_name but different partitions should not cancel."""
        from pypaimon.manifest.schema.file_entry import FileEntry
        entries = [
            self._entry(0, ['p1'], 0, 'data.parquet'),
            self._entry(1, ['p2'], 0, 'data.parquet'),
        ]
        result = FileEntry.merge_entries(entries)
        self.assertEqual(len(result), 2)

    def test_different_buckets_not_matched(self):
        """Same file_name but different buckets should not cancel."""
        from pypaimon.manifest.schema.file_entry import FileEntry
        entries = [
            self._entry(0, [], 0, 'data.parquet'),
            self._entry(1, [], 1, 'data.parquet'),
        ]
        result = FileEntry.merge_entries(entries)
        self.assertEqual(len(result), 2)

    def test_duplicate_add_raises_error(self):
        """Adding the same file twice should raise RuntimeError."""
        from pypaimon.manifest.schema.file_entry import FileEntry
        entries = [
            self._entry(0, [], 0, 'data.parquet'),
            self._entry(0, [], 0, 'data.parquet'),
        ]
        with self.assertRaises(RuntimeError):
            FileEntry.merge_entries(entries)

    def test_multi_partition_merge(self):
        """Multiple partitions with interleaved ADD/DELETE."""
        from pypaimon.manifest.schema.file_entry import FileEntry
        entries = [
            self._entry(0, ['p1'], 0, 'a.parquet'),
            self._entry(0, ['p2'], 0, 'b.parquet'),
            self._entry(1, ['p1'], 0, 'a.parquet'),  # cancels p1/a
            self._entry(0, ['p1'], 0, 'c.parquet'),
        ]
        result = FileEntry.merge_entries(entries)
        self.assertEqual(len(result), 2)
        names = {e.file.file_name for e in result}
        self.assertEqual(names, {'b.parquet', 'c.parquet'})


if __name__ == '__main__':
    unittest.main()
