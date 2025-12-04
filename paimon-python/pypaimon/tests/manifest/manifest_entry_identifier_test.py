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
from unittest.mock import Mock

from pypaimon.catalog.filesystem_catalog import FileSystemCatalog
from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.manifest.schema.manifest_file_meta import ManifestFileMeta
from pypaimon.schema.schema import Schema
from pypaimon.table.row.binary_row import BinaryRow
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.schema.data_field import DataField
from pypaimon.table.schema.data_types import AtomicType
from pypaimon.table.schema.simple_stats import SimpleStats
import pyarrow as pa


class ManifestEntryIdentifierTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.catalog = FileSystemCatalog(cls.tempdir)

    def setUp(self):
        pa_schema = pa.schema([('id', pa.int32()), ('value', pa.string())])
        schema = Schema.from_pyarrow_schema(pa_schema)
        self.catalog.create_table('default.test_table', schema, False)
        self.table = self.catalog.get_table('default.test_table')
        self.manifest_file_manager = ManifestFileManager(self.table)

    def test_entry_identifier_with_different_levels(self):
        partition_fields = []
        partition = GenericRow([], partition_fields)

        # Create ADD entry with level=0
        add_file_meta = DataFileMeta(
            file_name="data-1.parquet",
            file_size=1024,
            row_count=100,
            min_key=BinaryRow([], []),
            max_key=BinaryRow([], []),
            key_stats=SimpleStats(
                min_values=BinaryRow([], []),
                max_values=BinaryRow([], []),
                null_counts=[]
            ),
            value_stats=SimpleStats(
                min_values=BinaryRow([], []),
                max_values=BinaryRow([], []),
                null_counts=[]
            ),
            min_sequence_number=1,
            max_sequence_number=100,
            schema_id=0,
            level=0,  # Level 0
            extra_files=None,
            creation_time=1234567890,
            delete_row_count=0,
            embedded_index=None,
            file_source=None,
            value_stats_cols=None,
            external_path=None,
            first_row_id=None,
            write_cols=None
        )

        add_entry = ManifestEntry(
            kind=0,  # ADD
            partition=partition,
            bucket=0,
            total_buckets=1,
            file=add_file_meta
        )

        # Create DELETE entry with level=1, same file_name
        delete_file_meta = DataFileMeta(
            file_name="data-1.parquet",  # Same file name
            file_size=1024,
            row_count=100,
            min_key=BinaryRow([], []),
            max_key=BinaryRow([], []),
            key_stats=SimpleStats(
                min_values=BinaryRow([], []),
                max_values=BinaryRow([], []),
                null_counts=[]
            ),
            value_stats=SimpleStats(
                min_values=BinaryRow([], []),
                max_values=BinaryRow([], []),
                null_counts=[]
            ),
            min_sequence_number=101,
            max_sequence_number=200,
            schema_id=0,
            level=1,  # Different level!
            extra_files=None,
            creation_time=1234567891,
            delete_row_count=0,
            embedded_index=None,
            file_source=None,
            value_stats_cols=None,
            external_path=None,
            first_row_id=None,
            write_cols=None
        )

        delete_entry = ManifestEntry(
            kind=1,  # DELETE
            partition=partition,
            bucket=0,
            total_buckets=1,
            file=delete_file_meta
        )

        # Simulate reading from two manifest files
        manifest_file_1 = ManifestFileMeta(
            version=2,
            file_name="manifest-1.avro",
            file_size=1024,
            num_added_files=1,
            num_deleted_files=0,
            partition_stats=None,
            schema_id=0
        )

        manifest_file_2 = ManifestFileMeta(
            version=2,
            file_name="manifest-2.avro",
            file_size=1024,
            num_added_files=0,
            num_deleted_files=1,
            partition_stats=None,
            schema_id=0
        )

        self.manifest_file_manager.write(manifest_file_1.file_name, [add_entry])
        self.manifest_file_manager.write(manifest_file_2.file_name, [delete_entry])

        manifest_files = [manifest_file_1, manifest_file_2]
        final_entries = self.manifest_file_manager.read_entries_parallel(manifest_files)

        self.assertEqual(len(final_entries), 1, "ADD entry should remain after merge")
        self.assertEqual(final_entries[0].kind, 0, "Remaining entry should be ADD")
        self.assertEqual(final_entries[0].file.level, 0, "Remaining entry should have level=0")

    def test_entry_identifier_with_different_extra_files(self):
        partition_fields = []
        partition = GenericRow([], partition_fields)

        # Create ADD entry with extra_files=["index.idx"]
        add_file_meta = DataFileMeta(
            file_name="data-1.parquet",
            file_size=1024,
            row_count=100,
            min_key=BinaryRow([], []),
            max_key=BinaryRow([], []),
            key_stats=SimpleStats(
                min_values=BinaryRow([], []),
                max_values=BinaryRow([], []),
                null_counts=[]
            ),
            value_stats=SimpleStats(
                min_values=BinaryRow([], []),
                max_values=BinaryRow([], []),
                null_counts=[]
            ),
            min_sequence_number=1,
            max_sequence_number=100,
            schema_id=0,
            level=0,
            extra_files=["index.idx"],  # Has extra file
            creation_time=1234567890,
            delete_row_count=0,
            embedded_index=None,
            file_source=None,
            value_stats_cols=None,
            external_path=None,
            first_row_id=None,
            write_cols=None
        )

        add_entry = ManifestEntry(
            kind=0,  # ADD
            partition=partition,
            bucket=0,
            total_buckets=1,
            file=add_file_meta
        )

        # Create DELETE entry with no extra_files, same file_name
        delete_file_meta = DataFileMeta(
            file_name="data-1.parquet",  # Same file name
            file_size=1024,
            row_count=100,
            min_key=BinaryRow([], []),
            max_key=BinaryRow([], []),
            key_stats=SimpleStats(
                min_values=BinaryRow([], []),
                max_values=BinaryRow([], []),
                null_counts=[]
            ),
            value_stats=SimpleStats(
                min_values=BinaryRow([], []),
                max_values=BinaryRow([], []),
                null_counts=[]
            ),
            min_sequence_number=101,
            max_sequence_number=200,
            schema_id=0,
            level=0,
            extra_files=None,  # No extra files
            creation_time=1234567891,
            delete_row_count=0,
            embedded_index=None,
            file_source=None,
            value_stats_cols=None,
            external_path=None,
            first_row_id=None,
            write_cols=None
        )

        delete_entry = ManifestEntry(
            kind=1,  # DELETE
            partition=partition,
            bucket=0,
            total_buckets=1,
            file=delete_file_meta
        )

        manifest_file_1 = ManifestFileMeta(
            version=2,
            file_name="manifest-1.avro",
            file_size=1024,
            num_added_files=1,
            num_deleted_files=0,
            partition_stats=None,
            schema_id=0
        )

        manifest_file_2 = ManifestFileMeta(
            version=2,
            file_name="manifest-2.avro",
            file_size=1024,
            num_added_files=0,
            num_deleted_files=1,
            partition_stats=None,
            schema_id=0
        )

        self.manifest_file_manager.write(manifest_file_1.file_name, [add_entry])
        self.manifest_file_manager.write(manifest_file_2.file_name, [delete_entry])

        manifest_files = [manifest_file_1, manifest_file_2]
        final_entries = self.manifest_file_manager.read_entries_parallel(manifest_files)

        # With correct identifier (including extra_files), these should be different entries
        self.assertEqual(len(final_entries), 1, "ADD entry should remain after merge")
        self.assertEqual(final_entries[0].kind, 0, "Remaining entry should be ADD")
        self.assertEqual(final_entries[0].file.extra_files, ["index.idx"])

    def test_entry_identifier_with_different_external_paths(self):
        """Test that entries with same partition/bucket/file_name but different external_paths are correctly distinguished."""
        partition_fields = []
        partition = GenericRow([], partition_fields)

        # Create ADD entry with external_path
        add_file_meta = DataFileMeta(
            file_name="data-1.parquet",
            file_size=1024,
            row_count=100,
            min_key=BinaryRow([], []),
            max_key=BinaryRow([], []),
            key_stats=SimpleStats(
                min_values=BinaryRow([], []),
                max_values=BinaryRow([], []),
                null_counts=[]
            ),
            value_stats=SimpleStats(
                min_values=BinaryRow([], []),
                max_values=BinaryRow([], []),
                null_counts=[]
            ),
            min_sequence_number=1,
            max_sequence_number=100,
            schema_id=0,
            level=0,
            extra_files=None,
            creation_time=1234567890,
            delete_row_count=0,
            embedded_index=None,
            file_source=None,
            value_stats_cols=None,
            external_path="s3://bucket/path/data-1.parquet",  # Has external path
            first_row_id=None,
            write_cols=None
        )

        add_entry = ManifestEntry(
            kind=0,  # ADD
            partition=partition,
            bucket=0,
            total_buckets=1,
            file=add_file_meta
        )

        # Create DELETE entry with no external_path, same file_name
        delete_file_meta = DataFileMeta(
            file_name="data-1.parquet",  # Same file name
            file_size=1024,
            row_count=100,
            min_key=BinaryRow([], []),
            max_key=BinaryRow([], []),
            key_stats=SimpleStats(
                min_values=BinaryRow([], []),
                max_values=BinaryRow([], []),
                null_counts=[]
            ),
            value_stats=SimpleStats(
                min_values=BinaryRow([], []),
                max_values=BinaryRow([], []),
                null_counts=[]
            ),
            min_sequence_number=101,
            max_sequence_number=200,
            schema_id=0,
            level=0,
            extra_files=None,
            creation_time=1234567891,
            delete_row_count=0,
            embedded_index=None,
            file_source=None,
            value_stats_cols=None,
            external_path=None,  # No external path
            first_row_id=None,
            write_cols=None
        )

        delete_entry = ManifestEntry(
            kind=1,  # DELETE
            partition=partition,
            bucket=0,
            total_buckets=1,
            file=delete_file_meta
        )

        manifest_file_1 = ManifestFileMeta(
            version=2,
            file_name="manifest-1.avro",
            file_size=1024,
            num_added_files=1,
            num_deleted_files=0,
            partition_stats=None,
            schema_id=0
        )

        manifest_file_2 = ManifestFileMeta(
            version=2,
            file_name="manifest-2.avro",
            file_size=1024,
            num_added_files=0,
            num_deleted_files=1,
            partition_stats=None,
            schema_id=0
        )

        self.manifest_file_manager.write(manifest_file_1.file_name, [add_entry])
        self.manifest_file_manager.write(manifest_file_2.file_name, [delete_entry])

        manifest_files = [manifest_file_1, manifest_file_2]
        final_entries = self.manifest_file_manager.read_entries_parallel(manifest_files)

        self.assertEqual(len(final_entries), 1, "ADD entry should remain after merge")
        self.assertEqual(final_entries[0].kind, 0, "Remaining entry should be ADD")
        self.assertEqual(final_entries[0].file.external_path, "s3://bucket/path/data-1.parquet")

    def test_entry_identifier_matching_same_file(self):
        partition_fields = []
        partition = GenericRow([], partition_fields)

        # Create ADD entry
        add_file_meta = DataFileMeta(
            file_name="data-1.parquet",
            file_size=1024,
            row_count=100,
            min_key=BinaryRow([], []),
            max_key=BinaryRow([], []),
            key_stats=SimpleStats(
                min_values=BinaryRow([], []),
                max_values=BinaryRow([], []),
                null_counts=[]
            ),
            value_stats=SimpleStats(
                min_values=BinaryRow([], []),
                max_values=BinaryRow([], []),
                null_counts=[]
            ),
            min_sequence_number=1,
            max_sequence_number=100,
            schema_id=0,
            level=0,
            extra_files=None,
            creation_time=1234567890,
            delete_row_count=0,
            embedded_index=None,
            file_source=None,
            value_stats_cols=None,
            external_path=None,
            first_row_id=None,
            write_cols=None
        )

        add_entry = ManifestEntry(
            kind=0,  # ADD
            partition=partition,
            bucket=0,
            total_buckets=1,
            file=add_file_meta
        )

        delete_file_meta = DataFileMeta(
            file_name="data-1.parquet",  # Same file name
            file_size=1024,
            row_count=100,
            min_key=BinaryRow([], []),
            max_key=BinaryRow([], []),
            key_stats=SimpleStats(
                min_values=BinaryRow([], []),
                max_values=BinaryRow([], []),
                null_counts=[]
            ),
            value_stats=SimpleStats(
                min_values=BinaryRow([], []),
                max_values=BinaryRow([], []),
                null_counts=[]
            ),
            min_sequence_number=101,
            max_sequence_number=200,
            schema_id=0,
            level=0,  # Same level
            extra_files=None,  # Same extra_files
            creation_time=1234567891,
            delete_row_count=0,
            embedded_index=None,  # Same embedded_index
            file_source=None,
            value_stats_cols=None,
            external_path=None,  # Same external_path
            first_row_id=None,
            write_cols=None
        )

        delete_entry = ManifestEntry(
            kind=1,  # DELETE
            partition=partition,
            bucket=0,
            total_buckets=1,
            file=delete_file_meta
        )

        manifest_file_1 = ManifestFileMeta(
            version=2,
            file_name="manifest-1.avro",
            file_size=1024,
            num_added_files=1,
            num_deleted_files=0,
            partition_stats=None,
            schema_id=0
        )

        manifest_file_2 = ManifestFileMeta(
            version=2,
            file_name="manifest-2.avro",
            file_size=1024,
            num_added_files=0,
            num_deleted_files=1,
            partition_stats=None,
            schema_id=0
        )

        self.manifest_file_manager.write(manifest_file_1.file_name, [add_entry])
        self.manifest_file_manager.write(manifest_file_2.file_name, [delete_entry])

        manifest_files = [manifest_file_1, manifest_file_2]
        final_entries = self.manifest_file_manager.read_entries_parallel(manifest_files)

        self.assertEqual(len(final_entries), 0, "Matching ADD and DELETE entries should both be removed")


if __name__ == '__main__':
    unittest.main()

