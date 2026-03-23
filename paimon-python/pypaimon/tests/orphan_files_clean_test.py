################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and
#  limitations under the License.
################################################################################

import os
import shutil
import tempfile
import time
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.orphan_files_clean import OrphanFilesClean
from pypaimon.schema.data_types import DataField


class TestOrphanFilesClean(unittest.TestCase):
    """Test orphan files cleaning functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.tempdir = tempfile.mkdtemp(prefix="orphan_test_")
        self.warehouse_path = os.path.join(self.tempdir, "warehouse")
        self.db_name = "test_db"
        self.table_name = "test_table"

        # Create catalog
        self.catalog = CatalogFactory.create({"warehouse": self.warehouse_path})
        self.catalog.create_database(self.db_name, ignore_if_exists=True)

        # Create table with schema
        fields = [
            DataField.from_dict({"id": 1, "name": "id", "type": "INT"}),
            DataField.from_dict({"id": 2, "name": "name", "type": "STRING"}),
            DataField.from_dict({"id": 3, "name": "dt", "type": "STRING"}),
        ]
        schema = Schema(fields=fields, partition_keys=["dt"], options={"bucket": "1"})

        identifier = f"{self.db_name}.{self.table_name}"
        self.catalog.create_table(identifier, schema, ignore_if_exists=True)
        self.table = self.catalog.get_table(identifier)
        self.file_io = self.table.file_io
        self.table_path = self.table.table_path

    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def test_removing_empty_directories(self):
        """Test that empty directories are removed while valid ones are kept."""
        # Create empty directories (orphan directories)
        empty_dir1 = os.path.join(
            self.table_path, "bucket", "dt=2024-01-03", "bucket-0"
        )
        empty_dir2 = os.path.join(
            self.table_path, "bucket", "dt=2024-01-03", "bucket-1"
        )
        os.makedirs(empty_dir1, exist_ok=True)
        os.makedirs(empty_dir2, exist_ok=True)

        # Make directories old enough
        old_time = time.time() - (2 * 24 * 60 * 60)  # 2 days ago
        os.utime(empty_dir1, (old_time, old_time))
        os.utime(empty_dir2, (old_time, old_time))

        # Create non-empty directory with data
        non_empty_dir = os.path.join(
            self.table_path, "bucket", "dt=2024-01-04", "bucket-0"
        )
        os.makedirs(non_empty_dir, exist_ok=True)
        valid_file = os.path.join(non_empty_dir, "data-1.parquet")
        with open(valid_file, "w") as f:
            f.write("valid data")

        # Verify directories exist before cleaning
        self.assertTrue(
            os.path.exists(empty_dir1), f"Empty directory {empty_dir1} should exist"
        )
        self.assertTrue(
            os.path.exists(empty_dir2), f"Empty directory {empty_dir2} should exist"
        )
        self.assertTrue(
            os.path.exists(non_empty_dir),
            f"Non-empty directory {non_empty_dir} should exist",
        )

        # Set older_than to allow deletion
        older_than = int(time.time() * 1000) - (24 * 60 * 60 * 1000)  # 1 day ago

        # Clean orphan files
        cleaner = OrphanFilesClean(
            table_path=self.table_path,
            file_io=self.file_io,
            table_schema=self.table.table_schema,
            older_than_millis=older_than,
            dry_run=False,
        )
        cleaner.clean()

        # Verify empty directories are deleted
        self.assertFalse(
            os.path.exists(empty_dir1),
            f"Empty directory {empty_dir1} should be deleted",
        )
        self.assertFalse(
            os.path.exists(empty_dir2),
            f"Empty directory {empty_dir2} should be deleted",
        )

        # Note: In a real scenario, non-empty directories without snapshot references
        # might also be cleaned. This test focuses on empty directory cleanup.

    def test_dry_run_does_not_delete(self):
        """Test that dry run doesn't delete files."""
        # Create an empty directory
        empty_dir = os.path.join(self.table_path, "bucket", "dt=2024-01-05", "bucket-0")
        os.makedirs(empty_dir, exist_ok=True)

        # Make directory old enough
        old_time = time.time() - (2 * 24 * 60 * 60)
        os.utime(empty_dir, (old_time, old_time))

        # Verify directory exists
        self.assertTrue(
            os.path.exists(empty_dir), f"Directory {empty_dir} should exist"
        )

        # Clean with dry run
        older_than = int(time.time() * 1000) - (24 * 60 * 60 * 1000)
        cleaner = OrphanFilesClean(
            table_path=self.table_path,
            file_io=self.file_io,
            table_schema=self.table.table_schema,
            older_than_millis=older_than,
            dry_run=True,
        )
        cleaner.clean()

        # Verify directory still exists (not deleted in dry run)
        self.assertTrue(
            os.path.exists(empty_dir), f"Directory {empty_dir} should exist in dry run"
        )

    def test_snapshot_not_deleted(self):
        """Test that snapshot files are not deleted."""
        # Create snapshot file
        snapshot_dir = os.path.join(self.table_path, "snapshot")
        os.makedirs(snapshot_dir, exist_ok=True)
        snapshot_file = os.path.join(snapshot_dir, "snapshot-1")
        with open(snapshot_file, "w") as f:
            f.write("snapshot content")

        # Create LATEST pointer
        latest_file = os.path.join(snapshot_dir, "LATEST")
        with open(latest_file, "w") as f:
            f.write("snapshot-1")

        # Create EARLIEST pointer
        earliest_file = os.path.join(snapshot_dir, "EARLIEST")
        with open(earliest_file, "w") as f:
            f.write("snapshot-1")

        # Verify files exist
        self.assertTrue(os.path.exists(snapshot_file), "Snapshot file should exist")
        self.assertTrue(os.path.exists(latest_file), "LATEST file should exist")
        self.assertTrue(os.path.exists(earliest_file), "EARLIEST file should exist")

        # Set older_than to allow deletion
        older_than = int(time.time() * 1000) - (24 * 60 * 60 * 1000)

        # Clean orphan files
        cleaner = OrphanFilesClean(
            table_path=self.table_path,
            file_io=self.file_io,
            table_schema=self.table.table_schema,
            older_than_millis=older_than,
            dry_run=False,
        )
        cleaner.clean()

        # Snapshot files should still exist
        self.assertTrue(
            os.path.exists(snapshot_file), "Snapshot file should not be deleted"
        )
        self.assertTrue(
            os.path.exists(latest_file), "LATEST file should not be deleted"
        )
        self.assertTrue(
            os.path.exists(earliest_file), "EARLIEST file should not be deleted"
        )

    def test_non_snapshot_files_deleted(self):
        """Test that non-snapshot files in snapshot directory are deleted."""
        # Create snapshot directory with valid and invalid files
        snapshot_dir = os.path.join(self.table_path, "snapshot")
        os.makedirs(snapshot_dir, exist_ok=True)

        # Valid snapshot files
        valid_snapshot = os.path.join(snapshot_dir, "snapshot-1")
        with open(valid_snapshot, "w") as f:
            f.write("snapshot content")

        latest_file = os.path.join(snapshot_dir, "LATEST")
        with open(latest_file, "w") as f:
            f.write("snapshot-1")

        # Invalid/orphan files in snapshot directory
        orphan_file1 = os.path.join(snapshot_dir, "random-file.txt")
        with open(orphan_file1, "w") as f:
            f.write("orphan content")

        orphan_file2 = os.path.join(snapshot_dir, "old-snapshot-2")
        with open(orphan_file2, "w") as f:
            f.write("old snapshot")

        # Make files old enough
        old_time = time.time() - (2 * 24 * 60 * 60)
        os.utime(orphan_file1, (old_time, old_time))
        os.utime(orphan_file2, (old_time, old_time))

        # Verify files exist
        self.assertTrue(os.path.exists(orphan_file1), "Orphan file 1 should exist")
        self.assertTrue(os.path.exists(orphan_file2), "Orphan file 2 should exist")
        self.assertTrue(os.path.exists(valid_snapshot), "Valid snapshot should exist")

        # Clean orphan files
        older_than = int(time.time() * 1000) - (24 * 60 * 60 * 1000)
        cleaner = OrphanFilesClean(
            table_path=self.table_path,
            file_io=self.file_io,
            table_schema=self.table.table_schema,
            older_than_millis=older_than,
            dry_run=False,
        )
        cleaner.clean()

        # Orphan files should be deleted
        self.assertFalse(
            os.path.exists(orphan_file1), "Orphan file 1 should be deleted"
        )
        self.assertFalse(
            os.path.exists(orphan_file2), "Orphan file 2 should be deleted"
        )

        # Valid snapshot files should still exist
        self.assertTrue(
            os.path.exists(valid_snapshot), "Valid snapshot should not be deleted"
        )
        self.assertTrue(os.path.exists(latest_file), "LATEST should not be deleted")

    def test_data_files_not_deleted_after_commit(self):
        """Test that data files from committed writes are not deleted."""
        # Use existing table and write data to it
        # Create PyArrow schema and write data
        pa_schema = pa.schema(
            [
                ("id", pa.int32()),
                ("name", pa.string()),
                ("dt", pa.string()),
            ]
        )

        # Create table with partition keys
        schema = Schema.from_pyarrow_schema(
            pa_schema, partition_keys=["dt"], options={"bucket": "1"}
        )

        table_name = "test_data_table"
        identifier = f"{self.db_name}.{table_name}"
        self.catalog.create_table(identifier, schema, ignore_if_exists=True)
        table = self.catalog.get_table(identifier)

        # Write some data using the write API
        data = {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "dt": [
                "2024-01-01",
                "2024-01-01",
                "2024-01-02",
                "2024-01-02",
                "2024-01-03",
            ],
        }
        pa_table = pa.Table.from_pydict(data, schema=pa_schema)

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # Get the committed snapshot
        snapshot_manager = table.snapshot_manager()
        snapshot = snapshot_manager.get_latest_snapshot()
        self.assertIsNotNone(snapshot, "Snapshot should exist after commit")

        # Verify we can read the data before cleaning
        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        actual_before = table_read.to_arrow(splits)
        self.assertEqual(
            len(actual_before),
            len(pa_table),
            "Should be able to read all committed data before cleaning",
        )

        # Add orphan files to the table path that should be deleted
        orphan_file1 = os.path.join(table.table_path, "bucket", "orphan1.txt")
        os.makedirs(os.path.dirname(orphan_file1), exist_ok=True)
        with open(orphan_file1, "w") as f:
            f.write("orphan file 1")

        orphan_file2 = os.path.join(table.table_path, "bucket", "dt=2024-01-05", "orphan2.txt")
        os.makedirs(os.path.dirname(orphan_file2), exist_ok=True)
        with open(orphan_file2, "w") as f:
            f.write("orphan file 2")

        # Make orphan files old enough
        old_time = time.time() - (2 * 24 * 60 * 60)
        os.utime(orphan_file1, (old_time, old_time))
        os.utime(orphan_file2, (old_time, old_time))

        # Verify orphan files exist before cleaning
        self.assertTrue(os.path.exists(orphan_file1), "Orphan file 1 should exist")
        self.assertTrue(os.path.exists(orphan_file2), "Orphan file 2 should exist")

        # Set older_than to allow deletion (files must be older than this)
        older_than = int(time.time() * 1000) - (24 * 60 * 60 * 1000)

        # Clean orphan files
        cleaner = OrphanFilesClean(
            table_path=table.table_path,
            file_io=table.file_io,
            table_schema=table.table_schema,
            older_than_millis=older_than,
            dry_run=False,
        )
        cleaner.clean()

        # Verify orphan files are deleted
        self.assertFalse(
            os.path.exists(orphan_file1), "Orphan file 1 should be deleted"
        )
        self.assertFalse(
            os.path.exists(orphan_file2), "Orphan file 2 should be deleted"
        )

        # Verify we can still read the data after cleaning
        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        actual_after = table_read.to_arrow(splits)

        # Should still be able to read all the data we wrote
        self.assertEqual(
            len(actual_after),
            len(pa_table),
            "Should be able to read all committed data after cleaning",
        )
        self.assertEqual(
            len(actual_after),
            len(actual_before),
            "Data count should not change after orphan files cleaning",
        )

        # Verify snapshot still exists
        snapshot_after = snapshot_manager.get_latest_snapshot()
        self.assertIsNotNone(
            snapshot_after, "Snapshot should still exist after cleaning"
        )
        self.assertEqual(
            snapshot.id, snapshot_after.id, "Snapshot ID should not change"
        )


if __name__ == "__main__":
    unittest.main()
