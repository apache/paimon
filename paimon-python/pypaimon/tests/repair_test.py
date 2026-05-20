# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Unit tests for the repair operation."""

import json
import os
import shutil
import tempfile
import unittest

from pypaimon import CatalogFactory, Schema
from pypaimon.operation.repair import (  # noqa: F401
    RepairIssue,
    RepairReport,
    TableRepair,
    repair_table,
    repair_database,
    repair_catalog,
)
from pypaimon.schema.data_types import AtomicType, DataField


class TestRepairReport(unittest.TestCase):
    """Tests for RepairReport data class."""

    def test_empty_report_is_healthy(self):
        report = RepairReport(table_path="/test/path")
        self.assertTrue(report.is_healthy)
        self.assertFalse(report.has_errors)
        self.assertIn("HEALTHY", report.summary())

    def test_report_with_warning(self):
        report = RepairReport(table_path="/test/path")
        report.issues.append(RepairIssue(
            level="warning", category="snapshot", message="Something minor"
        ))
        self.assertFalse(report.is_healthy)
        self.assertFalse(report.has_errors)
        self.assertIn("WARN", report.summary())

    def test_report_with_error(self):
        report = RepairReport(table_path="/test/path")
        report.issues.append(RepairIssue(
            level="error", category="manifest_list", message="File missing",
            path="/some/path"
        ))
        self.assertFalse(report.is_healthy)
        self.assertTrue(report.has_errors)
        self.assertIn("ERROR", report.summary())
        self.assertIn("/some/path", report.summary())

    def test_report_summary_counts(self):
        report = RepairReport(table_path="/test/path")
        report.snapshots_checked = 3
        report.manifest_lists_checked = 6
        report.manifest_files_checked = 12
        report.data_files_checked = 0
        summary = report.summary()
        self.assertIn("Snapshots checked: 3", summary)
        self.assertIn("Manifest lists checked: 6", summary)
        self.assertIn("Manifest files checked: 12", summary)

    def test_report_with_fixes(self):
        report = RepairReport(table_path="/test/path")
        report.fixes_applied.append("Updated LATEST file")
        summary = report.summary()
        self.assertIn("Fixes applied: 1", summary)
        self.assertIn("Updated LATEST file", summary)


class TestTableRepairWithFilesystem(unittest.TestCase):
    """Integration tests for TableRepair with actual filesystem."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="repair_test_")
        self.warehouse = os.path.join(self.temp_dir, "warehouse")

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _create_catalog_and_table(self):
        """Helper to create a catalog with a test table containing data."""
        catalog = CatalogFactory.create({"warehouse": self.warehouse})
        catalog.create_database("test_db", False)

        schema = Schema(
            fields=[
                DataField(0, "id", AtomicType("INT")),
                DataField(1, "name", AtomicType("STRING")),
            ],
            partition_keys=[],
            primary_keys=["id"],
            options={},
            comment=""
        )
        catalog.create_table("test_db.test_table", schema, False)
        return catalog

    def test_repair_empty_table_no_snapshots(self):
        """A newly created table with no data should report 'no snapshots'."""
        catalog = self._create_catalog_and_table()
        report = catalog.repair_table("test_db.test_table")
        # Newly created tables have no snapshot dir content
        self.assertFalse(report.has_errors)

    def test_repair_healthy_table_with_snapshot(self):
        """A table with a valid snapshot should be reported as healthy."""
        catalog = self._create_catalog_and_table()
        catalog.get_table("test_db.test_table")  # noqa: F841

        # Create a minimal snapshot manually
        table_path = os.path.join(self.warehouse, "test_db.db", "test_table")
        snapshot_dir = os.path.join(table_path, "snapshot")
        manifest_dir = os.path.join(table_path, "manifest")
        os.makedirs(snapshot_dir, exist_ok=True)
        os.makedirs(manifest_dir, exist_ok=True)

        # Create empty manifest list files
        self._write_empty_avro(os.path.join(manifest_dir, "manifest-list-base-1"))
        self._write_empty_avro(os.path.join(manifest_dir, "manifest-list-delta-1"))

        # Create a snapshot
        snapshot_data = {
            "version": 3,
            "id": 1,
            "schemaId": 0,
            "baseManifestList": "manifest-list-base-1",
            "deltaManifestList": "manifest-list-delta-1",
            "totalRecordCount": 0,
            "deltaRecordCount": 0,
            "commitUser": "test",
            "commitIdentifier": 1,
            "commitKind": "APPEND",
            "timeMillis": 1000000
        }
        with open(os.path.join(snapshot_dir, "snapshot-1"), 'w') as f:
            json.dump(snapshot_data, f)

        # Write LATEST
        with open(os.path.join(snapshot_dir, "LATEST"), 'w') as f:
            f.write("1")

        report = catalog.repair_table("test_db.test_table")
        self.assertEqual(report.snapshots_checked, 1)
        self.assertEqual(report.manifest_lists_checked, 2)
        self.assertTrue(report.is_healthy)

    def test_repair_detects_missing_manifest_list(self):
        """Should detect when a manifest list referenced by snapshot is missing."""
        catalog = self._create_catalog_and_table()

        table_path = os.path.join(self.warehouse, "test_db.db", "test_table")
        snapshot_dir = os.path.join(table_path, "snapshot")
        manifest_dir = os.path.join(table_path, "manifest")
        os.makedirs(snapshot_dir, exist_ok=True)
        os.makedirs(manifest_dir, exist_ok=True)

        # Create only delta manifest list, not base
        self._write_empty_avro(os.path.join(manifest_dir, "manifest-list-delta-1"))

        snapshot_data = {
            "version": 3,
            "id": 1,
            "schemaId": 0,
            "baseManifestList": "manifest-list-base-1",
            "deltaManifestList": "manifest-list-delta-1",
            "totalRecordCount": 0,
            "deltaRecordCount": 0,
            "commitUser": "test",
            "commitIdentifier": 1,
            "commitKind": "APPEND",
            "timeMillis": 1000000
        }
        with open(os.path.join(snapshot_dir, "snapshot-1"), 'w') as f:
            json.dump(snapshot_data, f)
        with open(os.path.join(snapshot_dir, "LATEST"), 'w') as f:
            f.write("1")

        report = catalog.repair_table("test_db.test_table")
        self.assertTrue(report.has_errors)
        error_messages = [i.message for i in report.issues if i.level == "error"]
        self.assertTrue(any("manifest-list-base-1" in m or "missing" in m.lower()
                            for m in error_messages))

    def test_repair_detects_dangling_latest(self):
        """Should detect when LATEST points to a non-existent snapshot."""
        catalog = self._create_catalog_and_table()

        table_path = os.path.join(self.warehouse, "test_db.db", "test_table")
        snapshot_dir = os.path.join(table_path, "snapshot")
        os.makedirs(snapshot_dir, exist_ok=True)

        # LATEST points to snapshot-5, but only snapshot-1 exists
        manifest_dir = os.path.join(table_path, "manifest")
        os.makedirs(manifest_dir, exist_ok=True)
        self._write_empty_avro(os.path.join(manifest_dir, "manifest-list-base-1"))
        self._write_empty_avro(os.path.join(manifest_dir, "manifest-list-delta-1"))

        snapshot_data = {
            "version": 3,
            "id": 1,
            "schemaId": 0,
            "baseManifestList": "manifest-list-base-1",
            "deltaManifestList": "manifest-list-delta-1",
            "totalRecordCount": 0,
            "deltaRecordCount": 0,
            "commitUser": "test",
            "commitIdentifier": 1,
            "commitKind": "APPEND",
            "timeMillis": 1000000
        }
        with open(os.path.join(snapshot_dir, "snapshot-1"), 'w') as f:
            json.dump(snapshot_data, f)
        with open(os.path.join(snapshot_dir, "LATEST"), 'w') as f:
            f.write("5")

        report = catalog.repair_table("test_db.test_table")
        self.assertTrue(report.has_errors)
        error_messages = [i.message for i in report.issues if i.level == "error"]
        self.assertTrue(any("snapshot-5" in m for m in error_messages))

    def test_repair_fix_dangling_latest(self):
        """Should fix LATEST to point to the newest valid snapshot."""
        catalog = self._create_catalog_and_table()

        table_path = os.path.join(self.warehouse, "test_db.db", "test_table")
        snapshot_dir = os.path.join(table_path, "snapshot")
        manifest_dir = os.path.join(table_path, "manifest")
        os.makedirs(snapshot_dir, exist_ok=True)
        os.makedirs(manifest_dir, exist_ok=True)

        self._write_empty_avro(os.path.join(manifest_dir, "manifest-list-base-1"))
        self._write_empty_avro(os.path.join(manifest_dir, "manifest-list-delta-1"))

        snapshot_data = {
            "version": 3,
            "id": 1,
            "schemaId": 0,
            "baseManifestList": "manifest-list-base-1",
            "deltaManifestList": "manifest-list-delta-1",
            "totalRecordCount": 0,
            "deltaRecordCount": 0,
            "commitUser": "test",
            "commitIdentifier": 1,
            "commitKind": "APPEND",
            "timeMillis": 1000000
        }
        with open(os.path.join(snapshot_dir, "snapshot-1"), 'w') as f:
            json.dump(snapshot_data, f)
        with open(os.path.join(snapshot_dir, "LATEST"), 'w') as f:
            f.write("99")

        # Repair with fix=True
        report = catalog.repair_table("test_db.test_table", dry_run=False)
        self.assertTrue(len(report.fixes_applied) > 0)
        self.assertIn("snapshot-1", report.fixes_applied[0])

        # Verify LATEST is now correct
        with open(os.path.join(snapshot_dir, "LATEST"), 'r') as f:
            self.assertEqual(f.read().strip(), "1")

    def test_repair_corrupted_snapshot_file(self):
        """Should detect unreadable/corrupted snapshot files."""
        catalog = self._create_catalog_and_table()

        table_path = os.path.join(self.warehouse, "test_db.db", "test_table")
        snapshot_dir = os.path.join(table_path, "snapshot")
        os.makedirs(snapshot_dir, exist_ok=True)

        # Write garbage to the snapshot file
        with open(os.path.join(snapshot_dir, "snapshot-1"), 'w') as f:
            f.write("this is not valid json{{{")
        with open(os.path.join(snapshot_dir, "LATEST"), 'w') as f:
            f.write("1")

        report = catalog.repair_table("test_db.test_table")
        self.assertTrue(report.has_errors)
        error_messages = [i.message for i in report.issues if i.level == "error"]
        self.assertTrue(any("corrupted" in m.lower() or "unreadable" in m.lower()
                            for m in error_messages))

    def test_repair_database_level(self):
        """Should repair all tables in a database."""
        catalog = CatalogFactory.create({"warehouse": self.warehouse})
        catalog.create_database("mydb", False)

        schema = Schema(
            fields=[DataField(0, "id", AtomicType("INT"))],
            partition_keys=[],
            primary_keys=["id"],
            options={},
            comment=""
        )
        catalog.create_table("mydb.t1", schema, False)
        catalog.create_table("mydb.t2", schema, False)

        reports = catalog.repair_database("mydb")
        self.assertEqual(len(reports), 2)

    def test_repair_catalog_level(self):
        """Should repair all tables in all databases."""
        catalog = CatalogFactory.create({"warehouse": self.warehouse})
        catalog.create_database("db1", False)
        catalog.create_database("db2", False)

        schema = Schema(
            fields=[DataField(0, "id", AtomicType("INT"))],
            partition_keys=[],
            primary_keys=["id"],
            options={},
            comment=""
        )
        catalog.create_table("db1.t1", schema, False)
        catalog.create_table("db2.t2", schema, False)

        reports = catalog.repair_catalog()
        self.assertEqual(len(reports), 2)

    def test_dry_run_does_not_modify(self):
        """Dry run should not change any files."""
        catalog = self._create_catalog_and_table()

        table_path = os.path.join(self.warehouse, "test_db.db", "test_table")
        snapshot_dir = os.path.join(table_path, "snapshot")
        os.makedirs(snapshot_dir, exist_ok=True)

        with open(os.path.join(snapshot_dir, "LATEST"), 'w') as f:
            f.write("99")

        # Dry run - should not fix
        report = catalog.repair_table("test_db.test_table", dry_run=True)
        self.assertEqual(len(report.fixes_applied), 0)

        # Verify LATEST was NOT changed
        with open(os.path.join(snapshot_dir, "LATEST"), 'r') as f:
            self.assertEqual(f.read().strip(), "99")

    def _write_empty_avro(self, path: str):
        """Write an empty Avro file (valid manifest list with no records)."""
        import fastavro
        from io import BytesIO
        from pypaimon.manifest.schema.manifest_file_meta import MANIFEST_FILE_META_SCHEMA

        buffer = BytesIO()
        fastavro.writer(buffer, MANIFEST_FILE_META_SCHEMA, [])
        with open(path, 'wb') as f:
            f.write(buffer.getvalue())

    def _write_manifest_list_with_entry(self, path: str, manifest_file_name: str):
        """Write a manifest list Avro referencing a single manifest file."""
        import fastavro
        from io import BytesIO
        from pypaimon.manifest.schema.manifest_file_meta import MANIFEST_FILE_META_SCHEMA

        record = {
            "_VERSION": 1,
            "_FILE_NAME": manifest_file_name,
            "_FILE_SIZE": 100,
            "_NUM_ADDED_FILES": 1,
            "_NUM_DELETED_FILES": 0,
            "_PARTITION_STATS": {"_MIN_VALUES": b"", "_MAX_VALUES": b"", "_NULL_COUNTS": None},
            "_SCHEMA_ID": 0,
            "_MIN_ROW_ID": None,
            "_MAX_ROW_ID": None,
        }
        buffer = BytesIO()
        fastavro.writer(buffer, MANIFEST_FILE_META_SCHEMA, [record])
        with open(path, 'wb') as f:
            f.write(buffer.getvalue())

    def _write_manifest_with_data_file(self, path: str, partition_bytes: bytes,
                                       bucket: int, file_name: str,
                                       external_path=None, kind=0):
        """Write a manifest Avro with a single data file entry."""
        import fastavro
        from io import BytesIO
        from pypaimon.manifest.schema.manifest_entry import MANIFEST_ENTRY_SCHEMA

        record = {
            "_VERSION": 1,
            "_KIND": kind,
            "_PARTITION": partition_bytes,
            "_BUCKET": bucket,
            "_TOTAL_BUCKETS": 1,
            "_FILE": {
                "_FILE_NAME": file_name,
                "_FILE_SIZE": 1024,
                "_ROW_COUNT": 10,
                "_MIN_KEY": b"",
                "_MAX_KEY": b"",
                "_KEY_STATS": {"_MIN_VALUES": b"", "_MAX_VALUES": b"", "_NULL_COUNTS": None},
                "_VALUE_STATS": {"_MIN_VALUES": b"", "_MAX_VALUES": b"", "_NULL_COUNTS": None},
                "_MIN_SEQUENCE_NUMBER": 0,
                "_MAX_SEQUENCE_NUMBER": 0,
                "_SCHEMA_ID": 0,
                "_LEVEL": 0,
                "_EXTRA_FILES": [],
                "_CREATION_TIME": None,
                "_DELETE_ROW_COUNT": None,
                "_EMBEDDED_FILE_INDEX": None,
                "_FILE_SOURCE": None,
                "_VALUE_STATS_COLS": None,
                "_EXTERNAL_PATH": external_path,
                "_FIRST_ROW_ID": None,
                "_WRITE_COLS": None,
            }
        }
        buffer = BytesIO()
        fastavro.writer(buffer, MANIFEST_ENTRY_SCHEMA, [record])
        with open(path, 'wb') as f:
            f.write(buffer.getvalue())

    def _serialize_partition(self, values, fields):
        """Serialize partition values using GenericRowSerializer."""
        from pypaimon.table.row.generic_row import GenericRow, GenericRowSerializer
        row = GenericRow(values, fields)
        return GenericRowSerializer.to_bytes(row)

    def test_check_data_files_detects_missing(self):
        """check_data_files=True should detect missing data files."""
        catalog = self._create_catalog_and_table()

        table_path = os.path.join(self.warehouse, "test_db.db", "test_table")
        snapshot_dir = os.path.join(table_path, "snapshot")
        manifest_dir = os.path.join(table_path, "manifest")
        os.makedirs(snapshot_dir, exist_ok=True)
        os.makedirs(manifest_dir, exist_ok=True)

        # Non-partitioned table: partition is an empty row (0 fields)
        partition_bytes = self._serialize_partition([], [])

        # Write manifest file referencing a data file that doesn't exist
        self._write_manifest_with_data_file(
            os.path.join(manifest_dir, "manifest-1"),
            partition_bytes, bucket=0, file_name="data-abc.orc"
        )

        # Write manifest list referencing the manifest
        self._write_manifest_list_with_entry(
            os.path.join(manifest_dir, "manifest-list-base-1"), "manifest-1"
        )
        self._write_empty_avro(os.path.join(manifest_dir, "manifest-list-delta-1"))

        # Create snapshot
        snapshot_data = {
            "version": 3, "id": 1, "schemaId": 0,
            "baseManifestList": "manifest-list-base-1",
            "deltaManifestList": "manifest-list-delta-1",
            "totalRecordCount": 10, "deltaRecordCount": 0,
            "commitUser": "test", "commitIdentifier": 1,
            "commitKind": "APPEND", "timeMillis": 1000000
        }
        with open(os.path.join(snapshot_dir, "snapshot-1"), 'w') as f:
            json.dump(snapshot_data, f)
        with open(os.path.join(snapshot_dir, "LATEST"), 'w') as f:
            f.write("1")

        # Without check_data_files: should be healthy
        report = catalog.repair_table("test_db.test_table", check_data_files=False)
        self.assertEqual(report.data_files_checked, 0)
        self.assertFalse(report.has_errors)

        # With check_data_files: should find the missing data file
        report = catalog.repair_table("test_db.test_table", check_data_files=True)
        self.assertGreater(report.data_files_checked, 0)
        self.assertTrue(report.has_errors)
        data_file_issues = [i for i in report.issues if i.category == "data_file"]
        self.assertEqual(len(data_file_issues), 1)
        self.assertIn("data-abc.orc", data_file_issues[0].path)

    def test_check_data_files_skips_delete_entries(self):
        """DELETE entries for removed files should not be flagged as missing."""
        catalog = self._create_catalog_and_table()

        table_path = os.path.join(self.warehouse, "test_db.db", "test_table")
        snapshot_dir = os.path.join(table_path, "snapshot")
        manifest_dir = os.path.join(table_path, "manifest")
        os.makedirs(snapshot_dir, exist_ok=True)
        os.makedirs(manifest_dir, exist_ok=True)

        partition_bytes = self._serialize_partition([], [])

        # Write manifest with a DELETE entry (_KIND=1) for a file that doesn't exist
        self._write_manifest_with_data_file(
            os.path.join(manifest_dir, "manifest-1"),
            partition_bytes, bucket=0, file_name="deleted-file.orc",
            kind=1  # DELETE
        )
        self._write_manifest_list_with_entry(
            os.path.join(manifest_dir, "manifest-list-base-1"), "manifest-1"
        )
        self._write_empty_avro(os.path.join(manifest_dir, "manifest-list-delta-1"))

        snapshot_data = {
            "version": 3, "id": 1, "schemaId": 0,
            "baseManifestList": "manifest-list-base-1",
            "deltaManifestList": "manifest-list-delta-1",
            "totalRecordCount": 0, "deltaRecordCount": 0,
            "commitUser": "test", "commitIdentifier": 1,
            "commitKind": "APPEND", "timeMillis": 1000000
        }
        with open(os.path.join(snapshot_dir, "snapshot-1"), 'w') as f:
            json.dump(snapshot_data, f)
        with open(os.path.join(snapshot_dir, "LATEST"), 'w') as f:
            f.write("1")

        report = catalog.repair_table("test_db.test_table", check_data_files=True)
        self.assertEqual(report.data_files_checked, 0)
        self.assertFalse(report.has_errors)

    def test_check_data_files_passes_when_file_exists(self):
        """check_data_files=True should pass when data files exist."""
        catalog = self._create_catalog_and_table()

        table_path = os.path.join(self.warehouse, "test_db.db", "test_table")
        snapshot_dir = os.path.join(table_path, "snapshot")
        manifest_dir = os.path.join(table_path, "manifest")
        os.makedirs(snapshot_dir, exist_ok=True)
        os.makedirs(manifest_dir, exist_ok=True)

        partition_bytes = self._serialize_partition([], [])

        self._write_manifest_with_data_file(
            os.path.join(manifest_dir, "manifest-1"),
            partition_bytes, bucket=0, file_name="data-abc.orc"
        )
        self._write_manifest_list_with_entry(
            os.path.join(manifest_dir, "manifest-list-base-1"), "manifest-1"
        )
        self._write_empty_avro(os.path.join(manifest_dir, "manifest-list-delta-1"))

        # Create the data file at the expected location
        data_file_dir = os.path.join(table_path, "bucket-0")
        os.makedirs(data_file_dir, exist_ok=True)
        with open(os.path.join(data_file_dir, "data-abc.orc"), 'wb') as f:
            f.write(b"fake data")

        snapshot_data = {
            "version": 3, "id": 1, "schemaId": 0,
            "baseManifestList": "manifest-list-base-1",
            "deltaManifestList": "manifest-list-delta-1",
            "totalRecordCount": 10, "deltaRecordCount": 0,
            "commitUser": "test", "commitIdentifier": 1,
            "commitKind": "APPEND", "timeMillis": 1000000
        }
        with open(os.path.join(snapshot_dir, "snapshot-1"), 'w') as f:
            json.dump(snapshot_data, f)
        with open(os.path.join(snapshot_dir, "LATEST"), 'w') as f:
            f.write("1")

        report = catalog.repair_table("test_db.test_table", check_data_files=True)
        self.assertGreater(report.data_files_checked, 0)
        self.assertFalse(report.has_errors)

    def test_check_data_files_with_partition(self):
        """check_data_files should construct correct path for partitioned tables."""
        catalog = CatalogFactory.create({"warehouse": self.warehouse})
        catalog.create_database("test_db", False)

        schema = Schema(
            fields=[
                DataField(0, "id", AtomicType("INT")),
                DataField(1, "dt", AtomicType("STRING")),
                DataField(2, "name", AtomicType("STRING")),
            ],
            partition_keys=["dt"],
            primary_keys=["id"],
            options={},
            comment=""
        )
        catalog.create_table("test_db.part_table", schema, False)

        table_path = os.path.join(self.warehouse, "test_db.db", "part_table")
        snapshot_dir = os.path.join(table_path, "snapshot")
        manifest_dir = os.path.join(table_path, "manifest")
        os.makedirs(snapshot_dir, exist_ok=True)
        os.makedirs(manifest_dir, exist_ok=True)

        # Serialize partition with dt="2024-01-01"
        dt_field = DataField(1, "dt", AtomicType("STRING"))
        partition_bytes = self._serialize_partition(["2024-01-01"], [dt_field])

        self._write_manifest_with_data_file(
            os.path.join(manifest_dir, "manifest-1"),
            partition_bytes, bucket=0, file_name="data-part.orc"
        )
        self._write_manifest_list_with_entry(
            os.path.join(manifest_dir, "manifest-list-base-1"), "manifest-1"
        )
        self._write_empty_avro(os.path.join(manifest_dir, "manifest-list-delta-1"))

        # Create data file at partitioned path
        data_file_dir = os.path.join(table_path, "dt=2024-01-01", "bucket-0")
        os.makedirs(data_file_dir, exist_ok=True)
        with open(os.path.join(data_file_dir, "data-part.orc"), 'wb') as f:
            f.write(b"fake data")

        snapshot_data = {
            "version": 3, "id": 1, "schemaId": 0,
            "baseManifestList": "manifest-list-base-1",
            "deltaManifestList": "manifest-list-delta-1",
            "totalRecordCount": 10, "deltaRecordCount": 0,
            "commitUser": "test", "commitIdentifier": 1,
            "commitKind": "APPEND", "timeMillis": 1000000
        }
        with open(os.path.join(snapshot_dir, "snapshot-1"), 'w') as f:
            json.dump(snapshot_data, f)
        with open(os.path.join(snapshot_dir, "LATEST"), 'w') as f:
            f.write("1")

        report = catalog.repair_table("test_db.part_table", check_data_files=True)
        self.assertGreater(report.data_files_checked, 0)
        self.assertFalse(report.has_errors)

    def test_check_data_files_custom_partition_default_name(self):
        """Tables with custom partition.default-name use that value for null partitions."""
        catalog = CatalogFactory.create({"warehouse": self.warehouse})
        catalog.create_database("test_db", False)

        schema = Schema(
            fields=[
                DataField(0, "id", AtomicType("INT")),
                DataField(1, "region", AtomicType("STRING")),
                DataField(2, "name", AtomicType("STRING")),
            ],
            partition_keys=["region"],
            primary_keys=["id"],
            options={"partition.default-name": "UNSET"},
            comment=""
        )
        catalog.create_table("test_db.custom_part", schema, False)

        table_path = os.path.join(self.warehouse, "test_db.db", "custom_part")
        snapshot_dir = os.path.join(table_path, "snapshot")
        manifest_dir = os.path.join(table_path, "manifest")
        os.makedirs(snapshot_dir, exist_ok=True)
        os.makedirs(manifest_dir, exist_ok=True)

        # Serialize partition with region=None (null)
        region_field = DataField(1, "region", AtomicType("STRING"))
        partition_bytes = self._serialize_partition([None], [region_field])

        self._write_manifest_with_data_file(
            os.path.join(manifest_dir, "manifest-1"),
            partition_bytes, bucket=0, file_name="data-null.orc"
        )
        self._write_manifest_list_with_entry(
            os.path.join(manifest_dir, "manifest-list-base-1"), "manifest-1"
        )
        self._write_empty_avro(os.path.join(manifest_dir, "manifest-list-delta-1"))

        # Create data file at custom default partition path
        data_file_dir = os.path.join(table_path, "region=UNSET", "bucket-0")
        os.makedirs(data_file_dir, exist_ok=True)
        with open(os.path.join(data_file_dir, "data-null.orc"), 'wb') as f:
            f.write(b"fake data")

        snapshot_data = {
            "version": 3, "id": 1, "schemaId": 0,
            "baseManifestList": "manifest-list-base-1",
            "deltaManifestList": "manifest-list-delta-1",
            "totalRecordCount": 10, "deltaRecordCount": 0,
            "commitUser": "test", "commitIdentifier": 1,
            "commitKind": "APPEND", "timeMillis": 1000000
        }
        with open(os.path.join(snapshot_dir, "snapshot-1"), 'w') as f:
            json.dump(snapshot_data, f)
        with open(os.path.join(snapshot_dir, "LATEST"), 'w') as f:
            f.write("1")

        report = catalog.repair_table("test_db.custom_part", check_data_files=True)
        self.assertGreater(report.data_files_checked, 0)
        self.assertFalse(report.has_errors)

    def test_repair_branch_table(self):
        """Repairing a branch-qualified identifier checks the branch snapshot dir."""
        catalog = self._create_catalog_and_table()
        catalog.get_table("test_db.test_table")  # noqa: F841

        table_path = os.path.join(self.warehouse, "test_db.db", "test_table")
        manifest_dir = os.path.join(table_path, "manifest")
        os.makedirs(manifest_dir, exist_ok=True)

        # Create main branch snapshot (healthy)
        main_snapshot_dir = os.path.join(table_path, "snapshot")
        os.makedirs(main_snapshot_dir, exist_ok=True)
        self._write_empty_avro(os.path.join(manifest_dir, "manifest-list-base-main"))
        self._write_empty_avro(os.path.join(manifest_dir, "manifest-list-delta-main"))
        snapshot_main = {
            "version": 3, "id": 1, "schemaId": 0,
            "baseManifestList": "manifest-list-base-main",
            "deltaManifestList": "manifest-list-delta-main",
            "totalRecordCount": 0, "deltaRecordCount": 0,
            "commitUser": "test", "commitIdentifier": 1,
            "commitKind": "APPEND", "timeMillis": 1000000
        }
        with open(os.path.join(main_snapshot_dir, "snapshot-1"), 'w') as f:
            json.dump(snapshot_main, f)
        with open(os.path.join(main_snapshot_dir, "LATEST"), 'w') as f:
            f.write("1")

        # Create branch "b1" with a dangling LATEST
        branch_snapshot_dir = os.path.join(table_path, "branch", "branch-b1", "snapshot")
        os.makedirs(branch_snapshot_dir, exist_ok=True)
        self._write_empty_avro(os.path.join(manifest_dir, "manifest-list-base-b1"))
        self._write_empty_avro(os.path.join(manifest_dir, "manifest-list-delta-b1"))
        snapshot_b1 = {
            "version": 3, "id": 1, "schemaId": 0,
            "baseManifestList": "manifest-list-base-b1",
            "deltaManifestList": "manifest-list-delta-b1",
            "totalRecordCount": 0, "deltaRecordCount": 0,
            "commitUser": "test", "commitIdentifier": 1,
            "commitKind": "APPEND", "timeMillis": 1000000
        }
        with open(os.path.join(branch_snapshot_dir, "snapshot-1"), 'w') as f:
            json.dump(snapshot_b1, f)
        # LATEST points to non-existent snapshot-99
        with open(os.path.join(branch_snapshot_dir, "LATEST"), 'w') as f:
            f.write("99")

        # Repair main branch - should be healthy
        report_main = catalog.repair_table("test_db.test_table")
        self.assertFalse(report_main.has_errors)

        # Repair branch b1 - should detect dangling LATEST
        report_branch = catalog.repair_table("test_db.test_table$branch_b1")
        self.assertTrue(report_branch.has_errors)
        error_messages = [i.message for i in report_branch.issues if i.level == "error"]
        self.assertTrue(any("snapshot-99" in m for m in error_messages))

    def test_fix_latest_respects_check_data_files(self):
        """_fix_latest_file should not select a snapshot with missing data files
        when check_data_files=True."""
        catalog = self._create_catalog_and_table()

        table_path = os.path.join(self.warehouse, "test_db.db", "test_table")
        snapshot_dir = os.path.join(table_path, "snapshot")
        manifest_dir = os.path.join(table_path, "manifest")
        os.makedirs(snapshot_dir, exist_ok=True)
        os.makedirs(manifest_dir, exist_ok=True)

        partition_bytes = self._serialize_partition([], [])

        # Snapshot 1: healthy (empty manifests)
        self._write_empty_avro(os.path.join(manifest_dir, "manifest-list-base-1"))
        self._write_empty_avro(os.path.join(manifest_dir, "manifest-list-delta-1"))
        snapshot1 = {
            "version": 3, "id": 1, "schemaId": 0,
            "baseManifestList": "manifest-list-base-1",
            "deltaManifestList": "manifest-list-delta-1",
            "totalRecordCount": 0, "deltaRecordCount": 0,
            "commitUser": "test", "commitIdentifier": 1,
            "commitKind": "APPEND", "timeMillis": 1000000
        }
        with open(os.path.join(snapshot_dir, "snapshot-1"), 'w') as f:
            json.dump(snapshot1, f)

        # Snapshot 2: references a missing data file
        self._write_manifest_with_data_file(
            os.path.join(manifest_dir, "manifest-2"),
            partition_bytes, bucket=0, file_name="missing-file.orc"
        )
        self._write_manifest_list_with_entry(
            os.path.join(manifest_dir, "manifest-list-base-2"), "manifest-2"
        )
        self._write_empty_avro(os.path.join(manifest_dir, "manifest-list-delta-2"))
        snapshot2 = {
            "version": 3, "id": 2, "schemaId": 0,
            "baseManifestList": "manifest-list-base-2",
            "deltaManifestList": "manifest-list-delta-2",
            "totalRecordCount": 10, "deltaRecordCount": 10,
            "commitUser": "test", "commitIdentifier": 2,
            "commitKind": "APPEND", "timeMillis": 2000000
        }
        with open(os.path.join(snapshot_dir, "snapshot-2"), 'w') as f:
            json.dump(snapshot2, f)

        # LATEST points to non-existent snapshot-99
        with open(os.path.join(snapshot_dir, "LATEST"), 'w') as f:
            f.write("99")

        # Repair with check_data_files=True: should skip snapshot-2 and pick snapshot-1
        report = catalog.repair_table("test_db.test_table",
                                      check_data_files=True, dry_run=False)
        self.assertTrue(any("snapshot-1" in fix for fix in report.fixes_applied))

        with open(os.path.join(snapshot_dir, "LATEST"), 'r') as f:
            self.assertEqual(f.read().strip(), "1")


if __name__ == '__main__':
    unittest.main()
