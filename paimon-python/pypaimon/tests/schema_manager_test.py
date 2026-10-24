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

import os
import tempfile
import unittest

from pypaimon.branch.branch_manager import BranchManager
from pypaimon.common.identifier import DEFAULT_MAIN_BRANCH
from pypaimon.common.file_io import FileIO
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.schema.data_types import (ArrayType, AtomicType, DataField,
                                        MapType, RowType)
from pypaimon.schema.schema import Schema
from pypaimon.schema.schema_change import SetOption
from pypaimon.schema.schema_manager import SchemaManager
from pypaimon.schema.table_schema import TableSchema


class TestSchemaManagerBranch(unittest.TestCase):
    """Test cases for SchemaManager branch support."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.file_io = FileIO.get(self.temp_dir, Options({}))
        self.table_path = f"{self.temp_dir}/test_db.db/test_table"

    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        if hasattr(self, 'temp_dir') and os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_default_branch(self):
        """Test that SchemaManager uses default branch by default."""
        schema_manager = SchemaManager(self.file_io, self.table_path)
        self.assertEqual(schema_manager.branch, DEFAULT_MAIN_BRANCH)
        # branch path should be table path for main branch
        self.assertEqual(schema_manager.branch_path, self.table_path)

    def test_custom_branch(self):
        """Test that SchemaManager can be created with a custom branch."""
        branch_name = "feature-branch"
        schema_manager = SchemaManager(self.file_io, self.table_path, branch_name)
        self.assertEqual(schema_manager.branch, branch_name)
        # branch path should include branch name
        expected_branch_path = BranchManager.branch_path(
            self.table_path, branch_name
        )
        self.assertEqual(schema_manager.branch_path, expected_branch_path)

    def test_copy_with_branch(self):
        """Test that SchemaManager.copy_with_branch creates a new manager for a different branch."""
        # Create manager with main branch
        main_manager = SchemaManager(self.file_io, self.table_path)
        self.assertEqual(main_manager.branch, DEFAULT_MAIN_BRANCH)

        # Create manager for feature branch
        feature_branch = "feature-branch"
        feature_manager = main_manager.copy_with_branch(feature_branch)
        self.assertEqual(feature_manager.branch, feature_branch)
        # Original manager should still be on main branch
        self.assertEqual(main_manager.branch, DEFAULT_MAIN_BRANCH)

    def test_branch_path_for_main(self):
        """Test branch_path property for main branch."""
        schema_manager = SchemaManager(self.file_io, self.table_path)
        self.assertEqual(schema_manager.branch_path, self.table_path)

    def test_branch_path_for_custom_branch(self):
        """Test branch_path property for custom branch."""
        branch_name = "test-branch"
        schema_manager = SchemaManager(self.file_io, self.table_path, branch_name)
        expected_path = f"{self.table_path}/branch/branch-{branch_name}"
        self.assertEqual(schema_manager.branch_path, expected_path)


class TestBlobPartitionValidation(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.file_io = FileIO.get(self.temp_dir, Options({}))

    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @staticmethod
    def _schema(blob_type):
        return Schema(
            fields=[
                DataField(0, "id", AtomicType("INT")),
                DataField(1, "payload", blob_type),
            ],
            partition_keys=["payload"],
            options={
                "row-tracking.enabled": "true",
                "data-evolution.enabled": "true",
            },
        )

    def test_create_table_rejects_blob_partition_key(self):
        blob_types = [
            AtomicType("BLOB"),
            ArrayType(True, AtomicType("BLOB")),
            MapType(
                True,
                AtomicType("STRING", False),
                AtomicType("BLOB"),
            ),
        ]
        for index, blob_type in enumerate(blob_types):
            with self.subTest(blob_type=blob_type):
                manager = SchemaManager(
                    self.file_io,
                    f"{self.temp_dir}/test_db.db/create_{index}",
                )
                with self.assertRaisesRegex(
                    ValueError,
                    "can not be part of partition keys",
                ):
                    manager.create_table(self._schema(blob_type))

    def test_commit_rejects_array_blob_partition_key(self):
        manager = SchemaManager(
            self.file_io,
            f"{self.temp_dir}/test_db.db/commit",
        )
        table_schema = TableSchema.from_schema(
            0,
            self._schema(ArrayType(True, AtomicType("BLOB"))),
        )
        with self.assertRaisesRegex(
            ValueError,
            "can not be part of partition keys",
        ):
            manager.commit(table_schema)

    def test_create_table_allows_unsupported_map_blob_key(self):
        manager = SchemaManager(
            self.file_io,
            f"{self.temp_dir}/test_db.db/unsupported_map_key",
        )
        schema = Schema(
            fields=[
                DataField(0, "id", AtomicType("INT")),
                DataField(
                    1,
                    "payload",
                    MapType(
                        True,
                        AtomicType("BOOLEAN", False),
                        AtomicType("BLOB"),
                    ),
                ),
            ],
            options={
                "row-tracking.enabled": "true",
                "data-evolution.enabled": "true",
            },
        )

        table_schema = manager.create_table(schema)
        self.assertEqual(table_schema.fields[1].type, schema.fields[1].type)

    def test_create_table_rejects_unsupported_nested_blob(self):
        nested_types = [
            MapType(True, AtomicType("BLOB", False), AtomicType("INT")),
            RowType(
                True,
                [DataField(2, "nested_blob", AtomicType("BLOB"))],
            ),
            ArrayType(True, ArrayType(True, AtomicType("BLOB"))),
            MapType(
                True,
                AtomicType("STRING", False),
                ArrayType(True, AtomicType("BLOB")),
            ),
        ]

        for index, nested_type in enumerate(nested_types):
            with self.subTest(nested_type=nested_type):
                manager = SchemaManager(
                    self.file_io,
                    f"{self.temp_dir}/test_db.db/nested_blob_{index}",
                )
                schema = Schema(
                    fields=[
                        DataField(0, "id", AtomicType("INT")),
                        DataField(1, "payload", nested_type),
                    ],
                )
                with self.assertRaisesRegex(
                    ValueError,
                    "unsupported nested BLOB type",
                ):
                    manager.create_table(schema)

    def test_create_primary_key_blob_tables(self):
        blob_types = [
            ("scalar", AtomicType("BLOB"), {}),
            ("array", ArrayType(True, AtomicType("BLOB")), {}),
            (
                "map",
                MapType(
                    True,
                    AtomicType("STRING", False),
                    AtomicType("BLOB"),
                ),
                {},
            ),
            (
                "descriptor",
                AtomicType("BLOB"),
                {"blob-descriptor-field": "payload", "changelog-producer": "input"},
            ),
            (
                "view",
                AtomicType("BLOB"),
                {"blob-view-field": "payload", "changelog-producer": "input"},
            ),
            (
                "partial_update_ignore_retract",
                AtomicType("BLOB"),
                {
                    "merge-engine": "partial-update",
                    "fields.ts.sequence-group": "payload",
                    "fields.payload.aggregate-function": "last_non_null_value",
                    "fields.payload.ignore-retract": "true",
                },
            ),
            (
                "partial_update_ignore_delete",
                AtomicType("BLOB"),
                {
                    "merge-engine": "partial-update",
                    "fields.ts.sequence-group": "payload",
                    "fields.payload.aggregate-function": "last_non_null_value",
                    "partial-update.ignore-delete": "true",
                },
            ),
            (
                "partial_update_last_value",
                AtomicType("BLOB"),
                {
                    "merge-engine": "partial-update",
                    "fields.ts.sequence-group": "payload",
                    "fields.payload.aggregate-function": "last_value",
                },
            ),
            (
                "partial_update_inline_blob_agg",
                AtomicType("BLOB"),
                {
                    "merge-engine": "partial-update",
                    "blob-descriptor-field": "payload",
                    "fields.ts.sequence-group": "payload",
                    "fields.payload.aggregate-function": "last_non_null_value",
                },
            ),
        ]
        for suffix, blob_type, extra_options in blob_types:
            with self.subTest(kind=suffix):
                manager = SchemaManager(
                    self.file_io,
                    f"{self.temp_dir}/test_db.db/pk_blob_{suffix}",
                )
                schema = Schema(
                    fields=[
                        DataField(0, "id", AtomicType("INT", False)),
                        DataField(1, "ts", AtomicType("INT", False)),
                        DataField(2, "payload", blob_type),
                    ],
                    primary_keys=["id"],
                    options=extra_options,
                )
                table_schema = manager.create_table(schema)
                self.assertEqual(table_schema.primary_keys, ["id"])
                self.assertEqual(table_schema.fields[2].type, blob_type)

    def test_create_primary_key_blob_table_validation(self):
        cases = [
            (
                "row_tracking_with_pk",
                [
                    DataField(0, "id", AtomicType("INT", False)),
                    DataField(1, "payload", AtomicType("BLOB")),
                ],
                ["id"],
                {
                    "row-tracking.enabled": "true",
                    "data-evolution.enabled": "true",
                },
                "Cannot define primary-key for row tracking table.",
            ),
            (
                "video_frame_on_pk",
                [
                    DataField(0, "id", AtomicType("INT", False)),
                    DataField(1, "payload", AtomicType("BLOB")),
                ],
                ["id"],
                {"video-frame-field": "payload"},
                "'video-frame-field' only supports append-only tables.",
            ),
            (
                "managed_blob_in_pk",
                [
                    DataField(0, "id", AtomicType("INT", False)),
                    DataField(1, "payload", AtomicType("BLOB", False)),
                ],
                ["id", "payload"],
                {},
                r"Managed BLOB fields cannot be primary keys: \['payload'\]\.",
            ),
            (
                "managed_blob_in_bucket_key",
                [
                    DataField(0, "id", AtomicType("INT", False)),
                    DataField(1, "payload", AtomicType("BLOB")),
                ],
                ["id"],
                {"bucket-key": "payload"},
                r"Managed BLOB fields cannot be bucket keys: \['payload'\]\.",
            ),
            (
                "managed_blob_in_sequence_field",
                [
                    DataField(0, "id", AtomicType("INT", False)),
                    DataField(1, "payload", AtomicType("BLOB")),
                ],
                ["id"],
                {"sequence.field": "payload"},
                r"Managed BLOB fields cannot be sequence fields: \['payload'\]\.",
            ),
            (
                "inline_blob_in_pk",
                [
                    DataField(0, "id", AtomicType("INT", False)),
                    DataField(1, "payload", AtomicType("BLOB", False)),
                ],
                ["id", "payload"],
                {"blob-descriptor-field": "payload"},
                "can not be part of primary keys",
            ),
            (
                "inline_blob_in_bucket_key",
                [
                    DataField(0, "id", AtomicType("INT", False)),
                    DataField(1, "payload", AtomicType("BLOB")),
                ],
                ["id"],
                {"blob-view-field": "payload", "bucket-key": "payload"},
                "can not be part of bucket keys",
            ),
            (
                "managed_blob_invalid_merge_engine",
                [
                    DataField(0, "id", AtomicType("INT", False)),
                    DataField(1, "payload", AtomicType("BLOB")),
                ],
                ["id"],
                {"merge-engine": "aggregation"},
                "Primary-key managed BLOB tables only support the deduplicate, "
                "partial-update or first-row merge engine.",
            ),
            (
                "managed_blob_invalid_changelog_producer",
                [
                    DataField(0, "id", AtomicType("INT", False)),
                    DataField(1, "payload", AtomicType("BLOB")),
                ],
                ["id"],
                {"changelog-producer": "input"},
                "Primary-key managed BLOB tables only support changelog-producer 'none'.",
            ),
            (
                "managed_blob_external_paths",
                [
                    DataField(0, "id", AtomicType("INT", False)),
                    DataField(1, "payload", AtomicType("BLOB")),
                ],
                ["id"],
                {"data-file.external-paths": "file:///tmp/ext"},
                "Primary-key managed BLOB tables do not support 'data-file.external-paths'.",
            ),
            (
                "managed_blob_pk_clustering_override",
                [
                    DataField(0, "id", AtomicType("INT", False)),
                    DataField(1, "payload", AtomicType("BLOB")),
                ],
                ["id"],
                {"pk-clustering-override": "true"},
                "Primary-key managed BLOB tables do not support 'pk-clustering-override'.",
            ),
            (
                "managed_blob_partial_update_retract_agg",
                [
                    DataField(0, "id", AtomicType("INT", False)),
                    DataField(1, "ts", AtomicType("INT", False)),
                    DataField(2, "payload", AtomicType("BLOB")),
                ],
                ["id"],
                {
                    "merge-engine": "partial-update",
                    "fields.ts.sequence-group": "payload",
                    "fields.payload.aggregate-function": "last_non_null_value",
                },
                r"Managed BLOB field 'payload' cannot use aggregate function 'last_non_null_value' "
                r"because managed BLOB payloads are not retained in retract messages\. "
                r"Set 'fields\.payload\.ignore-retract' to true to ignore retract messages\.",
            ),
            (
                "managed_blob_partial_update_default_agg",
                [
                    DataField(0, "id", AtomicType("INT", False)),
                    DataField(1, "ts", AtomicType("INT", False)),
                    DataField(2, "payload", AtomicType("BLOB")),
                ],
                ["id"],
                {
                    "merge-engine": "partial-update",
                    "fields.ts.sequence-group": "payload",
                    "fields.default-aggregate-function": "last_non_null_value",
                },
                r"Managed BLOB field 'payload' cannot use aggregate function 'last_non_null_value' "
                r"because managed BLOB payloads are not retained in retract messages\. "
                r"Set 'fields\.payload\.ignore-retract' to true to ignore retract messages\.",
            ),
            (
                "managed_blob_as_sequence_group_ordering_field",
                [
                    DataField(0, "id", AtomicType("INT", False)),
                    DataField(1, "ts", AtomicType("INT", False)),
                    DataField(2, "payload", AtomicType("BLOB")),
                ],
                ["id"],
                {
                    "merge-engine": "partial-update",
                    "fields.payload.sequence-group": "ts",
                },
                r"Field 'payload' with type BLOB cannot be used as a sequence-group ordering "
                r"field in option 'fields\.payload\.sequence-group'\.",
            ),
            (
                "inline_blob_as_sequence_group_ordering_field",
                [
                    DataField(0, "id", AtomicType("INT", False)),
                    DataField(1, "ts", AtomicType("INT", False)),
                    DataField(2, "payload", AtomicType("BLOB")),
                ],
                ["id"],
                {
                    "merge-engine": "partial-update",
                    "blob-descriptor-field": "payload",
                    "fields.payload.sequence-group": "ts",
                },
                r"Field 'payload' with type BLOB cannot be used as a sequence-group ordering "
                r"field in option 'fields\.payload\.sequence-group'\.",
            ),
            (
                "pk_as_sequence_group_ordering_field",
                [
                    DataField(0, "id", AtomicType("INT", False)),
                    DataField(1, "ts", AtomicType("INT", False)),
                    DataField(2, "payload", AtomicType("BLOB")),
                ],
                ["id"],
                {
                    "merge-engine": "partial-update",
                    "fields.id.sequence-group": "payload",
                },
                r"The sequence-group 'fields\.id\.sequence-group' contains primary key field "
                r"'id', which is not allowed\. Primary key columns cannot be put in "
                r"sequence-group\.",
            ),
            (
                "pk_as_sequence_group_protected_field",
                [
                    DataField(0, "id", AtomicType("INT", False)),
                    DataField(1, "ts", AtomicType("INT", False)),
                    DataField(2, "payload", AtomicType("BLOB")),
                ],
                ["id"],
                {
                    "merge-engine": "partial-update",
                    "fields.ts.sequence-group": "id,payload",
                },
                r"The sequence-group 'fields\.ts\.sequence-group' contains primary key field "
                r"'id', which is not allowed\. Primary key columns cannot be put in "
                r"sequence-group\.",
            ),
        ]

        for name, fields, pks, opts, error_regex in cases:
            with self.subTest(case=name):
                manager = SchemaManager(
                    self.file_io,
                    f"{self.temp_dir}/test_db.db/pk_blob_invalid_{name}",
                )
                schema = Schema(
                    fields=fields,
                    primary_keys=pks,
                    options=opts,
                )
                with self.assertRaisesRegex(ValueError, error_regex):
                    manager.create_table(schema)


class TestOptionValidation(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.file_io = FileIO.get(self.temp_dir, Options({}))
        self.table_path = f"{self.temp_dir}/test_db.db/test_table"

    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @staticmethod
    def _schema(options=None):
        return Schema(
            fields=[DataField(0, "id", AtomicType("INT"))],
            options=options or {},
        )

    def test_create_rejects_invalid_target_file_row_num(self):
        key = CoreOptions.TARGET_FILE_ROW_NUM.key()
        max_value = CoreOptions.TARGET_FILE_ROW_NUM.default_value()
        manager = SchemaManager(self.file_io, self.table_path)

        invalid_values = [
            ("0", "should be at least 1"),
            (str(max_value + 1), f"should be at most {max_value}"),
        ]
        for value, message in invalid_values:
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, f"{key} {message}"):
                    manager.create_table(self._schema({key: value}))

    def test_alter_rejects_invalid_target_file_row_num(self):
        key = CoreOptions.TARGET_FILE_ROW_NUM.key()
        max_value = CoreOptions.TARGET_FILE_ROW_NUM.default_value()
        manager = SchemaManager(self.file_io, self.table_path)
        manager.create_table(self._schema())

        invalid_values = [
            ("-1", "should be at least 1"),
            (str(max_value + 1), f"should be at most {max_value}"),
        ]
        for value, message in invalid_values:
            with self.subTest(value=value):
                with self.assertRaisesRegex(RuntimeError, f"{key} {message}"):
                    manager.commit_changes([SetOption(key, value)])

        self.assertEqual(manager.latest().id, 0)
        self.assertNotIn(key, manager.latest().options)


class TestRowTrackingValidation(unittest.TestCase):
    """Row tracking tables must follow Java SchemaValidation#validateRowTracking."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.file_io = FileIO.get(self.temp_dir, Options({}))
        self.table_path = f"{self.temp_dir}/test_db.db/test_table"

    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @staticmethod
    def _schema(options=None, primary_keys=None):
        return Schema(
            fields=[
                DataField(0, "id", AtomicType("INT", False)),
                DataField(1, "name", AtomicType("STRING")),
            ],
            primary_keys=primary_keys or [],
            options=options or {},
        )

    def test_create_rejects_row_tracking_with_fixed_or_postpone_bucket(self):
        manager = SchemaManager(self.file_io, self.table_path)
        for bucket in ["1", "16", "-2"]:
            with self.subTest(bucket=bucket):
                with self.assertRaisesRegex(
                    ValueError,
                    "Cannot define bucket for row tracking table, "
                    "it only support bucket = -1",
                ):
                    manager.create_table(self._schema({
                        "row-tracking.enabled": "true",
                        "bucket": bucket,
                    }))
        self.assertIsNone(manager.latest())

    def test_create_rejects_row_tracking_with_primary_key(self):
        manager = SchemaManager(self.file_io, self.table_path)
        with self.assertRaisesRegex(
            ValueError, "Cannot define primary-key for row tracking table"
        ):
            manager.create_table(self._schema(
                {"row-tracking.enabled": "true"}, primary_keys=["id"]))
        self.assertIsNone(manager.latest())

    def test_create_allows_row_tracking_with_unaware_bucket(self):
        for options in [
            {"row-tracking.enabled": "true"},
            {"row-tracking.enabled": "true", "bucket": "-1"},
            {"row-tracking.enabled": "true", "data-evolution.enabled": "true"},
        ]:
            with self.subTest(options=options):
                temp_dir = tempfile.mkdtemp(dir=self.temp_dir)
                manager = SchemaManager(self.file_io, f"{temp_dir}/t")
                manager.create_table(self._schema(options))
                self.assertEqual(manager.latest().id, 0)

    def test_create_rejects_data_evolution_without_row_tracking(self):
        manager = SchemaManager(self.file_io, self.table_path)
        for options in [
            {"data-evolution.enabled": "true"},
            {"data-evolution.enabled": "true", "row-tracking.enabled": "false"},
        ]:
            with self.subTest(options=options):
                with self.assertRaisesRegex(
                    ValueError,
                    "Data evolution config must enabled with row-tracking.enabled",
                ):
                    manager.create_table(self._schema(options))
        self.assertIsNone(manager.latest())

    def test_create_allows_fixed_bucket_without_row_tracking(self):
        manager = SchemaManager(self.file_io, self.table_path)
        manager.create_table(self._schema({"bucket": "4"}, primary_keys=["id"]))
        self.assertEqual(manager.latest().id, 0)

    def test_alter_rejects_enabling_row_tracking_on_fixed_bucket_table(self):
        manager = SchemaManager(self.file_io, self.table_path)
        manager.create_table(self._schema({"bucket": "4"}, primary_keys=["id"]))

        with self.assertRaisesRegex(
            RuntimeError, "Cannot define primary-key for row tracking table"
        ):
            manager.commit_changes([SetOption("row-tracking.enabled", "true")])

        self.assertEqual(manager.latest().id, 0)
        self.assertNotIn("row-tracking.enabled", manager.latest().options)


if __name__ == '__main__':
    unittest.main()
