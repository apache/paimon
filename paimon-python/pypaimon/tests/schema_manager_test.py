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
from unittest.mock import patch

from pypaimon.catalog.catalog_exception import ColumnNotExistException
from pypaimon.branch.branch_manager import BranchManager
from pypaimon.common.identifier import DEFAULT_MAIN_BRANCH
from pypaimon.common.file_io import FileIO
from pypaimon.common.options.core_options import CoreOptions, MergeEngine
from pypaimon.common.options.options import Options
from pypaimon.schema.data_types import (ArrayType, AtomicType, DataField,
                                        MapType, RowType)
from pypaimon.schema.default_value import validate_default_value
from pypaimon.schema.schema import Schema
from pypaimon.schema.schema_change import (RemoveOption, SetOption,
                                           UpdateColumnDefaultValue)
from pypaimon.schema.schema_manager import SchemaManager
from pypaimon.schema.table_schema import TableSchema
from pypaimon.snapshot.snapshot_manager import SnapshotManager


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


class TestAlterTableOptions(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.file_io = FileIO.get(self.temp_dir.name, Options({}))
        self.manager = SchemaManager(self.file_io, self.temp_dir.name)

    def _alter(self, options, changes, has_snapshots=True):
        schema = TableSchema.from_schema(0, Schema(
            fields=[DataField(0, "id", AtomicType("INT"))], options=options))
        with patch.object(SnapshotManager, "get_latest_snapshot",
                          return_value=object() if has_snapshots else None):
            return self.manager._generate_table_schema(schema, changes).options

    def _assert_rejected(self, options, changes, message):
        with self.assertRaises(NotImplementedError) as ctx:
            self._alter(options, changes)
        self.assertEqual(str(ctx.exception), message)

    def test_immutable_options(self):
        for key in CoreOptions.IMMUTABLE_OPTIONS | {"bucket-function.provider"}:
            with self.subTest(key=key):
                old_value = "false" if key == "primary-key.nullable" else "old"
                for options, change in [
                    ({}, SetOption(key, "new")),
                    ({key: old_value}, SetOption(key, "new")),
                    ({}, RemoveOption(key)),
                    ({key: old_value}, RemoveOption(key)),
                ]:
                    self._assert_rejected(options, [change], f"Change '{key}' is not supported yet.")
                self.assertEqual(self._alter({key: old_value}, [SetOption(key, old_value)]), {key: old_value})

    def test_bucket_changes(self):
        for old_value, new_value, message in [
            (None, "4", "Cannot change bucket when it is -1."),
            (None, "-1", "Cannot change bucket when it is -1."),
            ("-1", "4", "Cannot change bucket when it is -1."),
            ("-1", "-01", "Cannot change bucket when it is -1."),
            ("4", "-1", "Cannot change bucket to -1."),
        ]:
            with self.subTest(old=old_value, new=new_value):
                options = {} if old_value is None else {"bucket": old_value}
                self._assert_rejected(options, [SetOption("bucket", new_value)], message)
        for options in [{}, {"bucket": "-1"}, {"bucket": "4"}]:
            self._assert_rejected(options, [RemoveOption("bucket")], "Cannot reset bucket.")
        for old_value, new_value in [
            ("-1", "-1"), ("4", "8"), ("4", "+4"), ("-2", "4"),
            ("4", "2147483647"), ("4", "-2147483648"), ("4", "\uff14"), ("4", "0" * 5000 + "4"),
        ]:
            self.assertEqual(
                self._alter({"bucket": old_value}, [SetOption("bucket", new_value)]),
                {"bucket": new_value})

    def test_bucket_requires_java_integer(self):
        for value in ["", "+", "invalid", " 4", "4 ", "4_0", "1.0", "2147483648", "-2147483649", "\U0001d7dc"]:
            for old_value, new_value in [("4", value), (value, "4")]:
                with self.subTest(old=old_value, new=new_value):
                    with self.assertRaises(ValueError) as ctx:
                        self._alter({"bucket": old_value}, [SetOption("bucket", new_value)])
                    self.assertEqual(str(ctx.exception), f'For input string: "{value}"')

    def test_deletion_vectors_changes(self):
        key = "deletion-vectors.enabled"
        for old_value, new_value in [(None, "true"), ("false", "true"), ("true", "false"), ("true", None)]:
            for modifiable in [None, "false", "no"]:
                options = {} if old_value is None else {key: old_value}
                if modifiable is not None:
                    options["deletion-vectors.modifiable"] = modifiable
                message = (
                    f"Cannot change deletion vectors mode from {old_value or 'false'} to {new_value or 'false'}. "
                    "If modifying table deletion-vectors mode without full-compaction, "
                    "this may result in data duplication. If you are confident, you can set table option "
                    "'deletion-vectors.modifiable' = 'true' to allow deletion vectors modification.")
                changes = [SetOption(key, new_value)]
                if new_value == "false":
                    changes.append(RemoveOption(key))
                for change in changes:
                    with self.subTest(old=old_value, new=new_value, modifiable=modifiable, change=change):
                        self._assert_rejected(options, [change], message)
        for old_value, new_value in [(None, "false"), ("false", "FALSE"), ("true", "TRUE")]:
            options = {} if old_value is None else {key: old_value}
            self.assertEqual(self._alter(options, [SetOption(key, new_value)])[key], new_value)
        for options in [{}, {key: "false"}]:
            self.assertEqual(self._alter(options, [RemoveOption(key)]), {})
        for change in [SetOption(key, "true"), SetOption(key, "false"), RemoveOption(key)]:
            for old_value in ["false", "true"]:
                options = {key: old_value, "deletion-vectors.modifiable": "TRUE"}
                result = self._alter(options, [change])
                self.assertEqual(result.get(key), getattr(change, "value", None))

    def test_options_cannot_be_disabled(self):
        for key, message in [
            ("ignore-delete", "Cannot change ignore-delete from true to false."),
            ("ignore-update-before", "Cannot change ignore-update-before from true to false."),
            ("data-evolution.nested-field.enabled",
             "Cannot disable table option 'data-evolution.nested-field.enabled'."),
        ]:
            with self.subTest(key=key):
                for change in [SetOption(key, "false"), SetOption(key, None), SetOption(key, "no"),
                               RemoveOption(key)]:
                    self._assert_rejected({key: "TRUE"}, [change], message)
                for old_value in [None, "false", "true"]:
                    options = {} if old_value is None else {key: old_value}
                    self.assertEqual(self._alter(options, [SetOption(key, "true")]), {key: "true"})
                for options in [{}, {key: "false"}]:
                    self.assertEqual(self._alter(options, [RemoveOption(key)]), {})

    def test_clustering_columns(self):
        key = "clustering.columns"
        for override in ["true", "false"]:
            for old_value in [None, "id"]:
                options = {"pk-clustering-override": override}
                if old_value is not None:
                    options[key] = old_value
                self._assert_rejected(options, [SetOption(key, "name")],
                                      "Cannot change clustering.columns when pk-clustering-override enabled.")
                self._assert_rejected(options, [RemoveOption(key)],
                                      "Cannot reset clustering.columns when pk-clustering-override enabled.")
            self.assertEqual(self._alter(options, [SetOption(key, "id")]), options)
        self.assertEqual(self._alter({}, [SetOption(key, "id")]), {key: "id"})
        self.assertEqual(self._alter({key: "id"}, [RemoveOption(key)]), {})

    def test_guards_require_snapshots(self):
        for options, key, value in [
            ({"bucket": "-1"}, "bucket", "4"),
            ({"bucket": "4"}, "bucket", "-1"),
            ({"deletion-vectors.enabled": "true"}, "deletion-vectors.enabled", "false"),
            ({"ignore-delete": "true"}, "ignore-delete", "false"),
            ({"ignore-update-before": "true"}, "ignore-update-before", "false"),
            ({"data-evolution.nested-field.enabled": "true"}, "data-evolution.nested-field.enabled", "false"),
            ({"pk-clustering-override": "true", "clustering.columns": "id"}, "clustering.columns", "name"),
            ({"bucket-function.provider": "old"}, "bucket-function.provider", "new"),
        ]:
            with self.subTest(key=key):
                self.assertEqual(self._alter(options, [SetOption(key, value)], False)[key], value)
                self.assertNotIn(key, self._alter(options, [RemoveOption(key)], False))
        for change in [SetOption("type", "format-table"), RemoveOption("type")]:
            with self.assertRaisesRegex(NotImplementedError, "Change 'type' is not supported yet."):
                self._alter({}, [change], False)

    def test_checks_accept_native_option_values(self):
        self.assertEqual(self._alter({"bucket": 2}, [SetOption("bucket", "4")]), {"bucket": "4"})
        self.assertEqual(self._alter({"bucket": -1}, [SetOption("bucket", "-1")]), {"bucket": -1})
        self.assertEqual(self._alter({"ignore-delete": True}, [SetOption("ignore-delete", "true")]),
                         {"ignore-delete": "true"})
        self.assertEqual(self._alter({"partition.default-name": True}, [SetOption("partition.default-name", "true")]),
                         {"partition.default-name": "true"})
        self.assertEqual(self._alter({"deletion-vectors.enabled": "yes"}, [SetOption("deletion-vectors.enabled", "1")]),
                         {"deletion-vectors.enabled": "1"})
        with self.assertRaisesRegex(NotImplementedError, "Cannot change deletion vectors mode"):
            self._alter({"deletion-vectors.enabled": "false"}, [SetOption("deletion-vectors.enabled", "yes")])
        dv = "deletion-vectors.enabled"
        self.assertEqual(self._alter({dv: 1.0}, [SetOption(dv, "true")]), {dv: "true"})
        self.assertEqual(self._alter({"ignore-update-before": "yes"}, [SetOption("ignore-update-before", "false")]),
                         {"ignore-update-before": "false"})
        self.assertEqual(self._alter({"merge-engine": "first-row"}, [SetOption("merge-engine", MergeEngine.FIRST_ROW)]),
                         {"merge-engine": "first-row"})
        with self.assertRaisesRegex(NotImplementedError, "Cannot change ignore-delete from true to false"):
            self._alter({"ignore-delete": True}, [SetOption("ignore-delete", "false")])
        options = {"deletion-vectors.modifiable": True}
        self.assertEqual(self._alter(options, [SetOption("deletion-vectors.enabled", "true")]),
                         {"deletion-vectors.modifiable": True, "deletion-vectors.enabled": "true"})

    def test_checks_use_original_options(self):
        key = "deletion-vectors.enabled"
        modifiable = "deletion-vectors.modifiable"
        for changes in [
            [SetOption(modifiable, "true"), SetOption(key, "true")],
            [SetOption(modifiable, "true"), RemoveOption(key)],
        ]:
            options = {key: "true"} if isinstance(changes[1], RemoveOption) else {}
            with self.assertRaisesRegex(NotImplementedError, "Cannot change deletion vectors mode"):
                self._alter(options, changes)
        self.assertEqual(self._alter({modifiable: "true"}, [RemoveOption(modifiable), SetOption(key, "true")]),
                         {key: "true"})
        self.assertEqual(self._alter({}, [SetOption("ignore-delete", "true"), SetOption("ignore-delete", "false")]),
                         {"ignore-delete": "false"})
        self.assertEqual(self._alter({"bucket": "4"}, [SetOption("bucket", "8"), SetOption("bucket", "4")]),
                         {"bucket": "4"})

    def test_normalized_unchanged_options(self):
        for options in [{}, {"type": "table"}, {"type": "TABLE"}]:
            self.assertEqual(self._alter(options, [SetOption("type", "TaBlE")]), options)
        schema = TableSchema.from_schema(0, Schema(
            fields=[DataField(0, "id", AtomicType("INT"))], primary_keys=["id"], partition_keys=["id"]))
        with patch.object(SnapshotManager, "get_latest_snapshot", return_value=object()):
            result = self.manager._generate_table_schema(schema, [
                SetOption("primary-key", " id, "), SetOption("partition", " id, ")])
        self.assertEqual(result.options, schema.options)


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


class TestUpdateColumnDefaultValue(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.file_io = FileIO.get(self.temp_dir, Options({}))
        self.table_path = f"{self.temp_dir}/test_db.db/defaults"
        self.manager = SchemaManager(self.file_io, self.table_path)
        self.manager.create_table(Schema(fields=[
            DataField(0, "id", AtomicType("INT")),
            DataField(1, "name", AtomicType("STRING")),
            DataField(2, "info", RowType(True, [
                DataField(3, "uid", AtomicType("INT")),
            ])),
            DataField(4, "values", ArrayType(True, AtomicType("INT"))),
        ]))

    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _field(self, name):
        for field in self.manager.latest().fields:
            if field.name == name:
                return field
        raise AssertionError(name)

    def test_sets_and_clears_default_value(self):
        updated = self.manager.commit_changes([
            UpdateColumnDefaultValue(["id"], "10"),
            UpdateColumnDefaultValue(["name"], "'hello'"),
        ])
        self.assertEqual("10", updated.fields[0].default_value)
        self.assertEqual("'hello'", updated.fields[1].default_value)

        cleared = self.manager.commit_changes([
            UpdateColumnDefaultValue(["id"], None),
        ])
        self.assertIsNone(cleared.fields[0].default_value)
        self.assertEqual("'hello'", self._field("name").default_value)

    def test_sets_nested_field_default(self):
        updated = self.manager.commit_changes([
            UpdateColumnDefaultValue(["info", "uid"], "7"),
        ])
        info = updated.fields[2]
        self.assertEqual("7", info.type.fields[0].default_value)
        self.assertIsNone(info.default_value)

    def test_rejects_value_that_does_not_match_type(self):
        with self.assertRaisesRegex(RuntimeError, "Unsupported default value `nope`"):
            self.manager.commit_changes([
                UpdateColumnDefaultValue(["id"], "nope"),
            ])
        self.assertIsNone(self._field("id").default_value)
        self.assertEqual(0, self.manager.latest().id)

    def test_rejects_missing_column(self):
        with self.assertRaises(ColumnNotExistException):
            self.manager.commit_changes([
                UpdateColumnDefaultValue(["missing"], "1"),
            ])

    def test_accepts_scalar_defaults_java_accepts(self):
        cases = [
            ("BOOLEAN", "yes"),
            ("TINYINT", "127"),
            ("TINYINT", "-128"),
            ("SMALLINT", "32767"),
            ("INT", "10."),
            ("INT", ".5"),
            ("BIGINT", "9223372036854775807"),
            ("DECIMAL(10, 2)", "12345"),
            ("DECIMAL(10, 2)", "+1.25e2"),
            ("DOUBLE", "0x1.0p0"),
            ("FLOAT", "1.25f"),
            ("DOUBLE", "NaN"),
            ("DOUBLE", "-Infinity"),
            ("DOUBLE", " 1.5 "),
            ("DATE", "2024-1-1"),
            ("DATE", "0"),
            ("DATE", "2147483647"),
            ("DATE", "2024-01"),
            ("DATE", "2024-01-01 12:00:00"),
            ("DATE", "0000-02-29"),
            ("TIME", "1:2:3"),
            ("TIME", "0"),
            ("TIME", "2147483647"),
            ("TIMESTAMP(3)", "2024-01-01"),
            ("TIMESTAMP(9)", "2024-01-01 12:00:00.123456789"),
            ("TIMESTAMP(3)", "2024-1-1 1:2:3"),
            ("TIMESTAMP(3)", "1700000000000"),
            ("TIMESTAMP(3)", "9223372036854775807"),
            # The epoch path supports precision 0, 3, 6 and 9; a bare
            # TIMESTAMP has precision 6.
            ("TIMESTAMP(0)", "1700000000000"),
            ("TIMESTAMP(9)", "1700000000000"),
            ("TIMESTAMP", "1700000000000"),
            ("TIMESTAMP(3) NOT NULL", "1700000000000"),
            # Other precisions still take string timestamps.
            ("TIMESTAMP(2)", "2024-01-01 12:00:00.12"),
            ("STRING", "hello"),
            ("STRING", "''"),
            ("STRING", "'abc"),
            # Epoch fast paths: StringUtils.isNumeric allows a leading '-'.
            ("DATE", "-1"),
            ("DATE", "-2147483648"),
            ("TIME", "-1"),
            ("TIMESTAMP(3)", "-100"),
            ("TIMESTAMP(3)", "-9223372036854775808"),
            # DateTimeUtils.parseTime does not bound the components and takes
            # an optional timezone suffix.
            ("TIME", "24:00:00"),
            ("TIME", "99:00:00"),
            ("TIME", "12:99:00"),
            ("TIME", "12:00:99"),
            ("TIME", "12"),
            ("TIME", "12:30"),
            ("TIME", "12:00:00+01:00"),
            ("TIME", "12:30+01"),
            ("TIME", "12+01"),
            ("TIME", "12:00:00.9999999999"),
            # appendFraction has a minimum width of 0, so a bare '.' parses.
            ("TIME", "12:00:00."),
            ("TIMESTAMP(3)", "2024-01-01 12:00:00."),
            ("TIMESTAMP(3)", "2024-01-01T12:00"),
            # The SMART resolver clamps the day to the end of the month.
            ("TIMESTAMP(3)", "2024-02-30"),
            ("TIMESTAMP(3)", "2023-02-29"),
            ("TIMESTAMP(3)", "2024-04-31"),
            # It also reads 24:00 as midnight of the following day.
            ("TIMESTAMP(3)", "2024-01-01 24:00:00"),
            ("TIMESTAMP(3)", "2024-12-31 24:00:00.000"),
            ("TIMESTAMP(3)", "2024-02-31 24:00:00."),
            ("TIMESTAMP(3)", "2024-01-01T24:00"),
            ("TIMESTAMP_LTZ(3)", "2024-01-01 24:00:00"),
            ("TIMESTAMP(3) WITH LOCAL TIME ZONE", "2024-01-01 24:00:00"),
            # 'yyyy' takes more than four digits only after a '+'.
            ("TIMESTAMP(3)", "+12345-01-01"),
            ("TIMESTAMP(3)", "+02024-01-01"),
            ("TIMESTAMP(3)", "+999999999-12-31"),
            ("TIMESTAMP(3)", "+999999999-12-30 24:00:00"),
            # BigDecimal accepts Unicode digits, and out-of-precision values
            # become a null default rather than failing.
            ("DECIMAL(10, 2)", "１２.５"),
            ("DECIMAL(10, 2)", "12345678901"),
            ("DECIMAL(10, 2)", "1e100000"),
            ("DECIMAL(10, 2)", "1e-100000"),
            # A zero never needs a power of ten to rescale.
            ("DECIMAL(10, 2)", "0e-2147483645"),
            ("DECIMAL(10, 2)", "0e2147483645"),
            ("DECIMAL(10, 2)", "1e" + "0" * 5000 + "2"),
        ]
        for type_name, value in cases:
            with self.subTest(type_name=type_name, value=value):
                validate_default_value(AtomicType(type_name), value)

    def test_rejects_scalar_defaults_java_rejects(self):
        cases = [
            ("TINYINT", "128"),
            ("SMALLINT", "32768"),
            ("INT", "2147483648"),
            ("INT", " 10 "),
            ("INT", "10\n"),
            ("INT", "１２"),
            ("DOUBLE", "1_000"),
            ("FLOAT", "inf"),
            ("DOUBLE", "nan"),
            ("DOUBLE", "\u00a01.5\u00a0"),
            ("DECIMAL(10, 2)", "NaN"),
            ("DECIMAL(10, 2)", "Infinity"),
            ("DECIMAL(10, 2)", "1_000"),
            ("DECIMAL(10, 2)", " 1.25 "),
            # BigDecimal(String) needs the exponent and the scale in int range.
            ("DECIMAL(10, 2)", "1e2147483648"),
            ("DECIMAL(10, 2)", "1e-2147483648"),
            ("DECIMAL(10, 2)", "1.5e-2147483647"),
            ("DECIMAL(10, 2)", "0e-2147483648"),
            ("DECIMAL(10, 2)", "1e12345678901"),
            # setScale cannot build the power of ten needed to rescale these.
            ("DECIMAL(10, 2)", "1e2147483647"),
            ("DECIMAL(10, 2)", "1e-2147483646"),
            ("DECIMAL(10, 2)", "1e536870918"),
            ("DECIMAL(10, 2)", "1e-536870922"),
            ("DATE", "2147483648"),
            ("DATE", "2024-13"),
            ("DATE", "2023-02-29 12:00:00"),
            ("TIME", "2147483648"),
            # parseTime searches the whole string for ':', so the offset's
            # colon is taken as the second separator.
            ("TIME", "12:30+01:00"),
            ("TIME", "12+01:00"),
            ("TIME", "12:30:00+01:00:00"),
            ("TIMESTAMP(3)", "9223372036854775808"),
            ("TIMESTAMP(3)", " 1700000000000 "),
            ("TIMESTAMP(3)", "not-a-timestamp"),
            ("TIMESTAMP_LTZ(3)", "1700000000000"),
            # The type string Java writes for TIMESTAMP_LTZ has no epoch path.
            ("TIMESTAMP(3) WITH LOCAL TIME ZONE", "1700000000000"),
            # isNumeric rejects a bare sign, a '+' sign and out-of-range values.
            ("DATE", "+1"),
            ("DATE", "-"),
            ("DATE", "-2147483649"),
            ("TIMESTAMP(3)", "+1"),
            ("TIMESTAMP(3)", "-9223372036854775809"),
            # fromMillisToTimestamp throws for any other precision.
            ("TIMESTAMP(2)", "1700000000000"),
            ("TIMESTAMP(1)", "-1"),
            ("TIMESTAMP(7)", "0"),
            # Java strips a lone quote with substring(1, 0), which throws.
            ("STRING", "'"),
            ("CHAR(1)", "'"),
            ("BYTES", "'"),
            # TIMESTAMP still bounds month, day and the time components, and
            # the formatter reads 'yyyy' as an era year, so 0000 fails.
            ("TIMESTAMP(3)", "2024-13-01"),
            ("TIMESTAMP(3)", "2024-01-32"),
            ("TIMESTAMP(3)", "2024-01-00"),
            ("TIMESTAMP(3)", "0000-01-01"),
            ("TIMESTAMP(3)", "2024-01-01 25:00:00"),
            ("TIMESTAMP(3)", "2024-01-01 12:60:00"),
            ("TIMESTAMP(3)", "2024-01-01 12:00:60"),
            # A space form needs hour:minute:second, 'T' needs hour:minute.
            ("TIMESTAMP(3)", "2024-01-01 12:00"),
            ("TIMESTAMP(3)", "2024-01"),
            ("TIMESTAMP(3)", "2024-01-01\n"),
            ("TIMESTAMP(3)", "2024-01-01 12:00:00.9999999999"),
            # 24:00 is only midnight, and not past the last representable day.
            ("TIMESTAMP(3)", "2024-01-01 24:00:01"),
            ("TIMESTAMP(3)", "2024-01-01 24:00:00.001"),
            ("TIMESTAMP(3)", "2024-01-01T24:01"),
            ("TIMESTAMP(3)", "+999999999-12-31 24:00:00"),
            # Years need '+' to exceed four digits and must fit an era year.
            ("TIMESTAMP(3)", "12345-01-01"),
            ("TIMESTAMP(3)", "+2024-01-01"),
            ("TIMESTAMP(3)", "-2024-01-01"),
            ("TIMESTAMP(3)", "+00000-01-01"),
            ("TIMESTAMP(3)", "+1000000000-01-01"),
            ("TIMESTAMP(3)", "+00000000000000002024-01-01"),
            # The formatter only reads ASCII digits.
            ("TIMESTAMP(3)", "２０２４-０１-０１ ００:００:００"),
            ("TIMESTAMP(3)", "2024-01-01 ００:００:００"),
            ("TIMESTAMP(3)", "2024-01-01 00:00:00.５"),
            ("TIMESTAMP(3)", "٢٠٢٤-٠١-٠١"),
            ("TIMESTAMP_LTZ(3)", "２０２４-０１-０１ ００:００:００"),
        ]
        for type_name, value in cases:
            with self.subTest(type_name=type_name, value=value):
                with self.assertRaisesRegex(
                        RuntimeError, "Unsupported default value"):
                    validate_default_value(AtomicType(type_name), value)

    def test_invalid_defaults_do_not_commit_schema(self):
        cases = [
            ("INT", " 10 "),
            ("DOUBLE", "1_000"),
            ("DECIMAL(10, 2)", "NaN"),
            ("TIMESTAMP(3)", "9223372036854775808"),
            ("TIME", "12:30+01:00"),
            ("TIMESTAMP(3)", "２０２４-０１-０１ ００:００:００"),
            ("DECIMAL(10, 2)", "1e2147483648"),
            ("TIMESTAMP(2)", "1700000000000"),
            ("STRING", "'"),
        ]
        manager = SchemaManager(self.file_io, f"{self.temp_dir}/invalid_defaults")
        manager.create_table(Schema(fields=[
            DataField(i, f"c{i}", AtomicType(type_name))
            for i, (type_name, _) in enumerate(cases)
        ]))
        for i, (type_name, value) in enumerate(cases):
            with self.subTest(type_name=type_name, value=value):
                with self.assertRaisesRegex(RuntimeError, "Unsupported default value"):
                    manager.commit_changes([
                        UpdateColumnDefaultValue([f"c{i}"], value),
                    ])
                self.assertEqual(0, manager.latest().id)
                self.assertIsNone(manager.latest().fields[i].default_value)

    def test_rejects_collection_defaults(self):
        with self.assertRaisesRegex(RuntimeError, "scalar defaults only"):
            self.manager.commit_changes([
                UpdateColumnDefaultValue(["values"], "[1, 2]"),
            ])
        self.assertIsNone(self._field("values").default_value)
        self.assertEqual(0, self.manager.latest().id)


if __name__ == '__main__':
    unittest.main()
