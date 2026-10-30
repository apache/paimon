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

from pypaimon.branch.branch_manager import BranchManager
from pypaimon.common.identifier import DEFAULT_MAIN_BRANCH
from pypaimon.common.file_io import FileIO
from pypaimon.common.options.core_options import CoreOptions, MergeEngine
from pypaimon.common.options.options import Options
from pypaimon.schema.data_types import (ArrayType, AtomicType, DataField,
                                        MapType, RowType)
from pypaimon.schema.schema import Schema
from pypaimon.schema.schema_change import RemoveOption, SetOption
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

    def test_primary_key_video_frame_field_is_rejected(self):
        fields = [DataField(0, 'id', AtomicType('INT', False)), DataField(1, 'payload', AtomicType('BLOB'))]
        manager = SchemaManager(self.file_io, self.temp_dir + '/video')
        with self.assertRaisesRegex(ValueError, 'only supports append-only tables'):
            manager.create_table(Schema(fields=fields, primary_keys=['id'], options={
                'bucket': '1', 'video-frame-field': 'payload'}))
        self.assertIsNone(manager.latest())
        manager.create_table(Schema(fields=fields, primary_keys=['id'], options={'bucket': '1'}))
        with self.assertRaisesRegex(RuntimeError, 'only supports append-only tables'):
            manager.commit_changes([SetOption('video-frame-field', 'payload')])
        self.assertEqual(manager.latest().id, 0)
        self.assertNotIn('video-frame-field', manager.latest().options)

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

    def test_create_table_accepts_primary_key_map_blob(self):
        manager = SchemaManager(
            self.file_io,
            f"{self.temp_dir}/test_db.db/pk_map_blob",
        )
        schema = Schema(
            fields=[
                DataField(0, "id", AtomicType("INT", False)),
                DataField(
                    1,
                    "payload",
                    MapType(
                        True,
                        AtomicType("STRING", False),
                        AtomicType("BLOB"),
                    ),
                ),
            ],
            primary_keys=["id"],
            options={"bucket": "1"},
        )

        manager.create_table(schema)
        self.assertEqual(manager.latest().primary_keys, ["id"])

    def test_create_table_rejects_managed_blob_primary_key(self):
        manager = SchemaManager(
            self.file_io,
            f"{self.temp_dir}/test_db.db/pk_blob_key",
        )
        schema = Schema(
            fields=[
                DataField(0, "id", AtomicType("INT", False)),
                DataField(1, "payload", AtomicType("BLOB")),
            ],
            primary_keys=["payload", "id"],
            options={"bucket": "1"},
        )

        with self.assertRaisesRegex(ValueError, "cannot be primary keys"):
            manager.create_table(schema)


class TestPartialUpdateBlobValidation(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.file_io = FileIO.get(self.temp_dir, Options({}))

    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @staticmethod
    def _schema(options, payload_type=None):
        return Schema(fields=[
            DataField(0, 'id', AtomicType('INT', False)),
            DataField(1, 'payload', payload_type or AtomicType('BLOB')),
            DataField(2, 'ts', AtomicType('INT')),
        ], primary_keys=['id'], options=dict({'bucket': '1', 'merge-engine': 'partial-update'}, **options))

    def test_rejects_blob_sequence_group_ordering(self):
        types = [AtomicType('BLOB'), ArrayType(True, AtomicType('BLOB')),
                 MapType(True, AtomicType('STRING', False), AtomicType('BLOB'))]
        for index, payload_type in enumerate(types):
            for ordering in ('payload', 'ts,payload'):
                with self.subTest(payload_type=payload_type, ordering=ordering):
                    manager = SchemaManager(self.file_io, '%s/ordering_%s_%s' % (
                        self.temp_dir, index, ordering))
                    options = {'fields.%s.sequence-group' % ordering: 'ts'}
                    with self.assertRaisesRegex(ValueError, 'sequence-group ordering field'):
                        manager.create_table(self._schema(options, payload_type))
                    self.assertIsNone(manager.latest())
        for inline in ('blob-descriptor-field', 'blob-view-field'):
            manager = SchemaManager(self.file_io, self.temp_dir + '/' + inline)
            options = {inline: 'payload', 'fields.payload.sequence-group': 'ts'}
            with self.assertRaisesRegex(ValueError, 'sequence-group ordering field'):
                manager.create_table(self._schema(options))
            self.assertIsNone(manager.latest())

    def test_managed_blob_aggregate_retract_validation(self):
        for key in ('fields.payload.aggregate-function', 'fields.default-aggregate-function'):
            for index, extra in enumerate(({}, {'fields.payload.ignore-retract': 'true'},
                                           {'partial-update.ignore-delete': 'true'},
                                           {key: 'last_value'},
                                           {'blob-descriptor-field': 'payload'})):
                with self.subTest(key=key, extra=extra):
                    manager = SchemaManager(self.file_io, '%s/%s_%s' % (self.temp_dir, key, index))
                    options = {'fields.ts.sequence-group': 'payload', key: 'last_non_null_value'}
                    options.update(extra)
                    if extra:
                        manager.create_table(self._schema(options))
                        self.assertIsNotNone(manager.latest())
                    else:
                        with self.assertRaisesRegex(ValueError, 'fields.payload.ignore-retract'):
                            manager.create_table(self._schema(options))
                        self.assertIsNone(manager.latest())

    def test_alter_rejects_invalid_blob_options_without_persisting(self):
        cases = [
            ({}, {'fields.payload.sequence-group': 'ts'}, 'sequence-group ordering field'),
            ({'fields.ts.sequence-group': 'payload'},
             {'fields.payload.aggregate-function': 'last_non_null_value'}, 'ignore-retract'),
            ({'fields.ts.sequence-group': 'payload', 'fields.default-aggregate-function': 'last_non_null_value',
              'fields.payload.ignore-retract': 'true'},
             {'fields.payload.ignore-retract': 'false'}, 'ignore-retract'),
        ]
        for index, (options, changes, message) in enumerate(cases):
            with self.subTest(options=options, changes=changes):
                manager = SchemaManager(self.file_io, '%s/alter_%s' % (self.temp_dir, index))
                manager.create_table(self._schema(options))
                original = manager.latest().options.copy()
                with self.assertRaisesRegex(RuntimeError, message):
                    manager.commit_changes([SetOption(key, value) for key, value in changes.items()])
                self.assertEqual(manager.latest().id, 0)
                self.assertEqual(manager.latest().options, original)

    def test_managed_blob_ignore_retract_requires_java_boolean(self):
        for index, value in enumerate(('yes', '1', ' true ', 'on', '0', 'no', 'false ', 'TRUE', 'TrUe')):
            with self.subTest(value=value):
                manager = SchemaManager(self.file_io, '%s/boolean_%s' % (self.temp_dir, index))
                options = {'fields.ts.sequence-group': 'payload',
                           'fields.payload.aggregate-function': 'last_non_null_value',
                           'fields.payload.ignore-retract': value}
                if value.lower() == 'true':
                    manager.create_table(self._schema(options))
                    self.assertIsNotNone(manager.latest())
                else:
                    with self.assertRaisesRegex(ValueError, 'fields.payload.ignore-retract'):
                        manager.create_table(self._schema(options))
                    self.assertIsNone(manager.latest())

    def test_accepts_unprotected_and_inactive_sequence_group_options(self):
        for index, options in enumerate((
                {'fields.payload.aggregate-function': 'last_non_null_value'},
                {'fields.ts.sequence-group': 'payload'},
                {'merge-engine': 'deduplicate', 'fields.payload.sequence-group': 'ts'})):
            manager = SchemaManager(self.file_io, '%s/valid_%s' % (self.temp_dir, index))
            manager.create_table(self._schema(options))
            self.assertIsNotNone(manager.latest())


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


if __name__ == '__main__':
    unittest.main()
