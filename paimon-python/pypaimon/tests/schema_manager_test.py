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

from pypaimon.catalog.catalog_exception import ColumnNotExistException
from pypaimon.branch.branch_manager import BranchManager
from pypaimon.common.identifier import DEFAULT_MAIN_BRANCH
from pypaimon.common.file_io import FileIO
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.schema.data_types import (ArrayType, AtomicType, DataField,
                                        MapType, RowType)
from pypaimon.schema.default_value import validate_default_value
from pypaimon.schema.schema import Schema
from pypaimon.schema.schema_change import SetOption, UpdateColumnDefaultValue
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

    def test_create_table_still_rejects_primary_key_map_blob(self):
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
            options={
                "row-tracking.enabled": "true",
                "data-evolution.enabled": "true",
            },
        )

        with self.assertRaisesRegex(
            ValueError,
            "MAP<X, BLOB> type is not supported with primary key",
        ):
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
