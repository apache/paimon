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
from pypaimon.common.options.options import Options
from pypaimon.schema.data_types import (ArrayType, AtomicType, DataField,
                                        MapType, RowType)
from pypaimon.schema.schema import Schema
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

    def test_create_table_rejects_unsupported_map_blob_key(self):
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

        with self.assertRaisesRegex(
            ValueError, "Unsupported key type for MAP<X, BLOB>"
        ):
            manager.create_table(schema)

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


if __name__ == '__main__':
    unittest.main()
