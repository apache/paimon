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
#  specific language governing permissions and limitations
#  under the License.
################################################################################

import os
import tempfile
import unittest

from pypaimon.branch.branch_manager import BranchManager, DEFAULT_MAIN_BRANCH
from pypaimon.common.file_io import FileIO
from pypaimon.common.options.options import Options
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


if __name__ == '__main__':
    unittest.main()
