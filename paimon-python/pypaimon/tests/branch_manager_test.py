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

import os
import shutil
import tempfile
import unittest

from pypaimon.branch.branch_manager import BranchManager, DEFAULT_MAIN_BRANCH
from pypaimon.branch.filesystem_branch_manager import FileSystemBranchManager


class BranchManagerTest(unittest.TestCase):
    """Test BranchManager static methods."""

    def test_is_main_branch(self):
        """Test is_main_branch method."""
        self.assertTrue(BranchManager.is_main_branch("main"))
        self.assertFalse(BranchManager.is_main_branch("feature"))
        self.assertFalse(BranchManager.is_main_branch("develop"))

    def test_normalize_branch(self):
        """Test normalize_branch method."""
        self.assertEqual(BranchManager.normalize_branch(None), DEFAULT_MAIN_BRANCH)
        self.assertEqual(BranchManager.normalize_branch(""), DEFAULT_MAIN_BRANCH)
        self.assertEqual(BranchManager.normalize_branch("  "), DEFAULT_MAIN_BRANCH)
        self.assertEqual(BranchManager.normalize_branch("main"), "main")
        self.assertEqual(BranchManager.normalize_branch("feature"), "feature")
        self.assertEqual(BranchManager.normalize_branch("  feature  "), "feature")

    def test_branch_path(self):
        """Test branch_path method."""
        table_path = "/path/to/table"
        self.assertEqual(BranchManager.branch_path(table_path, "main"), table_path)
        self.assertEqual(
            BranchManager.branch_path(table_path, "feature"),
            "/path/to/table/branch/branch-feature"
        )
        self.assertEqual(
            BranchManager.branch_path(table_path, "develop"),
            "/path/to/table/branch/branch-develop"
        )

    def test_validate_branch_main_branch(self):
        """Test validate_branch rejects main branch."""
        with self.assertRaises(ValueError) as context:
            BranchManager.validate_branch("main")
        self.assertIn("default branch", str(context.exception))

    def test_validate_branch_blank(self):
        """Test validate_branch rejects blank branch names."""
        with self.assertRaises(ValueError) as context:
            BranchManager.validate_branch("")
        self.assertIn("blank", str(context.exception))

        with self.assertRaises(ValueError) as context:
            BranchManager.validate_branch("  ")
        self.assertIn("blank", str(context.exception))

    def test_validate_branch_numeric(self):
        """Test validate_branch rejects pure numeric branch names."""
        with self.assertRaises(ValueError) as context:
            BranchManager.validate_branch("123")
        self.assertIn("pure numeric", str(context.exception))

    def test_validate_branch_valid(self):
        """Test validate_branch accepts valid branch names."""
        # Should not raise exception
        BranchManager.validate_branch("feature")
        BranchManager.validate_branch("develop")
        BranchManager.validate_branch("feature-branch-123")

    def test_fast_forward_validate_to_main(self):
        """Test fast_forward_validate rejects fast-forward to main."""
        with self.assertRaises(ValueError) as context:
            BranchManager.fast_forward_validate("main", "feature")
        self.assertIn("do not use in fast-forward", str(context.exception))

    def test_fast_forward_validate_blank(self):
        """Test fast_forward_validate rejects blank branch name."""
        with self.assertRaises(ValueError) as context:
            BranchManager.fast_forward_validate("", "feature")
        self.assertIn("blank", str(context.exception))

    def test_fast_forward_validate_same_branch(self):
        """Test fast_forward_validate rejects fast-forward to same branch."""
        with self.assertRaises(ValueError) as context:
            BranchManager.fast_forward_validate("feature", "feature")
        self.assertIn("from the current branch", str(context.exception))


class FileSystemBranchManagerTest(unittest.TestCase):
    """Test FileSystemBranchManager."""

    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.table_path = os.path.join(self.temp_dir, "test_table")
        from pypaimon.filesystem.local_file_io import LocalFileIO
        self.file_io = LocalFileIO(self.table_path)

    def tearDown(self):
        """Clean up test environment."""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_branch_directory(self):
        """Test branch_directory method."""
        # Create a mock manager with minimal dependencies
        manager = FileSystemBranchManager(
            file_io=self.file_io,
            table_path=self.table_path,
            snapshot_manager=None,
            tag_manager=None,
            schema_manager=None
        )

        expected_dir = f"{self.table_path}/branch"
        self.assertEqual(manager._branch_directory(), expected_dir)

    def test_branch_path(self):
        """Test branch_path method."""
        manager = FileSystemBranchManager(
            file_io=self.file_io,
            table_path=self.table_path,
            snapshot_manager=None,
            tag_manager=None,
            schema_manager=None
        )

        self.assertEqual(manager.branch_path("main"), self.table_path)
        self.assertEqual(
            manager.branch_path("feature"),
            f"{self.table_path}/branch/branch-feature"
        )

    def test_branches_empty(self):
        """Test branches returns empty list when no branches exist."""
        manager = FileSystemBranchManager(
            file_io=self.file_io,
            table_path=self.table_path,
            snapshot_manager=None,
            tag_manager=None,
            schema_manager=None
        )

        branches = manager.branches()
        self.assertEqual(branches, [])

    def test_branch_exists(self):
        """Test branch_exists method."""
        manager = FileSystemBranchManager(
            file_io=self.file_io,
            table_path=self.table_path,
            snapshot_manager=None,
            tag_manager=None,
            schema_manager=None
        )

        # Create a branch directory
        branch_dir = os.path.join(self.table_path, "branch", "branch-feature")
        os.makedirs(branch_dir, exist_ok=True)

        self.assertTrue(manager.branch_exists("feature"))
        self.assertFalse(manager.branch_exists("develop"))


if __name__ == '__main__':
    unittest.main()
