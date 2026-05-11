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

import unittest
from unittest.mock import MagicMock

from pypaimon.branch.catalog_branch_manager import CatalogBranchManager
from pypaimon.catalog.catalog_exception import (
    BranchAlreadyExistException,
    BranchNotExistException,
    TagNotExistException
)
from pypaimon.common.identifier import Identifier


class TestCatalogBranchManager(unittest.TestCase):
    """Test cases for CatalogBranchManager."""

    def setUp(self):
        """Set up test fixtures."""
        self.catalog_loader = MagicMock()
        self.catalog = MagicMock()
        self.catalog_loader.load.return_value = self.catalog
        self.identifier = Identifier("test_db", "test_table")
        self.branch_manager = CatalogBranchManager(self.catalog_loader, self.identifier)

    def test_create_branch_basic(self):
        """Test basic branch creation."""
        self.branch_manager.create_branch("test-branch")

        self.catalog.create_branch.assert_called_once_with(
            self.identifier, "test-branch", None
        )

    def test_create_branch_from_tag(self):
        """Test creating branch from tag."""
        self.branch_manager.create_branch("test-branch", "tag-1")

        self.catalog.create_branch.assert_called_once_with(
            self.identifier, "test-branch", "tag-1"
        )

    def test_create_branch_ignore_if_exists(self):
        """Test creating branch with ignore_if_exists flag."""
        self.catalog.list_branches.return_value = ["existing-branch"]
        self.branch_manager.create_branch("existing-branch", ignore_if_exists=True)

        # Should not call create_branch if branch exists and ignore_if_exists is True
        self.catalog.create_branch.assert_not_called()
        # Reset for next test
        self.catalog.list_branches.return_value = []

    def test_create_branch_already_exists(self):
        """Test creating branch when it already exists."""
        self.catalog.list_branches.return_value = ["existing-branch"]
        self.catalog.create_branch.side_effect = BranchAlreadyExistException("existing-branch")

        with self.assertRaises(ValueError) as cm:
            self.branch_manager.create_branch("existing-branch")

        self.assertIn("already exists", str(cm.exception))

    def test_create_branch_with_branch_not_exist_exception(self):
        """Test creating branch when catalog raises BranchNotExistException."""
        self.catalog.create_branch.side_effect = BranchNotExistException("test-branch")

        with self.assertRaises(ValueError) as cm:
            self.branch_manager.create_branch("test-branch")

        self.assertIn("doesn't exist", str(cm.exception))

    def test_create_branch_with_tag_not_exist_exception(self):
        """Test creating branch when catalog raises TagNotExistException."""
        self.catalog.create_branch.side_effect = TagNotExistException("tag-1")

        with self.assertRaises(ValueError) as cm:
            self.branch_manager.create_branch("test-branch", "tag-1")

        self.assertIn("doesn't exist", str(cm.exception))

    def test_create_branch_with_branch_already_exist_exception(self):
        """Test creating branch when catalog raises BranchAlreadyExistException."""
        self.catalog.create_branch.side_effect = BranchAlreadyExistException("test-branch")

        with self.assertRaises(ValueError) as cm:
            self.branch_manager.create_branch("test-branch")

        self.assertIn("already exists", str(cm.exception))

    def test_drop_branch(self):
        """Test dropping a branch."""
        self.branch_manager.drop_branch("test-branch")

        self.catalog.drop_branch.assert_called_once_with(self.identifier, "test-branch")

    def test_drop_branch_not_exist(self):
        """Test dropping a non-existent branch."""
        self.catalog.drop_branch.side_effect = BranchNotExistException("test-branch")

        with self.assertRaises(ValueError) as cm:
            self.branch_manager.drop_branch("test-branch")

        self.assertIn("doesn't exist", str(cm.exception))

    def test_fast_forward(self):
        """Test fast-forward operation."""
        self.branch_manager.fast_forward("test-branch")

        self.catalog.fast_forward.assert_called_once_with(self.identifier, "test-branch")

    def test_fast_forward_to_main(self):
        """Test fast-forward to main branch should raise error."""
        from pypaimon.common.identifier import Identifier
        identifier_with_branch = Identifier.create("test_db", "test_table", branch="feature")
        branch_manager = CatalogBranchManager(self.catalog_loader, identifier_with_branch)

        with self.assertRaises(ValueError) as cm:
            branch_manager.fast_forward("main")

        self.assertIn("do not use in fast-forward", str(cm.exception))

    def test_fast_forward_to_current_branch(self):
        """Test fast-forward to current branch should raise error."""
        from pypaimon.common.identifier import Identifier
        identifier_with_branch = Identifier.create("test_db", "test_table", branch="feature")
        branch_manager = CatalogBranchManager(self.catalog_loader, identifier_with_branch)

        with self.assertRaises(ValueError) as cm:
            branch_manager.fast_forward("feature")

        self.assertIn("is not allowed", str(cm.exception))

    def test_branches(self):
        """Test listing branches."""
        expected_branches = ["main", "feature-1", "feature-2"]
        self.catalog.list_branches.return_value = expected_branches

        branches = self.branch_manager.branches()

        self.assertEqual(branches, expected_branches)
        self.catalog.list_branches.assert_called_once_with(self.identifier)

    def test_branches_empty(self):
        """Test listing branches when none exist."""
        self.catalog.list_branches.return_value = []

        branches = self.branch_manager.branches()

        self.assertEqual(branches, [])
        self.catalog.list_branches.assert_called_once_with(self.identifier)

    def test_validate_branch_names(self):
        """Test branch name validation."""
        # Valid branch names
        valid_names = ["feature-1", "dev-branch", "release-v1.0"]

        for name in valid_names:
            # Should not raise exception for valid names
            try:
                self.branch_manager.create_branch(name)
            except ValueError as e:
                # Ignore validation errors if any
                if "already exists" not in str(e):
                    raise

    def test_invalid_branch_names(self):
        """Test invalid branch name validation."""
        # "main" is not allowed
        with self.assertRaises(ValueError) as cm:
            self.branch_manager.create_branch("main")
        self.assertIn("default branch", str(cm.exception))

        # Empty branch name
        with self.assertRaises(ValueError) as cm:
            self.branch_manager.create_branch("")
        self.assertIn("is blank", str(cm.exception))

        # Pure numeric branch name
        with self.assertRaises(ValueError) as cm:
            self.branch_manager.create_branch("123")
        self.assertIn("pure numeric", str(cm.exception))


if __name__ == '__main__':
    unittest.main()
