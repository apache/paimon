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
from pypaimon.branch.filesystem_branch_manager import FileSystemBranchManager
from pypaimon.catalog.catalog_environment import CatalogEnvironment
from pypaimon.catalog.catalog_loader import CatalogLoader
from pypaimon.common.identifier import Identifier
from pypaimon.table.file_store_table import FileStoreTable


class MockCatalogLoader(CatalogLoader):
    """Mock catalog loader for testing."""

    def __init__(self, catalog):
        self.catalog = catalog

    def load(self):
        return self.catalog


class TestFileStoreTableBranchManager(unittest.TestCase):
    """Test cases for FileStoreTable.branch_manager() method."""

    def setUp(self):
        """Set up test fixtures."""
        import tempfile

        # Create temporary directory for test
        self.temp_dir = tempfile.mkdtemp()

        # Create mock file IO
        from pypaimon.common.file_io import FileIO
        from pypaimon.common.options.options import Options

        self.file_io = FileIO.get(self.temp_dir, Options({}))

        # Create identifier
        self.identifier = Identifier("test_db", "test_table")

        # Create table path
        self.table_path = f"{self.temp_dir}/test_db.db/test_table"

        # Create minimal schema using pyarrow
        import pyarrow as pa
        from pypaimon.schema.schema import Schema
        from pypaimon.schema.table_schema import TableSchema
        
        pa_schema = pa.schema([
            pa.field('pk', pa.int64(), nullable=False),
            pa.field('value', pa.string(), nullable=True)
        ])
        
        schema = Schema.from_pyarrow_schema(pa_schema)
        self.table_schema = TableSchema.from_schema(schema_id=0, schema=schema)

        # Create catalog environment (without catalog loader)
        self.catalog_environment = CatalogEnvironment.empty()

        # Initialize schema manager and commit schema
        from pypaimon.schema.schema_manager import SchemaManager
        schema_manager = SchemaManager(self.file_io, self.table_path)
        schema_manager.commit(self.table_schema)

    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        if hasattr(self, 'temp_dir') and self.temp_dir:
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_branch_manager_returns_filesystem_branch_manager_without_catalog(self):
        """Test that branch_manager returns FileSystemBranchManager when no catalog loader."""
        table = FileStoreTable(
            self.file_io,
            self.identifier,
            self.table_path,
            self.table_schema,
            self.catalog_environment
        )

        branch_mgr = table.branch_manager()

        self.assertIsInstance(branch_mgr, FileSystemBranchManager)

    def test_branch_manager_returns_catalog_branch_manager_with_catalog(self):
        """Test that branch_manager returns CatalogBranchManager when catalog loader exists."""
        # Create mock catalog
        catalog = MagicMock()
        catalog.list_branches.return_value = []

        # Create catalog loader
        catalog_loader = MockCatalogLoader(catalog)

        # Create catalog environment with catalog loader
        catalog_environment = CatalogEnvironment(
            identifier=self.identifier,
            uuid=None,
            catalog_loader=catalog_loader,
            supports_version_management=True
        )

        table = FileStoreTable(
            self.file_io,
            self.identifier,
            self.table_path,
            self.table_schema,
            catalog_environment
        )

        branch_mgr = table.branch_manager()

        self.assertIsInstance(branch_mgr, CatalogBranchManager)

    def test_catalog_branch_manager_integration(self):
        """Test CatalogBranchManager integration with table."""
        # Create mock catalog
        catalog = MagicMock()
        catalog.list_branches.return_value = []

        # Create catalog loader
        catalog_loader = MockCatalogLoader(catalog)

        # Create catalog environment with catalog loader
        catalog_environment = CatalogEnvironment(
            identifier=self.identifier,
            uuid=None,
            catalog_loader=catalog_loader,
            supports_version_management=True
        )

        table = FileStoreTable(
            self.file_io,
            self.identifier,
            self.table_path,
            self.table_schema,
            catalog_environment
        )

        branch_mgr = table.branch_manager()

        # Test create branch
        branch_mgr.create_branch("feature-branch")
        catalog.create_branch.assert_called_once_with(
            self.identifier, "feature-branch", None
        )

        # Test list branches
        catalog.list_branches.return_value = ["main", "feature-branch"]
        branches = branch_mgr.branches()
        self.assertEqual(branches, ["main", "feature-branch"])

        # Test drop branch
        branch_mgr.drop_branch("feature-branch")
        catalog.drop_branch.assert_called_once_with(self.identifier, "feature-branch")

    def test_rename_branch_basic(self):
        """Test rename branch basic functionality."""
        table = FileStoreTable(
            self.file_io,
            self.identifier,
            self.table_path,
            self.table_schema,
            self.catalog_environment
        )

        branch_mgr = table.branch_manager()

        # Create a branch first
        branch_mgr.create_branch("old-branch")
        self.assertTrue(branch_mgr.branch_exists("old-branch"))

        # Rename the branch
        branch_mgr.rename_branch("old-branch", "new-branch")

        # Check old branch doesn't exist
        self.assertFalse(branch_mgr.branch_exists("old-branch"))

        # Check new branch exists
        self.assertTrue(branch_mgr.branch_exists("new-branch"))

    def test_rename_branch_from_nonexistent(self):
        """Test renaming from non-existent branch raises error."""
        table = FileStoreTable(
            self.file_io,
            self.identifier,
            self.table_path,
            self.table_schema,
            self.catalog_environment
        )

        branch_mgr = table.branch_manager()

        with self.assertRaises(ValueError) as context:
            branch_mgr.rename_branch("nonexistent-branch", "new-branch")

        self.assertIn("doesn't exist", str(context.exception))

    def test_rename_branch_to_existing(self):
        """Test renaming to existing branch raises error."""
        table = FileStoreTable(
            self.file_io,
            self.identifier,
            self.table_path,
            self.table_schema,
            self.catalog_environment
        )

        branch_mgr = table.branch_manager()

        # Create two branches
        branch_mgr.create_branch("branch1")
        branch_mgr.create_branch("branch2")

        with self.assertRaises(ValueError) as context:
            branch_mgr.rename_branch("branch1", "branch2")

        self.assertIn("already exists", str(context.exception))

    def test_rename_main_branch_fails(self):
        """Test renaming main branch raises error."""
        table = FileStoreTable(
            self.file_io,
            self.identifier,
            self.table_path,
            self.table_schema,
            self.catalog_environment
        )

        branch_mgr = table.branch_manager()

        with self.assertRaises(ValueError) as context:
            branch_mgr.rename_branch("main", "new-branch")

        self.assertIn("main branch", str(context.exception))

    def test_rename_branch_with_invalid_names(self):
        """Test renaming with invalid branch names."""
        table = FileStoreTable(
            self.file_io,
            self.identifier,
            self.table_path,
            self.table_schema,
            self.catalog_environment
        )

        branch_mgr = table.branch_manager()

        # Test blank source name
        with self.assertRaises(ValueError) as context:
            branch_mgr.rename_branch("", "new-branch")
        self.assertIn("blank", str(context.exception))

        # Test blank target name
        with self.assertRaises(ValueError) as context:
            branch_mgr.rename_branch("old-branch", "")
        self.assertIn("blank", str(context.exception))

    def test_catalog_branch_manager_rename_branch(self):
        """Test CatalogBranchManager rename_branch calls catalog correctly."""
        # Create mock catalog
        catalog = MagicMock()
        catalog.list_branches.return_value = []

        # Create catalog loader
        catalog_loader = MockCatalogLoader(catalog)

        # Create catalog environment with catalog loader
        catalog_environment = CatalogEnvironment(
            identifier=self.identifier,
            uuid=None,
            catalog_loader=catalog_loader,
            supports_version_management=True
        )

        table = FileStoreTable(
            self.file_io,
            self.identifier,
            self.table_path,
            self.table_schema,
            catalog_environment
        )

        branch_mgr = table.branch_manager()

        # Test rename branch
        branch_mgr.rename_branch("old-branch", "new-branch")
        catalog.rename_branch.assert_called_once_with(
            self.identifier, "old-branch", "new-branch"
        )


if __name__ == '__main__':
    unittest.main()
