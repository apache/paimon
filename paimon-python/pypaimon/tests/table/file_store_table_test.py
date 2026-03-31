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

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.common.options.core_options import CoreOptions

from pypaimon.table.file_store_table import FileStoreTable


class FileStoreTableTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({
            'warehouse': cls.warehouse
        })
        cls.catalog.create_database('default', True)

        cls.pa_schema = pa.schema([
            ('user_id', pa.int32()),
            ('item_id', pa.int64()),
            ('behavior', pa.string()),
            ('dt', pa.string())
        ])
        schema = Schema.from_pyarrow_schema(cls.pa_schema, partition_keys=['dt'],
                                            options={CoreOptions.BUCKET.key(): "2"})
        cls.catalog.create_table('default.test_copy_with_new_options', schema, False)
        cls.table = cls.catalog.get_table('default.test_copy_with_new_options')

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def test_copy_with_new_options(self):
        """Test copy method with new options."""
        new_options = {"new.option": "new_value", "bucket": "2"}

        # Call copy method
        copied_table = self.table.copy(new_options)

        # Verify the copied table is a new instance
        self.assertIsNot(copied_table, self.table)
        self.assertIsInstance(copied_table, FileStoreTable)

        # Verify the new option is added
        self.assertIn("new.option", copied_table.table_schema.options)
        self.assertEqual(copied_table.table_schema.options["new.option"], "new_value")

    def test_copy_raises_error_when_changing_bucket(self):
        """Test copy method raises ValueError when trying to change bucket number."""
        # Get current bucket value
        current_bucket = self.table.options.bucket()

        # Try to change bucket number to a different value
        new_bucket_value = current_bucket + 1
        new_options = {CoreOptions.BUCKET.key(): new_bucket_value}

        # Verify ValueError is raised
        with self.assertRaises(ValueError) as context:
            self.table.copy(new_options)

        self.assertIn("Cannot change bucket number", str(context.exception))

    def test_consumer_manager(self):
        """Test that FileStoreTable has consumer_manager method."""
        # Get consumer_manager
        consumer_manager = self.table.consumer_manager()

        # Verify consumer_manager type
        from pypaimon.consumer.consumer_manager import ConsumerManager
        self.assertIsInstance(consumer_manager, ConsumerManager)

        # Verify consumer_manager has correct branch
        from pypaimon.consumer.consumer_manager import DEFAULT_MAIN_BRANCH
        self.assertEqual(self.table.current_branch(), DEFAULT_MAIN_BRANCH)

        # Test basic consumer operations through table.consumer_manager()
        from pypaimon.consumer.consumer import Consumer
        consumer_manager.reset_consumer("test_consumer", Consumer(next_snapshot=5))
        consumer = consumer_manager.consumer("test_consumer")
        self.assertIsNotNone(consumer)
        self.assertEqual(consumer.next_snapshot, 5)

        # Test min_next_snapshot
        min_snapshot = consumer_manager.min_next_snapshot()
        self.assertEqual(min_snapshot, 5)

    def test_consumer_manager_with_branch(self):
        """Test consumer_manager with branch option."""
        # Create table with branch option
        branch_name = "feature_branch"
        schema = Schema.from_pyarrow_schema(
            self.pa_schema,
            partition_keys=['dt'],
            options={
                CoreOptions.BUCKET.key(): "2",
                "branch": branch_name
            }
        )
        self.catalog.create_table('default.test_branch_table', schema, False)
        branch_table = self.catalog.get_table('default.test_branch_table')

        # Get consumer_manager and verify it has correct branch
        branch_consumer_manager = branch_table.consumer_manager()
        self.assertEqual(branch_table.current_branch(), branch_name)

        # Test consumer operations on branch
        from pypaimon.consumer.consumer import Consumer
        branch_consumer_manager.reset_consumer("branch_consumer", Consumer(next_snapshot=10))
        consumer = branch_consumer_manager.consumer("branch_consumer")
        self.assertIsNotNone(consumer)
        self.assertEqual(consumer.next_snapshot, 10)

    def test_branch_manager(self):
        """Test that FileStoreTable has branch_manager method."""
        # Get branch_manager
        branch_manager = self.table.branch_manager()

        # Verify branch_manager type
        from pypaimon.branch.filesystem_branch_manager import FileSystemBranchManager
        self.assertIsInstance(branch_manager, FileSystemBranchManager)

        # Verify branch_manager has correct current branch
        from pypaimon.common.identifier import DEFAULT_MAIN_BRANCH
        self.assertEqual(branch_manager.current_branch, DEFAULT_MAIN_BRANCH)

        # Test basic branch operations
        # Create a new branch
        branch_manager.create_branch("feature1", ignore_if_exists=False)

        # Verify branch exists
        self.assertTrue(branch_manager.branch_exists("feature1"))

        # List all branches
        branches = branch_manager.branches()
        self.assertIn("feature1", branches)

        # Create another branch
        branch_manager.create_branch("feature2", ignore_if_exists=False)
        self.assertTrue(branch_manager.branch_exists("feature2"))

        # Test ignore_if_exists
        branch_manager.create_branch("feature1", ignore_if_exists=True)
        self.assertTrue(branch_manager.branch_exists("feature1"))

        # Test error when branch already exists
        with self.assertRaises(ValueError) as context:
            branch_manager.create_branch("feature1", ignore_if_exists=False)
        self.assertIn("already exists", str(context.exception))

        # Drop a branch
        branch_manager.drop_branch("feature2")
        self.assertFalse(branch_manager.branch_exists("feature2"))

        # Test error when branch doesn't exist
        with self.assertRaises(ValueError) as context:
            branch_manager.drop_branch("non_existent")
        self.assertIn("doesn't exist", str(context.exception))

    def test_branch_manager_validation(self):
        """Test branch_manager validation for invalid branch names."""
        branch_manager = self.table.branch_manager()

        # Test main branch validation
        with self.assertRaises(ValueError) as context:
            branch_manager.create_branch("main")
        self.assertIn("default branch", str(context.exception))

        # Test blank branch name
        with self.assertRaises(ValueError) as context:
            branch_manager.create_branch("")
        self.assertIn("blank", str(context.exception))

        # Test numeric branch name
        with self.assertRaises(ValueError) as context:
            branch_manager.create_branch("123")
        self.assertIn("pure numeric", str(context.exception))

    def test_branch_manager_with_catalog_loader(self):
        """Test that branch_manager returns CatalogBranchManager when catalog loader is available."""
        from unittest.mock import MagicMock
        from pypaimon.branch.catalog_branch_manager import CatalogBranchManager
        from pypaimon.catalog.catalog_environment import CatalogEnvironment
        from pypaimon.catalog.catalog_loader import CatalogLoader

        # Create mock catalog
        mock_catalog = MagicMock()
        mock_catalog.list_branches.return_value = []

        # Create mock catalog loader that implements CatalogLoader interface
        class MockCatalogLoader(CatalogLoader):
            def load(self):
                return mock_catalog

        # Create a new table with catalog environment
        catalog_environment = CatalogEnvironment(
            identifier=self.table.identifier,
            uuid=None,
            catalog_loader=MockCatalogLoader(),
            supports_version_management=True
        )

        # Save original catalog environment
        original_env = self.table.catalog_environment

        try:
            # Replace catalog environment
            self.table.catalog_environment = catalog_environment

            # Get branch manager
            branch_manager = self.table.branch_manager()

            # Verify it returns CatalogBranchManager
            self.assertIsInstance(branch_manager, CatalogBranchManager)

        finally:
            # Restore original catalog environment
            self.table.catalog_environment = original_env

    def test_changelog_manager(self):
        """Test that FileStoreTable has changelog_manager method."""
        # Get changelog_manager
        changelog_manager = self.table.changelog_manager()

        # Verify changelog_manager type
        from pypaimon.changelog.changelog_manager import ChangelogManager
        self.assertIsInstance(changelog_manager, ChangelogManager)

        # Verify changelog_manager has correct branch
        from pypaimon.branch.branch_manager import DEFAULT_MAIN_BRANCH
        self.assertEqual(self.table.current_branch(), DEFAULT_MAIN_BRANCH)
        self.assertEqual(changelog_manager.branch, DEFAULT_MAIN_BRANCH)

    def test_changelog_manager_with_branch(self):
        """Test changelog_manager with branch option."""
        # Create table with branch option
        branch_name = "feature"
        schema = Schema.from_pyarrow_schema(
            self.pa_schema,
            partition_keys=['dt'],
            options={
                CoreOptions.BUCKET.key(): "2",
                "branch": branch_name
            }
        )
        self.catalog.create_table('default.test_changelog_branch_table', schema, False)
        branch_table = self.catalog.get_table('default.test_changelog_branch_table')

        # Get changelog_manager and verify it has correct branch
        branch_changelog_manager = branch_table.changelog_manager()
        self.assertEqual(branch_table.current_branch(), branch_name)
        self.assertEqual(branch_changelog_manager.branch, branch_name)

        # Verify changelog directory path is correct
        expected_changelog_dir = f"{branch_table.table_path}/branch/branch-feature/changelog"
        self.assertEqual(branch_changelog_manager.changelog_directory(), expected_changelog_dir)

    def test_changelog_manager_path_generation(self):
        """Test that changelog_manager generates correct paths."""
        changelog_manager = self.table.changelog_manager()

        # Test changelog directory path
        expected_dir = f"{self.table.table_path}/changelog"
        self.assertEqual(changelog_manager.changelog_directory(), expected_dir)

        # Test changelog file path
        snapshot_id = 123
        expected_path = f"{self.table.table_path}/changelog/changelog-{snapshot_id}"
        self.assertEqual(changelog_manager.long_lived_changelog_path(snapshot_id), expected_path)

    def test_changelog_manager_latest_and_earliest_none(self):
        """Test that latest and earliest changelog IDs are None when no changelog exists."""
        changelog_manager = self.table.changelog_manager()

        # No changelog files should exist yet
        self.assertIsNone(changelog_manager.latest_long_lived_changelog_id())
        self.assertIsNone(changelog_manager.earliest_long_lived_changelog_id())

    def test_current_branch(self):
        """Test that current_branch returns the branch from options."""
        from pypaimon.branch.branch_manager import DEFAULT_MAIN_BRANCH

        # Default table should have main branch
        self.assertEqual(self.table.current_branch(), DEFAULT_MAIN_BRANCH)

        # Table with branch option should return that branch
        branch_name = "feature_branch"
        schema = Schema.from_pyarrow_schema(
            self.pa_schema,
            partition_keys=['dt'],
            options={
                CoreOptions.BUCKET.key(): "2",
                "branch": branch_name
            }
        )
        self.catalog.create_table('default.test_current_branch', schema, False)
        branch_table = self.catalog.get_table('default.test_current_branch')
        self.assertEqual(branch_table.current_branch(), branch_name)

    def test_copy_with_branch(self):
        """Test copy method with branch option."""
        branch_name = "test_branch"

        # Copy table with branch option
        new_options = {"branch": branch_name}
        copied_table = self.table.copy(new_options)

        # Verify branch is set correctly
        self.assertEqual(copied_table.current_branch(), branch_name)

        # Verify schema_manager has the correct branch
        from pypaimon.branch.branch_manager import DEFAULT_MAIN_BRANCH
        self.assertEqual(self.table.schema_manager.branch, DEFAULT_MAIN_BRANCH)
        self.assertEqual(copied_table.schema_manager.branch, branch_name)

        # Verify other properties are preserved
        self.assertEqual(copied_table.identifier, self.table.identifier)
        self.assertEqual(copied_table.table_path, self.table.table_path)

    def test_rename_branch_basic(self):
        """Test rename_branch method."""
        # Get branch_manager
        branch_manager = self.table.branch_manager()

        # Create a branch first
        branch_manager.create_branch("old-branch")
        self.assertTrue(branch_manager.branch_exists("old-branch"))

        # Rename the branch using table's rename_branch method
        self.table.rename_branch("old-branch", "new-branch")

        # Verify old branch doesn't exist
        self.assertFalse(branch_manager.branch_exists("old-branch"))

        # Verify new branch exists
        self.assertTrue(branch_manager.branch_exists("new-branch"))

    def test_rename_branch_from_nonexistent(self):
        """Test renaming from non-existent branch raises error."""
        with self.assertRaises(ValueError) as context:
            self.table.rename_branch("nonexistent-branch", "new-branch")

        self.assertIn("doesn't exist", str(context.exception))

    def test_rename_branch_to_existing(self):
        """Test renaming to existing branch raises error."""
        # Get branch_manager
        branch_manager = self.table.branch_manager()

        # Create two branches
        branch_manager.create_branch("branch1")
        branch_manager.create_branch("branch2")

        with self.assertRaises(ValueError) as context:
            self.table.rename_branch("branch1", "branch2")

        self.assertIn("already exists", str(context.exception))

    def test_rename_main_branch_fails(self):
        """Test renaming main branch raises error."""
        with self.assertRaises(ValueError) as context:
            self.table.rename_branch("main", "new-branch")

        self.assertIn("main branch", str(context.exception))

    def test_comment_none(self):
        """Test that comment() returns None when table has no comment."""
        # Default table created without comment should return None
        self.assertIsNone(self.table.comment())

    def test_comment_with_value(self):
        """Test that comment() returns the correct comment when table has one."""
        # Create a table with a comment
        comment_text = "This is a test table comment"
        schema = Schema.from_pyarrow_schema(
            self.pa_schema,
            partition_keys=['dt'],
            options={CoreOptions.BUCKET.key(): "2"},
            comment=comment_text
        )
        self.catalog.create_table('default.test_comment_table', schema, False)
        comment_table = self.catalog.get_table('default.test_comment_table')

        # Verify comment is returned correctly
        self.assertEqual(comment_table.comment(), comment_text)

    def test_comment_preserved_after_copy(self):
        """Test that comment is preserved after copying the table."""
        # Create a table with a comment
        comment_text = "Test comment for copy"
        schema = Schema.from_pyarrow_schema(
            self.pa_schema,
            partition_keys=['dt'],
            options={CoreOptions.BUCKET.key(): "2"},
            comment=comment_text
        )
        self.catalog.create_table('default.test_comment_copy_table', schema, False)
        original_table = self.catalog.get_table('default.test_comment_copy_table')

        # Verify original table has comment
        self.assertEqual(original_table.comment(), comment_text)

        # Copy the table
        copied_table = original_table.copy({})

        # Verify comment is preserved in copied table
        self.assertEqual(copied_table.comment(), comment_text)

    def test_truncate_table(self):
        """Test truncate_table functionality."""
        # Create a write builder
        write_builder = self.table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        # Write some data (use int32 for user_id to match table schema)
        data = pa.Table.from_pydict({
            'user_id': [1, 2, 3],
            'item_id': [100, 200, 300],
            'behavior': ['view', 'click', 'purchase'],
            'dt': ['2024-01-01', '2024-01-02', '2024-01-03']
        }, schema=self.pa_schema)
        table_write.write_arrow(data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # Verify data was written
        read_builder = self.table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        result_before = table_read.to_arrow(table_scan.plan().splits())
        self.assertEqual(result_before.num_rows, 3)

        # Truncate the table
        write_builder = self.table.new_batch_write_builder()
        table_commit = write_builder.new_commit()
        table_commit.truncate_table()
        table_commit.close()

        # Verify data is truncated
        read_builder = self.table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        result_after = table_read.to_arrow(table_scan.plan().splits())
        self.assertEqual(result_after.num_rows, 0)

        # Verify new snapshot was created
        snapshot = self.table.snapshot_manager().get_latest_snapshot()
        self.assertIsNotNone(snapshot)
        self.assertEqual(snapshot.commit_kind, 'OVERWRITE')

    def test_truncate_empty_table(self):
        """Test truncate_table on an empty table."""
        # Truncate empty table - should still work
        write_builder = self.table.new_batch_write_builder()
        table_commit = write_builder.new_commit()
        table_commit.truncate_table()
        table_commit.close()

        # Verify table is still empty
        read_builder = self.table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        result = table_read.to_arrow(table_scan.plan().splits())
        self.assertEqual(result.num_rows, 0)

    def test_truncate_table_multiple_times(self):
        """Test multiple consecutive truncate operations."""
        # Write and truncate multiple times
        for i in range(3):
            # Write some data
            write_builder = self.table.new_batch_write_builder()
            table_write = write_builder.new_write()
            table_commit = write_builder.new_commit()

            data = pa.Table.from_pydict({
                'user_id': [i, i + 1],
                'item_id': [i * 100, (i + 1) * 100],
                'behavior': ['view', 'click'],
                'dt': [f'2024-01-{i+1:02d}', f'2024-01-{i+2:02d}']
            }, schema=self.pa_schema)
            table_write.write_arrow(data)
            table_commit.commit(table_write.prepare_commit())
            table_write.close()
            table_commit.close()

            # Verify data was written
            read_builder = self.table.new_read_builder()
            table_scan = read_builder.new_scan()
            table_read = read_builder.new_read()
            result = table_read.to_arrow(table_scan.plan().splits())
            self.assertEqual(result.num_rows, 2)

            # Truncate
            write_builder = self.table.new_batch_write_builder()
            table_commit = write_builder.new_commit()
            table_commit.truncate_table()
            table_commit.close()

            # Verify data is truncated
            read_builder = self.table.new_read_builder()
            table_scan = read_builder.new_scan()
            table_read = read_builder.new_read()
            result = table_read.to_arrow(table_scan.plan().splits())
            self.assertEqual(result.num_rows, 0)
