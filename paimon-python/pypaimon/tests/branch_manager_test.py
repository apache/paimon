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
from pypaimon.branch.branch_manager import BranchManager, DEFAULT_MAIN_BRANCH
from pypaimon.branch.filesystem_branch_manager import FileSystemBranchManager
from pypaimon.common.identifier import Identifier


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


class SnapshotManagerBranchAwarenessTest(unittest.TestCase):
    """Pin down SnapshotManager's branch-aware path computation.

    Mirrors Java ``SnapshotManager.copyWithBranch`` (utils/SnapshotManager.java):
    a non-main branch must produce paths under
    ``{table_path}/branch/branch-{name}/snapshot/...``.
    """

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="unittest_snapshot_branch_")
        warehouse = os.path.join(self.temp_dir, "warehouse")
        os.makedirs(warehouse, exist_ok=True)
        self.catalog = CatalogFactory.create({"warehouse": warehouse})
        self.catalog.create_database("default", True)

        self.pa_schema = pa.schema([("id", pa.int64()), ("value", pa.string())])
        self.identifier = Identifier.from_string("default.snapshot_branch_table")
        self.catalog.create_table(
            self.identifier, Schema.from_pyarrow_schema(self.pa_schema), False)
        self.table = self.catalog.get_table(self.identifier)

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_default_constructor_targets_main_branch(self):
        sm = self.table.snapshot_manager()
        self.assertEqual(sm.branch, "main")
        self.assertEqual(sm.snapshot_dir, f"{self.table.table_path.rstrip('/')}/snapshot")

    def test_copy_with_branch_returns_branch_aware_paths(self):
        sm = self.table.snapshot_manager()
        branch_sm = sm.copy_with_branch("b1")

        self.assertIsNot(branch_sm, sm)
        self.assertEqual(branch_sm.branch, "b1")

        expected_dir = f"{self.table.table_path.rstrip('/')}/branch/branch-b1/snapshot"
        self.assertEqual(branch_sm.snapshot_dir, expected_dir)
        self.assertEqual(branch_sm.get_snapshot_path(7), f"{expected_dir}/snapshot-7")
        self.assertNotEqual(sm.get_snapshot_path(7), branch_sm.get_snapshot_path(7))

    def test_copy_with_branch_rebranches_snapshot_loader(self):
        from pypaimon.common.identifier import Identifier
        from pypaimon.snapshot.snapshot_loader import SnapshotLoader

        sm = self.table.snapshot_manager()
        # FileSystem path has no loader; inject one to exercise the
        # rebranch code path. Mirrors Java SnapshotLoaderImpl.copyWithBranch.
        sm.snapshot_loader = SnapshotLoader(
            catalog_loader=object(),
            identifier=Identifier(database="default",
                                  object="snapshot_branch_table"),
        )

        branch_sm = sm.copy_with_branch("b1")

        self.assertIsNotNone(branch_sm.snapshot_loader)
        self.assertIsNot(branch_sm.snapshot_loader, sm.snapshot_loader)
        self.assertEqual(branch_sm.snapshot_loader.identifier.branch, "b1")
        self.assertEqual(
            branch_sm.snapshot_loader.identifier.database,
            sm.snapshot_loader.identifier.database)
        self.assertEqual(
            branch_sm.snapshot_loader.identifier.get_table_name(),
            sm.snapshot_loader.identifier.get_table_name())
        # Original loader's identifier untouched.
        self.assertIsNone(sm.snapshot_loader.identifier.branch)


class FileSystemBranchManagerEndToEndTest(unittest.TestCase):
    """Catalog-driven end-to-end tests that exercise ``_copy_with_branch``.

    These regress the from-tag and fast-forward paths that previously
    raised ``SameFileError``: the SnapshotManager produced by
    ``_copy_with_branch`` still pointed at the main-branch snapshot dir,
    so ``copy_file(src, dst)`` collapsed to ``src == dst``.
    """

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="unittest_fs_branch_e2e_")
        warehouse = os.path.join(self.temp_dir, "warehouse")
        os.makedirs(warehouse, exist_ok=True)
        self.catalog = CatalogFactory.create({"warehouse": warehouse})
        self.catalog.create_database("default", True)

        self.pa_schema = pa.schema([("id", pa.int64()), ("value", pa.string())])
        self.identifier = Identifier.from_string("default.fs_branch_e2e_table")
        self.catalog.create_table(
            self.identifier, Schema.from_pyarrow_schema(self.pa_schema), False)

        table = self.catalog.get_table(self.identifier)
        wb = table.new_batch_write_builder()
        w = wb.new_write()
        w.write_arrow(pa.Table.from_pydict(
            {"id": [1, 2, 3], "value": ["a", "b", "c"]}, schema=self.pa_schema))
        wb.new_commit().commit(w.prepare_commit())
        w.close()

        self.table = self.catalog.get_table(self.identifier)
        self.table_root = self.table.table_path.rstrip('/')

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _latest_snapshot_id(self) -> int:
        latest = self.table.snapshot_manager().get_latest_snapshot()
        self.assertIsNotNone(latest)
        return latest.id

    def test_create_branch_from_tag_lands_files_under_branch_dir(self):
        snapshot_id = self._latest_snapshot_id()

        self.table.create_tag("t1")
        bm = self.table.branch_manager()
        bm.create_branch("b1", tag_name="t1")

        branch_root = f"{self.table_root}/branch/branch-b1"
        self.assertTrue(os.path.isdir(branch_root))
        self.assertTrue(
            os.path.isfile(f"{branch_root}/snapshot/snapshot-{snapshot_id}"))
        self.assertTrue(os.path.isfile(f"{branch_root}/tag/tag-t1"))
        # At least one schema file (schema-0) must have been copied.
        schema_dir = f"{branch_root}/schema"
        self.assertTrue(os.path.isdir(schema_dir))
        self.assertTrue(
            any(name.startswith("schema-") for name in os.listdir(schema_dir)))

    def test_fast_forward_after_create_branch_from_tag(self):
        self.table.create_tag("t1")
        bm = self.table.branch_manager()
        bm.create_branch("b1", tag_name="t1")
        # Must not raise: previously fast_forward's copy_files(src=branch_dir,
        # dst=main_dir) collapsed to src == dst because the branch-side
        # SnapshotManager still pointed at the main snapshot dir.
        bm.fast_forward("b1")


if __name__ == '__main__':
    unittest.main()
