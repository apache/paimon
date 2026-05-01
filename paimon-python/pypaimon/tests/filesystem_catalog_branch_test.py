################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

"""End-to-end tests for branch CRUD on ``FileSystemCatalog``.

Mirrors the ``RESTCatalogBranchCRUDTest`` matrix from the REST branch
tests but exercises the local filesystem path. Pins down the exception
types and return shapes the catalog layer must produce regardless of
which catalog implementation is in use.
"""

import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.catalog.catalog_exception import (BranchAlreadyExistException,
                                                BranchNotExistException,
                                                TableNotExistException,
                                                TagNotExistException)
from pypaimon.common.identifier import Identifier


class FileSystemCatalogBranchCRUDTest(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="unittest_fs_branch_")
        warehouse = os.path.join(self.temp_dir, "warehouse")
        os.makedirs(warehouse, exist_ok=True)
        self.catalog = CatalogFactory.create({"warehouse": warehouse})
        self.catalog.create_database("default", True)

        self.pa_schema = pa.schema([
            ("id", pa.int64()),
            ("value", pa.string()),
        ])
        self.identifier = Identifier.from_string("default.test_branch_table")
        self.catalog.create_table(
            self.identifier,
            Schema.from_pyarrow_schema(self.pa_schema),
            False,
        )
        # Commit one batch so the table has a snapshot to base branches on.
        table = self.catalog.get_table(self.identifier)
        wb = table.new_batch_write_builder()
        w = wb.new_write()
        w.write_arrow(pa.Table.from_pydict(
            {"id": [1, 2, 3], "value": ["a", "b", "c"]},
            schema=self.pa_schema,
        ))
        wb.new_commit().commit(w.prepare_commit())
        w.close()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    # -- create + list --------------------------------------------------------

    def test_create_branch_without_from_tag(self):
        self.catalog.create_branch(self.identifier, "b1")
        self.assertEqual(self.catalog.list_branches(self.identifier), ["b1"])

    def test_create_branch_duplicate_raises(self):
        self.catalog.create_branch(self.identifier, "b1")
        with self.assertRaises(BranchAlreadyExistException) as cm:
            self.catalog.create_branch(self.identifier, "b1")
        self.assertEqual(cm.exception.branch, "b1")

    def test_create_branch_table_not_exists(self):
        with self.assertRaises(TableNotExistException):
            self.catalog.create_branch(
                Identifier.from_string("default.no_such_table"), "b1")

    def test_create_branch_from_nonexistent_tag_raises(self):
        with self.assertRaises(TagNotExistException) as cm:
            self.catalog.create_branch(
                self.identifier, "b1", tag_name="absent_tag")
        self.assertEqual(cm.exception.tag, "absent_tag")

    # NOTE: ``test_create_branch_from_existing_tag`` (a true happy-path
    # ``create_branch(tag_name=...)``) is not included here. The
    # ``FileSystemBranchManager`` "from-tag" path has a pre-existing bug
    # (``branch_snapshot_manager`` is constructed without switching to
    # the new branch's path, so ``copy_file(src, dst)`` ends up with
    # ``src == dst`` and raises ``SameFileError``). That's a manager-
    # level fix, not in the scope of this catalog-layer thin wrapper.
    # Catalog-layer error translation for the from-tag path is still
    # covered by ``test_create_branch_from_nonexistent_tag_raises``.

    # -- list -----------------------------------------------------------------

    def test_list_branches_returns_created(self):
        for name in ("b1", "b2", "b3"):
            self.catalog.create_branch(self.identifier, name)
        self.assertEqual(
            sorted(self.catalog.list_branches(self.identifier)),
            ["b1", "b2", "b3"],
        )

    def test_list_branches_empty(self):
        # Fresh table with no branches created.
        self.assertEqual(self.catalog.list_branches(self.identifier), [])

    def test_list_branches_table_not_exists(self):
        with self.assertRaises(TableNotExistException):
            self.catalog.list_branches(
                Identifier.from_string("default.no_such_table"))

    # -- rename ---------------------------------------------------------------

    def test_rename_branch_happy(self):
        self.catalog.create_branch(self.identifier, "b1")
        self.catalog.rename_branch(self.identifier, "b1", "b2")
        listed = self.catalog.list_branches(self.identifier)
        self.assertNotIn("b1", listed)
        self.assertIn("b2", listed)

    def test_rename_branch_to_existing_raises(self):
        self.catalog.create_branch(self.identifier, "b1")
        self.catalog.create_branch(self.identifier, "b2")
        with self.assertRaises(BranchAlreadyExistException) as cm:
            self.catalog.rename_branch(self.identifier, "b1", "b2")
        self.assertEqual(cm.exception.branch, "b2")

    def test_rename_branch_from_missing_raises(self):
        with self.assertRaises(BranchNotExistException) as cm:
            self.catalog.rename_branch(self.identifier, "absent", "b2")
        self.assertEqual(cm.exception.branch, "absent")

    # -- drop -----------------------------------------------------------------

    def test_drop_branch_happy(self):
        self.catalog.create_branch(self.identifier, "b1")
        self.catalog.drop_branch(self.identifier, "b1")
        self.assertNotIn(
            "b1", self.catalog.list_branches(self.identifier))

    def test_drop_branch_missing_raises(self):
        with self.assertRaises(BranchNotExistException) as cm:
            self.catalog.drop_branch(self.identifier, "absent")
        self.assertEqual(cm.exception.branch, "absent")

    # -- fast_forward ---------------------------------------------------------

    def test_fast_forward_missing_raises(self):
        with self.assertRaises(BranchNotExistException) as cm:
            self.catalog.fast_forward(self.identifier, "absent")
        self.assertEqual(cm.exception.branch, "absent")

    # NOTE: a true happy-path ``fast_forward`` end-to-end test is not
    # included here for the same reason as the create-branch-from-tag
    # case above — it requires the manager-level fix to the from-tag
    # path (so the branch carries a snapshot for fast-forward to move).
    # Catalog-layer error translation is covered by the missing-branch
    # case above.


if __name__ == "__main__":
    unittest.main()
