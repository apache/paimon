"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import os
import shutil
import tempfile
import unittest

from pypaimon import CatalogFactory
from pypaimon.catalog.catalog_exception import (BranchAlreadyExistException,
                                                BranchNotExistException,
                                                TableNotExistException)
from pypaimon.common.identifier import Identifier
from pypaimon.tests.rest.rest_base_test import RESTBaseTest


class RESTCatalogBranchCRUDTest(RESTBaseTest):
    """End-to-end Branch CRUD against the in-process mock REST server.

    Mirrors the matrix exercised by Java
    ``RESTCatalogTest::testBranches`` (paimon-core/.../rest/RESTCatalogTest.java:2138-2191).
    """

    def _identifier(self):
        # The base test creates ``default.test_reader_iterator`` and commits a
        # snapshot in setUp.
        return Identifier.from_string("default.test_reader_iterator")

    def test_create_branch_table_not_exist(self):
        with self.assertRaises(TableNotExistException):
            self.rest_catalog.create_branch(
                Identifier.from_string("default.no_such_table"), "b1")

    def test_list_branches_table_not_exist(self):
        with self.assertRaises(TableNotExistException):
            self.rest_catalog.list_branches(
                Identifier.from_string("default.no_such_table"))

    def test_create_branch_without_from_tag(self):
        identifier = self._identifier()
        self.rest_catalog.create_branch(identifier, "b1")
        self.assertEqual(self.rest_catalog.list_branches(identifier), ["b1"])

    def test_create_branch_duplicate_raises(self):
        identifier = self._identifier()
        self.rest_catalog.create_branch(identifier, "b1")
        with self.assertRaises(BranchAlreadyExistException) as cm:
            self.rest_catalog.create_branch(identifier, "b1")
        self.assertEqual(cm.exception.branch, "b1")

    def test_list_branches_returns_created(self):
        identifier = self._identifier()
        for name in ("b1", "b2"):
            self.rest_catalog.create_branch(identifier, name)
        self.assertEqual(
            sorted(self.rest_catalog.list_branches(identifier)), ["b1", "b2"])

    def test_rename_branch_happy(self):
        identifier = self._identifier()
        self.rest_catalog.create_branch(identifier, "b1")
        self.rest_catalog.rename_branch(identifier, "b1", "b2")
        result = self.rest_catalog.list_branches(identifier)
        self.assertNotIn("b1", result)
        self.assertIn("b2", result)

    def test_rename_branch_to_existing_raises(self):
        identifier = self._identifier()
        self.rest_catalog.create_branch(identifier, "b1")
        self.rest_catalog.create_branch(identifier, "b2")
        with self.assertRaises(BranchAlreadyExistException) as cm:
            self.rest_catalog.rename_branch(identifier, "b1", "b2")
        self.assertEqual(cm.exception.branch, "b2")

    def test_rename_branch_from_missing_raises(self):
        identifier = self._identifier()
        with self.assertRaises(BranchNotExistException) as cm:
            self.rest_catalog.rename_branch(identifier, "absent", "b2")
        self.assertEqual(cm.exception.branch, "absent")

    def test_drop_branch_happy(self):
        identifier = self._identifier()
        self.rest_catalog.create_branch(identifier, "b1")
        self.rest_catalog.drop_branch(identifier, "b1")
        self.assertNotIn("b1", self.rest_catalog.list_branches(identifier))

    def test_drop_branch_missing_raises(self):
        identifier = self._identifier()
        with self.assertRaises(BranchNotExistException) as cm:
            self.rest_catalog.drop_branch(identifier, "absent")
        self.assertEqual(cm.exception.branch, "absent")

    def test_fast_forward_missing_raises(self):
        identifier = self._identifier()
        with self.assertRaises(BranchNotExistException) as cm:
            self.rest_catalog.fast_forward(identifier, "absent")
        self.assertEqual(cm.exception.branch, "absent")

    def test_fast_forward_happy(self):
        identifier = self._identifier()
        self.rest_catalog.create_branch(identifier, "b1")
        # Mock fast-forward is a no-op acknowledgement; the call must not raise.
        self.rest_catalog.fast_forward(identifier, "b1")


class FilesystemCatalogBranchInheritsNotImplementedTest(unittest.TestCase):
    """The filesystem catalog inherits the abstract ``NotImplementedError`` stubs.

    A concrete filesystem branch implementation requires a Python-side
    BranchManager and is tracked separately. The point of this test is to
    confirm the new ``rename_branch`` abstract stub closes the API gap:
    on master ``Catalog.rename_branch`` did not exist (AttributeError),
    after this PR it raises ``NotImplementedError``.
    """

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="unittest_branch_")
        warehouse = os.path.join(self.temp_dir, "warehouse")
        os.makedirs(warehouse, exist_ok=True)
        self.catalog = CatalogFactory.create({"warehouse": warehouse})

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_rename_branch_raises_not_implemented(self):
        with self.assertRaises(NotImplementedError) as cm:
            self.catalog.rename_branch(
                Identifier.from_string("default.tbl"), "b1", "b2")
        self.assertIn("rename_branch", str(cm.exception))


if __name__ == "__main__":
    unittest.main()
