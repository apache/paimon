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

import unittest

from pypaimon.api.api_response import GetTagResponse
from pypaimon.catalog.catalog_exception import (TableNotExistException,
                                                TagAlreadyExistException,
                                                TagNotExistException)
from pypaimon.common.identifier import Identifier
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.tests.rest.rest_base_test import RESTBaseTest


class RESTCatalogTagCRUDTest(RESTBaseTest):
    """End-to-end Tag CRUD against the in-process mock REST server.

    Mirrors the matrix exercised by Java
    ``RESTCatalogTest::testTags`` (paimon-core/.../rest/RESTCatalogTest.java).
    """

    def _identifier(self):
        # The base test creates ``default.test_reader_iterator`` and commits a
        # snapshot in setUp, so it already has snapshot id 1 to tag.
        return Identifier.from_string("default.test_reader_iterator")

    def test_create_and_get_tag(self):
        identifier = self._identifier()
        self.rest_catalog.create_tag(identifier, "t1")
        response = self.rest_catalog.get_tag(identifier, "t1")
        self.assertIsInstance(response, GetTagResponse)
        self.assertEqual(response.tag_name, "t1")
        self.assertIsInstance(response.snapshot, Snapshot)

    def test_list_tags_paged_basic(self):
        identifier = self._identifier()
        for name in ("t1", "t2", "t3"):
            self.rest_catalog.create_tag(identifier, name)

        all_tags = self.rest_catalog.list_tags_paged(identifier)
        self.assertEqual(sorted(all_tags.elements), ["t1", "t2", "t3"])

        first_page = self.rest_catalog.list_tags_paged(identifier, max_results=2)
        self.assertEqual(len(first_page.elements), 2)
        self.assertIsNotNone(first_page.next_page_token)

        second_page = self.rest_catalog.list_tags_paged(
            identifier, max_results=2, page_token=first_page.next_page_token)
        self.assertEqual(len(second_page.elements), 1)
        self.assertIsNone(second_page.next_page_token)

    def test_list_tags_paged_with_prefix(self):
        identifier = self._identifier()
        for name in ("prod_v1", "prod_v2", "dev_v1"):
            self.rest_catalog.create_tag(identifier, name)

        result = self.rest_catalog.list_tags_paged(
            identifier, tag_name_prefix="prod_")
        self.assertEqual(sorted(result.elements), ["prod_v1", "prod_v2"])

    def test_create_tag_with_snapshot_id(self):
        identifier = self._identifier()
        self.rest_catalog.create_tag(identifier, "t1", snapshot_id=1)
        response = self.rest_catalog.get_tag(identifier, "t1")
        self.assertIsNotNone(response.snapshot)
        self.assertEqual(response.snapshot.id, 1)

    def test_create_tag_with_time_retained(self):
        identifier = self._identifier()
        self.rest_catalog.create_tag(identifier, "t1", time_retained="1d")
        response = self.rest_catalog.get_tag(identifier, "t1")
        self.assertEqual(response.tag_time_retained, "1d")

    def test_create_tag_already_exists_raises(self):
        identifier = self._identifier()
        self.rest_catalog.create_tag(identifier, "t1")
        with self.assertRaises(TagAlreadyExistException):
            self.rest_catalog.create_tag(identifier, "t1")

    def test_create_tag_already_exists_ignore(self):
        identifier = self._identifier()
        self.rest_catalog.create_tag(identifier, "t1")
        # ignore_if_exists=True swallows the conflict; no exception raised.
        self.rest_catalog.create_tag(identifier, "t1", ignore_if_exists=True)
        # Still only one tag.
        result = self.rest_catalog.list_tags_paged(identifier)
        self.assertEqual(result.elements, ["t1"])

    def test_create_tag_table_not_exists(self):
        with self.assertRaises(TableNotExistException):
            self.rest_catalog.create_tag(
                Identifier.from_string("default.no_such_table"), "t1")

    def test_get_tag_not_exists(self):
        identifier = self._identifier()
        with self.assertRaises(TagNotExistException):
            self.rest_catalog.get_tag(identifier, "absent")

    def test_delete_tag(self):
        identifier = self._identifier()
        self.rest_catalog.create_tag(identifier, "t1")
        self.rest_catalog.delete_tag(identifier, "t1")
        result = self.rest_catalog.list_tags_paged(identifier)
        self.assertNotIn("t1", result.elements)

    def test_delete_tag_not_exists(self):
        identifier = self._identifier()
        with self.assertRaises(TagNotExistException):
            self.rest_catalog.delete_tag(identifier, "absent")


# Note: the previous ``FilesystemCatalogTagInheritsNotImplementedTest`` class
# has been removed because FileSystemCatalog now overrides the tag CRUD
# methods (no longer raises NotImplementedError). The new behavior is
# covered by ``pypaimon/tests/filesystem_catalog_tag_test.py``.


if __name__ == "__main__":
    unittest.main()
