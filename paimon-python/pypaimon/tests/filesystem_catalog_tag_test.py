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

"""End-to-end tests for tag CRUD on ``FileSystemCatalog``.

Mirrors the ``RESTCatalogTagCRUDTest`` matrix from the REST tag tests
but exercises the local filesystem path. The tests pin down the
exception types and the ``GetTagResponse`` / ``PagedList`` return shapes
that the catalog layer must produce regardless of which catalog
implementation is in use.
"""

import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.api.api_response import GetTagResponse, PagedList
from pypaimon.catalog.catalog_exception import (TableNotExistException,
                                                TagAlreadyExistException,
                                                TagNotExistException)
from pypaimon.common.identifier import Identifier
from pypaimon.snapshot.snapshot import Snapshot


class FileSystemCatalogTagCRUDTest(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="unittest_fs_tag_")
        warehouse = os.path.join(self.temp_dir, "warehouse")
        os.makedirs(warehouse, exist_ok=True)
        self.catalog = CatalogFactory.create({"warehouse": warehouse})
        self.catalog.create_database("default", True)

        self.pa_schema = pa.schema([
            ("id", pa.int64()),
            ("value", pa.string()),
        ])
        self.identifier = Identifier.from_string("default.test_tag_table")
        self.catalog.create_table(
            self.identifier,
            Schema.from_pyarrow_schema(self.pa_schema),
            False,
        )
        # Commit one batch so the table has a snapshot to tag.
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

    # -- create + get --------------------------------------------------------

    def test_create_and_get_tag(self):
        self.catalog.create_tag(self.identifier, "t1")
        response = self.catalog.get_tag(self.identifier, "t1")
        self.assertIsInstance(response, GetTagResponse)
        self.assertEqual(response.tag_name, "t1")
        self.assertIsInstance(response.snapshot, Snapshot)
        # FileSystemCatalog does not yet track tag_create_time /
        # tag_time_retained — both must be None until the underlying Tag
        # dataclass is extended.
        self.assertIsNone(response.tag_create_time)
        self.assertIsNone(response.tag_time_retained)

    def test_create_tag_with_snapshot_id(self):
        self.catalog.create_tag(self.identifier, "t1", snapshot_id=1)
        response = self.catalog.get_tag(self.identifier, "t1")
        self.assertIsNotNone(response.snapshot)
        self.assertEqual(response.snapshot.id, 1)

    # -- create error paths --------------------------------------------------

    def test_create_tag_already_exists_raises(self):
        self.catalog.create_tag(self.identifier, "t1")
        with self.assertRaises(TagAlreadyExistException) as cm:
            self.catalog.create_tag(self.identifier, "t1")
        self.assertEqual(cm.exception.tag, "t1")

    def test_create_tag_already_exists_ignore(self):
        self.catalog.create_tag(self.identifier, "t1")
        # ignore_if_exists=True must swallow the conflict, leaving the
        # original tag in place.
        self.catalog.create_tag(self.identifier, "t1", ignore_if_exists=True)
        result = self.catalog.list_tags_paged(self.identifier)
        self.assertEqual(result.elements, ["t1"])

    def test_create_tag_with_time_retained_raises_not_implemented(self):
        with self.assertRaises(NotImplementedError) as cm:
            self.catalog.create_tag(
                self.identifier, "t1", time_retained="1d")
        self.assertIn("time_retained", str(cm.exception))

    def test_create_tag_table_not_exists(self):
        with self.assertRaises(TableNotExistException):
            self.catalog.create_tag(
                Identifier.from_string("default.no_such_table"), "t1")

    # -- get error paths -----------------------------------------------------

    def test_get_tag_not_exists(self):
        with self.assertRaises(TagNotExistException) as cm:
            self.catalog.get_tag(self.identifier, "absent")
        self.assertEqual(cm.exception.tag, "absent")

    # -- delete --------------------------------------------------------------

    def test_delete_tag_happy(self):
        self.catalog.create_tag(self.identifier, "t1")
        self.catalog.delete_tag(self.identifier, "t1")
        self.assertNotIn(
            "t1", self.catalog.list_tags_paged(self.identifier).elements)

    def test_delete_tag_not_exists(self):
        with self.assertRaises(TagNotExistException) as cm:
            self.catalog.delete_tag(self.identifier, "absent")
        self.assertEqual(cm.exception.tag, "absent")

    # -- list_tags_paged -----------------------------------------------------

    def test_list_tags_paged_basic(self):
        for name in ("t1", "t2", "t3"):
            self.catalog.create_tag(self.identifier, name)

        all_tags = self.catalog.list_tags_paged(self.identifier)
        self.assertIsInstance(all_tags, PagedList)
        self.assertEqual(sorted(all_tags.elements), ["t1", "t2", "t3"])
        self.assertIsNone(all_tags.next_page_token)

        first_page = self.catalog.list_tags_paged(
            self.identifier, max_results=2)
        self.assertEqual(len(first_page.elements), 2)
        self.assertIsNotNone(first_page.next_page_token)

        second_page = self.catalog.list_tags_paged(
            self.identifier,
            max_results=2,
            page_token=first_page.next_page_token,
        )
        self.assertEqual(len(second_page.elements), 1)
        self.assertIsNone(second_page.next_page_token)

    def test_list_tags_paged_with_prefix(self):
        for name in ("prod_v1", "prod_v2", "dev_v1"):
            self.catalog.create_tag(self.identifier, name)

        result = self.catalog.list_tags_paged(
            self.identifier, tag_name_prefix="prod_")
        self.assertEqual(sorted(result.elements), ["prod_v1", "prod_v2"])

    def test_list_tags_paged_empty_table(self):
        # Fresh table with no tags created.
        result = self.catalog.list_tags_paged(self.identifier)
        self.assertEqual(result.elements, [])
        self.assertIsNone(result.next_page_token)


if __name__ == "__main__":
    unittest.main()
