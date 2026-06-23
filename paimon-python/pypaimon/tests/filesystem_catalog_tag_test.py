# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""End-to-end tests for tag CRUD on ``FileSystemCatalog``.

Mirrors the ``RESTCatalogTagCRUDTest`` matrix from the REST tag tests
but exercises the local filesystem path. The tests pin down the
exception types and the ``GetTagResponse`` / ``PagedList`` return shapes
that the catalog layer must produce regardless of which catalog
implementation is in use.
"""

import json
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
        # A tag created without a retention carries no create-time / TTL: the
        # file is a plain Snapshot JSON, so both surface as None.
        self.assertIsNone(response.tag_create_time)
        self.assertIsNone(response.tag_time_retained)

    def test_create_tag_without_ttl_writes_plain_snapshot(self):
        # No-TTL tags must be written as plain Snapshot JSON (no tag-specific
        # fields) so the file stays readable by older / Java readers.
        self.catalog.create_tag(self.identifier, "t1")
        table = self.catalog.get_table(self.identifier)
        tag_path = table.tag_manager().tag_path("t1")
        content = json.loads(table.file_io.read_file_utf8(tag_path))
        self.assertNotIn("tagCreateTime", content)
        self.assertNotIn("tagTimeRetained", content)

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

    def test_create_tag_with_time_retained(self):
        self.catalog.create_tag(self.identifier, "t1", time_retained="1d")
        response = self.catalog.get_tag(self.identifier, "t1")
        # create_time surfaces as epoch millis, time_retained as an ISO-8601
        # duration string (matching Java's Duration.toString()).
        self.assertIsNotNone(response.tag_create_time)
        self.assertIsInstance(response.tag_create_time, int)
        self.assertEqual(response.tag_time_retained, "PT24H")

    def test_create_tag_with_time_retained_writes_java_compatible_json(self):
        self.catalog.create_tag(self.identifier, "t1", time_retained="1d")
        table = self.catalog.get_table(self.identifier)
        tag_path = table.tag_manager().tag_path("t1")
        content = json.loads(table.file_io.read_file_utf8(tag_path))
        # On-disk shape must match Java: LocalDateTime array + Duration seconds.
        self.assertIsInstance(content["tagCreateTime"], list)
        self.assertEqual(len(content["tagCreateTime"]), 7)
        self.assertEqual(content["tagTimeRetained"], 86400.0)

    def test_create_tag_with_sub_millisecond_time_retained_preserved(self):
        # A sub-millisecond but microsecond-representable retention must be kept
        # (500us -> 0.0005s / PT0.0005S), not rounded to a zero-TTL tag.
        self.catalog.create_tag(self.identifier, "t1", time_retained="500micro")
        table = self.catalog.get_table(self.identifier)
        tag_path = table.tag_manager().tag_path("t1")
        content = json.loads(table.file_io.read_file_utf8(tag_path))
        self.assertEqual(content["tagTimeRetained"], 0.0005)
        response = self.catalog.get_tag(self.identifier, "t1")
        self.assertEqual(response.tag_time_retained, "PT0.0005S")

    def test_create_tag_with_sub_microsecond_time_retained_rejected(self):
        # A retention finer than a microsecond cannot be represented by Python's
        # timedelta; it must raise instead of silently writing a zero-TTL tag,
        # and no tag file should be left behind.
        for retained in ("1ns", "999ns"):
            with self.assertRaises(ValueError):
                self.catalog.create_tag(
                    self.identifier, "t1", time_retained=retained)
            with self.assertRaises(TagNotExistException):
                self.catalog.get_tag(self.identifier, "t1")

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

    # -- replace_tag -----------------------------------------------------------

    def test_replace_tag_with_snapshot_id(self):
        table = self.catalog.get_table(self.identifier)
        # Create a second snapshot
        wb = table.new_batch_write_builder()
        w = wb.new_write()
        w.write_arrow(pa.Table.from_pydict(
            {"id": [4, 5], "value": ["d", "e"]},
            schema=self.pa_schema,
        ))
        wb.new_commit().commit(w.prepare_commit())
        w.close()

        # Create tag pointing to snapshot 1
        table.create_tag("replace_test", snapshot_id=1)
        tag = table.tag_manager().get("replace_test")
        self.assertEqual(tag.trim_to_snapshot().id, 1)

        # Replace tag to point to snapshot 2
        table.replace_tag("replace_test", snapshot_id=2)
        tag = table.tag_manager().get("replace_test")
        self.assertEqual(tag.trim_to_snapshot().id, 2)

    def test_replace_tag_with_latest_snapshot(self):
        table = self.catalog.get_table(self.identifier)
        # Create a second snapshot
        wb = table.new_batch_write_builder()
        w = wb.new_write()
        w.write_arrow(pa.Table.from_pydict(
            {"id": [4], "value": ["d"]},
            schema=self.pa_schema,
        ))
        wb.new_commit().commit(w.prepare_commit())
        w.close()

        # Create tag pointing to snapshot 1
        table.create_tag("latest_test", snapshot_id=1)
        # Replace with latest (should be snapshot 2)
        table.replace_tag("latest_test")
        tag = table.tag_manager().get("latest_test")
        self.assertEqual(tag.trim_to_snapshot().id, 2)

    def test_replace_tag_with_time_retained(self):
        table = self.catalog.get_table(self.identifier)
        table.create_tag("ttl_replace", snapshot_id=1)
        # Replacing with a retention upgrades the plain-snapshot tag into a
        # TTL-carrying tag.
        table.replace_tag("ttl_replace", snapshot_id=1, time_retained="12h")
        response = self.catalog.get_tag(self.identifier, "ttl_replace")
        self.assertIsNotNone(response.tag_create_time)
        self.assertEqual(response.tag_time_retained, "PT12H")

    def test_replace_tag_not_exists_raises(self):
        table = self.catalog.get_table(self.identifier)
        with self.assertRaises(ValueError) as cm:
            table.replace_tag("nonexistent", snapshot_id=1)
        self.assertIn("doesn't exist", str(cm.exception))

    def test_replace_tag_snapshot_not_exists_raises(self):
        table = self.catalog.get_table(self.identifier)
        table.create_tag("exists_tag", snapshot_id=1)
        with self.assertRaises(ValueError) as cm:
            table.replace_tag("exists_tag", snapshot_id=999)
        self.assertIn("doesn't exist", str(cm.exception))


if __name__ == "__main__":
    unittest.main()
