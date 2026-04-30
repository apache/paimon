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

"""DTO and resource-path unit tests for the Catalog tag CRUD wire format.

These tests pin the Python wire format to the Java contract so review
against ``paimon-api/.../rest/requests/CreateTagRequest.java``,
``responses/GetTagResponse.java``, ``responses/ListTagsResponse.java``,
and ``ResourcePaths.java`` does not need to inspect a running mock
server. They explicitly assert the *exact* JSON field names and URL
templates Java uses, and that ``ignoreIfExists`` is NOT serialized
(it is a client-side flag — see ``RESTCatalog.createTag``).
"""

import json
import unittest

from pypaimon.api.api_request import CreateTagRequest
from pypaimon.api.api_response import GetTagResponse, ListTagsResponse
from pypaimon.api.resource_paths import ResourcePaths
from pypaimon.common.json_util import JSON
from pypaimon.snapshot.snapshot import Snapshot


class CreateTagRequestSerdeTest(unittest.TestCase):

    def test_to_json_minimal(self):
        request = CreateTagRequest(tag_name="t1")
        rendered = JSON.to_json(request)
        # Field name must be ``tagName`` (Java) — not ``tag_name``.
        self.assertIn('"tagName": "t1"', rendered)
        # Strict alignment: ``ignoreIfExists`` MUST NOT appear in the
        # serialized request. ``ignore_if_exists`` is a client-side flag
        # handled by RESTCatalog and must never be sent over the wire.
        self.assertNotIn("ignoreIfExists", rendered)
        self.assertNotIn("ignore_if_exists", rendered)

    def test_to_json_full_uses_java_field_names(self):
        request = CreateTagRequest(
            tag_name="release-1.0",
            snapshot_id=42,
            time_retained="1d",
        )
        parsed = json.loads(JSON.to_json(request))
        self.assertEqual(parsed["tagName"], "release-1.0")
        self.assertEqual(parsed["snapshotId"], 42)
        self.assertEqual(parsed["timeRetained"], "1d")
        self.assertNotIn("ignoreIfExists", parsed)
        self.assertEqual(set(parsed.keys()), {"tagName", "snapshotId", "timeRetained"})


class GetTagResponseSerdeTest(unittest.TestCase):

    def _snapshot_payload(self):
        return {
            "version": 3,
            "id": 7,
            "schemaId": 0,
            "baseManifestList": "manifest-list-base",
            "deltaManifestList": "manifest-list-delta",
            "totalRecordCount": 100,
            "deltaRecordCount": 100,
            "commitUser": "rest-server",
            "commitIdentifier": 1,
            "commitKind": "APPEND",
            "timeMillis": 12345,
        }

    def test_from_json_populates_all_fields(self):
        payload = json.dumps({
            "tagName": "release-1.0",
            "snapshot": self._snapshot_payload(),
            "tagCreateTime": 99999,
            "tagTimeRetained": "1d",
        })
        response = JSON.from_json(payload, GetTagResponse)
        self.assertEqual(response.tag_name, "release-1.0")
        self.assertIsInstance(response.snapshot, Snapshot)
        self.assertEqual(response.snapshot.id, 7)
        self.assertEqual(response.tag_create_time, 99999)
        self.assertEqual(response.tag_time_retained, "1d")


class ListTagsResponseSerdeTest(unittest.TestCase):

    def test_from_json_populates_data_and_token(self):
        response = JSON.from_json(
            json.dumps({"tags": ["t1", "t2"], "nextPageToken": "tok"}),
            ListTagsResponse,
        )
        self.assertEqual(response.data(), ["t1", "t2"])
        self.assertEqual(response.get_next_page_token(), "tok")

    def test_from_json_empty_payload(self):
        response = JSON.from_json(
            json.dumps({"tags": None, "nextPageToken": None}),
            ListTagsResponse,
        )
        self.assertIn(response.data(), (None, []))
        self.assertIsNone(response.get_next_page_token())


class ResourcePathsTagsTest(unittest.TestCase):

    def test_tags_collection_url(self):
        paths = ResourcePaths(prefix="mock")
        self.assertEqual(
            paths.tags("db", "tbl"),
            "/v1/mock/databases/db/tables/tbl/tags",
        )

    def test_single_tag_url(self):
        paths = ResourcePaths(prefix="mock")
        self.assertEqual(
            paths.tag("db", "tbl", "release-1.0"),
            "/v1/mock/databases/db/tables/tbl/tags/release-1.0",
        )

    def test_single_tag_url_url_encodes_tag_name(self):
        # RESTUtil.encode_string escapes characters that are not URL-safe.
        # A space round-trips to ``%20``; this matches Java's ``RESTUtil``.
        paths = ResourcePaths(prefix="mock")
        url = paths.tag("db", "tbl", "release 1.0")
        self.assertEqual(url, "/v1/mock/databases/db/tables/tbl/tags/release%201.0")


if __name__ == "__main__":
    unittest.main()
