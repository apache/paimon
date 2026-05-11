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

"""DTO and resource-path unit tests for Catalog branch CRUD wire format.

These tests pin the Python wire format to the Java contract so review
against ``paimon-api/.../rest/requests/CreateBranchRequest.java``,
``RenameBranchRequest.java``, ``ForwardBranchRequest.java``,
``responses/ListBranchesResponse.java`` and ``ResourcePaths.java`` does
not need to inspect a running mock server. They explicitly assert the
*exact* JSON field names and URL templates Java uses, and that
``ListBranchesResponse`` is **not** paged (Java's ``listBranches``
returns plain ``List<String>`` — pypaimon mirrors this).
"""

import json
import unittest

from pypaimon.api.api_request import (CreateBranchRequest, ForwardBranchRequest,
                                      RenameBranchRequest)
from pypaimon.api.api_response import ListBranchesResponse
from pypaimon.api.resource_paths import ResourcePaths
from pypaimon.common.json_util import JSON


class CreateBranchRequestSerdeTest(unittest.TestCase):

    def test_with_tag_uses_java_field_names(self):
        request = CreateBranchRequest(branch="b1", from_tag="t1")
        parsed = json.loads(JSON.to_json(request))
        # Field names are Java's ``branch`` / ``fromTag`` — not ``branch_name``.
        self.assertEqual(parsed["branch"], "b1")
        self.assertEqual(parsed["fromTag"], "t1")
        self.assertEqual(set(parsed.keys()), {"branch", "fromTag"})

    def test_without_tag_serializes_null_from_tag(self):
        request = CreateBranchRequest(branch="b1")
        parsed = json.loads(JSON.to_json(request))
        self.assertEqual(parsed["branch"], "b1")
        # ``fromTag`` is included as null when not set (matches Java
        # @Nullable behavior; jackson serializes nullable Long as null).
        self.assertIsNone(parsed.get("fromTag"))


class RenameBranchRequestSerdeTest(unittest.TestCase):

    def test_uses_to_branch_field(self):
        request = RenameBranchRequest(to_branch="b2")
        parsed = json.loads(JSON.to_json(request))
        self.assertEqual(parsed, {"toBranch": "b2"})


class ForwardBranchRequestSerdeTest(unittest.TestCase):

    def test_serializes_empty_object(self):
        # Java's ForwardBranchRequest is an empty body. The Python wire DTO
        # must serialize to {} (not null, not an array, not absent).
        rendered = JSON.to_json(ForwardBranchRequest())
        self.assertEqual(json.loads(rendered), {})


class ListBranchesResponseSerdeTest(unittest.TestCase):

    def test_from_json_populates_branches(self):
        response = JSON.from_json(
            json.dumps({"branches": ["b1", "b2"]}),
            ListBranchesResponse,
        )
        self.assertEqual(response.branches, ["b1", "b2"])

    def test_response_is_not_paged(self):
        # ListBranchesResponse must NOT be a paged response — Java's
        # ListBranchesResponse has no ``nextPageToken``. This locks down
        # the contract; if a future change accidentally extends it to
        # PagedResponse this test fails.
        response = ListBranchesResponse(branches=["b1"])
        self.assertFalse(hasattr(response, "next_page_token"))
        # Serialized form must not contain the paging key.
        self.assertNotIn("nextPageToken", JSON.to_json(response))


class ResourcePathsBranchesTest(unittest.TestCase):

    def setUp(self):
        self.paths = ResourcePaths(prefix="mock")

    def test_branches_collection_url(self):
        self.assertEqual(
            self.paths.branches("db", "tbl"),
            "/v1/mock/databases/db/tables/tbl/branches",
        )

    def test_single_branch_url(self):
        self.assertEqual(
            self.paths.branch("db", "tbl", "b1"),
            "/v1/mock/databases/db/tables/tbl/branches/b1",
        )

    def test_rename_branch_url(self):
        self.assertEqual(
            self.paths.rename_branch("db", "tbl", "b1"),
            "/v1/mock/databases/db/tables/tbl/branches/b1/rename",
        )

    def test_forward_branch_url(self):
        self.assertEqual(
            self.paths.forward_branch("db", "tbl", "b1"),
            "/v1/mock/databases/db/tables/tbl/branches/b1/forward",
        )

    def test_branch_url_url_encodes_branch_name(self):
        # RESTUtil.encode_string escapes characters that are not URL-safe;
        # a space round-trips to ``%20``. Mirrors the existing tag-name path.
        self.assertEqual(
            self.paths.branch("db", "tbl", "release 1.0"),
            "/v1/mock/databases/db/tables/tbl/branches/release%201.0",
        )


if __name__ == "__main__":
    unittest.main()
