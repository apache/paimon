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

import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qsl, urlsplit

from pypaimon.api.rest_api import RESTApi
from pypaimon.api.rest_exception import ForbiddenException
from pypaimon.api.rest_permission_management import RESTPermissionManagement
from pypaimon.management.list_permissions_request import \
    ListPermissionsRequest
from pypaimon.management.permission_assignment import PermissionAssignment
from pypaimon.management.permission_columns import PermissionColumns
from pypaimon.management.permission_resource import PermissionResource
from pypaimon.management.resource_type import ResourceType

BASE_PATH = "/v1/catalog/permissions"

LIST_RESPONSE = (
    '{"permissions":[{"resource":{"type":"TABLE",'
    '"database":"sales","table":"orders"},'
    '"access":"SELECT",'
    '"principal":"analyst"}],'
    '"nextPageToken":"next"}')


def assignment(principal):
    return PermissionAssignment(
        PermissionResource(ResourceType.TABLE, "sales", "orders", None, None),
        "SELECT",
        principal)


class RESTPermissionManagementTest(unittest.TestCase):

    def setUp(self):
        recorded = self.recorded = {"revoke_calls": 0}

        class Handler(BaseHTTPRequestHandler):

            def log_message(self, *args):
                pass

            def respond(self, code, body=None):
                data = body.encode("utf-8") if body else b""
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self):
                recorded["authorization"] = self.headers.get("Authorization")
                url = urlsplit(self.path)
                if url.path == BASE_PATH:
                    recorded["list_query"] = dict(parse_qsl(url.query, keep_blank_values=True))
                    self.respond(200, LIST_RESPONSE)
                else:
                    self.respond(404, '{"message":"missing","code":404}')

            def do_POST(self):
                recorded["authorization"] = self.headers.get("Authorization")
                body = self.rfile.read(int(self.headers.get("Content-Length", 0))).decode("utf-8")
                path = urlsplit(self.path).path
                if path == BASE_PATH + "/grant":
                    recorded["grant"] = body
                    if "denied" in body:
                        self.respond(403, '{"message":"forbidden","code":403}')
                    else:
                        self.respond(200)
                elif path == BASE_PATH + "/revoke":
                    recorded["revoke"] = body
                    recorded["revoke_calls"] += 1
                    self.respond(200)
                else:
                    self.respond(404, '{"message":"missing","code":404}')

        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        options = {
            "uri": "http://127.0.0.1:{}".format(self.server.server_port),
            "token.provider": "bear",
            "token": "secret",
            "prefix": "catalog",
        }
        self.management = RESTPermissionManagement(RESTApi(options, False))

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()

    def test_list_uses_prefix_and_complete_filters(self):
        page = self.management.list_permissions(ListPermissionsRequest(
            ResourceType.TABLE, "sales", "orders", None, None, "analyst", None, "start", 25))

        self.assertEqual(1, len(page.elements))
        self.assertEqual("analyst", page.elements[0].get_principal())
        self.assertEqual("next", page.next_page_token)
        self.assertEqual(
            {"principal": "analyst", "resourceType": "TABLE", "database": "sales",
             "table": "orders", "maxResults": "25", "pageToken": "start"},
            self.recorded["list_query"])
        self.assertEqual("Bearer secret", self.recorded["authorization"])

    def test_list_sends_a_whitespace_page_token_verbatim(self):
        request = ListPermissionsRequest(ResourceType.CATALOG, page_token=" \t")
        self.management.list_permissions(request)
        self.assertEqual(
            {"resourceType": "CATALOG", "pageToken": " \t"}, self.recorded["list_query"])

        self.management.list_permissions(request.with_page_token(""))
        self.assertEqual({"resourceType": "CATALOG"}, self.recorded["list_query"])

    def test_grant_and_revoke_use_structured_wire_shapes(self):
        granted = assignment("analyst")
        self.management.grant_permission(granted)
        self.management.revoke_permission(
            granted.get_resource(), "select", granted.get_principal())

        grant = json.loads(self.recorded["grant"])
        self.assertEqual(
            {"type": "TABLE", "database": "sales", "table": "orders"}, grant["resource"])
        self.assertEqual("analyst", grant["principal"])
        for absent in ("columns", "policy", "grantOption", "catalog"):
            self.assertNotIn(absent, grant)

        revoke = json.loads(self.recorded["revoke"])
        self.assertEqual(
            {"type": "TABLE", "database": "sales", "table": "orders"}, revoke["resource"])
        self.assertEqual("SELECT", revoke["access"])
        self.assertEqual("analyst", revoke["principal"])
        for absent in ("expireTime", "grantOption"):
            self.assertNotIn(absent, revoke)

    def test_forbidden_grant_preserves_rest_error_translation(self):
        with self.assertRaisesRegex(ForbiddenException, "forbidden"):
            self.management.grant_permission(assignment("denied"))

    def test_column_grant_carries_range_but_revoke_uses_only_identity(self):
        granted = PermissionAssignment(
            PermissionResource(ResourceType.COLUMN, "sales", "orders", None, None),
            "SELECT",
            "analyst",
            PermissionColumns(["id", "region"], None))

        self.management.grant_permission(granted)
        grant = json.loads(self.recorded["grant"])
        self.assertEqual(["id", "region"], grant["columns"]["columnNames"])

        self.management.revoke_permission(
            granted.get_resource(), granted.get_access(), granted.get_principal())
        revoke = json.loads(self.recorded["revoke"])
        self.assertEqual("COLUMN", revoke["resource"]["type"])
        self.assertNotIn("columns", revoke)

    def test_repeated_revoke_is_idempotent(self):
        revoked = assignment("missing")
        for _ in range(2):
            self.management.revoke_permission(
                revoked.get_resource(), revoked.get_access(), revoked.get_principal())

        self.assertEqual(2, self.recorded["revoke_calls"])


if __name__ == '__main__':
    unittest.main()
