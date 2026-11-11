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

from pypaimon.api.resource_paths import ResourcePaths
from pypaimon.api.rest_api import RESTApi
from pypaimon.api.rest_exception import (AlreadyExistsException,
                                         NoSuchResourceException)
from pypaimon.api.rest_policy_management import RESTPolicyManagement
from pypaimon.common.json_util import JSON
from pypaimon.management.column_mask import ColumnMask
from pypaimon.management.data_policy import DataPolicy
from pypaimon.management.list_policies_request import ListPoliciesRequest
from pypaimon.management.permission_resource import PermissionResource
from pypaimon.management.policy_management import PolicyAlreadyExistException
from pypaimon.management.policy_type import PolicyType
from pypaimon.management.resource_type import ResourceType

COLLECTION_PATH = "/v1/catalog+id/databases/sales/tables/orders/policies"
DROP_PATH = COLLECTION_PATH + "/drop"


def table_resource():
    return PermissionResource(ResourceType.TABLE, "sales", "orders", None, None)


def policy():
    return DataPolicy.column_mask(
        table_resource(),
        ColumnMask("email", '{"name":"FIELD_REF","fieldRef":{"index":0,'
                            '"name":"region","type":"STRING"}}'),
        "analyst")


class RESTPolicyManagementTest(unittest.TestCase):

    def setUp(self):
        recorded = self.recorded = {"drop_calls": 0, "create_error": None, "drop_error": None}
        list_response = '{"policies":[' + JSON.to_json(policy()) + '],"nextPageToken":"next"}'

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
                url = urlsplit(self.path)
                if url.path == COLLECTION_PATH:
                    recorded["list_query"] = dict(parse_qsl(url.query, keep_blank_values=True))
                    self.respond(200, list_response)
                else:
                    self.respond(404, '{"message":"missing","code":404}')

            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("Content-Length", 0))).decode("utf-8")
                path = urlsplit(self.path).path
                if path == COLLECTION_PATH:
                    recorded["create_body"] = body
                    error = recorded["create_error"]
                    self.respond(409, error) if error else self.respond(200)
                elif path == DROP_PATH:
                    recorded["drop_body"] = body
                    recorded["drop_calls"] += 1
                    error = recorded["drop_error"]
                    self.respond(404, error) if error else self.respond(200)
                else:
                    self.respond(404, '{"message":"missing","code":404}')

        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        options = {
            "uri": "http://127.0.0.1:{}".format(self.server.server_port),
            "token.provider": "bear",
            "token": "secret",
            "prefix": "catalog id",
        }
        self.api = RESTApi(options, False)
        self.management = RESTPolicyManagement(self.api)

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()

    def drop(self, ignore_if_not_exists):
        dropped = policy()
        self.management.drop_policy(
            dropped.get_resource(), dropped.type(), dropped.get_principal(),
            dropped.get_column_mask().get_on_column(), ignore_if_not_exists)

    def test_policies_are_nested_under_attachment_resource(self):
        paths = ResourcePaths("catalog/id")
        catalog = PermissionResource(ResourceType.CATALOG, None, None, None, None)
        database = PermissionResource(ResourceType.DATABASE, "sales db", None, None, None)
        table = PermissionResource(ResourceType.TABLE, "sales db", "orders/all", None, None)

        for resource in (catalog, database):
            with self.assertRaisesRegex(ValueError, "TABLE"):
                paths.policies(resource)
        self.assertEqual(
            "/v1/catalog%2Fid/databases/sales+db/tables/orders%2Fall/policies",
            paths.policies(table))
        self.assertEqual(
            "/v1/catalog%2Fid/databases/sales+db/tables/orders%2Fall/policies/drop",
            paths.drop_policy(table))

    def test_list_uses_resource_nested_path_and_identity_filters(self):
        policies = self.management.list_policies(ListPoliciesRequest(
            table_resource(), PolicyType.COLUMN_MASKING, "analyst", "email", "start", 25))

        self.assertEqual(1, len(policies.elements))
        self.assertEqual("next", policies.next_page_token)
        self.assertEqual(
            {"type": "COLUMN_MASKING", "principal": "analyst", "column": "email",
             "maxResults": "25", "pageToken": "start"},
            self.recorded["list_query"])

    def test_list_sends_a_whitespace_page_token_verbatim(self):
        request = ListPoliciesRequest(table_resource(), page_token=" \t")
        self.management.list_policies(request)
        self.assertEqual({"pageToken": " \t"}, self.recorded["list_query"])

        self.management.list_policies(request.with_page_token(""))
        self.assertEqual({}, self.recorded["list_query"])

    def test_create_and_drop_use_post_endpoints(self):
        self.management.create_policy(policy())
        self.drop(False)

        create = json.loads(self.recorded["create_body"])
        self.assertEqual("analyst", create["principal"])
        self.assertNotIn("resource", create)
        drop = json.loads(self.recorded["drop_body"])
        self.assertEqual("COLUMN_MASKING", drop["type"])
        self.assertEqual("email", drop["column"])
        self.assertEqual(1, self.recorded["drop_calls"])

    def test_create_maps_only_policy_conflict(self):
        self.recorded["create_error"] = (
            '{"resourceType":"POLICY","resourceName":"COLUMN_MASKING:analyst:email",'
            '"message":"already exists","code":409}')
        with self.assertRaisesRegex(PolicyAlreadyExistException, r"COLUMN_MASKING\(email\)") as ctx:
            self.management.create_policy(policy())
        self.assertIsInstance(ctx.exception.__cause__, AlreadyExistsException)

        self.recorded["create_error"] = (
            '{"resourceType":"TABLE","resourceName":"orders",'
            '"message":"table conflict","code":409}')
        with self.assertRaisesRegex(AlreadyExistsException, "table conflict"):
            self.management.create_policy(policy())

    def test_drop_if_exists_only_ignores_missing_policy(self):
        self.recorded["drop_error"] = (
            '{"resourceType":"POLICY","resourceName":"COLUMN_MASKING:analyst:email",'
            '"message":"missing","code":404}')
        self.drop(True)
        with self.assertRaises(NoSuchResourceException):
            self.drop(False)

        self.recorded["drop_error"] = (
            '{"resourceType":"TABLE","resourceName":"orders",'
            '"message":"missing table","code":404}')
        with self.assertRaisesRegex(NoSuchResourceException, "missing table"):
            self.drop(True)


if __name__ == '__main__':
    unittest.main()
