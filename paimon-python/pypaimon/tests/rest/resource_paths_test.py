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


import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

from pypaimon.api.auth import DLFAuthProvider
from pypaimon.api.client import HttpClient
from pypaimon.api.resource_paths import ResourcePaths
from pypaimon.api.token_loader import DLFToken
from pypaimon.api.typedef import RESTAuthParameter


class ResourcePathsTest(unittest.TestCase):

    def test_url_encode(self):
        database = "test_db"
        object_name = "test_table$snapshot"
        resource_paths = ResourcePaths("paimon")
        self.assertEqual(
            "/v1/paimon/databases/test_db/tables/test_table%24snapshot",
            resource_paths.table(database, object_name))
        resource_paths = ResourcePaths("paimon/aaaa")
        self.assertEqual(
            "/v1/paimon%2Faaaa/databases/test_db/tables/test_table%24snapshot",
            resource_paths.table(database, object_name))

    def test_database_and_table_names_are_url_encoded(self):
        paths = ResourcePaths("catalog/id")
        self.assertEqual(
            "/v1/catalog%2Fid/databases/sales+db/tables/orders%2Fall",
            paths.table("sales db", "orders/all"))
        self.assertEqual(
            "/v1/catalog%2Fid/databases/sales+db/table-details", paths.table_details("sales db"))

    def test_table_details_signature_matches_the_path_sent(self):
        received = []

        class Handler(BaseHTTPRequestHandler):

            def log_message(self, *args):
                pass

            def do_GET(self):
                received.append((self.path, dict(self.headers)))
                self.send_response(204)
                self.end_headers()

        server = HTTPServer(("127.0.0.1", 0), Handler)
        self.addCleanup(server.server_close)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join)
        self.addCleanup(server.shutdown)
        uri = "http://127.0.0.1:{}".format(server.server_port)
        client = HttpClient(uri)
        self.addCleanup(client.session.close)
        token = DLFToken("test-ak", "test-sk", "test-token", None)
        paths = ResourcePaths("catalog~id")

        for algorithm in ("default", "openapi-v4"):
            provider = DLFAuthProvider(uri, "cn-hangzhou", algorithm, token=token)
            for database, encoded in (("sales~db", "sales~db"),
                                      ("sales~ db/a+b*", "sales~+db%2Fa%2Bb*"),
                                      ("sales%7Edb", "sales%257Edb")):
                with self.subTest(algorithm=algorithm, database=database):
                    signed = []

                    def sign(parameter):
                        signed.append(parameter.path)
                        return provider.merge_auth_header({}, parameter)

                    client.get(paths.table_details(database), None, sign)
                    path, headers = received[-1]
                    self.assertEqual(
                        "/v1/catalog~id/databases/{}/table-details".format(encoded), path)
                    expected = provider.signer.authorization(
                        RESTAuthParameter("GET", path, "", {}), token,
                        provider.extract_host(uri), headers)
                    self.assertEqual(expected, headers["Authorization"])
                    self.assertEqual([path], signed)


if __name__ == '__main__':
    unittest.main()
