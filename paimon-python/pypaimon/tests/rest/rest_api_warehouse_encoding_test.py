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
from urllib.parse import parse_qs, urlsplit

from pypaimon.api.api_response import ConfigResponse
from pypaimon.api.rest_api import RESTApi
from pypaimon.common.json_util import JSON


class RESTApiWarehouseEncodingTest(unittest.TestCase):

    server = None

    def tearDown(self):
        if self.server is not None:
            self.server.shutdown()
            self.server.server_close()

    def test_warehouse_query_parameter_not_double_encoded(self):
        warehouse = "file:///tmp/paimon-warehouse"
        config = JSON.to_json(ConfigResponse(
            defaults={"warehouse": warehouse, "prefix": "paimon"}, overrides={}))
        received = []

        class Handler(BaseHTTPRequestHandler):

            def log_message(self, *args):
                pass

            def do_GET(self):
                received.append(self.path)
                body = config.encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

        RESTApi({
            "uri": "http://127.0.0.1:{}".format(self.server.server_port),
            "warehouse": warehouse,
            "token": "token",
            "token.provider": "bear",
        })

        received_warehouse = parse_qs(urlsplit(received[0]).query)["warehouse"][0]
        self.assertEqual(warehouse, received_warehouse, "warehouse query param was double-encoded")


if __name__ == '__main__':
    unittest.main()
