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
from unittest.mock import Mock

from pypaimon.api.predict import PredictRequest, PredictResponse
from pypaimon.api.rest_api import RESTApi
from pypaimon.api.rest_exception import NoSuchResourceException, ServiceUnavailableException
from pypaimon.catalog.rest.rest_catalog import RESTCatalog
from pypaimon.common.identifier import Identifier
from pypaimon.common.json_util import JSON
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.multimodal.table import MultimodalTable
from pypaimon.table.file_store_table import FileStoreTable


def request(**kwargs):
    values = dict(model="quality", inputs=[{"column": "content", "modality": "image"}],
                  result_columns=["id"], prompt="Classify")
    values.update(kwargs)
    return PredictRequest(**values)


class PredictTest(unittest.TestCase):
    def test_request_round_trip(self):
        value = request(snapshot_id=7, where="dt = 'today'")
        body = JSON.to_json(value)
        self.assertEqual(value, JSON.from_json(body, PredictRequest))
        self.assertNotIn("tag_name", json.loads(body))
        self.assertNotIn("identifier", json.loads(body))

    def test_invalid_requests(self):
        for changes in [dict(inputs=[]), dict(inputs=[{"column": "x", "modality": "other"}]),
                        dict(inputs=[{"column": "x", "modality": "text", "endpoint": "url"}]),
                        dict(result_columns=["id", "id"]), dict(result_columns="id"),
                        dict(snapshot_id=True), dict(snapshot_id=0), dict(snapshot_id=2 ** 63),
                        dict(model=" "),
                        dict(tag_name="v1", snapshot_id=1), dict(where="")]:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                request(**changes)

    def test_table_view_preserves_snapshot(self):
        for options in ({}, {"scan.snapshot-id": "7"}, {"scan.tag-name": "release"}):
            raw = Mock(spec=FileStoreTable)
            raw.options = CoreOptions(Options(options))
            raw.identifier = Identifier.create("db", "images")
            catalog = Mock()
            catalog.predict.return_value = PredictResponse("job1", "results.job1", 7, "v1")
            table = MultimodalTable(catalog, "db.images", raw)
            result = table.predict(model="quality", inputs=request().inputs,
                                   result_columns=["id"], prompt="Classify")
            sent = catalog.predict.call_args.args[1]
            self.assertEqual(7 if "scan.snapshot-id" in options else None, sent.snapshot_id)
            self.assertEqual(options.get("scan.tag-name"), sent.tag_name)
            self.assertEqual("job1", result["job_id"])
            raw.new_read_builder.assert_not_called()

    def test_unsupported_view_does_not_submit(self):
        raw = Mock(spec=FileStoreTable)
        raw.options = CoreOptions(Options({"scan.timestamp-millis": "1234"}))
        catalog = Mock()
        table = MultimodalTable(catalog, "db.images", raw)
        with self.assertRaises(ValueError):
            table.predict(model="quality", inputs=request().inputs,
                          result_columns=[], prompt="Classify")
        catalog.predict.assert_not_called()
        with self.assertRaises(NotImplementedError):
            MultimodalTable(object(), "db.images", raw).predict(
                model="quality", inputs=[], result_columns=[], prompt="Classify")

    def test_http_submission_and_errors(self):
        calls = []
        state = {"status": 202, "body": JSON.to_json(
            PredictResponse("job1", "results.job1", 7, "v1"))}

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                calls.append((self.path, json.loads(self.rfile.read(
                    int(self.headers["Content-Length"]))), self.headers.get("X-Test")))
                body = state["body"].encode()
                self.send_response(state["status"])
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        server = HTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            api = RESTApi({"uri": "http://127.0.0.1:%s" % server.server_port,
                           "token.provider": "bear", "token": "test",
                           "prefix": "catalog", "header.X-Test": "reuse"}, False)
            catalog = object.__new__(RESTCatalog)
            catalog.rest_api = api
            result = catalog.predict(Identifier.create("a/b", "t x"), request())
            self.assertEqual("job1", result.job_id)
            self.assertEqual("/v1/catalog/databases/a%2Fb/tables/t+x/predict", calls[0][0])
            self.assertEqual("reuse", calls[0][2])
            for status, error in [(501, NotImplementedError), (404, NoSuchResourceException),
                                  (503, ServiceUnavailableException)]:
                state.update(status=status, body=json.dumps({"code": status, "message": "no",
                                                             "resourceType": "TABLE"}))
                with self.assertRaises(error):
                    catalog.predict("db.images", request())
            self.assertEqual(4, len(calls))
        finally:
            server.shutdown()
            server.server_close()
            thread.join()
