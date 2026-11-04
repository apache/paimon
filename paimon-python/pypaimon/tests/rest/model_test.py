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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

from pypaimon.api.model import (
    CreateModelRequest,
    GetModelResponse,
    ListModelsResponse,
)
from pypaimon.api.rest_api import RESTApi
from pypaimon.catalog.catalog_exception import (
    DatabaseNotExistException,
    ModelAlreadyExistException,
    ModelNotExistException,
)
from pypaimon.catalog.model import Model
from pypaimon.catalog.rest.rest_catalog import RESTCatalog
from pypaimon.common.identifier import Identifier
from pypaimon.common.json_util import JSON


def _inputs():
    return {
        "video": {"data_type": "BLOB", "modality": "video"},
        "asr": {"data_type": "STRING"},
    }


def _response():
    return GetModelResponse(
        name="qwen_vl",
        provider="bailian",
        model="qwen3.8-flash",
        inputs=_inputs(),
        properties={"enable_thinking": False},
    )


class ModelTest(unittest.TestCase):

    def test_model_metadata(self):
        model = Model(
            "quality.qwen_vl",
            "bailian",
            "qwen3.8-flash",
            _inputs(),
            properties={"enable_thinking": False},
        )
        self.assertEqual("quality.qwen_vl", model.full_name)
        self.assertEqual("qwen_vl", model.name)
        self.assertEqual("BLOB", model.to_dict()["inputs"]["video"]["data_type"])

    def test_wire_format(self):
        request = CreateModelRequest(
            "qwen_vl",
            "bailian",
            "qwen3.8-flash",
            _inputs(),
            properties={"enable_thinking": False},
        )
        body = json.loads(JSON.to_json(request))
        self.assertEqual("BLOB", body["inputs"]["video"]["dataType"])
        self.assertNotIn("data_type", body["inputs"]["video"])
        self.assertNotIn("outputs", body)

        response = JSON.from_json(JSON.to_json(_response()), GetModelResponse)
        self.assertEqual("BLOB", response.inputs["video"]["data_type"])
        self.assertEqual("quality.qwen_vl", response.to_model("quality").full_name)

        listed = JSON.from_json(
            JSON.to_json(ListModelsResponse([_response()], "next")),
            ListModelsResponse,
        )
        self.assertEqual("next", listed.next_page_token)
        self.assertEqual("qwen_vl", listed.models[0].name)

    def test_validation(self):
        invalid = [
            {"provider": ""},
            {"model": " "},
            {"inputs": {}},
            {"inputs": {"video": {}}},
            {"inputs": {"video": {"data_type": ""}}},
            {"properties": []},
        ]
        for changes in invalid:
            values = {
                "name": "qwen_vl",
                "provider": "bailian",
                "model": "qwen3.8-flash",
                "inputs": _inputs(),
            }
            values.update(changes)
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                CreateModelRequest(**values)

    def test_rest_catalog_crud_and_errors(self):
        calls = []
        state = {"status": 200, "resource_type": None}

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _send(self, payload=None):
                if state["status"] != 200:
                    payload = {
                        "code": state["status"],
                        "message": "failed",
                        "resourceType": state["resource_type"],
                    }
                if payload is None:
                    body = b""
                elif isinstance(payload, dict):
                    body = json.dumps(payload).encode()
                else:
                    body = JSON.to_json(payload).encode()
                self.send_response(state["status"])
                if body:
                    self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                if body:
                    self.wfile.write(body)

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                calls.append(("POST", self.path, body))
                self._send(_response())

            def do_GET(self):
                calls.append(("GET", self.path, None))
                if self.path.endswith("/models"):
                    self._send(ListModelsResponse([_response()], None))
                else:
                    self._send(_response())

            def do_DELETE(self):
                calls.append(("DELETE", self.path, None))
                self._send()

        server = HTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            api = RESTApi(
                {
                    "uri": "http://127.0.0.1:%s" % server.server_port,
                    "token.provider": "bear",
                    "token": "test",
                    "prefix": "catalog",
                },
                False,
            )
            catalog = object.__new__(RESTCatalog)
            catalog.rest_api = api

            created = catalog.create_model(
                "a/b.qwen_vl",
                "bailian",
                "qwen3.8-flash",
                _inputs(),
                properties={"enable_thinking": False},
            )
            self.assertEqual("a/b.qwen_vl", created.full_name)
            self.assertEqual("/v1/catalog/databases/a%2Fb/models", calls[-1][1])
            self.assertEqual("BLOB", calls[-1][2]["inputs"]["video"]["dataType"])

            self.assertEqual("qwen_vl", catalog.get_model("a/b.qwen_vl").name)
            self.assertEqual("/v1/catalog/databases/a%2Fb/models/qwen_vl", calls[-1][1])
            self.assertEqual(["qwen_vl"], [model.name for model in catalog.list_models("a/b")])
            catalog.drop_model(Identifier.create("a/b", "qwen_vl"))

            state.update(status=409, resource_type="MODEL")
            with self.assertRaises(ModelAlreadyExistException):
                catalog.create_model(
                    "quality.qwen_vl", "bailian", "qwen3.8-flash", _inputs()
                )

            state.update(status=404, resource_type="MODEL")
            with self.assertRaises(ModelNotExistException):
                catalog.get_model("quality.missing")

            state.update(status=404, resource_type="DATABASE")
            with self.assertRaises(DatabaseNotExistException):
                catalog.get_model("missing.qwen_vl")

            state.update(status=501, resource_type=None)
            with self.assertRaises(NotImplementedError):
                catalog.list_models("quality")
        finally:
            server.shutdown()
            server.server_close()
            thread.join()


if __name__ == "__main__":
    unittest.main()
