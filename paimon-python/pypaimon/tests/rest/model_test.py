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
from unittest.mock import Mock

import pytest

from pypaimon.api.model import (
    CreateModelRequest,
    GetModelResponse,
    ListModelsResponse,
)
from pypaimon.api.rest_api import RESTApi
from pypaimon.api.rest_exception import (
    AlreadyExistsException,
    NoSuchResourceException,
)
from pypaimon.catalog.catalog_exception import (
    DatabaseNotExistException,
    ModelAlreadyExistException,
    ModelNotExistException,
)
from pypaimon.catalog.model import (
    Model,
    ModelField,
    ProviderModelDefinition,
)
from pypaimon.catalog.rest.rest_catalog import RESTCatalog
from pypaimon.common.identifier import Identifier
from pypaimon.common.json_util import JSON


def _inputs():
    return [
        ModelField("video", "BLOB", "video"),
        ModelField("asr", "STRING"),
    ]


def _definition(outputs=None):
    return ProviderModelDefinition(
        provider="bailian",
        provider_model="qwen3.8-flash",
        inputs=_inputs(),
        outputs=outputs,
        options={"enable_thinking": "false"},
    )


def _response():
    return GetModelResponse(
        name="qwen_vl",
        comment="Video quality model",
        definition=_definition(),
    )


class ModelTest(unittest.TestCase):

    def test_model_metadata(self):
        model = Model(
            "quality.qwen_vl",
            _definition(),
            comment="Video quality model",
        )
        self.assertEqual("quality.qwen_vl", model.full_name)
        self.assertEqual("qwen_vl", model.name)
        self.assertEqual(
            "BLOB",
            model.to_dict()["definition"]["inputs"][0]["data_type"],
        )

    def test_wire_format(self):
        request = CreateModelRequest(
            "qwen_vl",
            _definition(),
            comment="Video quality model",
        )
        body = json.loads(JSON.to_json(request))
        self.assertEqual("provider", body["definition"]["type"])
        self.assertEqual(
            "BLOB", body["definition"]["inputs"][0]["dataType"])
        self.assertNotIn("data_type", body["definition"]["inputs"][0])
        self.assertNotIn("outputs", body["definition"])

        response_body = json.loads(JSON.to_json(_response()))
        self.assertNotIn("outputs", response_body["definition"])
        response = JSON.from_json(
            json.dumps(response_body), GetModelResponse)
        definition = response.definition
        self.assertIsInstance(definition, ProviderModelDefinition)
        self.assertEqual("BLOB", definition.inputs[0].data_type)
        self.assertEqual(
            "quality.qwen_vl", response.to_model("quality").full_name)

        listed = JSON.from_json(
            JSON.to_json(ListModelsResponse([_response()], "next")),
            ListModelsResponse,
        )
        self.assertEqual("next", listed.next_page_token)
        self.assertEqual("qwen_vl", listed.models[0].name)

        empty = JSON.from_json("{}", ListModelsResponse)
        self.assertEqual([], empty.data())
        self.assertEqual([], json.loads(JSON.to_json(empty))["models"])

    def test_validation(self):
        invalid_definitions = [
            {
                "type": "provider",
                "provider": "",
                "provider_model": "model",
                "inputs": [{"name": "text", "data_type": "STRING"}],
            },
            {
                "type": "provider",
                "provider": "provider",
                "provider_model": " ",
                "inputs": [{"name": "text", "data_type": "STRING"}],
            },
            {
                "type": "provider",
                "provider": "provider",
                "provider_model": "model",
                "inputs": [],
            },
            {
                "type": "provider",
                "provider": "provider",
                "provider_model": "model",
                "inputs": [{"name": "text"}],
            },
            {
                "type": "provider",
                "provider": "provider",
                "provider_model": "model",
                "inputs": [
                    {"name": "text", "data_type": "STRING"},
                    {"name": "text", "data_type": "STRING"},
                ],
            },
            {
                "type": "provider",
                "provider": "provider",
                "provider_model": "model",
                "inputs": [{"name": "text", "data_type": "STRING"}],
                "options": {"thinking": False},
            },
            {"type": "resource"},
        ]
        for definition in invalid_definitions:
            with self.subTest(definition=definition), self.assertRaises(
                    ValueError):
                CreateModelRequest("qwen_vl", definition)

        with self.assertRaises(ValueError):
            CreateModelRequest("qwen_vl", _definition(), comment=" ")

    def test_rest_catalog_crud_and_errors(self):
        calls = []
        content_types = []
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
                body = json.loads(
                    self.rfile.read(int(self.headers["Content-Length"])))
                content_types.append(self.headers.get("Content-Type"))
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
                _definition(),
                comment="Video quality model",
            )
            self.assertEqual("a/b.qwen_vl", created.full_name)
            self.assertEqual(
                "/v1/catalog/databases/a%2Fb/models", calls[-1][1])
            self.assertEqual(
                "BLOB",
                calls[-1][2]["definition"]["inputs"][0]["dataType"],
            )
            self.assertEqual("application/json", content_types[-1])

            self.assertEqual(
                "qwen_vl", catalog.get_model("a/b.qwen_vl").name)
            self.assertEqual(
                "/v1/catalog/databases/a%2Fb/models/qwen_vl",
                calls[-1][1],
            )
            self.assertEqual(
                ["qwen_vl"],
                [model.name for model in catalog.list_models("a/b")],
            )
            catalog.drop_model(Identifier.create("a/b", "qwen_vl"))

            catalog.create_model("quality.123", _definition())
            self.assertEqual(
                "/v1/catalog/databases/quality/models", calls[-1][1])
            catalog.get_model("quality.123")
            self.assertEqual(
                "/v1/catalog/databases/quality/models/123", calls[-1][1])
            catalog.drop_model("quality.123")
            self.assertEqual(
                "/v1/catalog/databases/quality/models/123", calls[-1][1])

            state.update(status=409, resource_type="MODEL")
            with self.assertRaises(ModelAlreadyExistException):
                catalog.create_model("quality.qwen_vl", _definition())

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

    def test_idempotent_create_and_drop(self):
        api = Mock()
        catalog = object.__new__(RESTCatalog)
        catalog.rest_api = api
        identifier = Identifier.create("quality", "qwen_vl")

        api.create_model.side_effect = AlreadyExistsException(
            "MODEL", "qwen_vl", "already exists"
        )
        api.get_model.return_value = _response()
        existing = catalog.create_model(
            identifier,
            _definition(),
            ignore_if_exists=True,
        )
        self.assertEqual("quality.qwen_vl", existing.full_name)
        api.get_model.assert_called_once_with(identifier)

        api.drop_model.side_effect = NoSuchResourceException(
            "MODEL", "qwen_vl", "not found"
        )
        with self.assertRaises(ModelNotExistException):
            catalog.drop_model(identifier)
        api.drop_model.reset_mock()
        catalog.drop_model(identifier, ignore_if_not_exists=True)
        api.drop_model.assert_called_once_with(identifier)

        api.drop_model.side_effect = NoSuchResourceException(
            "DATABASE", "quality", "not found"
        )
        with self.assertRaises(DatabaseNotExistException):
            catalog.drop_model(identifier, ignore_if_not_exists=True)

        api.drop_model.side_effect = NoSuchResourceException(
            "", "", "route not found"
        )
        with self.assertRaises(NoSuchResourceException):
            catalog.drop_model(identifier, ignore_if_not_exists=True)

    def test_model_paths_reject_dot_segments_before_sending(self):
        api = RESTApi(
            {
                "uri": "http://127.0.0.1:1",
                "token.provider": "bear",
                "token": "test",
                "prefix": "catalog",
            },
            False,
        )
        api.client = Mock()
        request = CreateModelRequest("qwen_vl", _definition())

        for segment in (".", ".."):
            with self.subTest(
                    segment=segment,
                    operation="list"), self.assertRaises(ValueError):
                api.list_models(segment)

            invalid_model = Identifier.create("quality", segment)
            invalid_request = CreateModelRequest(segment, _definition())
            with self.subTest(
                    segment=segment,
                    operation="create"), self.assertRaises(ValueError):
                api.create_model(invalid_model, invalid_request)
            with self.subTest(
                    segment=segment,
                    operation="get"), self.assertRaises(ValueError):
                api.get_model(invalid_model)
            with self.subTest(
                    segment=segment,
                    operation="drop"), self.assertRaises(ValueError):
                api.drop_model(invalid_model)

            invalid_database = Identifier.create(segment, "qwen_vl")
            with self.subTest(
                    segment=segment,
                    operation="database"), self.assertRaises(ValueError):
                api.create_model(invalid_database, request)

        api.client.get.assert_not_called()
        api.client.get_with_params.assert_not_called()
        api.client.post_with_response_type.assert_not_called()
        api.client.delete.assert_not_called()

    def test_list_models_follows_cursor_after_empty_page(self):
        api = RESTApi(
            {
                "uri": "http://127.0.0.1:1",
                "token.provider": "bear",
                "token": "test",
                "prefix": "catalog",
            },
            False,
        )
        api.client = Mock()
        api.client.get_with_params.side_effect = [
            ListModelsResponse([], "second"),
            ListModelsResponse([_response()], None),
        ]

        self.assertEqual(
            ["qwen_vl"],
            [model.name for model in api.list_models("quality")],
        )
        self.assertEqual(2, api.client.get_with_params.call_count)
        self.assertEqual(
            {"pageToken": "second"},
            api.client.get_with_params.call_args_list[1][0][1],
        )


def test_model_crud_round_trip_with_rest_catalog_server(native_rest_catalog):
    identifier = "default.qwen_vl"
    outputs = [ModelField("result", "STRING")]

    created = native_rest_catalog.create_model(
        identifier,
        _definition(outputs),
        comment="Video quality model",
    )
    assert created.full_name == identifier
    persisted = native_rest_catalog.get_model(identifier).definition
    assert isinstance(persisted, ProviderModelDefinition)
    assert persisted.outputs == outputs
    assert [model.name for model in native_rest_catalog.list_models(
        "default")] == ["qwen_vl"]

    with pytest.raises(ModelAlreadyExistException):
        native_rest_catalog.create_model(identifier, _definition())
    assert native_rest_catalog.create_model(
        identifier,
        _definition(),
        ignore_if_exists=True,
    ).full_name == identifier

    native_rest_catalog.drop_model(identifier)
    with pytest.raises(ModelNotExistException):
        native_rest_catalog.get_model(identifier)
    native_rest_catalog.drop_model(identifier, ignore_if_not_exists=True)

    native_rest_catalog.drop_database("default")
    with pytest.raises(DatabaseNotExistException):
        native_rest_catalog.get_model(identifier)


if __name__ == "__main__":
    unittest.main()
