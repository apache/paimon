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
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest.mock import Mock, patch

from pypaimon.api.api_response import PagedList
from pypaimon.api.prediction import (
    GetPredictionResponse,
    ListPredictionsResponse,
    PredictionOutput as ApiPredictionOutput,
    PredictionSource,
    SubmitPredictionRequest,
)
from pypaimon.api.rest_api import RESTApi
from pypaimon.catalog.prediction import (
    Prediction,
    PredictionOutput,
    PredictionState,
    PredictionSpec,
    PredictionStatus,
    PredictionTerminationCode,
    PredictionTerminationDetails,
)
from pypaimon.catalog.rest.rest_catalog import RESTCatalog
from pypaimon.common.identifier import Identifier
from pypaimon.common.json_util import JSON
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.multimodal.prediction_wait import PredictionWait
from pypaimon.multimodal.table import MultimodalTable
from pypaimon.table.file_store_table import FileStoreTable


def _spec(**changes):
    values = dict(
        model="quality.qwen_vl",
        inputs={"video": "content"},
        instructions="Classify",
    )
    values.update(changes)
    return PredictionSpec(**values)


def _prediction(status=None, **changes):
    if status is None:
        status = PredictionStatus(PredictionState.PENDING)
    values = dict(
        prediction_id="prediction-1",
        source=Identifier.create("quality", "clips"),
        name="daily-quality",
        status=status,
        snapshot_id=7,
        model="quality.qwen_vl",
    )
    values.update(changes)
    return Prediction(**values)


def _response(status=None, **changes):
    if status is None:
        status = PredictionStatus(PredictionState.PENDING)
    values = dict(
        prediction_id="prediction-1",
        source=PredictionSource("quality", "clips"),
        name="daily-quality",
        status=status,
        snapshot_id=7,
        model=Identifier.create("quality", "qwen_vl"),
    )
    values.update(changes)
    return GetPredictionResponse(**values)


class PredictionTest(unittest.TestCase):

    def test_request_and_response_wire_format(self):
        request = SubmitPredictionRequest.from_spec(
            Identifier.create("quality", "clips"), _spec())
        body = json.loads(JSON.to_json(request))
        self.assertTrue(body["idempotencyToken"])
        self.assertEqual(
            body["idempotencyToken"],
            json.loads(JSON.to_json(request))["idempotencyToken"],
        )
        self.assertNotIn("resultColumns", body)
        self.assertNotIn("mergeKeyColumns", body)
        self.assertNotIn("prompt", body)
        self.assertEqual("Classify", body["instructions"])
        self.assertNotIn("outputSchema", body)
        self.assertEqual(
            {"database": "quality", "table": "clips"}, body["source"])
        self.assertEqual(
            {"database": "quality", "object": "qwen_vl"}, body["model"])
        self.assertNotIn("snapshotId", body)
        self.assertNotIn("tagName", body)

        versioned = SubmitPredictionRequest.from_spec(
            Identifier.create("quality", "clips"),
            _spec(options={"scan.tag-name": "training"}),
        )
        versioned_body = json.loads(JSON.to_json(versioned))
        self.assertEqual(
            {"scan.tag-name": "training"}, versioned_body["options"])
        self.assertNotIn("tagName", versioned_body)

        keyed = SubmitPredictionRequest.from_spec(
            Identifier.create("quality", "clips"),
            _spec(
                merge_key_columns=["clip_id"],
                idempotency_token="retry-1",
            ),
        )
        keyed_body = json.loads(JSON.to_json(keyed))
        self.assertEqual(["clip_id"], keyed_body["mergeKeyColumns"])
        self.assertEqual("retry-1", keyed_body["idempotencyToken"])

        response = JSON.from_json(
            JSON.to_json(_response()), GetPredictionResponse)
        self.assertEqual(
            PredictionState.PENDING, response.to_prediction().status.state)
        self.assertIsNone(
            response.to_prediction().status.termination_details)
        pending_body = json.loads(JSON.to_json(_response()))
        for field in (
                "createdAt", "startedAt", "finishedAt",
                "modelVersion", "outputTable", "output", "startTime",
                "endTime"):
            self.assertNotIn(field, pending_body)
        terminated = PredictionStatus(
            PredictionState.TERMINATED,
            PredictionTerminationDetails(
                PredictionTerminationCode.FAILED,
                "Model request failed",
            ),
        )
        completed_response = _response(
            terminated,
            output=ApiPredictionOutput(
                PredictionSource("system", "prediction_results")),
            start_time=10,
            end_time=20,
        )
        terminated_body = json.loads(JSON.to_json(completed_response))
        self.assertEqual(
            {
                "state": "TERMINATED",
                "terminationDetails": {
                    "code": "FAILED",
                    "message": "Model request failed",
                },
            },
            terminated_body["status"],
        )
        self.assertEqual(10, terminated_body["startTime"])
        self.assertEqual(20, terminated_body["endTime"])
        self.assertEqual(
            {"table": {"database": "system", "table": "prediction_results"}},
            terminated_body["output"],
        )
        for field in (
                "createdAt", "startedAt", "finishedAt", "modelVersion",
                "outputTable"):
            self.assertNotIn(field, terminated_body)
        completed = JSON.from_json(
            JSON.to_json(completed_response), GetPredictionResponse)
        self.assertEqual(
            terminated,
            completed.status,
        )
        self.assertEqual(
            PredictionOutput(
                Identifier.create("system", "prediction_results")),
            completed.to_prediction().output,
        )
        page = JSON.from_json(
            JSON.to_json(ListPredictionsResponse([_response()], "next")),
            ListPredictionsResponse,
        )
        self.assertEqual("prediction-1", page.predictions[0].prediction_id)
        self.assertEqual("next", page.next_page_token)

    def test_validation(self):
        invalid = [
            dict(model=""),
            dict(inputs={}),
            dict(inputs={"video": ""}),
            dict(merge_key_columns=["id", "id"]),
            dict(merge_key_columns="id"),
            dict(instructions=" "),
            dict(options={"scan.snapshot-id": "0"}),
            dict(options={
                "scan.snapshot-id": "1",
                "scan.tag-name": "v1",
            }),
            dict(options={"parallelism": 2}),
            dict(idempotency_token="x" * 65),
        ]
        for changes in invalid:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                _spec(**changes)
        with self.assertRaises(ValueError):
            _spec(model=Identifier.create("", "qwen_vl"))
        with self.assertRaises(ValueError):
            PredictionStatus(PredictionState.TERMINATED)
        with self.assertRaises(ValueError):
            PredictionStatus(
                PredictionState.RUNNING,
                PredictionTerminationDetails(
                    PredictionTerminationCode.FAILED),
            )

    def test_terminating_does_not_require_final_result(self):
        terminating = PredictionStatus(PredictionState.TERMINATING)
        self.assertFalse(terminating.terminal)
        for code in (
                PredictionTerminationCode.CANCELED,
                PredictionTerminationCode.SUCCESS,
        ):
            with self.subTest(code=code):
                terminated = PredictionStatus(
                    PredictionState.TERMINATED,
                    PredictionTerminationDetails(code),
                )
                self.assertTrue(terminated.terminal)
                self.assertEqual(code, terminated.termination_details.code)

    def test_structured_model_preserves_dots(self):
        request = SubmitPredictionRequest.from_spec(
            Identifier.create("quality", "clips"),
            _spec(model=Identifier.create("quality.archive", "qwen.vl")),
        )
        body = json.loads(JSON.to_json(request))

        self.assertEqual(
            {"database": "quality.archive", "object": "qwen.vl"},
            body["model"],
        )

    def test_table_submit_fixes_view_and_returns_waiter(self):
        raw = Mock(spec=FileStoreTable)
        raw.options = CoreOptions(Options({"scan.snapshot-id": "7"}))
        raw.identifier = Identifier.create("quality", "clips")
        catalog = Mock()
        catalog.submit_prediction.return_value = _prediction()
        table = MultimodalTable(catalog, "quality.clips", raw)

        waiter = table.predict(
            name="daily-quality",
            model="quality.qwen_vl",
            inputs={"video": "content"},
            instructions="Classify",
            idempotency_token="retry-1",
        )

        self.assertIsInstance(waiter, PredictionWait)
        self.assertEqual("prediction-1", waiter.prediction_id)
        sent = catalog.submit_prediction.call_args[0][1]
        self.assertEqual({"scan.snapshot-id": "7"}, sent.options)
        self.assertEqual([], sent.merge_key_columns)
        self.assertEqual("retry-1", sent.idempotency_token)
        raw.new_read_builder.assert_not_called()

    def test_table_queries_return_metadata_and_cancel_returns_waiter(self):
        raw = Mock(spec=FileStoreTable)
        raw.identifier = Identifier.create("quality", "clips")
        catalog = Mock()
        prediction = _prediction()
        page = PagedList([prediction], "next")
        catalog.get_prediction.return_value = prediction
        catalog.list_predictions.return_value = page
        catalog.cancel_prediction.return_value = prediction
        table = MultimodalTable(catalog, "quality.clips", raw)

        self.assertIs(prediction, table.get_prediction("prediction-1"))
        self.assertIs(
            page,
            table.list_predictions(state=PredictionState.PENDING),
        )
        waiter = table.cancel_prediction("prediction-1")
        self.assertIsInstance(waiter, PredictionWait)
        self.assertEqual("prediction-1", waiter.prediction_id)

    @patch("pypaimon.multimodal.prediction_wait.random.random", return_value=0)
    @patch("pypaimon.multimodal.prediction_wait.time.sleep")
    def test_result_is_client_side_polling(self, sleep, random):
        catalog = Mock()
        catalog.get_prediction.side_effect = [
            _prediction(PredictionStatus(PredictionState.RUNNING)),
            _prediction(PredictionStatus(
                PredictionState.TERMINATED,
                PredictionTerminationDetails(
                    PredictionTerminationCode.SUCCESS),
            )),
        ]
        waiter = PredictionWait(catalog, _prediction())
        observed = []
        result = waiter.result(
            timeout=timedelta(seconds=10),
            callback=lambda prediction: observed.append(
                prediction.status.state),
        )
        self.assertEqual(PredictionState.TERMINATED, result.status.state)
        self.assertEqual(
            PredictionTerminationCode.SUCCESS,
            result.status.termination_details.code,
        )
        self.assertEqual([PredictionState.RUNNING], observed)
        self.assertEqual(2, catalog.get_prediction.call_count)
        sleep.assert_called_once_with(1)

    def test_result_returns_failed_and_canceled_predictions(self):
        for code in (
                PredictionTerminationCode.FAILED,
                PredictionTerminationCode.CANCELED,
        ):
            with self.subTest(code=code):
                catalog = Mock()
                catalog.get_prediction.return_value = _prediction(
                    PredictionStatus(
                        PredictionState.TERMINATED,
                        PredictionTerminationDetails(code, "terminal"),
                    ))
                waiter = PredictionWait(catalog, _prediction())
                result = waiter.result(timeout=timedelta(seconds=1))
                self.assertEqual(
                    code, result.status.termination_details.code)

    @patch(
        "pypaimon.multimodal.prediction_wait.time.monotonic",
        side_effect=[0, 2],
    )
    def test_result_timeout_does_not_cancel_prediction(self, monotonic):
        catalog = Mock()
        waiter = PredictionWait(catalog, _prediction())

        with self.assertRaisesRegex(
                TimeoutError, "Prediction prediction-1 did not finish"):
            waiter.result(timeout=timedelta(seconds=1))

        catalog.get_prediction.assert_not_called()
        catalog.cancel_prediction.assert_not_called()

    def test_rest_round_trip_paths_and_filters(self):
        calls = []

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _send(self, payload, status=200):
                body = JSON.to_json(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0"))
                body = json.loads(self.rfile.read(length)) if length else None
                calls.append(("POST", self.path, body))
                status = 202 if self.path.endswith("/predictions") else 200
                self._send(_response(), status)

            def do_GET(self):
                calls.append(("GET", self.path, None))
                if "/predictions?" in self.path:
                    self._send(ListPredictionsResponse([_response()], "next"))
                else:
                    self._send(_response())

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
            identifier = Identifier.create("a/b", "clip table")

            self.assertEqual("prediction-1", catalog.submit_prediction(
                identifier, _spec()).prediction_id)
            self.assertEqual(
                "/v1/catalog/predictions",
                calls[-1][1],
            )
            self.assertEqual(
                {"database": "a/b", "table": "clip table"},
                calls[-1][2]["source"],
            )
            self.assertTrue(calls[-1][2]["idempotencyToken"])
            self.assertNotIn("mergeKeyColumns", calls[-1][2])

            page = catalog.list_predictions(
                identifier,
                name="daily-quality",
                state=PredictionState.RUNNING,
                max_results=10,
                page_token="p1",
            )
            self.assertEqual("prediction-1", page.elements[0].prediction_id)
            self.assertIn("name=daily-quality", calls[-1][1])
            self.assertIn("database=a%2Fb", calls[-1][1])
            self.assertIn("table=clip+table", calls[-1][1])
            self.assertIn("state=RUNNING", calls[-1][1])
            self.assertIn("maxResults=10", calls[-1][1])
            self.assertIn("pageToken=p1", calls[-1][1])

            catalog.get_prediction("prediction/1")
            self.assertTrue(
                calls[-1][1].endswith("/predictions/prediction%2F1"))
            catalog.cancel_prediction("prediction/1")
            self.assertTrue(
                calls[-1][1].endswith(
                    "/predictions/prediction%2F1/cancel"))
        finally:
            server.shutdown()
            server.server_close()
            thread.join()


if __name__ == "__main__":
    unittest.main()
