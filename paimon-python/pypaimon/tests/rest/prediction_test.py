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
from unittest.mock import Mock

from pypaimon.api.prediction import (
    GetPredictionResponse,
    ListPredictionsResponse,
    PredictionSource,
    SubmitPredictionRequest,
)
from pypaimon.api.rest_api import RESTApi
from pypaimon.catalog.prediction import (
    Prediction,
    PredictionSpec,
    PredictionStatus,
)
from pypaimon.catalog.rest.rest_catalog import RESTCatalog
from pypaimon.common.identifier import Identifier
from pypaimon.common.json_util import JSON
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.multimodal.prediction import PredictionJob
from pypaimon.multimodal.table import MultimodalTable
from pypaimon.table.file_store_table import FileStoreTable


def _spec(**changes):
    values = dict(
        model="quality.qwen_vl",
        inputs={"video": "content"},
        result_columns=["clip_id"],
        prompt="Classify",
        output_schema={"result": "STRING"},
    )
    values.update(changes)
    return PredictionSpec(**values)


def _prediction(status=PredictionStatus.PENDING, **changes):
    values = dict(
        prediction_id="prediction-1",
        source=Identifier.create("quality", "clips"),
        name="daily-quality",
        status=status,
        snapshot_id=7,
        model="quality.qwen_vl",
        model_version="1",
        output_table="system.prediction_results",
        created_at=10,
    )
    values.update(changes)
    return Prediction(**values)


def _response(status="PENDING", **changes):
    values = dict(
        prediction_id="prediction-1",
        source=PredictionSource("quality", "clips"),
        name="daily-quality",
        status=status,
        snapshot_id=7,
        model="quality.qwen_vl",
        model_version="1",
        output_table="system.prediction_results",
        created_at=10,
    )
    values.update(changes)
    return GetPredictionResponse(**values)


class PredictionTest(unittest.TestCase):

    def test_request_and_response_wire_format(self):
        request = SubmitPredictionRequest.from_spec(
            Identifier.create("quality", "clips"), _spec())
        body = json.loads(JSON.to_json(request))
        self.assertTrue(body["idempotencyKey"])
        self.assertEqual(
            body["idempotencyKey"],
            json.loads(JSON.to_json(request))["idempotencyKey"],
        )
        self.assertEqual(["clip_id"], body["resultColumns"])
        self.assertEqual({"result": "STRING"}, body["outputSchema"])
        self.assertEqual(
            {"database": "quality", "table": "clips"}, body["source"])
        self.assertNotIn("snapshotId", body)

        response = JSON.from_json(
            JSON.to_json(_response()), GetPredictionResponse)
        self.assertEqual(
            PredictionStatus.PENDING, response.to_prediction().status)
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
            dict(result_columns=["id", "id"]),
            dict(result_columns="id"),
            dict(prompt=" "),
            dict(output_schema={}),
            dict(snapshot_id=0),
            dict(snapshot_id=1, tag_name="v1"),
        ]
        for changes in invalid:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                _spec(**changes)

    def test_table_submit_fixes_view_and_returns_job(self):
        raw = Mock(spec=FileStoreTable)
        raw.options = CoreOptions(Options({"scan.snapshot-id": "7"}))
        raw.identifier = Identifier.create("quality", "clips")
        catalog = Mock()
        catalog.submit_prediction.return_value = _prediction()
        table = MultimodalTable(catalog, "quality.clips", raw)

        job = table.predict(
            name="daily-quality",
            model="quality.qwen_vl",
            inputs={"video": "content"},
            result_columns=["clip_id"],
            prompt="Classify",
            output_schema={"result": "STRING"},
        )

        self.assertIsInstance(job, PredictionJob)
        self.assertEqual("prediction-1", job.prediction_id)
        sent = catalog.submit_prediction.call_args.args[1]
        self.assertEqual(7, sent.snapshot_id)
        self.assertIsNone(sent.tag_name)
        raw.new_read_builder.assert_not_called()

    def test_wait_is_client_side_polling(self):
        catalog = Mock()
        catalog.get_prediction.side_effect = [
            _prediction(PredictionStatus.RUNNING),
            _prediction(
                PredictionStatus.SUCCEEDED, total=2, succeeded=2, failed=0),
        ]
        job = PredictionJob(catalog, _prediction())
        result = job.wait(timeout=timedelta(seconds=1), poll_interval=0)
        self.assertIs(job, result)
        self.assertEqual(PredictionStatus.SUCCEEDED, job.status)
        self.assertEqual(2, catalog.get_prediction.call_count)

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
            self.assertTrue(calls[-1][2]["idempotencyKey"])

            page = catalog.list_predictions(
                identifier,
                name="daily-quality",
                status=PredictionStatus.RUNNING,
                max_results=10,
                page_token="p1",
            )
            self.assertEqual("prediction-1", page.elements[0].prediction_id)
            self.assertIn("name=daily-quality", calls[-1][1])
            self.assertIn("database=a%2Fb", calls[-1][1])
            self.assertIn("table=clip+table", calls[-1][1])
            self.assertIn("status=RUNNING", calls[-1][1])
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
