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

import inspect
import os
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Dict, List
from urllib.parse import urlsplit

import yaml

from pypaimon.api.rest_api import RESTApi
from pypaimon.common.identifier import Identifier
from pypaimon.function.function import FunctionImpl
from pypaimon.function.function_change import FunctionChange
from pypaimon.management.data_policy import DataPolicy
from pypaimon.management.list_permissions_request import ListPermissionsRequest
from pypaimon.management.list_policies_request import ListPoliciesRequest
from pypaimon.management.permission_assignment import PermissionAssignment
from pypaimon.management.permission_resource import PermissionResource
from pypaimon.management.policy_type import PolicyType
from pypaimon.management.resource_type import ResourceType
from pypaimon.management.row_filter import RowFilter
from pypaimon.schema.schema import Schema
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.snapshot.snapshot_commit import PartitionStatistics
from pypaimon.table.instant import Instant

# RESTApi methods with no entry in dlf_openapi_actions; they are signed without an action.
WITHOUT_ACTION = frozenset()

_SPEC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), *[os.pardir] * 4, "docs", "static")
_SPEC_FILES = ("rest-catalog-open-api.yaml", "rest-management-open-api.yaml")
_HTTP_METHODS = ("get", "put", "post", "delete", "patch", "head")

_TABLE = PermissionResource(ResourceType.TABLE, "db", "t", None, None)

# A value per parameter annotation, else per name; "SELECT" is valid both as a name and as an access.
_ARGUMENTS = {
    str: "SELECT",
    bool: False,
    Identifier: Identifier.create("db", "t"),
    Schema: Schema(),
    List: [FunctionChange.set_option("k", "v")],  # serializes for alter_table and alter_function
    List[Dict[str, str]]: [{"dt": "1"}],
    List[PartitionStatistics]: [],
    Snapshot: Snapshot(version=3, id=1, schema_id=0, base_manifest_list="b", delta_manifest_list="d",
                       total_record_count=1, delta_record_count=1, commit_user="u",
                       commit_identifier=1, commit_kind="APPEND", time_millis=0),
    PermissionResource: _TABLE,
    PermissionAssignment: PermissionAssignment(_TABLE, "SELECT", "x1"),
    ListPermissionsRequest: ListPermissionsRequest(ResourceType.TABLE, "db", "t"),
    ListPoliciesRequest: ListPoliciesRequest(_TABLE),
    PolicyType: PolicyType.ROW_FILTER,
    DataPolicy: DataPolicy.row_filter(_TABLE, RowFilter("x1"), "x1"),
    "identifier": Identifier.create("db", "t"),
    "instant": Instant.snapshot(1),
    "function": FunctionImpl(Identifier.create("db", "f")),
}


class RESTApiCoverageTest(unittest.TestCase):
    """Every request RESTApi sends must be in the OpenAPI spec and carry an openapi-v4 action."""

    def setUp(self):
        requests = self.requests = []

        class Handler(BaseHTTPRequestHandler):

            def log_message(self, *args):
                pass

            def record(self):
                self.rfile.read(int(self.headers.get("Content-Length", 0)))
                requests.append((self.command, urlsplit(self.path).path, self.headers.get("x-acs-action")))
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", "2")
                self.end_headers()
                self.wfile.write(b"{}")

            do_GET = do_POST = do_DELETE = record

        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.options = {
            "uri": "http://127.0.0.1:{}".format(self.server.server_port),
            "prefix": "clg-paimon-1",
            "token.provider": "dlf",
            "dlf.access-key-id": "ak",
            "dlf.access-key-secret": "sk",
            "dlf.region": "cn-hangzhou",
            "dlf.signing-algorithm": "openapi-v4",
        }

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()

    def test_every_request_carries_an_action(self):
        """A new REST call fails here until it is added to dlf_openapi_actions or WITHOUT_ACTION."""
        without_action = {}
        for name, requests in self._send_every_request().items():
            for verb, path, action in requests:
                if action is None:
                    without_action.setdefault(name, []).append(verb + " " + path)

        missing = {name: paths for name, paths in without_action.items() if name not in WITHOUT_ACTION}
        self.assertEqual({}, missing, "requests without x-acs-action; map them in dlf_openapi_actions")
        self.assertEqual(set(), WITHOUT_ACTION - set(without_action),
                         "WITHOUT_ACTION entries that now resolve; remove them")

    def test_every_request_is_in_the_openapi_spec(self):
        """A new REST call fails here until docs/static/rest-*-open-api.yaml lists it."""
        spec = _spec_operations()
        unspecified = {}
        for name, requests in self._send_every_request().items():
            for verb, path, _ in requests:
                if not _in_spec(spec, verb, path):
                    unspecified.setdefault(name, []).append(verb + " " + path)

        self.assertEqual({}, unspecified, "requests the OpenAPI spec lacks; add them to it")

    def _send_every_request(self):
        """Requests sent by each public method, by name; "RESTApi" holds the constructor's."""
        try:
            RESTApi(dict(self.options, warehouse="x1"))
        except Exception:
            pass  # a reply of {} may not parse; only the request matters
        sent = {"RESTApi": list(self.requests)}
        silent = [] if self.requests else ["RESTApi"]
        api = RESTApi(self.options, False)
        for name, method in sorted(vars(RESTApi).items()):
            if name.startswith("_") or name == "get_options" or not inspect.isfunction(method):
                continue
            del self.requests[:]
            try:
                getattr(api, name)(*_arguments(method))
            except Exception:
                pass  # a reply of {} may not parse; only the request matters
            if not self.requests:
                silent.append(name)
            sent[name] = list(self.requests)

        self.assertEqual([], silent, "methods that sent no request; give them arguments")
        return sent


def _spec_operations():
    operations = set()
    for file in _SPEC_FILES:
        with open(os.path.join(_SPEC_DIR, file), encoding="utf-8") as f:
            for path, item in yaml.safe_load(f)["paths"].items():
                operations.update((method.upper(), path) for method in item if method in _HTTP_METHODS)
    return operations


def _in_spec(spec, verb, path):
    segments = path.split("/")
    for method, template in spec:
        parts = template.split("/")
        if method == verb and len(parts) == len(segments) and all(
                segment != "" if part.startswith("{") else part == segment
                for part, segment in zip(parts, segments)):
            return True
    return False


def _arguments(method):
    arguments = []
    for parameter in list(inspect.signature(method).parameters.values())[1:]:
        if parameter.default is not inspect.Parameter.empty:
            continue
        annotation = parameter.annotation
        if type(None) in getattr(annotation, "__args__", ()):
            arguments.append(None)
        else:
            arguments.append(_ARGUMENTS.get(annotation, _ARGUMENTS.get(parameter.name)))
    return arguments


if __name__ == '__main__':
    unittest.main()
