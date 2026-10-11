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
from unittest import mock

import requests

from pypaimon import PaimonVirtualFileSystem, build_info
from pypaimon.api.rest_api import RESTApi
from pypaimon.api.token_loader import HTTPClient
from pypaimon.common import user_agent
from pypaimon.common.options import Options

IDENTITY = "pypaimon/2.2.dev0"
REST_USER_AGENT = "{}(python-requests/{})".format(IDENTITY, requests.__version__)


class UserAgentIdentityTest(unittest.TestCase):

    def setUp(self):
        user_agent.identity.cache_clear()
        self.addCleanup(user_agent.identity.cache_clear)

    def test_identity_uses_build_version(self):
        with mock.patch.object(build_info, "version", return_value="2.2.dev"):
            self.assertEqual("pypaimon/2.2.dev", user_agent.identity())

    def test_identity_without_build_version(self):
        with mock.patch.object(build_info, "version", return_value=None):
            self.assertEqual("pypaimon", user_agent.identity())

    def test_build_version_from_full_version(self):
        for full_version, expected in (("python-2.2.dev-abc123", "2.2.dev"),
                                       ("python-2.2.0-UNKNOWN", "2.2.0"),
                                       ("UNKNOWN", None)):
            with mock.patch.object(build_info, "_FULL_VERSION", full_version):
                self.assertEqual(expected, build_info.version())


class UserAgentFormatTest(unittest.TestCase):

    def test_format(self):
        self.assertEqual("m/1(t/2)", user_agent.format_user_agent("m/1", "t/2"))
        self.assertEqual("m/1(t/2;a;b) ext k/v",
                         user_agent.format_user_agent("m/1", "t/2", ["a", "", "b"], "ext k/v"))

    def test_rest_user_agent(self):
        with mock.patch.object(user_agent, "identity", return_value=IDENTITY):
            self.assertEqual(REST_USER_AGENT, user_agent.rest_user_agent())
            self.assertEqual(REST_USER_AGENT[:-1] + ";Flink) vvr", user_agent.rest_user_agent(
                Options({"user-agent.features": " Flink ", "user-agent.extended": "vvr"})))
            self.assertEqual("MyApp/1.0(python-requests/{})".format(requests.__version__),
                             user_agent.rest_user_agent(Options({"user-agent.module": "MyApp/1.0"})))

    def test_with_feature_goes_first_once(self):
        options = Options({"user-agent.features": "Flink PythonPVFS"})
        user_agent.with_feature(options, "PythonPVFS")
        user_agent.with_feature(options, "PythonPVFS")
        self.assertEqual("PythonPVFS Flink", options.to_map()["user-agent.features"])


class RestUserAgentTest(unittest.TestCase):

    def setUp(self):
        self.requests = []
        recorded = self.requests

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                recorded.append(dict(self.headers))
                body = ({"defaults": {}} if "/config" in self.path
                        else {"databases": [], "nextPageToken": None})
                data = json.dumps(body).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *args):
                pass

        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.uri = "http://127.0.0.1:{}".format(self.server.server_port)

        patcher = mock.patch.object(user_agent, "identity", return_value=IDENTITY)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _options(self, **extra):
        options = {"uri": self.uri, "warehouse": "wh", "token.provider": "bear", "token": "t"}
        options.update(extra)
        return options

    def _user_agents(self):
        return [headers.get("User-Agent") for headers in self.requests]

    def test_default_user_agent(self):
        RESTApi(Options(self._options())).list_databases()

        self.assertEqual([REST_USER_AGENT, REST_USER_AGENT], self._user_agents())

    def test_common_options_user_agent(self):
        options = self._options(**{"user-agent.features": "Flink", "user-agent.extended": "vvr"})
        RESTApi(Options(options)).list_databases()

        expected = REST_USER_AGENT[:-1] + ";Flink) vvr"
        self.assertEqual([expected, expected], self._user_agents())

    def test_user_set_user_agent_wins(self):
        options = self._options(**{"header.User-Agent": "starrocks/user", "user-agent.extended": "vvr"})
        RESTApi(Options(options)).list_databases()

        self.assertEqual(["starrocks/user", "starrocks/user"], self._user_agents())

    def test_pvfs_user_agent(self):
        PaimonVirtualFileSystem(self._options(**{"user-agent.features": "Flink"})).ls("pvfs://wh/")

        pvfs_user_agent = REST_USER_AGENT[:-1] + ";PythonPVFS;Flink)"
        self.assertEqual([pvfs_user_agent, pvfs_user_agent], self._user_agents())
        for headers in self.requests:
            self.assertNotIn("http_user_agent", {key.lower() for key in headers})

    def test_token_loader_user_agent(self):
        HTTPClient().get(self.uri + "/v1/config")

        self.assertEqual([REST_USER_AGENT], self._user_agents())


if __name__ == "__main__":
    unittest.main()
