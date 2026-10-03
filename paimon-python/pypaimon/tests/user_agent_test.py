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

import unittest
from unittest.mock import patch

import requests

from pypaimon.api.api_response import ConfigResponse
from pypaimon.api.rest_api import RESTApi
from pypaimon.api.typedef import RESTAuthParameter
from pypaimon.common.options.config import CatalogOptions


class UserAgentTest(unittest.TestCase):

    def create_api(self, client=None, defaults=None, overrides=None):
        options = {
            CatalogOptions.URI.key(): "http://catalog",
            CatalogOptions.WAREHOUSE.key(): "warehouse",
            CatalogOptions.TOKEN_PROVIDER.key(): "bear",
            CatalogOptions.TOKEN.key(): "token",
        }
        options.update(client or {})
        with patch("pypaimon.api.client.HttpClient.get_with_params",
                   return_value=ConfigResponse(defaults=defaults or {}, overrides=overrides)), patch(
                       "pypaimon.common.user_agent.identity", return_value="pypaimon/2.3.0"):
            return RESTApi(options)

    def prepared_user_agent(self, api):
        headers = api.rest_auth_function.apply(RESTAuthParameter("GET", "/v1/config", ""))
        request = requests.Request("GET", "http://catalog/v1/config", headers=headers)
        return api.client.session.prepare_request(request).headers["User-Agent"]

    def test_lowercase_client_wins_over_server_default(self):
        api = self.create_api({"header.user-agent": "client/1"},
                              {"header.User-Agent": "default/1"})
        self.assertEqual("client/1", self.prepared_user_agent(api))

    def test_mixed_case_client_wins_over_server_default(self):
        api = self.create_api({"header.USER-AGENT": "client/1"},
                              {"header.User-Agent": "default/1"})
        self.assertEqual("client/1", self.prepared_user_agent(api))

    def test_lowercase_override_wins_over_client_and_default(self):
        api = self.create_api({"header.User-Agent": "client/1"},
                              {"header.USER-AGENT": "default/1"},
                              {"header.user-agent": "override/1"})
        self.assertEqual("override/1", self.prepared_user_agent(api))

    def test_uppercase_override_wins_over_lowercase_client(self):
        api = self.create_api({"header.user-agent": "client/1"},
                              {"header.User-Agent": "default/1"},
                              {"header.USER-AGENT": "override/1"})
        self.assertEqual("override/1", self.prepared_user_agent(api))

    def test_uppercase_override_wins_over_default_using_same_key_as_client(self):
        api = self.create_api({"header.user-agent": "client/1"},
                              {"header.user-agent": "default/1"},
                              {"header.User-Agent": "override/1"})
        self.assertEqual("override/1", self.prepared_user_agent(api))

    def test_config_request_preserves_lowercase_client_user_agent(self):
        with patch("pypaimon.api.client.HttpClient.get_with_params",
                   return_value=ConfigResponse(defaults={}, overrides=None)) as config:
            api = RESTApi({"uri": "http://catalog", "warehouse": "warehouse",
                           "token.provider": "bear", "token": "token",
                           "header.user-agent": "client/1"})
        headers = config.call_args[0][3].apply(RESTAuthParameter("GET", "/v1/config", ""))
        prepared = api.client.session.prepare_request(
            requests.Request("GET", "http://catalog/v1/config", headers=headers))
        self.assertEqual("client/1", prepared.headers["User-Agent"])

    def test_server_default_used_without_client(self):
        api = self.create_api(defaults={"header.user-agent": "default/1"})
        self.assertEqual("default/1", self.prepared_user_agent(api))

    def test_missing_custom_user_agent_keeps_upstream_unified_format(self):
        api = self.create_api()
        expected = "pypaimon/2.3.0(python-requests/{})".format(requests.__version__)
        self.assertEqual(expected, self.prepared_user_agent(api))

    def test_response_updates_unified_user_agent_options(self):
        api = self.create_api(overrides={"user-agent.module": "MyApp/1.0",
                                         "user-agent.features": "Flink"})
        expected = "MyApp/1.0(python-requests/{};Flink)".format(requests.__version__)
        self.assertEqual(expected, self.prepared_user_agent(api))

    def test_none_override_keeps_client(self):
        api = self.create_api({"header.user-agent": "client/1"},
                              overrides={"header.User-Agent": None})
        self.assertEqual("client/1", self.prepared_user_agent(api))


if __name__ == "__main__":
    unittest.main()
