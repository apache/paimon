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

import platform
import unittest
from unittest import mock

from pypaimon.api.rest_api import RESTApi
from pypaimon.common.options.config import CatalogOptions
from pypaimon.common.user_agent import get_pypaimon_version, get_user_agent
from pypaimon.filesystem.pvfs import PaimonVirtualFileSystem


class UserAgentTest(unittest.TestCase):

    @mock.patch(
        "pypaimon.common.user_agent._distribution_version",
        return_value="1.2.3",
    )
    def test_user_agent_contains_client_and_runtime_versions(self, _):
        self.assertEqual(
            "PythonPVFS PyPaimon/1.2.3 Python/{}".format(
                platform.python_version()
            ),
            get_user_agent("PythonPVFS"),
        )

    @mock.patch(
        "pypaimon.common.user_agent._distribution_version",
        side_effect=RuntimeError("missing metadata"),
    )
    def test_missing_distribution_metadata_returns_unknown(self, _):
        self.assertEqual("unknown", get_pypaimon_version())

    def test_rest_api_uses_default_user_agent(self):
        api = RESTApi(self._options(), config_required=False)

        self.assertEqual(
            get_user_agent(),
            api.rest_auth_function.init_header["User-Agent"],
        )
        self.assertEqual(
            "header.User-Agent", CatalogOptions.HTTP_USER_AGENT_HEADER.key()
        )
        self.assertIsNone(
            CatalogOptions.HTTP_USER_AGENT_HEADER.default_value()
        )
        self.assertIn(
            "header.HTTP_USER_AGENT",
            str(CatalogOptions.HTTP_USER_AGENT_HEADER),
        )

    def test_rest_api_uses_configured_user_agent(self):
        options = self._options()
        options[CatalogOptions.HTTP_USER_AGENT_HEADER.key()] = "custom-client/1.0"

        api = RESTApi(options, config_required=False)

        self.assertEqual(
            "custom-client/1.0",
            api.rest_auth_function.init_header["User-Agent"],
        )

    def test_rest_api_preserves_case_insensitive_user_agent(self):
        options = self._options()
        options["header.user-agent"] = "custom-client/1.0"

        api = RESTApi(options, config_required=False)

        self.assertEqual(
            "custom-client/1.0",
            api.rest_auth_function.init_header["User-Agent"],
        )
        self.assertNotIn("user-agent", api.rest_auth_function.init_header)

    def test_rest_api_uses_legacy_configured_user_agent(self):
        options = self._options()
        options["header.HTTP_USER_AGENT"] = "legacy-client/1.0"

        api = RESTApi(options, config_required=False)

        self.assertEqual(
            "legacy-client/1.0",
            api.rest_auth_function.init_header["User-Agent"],
        )
        self.assertNotIn(
            "HTTP_USER_AGENT",
            api.rest_auth_function.init_header,
        )

    def test_new_user_agent_key_takes_precedence_over_legacy_key(self):
        options = self._options()
        options["header.HTTP_USER_AGENT"] = "legacy-client/1.0"
        options[CatalogOptions.HTTP_USER_AGENT_HEADER.key()] = "custom-client/1.0"

        api = RESTApi(options, config_required=False)

        self.assertEqual(
            "custom-client/1.0",
            api.rest_auth_function.init_header["User-Agent"],
        )
        self.assertNotIn(
            "HTTP_USER_AGENT",
            api.rest_auth_function.init_header,
        )

    def test_canonical_user_agent_takes_precedence_over_other_casing(self):
        options = self._options()
        options["header.user-agent"] = "lowercase-client/1.0"
        options[CatalogOptions.HTTP_USER_AGENT_HEADER.key()] = "custom-client/1.0"

        api = RESTApi(options, config_required=False)

        self.assertEqual(
            "custom-client/1.0",
            api.rest_auth_function.init_header["User-Agent"],
        )
        self.assertNotIn("user-agent", api.rest_auth_function.init_header)

    def test_pvfs_adds_client_name_to_default_user_agent(self):
        fs = PaimonVirtualFileSystem(
            self._options(), skip_instance_cache=True
        )

        self.assertEqual(
            get_user_agent("PythonPVFS"),
            fs.options.get(CatalogOptions.HTTP_USER_AGENT_HEADER),
        )

    def test_pvfs_preserves_configured_user_agent(self):
        options = self._options()
        options[CatalogOptions.HTTP_USER_AGENT_HEADER.key()] = "custom-client/1.0"

        fs = PaimonVirtualFileSystem(options, skip_instance_cache=True)

        self.assertEqual(
            "custom-client/1.0",
            fs.options.get(CatalogOptions.HTTP_USER_AGENT_HEADER),
        )

    def test_pvfs_preserves_case_insensitive_user_agent(self):
        options = self._options()
        options["header.user-agent"] = "custom-client/1.0"

        fs = PaimonVirtualFileSystem(options, skip_instance_cache=True)

        self.assertEqual(
            "custom-client/1.0",
            fs.options.to_map()["header.user-agent"],
        )
        self.assertFalse(
            fs.options.contains_key(
                CatalogOptions.HTTP_USER_AGENT_HEADER.key()
            )
        )

    def test_pvfs_preserves_legacy_configured_user_agent(self):
        options = self._options()
        options["header.HTTP_USER_AGENT"] = "legacy-client/1.0"

        fs = PaimonVirtualFileSystem(options, skip_instance_cache=True)

        self.assertEqual(
            "legacy-client/1.0",
            fs.options.get(CatalogOptions.HTTP_USER_AGENT_HEADER),
        )
        self.assertFalse(
            fs.options.contains_key(
                CatalogOptions.HTTP_USER_AGENT_HEADER.key()
            )
        )

    @staticmethod
    def _options():
        return {
            "uri": "http://localhost:8080",
            "token.provider": "bear",
            "token": "token",
        }


if __name__ == '__main__':
    unittest.main()
