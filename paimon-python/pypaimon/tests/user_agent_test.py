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

import platform
import unittest
from unittest.mock import patch

from pypaimon.api.api_response import ConfigResponse
from pypaimon.api.rest_api import RESTApi
from pypaimon.api.typedef import RESTAuthParameter
from pypaimon.common.options.config import CatalogOptions, OssOptions
from pypaimon.filesystem.pvfs import PaimonVirtualFileSystem


class UserAgentTest(unittest.TestCase):

    def test_rest_api_uses_sdk_and_python_versions_in_default_user_agent(self):
        with patch(
                "pypaimon.api.rest_api.build_info.full_version",
                return_value="python-2.3.0-deadbeef"), patch(
                    "pypaimon.api.rest_api.platform.python_version",
                    return_value="3.11.9"):
            rest_api = RESTApi(
                {
                    CatalogOptions.URI.key(): "http://catalog",
                    CatalogOptions.TOKEN_PROVIDER.key(): "bear",
                    CatalogOptions.TOKEN.key(): "token",
                },
                config_required=False,
            )

        self.assertEqual(
            "PyPaimon/2.3.0 Python/3.11.9",
            rest_api.rest_auth_function.init_header["User-Agent"],
        )

    def test_rest_api_uses_unknown_version_when_version_lookup_fails(self):
        with patch(
                "pypaimon.api.rest_api.build_info.full_version",
                side_effect=RuntimeError("unavailable")), patch(
                    "pypaimon.api.rest_api.platform.python_version",
                    return_value="3.11.9"):
            rest_api = RESTApi(
                {
                    CatalogOptions.URI.key(): "http://catalog",
                    CatalogOptions.TOKEN_PROVIDER.key(): "bear",
                    CatalogOptions.TOKEN.key(): "token",
                },
                config_required=False,
            )

        self.assertEqual(
            "PyPaimon/unknown Python/3.11.9",
            rest_api.rest_auth_function.init_header["User-Agent"],
        )

    def test_rest_api_uses_unknown_version_when_version_info_is_invalid(self):
        with patch(
                "pypaimon.api.rest_api.build_info.full_version",
                return_value="UNKNOWN"), patch(
                    "pypaimon.api.rest_api.platform.python_version",
                    return_value="3.11.9"):
            rest_api = RESTApi(
                {
                    CatalogOptions.URI.key(): "http://catalog",
                    CatalogOptions.TOKEN_PROVIDER.key(): "bear",
                    CatalogOptions.TOKEN.key(): "token",
                },
                config_required=False,
            )

        self.assertEqual(
            "PyPaimon/unknown Python/3.11.9",
            rest_api.rest_auth_function.init_header["User-Agent"],
        )

    def test_rest_api_preserves_configured_user_agent(self):
        rest_api = RESTApi(
            {
                CatalogOptions.URI.key(): "http://catalog",
                CatalogOptions.TOKEN_PROVIDER.key(): "bear",
                CatalogOptions.TOKEN.key(): "token",
                "header.User-Agent": "custom-client/1.0",
            },
            config_required=False,
        )

        self.assertEqual(
            "custom-client/1.0",
            rest_api.rest_auth_function.init_header["User-Agent"],
        )

    def test_rest_config_request_uses_default_user_agent(self):
        with patch("pypaimon.api.rest_api.HttpClient") as http_client_class, patch(
                "pypaimon.api.rest_api.build_info.full_version",
                return_value="python-2.3.0-deadbeef"), patch(
                    "pypaimon.api.rest_api.platform.python_version",
                    return_value="3.11.9"):
            http_client = http_client_class.return_value
            http_client.get_with_params.return_value = ConfigResponse(
                defaults={}, overrides=None)
            RESTApi(
                {
                    CatalogOptions.URI.key(): "http://catalog",
                    CatalogOptions.WAREHOUSE.key(): "warehouse",
                    CatalogOptions.TOKEN_PROVIDER.key(): "bear",
                    CatalogOptions.TOKEN.key(): "token",
                },
            )

        config_auth_function = http_client.get_with_params.call_args[0][3]
        config_headers = config_auth_function.apply(
            RESTAuthParameter("GET", "/v1/config", ""))
        self.assertEqual(
            "PyPaimon/2.3.0 Python/3.11.9",
            config_headers["User-Agent"],
        )

    def test_pvfs_adds_default_user_agent(self):
        pvfs = PaimonVirtualFileSystem(
            {OssOptions.OSS_ACCESS_KEY_ID.key(): "ak"},
            skip_instance_cache=True,
        )

        self.assertIn("header.User-Agent", pvfs.options.to_map())

    def test_pvfs_includes_sdk_and_python_versions_in_default_user_agent(self):
        with patch(
                "pypaimon.filesystem.pvfs.build_info.full_version",
                return_value="python-2.3.0-deadbeef"):
            pvfs = PaimonVirtualFileSystem(
                {OssOptions.OSS_ACCESS_KEY_ID.key(): "ak"},
                skip_instance_cache=True,
            )

        self.assertEqual(
            "PythonPVFS PyPaimon/2.3.0 Python/{}".format(
                platform.python_version()),
            pvfs.options.get(CatalogOptions.HTTP_USER_AGENT_HEADER),
        )

    def test_pvfs_preserves_configured_user_agent(self):
        pvfs = PaimonVirtualFileSystem(
            {
                OssOptions.OSS_ACCESS_KEY_ID.key(): "ak",
                "header.User-Agent": "custom-client/1.0",
            },
            skip_instance_cache=True,
        )

        self.assertEqual(
            "custom-client/1.0",
            pvfs.options.get(CatalogOptions.HTTP_USER_AGENT_HEADER),
        )


if __name__ == "__main__":
    unittest.main()
