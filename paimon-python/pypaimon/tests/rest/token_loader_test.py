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

import tempfile
import unittest
from pathlib import Path

from pypaimon.api.auth.factory import AuthProviderFactory
from pypaimon.api.token_loader import (
    DLFLocalFileTokenLoader,
    DLFToken,
    DLFTokenLoaderFactory,
)
from pypaimon.common.json_util import JSON
from pypaimon.common.options import Options
from pypaimon.common.options.config import CatalogOptions


class DLFLocalFileTokenLoaderTest(unittest.TestCase):

    def test_load_token_from_configured_path(self):
        token = DLFToken(
            access_key_id="access-key-id",
            access_key_secret="access-key-secret",
            security_token="security-token",
            expiration="2099-12-01T12:00:00Z",
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            token_path = Path(temp_dir) / "token.json"
            token_path.write_text(JSON.to_json(token), encoding="utf-8")
            options = Options({CatalogOptions.DLF_TOKEN_PATH.key(): str(token_path)})

            loader = DLFTokenLoaderFactory.create_token_loader(options)
            loaded_token = loader.load_token()

            self.assertIsInstance(loader, DLFLocalFileTokenLoader)
            self.assertEqual(str(token_path), loader.description())
            self.assertEqual(token.access_key_id, loaded_token.access_key_id)
            self.assertEqual(token.access_key_secret, loaded_token.access_key_secret)
            self.assertEqual(token.security_token, loaded_token.security_token)
            self.assertEqual(token.expiration, loaded_token.expiration)

    def test_auth_provider_uses_token_path_without_explicit_loader(self):
        token = DLFToken("access-key-id", "access-key-secret", "security-token")

        with tempfile.TemporaryDirectory() as temp_dir:
            token_path = Path(temp_dir) / "token.json"
            token_path.write_text(JSON.to_json(token), encoding="utf-8")
            options = Options({
                CatalogOptions.URI.key():
                    "https://cn-hangzhou-vpc.dlf.aliyuncs.com",
                CatalogOptions.TOKEN_PROVIDER.key(): "dlf",
                CatalogOptions.DLF_TOKEN_PATH.key(): str(token_path),
            })

            provider = AuthProviderFactory.create_auth_provider(options)

            self.assertIsInstance(provider.token_loader, DLFLocalFileTokenLoader)
            self.assertEqual("access-key-id", provider.get_token().access_key_id)

    def test_token_loader_takes_precedence_over_static_credentials(self):
        file_token = DLFToken("file-ak", "file-sk", "file-sts")

        with tempfile.TemporaryDirectory() as temp_dir:
            token_path = Path(temp_dir) / "token.json"
            token_path.write_text(JSON.to_json(file_token), encoding="utf-8")

            for loader_name in (None, "local_file"):
                with self.subTest(loader_name=loader_name):
                    option_values = {
                        CatalogOptions.URI.key():
                            "https://cn-hangzhou-vpc.dlf.aliyuncs.com",
                        CatalogOptions.TOKEN_PROVIDER.key(): "dlf",
                        CatalogOptions.DLF_TOKEN_PATH.key(): str(token_path),
                        CatalogOptions.DLF_ACCESS_KEY_ID.key(): "static-ak",
                        CatalogOptions.DLF_ACCESS_KEY_SECRET.key(): "static-sk",
                    }
                    if loader_name is not None:
                        option_values[
                            CatalogOptions.DLF_TOKEN_LOADER.key()
                        ] = loader_name

                    provider = AuthProviderFactory.create_auth_provider(
                        Options(option_values)
                    )

                    self.assertEqual("file-ak", provider.get_token().access_key_id)

    def test_unknown_token_loader_does_not_fallback_to_static_credentials(self):
        options = Options({
            CatalogOptions.URI.key():
                "https://cn-hangzhou-vpc.dlf.aliyuncs.com",
            CatalogOptions.TOKEN_PROVIDER.key(): "dlf",
            CatalogOptions.DLF_TOKEN_LOADER.key(): "unknown",
            CatalogOptions.DLF_ACCESS_KEY_ID.key(): "static-ak",
            CatalogOptions.DLF_ACCESS_KEY_SECRET.key(): "static-sk",
        })

        with self.assertRaisesRegex(ValueError, "Unknown DLF token loader: unknown"):
            AuthProviderFactory.create_auth_provider(options)

    def test_loader_reads_rotated_token(self):
        first_token = DLFToken("first-ak", "first-sk", "first-sts")
        second_token = DLFToken("second-ak", "second-sk", "second-sts")

        with tempfile.TemporaryDirectory() as temp_dir:
            token_path = Path(temp_dir) / "token.json"
            token_path.write_text(JSON.to_json(first_token), encoding="utf-8")
            loader = DLFLocalFileTokenLoader(str(token_path))

            self.assertEqual("first-ak", loader.load_token().access_key_id)
            token_path.write_text(JSON.to_json(second_token), encoding="utf-8")
            self.assertEqual("second-ak", loader.load_token().access_key_id)

    def test_auth_provider_reloads_expiring_token_from_path(self):
        expired_token = DLFToken(
            "first-ak", "first-sk", "first-sts", "2000-01-01T00:00:00Z"
        )
        fresh_token = DLFToken(
            "second-ak", "second-sk", "second-sts", "2099-01-01T00:00:00Z"
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            token_path = Path(temp_dir) / "token.json"
            token_path.write_text(JSON.to_json(expired_token), encoding="utf-8")
            options = Options({
                CatalogOptions.URI.key():
                    "https://cn-hangzhou-vpc.dlf.aliyuncs.com",
                CatalogOptions.TOKEN_PROVIDER.key(): "dlf",
                CatalogOptions.DLF_TOKEN_PATH.key(): str(token_path),
            })
            provider = AuthProviderFactory.create_auth_provider(options)

            self.assertEqual("first-ak", provider.get_token().access_key_id)
            token_path.write_text(JSON.to_json(fresh_token), encoding="utf-8")
            self.assertEqual("second-ak", provider.get_token().access_key_id)

    def test_malformed_token_file_does_not_leak_credentials(self):
        secret = "STSSECRET_AKID_9999"

        with tempfile.TemporaryDirectory() as temp_dir:
            token_path = Path(temp_dir) / "token.json"
            token_path.write_text(
                '{"AccessKeyId":"akid","AccessKeySecret":"%s" INVALID_JSON'
                % secret,
                encoding="utf-8",
            )

            with self.assertRaisesRegex(RuntimeError, "Failed to parse token file") as ctx:
                DLFLocalFileTokenLoader.read_token(str(token_path), max_retries=1)

            self.assertNotIn(secret, str(ctx.exception))
            self.assertIsNone(ctx.exception.__cause__)


if __name__ == "__main__":
    unittest.main()
