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

"""Every component takes io-cache routing from the effective options: catalog options merged with the token."""

import sys
import time
import unittest
from types import ModuleType, SimpleNamespace
from unittest import mock

from pypaimon.catalog.rest.rest_token_file_io import RESTTokenFileIO
from pypaimon.common.identifier import Identifier
from pypaimon.common.options import Options
from pypaimon.filesystem.io_cache_routing import IoCacheRouting


class SharedTokenTest(unittest.TestCase):
    """Two catalogs that set io-cache.enabled differently share the token the REST server vended for a table."""

    TABLE = "oss://bkt/wh/db.db/t"
    TOKEN = {
        "fs.oss.endpoint": "http://legacy.example.com",
        "fs.oss.accessKeyId": "ak",
        "fs.oss.accessKeySecret": "sk",
        "io-cache.enabled": "true",
        "io-cache.endpoint": "http://legacy.example.com",
        "io-cache.origin.endpoint": "https://origin.example.com",
        "io-cache.policy": "meta,read",
    }

    def setUp(self):
        self.vended = []
        patcher = mock.patch.object(RESTTokenFileIO, "_build_cache_key", return_value="shared-token-test")
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(RESTTokenFileIO._TOKEN_CACHE.pop, "shared-token-test", None)

    def _file_io(self, catalog_options):
        file_io = RESTTokenFileIO(Identifier.create("db", "t"), self.TABLE, Options(dict(catalog_options)))

        def load_table_token(identifier):
            self.vended.append(identifier)
            return SimpleNamespace(token=dict(self.TOKEN), expires_at_millis=int(time.time() * 1000) + 7_200_000)

        file_io.api_instance = SimpleNamespace(load_table_token=load_table_token)
        file_io.try_to_refresh_token()
        return file_io

    def _native_data_token(self, file_io):
        from pypaimon.execution.native.table import _NativeRestTableCache

        fake_df = ModuleType("pypaimon_rust.datafusion")
        fake_df.Table = mock.Mock()
        with mock.patch.dict(sys.modules, {"pypaimon_rust.datafusion": fake_df}):
            _NativeRestTableCache().get("response", "db", "t", file_io.catalog_options.to_map(),
                                        token_loader=file_io._existing_valid_token)
        return fake_df.Table.from_rest_response_with_token.call_args[1]["data_token"]

    def _daft_options(self, file_io):
        from pypaimon.daft.daft_paimon import _enrich_options_with_rest_token

        catalog_options = dict(file_io.catalog_options.to_map(), metastore="rest")
        return _enrich_options_with_rest_token(catalog_options, SimpleNamespace(file_io=file_io, table_path=self.TABLE))

    def _assert_routes(self, file_io, routed):
        endpoint = "https://origin.example.com" if routed else "http://legacy.example.com"
        self.assertEqual(routed, IoCacheRouting.create(file_io._merged_options(file_io.token).to_map()) is not None)
        self.assertEqual(endpoint, self._native_data_token(file_io)["fs.oss.endpoint"])
        if sys.version_info >= (3, 7):
            self.assertEqual(endpoint, self._daft_options(file_io)["fs.oss.endpoint"])

    def test_each_catalog_applies_its_own_switch_to_a_shared_token(self):
        for first, second in (({"io-cache.enabled": "false"}, {"io-cache.enabled": "true"}),
                              ({"io-cache.enabled": "false"}, {}),
                              ({"io-cache.enabled": "true"}, {"io-cache.enabled": "false"})):
            with self.subTest(first=first, second=second):
                RESTTokenFileIO._TOKEN_CACHE.pop("shared-token-test", None)
                self.vended.clear()
                file_ios = [self._file_io(first), self._file_io(second)]
                self.assertEqual(1, len(self.vended))
                self.assertEqual(self.TOKEN, RESTTokenFileIO._TOKEN_CACHE["shared-token-test"].token)
                for file_io, catalog_options in zip(file_ios, (first, second)):
                    self._assert_routes(file_io, catalog_options.get("io-cache.enabled", "true") == "true")

    def test_dlf_oss_endpoint_of_one_catalog_stays_with_it(self):
        pinned = self._file_io({"dlf.oss-endpoint": "oss-cn-test.example.com"})
        shared = self._file_io({})
        self.assertEqual("oss-cn-test.example.com", pinned._merged_options(pinned.token).to_map()["fs.oss.endpoint"])
        self.assertEqual("oss-cn-test.example.com", self._native_data_token(pinned)["fs.oss.endpoint"])
        self._assert_routes(shared, True)


if __name__ == "__main__":
    unittest.main()
