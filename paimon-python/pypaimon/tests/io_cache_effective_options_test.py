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
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from types import ModuleType, SimpleNamespace
from unittest import mock
from urllib.parse import unquote, urlsplit

from pypaimon.catalog.rest.rest_token_file_io import RESTTokenFileIO
from pypaimon.common.file_io import FileIO
from pypaimon.common.identifier import Identifier
from pypaimon.common.options import Options
from pypaimon.common.options.config import CatalogOptions
from pypaimon.common.uri_reader import UriReaderFactory
from pypaimon.filesystem.io_cache_routing import IoCacheRouting

OBJECT = "/bkt/raw/cam0/000123.bin"
URI = "oss://bkt/raw/cam0/000123.bin"


class _ObjectEndpoint:
    """An OSS endpoint for the real PyArrow client: path-style HEAD and GET of fixed objects."""

    def __init__(self, objects):
        self.objects = objects
        self.requests = []
        endpoint = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_HEAD(self):
                self._serve(False)

            def do_GET(self):
                self._serve(True)

            def _serve(self, with_body):
                path = unquote(urlsplit(self.path).path)
                endpoint.requests.append((self.command, path))
                data = endpoint.objects.get(path)
                if data is None:
                    body = b"<Error><Code>NoSuchKey</Code></Error>" if with_body else b""
                    self.send_response(404)
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                start, end, status = 0, len(data), 200
                ranged = self.headers.get("Range")
                if ranged and ranged.startswith("bytes="):
                    first, _, last = ranged[len("bytes="):].partition("-")
                    start, end, status = int(first), min(len(data), int(last) + 1 if last else len(data)), 206
                self.send_response(status)
                self.send_header("Content-Length", str(end - start))
                self.send_header("Content-Type", "application/octet-stream")
                self.send_header("ETag", '"etag"')
                self.send_header("Last-Modified", "Mon, 26 Oct 2026 00:00:00 GMT")
                if status == 206:
                    self.send_header("Content-Range", "bytes {}-{}/{}".format(start, end - 1, len(data)))
                self.end_headers()
                if with_body:
                    self.wfile.write(data[start:end])

        class Server(ThreadingMixIn, HTTPServer):
            daemon_threads = True

        self.server = Server(("127.0.0.1", 0), Handler)
        self.url = "http://127.0.0.1:{}".format(self.server.server_port)
        self.thread = threading.Thread(target=lambda: self.server.serve_forever(poll_interval=0.01), daemon=True)
        self.thread.start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()


class EffectiveOptionsTest(unittest.TestCase):
    """The origin holds the current object; a legacy cache, still fs.oss.endpoint for old clients, keeps an old one."""

    def setUp(self):
        self.origin = _ObjectEndpoint({OBJECT: b"NEW"})
        self.cache = _ObjectEndpoint({OBJECT: b"OLD"})
        self.addCleanup(self.origin.close)
        self.addCleanup(self.cache.close)

    def _routing_options(self):
        return {
            "fs.oss.impl": "legacy",
            "fs.oss.region": "cn-hangzhou",
            "fs.oss.accessKeyId": "ak",
            "fs.oss.accessKeySecret": "sk",
            "fs.oss.second.level.domain.enable": "true",
            "fs.oss.endpoint": self.cache.url,
            "io-cache.enabled": "true",
            "io-cache.endpoint": self.cache.url,
            "io-cache.target.default.path-style-access": "true",
            "io-cache.origin.endpoint": self.origin.url,
            "io-cache.policy": "meta,read",
        }

    def _assert_reads_origin(self, read):
        self.assertEqual(b"NEW", read())
        self.assertEqual([], self.cache.requests)
        self.assertTrue(self.origin.requests)

    def test_uri_reader_factory_reads_row_data_uris_from_origin(self):
        reader = UriReaderFactory(Options(self._routing_options())).create(URI)
        self._assert_reads_origin(lambda: reader.new_input_stream(URI).read())

    def test_source_file_io_reads_import_sources_from_origin(self):
        from pypaimon.multimodal.source_utils import _SourceFileIO

        source = _SourceFileIO(Options(self._routing_options()))
        self._assert_reads_origin(lambda: source.new_input_stream(URI).read())

    def test_file_io_of_a_catalog_without_tokens_reads_from_origin(self):
        from pypaimon.catalog.filesystem_catalog import FileSystemCatalog

        catalog = FileSystemCatalog(Options(dict(self._routing_options(), warehouse="oss://bkt/wh")))
        self._assert_reads_origin(lambda: catalog.file_io.new_input_stream(URI).read())

    def test_resolving_file_io_reads_from_origin(self):
        options = dict(self._routing_options(), **{CatalogOptions.RESOLVING_FILE_IO_ENABLED.key(): "true"})
        file_io = FileIO.get(URI, Options(options))
        self._assert_reads_origin(lambda: file_io.new_input_stream(URI).read())

    def test_file_ios_of_routing_targets_keep_their_endpoints(self):
        options = self._routing_options()
        routing = IoCacheRouting.create(options)
        target = FileIO.get(URI, Options(routing.target_options(options, "default")))
        self.assertEqual(b"OLD", target.new_input_stream(URI).read())
        self.assertEqual([], self.origin.requests)

    @unittest.skipIf(sys.version_info < (3, 7), "pypaimon.daft needs Python 3.7+")
    def test_daft_reads_catalog_only_routing_from_origin(self):
        from pypaimon.daft.daft_paimon import _enrich_options_with_rest_token

        options = dict(self._routing_options(), metastore="filesystem", warehouse="oss://bkt/wh")
        enriched = _enrich_options_with_rest_token(options, SimpleNamespace(table_path="oss://bkt/wh/db.db/t"))
        self.assertEqual((self.origin.url, "oss://bkt/wh"), (enriched["fs.oss.endpoint"], enriched["warehouse"]))

    def test_pvfs_reads_through_the_origin_of_catalog_only_routing(self):
        from pypaimon.filesystem.pvfs import PaimonVirtualFileSystem

        # The token carries credentials and the legacy endpoint; routing is configured on the catalog only.
        token = {key: value for key, value in self._routing_options().items() if not key.startswith("io-cache.")}
        catalog = dict({key: value for key, value in self._routing_options().items() if key.startswith("io-cache.")},
                       uri="http://rest.example.com", warehouse="wh", **{"fs.oss.impl": "legacy"})
        rest_api = SimpleNamespace(
            get_table=lambda identifier: SimpleNamespace(path="oss://bkt/raw", created_at=0, updated_at=0),
            load_table_token=lambda identifier: SimpleNamespace(
                token=token, expires_at_millis=int(time.time() * 1000) + 7_200_000))
        objects = {self.cache.url: b"old-object", self.origin.url: b"new-object"}
        backends = []

        def ossfs(options):
            backends.append(options.to_map()["fs.oss.endpoint"])
            return SimpleNamespace(cat_file=lambda path, start, end, **kwargs: objects[backends[-1]])

        pvfs = PaimonVirtualFileSystem(catalog, skip_instance_cache=True)
        with mock.patch.object(PaimonVirtualFileSystem, "_PaimonVirtualFileSystem__rest_api", return_value=rest_api), \
                mock.patch.object(PaimonVirtualFileSystem, "_get_ossfs_filesystem", side_effect=ossfs):
            self.assertEqual(b"new-object", pvfs.cat_file("pvfs://wh/db/t/cam0/000123.bin"))
        self.assertEqual([self.origin.url], backends)


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
