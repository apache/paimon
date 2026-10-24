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
from concurrent.futures import ThreadPoolExecutor
from http.server import HTTPServer
from urllib.request import urlopen

from pypaimon import CatalogFactory
from pypaimon.api.api_response import ConfigResponse
from pypaimon.api.auth import BearTokenAuthProvider
from pypaimon.tests.rest.rest_server import RESTCatalogServer


def _server(path):
    return RESTCatalogServer(str(path), BearTokenAuthProvider('token'),
                             ConfigResponse(defaults={'prefix': 'lifecycle'}), 'warehouse')


def _catalog(server):
    return CatalogFactory.create({
        'metastore': 'rest', 'uri': server.get_url(), 'warehouse': 'warehouse',
        'token.provider': 'bear', 'token': 'token', 'data-token.enabled': 'false'})


def test_shutdown_preserves_isolation_and_releases_threads_and_ports(tmp_path):
    servers = [_server(tmp_path / str(index)) for index in range(2)]
    try:
        for index, server in enumerate(servers):
            server.start()
            catalog = _catalog(server)
            catalog.create_database('database_' + str(index), False)
        for index, server in enumerate(servers):
            # The final response is complete and each server still has its own state.
            assert _catalog(server).list_databases() == ['database_' + str(index)]
    finally:
        for server in servers:
            server.shutdown()

    for server in servers:
        assert not server.server_thread.is_alive()
        assert server.server.socket.fileno() == -1
        with HTTPServer(('localhost', server.port), server._create_request_handler()):
            pass


def test_shutdown_waits_for_inflight_response(tmp_path, monkeypatch):
    server = _server(tmp_path)
    request_entered = threading.Event()
    release_request = threading.Event()
    response_complete = threading.Event()
    shutdown_entered = threading.Event()

    handler = server._create_request_handler()
    send_response = handler._send_response

    def record_response(self, status_code, body):
        send_response(self, status_code, body)
        response_complete.set()

    monkeypatch.setattr(handler, '_send_response', record_response)
    monkeypatch.setattr(server, '_create_request_handler', lambda: handler)
    route_request = server._route_request

    def block_request(*args):
        request_entered.set()
        assert release_request.wait(10)
        return route_request(*args)

    monkeypatch.setattr(server, '_route_request', block_request)
    server.start()

    def read_config():
        from urllib.request import Request
        url = server.get_url() + server.resource_paths.config() + '?warehouse=warehouse'
        with urlopen(Request(url, headers={'Authorization': 'Bearer token'}), timeout=10) as response:
            return json.loads(response.read().decode('utf-8'))

    def shutdown():
        shutdown_entered.set()
        server.shutdown()
        return response_complete.is_set()

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            request = executor.submit(read_config)
            try:
                assert request_entered.wait(10)
                stopped = executor.submit(shutdown)
                assert shutdown_entered.wait(10)
                assert not stopped.done()
            finally:
                release_request.set()
            assert request.result(timeout=10)['defaults'] == {'prefix': 'lifecycle'}
            assert stopped.result(timeout=10)
    finally:
        release_request.set()
        if server.server_thread.is_alive():
            server.shutdown()

    assert not server.server_thread.is_alive()
    assert server.server.socket.fileno() == -1
