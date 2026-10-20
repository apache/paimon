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


"""REST catalog metadata stays on Python even when Native table execution is enabled."""

from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
import builtins
import json
import threading
from unittest.mock import patch
from urllib.parse import parse_qsl, urlsplit

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.api.api_response import ErrorResponse
from pypaimon.api.auth.dlf_signer import DLFOpenApiV4Signer
from pypaimon.api.rest_exception import ServiceFailureException
from pypaimon.api.token_loader import DLFToken
from pypaimon.api.typedef import RESTAuthParameter
from pypaimon.catalog.catalog_context import CatalogContext
from pypaimon.catalog.rest.rest_catalog import RESTCatalog
from pypaimon.common.identifier import Identifier
from pypaimon.tests.native_branch_write_test import _assert_rows, _branch, _table, _write
from pypaimon.tests.rest.rest_server import RESTCatalogServer


@pytest.fixture
def transport_server():
    class Handler(BaseHTTPRequestHandler):
        def operation(self):
            size = int(self.headers.get('Content-Length', 0))
            data = self.rfile.read(size).decode() if size else None
            self.server.requests.append((self.command, self.path, self.headers))
            self.server.bodies.append(json.loads(data) if data else None)
            status = 200
            if self.path.startswith('/v1/config'):
                body = json.dumps({'defaults': {'prefix': 'test'}})
            elif self.server.require_v4 and not self.headers.get('Authorization', '').startswith('ACS4-HMAC-SHA256 '):
                status, body = 403, json.dumps({'message': 'wrong signature algorithm'})
            elif self.server.temporary_status and self.server.branch_requests == 0:
                status = self.server.temporary_status
                body = json.dumps({'message': 'temporary failure'})
                self.server.branch_requests += 1
            elif self.server.respond is not None:
                status, body = self.server.respond(self.command, self.path, data)
            else:
                body = json.dumps({'branches': ['dev']}) if self.command == 'GET' else 'OK'
            if self.server.require_v4:
                signed_headers = {key.lower(): value for key, value in self.headers.items()
                                  if key.lower().startswith('x-acs-') or key.lower() in ('host', 'content-type')}
                url = urlsplit(self.path)
                expected = DLFOpenApiV4Signer('cn-hangzhou').authorization(
                    RESTAuthParameter(self.command, url.path, data, dict(parse_qsl(url.query, keep_blank_values=True))),
                    self.server.expected_token,
                    self.headers['Host'], signed_headers)
                if self.headers.get('Authorization') != expected:
                    status, body = 403, json.dumps({'message': 'signature mismatch'})
            output = body.encode()
            self.send_response(status)
            self.send_header('Content-Length', str(len(output)))
            self.end_headers()
            self.wfile.write(output)

        do_GET = do_POST = do_DELETE = operation

        def log_message(self, *args):
            pass

    server = HTTPServer(('localhost', 0), Handler)
    server.requests = []
    server.bodies = []
    server.respond = None
    server.require_v4 = False
    server.temporary_status = server.branch_requests = 0
    server.expected_token = DLFToken('test-key', 'test-secret', 'test-sts', None)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def transport_catalog(server, **extra):
    options = {
        'metastore': 'rest', 'uri': 'http://localhost:%d' % server.server_port, 'warehouse': 'w',
        'token.provider': 'bear', 'token': 'test-token',
        'read.native.enabled': 'true', 'write.native.enabled': 'true',
    }
    options.update(extra)
    return RESTCatalog(CatalogContext.create_from_options(options))


@contextmanager
def python_rest_only():
    original_import = builtins.__import__

    def checked_import(name, *args, **kwargs):
        if name == 'pypaimon_rust' or name.startswith('pypaimon_rust.'):
            raise AssertionError('REST catalog operations must use the Python client')
        return original_import(name, *args, **kwargs)

    with patch('builtins.__import__', side_effect=checked_import):
        yield


@pytest.mark.parametrize('operation', ['create_branch', 'drop_branch', 'fast_forward', 'list_branches'])
def test_branch_operations_use_python_with_native_options_enabled(transport_server, operation):
    server = transport_server
    server.require_v4 = True
    catalog = transport_catalog(server, **{
        'token.provider': 'dlf', 'dlf.region': 'cn-hangzhou', 'dlf.signing-algorithm': 'openapi-v4',
        'dlf.access-key-id': 'test-key', 'dlf.access-key-secret': 'test-secret',
        'dlf.security-token': 'test-sts',
    })
    args = () if operation == 'list_branches' else ('dev 一/?#',)
    server.requests.clear()
    with python_rest_only(), \
            patch.object(catalog.rest_api, operation, wraps=getattr(catalog.rest_api, operation)) as execute:
        result = getattr(catalog, operation)(Identifier('db 一/space', 'table'), *args)
    execute.assert_called_once()
    assert result == (['dev'] if operation == 'list_branches' else None)
    assert len(server.requests) == 1
    assert server.requests[0][2]['x-acs-action'] == {
        'create_branch': 'CreateBranch', 'drop_branch': 'DropBranch',
        'fast_forward': 'FastForwardBranch', 'list_branches': 'ListBranches',
    }[operation]


@pytest.mark.parametrize('operation', ['create_branch', 'drop_branch', 'fast_forward'])
def test_failed_branch_mutations_are_not_replayed(transport_server, operation):
    server = transport_server
    catalog = transport_catalog(server)
    server.respond = lambda *args: (500, '{"message":"uncertain publication"}')
    server.requests.clear()
    with python_rest_only(), pytest.raises(ServiceFailureException):
        getattr(catalog, operation)('db.table', 'dev')
    assert len(server.requests) == 1


@pytest.mark.parametrize('status', [429, 503])
def test_branch_listing_retries_temporary_status(transport_server, status):
    catalog = transport_catalog(transport_server)
    transport_server.requests.clear()
    transport_server.temporary_status = status
    with python_rest_only():
        assert catalog.list_branches('db.table') == ['dev']
    assert len(transport_server.requests) == 2
    assert all(request[0] == 'GET' for request in transport_server.requests)


@pytest.mark.parametrize('operation', ['create_branch', 'drop_branch', 'fast_forward', 'list_branches'])
@pytest.mark.parametrize('name', ['.', '..', '', ' '])
def test_invalid_branch_resource_paths_send_no_requests(transport_server, operation, name):
    catalog = transport_catalog(transport_server)
    transport_server.requests.clear()
    args = () if operation == 'list_branches' else ('dev',)
    for identifier in (Identifier(name, 'table'), Identifier('db', name)):
        with pytest.raises(ValueError):
            getattr(catalog, operation)(identifier, *args)
    if operation in ('drop_branch', 'fast_forward'):
        with pytest.raises(ValueError):
            getattr(catalog, operation)('db.table', name)
    assert transport_server.requests == []


@pytest.mark.native_plan
def test_python_rest_branches_preserve_native_shared_data(tmp_path, native_rest_catalog):
    catalog = native_rest_catalog
    catalog, main = _table(tmp_path, catalog, True)
    seed = dict(id=1, value=10, p='a')
    added = dict(id=2, value=20, p='b')
    _write(main, [seed])
    empty = _branch(catalog, main, from_tag=False, name='empty')
    _assert_rows(empty, [])
    branch = _branch(catalog, main)
    _write(branch, [added])
    _assert_rows(main, [seed])
    _assert_rows(branch, [seed, added])
    assert catalog.list_branches(main.identifier) == ['dev', 'empty']
    paths = {path for path in tmp_path.rglob('*.parquet')}
    assert paths
    catalog.fast_forward(main.identifier, 'dev')
    catalog.drop_branch(main.identifier, 'dev')
    catalog.drop_branch(main.identifier, 'empty')
    assert catalog.list_branches(main.identifier) == []
    assert all(path.exists() for path in paths)
    catalog.create_branch(main.identifier, 'restored', 'base')
    restored = catalog.get_table(Identifier('default', 't', branch='restored'))
    _assert_rows(restored, [seed])


@pytest.mark.native_plan
def test_published_branch_survives_uncertain_response(tmp_path, native_rest_catalog):
    catalog = native_rest_catalog
    catalog, table = _table(tmp_path, catalog, True)
    original = RESTCatalogServer._branches_handle
    published = []

    def publish_then_fail(server, method, data, identifier):
        response = original(server, method, data, identifier)
        if method == 'POST':
            published.append(identifier)
            return server._mock_response(ErrorResponse(None, None, 'uncertain result', 500), 500)
        return response
    with patch.object(RESTCatalogServer, '_branches_handle', publish_then_fail):
        with pytest.raises(ServiceFailureException):
            catalog.create_branch(table.identifier, 'dev')
        assert catalog.list_branches(table.identifier) == ['dev']
    assert published == [table.identifier]


@pytest.mark.native_plan
def test_python_rest_branch_drop_preserves_native_blob_files(tmp_path, native_rest_catalog):
    catalog = native_rest_catalog
    schema = pa.schema([('id', pa.int32()), ('payload', pa.large_binary())])
    catalog.create_table('default.blobs', Schema.from_pyarrow_schema(schema, options={
        'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true', 'blob-field': 'payload',
        'write.native.enabled': 'true', 'commit.native.enabled': 'true',
    }), False)
    main = catalog.get_table('default.blobs')

    def write(table, value):
        builder = table.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        try:
            writer.write_arrow(pa.table({'id': [value], 'payload': [b'payload' * value]}, schema=schema))
            commit.commit(writer.prepare_commit())
        finally:
            writer.close()
            commit.close()
    write(main, 1)
    catalog.create_tag(main.identifier, 'base')
    catalog.create_branch(main.identifier, 'dev', 'base')
    branch = catalog.get_table(Identifier('default', 'blobs', branch='dev'))
    write(branch, 2)
    files = set(tmp_path.rglob('*.blob'))
    assert files
    catalog.drop_branch(main.identifier, 'dev')
    assert all(path.exists() for path in files)
    catalog.create_branch(main.identifier, 'restored', 'base')
    restored = catalog.get_table(Identifier('default', 'blobs', branch='restored'))
    builder = restored.new_read_builder()
    assert builder.new_read().to_arrow(builder.new_scan().plan().splits()).to_pydict() == {
        'id': [1], 'payload': [b'payload'],
    }
