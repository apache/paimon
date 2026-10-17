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

"""REST branch operations retain PyPaimon's contracts when Rust executes them."""

from contextlib import ExitStack, contextmanager
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.api.api_response import ErrorResponse
from pypaimon.api.rest_exception import (
    NoSuchResourceException, NotImplementedException, RESTException, ServiceFailureException,
)
from pypaimon.catalog.catalog_exception import (
    BranchAlreadyExistException, BranchNotExistException, IllegalArgumentError,
    TableNotExistException, TableNoPermissionException, TagNotExistException,
)
from pypaimon.common.identifier import Identifier
from pypaimon.tests.native_branch_write_test import _assert_rows, _branch, _table, _write
from pypaimon.tests.rest.rest_server import RESTCatalogServer

pytestmark = pytest.mark.native_plan
_OPERATIONS = ('create_branch', 'drop_branch', 'fast_forward', 'list_branches')


@pytest.fixture
def catalog(native_rest_catalog):
    native_rest_catalog.context.options.to_map().update({
        'read.native.enabled': 'true', 'write.native.enabled': 'true',
    })
    return native_rest_catalog


@contextmanager
def native_only(catalog):
    with ExitStack() as guards:
        for name in _OPERATIONS:
            guards.enter_context(patch.object(
                catalog.rest_api, name, side_effect=AssertionError('Python branch fallback: ' + name)))
        yield


def test_native_rest_branch_lifecycle_with_shared_data(tmp_path, catalog):
    catalog, main = _table(tmp_path, catalog, True)
    seed = dict(id=1, value=10, p='a')
    added = dict(id=2, value=20, p='b')
    _write(main, [seed])
    with native_only(catalog):
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


def test_native_branch_errors_keep_public_python_types(tmp_path, catalog):
    catalog, table = _table(tmp_path, catalog, True)
    with native_only(catalog):
        catalog.create_branch(table.identifier, 'dev')
        cases = [
            (TableNotExistException, lambda: catalog.create_branch('default.missing', 'dev')),
            (TableNotExistException, lambda: catalog.list_branches('default.missing')),
            (TagNotExistException, lambda: catalog.create_branch(table.identifier, 'tagged', 'missing')),
            (BranchAlreadyExistException, lambda: catalog.create_branch(table.identifier, 'dev')),
            (BranchNotExistException, lambda: catalog.drop_branch(table.identifier, 'missing')),
            (BranchNotExistException, lambda: catalog.fast_forward(table.identifier, 'missing')),
            (IllegalArgumentError, lambda: catalog.create_branch(table.identifier, 'main')),
        ]
        for expected, invoke in cases:
            with pytest.raises(expected):
                invoke()
        manager = table.branch_manager()
        manager.create_branch('dev', ignore_if_exists=True)
        with pytest.raises(ValueError, match='already exists'):
            manager.create_branch('dev')
        with pytest.raises(ValueError, match="doesn't exist"):
            manager.drop_branch('missing')


@pytest.mark.parametrize('operation', _OPERATIONS)
@pytest.mark.parametrize('name', ['.', '..', '', ' '])
def test_invalid_resource_paths_send_no_requests(catalog, operation, name):
    args = () if operation == 'list_branches' else ('dev',)
    with patch('pypaimon.catalog.rest.native_branch._native_catalog',
               side_effect=AssertionError('Invalid paths must be rejected before preparing a backend')):
        for identifier in (Identifier(name, 't'), Identifier('default', name)):
            with pytest.raises(ValueError):
                getattr(catalog, operation)(identifier, *args)
        if operation in ('drop_branch', 'fast_forward'):
            with pytest.raises(ValueError):
                getattr(catalog, operation)('default.t', name)


def test_native_catalog_constructor_uses_resolved_configuration(catalog):
    from pypaimon_rust.datafusion import PaimonCatalog

    with patch('pypaimon_rust.datafusion.PaimonCatalog', wraps=PaimonCatalog) as construct:
        with native_only(catalog):
            # A valid missing table reaches the catalog, rather than failing construction.
            with pytest.raises(TableNotExistException):
                catalog.list_branches('default.missing')
    options = construct.call_args.args[0]
    assert options['prefix'] == 'native-test'
    assert options['metastore'] == 'rest'
    assert construct.call_args.kwargs == {'config_required': False}


def test_preparation_failure_can_use_python_before_mutation(tmp_path, catalog):
    catalog, table = _table(tmp_path, catalog, True)
    with patch('pypaimon_rust.datafusion.PaimonCatalog', side_effect=ValueError('preparation failed')):
        catalog.create_branch(table.identifier, 'dev')
    with native_only(catalog):
        assert catalog.list_branches(table.identifier) == ['dev']


def test_fork_safety_failure_never_uses_python(catalog):
    from pypaimon_rust import ForkSafetyError

    error = ForkSafetyError('inherited native state')
    with patch('pypaimon_rust.datafusion.PaimonCatalog', side_effect=error), native_only(catalog):
        with pytest.raises(ForkSafetyError) as raised:
            catalog.list_branches('default.t')
    assert raised.value is error


@pytest.mark.parametrize('status,body,exception', [
    (403, '{"message":"denied"}', TableNoPermissionException),
    (429, 'too many requests', RESTException),
    (502, 'bad gateway', RESTException),
    (504, 'gateway timeout', RESTException),
])
def test_real_http_errors_preserve_python_exceptions(catalog, status, body, exception):
    # POST is deliberately used so neither HTTP client retries status responses.
    with patch.object(RESTCatalogServer, '_branches_handle', return_value=(body, status)) as handle, \
            native_only(catalog):
        with pytest.raises(exception):
            catalog.create_branch('default.t', 'dev')
    assert handle.call_count == 1


def test_native_response_parse_failure_is_a_rest_exception(catalog):
    with patch.object(RESTCatalogServer, '_branches_handle', return_value=('not JSON', 200)) as handle, \
            native_only(catalog):
        with pytest.raises(RESTException):
            catalog.list_branches('default.t')
    assert handle.call_count == 1


def test_native_connection_failure_is_a_rest_exception(catalog):
    import socket

    with socket.socket() as listener:
        listener.bind(('localhost', 0))
        # A bound, non-listening port rejects connections without racing port allocation.
        catalog.rest_api.client.uri = 'http://localhost:%d' % listener.getsockname()[1]
        with native_only(catalog), pytest.raises(RESTException):
            catalog.drop_branch('default.t', 'dev')


def test_published_branch_survives_uncertain_response(tmp_path, catalog):
    catalog, table = _table(tmp_path, catalog, True)
    original = RESTCatalogServer._branches_handle
    published = []

    def publish_then_fail(server, method, data, identifier):
        response = original(server, method, data, identifier)
        if method == 'POST':
            published.append(identifier)
            return server._mock_response(ErrorResponse(None, None, 'uncertain result', 500), 500)
        return response

    with patch.object(RESTCatalogServer, '_branches_handle', publish_then_fail), native_only(catalog):
        with pytest.raises(ServiceFailureException):
            catalog.create_branch(table.identifier, 'dev')
        assert catalog.list_branches(table.identifier) == ['dev']
    assert published == [table.identifier]


def test_custom_error_handler_uses_python_before_request(catalog):
    from pypaimon.api.client import ErrorHandler

    class CustomFailure(RESTException):
        pass

    class Handler(ErrorHandler):
        def accept(self, error, request_id):
            raise CustomFailure('custom error handler')

    catalog.rest_api.client.set_error_handler(Handler())
    with patch('pypaimon_rust.datafusion.PaimonCatalog', side_effect=AssertionError('custom handler bypassed')):
        with pytest.raises(CustomFailure, match='custom error handler'):
            catalog.list_branches('default.missing')


def test_native_keeps_live_endpoint_and_original_authentication(catalog):
    # Config merging changes options without rebuilding Python's existing client
    # and provider. Delegation must keep their endpoint, credentials and headers.
    from pypaimon_rust.datafusion import PaimonCatalog

    original_token = catalog.rest_api.rest_auth_function.auth_provider.token
    catalog.rest_api.options.to_map().update({
        'token.provider': 'dlf', 'token': 'server-token', 'dlf.token-loader': 'ecs',
        'uri': 'http://invalid.example', 'header.X-Test': 'stale',
    })
    catalog.rest_api.rest_auth_function.init_header['X-Test'] = 'current'
    catalog.rest_api.client.set_user_agent('branch-client')
    with patch('pypaimon_rust.datafusion.PaimonCatalog', wraps=PaimonCatalog) as construct, native_only(catalog):
        with pytest.raises(TableNotExistException):
            catalog.list_branches('default.missing')
    options = construct.call_args.args[0]
    assert options['token.provider'] == 'bear'
    assert options['token'] == original_token
    assert options['uri'] == catalog.rest_api.client.uri
    assert 'dlf.token-loader' not in options
    assert options['header.X-Test'] == 'current'
    assert options['header.User-Agent'] == 'branch-client'


@pytest.mark.parametrize('code,exception', [
    (403, TableNoPermissionException), (500, ServiceFailureException), (501, NotImplementedException),
    (404, NoSuchResourceException),
])
def test_native_errors_are_translated_without_python_retry(catalog, code, exception):
    from pypaimon_rust.datafusion import CatalogError

    error = CatalogError('native error')
    error.code, error.resource_type, error.resource_name, error.message = code, 'OTHER', 'unknown', 'native error'

    class Native:
        calls = 0

        def create_branch(self, *args):
            self.calls += 1
            raise error

    native = Native()
    with patch('pypaimon.catalog.rest.native_branch._native_catalog', return_value=(native, CatalogError)), \
            native_only(catalog):
        with pytest.raises(exception):
            catalog.create_branch('default.t', 'dev')
    assert native.calls == 1


def test_native_branch_blob_files_survive_drop(tmp_path, catalog):
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
    with native_only(catalog):
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
