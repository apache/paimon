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

from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.api.api_response import ConfigResponse, ErrorResponse, GetTableSnapshotResponse
from pypaimon.api.auth import BearTokenAuthProvider
from pypaimon.read.native_plan import native_method_available, native_runtime_available
from pypaimon.snapshot.table_snapshot import TableSnapshot
from pypaimon.table.row.blob import BlobViewStruct
from pypaimon.tests.rest.rest_server import RESTCatalogServer


pytestmark = [pytest.mark.native_plan, pytest.mark.skipif(
    not native_runtime_available(), reason='Rust planner required')]


@pytest.fixture
def rest_catalog(tmp_path):
    server = RESTCatalogServer(str(tmp_path), BearTokenAuthProvider('test-token'),
                               ConfigResponse(defaults={'prefix': 'native-test'}), 'warehouse')
    server.start()
    try:
        catalog = CatalogFactory.create({
            'metastore': 'rest', 'uri': server.get_url(), 'warehouse': 'warehouse',
            'token.provider': 'bear', 'token': 'test-token', 'data-token.enabled': 'false'})
        catalog.create_database('default', True)
        yield catalog, server
    finally:
        server.shutdown()


@pytest.fixture
def rest_source(rest_catalog):
    from pypaimon.tests.native_plan_resolved_schema_test import _write
    catalog, server = rest_catalog
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        pa.schema([('id', pa.int64()), ('value', pa.string())])), False)
    table = catalog.get_table('default.t')
    _write(table, [{'id': 1, 'value': 'old'}])
    first = table.snapshot_manager().get_latest_snapshot()
    _write(table, [{'id': 2, 'value': 'new'}])
    return table, server, first


@pytest.mark.parametrize('response', ['first', 'empty', 'missing'])
def test_catalog_snapshot_controls_native_plan(rest_source, response):
    from pypaimon.tests.native_plan_resolved_schema_test import _assert_parity
    table, server, first = rest_source
    if response == 'first':
        reply, code = GetTableSnapshotResponse(TableSnapshot(first, 1, 0, 1, first.time_millis)), 200
    elif response == 'empty':
        reply, code = GetTableSnapshotResponse(), 200
    else:
        code = 404
        reply = ErrorResponse('SNAPSHOT', 't', response, code)
    with patch.object(server, '_table_snapshot_handle', return_value=server._mock_response(reply, code)):
        expected = [{'id': 1, 'value': 'old'}]
        snapshot_id = 1
        if response in ('empty', 'missing'):
            expected, snapshot_id = [], None
        _assert_parity(table, expected, snapshot_id)


@pytest.mark.parametrize('code', [403, 404, 500, 501, 503])
def test_catalog_snapshot_failure_never_reads_disk(rest_source, code):
    from pypaimon.read.native_plan import native_plan
    table, server, _ = rest_source
    reply = ErrorResponse('TABLE', 't', 'snapshot unavailable', code)
    with patch.object(server, '_table_snapshot_handle', return_value=server._mock_response(reply, code)):
        # Test the binding directly too: a native error must not become a stale
        # filesystem plan before TableScan gets a chance to fall back.
        with pytest.raises(Exception, match='snapshot unavailable|does not exist|permission'):
            native_plan(table)
        with pytest.raises(Exception):
            table.copy({'scan.native-plan.enabled': 'false'}).new_read_builder().new_scan().plan()


@pytest.mark.parametrize('from_tag', [False, True], ids=['empty-branch', 'tagged-branch'])
def test_rest_branch_keeps_catalog_snapshot_and_schema(rest_source, rest_catalog, from_tag):
    from pypaimon.common.identifier import Identifier
    from pypaimon.tests.native_plan_resolved_schema_test import _assert_parity
    table, server, _ = rest_source
    catalog, _ = rest_catalog
    if from_tag:
        catalog.create_tag(table.identifier, 'first', 1)
    catalog.create_branch(table.identifier, 'dev', tag_name='first' if from_tag else None)
    branch = catalog.get_table(Identifier('default', 't', branch='dev')).copy({'read.batch-size': '1'})
    with patch.object(server, '_table_snapshot_handle', wraps=server._table_snapshot_handle) as load:
        _assert_parity(branch, [{'id': 1, 'value': 'old'}] if from_tag else [], 1 if from_tag else None)
        assert load.call_count >= 2
        assert all(call.args[2] == 'dev' for call in load.call_args_list)


@pytest.mark.parametrize('from_tag', [False, True], ids=['empty-branch', 'tagged-branch'])
def test_dynamic_branch_uses_native_catalog(rest_source, rest_catalog, from_tag):
    from pypaimon.read.native_plan import _resolved_rest_table_response, native_plan
    table, _, _ = rest_source
    catalog, _ = rest_catalog
    if from_tag:
        catalog.create_tag(table.identifier, 'first', 1)
    catalog.create_branch(table.identifier, 'dev', tag_name='first' if from_tag else None)
    branch = table.copy({'branch': 'dev', 'read.native.enabled': 'true'})
    assert branch.catalog_environment.rest_table_response == table.catalog_environment.rest_table_response
    assert _resolved_rest_table_response(branch) is None
    plan = native_plan(branch)
    assert plan.snapshot_id == (1 if from_tag else None)
    with patch('pypaimon.read.table_read.TableRead._create_split_read',
               side_effect=AssertionError('native read fell back')):
        rows = branch.new_read_builder().new_read().to_arrow(plan.splits()).to_pylist()
    assert rows == ([{'id': 1, 'value': 'old'}] if from_tag else [])


def test_resolved_rest_table_keeps_refreshable_file_io(rest_source, rest_catalog):
    from pypaimon.api.api_response import GetTableTokenResponse
    from pypaimon.read.native_plan import _resolved_schema_json
    from pypaimon_rust.datafusion import PaimonCatalog
    table, server, _ = rest_source
    catalog, _ = rest_catalog
    options = dict(catalog.context.options.to_map(), **{'data-token.enabled': 'true'})
    # Expiry is in the past, so a subsequent data access must refresh; no sleep.
    expired = server._mock_response(GetTableTokenResponse(token={}, expires_at_millis=0), 200)
    with patch.object(server, '_table_token_handle', return_value=expired):
        rt = PaimonCatalog(options).get_table('default.t')
        resolved = rt.copy_with_resolved_schema(_resolved_schema_json(table))
    assert resolved.latest_snapshot().id() == 2
    denied = server._mock_response(ErrorResponse('TABLE', 't', 'token denied', 403), 403)
    with patch.object(server, '_table_token_handle', return_value=denied) as refresh:
        with pytest.raises(Exception, match='token denied'):
            resolved.new_read_builder().new_scan().plan()
        refresh.assert_called()


@pytest.mark.skipif(not native_method_available('Table', 'from_rest_response_with_token'),
                    reason='REST data token reuse binding required')
@pytest.mark.parametrize('local_cache', [False, True])
def test_native_rest_reuses_python_data_token(
        rest_source, rest_catalog, tmp_path, local_cache):
    import time

    from pypaimon.catalog.rest.rest_token import RESTToken
    from pypaimon.catalog.rest.rest_token_file_io import RESTTokenFileIO
    from pypaimon.filesystem.caching_file_io import CachingFileIO
    from pypaimon.read.native_plan import _catalog_options, _resolved_rest_table_response
    from pypaimon_rust.datafusion import Table as NativeTable

    source, server, _ = rest_source
    catalog, _ = rest_catalog
    options = dict(catalog.context.options.to_map())
    options['data-token.enabled'] = 'true'
    if local_cache:
        options.update({'local-cache.enabled': 'true',
                        'local-cache.dir': str(tmp_path / 'cache')})
    server.set_table_token(
        source.identifier, RESTToken({}, int(time.time() * 1000) + 7_200_000))
    table = CatalogFactory.create(options).get_table(source.identifier)
    reused_table = CatalogFactory.create(options).get_table(source.identifier)
    uncached_table = CatalogFactory.create(options).get_table(source.identifier)

    def token_file_io(table):
        file_io = table.file_io
        assert (type(file_io) is CachingFileIO) == local_cache
        if type(file_io) is CachingFileIO:
            file_io = file_io._delegate
        assert type(file_io) is RESTTokenFileIO
        return file_io

    # The old bridge fetched a Python token and an independent Rust token.
    with patch.object(RESTTokenFileIO, '_TOKEN_CACHE', {}):
        with patch.object(server, '_table_token_handle',
                          wraps=server._table_token_handle) as load:
            token_file_io(table).valid_token()
            baseline = NativeTable.from_rest_response(
                _resolved_rest_table_response(table), database='default', table='t',
                rest_options=_catalog_options(table))
            assert baseline.new_read_builder().new_scan().plan().snapshot_id() == 2
            assert load.call_count == 2
            load.reset_mock()
            RESTTokenFileIO._TOKEN_CACHE.clear()

            token_file_io(reused_table).token = token_file_io(table).token

            for _ in range(2):
                assert reused_table.new_read_builder().new_scan().plan().snapshot_id == 2
            assert load.call_count == 0

            # No instance token: Rust obtains one instead of making Python refresh.
            assert uncached_table.new_read_builder().new_scan().plan().snapshot_id == 2
            assert load.call_count == 1


@pytest.mark.parametrize('branch', [None, 'dev'])
def test_rest_dotted_database_and_table_keep_identity(rest_catalog, branch):
    from pypaimon.common.identifier import Identifier
    from pypaimon.tests.native_plan_resolved_schema_test import _assert_parity, _write
    catalog, server = rest_catalog
    identifier = Identifier('namespace.database', 'table.with.dots')
    catalog.create_database(identifier.get_database_name(), False)
    catalog.create_table(identifier, Schema.from_pyarrow_schema(
        pa.schema([('id', pa.int64()), ('value', pa.string())])), False)
    table = catalog.get_table(identifier)
    _write(table, [{'id': 1, 'value': 'old'}])
    if branch:
        catalog.create_tag(identifier, 'first', 1)
        catalog.create_branch(identifier, branch, tag_name='first')
        _write(table, [{'id': 2, 'value': 'main'}])
        table = catalog.get_table(Identifier('namespace.database', 'table.with.dots', branch=branch))
    with patch.object(server, '_table_snapshot_handle', wraps=server._table_snapshot_handle) as load:
        _assert_parity(table, [{'id': 1, 'value': 'old'}], 1)
        assert all((call.args[1].get_database_name(), call.args[1].get_table_name())
                   == ('namespace.database', 'table.with.dots') for call in load.call_args_list)


def test_rest_blob_view_limit_filters_before_resolving_unselected_view(rest_catalog):
    catalog, _ = rest_catalog
    schema = pa.schema([('id', pa.int32()), ('payload', pa.large_binary())])
    options = {'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'}
    catalog.create_table('default.source', Schema.from_pyarrow_schema(
        schema, options=options), False)
    source = catalog.get_table('default.source')
    writer = source.new_batch_write_builder().new_write()
    writer.write_arrow(pa.Table.from_pydict({
        'id': [1, 2], 'payload': [b'first', b'selected'],
    }, schema=schema))
    source.new_batch_write_builder().new_commit().commit(writer.prepare_commit())
    writer.close()
    payload_id = next(field.id for field in source.table_schema.fields
                      if field.name == 'payload')

    catalog.create_table('default.views', Schema.from_pyarrow_schema(
        schema, options=dict(options, **{'blob-view-field': 'payload'})), False)
    views = catalog.get_table('default.views')
    writer = views.new_batch_write_builder().new_write()
    writer.write_arrow(pa.Table.from_pydict({
        'id': [10, 11], 'payload': [
            BlobViewStruct('default.source', payload_id, 99).serialize(),
            BlobViewStruct('default.source', payload_id, 1).serialize(),
        ],
    }, schema=schema))
    views.new_batch_write_builder().new_commit().commit(writer.prepare_commit())
    writer.close()

    views = views.copy({
        'scan.native-plan.enabled': 'true', 'read.native.enabled': 'true',
    })
    builder = views.new_read_builder().with_limit(1)
    builder.with_filter(builder.new_predicate_builder().equal('id', 11))
    scan = builder.new_scan()
    with patch.object(scan.file_scanner, 'scan',
                      side_effect=AssertionError('native view plan fell back')):
        plan = scan.plan()
    assert all(getattr(split, '_native_split', None) is not None
               for split in plan.splits())
    with patch('pypaimon.read.table_read.TableRead._create_split_read',
               side_effect=AssertionError('native view read fell back')):
        assert builder.new_read().to_arrow(plan.splits()).to_pylist() == [
            {'id': 11, 'payload': b'selected'}]


def test_reused_rest_environment_sees_new_snapshot(rest_source):
    from pypaimon.tests.native_plan_resolved_schema_test import _assert_parity, _write
    table, _, _ = rest_source
    rows = [{'id': 1, 'value': 'old'}, {'id': 2, 'value': 'new'}]
    _assert_parity(table, rows, 2)
    _write(table, [{'id': 3, 'value': 'latest'}])
    _assert_parity(table.copy({'read.batch-size': '1'}), rows + [{'id': 3, 'value': 'latest'}], 3)


@pytest.mark.skipif(not native_method_available('Table', 'from_rest_response'),
                    reason='REST response binding required')
def test_repeated_plans_reuse_remote_file_sizes(rest_source, tmp_path):
    import json
    from collections import Counter
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from pathlib import Path
    from threading import Thread
    from urllib.parse import urlparse

    from pypaimon.catalog.catalog_context import CatalogContext
    from pypaimon.catalog.catalog_environment import CatalogEnvironment
    from pypaimon.catalog.rest.rest_catalog_loader import RESTCatalogLoader
    from pypaimon.common.options.options import Options
    from pypaimon.read.native_plan import native_plan

    table, _, _ = rest_source
    root = Path(urlparse(table.table_path).path)
    requests = Counter()

    class ObjectStore(BaseHTTPRequestHandler):
        def do_HEAD(self):
            self.serve()

        def do_GET(self):
            self.serve()

        def serve(self):
            path = urlparse(self.path).path
            requests[self.command, path] += 1
            file = root / path[len('/bucket/t/'):]
            if not file.is_file():
                self.send_error(404)
                return
            data = file.read_bytes()
            size = len(data)
            byte_range = self.headers.get('Range')
            self.send_response(206 if byte_range else 200)
            if byte_range:
                start, end = byte_range[6:].split('-')
                start, end = int(start), int(end) if end else size - 1
                data = data[start:end + 1]
                self.send_header('Content-Range', 'bytes %s-%s/%s' % (start, end, size))
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            if self.command == 'GET':
                self.wfile.write(data)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(('127.0.0.1', 0), ObjectStore)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        options = dict(table.catalog_environment.catalog_loader.context().options.to_map(), **{
            's3.endpoint': 'http://127.0.0.1:%s' % server.server_port,
            's3.region': 'us-east-1', 's3.path.style.access': 'true', 's3.anonymous': 'true',
            'local-cache.enabled': 'true', 'local-cache.dir': str(tmp_path / 'cache')})
        response = json.loads(table.catalog_environment.rest_table_response)
        response['path'] = 's3://bucket/t'
        table.table_path = response['path']
        table.catalog_environment = CatalogEnvironment(
            identifier=table.identifier, uuid=response['id'], supports_version_management=True,
            catalog_loader=RESTCatalogLoader(CatalogContext.create_from_options(Options(options))),
            rest_table_response=json.dumps(response))
        first = native_plan(table)
        initial = requests.copy()
        assert sum(n for (method, _), n in initial.items() if method == 'HEAD') > 0
        second = native_plan(table.copy({'read.batch-size': '1'}))
        assert second.snapshot_id == first.snapshot_id == 2
        assert len(second.splits()) == len(first.splits()) > 0
        assert requests == initial
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


@pytest.fixture
def rest_blob_views(rest_catalog):
    catalog, server = rest_catalog
    schema = pa.schema([('id', pa.int32()), ('payload', pa.large_binary())])
    common = {'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'}
    for name in ['upstream', 'middle', 'root']:
        extra = {} if name == 'upstream' else {'blob-view-field': 'payload'}
        catalog.create_table('default.' + name, Schema.from_pyarrow_schema(
            schema, options=dict(common, **extra)), False)

    def write(table, rows):
        builder = table.new_batch_write_builder()
        writer = builder.new_write()
        try:
            writer.write_arrow(pa.Table.from_pylist(rows, schema=schema))
            builder.new_commit().commit(writer.prepare_commit())
        finally:
            writer.close()

    upstream = catalog.get_table('default.upstream')
    write(upstream, [{'id': 1, 'payload': b'actual'}, {'id': 2, 'payload': None}])
    middle = catalog.get_table('default.middle')
    write(middle, [dict(id=i, payload=BlobViewStruct(
        'default.upstream', upstream.field_dict['payload'].id, i).serialize()) for i in [0, 1]])
    root = catalog.get_table('default.root')
    write(root, [dict(id=i, payload=BlobViewStruct(
        middle.identifier, middle.field_dict['payload'].id, i).serialize()) for i in [0, 1]])
    return catalog, server, upstream, middle, root


def _require_blob_view_read_via(server, expected):
    from pypaimon.api.rest_api import RESTApi
    from pypaimon.api.rest_util import RESTUtil
    from pypaimon.common.identifier import Identifier
    from pypaimon.common.json_util import JSON

    route = server._route_request
    requests = []

    def require_read_via(method, resource_path, parameters, data, headers):
        if any('/tables/' + name in resource_path for name in ['upstream', 'middle']
               if name != expected.get_table_name()):
            encoded = next((value for key, value in headers.items()
                            if key.lower() == RESTApi.READ_VIA_HEADER.lower()), None)
            actual = JSON.from_json(RESTUtil.decode_string(encoded), Identifier) if encoded else None
            requests.append((resource_path, actual))
            if actual != expected:
                return server._mock_response(ErrorResponse(
                    'TABLE', 'upstream', 'dependency needs root Read-Via', 403), 403)
        return route(method, resource_path, parameters, data, headers)

    return patch.object(server, '_route_request', side_effect=require_read_via), requests


@pytest.mark.python_plan
@pytest.mark.python_read
@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('nested', [False, True])
def test_rest_blob_view_dependency_read_via(rest_blob_views, native, nested):
    from contextlib import ExitStack
    from pypaimon.table.row.blob import BlobDescriptor

    _, server, _, middle, root = rest_blob_views
    root = root if nested else middle
    root = root.copy({'scan.native-plan.enabled': str(native).lower(),
                      'read.native.enabled': str(native).lower()})
    guard, requests = _require_blob_view_read_via(server, root.identifier)
    with guard, ExitStack() as stack:
        if native:
            stack.enter_context(patch('pypaimon.read.table_read.TableRead._create_split_read',
                                      side_effect=AssertionError('native Blob view read fell back')))
        builder = root.new_read_builder()
        rows = builder.new_read().to_arrow(builder.new_scan().plan().splits()).sort_by('id').to_pylist()
        assert rows == [dict(id=0, payload=b'actual'), dict(id=1, payload=None)]
        descriptors = root.copy({'blob-as-descriptor': 'true'}).new_read_builder()
        values = descriptors.new_read().to_arrow(descriptors.new_scan().plan().splits()).sort_by('id')
        assert BlobDescriptor.is_blob_descriptor(values.column('payload')[0].as_py())
        assert values.column('payload')[1].as_py() is None
    assert requests and all(identifier == root.identifier for _, identifier in requests)


@pytest.mark.python_plan
@pytest.mark.python_read
@pytest.mark.parametrize('native', [False, True])
def test_rest_blob_view_merge_source_keeps_dependency_context(rest_blob_views, native):
    from contextlib import ExitStack
    from pypaimon.table.data_evolution_merge_into import WhenNotMatched

    catalog, server, _, _, root = rest_blob_views
    schema = pa.schema([('id', pa.int32()), ('payload', pa.large_binary())])
    catalog.create_table('default.target', Schema.from_pyarrow_schema(schema, options={
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'write.native.enabled': str(native).lower(),
        'read.native.enabled': str(native).lower(),
        'scan.native-plan.enabled': str(native).lower(),
    }), False)
    target = catalog.get_table('default.target')
    builder = target.new_batch_write_builder()
    guard, requests = _require_blob_view_read_via(server, root.identifier)
    with guard, ExitStack() as stack:
        if native:
            stack.enter_context(patch('pypaimon.table.data_evolution_merge_into._normalize_source',
                                      side_effect=AssertionError('Python MERGE source materialization')))
        messages = builder.new_update().merge_into(root, on=['id'], when_not_matched=[WhenNotMatched('*')])
        builder.new_commit().commit(messages)
    reader = target.new_read_builder()
    assert reader.new_read().to_arrow(reader.new_scan().plan().splits()).sort_by('id').to_pylist() == [
        dict(id=0, payload=b'actual'), dict(id=1, payload=None)]
    assert requests and all(identifier == root.identifier for _, identifier in requests)


@pytest.mark.python_plan
@pytest.mark.python_read
@pytest.mark.parametrize('native', [False, True])
def test_rest_blob_view_payload_uses_upstream_token(rest_blob_views, native):
    import json
    import time
    from contextlib import ExitStack
    from collections import Counter
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from pathlib import Path
    from threading import Thread
    from urllib.parse import urlparse

    from pypaimon.api.api_response import GetTableTokenResponse
    from pypaimon.catalog.rest.rest_token import RESTToken
    from pypaimon.catalog.rest.rest_token_file_io import RESTTokenFileIO
    from pypaimon.common.identifier import Identifier

    catalog, rest_server, upstream, middle, _ = rest_blob_views
    roots = {table.identifier.get_table_name(): Path(urlparse(table.table_path).path)
             for table in [upstream, middle]}
    requests = []

    class ObjectStore(BaseHTTPRequestHandler):
        def do_HEAD(self):
            self.serve()

        def do_GET(self):
            self.serve()

        def serve(self):
            path = urlparse(self.path).path
            parts = path.split('/', 3)
            if len(parts) != 4 or parts[1] != 'bucket' or parts[2] not in roots:
                self.send_error(404)
                return
            name, relative = parts[2:]
            auth = self.headers.get('Authorization', '')
            requests.append((name, relative, auth))
            if 'Credential=' + name + '-key/' not in auth:
                self.send_error(403, 'wrong table token')
                return
            file = roots[name] / relative
            if not file.is_file():
                self.send_error(404)
                return
            data = file.read_bytes()
            size = len(data)
            byte_range = self.headers.get('Range')
            self.send_response(206 if byte_range else 200)
            if byte_range:
                start, end = byte_range[6:].split('-')
                start, end = int(start), int(end) if end else size - 1
                data = data[start:end + 1]
                self.send_header('Content-Range', 'bytes %s-%s/%s' % (start, end, size))
            self.send_header('Content-Length', str(len(data)))
            self.send_header('ETag', '"immutable-fixture"')
            self.end_headers()
            if self.command == 'GET':
                self.wfile.write(data)

        def log_message(self, *args):
            pass

    store = ThreadingHTTPServer(('127.0.0.1', 0), ObjectStore)
    thread = Thread(target=store.serve_forever, daemon=True)
    thread.start()
    table_handle = rest_server._table_handle
    token_handle = rest_server._table_token_handle
    token_requests = Counter()

    def remote_table(method, data, identifier, response_identifier=None):
        body, status = table_handle(method, data, identifier, response_identifier)
        if method == 'GET' and identifier.get_table_name() in roots:
            response = json.loads(body)
            response['path'] = 's3://bucket/' + identifier.get_table_name()
            body = json.dumps(response)
        return body, status

    def token(method, identifier):
        name = identifier.get_table_name()
        token_requests[name] += 1
        # Force an upstream refresh without sleeps; it must keep Read-Via.
        if name == 'upstream' and token_requests[name] == 1:
            return rest_server._mock_response(GetTableTokenResponse(
                token={'s3.access-key': name + '-key', 's3.secret-key': 'test-secret'},
                expires_at_millis=0), 200)
        return token_handle(method, identifier)

    try:
        options = dict(catalog.context.options.to_map(), **{
            's3.endpoint': 'http://127.0.0.1:%s' % store.server_port,
            's3.region': 'us-east-1', 's3.path.style.access': 'true',
            'data-token.enabled': 'true'})
        for name in roots:
            rest_server.set_table_token(Identifier('default', name), RESTToken({
                's3.access-key': name + '-key', 's3.secret-key': 'test-secret'},
                int(time.time() * 1000) + 7_200_000))
        with patch.object(rest_server, '_table_handle', side_effect=remote_table), \
                patch.object(rest_server, '_table_token_handle', side_effect=token), \
                patch.object(RESTTokenFileIO, '_TOKEN_CACHE', {}):
            view = CatalogFactory.create(options).get_table('default.middle').copy({
                'scan.native-plan.enabled': str(native).lower(),
                'read.native.enabled': str(native).lower()})
            guard, dependency_requests = _require_blob_view_read_via(rest_server, view.identifier)
            with guard, ExitStack() as stack:
                if native:
                    stack.enter_context(patch('pypaimon.read.table_read.TableRead._create_split_read',
                                              side_effect=AssertionError('token view read fell back')))
                builder = view.new_read_builder()
                result = builder.new_read().to_arrow(builder.new_scan().plan().splits()).sort_by('id')
            assert result.to_pylist() == [dict(id=0, payload=b'actual'), dict(id=1, payload=None)]
            assert dependency_requests
        assert token_requests['upstream'] >= 2
        assert any(name == 'upstream' and path.endswith('.blob') for name, path, _ in requests)
        assert all('Credential=' + name + '-key/' in auth for name, _, auth in requests)
    finally:
        store.shutdown()
        thread.join()
        store.server_close()


@pytest.mark.python_plan
@pytest.mark.python_read
def test_native_rest_blob_views_group_upstream_fields_by_id(rest_catalog):
    from pypaimon.schema.schema_change import SchemaChange

    catalog, server = rest_catalog
    source_schema = pa.schema([('id', pa.int32()), ('other', pa.int32()), ('picture', pa.large_binary()),
                               ('thumbnail', pa.large_binary())])
    options = {'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'}
    catalog.create_table('default.upstream', Schema.from_pyarrow_schema(source_schema, options=options), False)
    source = catalog.get_table('default.upstream')
    writer = source.new_batch_write_builder().new_write()
    try:
        writer.write_arrow(pa.Table.from_pylist([
            dict(id=1, other=10, picture=b'picture-0', thumbnail=b'thumb-0'),
            dict(id=2, other=20, picture=b'picture-1', thumbnail=None),
        ], schema=source_schema))
        source.new_batch_write_builder().new_commit().commit(writer.prepare_commit())
    finally:
        writer.close()
    schema = pa.schema([('id', pa.int32()), ('left', pa.large_binary()), ('right', pa.large_binary())])
    catalog.create_table('default.grouped', Schema.from_pyarrow_schema(
        schema, options=dict(options, **{'blob-view-field': 'left,right'})), False)
    view = catalog.get_table('default.grouped').copy({
        'scan.native-plan.enabled': 'true', 'read.native.enabled': 'true'})
    picture_id = source.field_dict['picture'].id
    thumbnail_id = source.field_dict['thumbnail'].id
    refs = [dict(id=i, left=BlobViewStruct('default.upstream', picture_id, 1 - i).serialize(),
                 right=BlobViewStruct('default.upstream', thumbnail_id, i).serialize()) for i in [0, 1]]
    writer = view.new_batch_write_builder().new_write()
    try:
        writer.write_arrow(pa.Table.from_pylist(refs, schema=schema))
        view.new_batch_write_builder().new_commit().commit(writer.prepare_commit())
    finally:
        writer.close()
    # Drop an ordinary column so BLOB field IDs differ from their positions.
    catalog.alter_table('default.upstream', [SchemaChange.drop_column('id')])
    guard, requests = _require_blob_view_read_via(server, view.identifier)
    with guard, patch('pypaimon.read.table_read.TableRead._create_split_read',
                      side_effect=AssertionError('multi-field view read fell back')):
        builder = view.new_read_builder()
        rows = builder.new_read().to_arrow(builder.new_scan().plan().splits()).sort_by('id').to_pylist()
    assert rows == [dict(id=0, left=b'picture-1', right=b'thumb-0'),
                    dict(id=1, left=b'picture-0', right=None)]
    metadata = [path for path, _ in requests if path.endswith('/tables/upstream')]
    snapshots = [path for path, _ in requests if path.endswith('/tables/upstream/snapshot')]
    assert len(metadata) == len(snapshots) == 1


@pytest.mark.python_plan
@pytest.mark.python_read
@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('branch', [None, 'dev'])
@pytest.mark.parametrize('explicit', [False, True])
def test_rest_blob_view_keeps_branch_and_explicit_outer_context(rest_blob_views, native, branch, explicit):
    from contextlib import ExitStack
    from pypaimon.api.rest_api import RESTApi
    from pypaimon.api.rest_util import RESTUtil
    from pypaimon.common.identifier import Identifier
    from pypaimon.common.json_util import JSON

    catalog, server, _, _, root = rest_blob_views
    if branch:
        catalog.create_tag(root.identifier, 'base')
        catalog.create_branch(root.identifier, branch, tag_name='base')
    identifier = Identifier('default', 'root', branch=branch)
    outer = Identifier('outer 空间', 'parent.with.dots')
    header = RESTUtil.encode_string(JSON.to_json(outer, separators=(',', ':')))
    options = dict(catalog.context.options.to_map())
    if explicit:
        options[RESTApi.HEADER_PREFIX + RESTApi.READ_VIA_HEADER] = header
    view = CatalogFactory.create(options).get_table(identifier).copy({
        'scan.native-plan.enabled': str(native).lower(), 'read.native.enabled': str(native).lower()})
    expected = outer if explicit else view.identifier
    guard, requests = _require_blob_view_read_via(server, expected)
    with guard, ExitStack() as stack:
        if native:
            stack.enter_context(patch('pypaimon.read.table_read.TableRead._create_split_read',
                                      side_effect=AssertionError('branch view read fell back')))
        builder = view.new_read_builder()
        result = builder.new_read().to_arrow(builder.new_scan().plan().splits()).sort_by('id')
    assert result.to_pylist() == [dict(id=0, payload=b'actual'), dict(id=1, payload=None)]
    assert requests and all(actual == expected for _, actual in requests)


@pytest.mark.python_plan
@pytest.mark.python_read
def test_native_rest_blob_view_does_not_bypass_denied_dependency(rest_blob_views):
    _, server, _, _, root = rest_blob_views
    root = root.copy({'scan.native-plan.enabled': 'true', 'read.native.enabled': 'true'})
    route = server._route_request

    def deny_upstream(method, path, parameters, data, headers):
        if '/tables/upstream' in path:
            return server._mock_response(ErrorResponse('TABLE', 'upstream', 'upstream denied', 403), 403)
        return route(method, path, parameters, data, headers)

    with patch.object(server, '_route_request', side_effect=deny_upstream), \
            patch('pypaimon.read.table_read.TableRead._create_split_read',
                  side_effect=AssertionError('denied dependency fell back')):
        builder = root.new_read_builder()
        with pytest.raises(Exception, match='upstream denied'):
            builder.new_read().to_arrow(builder.new_scan().plan().splits())
