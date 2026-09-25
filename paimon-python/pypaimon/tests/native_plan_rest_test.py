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
