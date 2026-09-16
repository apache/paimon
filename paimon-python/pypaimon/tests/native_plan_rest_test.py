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
from pypaimon.read.native_plan import native_runtime_available
from pypaimon.snapshot.table_snapshot import TableSnapshot
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
