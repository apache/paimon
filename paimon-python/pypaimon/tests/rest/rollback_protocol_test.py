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
from types import SimpleNamespace
from urllib.parse import unquote_plus, urlsplit

import pyarrow as pa
import pytest
import requests

from pypaimon import Schema
from pypaimon.api.api_request import RollbackTableRequest
from pypaimon.api.api_response import GetTableResponse
from pypaimon.catalog.catalog_context import CatalogContext
from pypaimon.catalog.catalog_exception import TableNotExistException, TableNoPermissionException
from pypaimon.catalog.rest.rest_catalog import RESTCatalog
from pypaimon.catalog.table_rollback import TableRollback
from pypaimon.common.identifier import Identifier
from pypaimon.common.json_util import JSON
from pypaimon.common.options import Options
from pypaimon.read.native_plan import _resolved_rest_table_response
from pypaimon.table.instant import Instant
from pypaimon.write.commit.commit_rollback import CommitRollback
from pypaimon.write.native_commit import _rest_catalog_supported


@pytest.fixture
def transport(monkeypatch, tmp_path):
    state = SimpleNamespace(requests=[], uuid='loaded-table-uuid', status=200)
    schema = Schema.from_pyarrow_schema(pa.schema([('id', pa.int64())]))

    def send(session, request, **kwargs):
        state.requests.append(request)
        response = requests.Response()
        response.request = request
        response.url = request.url
        if request.method == 'GET':
            metadata = GetTableResponse(
                id=state.uuid,
                name=unquote_plus(urlsplit(request.url).path.rsplit('/', 1)[-1]),
                path=tmp_path.as_uri(), is_external=False, schema_id=0, schema=schema)
            response.status_code = 200
            response._content = JSON.to_json(metadata).encode('utf-8')
        else:
            response.status_code = state.status
            response._content = (b'' if state.status == 200 else json.dumps({
                'code': state.status, 'message': 'table UUID mismatch',
            }).encode('utf-8'))
        return response

    monkeypatch.setattr(requests.Session, 'send', send)
    return state


@pytest.fixture
def catalog(transport):
    return RESTCatalog(CatalogContext.create_from_options(Options({
        'metastore': 'rest', 'uri': 'http://127.0.0.1:1', 'warehouse': 'warehouse',
        'prefix': 'rollback-test', 'token.provider': 'bear', 'token': 'test-token',
        'data-token.enabled': 'false',
    })), config_required=False)


@pytest.fixture
def table(catalog, transport):
    loaded = catalog.get_table(Identifier('db', 'table'))
    transport.requests.clear()
    return loaded


def request_body(request):
    return json.loads(request.body)


@pytest.mark.parametrize('table_uuid', [None, 'loaded-table-uuid'])
def test_request_serialization(table_uuid):
    request = RollbackTableRequest(Instant.snapshot(1), 2, table_id=table_uuid)
    expected = {'instant': {'type': 'snapshot', 'snapshotId': 1}, 'fromSnapshot': 2}
    if table_uuid is not None:
        expected['tableId'] = table_uuid

    assert json.loads(JSON.to_json(request)) == expected


def test_legacy_request_constructor():
    assert json.loads(JSON.to_json(RollbackTableRequest(Instant.snapshot(1), None))) == {
        'instant': {'type': 'snapshot', 'snapshotId': 1}, 'fromSnapshot': None,
    }


@pytest.mark.parametrize('target,instant', [
    (1, {'type': 'snapshot', 'snapshotId': 1}),
    ('v1', {'type': 'tag', 'tagName': 'v1'}),
])
def test_table_rollback_transmits_loaded_uuid(table, transport, target, instant):
    table.rollback_to(target)

    assert len(transport.requests) == 1
    assert transport.requests[0].method == 'POST'
    assert request_body(transport.requests[0]) == {
        'instant': instant, 'fromSnapshot': None, 'tableId': 'loaded-table-uuid',
    }


def test_compact_rollback_transmits_uuid_and_expected_head(table, transport):
    rollback = CommitRollback(table.catalog_environment.catalog_table_rollback())

    assert rollback.try_to_rollback(SimpleNamespace(commit_kind='COMPACT', id=3))

    assert len(transport.requests) == 1
    assert request_body(transport.requests[0]) == {
        'instant': {'type': 'snapshot', 'snapshotId': 2},
        'fromSnapshot': 3, 'tableId': 'loaded-table-uuid',
    }


@pytest.mark.parametrize('entrypoint', ['catalog', 'api'])
def test_direct_rollback_accepts_optional_uuid(catalog, transport, entrypoint):
    delegate = catalog if entrypoint == 'catalog' else catalog.rest_api

    delegate.rollback_to(Identifier('db', 'table'), Instant.snapshot(1), 2, table_uuid='expected-uuid')

    assert len(transport.requests) == 1
    assert request_body(transport.requests[0]) == {
        'instant': {'type': 'snapshot', 'snapshotId': 1},
        'fromSnapshot': 2, 'tableId': 'expected-uuid',
    }


def test_existing_calls_and_encoded_branch_url(catalog, transport):
    identifier = Identifier('db space', 'table name', branch='dev / blue')

    catalog.rollback_to(identifier, Instant.snapshot(1))
    catalog.rollback_to(identifier, Instant.snapshot(1), 2)
    catalog.rollback_to('db.table', Instant.tag('v1'))

    assert [request_body(request)['fromSnapshot'] for request in transport.requests] == [None, 2, None]
    assert all(set(request_body(request)) == {'instant', 'fromSnapshot'} for request in transport.requests)
    path = urlsplit(transport.requests[0].url).path.split('/')
    assert len(path) == 8
    assert unquote_plus(path[4]) == identifier.get_database_name()
    assert unquote_plus(path[6]) == identifier.get_object_name()
    assert path[7] == 'rollback'


def test_table_rollback_without_uuid(catalog, transport):
    transport.uuid = None
    table = catalog.get_table(Identifier('db', 'table'))
    transport.requests.clear()

    table.rollback_to(1)

    assert len(transport.requests) == 1
    assert request_body(transport.requests[0]) == {
        'instant': {'type': 'snapshot', 'snapshotId': 1}, 'fromSnapshot': None,
    }


@pytest.mark.parametrize('status,exception', [(403, TableNoPermissionException), (404, TableNotExistException)])
def test_existing_error_mapping(catalog, transport, status, exception):
    transport.status = status

    with pytest.raises(exception):
        catalog.rollback_to(Identifier('db', 'table'), Instant.snapshot(1), table_uuid='expected-uuid')

    assert len(transport.requests) == 1
    assert request_body(transport.requests[0])['tableId'] == 'expected-uuid'


@pytest.mark.parametrize('target', [1, 'v1'])
def test_replaced_table_does_not_refresh_retry_or_clean_locally(table, transport, monkeypatch, target):
    transport.uuid = 'replacement-table-uuid'
    transport.status = 400

    def unexpected_cleanup():
        pytest.fail('Catalog rejection must not trigger local cleanup')

    monkeypatch.setattr(table, 'rollback_helper', unexpected_cleanup)

    with pytest.raises(RuntimeError, match='table UUID mismatch'):
        table.rollback_to(target)

    assert len(transport.requests) == 1
    assert transport.requests[0].method == 'POST'
    assert request_body(transport.requests[0])['tableId'] == 'loaded-table-uuid'


def test_compact_rollback_rejection_preserves_identity(table, transport):
    transport.status = 400
    rollback = CommitRollback(table.catalog_environment.catalog_table_rollback())

    assert not rollback.try_to_rollback(SimpleNamespace(commit_kind='COMPACT', id=3))

    assert len(transport.requests) == 1
    assert request_body(transport.requests[0])['tableId'] == 'loaded-table-uuid'
    assert request_body(transport.requests[0])['fromSnapshot'] == 3


@pytest.mark.parametrize('table_uuid', [None, 'known-uuid'])
def test_legacy_catalog_override_never_discards_known_uuid(table_uuid):
    calls = []

    class LegacyCatalog:
        def rollback_to(self, identifier, instant, from_snapshot=None):
            calls.append((identifier, instant, from_snapshot))

    identifier = Identifier('db', 'table')
    instant = Instant.snapshot(1)
    if table_uuid is None:
        TableRollback(LegacyCatalog(), identifier).rollback_to(instant, 2)
        assert calls == [(identifier, instant, 2)]
    else:
        with pytest.raises(RuntimeError) as failure:
            TableRollback(LegacyCatalog(), identifier, table_uuid).rollback_to(instant, 2)
        assert isinstance(failure.value.__cause__, TypeError)
        assert calls == []


@pytest.mark.parametrize('source,target', [(None, 'dev'), ('dev', None), ('dev', 'release')])
def test_copied_branch_rollback_omits_source_identity(catalog, transport, source, target):
    table = catalog.get_table(Identifier('db', 'table', branch=source))
    copied = table.copy({'branch': target})
    transport.requests.clear()

    copied.rollback_to(1)

    assert table.catalog_environment.uuid == 'loaded-table-uuid'
    assert copied.catalog_environment.uuid is None
    assert len(transport.requests) == 1
    assert 'tableId' not in request_body(transport.requests[0])
    object_name = unquote_plus(urlsplit(transport.requests[0].url).path.split('/')[-2])
    assert object_name == Identifier('db', 'table', branch=target).get_object_name()


def test_same_branch_copy_and_independent_load_preserve_identity(table, catalog, transport):
    assert table.copy({'scan.parallelism': '2'}).catalog_environment.uuid == 'loaded-table-uuid'
    assert table.copy({'branch': 'main'}).catalog_environment.uuid == 'loaded-table-uuid'
    assert _rest_catalog_supported(table)
    assert not _rest_catalog_supported(table.copy({'branch': 'dev'}))

    transport.uuid = 'loaded-dev-uuid'
    branch = catalog.get_table(Identifier('db', 'table', branch='dev'))
    assert branch.current_branch() == 'dev'
    assert branch.catalog_environment.uuid == 'loaded-dev-uuid'
    assert branch.copy({'branch': 'dev'}).catalog_environment.uuid == 'loaded-dev-uuid'
    assert branch.catalog_environment.snapshot_commit(None).uuid == 'loaded-dev-uuid'
    assert _rest_catalog_supported(branch)
    transport.requests.clear()

    branch.rollback_to(1)

    assert len(transport.requests) == 1
    assert request_body(transport.requests[0])['tableId'] == 'loaded-dev-uuid'


def test_copied_branch_rejects_source_read_metadata(table, monkeypatch):
    monkeypatch.setattr('pypaimon.read.native_plan.native_method_available', lambda *args: True)

    assert _resolved_rest_table_response(table) == table.catalog_environment.rest_table_response
    assert _resolved_rest_table_response(table.copy({'branch': 'main'})) is not None
    assert _resolved_rest_table_response(table.copy({'branch': 'dev'})) is None
