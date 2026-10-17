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

"""Existing REST version-management behavior through Rust and Python transports."""

import json
from unittest.mock import patch
from urllib.parse import parse_qs, unquote_plus, urlsplit

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.api.rest_exception import BadRequestException, NotImplementedException, ServiceFailureException
from pypaimon.catalog.catalog_exception import (
    TableNotExistException, TableNoPermissionException, TagAlreadyExistException, TagNotExistException,
)
from pypaimon.common.identifier import Identifier
from pypaimon.common.json_util import JSON
from pypaimon.table.instant import Instant
from pypaimon.tests import native_rest_branch_test as branch_fixtures

pytestmark = pytest.mark.native_plan
transport_server = branch_fixtures.transport_server
_OPERATIONS = ('create_tag', 'get_tag', 'list_tags_paged', 'delete_tag', 'load_snapshot', 'rollback_to')
_SNAPSHOT = dict(version=3, id=7, schemaId=0, baseManifestList='base', deltaManifestList='delta',
                 commitUser='writer', commitIdentifier=1, commitKind='APPEND', timeMillis=123,
                 uuid='snapshot-uuid', totalRecordCount=8, deltaRecordCount=1, properties={'source': 'test'},
                 baseManifestListSize=321, deltaManifestListSize=0)


@pytest.fixture
def version_server(transport_server, tmp_path):
    server = transport_server
    server.require_v4 = True
    server.uuid, server.head, server.status = 'incarnation-A', 9, None
    server.tags, server.next_token = ['z', 'a', 'z'], 'next +/%一'
    server.error_resource, server.error_name = None, None
    server.snapshot_response = dict(snapshot=dict(
        snapshot=_SNAPSHOT, recordCount=0, fileCount=2, fileSizeInBytes=123, lastFileCreationTime=456))
    schema = Schema.from_pyarrow_schema(pa.schema([('id', pa.int64())]))

    def respond(method, path, data):
        if server.status is not None:
            return server.status, json.dumps(dict(
                resourceType=server.error_resource, resourceName=server.error_name, message='rejected'))
        parsed = urlsplit(path)
        if parsed.path.endswith('/rollback'):
            body = json.loads(data)
            if 'tableId' in body and body['tableId'] != server.uuid:
                return 400, json.dumps({'message': 'table UUID mismatch'})
            if 'fromSnapshot' in body and body['fromSnapshot'] != server.head:
                return 400, json.dumps({'message': 'snapshot head mismatch'})
            instant = body['instant']
            server.head = instant['snapshotId'] if instant['type'] == 'snapshot' else 7
            return 200, 'rolled back'
        if parsed.path.endswith('/snapshot'):
            return 200, json.dumps(server.snapshot_response)
        if parsed.path.endswith('/tags'):
            return (200, json.dumps(dict(tags=server.tags, nextPageToken=server.next_token))) \
                if method == 'GET' else (200, 'created')
        if '/tags/' in parsed.path:
            return (200, json.dumps(dict(
                tagName=unquote_plus(parsed.path.rsplit('/', 1)[1]), snapshot=_SNAPSHOT,
                tagCreateTime=123, tagTimeRetained='2 d'))) if method == 'GET' else (200, 'deleted')
        return 200, json.dumps(dict(
            id=server.uuid, name=unquote_plus(parsed.path.rsplit('/', 1)[1]),
            schema=json.loads(JSON.to_json(schema)), schemaId=0, path=tmp_path.as_uri(), isExternal=False))

    server.respond = respond
    return server


def make_catalog(server, native=True):
    return branch_fixtures.transport_catalog(server, **{
        'token.provider': 'dlf', 'dlf.region': 'cn-hangzhou', 'dlf.signing-algorithm': 'openapi-v4',
        'dlf.access-key-id': 'test-key', 'dlf.access-key-secret': 'test-secret', 'dlf.security-token': 'test-sts',
        'read.native.enabled': str(native).lower(), 'write.native.enabled': str(native).lower(),
    })


def invoke(catalog, operation, identifier=Identifier('db /一', 'table', branch='dev /一')):
    if operation == 'create_tag':
        return catalog.create_tag(identifier, 'release /一', 7, '2 d')
    if operation in ('get_tag', 'delete_tag'):
        return getattr(catalog, operation)(identifier, 'release /一')
    if operation == 'list_tags_paged':
        return catalog.list_tags_paged(identifier, 3, 'next +/%一', 'prefix +/%一')
    if operation == 'rollback_to':
        return catalog.rollback_to(identifier, Instant.snapshot(7), 9, table_uuid='incarnation-A')
    return catalog.load_snapshot(identifier)


@pytest.mark.parametrize('operation', _OPERATIONS)
def test_version_operations_use_rust_with_v4_and_public_python_results(version_server, operation):
    server = version_server
    catalog = make_catalog(server)
    server.requests.clear()
    server.bodies.clear()
    with patch.object(catalog.rest_api, operation, side_effect=AssertionError('Python fallback')):
        result = invoke(catalog, operation)
    assert len(server.requests) == 1
    method, path, headers = server.requests[0]
    assert headers['Authorization'].startswith('ACS4-HMAC-SHA256 ')
    assert headers['x-acs-action'] == {
        'create_tag': 'CreateTag', 'get_tag': 'GetTag', 'list_tags_paged': 'ListTags',
        'delete_tag': 'DropTag', 'load_snapshot': 'GetTableSnapshot', 'rollback_to': 'RollbackToSnapshot',
    }[operation]
    assert '/tables/table%24branch_dev+%2F%E4%B8%80/' in path
    if operation == 'create_tag':
        assert server.bodies == [dict(tagName='release /一', snapshotId=7, timeRetained='2 d')]
    elif operation == 'rollback_to':
        assert server.bodies == [dict(
            instant=dict(type='snapshot', snapshotId=7), fromSnapshot=9, tableId='incarnation-A')]
        assert server.head == 7
    elif operation == 'get_tag':
        assert result.tag_name == 'release /一'
        assert result.snapshot.id == 7
        assert result.snapshot.uuid == 'snapshot-uuid'
        assert (result.snapshot.base_manifest_list_size, result.snapshot.delta_manifest_list_size) == (321, 0)
        assert result.tag_create_time == 123 and result.tag_time_retained == '2 d'
    elif operation == 'list_tags_paged':
        assert result.elements == ['z', 'a', 'z'] and result.next_page_token == 'next +/%一'
        assert parse_qs(urlsplit(path).query) == {
            'maxResults': ['3'], 'pageToken': ['next +/%一'], 'tagNamePrefix': ['prefix +/%一']}
    elif operation == 'load_snapshot':
        assert result.snapshot.id == 7 and result.snapshot.uuid == 'snapshot-uuid'
        assert (result.snapshot.base_manifest_list_size, result.snapshot.delta_manifest_list_size) == (321, 0)
        assert (result.record_count, result.file_count, result.file_size_in_bytes,
                result.last_file_creation_time) == (0, 2, 123, 456)
    else:
        assert result is None and method == 'DELETE'


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('tags,expected_token', [(None, None), ([], 'next +/%一')])
def test_null_tag_page_ends_paging_but_empty_page_preserves_token(version_server, native, tags, expected_token):
    server = version_server
    catalog = make_catalog(server, native)
    server.tags = tags
    server.requests.clear()
    page = catalog.list_tags_paged('db.table', 0, '', ' ')
    assert page.elements == [] and page.next_page_token == expected_token
    assert parse_qs(urlsplit(server.requests[0][1]).query, keep_blank_values=True) == {
        'pageToken': [''], 'tagNamePrefix': [' ']}


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('instant', [Instant.snapshot(7), Instant.tag('release /一')])
def test_optional_rollback_guards_are_omitted_on_both_paths(version_server, native, instant):
    server = version_server
    catalog = make_catalog(server, native)
    server.bodies.clear()
    catalog.rollback_to('db.table', instant)
    assert server.bodies == [{'instant': instant.to_dict()}]


@pytest.mark.parametrize('operation', _OPERATIONS)
@pytest.mark.parametrize('status', [403, 404])
def test_version_errors_keep_public_exception_types(version_server, operation, status):
    server = version_server
    catalog = make_catalog(server)
    server.requests.clear()
    server.status, server.error_resource, server.error_name = status, 'TABLE', 'db.table'
    expected = TableNoPermissionException if status == 403 else TableNotExistException
    with patch.object(catalog.rest_api, operation, side_effect=AssertionError('Python replay')):
        with pytest.raises(expected):
            invoke(catalog, operation)
    assert len(server.requests) == 1


@pytest.mark.parametrize('operation,resource,expected', [
    ('create_tag', 'TAG', TagAlreadyExistException),
    ('create_tag', 'SNAPSHOT', ValueError),
    ('get_tag', 'TAG', TagNotExistException),
    ('delete_tag', 'TAG', TagNotExistException),
    ('rollback_to', 'TAG', ValueError),
    ('rollback_to', 'SNAPSHOT', ValueError),
])
def test_missing_tag_and_snapshot_errors_remain_distinct(version_server, operation, resource, expected):
    server = version_server
    catalog = make_catalog(server)
    server.status = 409 if expected is TagAlreadyExistException else 404
    server.error_resource, server.error_name = resource, '7' if resource == 'SNAPSHOT' else 'release /一'
    with pytest.raises(expected):
        invoke(catalog, operation)


@pytest.mark.parametrize('operation', ['create_tag', 'delete_tag', 'rollback_to'])
def test_failure_after_publication_has_no_backend_replay_or_local_cleanup(version_server, operation, tmp_path):
    server = version_server
    catalog = make_catalog(server)
    sentinel = tmp_path / 'shared-data.parquet'
    sentinel.write_bytes(b'existing data')
    server.status = 500
    server.requests.clear()
    with patch.object(catalog.rest_api, operation, side_effect=AssertionError('Python replay')):
        with pytest.raises(ServiceFailureException):
            invoke(catalog, operation)
    assert len(server.requests) == 1
    assert sentinel.read_bytes() == b'existing data'


@pytest.mark.parametrize('rejection', ['identity', 'head'])
def test_loaded_table_rollback_preserves_identity_and_never_reloads_on_rejection(version_server, rejection):
    server = version_server
    catalog = make_catalog(server)
    table = catalog.get_table('db.table')
    server.requests.clear()
    server.bodies.clear()
    if rejection == 'identity':
        server.uuid = 'incarnation-B'
    with patch.object(table, 'rollback_helper', side_effect=AssertionError('local cleanup')):
        if rejection == 'identity':
            with pytest.raises(RuntimeError, match='table UUID mismatch'):
                table.rollback_to(7)
        else:
            with pytest.raises(BadRequestException, match='snapshot head mismatch'):
                catalog.rollback_to('db.table', Instant.snapshot(7), 8, table_uuid='incarnation-A')
    assert len(server.requests) == 1
    assert server.bodies[0]['tableId'] == 'incarnation-A'
    assert server.head == 9


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('response', [None, {}, {'snapshot': None}])
def test_empty_snapshot_responses_are_empty_views(version_server, native, response):
    server = version_server
    catalog = make_catalog(server, native)
    server.snapshot_response = response
    assert catalog.load_snapshot('db.table') is None


def test_empty_snapshot_error_is_not_a_missing_table(version_server):
    server = version_server
    catalog = make_catalog(server)
    server.status, server.error_resource = 404, 'SNAPSHOT'
    assert catalog.load_snapshot('db.table') is None


@pytest.mark.parametrize('operation', _OPERATIONS)
def test_unimplemented_version_endpoints_keep_rest_exception_and_never_replay(version_server, operation):
    server = version_server
    catalog = make_catalog(server)
    server.status = 501
    server.requests.clear()
    with patch.object(catalog.rest_api, operation, side_effect=AssertionError('Python replay')):
        with pytest.raises(NotImplementedException):
            invoke(catalog, operation)
    assert len(server.requests) == 1


@pytest.mark.parametrize('operation', _OPERATIONS)
def test_missing_native_operation_is_detected_before_execution(version_server, operation):
    server = version_server
    catalog = make_catalog(server)

    class UnavailableCatalog:
        def __init__(self, *args, **kwargs):
            raise AssertionError('Unavailable operation must be detected before constructing the catalog')

    server.requests.clear()
    with patch('pypaimon_rust.datafusion.PaimonCatalog', UnavailableCatalog), \
            patch.object(catalog.rest_api, operation, wraps=getattr(catalog.rest_api, operation)) as execute:
        invoke(catalog, operation)
    execute.assert_called_once()
    assert len(server.requests) == 1


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('snapshot_name', ['null', None])
def test_create_tag_on_empty_table_preserves_missing_snapshot(version_server, native, snapshot_name):
    server = version_server
    catalog = make_catalog(server, native)
    server.status, server.error_resource, server.error_name = 404, 'SNAPSHOT', snapshot_name
    server.requests.clear()
    with pytest.raises(ValueError, match='Snapshot'):
        catalog.create_tag('db.table', 'release')
    assert len(server.requests) == 1


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('operation', ['create_tag', 'get_tag', 'delete_tag'])
@pytest.mark.parametrize('resource', ['tag', 'snapshot'])
def test_resource_types_use_java_case_sensitive_names(version_server, native, operation, resource):
    server = version_server
    catalog = make_catalog(server, native)
    server.status, server.error_resource, server.error_name = 404, resource, '7'
    with pytest.raises(TableNotExistException):
        invoke(catalog, operation)
