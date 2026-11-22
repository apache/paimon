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
from urllib.parse import parse_qs, urlsplit

import pytest

from pypaimon.api.rest_exception import (
    BadRequestException, ForbiddenException, NoSuchResourceException, NotAuthorizedException,
    NotImplementedException, RESTException, ServiceFailureException,
)
from pypaimon.common.identifier import Identifier
from pypaimon.tests.rest import rest_branch_transport_test as transport


transport_server = transport.transport_server
BASE_PATH = '/v1/catalog%2Fid/databases'


@pytest.fixture
def catalog(transport_server):
    transport_server.require_v4 = True
    catalog = transport.transport_catalog(transport_server, **{
        'prefix': 'catalog/id', 'token.provider': 'dlf', 'dlf.region': 'cn-hangzhou',
        'dlf.signing-algorithm': 'openapi-v4', 'dlf.access-key-id': 'test-key',
        'dlf.access-key-secret': 'test-secret', 'dlf.security-token': 'test-sts',
    })
    transport_server.requests.clear()
    transport_server.bodies.clear()
    return catalog


@pytest.mark.parametrize('identifier,path', [
    ('sales.order.metrics', '/sales/semantic-views/order.metrics'),
    (Identifier.create('sales /\u534e+%', 'model.a/b+% ?#~'),
     '/sales+%2F%E5%8D%8E%2B%25/semantic-views/model.a%2Fb%2B%25+%3F%23~'),
    (Identifier.create('.', '..'), '/%2E/semantic-views/%2E%2E'),
    (Identifier.create('..', '%2E'), '/%2E%2E/semantic-views/%252E'),
    (Identifier.create('\u00a0', 'revenue'), '/%C2%A0/semantic-views/revenue'),
])
def test_get_semantic_view(catalog, transport_server, identifier, path):
    content = '# \u6307\u6807\r\nversion: 1\nfuture: {expr: "a + b\\c"}\n'
    transport_server.respond = lambda *_: (200, json.dumps({
        'name': 'returned-name', 'definition': {'format': 'provider-yaml', 'content': content},
    }))

    view = catalog.get_semantic_view(identifier)

    assert view.name == 'returned-name'
    assert view.definition.format == 'provider-yaml'
    assert view.definition.content == content
    [(method, target, headers)] = transport_server.requests
    assert (method, target) == ('GET', BASE_PATH + path)
    assert headers['x-acs-action'] == 'GetSemanticView'
    assert transport_server.bodies == [None]


def test_dot_only_names_preserve_existing_gateway_path_normalization(transport_server):
    def respond(method, target, body):
        path = urlsplit(target).path
        if path.endswith('/v1/config'):
            return 200, '{"defaults":{"prefix":"test"}}'
        if path.endswith('/databases'):
            return 200, '{"databases":["sales"],"nextPageToken":null}'
        return 200, '{"name":"..","definition":{"format":"provider-yaml","content":"model"}}'

    transport_server.respond = respond
    uri = 'http://localhost:{}/old/../gateway/%2E/%7E'.format(transport_server.server_port)
    catalog = transport.transport_catalog(transport_server, uri=uri)

    assert catalog.list_databases_paged().elements == ['sales']
    assert catalog.get_semantic_view(Identifier.create('.', '..')).name == '..'
    assert [urlsplit(target).path for _, target, _ in transport_server.requests] == [
        '/gateway/./~/v1/config',
        '/gateway/./~/v1/test/databases',
        '/gateway/./~/v1/test/databases/%2E/semantic-views/%2E%2E',
    ]


@pytest.mark.parametrize('prefix,encoded_prefix', [('.', '%2E'), ('..', '%2E%2E'), ('catalog/id', 'catalog%2Fid')])
@pytest.mark.parametrize('operation', ['get_semantic_view', 'list_semantic_views_paged'])
def test_semantic_views_use_prefix_discovered_from_config(transport_server, prefix, encoded_prefix, operation):
    collection_path = '/gateway/v1/{}/databases/sales/semantic-views'.format(encoded_prefix)
    view_path = collection_path + '/revenue'

    def respond(method, target, body):
        path = urlsplit(target).path
        if path == '/gateway/v1/config':
            return 200, json.dumps({'defaults': {'prefix': prefix}})
        if path == collection_path:
            return 200, '{"semanticViews":["revenue"]}'
        if path == view_path:
            return 200, '{"name":"revenue","definition":{"format":"provider-yaml","content":"model"}}'
        return 404, json.dumps({'message': 'Unexpected path: ' + path})

    transport_server.respond = respond
    uri = 'http://localhost:{}/gateway'.format(transport_server.server_port)
    catalog = transport.transport_catalog(transport_server, uri=uri)

    if operation == 'get_semantic_view':
        assert catalog.get_semantic_view('sales.revenue').name == 'revenue'
        resource_path = view_path
    else:
        assert catalog.list_semantic_views_paged('sales').elements == ['revenue']
        resource_path = collection_path
    assert [(method, urlsplit(target).path) for method, target, _ in transport_server.requests] == [
        ('GET', '/gateway/v1/config'), ('GET', resource_path),
    ]


def test_listed_semantic_view_name_can_be_used_to_read_definition(catalog, transport_server):
    name = '\u00a0'
    definition = {'format': '\u00a0', 'content': '\u2003'}
    replies = iter([{'semanticViews': [name]}, {'name': name, 'definition': definition}])
    transport_server.respond = lambda *_: (200, json.dumps(next(replies)))

    page = catalog.list_semantic_views_paged('sales')
    assert page.elements == [name]
    view = catalog.get_semantic_view(Identifier.create('sales', page.elements[0]))

    assert view.name == name
    assert view.definition.format == definition['format']
    assert view.definition.content == definition['content']
    assert [target for _, target, _ in transport_server.requests] == [
        BASE_PATH + '/sales/semantic-views', BASE_PATH + '/sales/semantic-views/%C2%A0',
    ]


def test_list_semantic_views_paged(catalog, transport_server):
    token = 'next +/%?&\u534e'
    replies = iter([
        {'semanticViews': ['revenue', 'margin'], 'nextPageToken': token},
        {'semanticViews': ['volume']},
        {'semanticViews': []},
        {'semanticViews': None},
    ])
    transport_server.respond = lambda *_: (200, json.dumps(next(replies)))

    first = catalog.list_semantic_views_paged('..', max_results=1000)
    last = catalog.list_semantic_views_paged('..', max_results=1, page_token=first.next_page_token)
    empty = catalog.list_semantic_views_paged('..')
    null_names = catalog.list_semantic_views_paged('..', page_token='')

    assert (first.elements, first.next_page_token) == (['revenue', 'margin'], token)
    assert (last.elements, last.next_page_token) == (['volume'], None)
    assert (empty.elements, empty.next_page_token) == ([], None)
    assert (null_names.elements, null_names.next_page_token) == ([], None)
    assert len(transport_server.requests) == 4
    for (method, target, headers), query in zip(transport_server.requests, [
        {'maxResults': ['1000']}, {'maxResults': ['1'], 'pageToken': [token]}, {}, {'pageToken': ['']},
    ]):
        url = urlsplit(target)
        assert (method, url.path) == ('GET', BASE_PATH + '/%2E%2E/semantic-views')
        assert parse_qs(url.query, keep_blank_values=True) == query
        assert headers['x-acs-action'] == 'ListSemanticViews'
    assert transport_server.bodies == [None] * 4


@pytest.mark.parametrize('status,exception', [
    (400, BadRequestException), (401, NotAuthorizedException), (403, ForbiddenException),
    (404, NoSuchResourceException), (500, ServiceFailureException), (501, NotImplementedException),
])
def test_semantic_view_rest_errors(catalog, transport_server, status, exception):
    transport_server.respond = lambda *_: (status, json.dumps({
        'resourceType': 'SEMANTIC_VIEW', 'resourceName': 'revenue', 'message': 'rejected by catalog',
    }))

    for call in (lambda: catalog.get_semantic_view('sales.revenue'),
                 lambda: catalog.list_semantic_views_paged('sales')):
        with pytest.raises(exception, match='rejected by catalog') as error:
            call()
        if status == 404:
            assert error.value.resource_type == 'SEMANTIC_VIEW'
            assert error.value.resource_name == 'revenue'
    assert [(method, urlsplit(target).path) for method, target, _ in transport_server.requests] == [
        ('GET', BASE_PATH + '/sales/semantic-views/revenue'),
        ('GET', BASE_PATH + '/sales/semantic-views'),
    ]


def test_invalid_semantic_view_response_is_rejected(catalog, transport_server):
    oversized = '\u534e' * (1024 * 1024 // 3 + 1)
    for response, message in (
        (None, 'response must not be null'),
        ({'name': 'revenue', 'definition': None}, 'definition must not be null'),
        ({'name': '', 'definition': {'format': 'provider-yaml', 'content': 'model'}}, 'name must not be blank'),
        ({'name': 'revenue', 'definition': {'format': None, 'content': 'model'}}, 'format must not be blank'),
        ({'name': 'revenue', 'definition': {'format': 'provider-yaml', 'content': ' '}}, 'content must not be blank'),
        ({'name': 'revenue', 'definition': {'format': 'provider-yaml', 'content': oversized}}, 'must not exceed 1 MiB'),
    ):
        transport_server.respond = lambda *_: (200, json.dumps(response))
        with pytest.raises(RESTException) as error:
            catalog.get_semantic_view('sales.revenue')
        assert message in str(error.value.get_cause() or error.value)


def test_null_semantic_view_page_is_rejected(catalog, transport_server):
    transport_server.respond = lambda *_: (200, 'null')
    with pytest.raises(RESTException, match='list response must not be null'):
        catalog.list_semantic_views_paged('sales')


def test_invalid_semantic_view_arguments_fail_before_http(catalog, transport_server):
    for page_size in (-1, 0, 1001, 2.5, True):
        with pytest.raises(ValueError, match='max_results'):
            catalog.list_semantic_views_paged('sales', max_results=page_size)
    with pytest.raises(ValueError, match='Database name'):
        catalog.list_semantic_views_paged('  ')
    for name in ('  ', '\x00\t'):
        with pytest.raises(ValueError, match='Semantic view name'):
            catalog.get_semantic_view(Identifier.create('sales', name))
    assert not transport_server.requests
