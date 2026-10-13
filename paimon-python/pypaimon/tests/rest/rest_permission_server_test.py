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

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.api.api_response import ConfigResponse
from pypaimon.api.auth import BearTokenAuthProvider
from pypaimon.api.rest_exception import (AlreadyExistsException, BadRequestException,
                                         NoSuchResourceException)
from pypaimon.catalog.catalog_exception import IllegalArgumentError, TableNoPermissionException
from pypaimon.common.identifier import Identifier
from pypaimon.management.list_permissions_request import ListPermissionsRequest
from pypaimon.management.permission_assignment import PermissionAssignment
from pypaimon.management.permission_columns import PermissionColumns
from pypaimon.management.permission_resource import PermissionResource
from pypaimon.management.resource_type import ResourceType
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.tests.rest.rest_function_test import _mock_function
from pypaimon.tests.rest.rest_server import RESTCatalogServer

USERS = Identifier.create('default', 'users')
COLUMNS = PermissionResource(ResourceType.COLUMN, 'default', 'users')
ARROW_SCHEMA = pa.schema([('id', pa.int64()), ('email', pa.string())])


def _schema(query_auth=True):
    return Schema.from_pyarrow_schema(ARROW_SCHEMA, options={'query-auth.enabled': 'true'} if query_auth else {})


@pytest.fixture
def rest(tmp_path):
    server = RESTCatalogServer(str(tmp_path), BearTokenAuthProvider('token'),
                               ConfigResponse(defaults={'prefix': 'mock'}), 'warehouse')
    server.start()
    try:
        catalog = CatalogFactory.create({
            'metastore': 'rest', 'uri': server.get_url(), 'warehouse': 'warehouse',
            'token.provider': 'bear', 'token': 'token', 'data-token.enabled': 'false'})
        catalog.create_database('default', False)
        catalog.create_table('default.users', _schema(), False)
        yield catalog, server
    finally:
        server.shutdown()


def _table(name):
    return PermissionResource(ResourceType.TABLE, 'default', name)


def _grants(catalog, name):
    grants = catalog.permission_management().list_permissions(
        ListPermissionsRequest(ResourceType.TABLE, 'default', name)).elements
    return [(grant.get_access(), grant.get_principal()) for grant in grants]


def test_table_grants_follow_rename_and_drop(rest):
    catalog, _ = rest
    catalog.permission_management().grant_permission(PermissionAssignment(_table('users'), 'SELECT', 'analyst'))

    catalog.rename_table('default.users', 'default.members')
    assert _grants(catalog, 'members') == [('SELECT', 'analyst')]
    catalog.create_table('default.users', _schema(), False)
    assert _grants(catalog, 'users') == []

    catalog.drop_table('default.members')
    catalog.create_table('default.members', _schema(), False)
    assert _grants(catalog, 'members') == []


def test_dropping_a_database_or_function_removes_its_grants(rest):
    catalog, server = rest
    area = Identifier.create('default', 'area')
    catalog.create_function(area, _mock_function(area), False)
    catalog.create_database('sales', False)
    for resource, access in ((PermissionResource(ResourceType.DATABASE, 'sales'), 'DESCRIBE'),
                             (PermissionResource(ResourceType.FUNCTION, 'default', function='area'), 'SELECT'),
                             (PermissionResource(ResourceType.DATABASE, 'default'), 'DESCRIBE')):
        catalog.permission_management().grant_permission(PermissionAssignment(resource, access, 'analyst'))

    catalog.drop_database('sales')
    catalog.drop_function(area, False)
    assert [(grant.get_resource().get_type(), grant.get_resource().get_database())
            for grant in server.permission_store.values()] == [(ResourceType.DATABASE, 'default')]


@pytest.mark.parametrize('expire_time, expired', [
    (None, False),
    ('2099-01-01T00:00:00.123000000Z', False),
    ('2099-01-01t00:00:00.5z', False),
    ('+12000-01-01T00:00:00Z', False),
    ('2020-01-01T24:00:00.000000000Z', True),
    ('-0001-01-01T00:00:00.999Z', True),
])
def test_column_grants_limit_the_selected_columns_until_they_expire(rest, expire_time, expired):
    catalog, server = rest
    catalog.permission_management().grant_permission(PermissionAssignment(
        COLUMNS, 'SELECT', 'analyst', PermissionColumns(excluded_column_names=['email']), expire_time))

    server.set_query_principals('analyst')
    catalog.auth_table_query(USERS, ['id'])
    for select in (['email'], None):
        if expired:
            catalog.auth_table_query(USERS, select)
        else:
            with pytest.raises(TableNoPermissionException):
                catalog.auth_table_query(USERS, select)


def test_column_grants_need_query_auth_and_existing_columns(rest):
    catalog, _ = rest
    catalog.create_table('default.plain', _schema(query_auth=False), False)
    with pytest.raises(AlreadyExistsException):
        catalog.permission_management().grant_permission(PermissionAssignment(
            PermissionResource(ResourceType.COLUMN, 'default', 'plain'), 'SELECT', 'analyst',
            PermissionColumns(column_names=['id'])))
    with pytest.raises(NoSuchResourceException):
        catalog.permission_management().grant_permission(PermissionAssignment(
            COLUMNS, 'SELECT', 'analyst', PermissionColumns(column_names=['phone'])))

    catalog.create_table('default.flagged', Schema.from_pyarrow_schema(
        ARROW_SCHEMA, options={'query-auth.enabled': True}), False)
    catalog.permission_management().grant_permission(PermissionAssignment(
        PermissionResource(ResourceType.COLUMN, 'default', 'flagged'), 'SELECT', 'analyst',
        PermissionColumns(column_names=['id'])))


def test_column_grants_follow_column_renames(rest):
    catalog, _ = rest
    catalog.permission_management().grant_permission(PermissionAssignment(
        COLUMNS, 'SELECT', 'analyst', PermissionColumns(column_names=['id', 'email'])))

    catalog.alter_table('default.users', [SchemaChange.rename_column('email', 'contact')])
    catalog.alter_table('default.users', [SchemaChange.drop_column('id')])
    grants = catalog.permission_management().list_permissions(
        ListPermissionsRequest(ResourceType.COLUMN, 'default', 'users')).elements
    assert [grant.get_columns().get_column_names() for grant in grants] == [('contact',)]
    for change in (SchemaChange.drop_column('contact'), SchemaChange.set_option('query-auth.enabled', 'false')):
        with pytest.raises(BadRequestException):
            catalog.alter_table('default.users', [change])


def test_permission_listings_are_paged(rest):
    catalog, _ = rest
    for principal in ('analyst', 'auditor'):
        catalog.permission_management().grant_permission(PermissionAssignment(_table('users'), 'SELECT', principal))

    first = catalog.permission_management().list_permissions(
        ListPermissionsRequest(ResourceType.TABLE, 'default', 'users', max_results=1))
    second = catalog.permission_management().list_permissions(ListPermissionsRequest(
        ResourceType.TABLE, 'default', 'users', page_token=first.next_page_token, max_results=1))
    assert [len(first.elements), len(second.elements), second.next_page_token] == [1, 1, None]
    assert [first.elements[0].get_principal(), second.elements[0].get_principal()] == ['analyst', 'auditor']


def test_tables_with_branches_cannot_have_column_grants(rest):
    catalog, _ = rest
    grant = PermissionAssignment(COLUMNS, 'SELECT', 'analyst', PermissionColumns(excluded_column_names=['email']))
    catalog.create_branch(USERS, 'b1')
    with pytest.raises(BadRequestException):
        catalog.permission_management().grant_permission(grant)

    catalog.drop_table('default.users')
    catalog.create_table('default.users', _schema(), False)
    catalog.permission_management().grant_permission(grant)
    with pytest.raises(IllegalArgumentError):
        catalog.create_branch(USERS, 'b1')
