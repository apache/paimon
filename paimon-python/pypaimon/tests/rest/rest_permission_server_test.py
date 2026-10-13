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

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.api.api_response import ConfigResponse
from pypaimon.api.auth import BearTokenAuthProvider
from pypaimon.api.rest_exception import (AlreadyExistsException, BadRequestException,
                                         NoSuchResourceException)
from pypaimon.catalog.catalog_exception import TableNoPermissionException
from pypaimon.common.identifier import Identifier
from pypaimon.management.column_mask import ColumnMask
from pypaimon.management.data_policy import DataPolicy
from pypaimon.management.list_permissions_request import ListPermissionsRequest
from pypaimon.management.list_policies_request import ListPoliciesRequest
from pypaimon.management.permission_assignment import PermissionAssignment
from pypaimon.management.permission_columns import PermissionColumns
from pypaimon.management.permission_resource import PermissionResource
from pypaimon.management.resource_type import ResourceType
from pypaimon.management.row_filter import RowFilter
from pypaimon.schema.data_types import AtomicType
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.tests.rest.rest_function_test import _mock_function
from pypaimon.tests.rest.rest_server import RESTCatalogServer

NULL_MASK = json.dumps({'name': 'NULL'})
STARS_MASK = json.dumps({'name': 'CONCAT', 'inputs': ['***']})
LOWER_MASK = json.dumps({'name': 'LOWER', 'inputs': [{'name': 'email', 'type': 'STRING'}]})
USERS = Identifier.create('default', 'users')
COLUMNS = PermissionResource(ResourceType.COLUMN, 'default', 'users')


def _schema(query_auth=True):
    return Schema.from_pyarrow_schema(pa.schema([('id', pa.int64()), ('email', pa.string()), ('joined', pa.date32())]),
                                      options={'query-auth.enabled': 'true'} if query_auth else {})


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


def _equals_one(transform, column, column_type):
    return RowFilter(json.dumps({'kind': 'LEAF', 'function': 'EQUAL', 'literals': [1], 'transform': {
        'name': transform, 'fieldRef': {'name': column, 'type': column_type}}}))


def _restrict(catalog, name):
    catalog.permission_management().grant_permission(PermissionAssignment(_table(name), 'SELECT', 'analyst'))
    catalog.policy_management().create_policy(
        DataPolicy.column_mask(_table(name), ColumnMask('email', STARS_MASK), 'analyst'))


def _access(catalog, name):
    grants = catalog.permission_management().list_permissions(
        ListPermissionsRequest(ResourceType.TABLE, 'default', name)).elements
    policies = catalog.policy_management().list_policies(ListPoliciesRequest(_table(name))).elements
    return ([(grant.get_access(), grant.get_principal()) for grant in grants],
            [(policy.get_resource().get_table(), policy.get_column_mask().get_on_column())
             for policy in policies])


def test_table_grants_and_policies_follow_rename_and_drop(rest):
    catalog, _ = rest
    _restrict(catalog, 'users')

    catalog.rename_table('default.users', 'default.members')
    assert _access(catalog, 'members') == ([('SELECT', 'analyst')], [('members', 'email')])
    catalog.create_table('default.users', _schema(), False)
    assert _access(catalog, 'users') == ([], [])

    catalog.drop_table('default.members')
    catalog.create_table('default.members', _schema(), False)
    assert _access(catalog, 'members') == ([], [])


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


def test_column_grants_and_policies_need_query_auth_and_existing_columns(rest):
    catalog, _ = rest
    catalog.create_table('default.plain', _schema(query_auth=False), False)
    with pytest.raises(AlreadyExistsException):
        catalog.permission_management().grant_permission(PermissionAssignment(
            PermissionResource(ResourceType.COLUMN, 'default', 'plain'), 'SELECT', 'analyst',
            PermissionColumns(column_names=['id'])))
    with pytest.raises(NoSuchResourceException):
        catalog.permission_management().grant_permission(PermissionAssignment(
            COLUMNS, 'SELECT', 'analyst', PermissionColumns(column_names=['phone'])))
    for policy in (DataPolicy.column_mask(_table('users'), ColumnMask('phone', STARS_MASK), 'analyst'),
                   DataPolicy.row_filter(_table('users'), _equals_one('FIELD_REF', 'phone', 'BIGINT'), 'analyst')):
        with pytest.raises(BadRequestException):
            catalog.policy_management().create_policy(policy)


@pytest.mark.parametrize('policy, column, other', [
    (DataPolicy.column_mask(_table('users'), ColumnMask('email', STARS_MASK), 'analyst'), 'email', 'id'),
    (DataPolicy.row_filter(_table('users'), _equals_one('FIELD_REF', 'id', 'BIGINT'), 'analyst'), 'id', 'email'),
    (DataPolicy.row_filter(_table('users'), _equals_one('YEAR', 'joined', 'DATE'), 'analyst'), 'joined', 'id'),
])
def test_schema_changes_cannot_orphan_policies(rest, policy, column, other):
    catalog, _ = rest
    catalog.policy_management().create_policy(policy)
    schema = catalog.get_table('default.users').table_schema
    for change in (SchemaChange.set_option('query-auth.enabled', 'false'),
                   SchemaChange.remove_option('query-auth.enabled'),
                   SchemaChange.rename_column(column, 'renamed'),
                   SchemaChange.drop_column(column)):
        with pytest.raises(BadRequestException):
            catalog.alter_table('default.users', [change])

    current = catalog.get_table('default.users').table_schema
    assert (current.id, current.options) == (schema.id, schema.options)
    policies = catalog.policy_management().list_policies(ListPoliciesRequest(_table('users'))).elements
    assert [stored.type() for stored in policies] == [policy.type()]
    catalog.alter_table('default.users', [SchemaChange.rename_column(other, 'renamed')])


def test_policy_expressions_must_fit_the_table_schema(rest):
    catalog, _ = rest
    policies = catalog.policy_management()
    for policy in (
            DataPolicy.row_filter(_table('users'), RowFilter(json.dumps(
                {'kind': 'COMPOUND', 'function': 'AND', 'children': []})), 'analyst'),
            DataPolicy.column_mask(_table('users'), ColumnMask('email', json.dumps(
                {'name': 'TRIM', 'inputs': [{'name': 'email', 'type': 'STRING'}]})), 'analyst'),
            DataPolicy.column_mask(_table('users'), ColumnMask('email', NULL_MASK), 'analyst'),
            DataPolicy.column_mask(_table('users'), ColumnMask('email', json.dumps(
                {'name': 'YEAR', 'fieldRef': {'name': 'joined', 'type': 'DATE'}})), 'analyst'),
            DataPolicy.column_mask(_table('users'), ColumnMask('email', json.dumps(
                {'name': 'LOWER', 'inputs': [{'name': 'id', 'type': 'BIGINT'}]})), 'analyst'),
            DataPolicy.row_filter(_table('users'), _equals_one('YEAR', 'email', 'STRING'), 'analyst')):
        with pytest.raises(BadRequestException):
            policies.create_policy(policy)

    policies.create_policy(DataPolicy.column_mask(_table('users'), ColumnMask('email', LOWER_MASK), 'analyst'))
    policies.create_policy(DataPolicy.column_mask(_table('users'), ColumnMask('email', json.dumps(
        {'name': 'CAST', 'fieldRef': {'name': 'id', 'type': 'BIGINT'}, 'type': 'VARCHAR(2147483647)'})), 'auditor'))
    policies.create_policy(DataPolicy.row_filter(_table('users'), _equals_one('YEAR', 'joined', 'DATE'), 'analyst'))
    for column, new_type in (('email', 'BIGINT'), ('joined', 'STRING')):
        with pytest.raises(BadRequestException):
            catalog.alter_table('default.users', [SchemaChange.update_column_type(column, AtomicType(new_type))])


def test_permission_and_policy_listings_are_paged(rest):
    catalog, _ = rest
    for principal in ('analyst', 'auditor'):
        catalog.permission_management().grant_permission(PermissionAssignment(_table('users'), 'SELECT', principal))
        catalog.policy_management().create_policy(
            DataPolicy.column_mask(_table('users'), ColumnMask('email', STARS_MASK), principal))

    for list_page in (
            lambda token: catalog.permission_management().list_permissions(ListPermissionsRequest(
                ResourceType.TABLE, 'default', 'users', page_token=token, max_results=1)),
            lambda token: catalog.policy_management().list_policies(ListPoliciesRequest(
                _table('users'), page_token=token, max_results=1))):
        first = list_page(None)
        second = list_page(first.next_page_token)
        assert [len(first.elements), len(second.elements), second.next_page_token] == [1, 1, None]
        assert [first.elements[0].get_principal(), second.elements[0].get_principal()] == ['analyst', 'auditor']


def test_column_grants_follow_column_renames(rest):
    catalog, _ = rest
    catalog.permission_management().grant_permission(PermissionAssignment(
        COLUMNS, 'SELECT', 'analyst', PermissionColumns(column_names=['id', 'email'])))

    catalog.alter_table('default.users', [SchemaChange.rename_column('email', 'contact')])
    catalog.alter_table('default.users', [SchemaChange.drop_column('id')])
    grants = catalog.permission_management().list_permissions(
        ListPermissionsRequest(ResourceType.COLUMN, 'default', 'users')).elements
    assert [grant.get_columns().get_column_names() for grant in grants] == [('contact',)]
    with pytest.raises(BadRequestException):
        catalog.alter_table('default.users', [SchemaChange.drop_column('contact')])


def test_masks_of_two_query_principals_on_one_column_deny_the_query(rest):
    catalog, server = rest
    for principal, transform in (('analyst', STARS_MASK), ('auditor', LOWER_MASK)):
        catalog.policy_management().create_policy(
            DataPolicy.column_mask(_table('users'), ColumnMask('email', transform), principal))

    server.set_query_principals('analyst')
    assert catalog.auth_table_query(USERS).column_masking == {'email': STARS_MASK}
    server.set_query_principals('analyst', 'auditor')
    with pytest.raises(TableNoPermissionException):
        catalog.auth_table_query(USERS)
