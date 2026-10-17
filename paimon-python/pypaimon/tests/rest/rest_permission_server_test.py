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
from pypaimon.catalog.catalog_exception import (IllegalArgumentError, TableNoPermissionException,
                                                TableNotExistException)
from pypaimon.common.identifier import Identifier
from pypaimon.management.column_mask import ColumnMask
from pypaimon.management.data_policy import DataPolicy
from pypaimon.management.list_permissions_request import ListPermissionsRequest
from pypaimon.management.list_policies_request import ListPoliciesRequest
from pypaimon.management.permission_assignment import PermissionAssignment
from pypaimon.management.permission_columns import PermissionColumns
from pypaimon.management.permission_resource import PermissionResource
from pypaimon.management.policy_type import PolicyType
from pypaimon.management.resource_type import ResourceType
from pypaimon.management.row_filter import RowFilter
from pypaimon.schema.data_types import AtomicType
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.tests.rest.rest_function_test import _mock_function
from pypaimon.tests.rest.rest_server import RESTCatalogServer

USERS = Identifier.create('default', 'users')
MEMBERS = Identifier.create('default', 'members')
COLUMNS = PermissionResource(ResourceType.COLUMN, 'default', 'users')
ARROW_SCHEMA = pa.schema([('id', pa.int64()), ('email', pa.string()), ('joined', pa.date32()), ('score', pa.float64()),
                          ('tags', pa.list_(pa.int32())), ('phone', pa.string())])


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


def _grants(catalog, name, resource_type=ResourceType.TABLE):
    grants = catalog.permission_management().list_permissions(
        ListPermissionsRequest(resource_type, 'default', name)).elements
    return [(grant.get_access(), grant.get_principal()) for grant in grants]


def _column_grants(catalog, name='users'):
    grants = catalog.permission_management().list_permissions(
        ListPermissionsRequest(ResourceType.COLUMN, 'default', name)).elements
    return [(grant.get_columns().get_column_names(), grant.get_columns().get_excluded_column_names())
            for grant in grants]


def test_table_and_column_grants_follow_rename_and_drop(rest):
    catalog, server = rest
    permissions = catalog.permission_management()
    permissions.grant_permission(PermissionAssignment(_table('users'), 'SELECT', 'analyst'))
    permissions.grant_permission(PermissionAssignment(
        COLUMNS, 'SELECT', 'analyst', PermissionColumns(excluded_column_names=['email'])))
    server.set_query_principals('analyst')

    catalog.rename_table('default.users', 'default.members')
    assert _grants(catalog, 'members') == [('SELECT', 'analyst')]
    assert _column_grants(catalog, 'members') == [(None, ('email',))]
    with pytest.raises(TableNoPermissionException):
        catalog.auth_table_query(MEMBERS, ['email'])
    catalog.create_table('default.users', _schema(), False)
    assert [_grants(catalog, 'users'), _column_grants(catalog)] == [[], []]
    catalog.auth_table_query(USERS, ['email'])

    catalog.drop_table('default.members')
    catalog.create_table('default.members', _schema(), False)
    assert [_grants(catalog, 'members'), _column_grants(catalog, 'members')] == [[], []]
    catalog.auth_table_query(MEMBERS, ['email'])


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


@pytest.mark.parametrize('columns', [PermissionColumns(column_names=['id']),
                                     PermissionColumns(excluded_column_names=['email', 'phone'])])
def test_column_grants_restrict_only_their_principal(rest, columns):
    catalog, server = rest
    catalog.permission_management().grant_permission(PermissionAssignment(COLUMNS, 'SELECT', 'analyst', columns))

    server.set_query_principals('auditor')
    catalog.auth_table_query(USERS, None)
    server.set_query_principals('analyst')
    catalog.auth_table_query(USERS, ['id'])
    for select in (['email'], ['id', 'phone'], None):
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
            COLUMNS, 'SELECT', 'analyst', PermissionColumns(column_names=['id', 'mobile'])))
    assert [_column_grants(catalog, 'plain'), _column_grants(catalog)] == [[], []]

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
    for change in (SchemaChange.drop_column('contact'), SchemaChange.set_option('query-auth.enabled', 'false')):
        with pytest.raises(BadRequestException):
            catalog.alter_table('default.users', [change])

    table = catalog.get_table('default.users')
    for schema in (table.table_schema, table.schema_manager.latest()):
        assert [field.name for field in schema.fields] == ['contact', 'joined', 'score', 'tags', 'phone']
        assert schema.options['query-auth.enabled'] == 'true'
    assert _column_grants(catalog) == [(('contact',), None)]


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
    assert _column_grants(catalog) == []

    catalog.drop_table('default.users')
    catalog.create_table('default.users', _schema(), False)
    catalog.permission_management().grant_permission(grant)
    with pytest.raises(IllegalArgumentError):
        catalog.create_branch(USERS, 'b1')


STARS_MASK = json.dumps({'name': 'CONCAT', 'inputs': ['***']})
LOWER_MASK = json.dumps({'name': 'LOWER', 'inputs': [{'name': 'email', 'type': 'STRING'}]})


def _row_filter(transform, column, column_type, function='EQUAL', literal=1):
    return RowFilter(json.dumps({'kind': 'LEAF', 'function': function, 'literals': [literal], 'transform': {
        'name': transform, 'fieldRef': {'name': column, 'type': column_type}}}))


def _mask(column, transform, principal='analyst', table='users'):
    return DataPolicy.column_mask(_table(table), ColumnMask(column, transform), principal)


def _policies(catalog, name):
    policies = catalog.policy_management().list_policies(ListPoliciesRequest(_table(name))).elements
    return [(policy.get_resource().get_table(), policy.get_column_mask().get_on_column()) for policy in policies]


def test_table_policies_follow_rename_and_drop(rest):
    catalog, _ = rest
    catalog.policy_management().create_policy(_mask('email', STARS_MASK))

    catalog.rename_table('default.users', 'default.members')
    assert _policies(catalog, 'members') == [('members', 'email')]
    catalog.create_table('default.users', _schema(), False)
    assert _policies(catalog, 'users') == []

    catalog.drop_table('default.members')
    catalog.create_table('default.members', _schema(), False)
    assert _policies(catalog, 'members') == []


def test_dropping_a_database_removes_only_its_tables_and_policies(rest):
    catalog, server = rest
    for database in ('sales', 'sales.eu'):
        catalog.create_database(database, False)
    tables = [PermissionResource(ResourceType.TABLE, 'sales', 'eu.users'),
              PermissionResource(ResourceType.TABLE, 'sales.eu', 'orders')]
    for table in tables:
        catalog.create_table(Identifier.create(table.get_database(), table.get_table()), _schema(), False)
        catalog.policy_management().create_policy(
            DataPolicy.column_mask(table, ColumnMask('email', STARS_MASK), 'analyst'))

    catalog.drop_database('sales', False, True)
    catalog.create_database('sales', False)
    with pytest.raises(TableNotExistException):
        catalog.get_table(Identifier.create('sales', 'eu.users'))
    assert len(catalog.policy_management().list_policies(ListPoliciesRequest(tables[1])).elements) == 1
    catalog.drop_database('sales.eu', False, True)
    assert server.policy_store == {}


@pytest.mark.parametrize('drop_database', [False, True])
def test_a_recreated_table_starts_from_its_own_schema(rest, drop_database):
    catalog, _ = rest
    catalog.alter_table('default.users', [SchemaChange.set_option('a', 'b')])
    if drop_database:
        catalog.drop_database('default', False, True)
        catalog.create_database('default', False)
    else:
        catalog.drop_table('default.users')
    catalog.create_table('default.users', Schema.from_pyarrow_schema(pa.schema([('id', pa.int64())])), False)
    catalog.alter_table('default.users', [SchemaChange.set_option('a', 'b')])
    assert [field.name for field in catalog.get_table('default.users').table_schema.fields] == ['id']


def test_policies_are_managed_without_read_access(rest):
    catalog, server = rest
    server.add_no_permission_database('default')
    server.add_no_permission_table(USERS)
    catalog.policy_management().create_policy(_mask('email', STARS_MASK))
    assert _policies(catalog, 'users') == [('users', 'email')]
    catalog.policy_management().drop_policy(_table('users'), PolicyType.COLUMN_MASKING, 'analyst', 'email', False)
    assert _policies(catalog, 'users') == []


def test_policies_need_query_auth_and_existing_columns(rest):
    catalog, _ = rest
    catalog.create_table('default.plain', _schema(query_auth=False), False)
    with pytest.raises(AlreadyExistsException):
        catalog.policy_management().create_policy(_mask('email', STARS_MASK, table='plain'))
    for policy in (_mask('mobile', STARS_MASK),
                   DataPolicy.row_filter(_table('users'), _row_filter('FIELD_REF', 'mobile', 'BIGINT'), 'analyst')):
        with pytest.raises(BadRequestException):
            catalog.policy_management().create_policy(policy)


@pytest.mark.parametrize('policy, column, other', [
    (_mask('email', STARS_MASK), 'email', 'id'),
    (DataPolicy.row_filter(_table('users'), _row_filter('FIELD_REF', 'id', 'BIGINT'), 'analyst'), 'id', 'email'),
    (DataPolicy.row_filter(_table('users'), _row_filter('YEAR', 'joined', 'DATE'), 'analyst'), 'joined', 'id'),
])
def test_schema_changes_cannot_orphan_policies(rest, policy, column, other):
    catalog, _ = rest
    catalog.policy_management().create_policy(policy)
    schema = catalog.get_table('default.users').table_schema
    column_type = next(field.type for field in schema.fields if field.name == column)
    for changes in ([SchemaChange.set_option('query-auth.enabled', 'false')],
                    [SchemaChange.remove_option('query-auth.enabled')],
                    [SchemaChange.rename_column(column, 'renamed')],
                    [SchemaChange.drop_column(column)],
                    [SchemaChange.rename_column(column, 'renamed'), SchemaChange.add_column(column, column_type)]):
        with pytest.raises(BadRequestException):
            catalog.alter_table('default.users', changes)

    current = catalog.get_table('default.users').table_schema
    assert (current.id, current.options) == (schema.id, schema.options)
    policies = catalog.policy_management().list_policies(ListPoliciesRequest(_table('users'))).elements
    assert [stored.type() for stored in policies] == [policy.type()]
    catalog.alter_table('default.users', [SchemaChange.rename_column(other, 'renamed')])


def test_policy_expressions_must_fit_the_table_schema(rest):
    catalog, _ = rest
    policies = catalog.policy_management()
    email, joined = {'name': 'email', 'type': 'STRING'}, {'name': 'joined', 'type': 'DATE'}
    masks = [('email', {'name': 'NULL'}), ('email', {'name': 'TRIM', 'inputs': [email]}),
             ('email', {'name': 'SUBSTRING', 'inputs': [123, 1]}), ('email', {'name': 'YEAR', 'fieldRef': joined}),
             ('email', {'name': 'LOWER', 'inputs': [{'name': 'id', 'type': 'BIGINT'}]}),
             ('joined', {'name': 'CAST', 'fieldRef': joined, 'type': 'DATE'}),
             ('email', {'name': 'CAST', 'fieldRef': {'name': 'id', 'type': 'BIGINT'}, 'type': 'VARCHAR(2147483647)'}),
             ('email', {'name': 'CONCAT', 'inputs': email}), ('email', {'name': 'CONCAT', 'inputs': None}),
             ('email', {'name': 'CONCAT', 'inputs': [{'name': 'email', 'type': 'INT'}, email]})]
    row_filters = [RowFilter(json.dumps(predicate)) for predicate in (
        None, {}, {'kind': 'COMPOUND', 'function': 'AND', 'children': []})] + [
        _row_filter('FIELD_REF', 'id', 'BIGINT', literal=float('nan')),
        _row_filter('YEAR', 'email', 'STRING'), _row_filter('FIELD_REF', 'id', 'BIGINT', literal='one'),
        _row_filter('FIELD_REF', 'id', 'BIGINT', function='NOT_REAL'),
        _row_filter('FIELD_REF', 'id', 'INTEGER'),
        _row_filter('FIELD_REF', 'email', 'STRING', literal=1.5),
        _row_filter('FIELD_REF', 'score', 'FLOAT', literal=1e11),
        _row_filter('FIELD_REF', 'joined', 'DATE', literal='2023-02-29'),
        RowFilter(json.dumps({'kind': 'LEAF', 'function': 'IS_NULL', 'literals': [], 'transform': {
            'name': 'CAST', 'fieldRef': {'name': 'score', 'type': 'DOUBLE'}, 'type': 'TIMESTAMP(6)'}})),
        RowFilter(json.dumps({'kind': 'LEAF', 'function': 'IS_NULL', 'literals': [], 'transform': {
            'name': 'CAST', 'fieldRef': email, 'type': {'type': 'ARRAY', 'element': {
                'type': 'VECTOR', 'element': 'FLOAT', 'length': 3}}}}))]
    for policy in ([_mask(column, json.dumps(transform)) for column, transform in masks]
                   + [DataPolicy.row_filter(_table('users'), row_filter, 'analyst') for row_filter in row_filters]):
        with pytest.raises(BadRequestException):
            policies.create_policy(policy)

    policies.create_policy(_mask('email', LOWER_MASK))
    policies.create_policy(_mask('email', json.dumps(
        {'name': 'CAST', 'fieldRef': {'name': 'id', 'type': 'BIGINT'}, 'type': 'STRING'}), 'auditor'))
    policies.create_policy(DataPolicy.row_filter(_table('users'), _row_filter('YEAR', 'joined', 'DATE'), 'analyst'))
    policies.create_policy(DataPolicy.row_filter(
        _table('users'), _row_filter('FIELD_REF', 'joined', 'DATE', literal='2024-02-29'), 'auditor'))
    policies.create_policy(DataPolicy.row_filter(_table('users'), RowFilter(json.dumps(
        {'kind': 'LEAF', 'function': 'EQUAL', 'literals': ['2024-01-01T00:00'], 'transform': {
            'name': 'CAST', 'fieldRef': {'name': 'id', 'type': 'BIGINT'}, 'type': 'TIMESTAMP(6)'}})), 'reader'))
    policies.create_policy(DataPolicy.row_filter(
        _table('users'), _row_filter('FIELD_REF', 'score', 'FLOAT', literal=1.5), 'writer'))
    for column, new_type in (('email', 'BIGINT'), ('joined', 'STRING')):
        with pytest.raises(BadRequestException):
            catalog.alter_table('default.users', [SchemaChange.update_column_type(column, AtomicType(new_type))])


def test_float_literals_block_widening_unless_exact(rest):
    catalog, _ = rest
    catalog.create_table('default.metrics', Schema.from_pyarrow_schema(
        pa.schema([('v', pa.float32())]), options={'query-auth.enabled': 'true'}), False)
    policies = catalog.policy_management()
    widen = [SchemaChange.update_column_type('v', AtomicType('DOUBLE'))]

    policies.create_policy(DataPolicy.row_filter(
        _table('metrics'), _row_filter('FIELD_REF', 'v', 'FLOAT', literal=1e11), 'analyst'))
    with pytest.raises(BadRequestException):
        catalog.alter_table('default.metrics', widen)

    policies.drop_policy(_table('metrics'), PolicyType.ROW_FILTER, 'analyst', None, False)
    policies.create_policy(DataPolicy.row_filter(
        _table('metrics'), _row_filter('FIELD_REF', 'v', 'FLOAT', literal=1.5), 'analyst'))
    catalog.alter_table('default.metrics', widen)
    assert [field.type.type for field in catalog.get_table('default.metrics').table_schema.fields] == ['DOUBLE']


def test_instants_take_a_leap_second_only_at_the_end_of_a_day(rest):
    catalog, _ = rest
    catalog.create_table('default.events', Schema.from_pyarrow_schema(
        pa.schema([('at', pa.timestamp('us', tz='UTC'))]), options={'query-auth.enabled': 'true'}), False)
    policies = catalog.policy_management()

    with pytest.raises(BadRequestException):
        policies.create_policy(DataPolicy.row_filter(_table('events'), _row_filter(
            'FIELD_REF', 'at', 'TIMESTAMP_LTZ(6)', literal='2024-01-01T12:34:60Z'), 'analyst'))
    policies.create_policy(DataPolicy.row_filter(_table('events'), _row_filter(
        'FIELD_REF', 'at', 'TIMESTAMP_LTZ(6)', literal='2016-12-31T23:59:60Z'), 'analyst'))
    stored = policies.list_policies(ListPoliciesRequest(_table('events'))).elements[0]
    assert json.loads(stored.get_row_filter().get_predicate())['literals'] == ['2016-12-31T23:59:59Z']


def test_policy_listings_are_paged(rest):
    catalog, _ = rest
    principals = ['user{:03}'.format(i) for i in range(102)]
    for principal in principals:
        catalog.policy_management().create_policy(_mask('email', STARS_MASK, principal))

    first = catalog.policy_management().list_policies(ListPoliciesRequest(_table('users'), max_results=1))
    second = catalog.policy_management().list_policies(
        ListPoliciesRequest(_table('users'), page_token=first.next_page_token, max_results=1000))
    assert [len(first.elements), len(second.elements), second.next_page_token] == [1, 101, None]
    assert [policy.get_principal() for policy in first.elements + second.elements] == principals


def test_policies_are_stored_as_java_serializes_them(rest):
    catalog, server = rest
    leaves = [{'kind': 'LEAF', 'function': 'IN', 'literals': [1.9, 2],
               'transform': {'name': 'FIELD_REF', 'fieldRef': {'name': 'id', 'type': 'BIGINT'}}},
              {'kind': 'LEAF', 'function': 'GREATER_THAN', 'literals': ['2024-02-29'],
               'transform': {'name': 'FIELD_REF', 'fieldRef': {'name': 'joined', 'type': 'DATE'}}},
              {'kind': 'LEAF', 'function': 'EQUAL', 'literals': [1e10], 'transform': {
                  'name': 'CAST', 'fieldRef': {'name': 'id', 'type': 'BIGINT'}, 'type': 'INT'}},
              {'kind': 'LEAF', 'function': 'EQUAL', 'literals': ['2024-02-29'],
               'transform': {'name': 'FIELD_REF', 'fieldRef': {'name': 'id', 'type': 'DATE'}}}]
    catalog.policy_management().create_policy(DataPolicy.row_filter(_table('users'), RowFilter(json.dumps(
        {'kind': 'COMPOUND', 'function': 'AND', 'children': leaves})), 'analyst'))
    catalog.policy_management().create_policy(_mask('email', LOWER_MASK))
    catalog.policy_management().create_policy(_mask('tags', json.dumps(
        {'name': 'CAST', 'fieldRef': {'name': 'email', 'type': 'STRING'},
         'type': {'type': 'ARRAY', 'element': 'INT'}})))

    server.set_query_principals('analyst')
    auth = catalog.auth_table_query(USERS)
    assert auth.filter == [
        '{"kind":"COMPOUND","function":"AND","children":['
        '{"kind":"LEAF","transform":{"name":"FIELD_REF","fieldRef":{"index":0,"name":"id","type":"BIGINT"}},'
        '"function":"IN","literals":[1,2]},'
        '{"kind":"LEAF","transform":{"name":"FIELD_REF","fieldRef":{"index":2,"name":"joined","type":"DATE"}},'
        '"function":"GREATER_THAN","literals":["2024-02-29"]},'
        '{"kind":"LEAF","transform":{"name":"CAST","fieldRef":{"index":0,"name":"id","type":"BIGINT"},"type":"INT"},'
        '"function":"EQUAL","literals":[2147483647]},'
        '{"kind":"LEAF","transform":{"name":"FIELD_REF","fieldRef":{"index":0,"name":"id","type":"BIGINT"}},'
        '"function":"EQUAL","literals":[19782]}]}']
    assert auth.column_masking == {
        'email': '{"name":"LOWER","inputs":[{"index":1,"name":"email","type":"STRING"}]}',
        'tags': '{"name":"CAST","fieldRef":{"index":1,"name":"email","type":"STRING"},'
                '"type":{"type":"ARRAY","element":"INT"}}'}


def test_query_auth_returns_policies_for_the_current_schema(rest):
    catalog, server = rest
    catalog.policy_management().create_policy(
        DataPolicy.row_filter(_table('users'), _row_filter('FIELD_REF', 'id', 'BIGINT'), 'analyst'))
    catalog.policy_management().create_policy(
        DataPolicy.row_filter(_table('users'), _row_filter('FIELD_REF', 'score', 'DOUBLE', literal=1.5), 'auditor'))
    catalog.alter_table('default.users', [SchemaChange.update_column_type('id', AtomicType('STRING'))])
    with pytest.raises(BadRequestException):
        catalog.alter_table('default.users', [SchemaChange.update_column_type('score', AtomicType('STRING'))])

    server.set_query_principals('analyst')
    assert catalog.auth_table_query(USERS).filter == [
        '{"kind":"LEAF","transform":{"name":"FIELD_REF","fieldRef":{"index":0,"name":"id","type":"STRING"}},'
        '"function":"EQUAL","literals":["1"]}']


def test_masks_of_two_query_principals_on_one_column_deny_the_query(rest):
    catalog, server = rest
    for principal, transform in (('analyst', STARS_MASK), ('auditor', LOWER_MASK)):
        catalog.policy_management().create_policy(_mask('email', transform, principal))

    server.set_query_principals('analyst')
    assert catalog.auth_table_query(USERS).column_masking == {'email': '{"name":"CONCAT","inputs":["***"]}'}
    server.set_query_principals('analyst', 'auditor')
    with pytest.raises(TableNoPermissionException):
        catalog.auth_table_query(USERS)


def test_tables_with_branches_cannot_have_policies(rest):
    catalog, _ = rest
    with pytest.raises(NoSuchResourceException):
        catalog.policy_management().create_policy(_mask('email', STARS_MASK, table='users$branch_missing'))
    with pytest.raises(TableNotExistException):
        catalog.auth_table_query(Identifier('default', 'users', branch='missing'))
    catalog.create_branch(USERS, 'b1')
    with pytest.raises(BadRequestException):
        catalog.policy_management().create_policy(_mask('email', STARS_MASK))

    catalog.drop_branch(USERS, 'b1')
    catalog.policy_management().create_policy(_mask('email', STARS_MASK))
    with pytest.raises(IllegalArgumentError):
        catalog.create_branch(USERS, 'b1')
