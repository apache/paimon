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
from pypaimon.catalog.catalog_exception import TableNoPermissionException
from pypaimon.management.column_mask import ColumnMask
from pypaimon.management.data_policy import DataPolicy
from pypaimon.management.list_permissions_request import ListPermissionsRequest
from pypaimon.management.list_policies_request import ListPoliciesRequest
from pypaimon.management.permission_assignment import PermissionAssignment
from pypaimon.management.permission_columns import PermissionColumns
from pypaimon.management.permission_resource import PermissionResource
from pypaimon.management.policy_management import PolicyAlreadyExistException
from pypaimon.management.policy_type import PolicyType
from pypaimon.management.resource_type import ResourceType
from pypaimon.management.row_filter import RowFilter
from pypaimon.read.native_plan import native_runtime_available
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.tests.rest.rest_server import RESTCatalogServer


pytestmark = [pytest.mark.native_plan, pytest.mark.skipif(
    not native_runtime_available(), reason='pypaimon-rust not installed')]

TOKEN = 'test-token'
ANALYST = 'analyst'
ROWS = [
    {'id': 1, 'region': 'eu', 'email': 'a@example.com', 'vip': True},
    {'id': 2, 'region': 'us', 'email': 'b@example.com', 'vip': False},
    {'id': 3, 'region': 'eu', 'email': 'c@example.com', 'vip': None},
]
EU_ONLY = json.dumps({
    'kind': 'LEAF',
    'transform': {'name': 'FIELD_REF',
                  'fieldRef': {'index': 1, 'name': 'region', 'type': 'STRING'}},
    'function': 'EQUAL',
    'literals': ['eu'],
}, separators=(',', ':'))
NULL_MASK = json.dumps({'name': 'NULL'}, separators=(',', ':'))
STARS_MASK = json.dumps({'name': 'CONCAT', 'inputs': ['***']}, separators=(',', ':'))
ORDERS = PermissionResource(ResourceType.TABLE, 'default', 'orders')


@pytest.fixture
def clients(tmp_path):
    from pypaimon_rust.datafusion import SQLContext

    server = RESTCatalogServer(str(tmp_path), BearTokenAuthProvider(TOKEN),
                               ConfigResponse(defaults={'prefix': 'permission-test'}),
                               'warehouse')
    server.register_management_principal(ANALYST)
    server.start()
    try:
        options = {'metastore': 'rest', 'uri': server.get_url(), 'warehouse': 'warehouse',
                   'token.provider': 'bear', 'token': TOKEN, 'data-token.enabled': 'false'}
        catalog = CatalogFactory.create(options)
        catalog.create_database('default', True)
        context = SQLContext()
        context.register_catalog('paimon', options)
        yield catalog, context, server
    finally:
        server.shutdown()


def _create_orders(catalog, rows=()):
    schema = pa.schema([('id', pa.int64()), ('region', pa.string()), ('email', pa.string()), ('vip', pa.bool_())])
    catalog.create_table('default.orders', Schema.from_pyarrow_schema(schema), False)
    if rows:
        table = catalog.get_table('default.orders')
        builder = table.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        try:
            writer.write_arrow(pa.Table.from_pylist(list(rows), schema=schema))
            commit.commit(writer.prepare_commit())
        finally:
            writer.close()
            commit.close()
    catalog.alter_table('default.orders', [SchemaChange.set_option('query-auth.enabled', 'true')])


def _sql(context, sql):
    return [row for batch in context.sql(sql) for row in batch.to_pylist()]


def _call_ok(context, sql):
    assert _sql(context, sql) == [{'result': 'OK'}], sql


def _python_rows(catalog, columns=('id', 'region', 'email', 'vip')):
    table = catalog.get_table('default.orders')
    read_builder = table.new_read_builder().with_projection(list(columns))
    splits = read_builder.new_scan().plan().splits()
    return sorted(read_builder.new_read().to_arrow(splits).to_pylist(), key=lambda row: row['id'])


def _sql_rows(context, columns=('id', 'region', 'email', 'vip')):
    return _sql(context, 'SELECT {} FROM paimon.default.orders ORDER BY id'.format(', '.join(columns)))


def _grants(catalog, resource):
    page = catalog.permission_management().list_permissions(ListPermissionsRequest(
        resource.get_type(), resource.get_database(), resource.get_table()))
    return [(grant.get_access(), grant.get_principal(), grant.get_expire_time(), grant.get_columns())
            for grant in page.elements]


def test_sql_grants_are_seen_by_pypaimon_and_back(clients):
    catalog, context, _ = clients
    _create_orders(catalog)
    permissions = catalog.permission_management()

    _call_ok(context, "CALL sys.grant_permission(resource_type => 'TABLE', access => 'select', "
                      "principal => 'analyst', database => 'default', table => 'orders', "
                      "expire_time => '2030-01-01T00:00:00Z')")
    assert _grants(catalog, ORDERS) == [('SELECT', ANALYST, '2030-01-01T00:00:00Z', None)]

    columns = PermissionResource(ResourceType.COLUMN, 'default', 'orders')
    permissions.grant_permission(PermissionAssignment(
        columns, 'SELECT', ANALYST, PermissionColumns(excluded_column_names=['email'])))
    column_grant = {
        'resource_type': 'COLUMN', 'database': 'default', 'table': 'orders', 'function': None,
        'view': None, 'access': 'SELECT', 'principal': ANALYST, 'column_names': None,
        'excluded_column_names': 'email', 'expire_time': None, 'next_page_token': None}
    assert _sql(context, "CALL sys.list_permissions(resource_type => 'COLUMN', "
                         "database => 'default', table => 'orders')") == [column_grant]

    _call_ok(context, "CALL sys.revoke_permission(resource_type => 'TABLE', access => 'SELECT', "
                      "principal => 'analyst', database => 'default', table => 'orders')")
    permissions.revoke_permission(columns, 'SELECT', ANALYST)
    assert _grants(catalog, ORDERS) == []
    assert _sql(context, "CALL sys.list_permissions(resource_type => 'COLUMN', "
                         "database => 'default', table => 'orders')") == []


def test_column_grants_restrict_both_readers(clients):
    catalog, context, server = clients
    _create_orders(catalog, ROWS)
    catalog.permission_management().grant_permission(PermissionAssignment(
        PermissionResource(ResourceType.COLUMN, 'default', 'orders'), 'SELECT', ANALYST,
        PermissionColumns(excluded_column_names=['email']), '2099-01-01T00:00:00.123000000Z'))

    server.set_query_principals(ANALYST)
    allowed = [{'id': row['id'], 'region': row['region']} for row in ROWS]
    assert _python_rows(catalog, ('id', 'region')) == allowed
    assert _sql_rows(context, ('id', 'region')) == allowed
    with pytest.raises(TableNoPermissionException):
        _python_rows(catalog)
    with pytest.raises(Exception, match='(?i)permission'):
        _sql_rows(context)


def _policies(catalog):
    page = catalog.policy_management().list_policies(ListPoliciesRequest(ORDERS))
    return sorted((policy.type().name, policy.get_principal(),
                   policy.get_row_filter() and policy.get_row_filter().get_predicate(),
                   policy.get_column_mask() and policy.get_column_mask().get_on_column(),
                   policy.get_column_mask() and policy.get_column_mask().get_transform())
                  for policy in page.elements)


def test_sql_and_pypaimon_manage_the_same_policies(clients):
    catalog, context, _ = clients
    _create_orders(catalog)
    policies = catalog.policy_management()

    _call_ok(context, "CALL sys.create_policy(database => 'default', table => 'orders', "
                      "policy_type => 'ROW_FILTER', principal => 'analyst', "
                      "predicate_json => '{}')".format(EU_ONLY))
    policies.create_policy(DataPolicy.column_mask(ORDERS, ColumnMask('vip', NULL_MASK), ANALYST))
    assert _policies(catalog) == [
        ('COLUMN_MASKING', ANALYST, None, 'vip', NULL_MASK),
        ('ROW_FILTER', ANALYST, EU_ONLY, None, None),
    ]
    assert sorted((row['policy_type'], row['on_column'], row['predicate_json'], row['transform_json'])
                  for row in _sql(context, "CALL sys.list_policies(database => 'default', "
                                           "table => 'orders', principal => 'analyst')")) == [
        ('COLUMN_MASKING', 'vip', None, NULL_MASK),
        ('ROW_FILTER', None, EU_ONLY, None),
    ]

    with pytest.raises(PolicyAlreadyExistException):
        policies.create_policy(DataPolicy.row_filter(ORDERS, RowFilter(EU_ONLY), ANALYST))
    with pytest.raises(Exception, match='already exists'):
        context.sql("CALL sys.create_policy(database => 'default', table => 'orders', "
                    "policy_type => 'COLUMN_MASKING', principal => 'analyst', "
                    "on_column => 'vip', transform_json => '{}')".format(NULL_MASK))

    _call_ok(context, "CALL sys.drop_policy(database => 'default', table => 'orders', "
                      "policy_type => 'COLUMN_MASKING', principal => 'analyst', column => 'vip')")
    policies.drop_policy(ORDERS, PolicyType.ROW_FILTER, ANALYST, None, False)
    assert _policies(catalog) == []
    _call_ok(context, "CALL sys.drop_policy(database => 'default', table => 'orders', "
                      "policy_type => 'ROW_FILTER', principal => 'analyst', if_exists => 'true')")


@pytest.mark.skipif(RUST_VERSION < (0, 5), reason='pypaimon-rust 0.5 or later required')
def test_row_filter_restricts_both_readers_alike(clients):
    catalog, context, server = clients
    _create_orders(catalog, ROWS)
    _call_ok(context, "CALL sys.create_policy(database => 'default', table => 'orders', "
                      "policy_type => 'ROW_FILTER', principal => 'analyst', "
                      "predicate_json => '{}')".format(EU_ONLY))

    assert _python_rows(catalog) == ROWS
    assert _sql_rows(context) == ROWS

    server.set_query_principals(ANALYST)
    eu_rows = [row for row in ROWS if row['region'] == 'eu']
    assert _python_rows(catalog) == eu_rows
    assert _sql_rows(context) == eu_rows
    assert _sql(context, 'SELECT count(*) AS n FROM paimon.default.orders') == [{'n': 2}]


@pytest.mark.skipif(RUST_VERSION < (0, 5), reason='pypaimon-rust 0.5 or later required')
def test_column_masks_mask_both_readers_alike(clients):
    catalog, context, server = clients
    _create_orders(catalog, ROWS)
    policies = catalog.policy_management()
    policies.create_policy(DataPolicy.column_mask(ORDERS, ColumnMask('vip', NULL_MASK), ANALYST))
    policies.create_policy(DataPolicy.column_mask(ORDERS, ColumnMask('region', STARS_MASK), ANALYST))

    server.set_query_principals(ANALYST)
    masked = [dict(row, region='***', vip=None) for row in ROWS]
    assert _python_rows(catalog) == masked
    assert _sql_rows(context) == masked

    server.set_query_principals('someone-else')
    assert _python_rows(catalog) == ROWS
    assert _sql_rows(context) == ROWS
