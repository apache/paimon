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
import unittest

from pypaimon.api.api_request import (GrantPermissionRequest,
                                      RevokePermissionRequest)
from pypaimon.api.api_response import ListPermissionsResponse
from pypaimon.common.json_util import JSON
from pypaimon.management.list_permissions_request import \
    ListPermissionsRequest
from pypaimon.management.permission_access import PermissionAccess
from pypaimon.management.permission_assignment import PermissionAssignment
from pypaimon.management.permission_columns import PermissionColumns
from pypaimon.management.permission_resource import PermissionResource
from pypaimon.management.resource_type import ResourceType

ASSIGNMENT_JSON = (
    '{"resource":{"type":"TABLE","database":"sales",'
    '"table":"orders"},"access":"SELECT",'
    '"principal":"analyst",'
    '"expireTime":"2027-01-01T00:00:00Z"}')

COLUMN_ASSIGNMENT_JSON = (
    '{"resource":{"type":"COLUMN","database":"sales",'
    '"table":"orders"},"access":"SELECT",'
    '"principal":"analyst","columns":{'
    '"columnNames":["id","region"]}}')


def catalog_resource():
    return PermissionResource(ResourceType.CATALOG, None, None, None, None)


def catalog_all_resource():
    return PermissionResource(ResourceType.CATALOG_ALL, None, None, None, None)


def database_resource():
    return PermissionResource(ResourceType.DATABASE, "sales", None, None, None)


def database_all_resource():
    return PermissionResource(ResourceType.DATABASE_ALL, "sales", None, None, None)


def table_resource():
    return PermissionResource(ResourceType.TABLE, "sales", "orders", None, None)


def column_resource():
    return PermissionResource(ResourceType.COLUMN, "sales", "orders", None, None)


def function_resource():
    return PermissionResource(ResourceType.FUNCTION, "sales", None, "calculate_tax", None)


class PermissionManagementJsonTest(unittest.TestCase):

    def assert_assignment(self, assignment):
        self.assertEqual(ResourceType.TABLE, assignment.get_resource().get_type())
        self.assertEqual("sales", assignment.get_resource().get_database())
        self.assertEqual("orders", assignment.get_resource().get_table())
        self.assertEqual("SELECT", assignment.get_access())
        self.assertEqual("analyst", assignment.get_principal())
        self.assertEqual("2027-01-01T00:00:00Z", assignment.get_expire_time())

    def test_assignment_deserializes(self):
        self.assert_assignment(JSON.from_json(ASSIGNMENT_JSON, PermissionAssignment))

    def test_assignment_deserializes_lower_case_resource_type(self):
        lower_case_json = ASSIGNMENT_JSON.replace('"TABLE"', '"table"')
        self.assert_assignment(JSON.from_json(lower_case_json, PermissionAssignment))

    def test_list_response_does_not_apply_grant_validation(self):
        precise_expiry = "2027-01-01T00:00:00.123456Z"
        response_json = (
            '{"permissions":['
            + ASSIGNMENT_JSON.replace("2027-01-01T00:00:00Z", precise_expiry)
            + ']}')

        response = JSON.from_json(response_json, ListPermissionsResponse)

        self.assertEqual(precise_expiry, response.get_permissions()[0].get_expire_time())
        with self.assertRaisesRegex(ValueError, "millisecond"):
            JSON.from_json(
                ASSIGNMENT_JSON.replace("2027-01-01T00:00:00Z", precise_expiry),
                GrantPermissionRequest)

    def test_listed_expire_time_must_be_a_string(self):
        with self.assertRaisesRegex(TypeError, "expireTime must be a string"):
            JSON.from_json(
                ASSIGNMENT_JSON.replace('"2027-01-01T00:00:00Z"', "1798761600000"),
                PermissionAssignment)

    def test_column_assignment_round_trips(self):
        assignment = JSON.from_json(COLUMN_ASSIGNMENT_JSON, PermissionAssignment)

        self.assertEqual(ResourceType.COLUMN, assignment.get_resource().get_type())
        self.assertEqual("SELECT", assignment.get_access())
        self.assertEqual(("id", "region"), assignment.get_columns().get_column_names())
        self.assertIsNone(assignment.get_columns().get_excluded_column_names())

        wire = json.loads(JSON.to_json(assignment))
        self.assertEqual({"columnNames": ["id", "region"]}, wire["columns"])

    def test_grant_and_revoke_use_privilege_only_wire_shapes(self):
        assignment = JSON.from_json(ASSIGNMENT_JSON, PermissionAssignment)
        grant = json.loads(JSON.to_json(GrantPermissionRequest(assignment)))

        self.assertEqual("SELECT", grant["access"])
        for absent in ("columns", "policy", "grantOption"):
            self.assertNotIn(absent, grant)

        revoke = json.loads(JSON.to_json(RevokePermissionRequest(
            assignment.get_resource(), assignment.get_access(), assignment.get_principal())))
        self.assertEqual("SELECT", revoke["access"])
        for absent in ("columns", "policy", "policyType", "grantOption", "expireTime"):
            self.assertNotIn(absent, revoke)

    def test_permission_request_without_expiry(self):
        permission_json = (
            '{"resource":{"type":"TABLE","database":"sales",'
            '"table":"orders"},"access":"select",'
            '"principal":"analyst"}')

        self.assertIsNone(
            JSON.from_json(permission_json, GrantPermissionRequest).get_expire_time())
        self.assertEqual(
            "SELECT", JSON.from_json(permission_json, RevokePermissionRequest).get_access())

    def test_access_and_permission_validation(self):
        self.assertEqual("CREATEDATABASE", PermissionAssignment(
            catalog_resource(), "createdatabase", "analyst").get_access())
        self.assertEqual("CREATEVIEW", PermissionAssignment(
            database_resource(), "createview", "analyst").get_access())
        self.assertEqual("SELECT", PermissionAssignment(
            function_resource(), "select", "analyst").get_access())
        expected_built_ins = {
            ResourceType.CATALOG: {"ALL", "ALTER", "DROP", "GRANT", "CREATEDATABASE"},
            ResourceType.CATALOG_ALL: {
                "ALL", "DESCRIBE", "ALTER", "DROP", "GRANT", "CREATETABLE", "CREATEVIEW",
                "CREATEFUNCTION", "LIST", "SELECT", "UPDATE"},
            ResourceType.DATABASE: {
                "ALL", "DESCRIBE", "ALTER", "DROP", "GRANT", "CREATETABLE", "CREATEVIEW",
                "CREATEFUNCTION", "LIST"},
            ResourceType.DATABASE_ALL: {"ALL", "ALTER", "DROP", "SELECT", "UPDATE", "GRANT"},
            ResourceType.TABLE: {"ALL", "ALTER", "DROP", "SELECT", "UPDATE", "GRANT"},
            ResourceType.VIEW: {"ALL", "ALTER", "DROP", "SELECT", "GRANT"},
            ResourceType.FUNCTION: {"ALL", "ALTER", "DROP", "SELECT", "GRANT"},
            ResourceType.COLUMN: {"SELECT"},
        }
        for resource_type, accesses in expected_built_ins.items():
            self.assertEqual(accesses, set(PermissionAccess.built_ins(resource_type)))

        included = PermissionColumns(["id", "region"], None)
        self.assertEqual(included, PermissionAssignment(
            column_resource(), "select", "analyst", included).get_columns())

        for resource, access, message in (
                (catalog_resource(), "SELECT", "not valid for CATALOG"),
                (database_resource(), "SELECT", "not valid for DATABASE"),
                (catalog_all_resource(), "CREATEDATABASE", "not valid for CATALOG_ALL"),
                (database_all_resource(), "LIST", "not valid for DATABASE_ALL"),
                (table_resource(), "CREATEVIEW", "not valid for TABLE"),
                (function_resource(), "UPDATE", "not valid for FUNCTION")):
            with self.assertRaisesRegex(ValueError, message):
                PermissionAssignment(resource, access, "analyst")
        with self.assertRaisesRegex(ValueError, "not valid for COLUMN"):
            PermissionAssignment(column_resource(), "UPDATE", "analyst", included)
        with self.assertRaisesRegex(ValueError, "columns is required"):
            PermissionAssignment(column_resource(), "SELECT", "analyst")
        with self.assertRaisesRegex(ValueError, "only valid for COLUMN"):
            PermissionAssignment(table_resource(), "SELECT", "analyst", included)
        with self.assertRaisesRegex(ValueError, "exactly one"):
            PermissionColumns(["id"], ["region"])
        with self.assertRaisesRegex(ValueError, "cannot be empty"):
            PermissionColumns([], None)
        with self.assertRaisesRegex(ValueError, "duplicate"):
            PermissionColumns(["id", "id"], None)
        with self.assertRaisesRegex(ValueError, "empty"):
            PermissionColumns([" "], None)
        for access in ("USE_CATALOG", "CREATE_DATABASE", "USE_DATABASE", "CREATE_TABLE",
                       "CREATE_VIEW", "CREATE_FUNCTION", "INSERT", "DELETE", "EXECUTE",
                       "MANAGE_PERMISSIONS", "vendor.example/read_sensitive"):
            with self.assertRaisesRegex(ValueError, "Unknown access"):
                PermissionAssignment(table_resource(), access, "analyst")
        with self.assertRaisesRegex(ValueError, "32"):
            PermissionAssignment(table_resource(), "A" * (PermissionAccess.MAX_LENGTH + 1), "analyst")
        with self.assertRaisesRegex(ValueError, "after canonicalization"):
            PermissionAssignment(table_resource(), "a/" + "ß" * 16, "analyst")
        with self.assertRaisesRegex(ValueError, "128"):
            PermissionAssignment(
                table_resource(), "SELECT", "p" * (PermissionAssignment.MAX_PRINCIPAL_LENGTH + 1))
        with self.assertRaisesRegex(ValueError, "millisecond"):
            PermissionAssignment(
                table_resource(), "SELECT", "analyst", expire_time="2027-01-01T00:00:00.000001Z")

    def test_a_string_is_not_a_column_list(self):
        with self.assertRaisesRegex(TypeError, "not a string"):
            PermissionColumns(None, "email")

    def test_permission_columns_defensively_copies_its_range(self):
        source = ["id", "region"]
        columns = PermissionColumns(source, None)

        source.clear()
        self.assertEqual(("id", "region"), columns.get_column_names())
        with self.assertRaises(AttributeError):
            columns.get_column_names().append("email")

    def test_permission_list_requires_exact_resource_and_bounds_page_size(self):
        with self.assertRaisesRegex(ValueError, "exact target"):
            ListPermissionsRequest(
                ResourceType.TABLE, "sales", None, None, None, None, None, None, 25)
        with self.assertRaisesRegex(ValueError, "at most 1000"):
            ListPermissionsRequest(
                ResourceType.CATALOG, None, None, None, None, None, None, None, 1001)

        database_access = ListPermissionsRequest(
            ResourceType.DATABASE, "sales", None, None, None, None, "createview", None, 25)
        self.assertEqual("CREATEVIEW", database_access.get_access())
        with self.assertRaisesRegex(ValueError, "not valid for DATABASE"):
            ListPermissionsRequest(
                ResourceType.DATABASE, "sales", None, None, None, None, "SELECT", None, 25)

        self.assertEqual(" \t", database_access.with_page_token(" \t").get_page_token())

    def test_resource_canonicalizes_blank_irrelevant_locators(self):
        catalog = PermissionResource(ResourceType.CATALOG, "", " ", None, None)
        catalog_all = PermissionResource(ResourceType.CATALOG_ALL, "", " ", None, None)

        self.assertEqual(catalog_resource(), catalog)
        self.assertEqual({"type": "CATALOG"}, json.loads(JSON.to_json(catalog)))
        self.assertEqual(catalog_all_resource(), catalog_all)
        self.assertEqual({"type": "CATALOG_ALL"}, json.loads(JSON.to_json(catalog_all)))
        self.assertEqual(
            {"type": "DATABASE_ALL", "database": "sales"},
            json.loads(JSON.to_json(database_all_resource())))

    def test_blankness_follows_java_string_trim(self):
        self.assertEqual("\u00a0", PermissionAssignment(
            table_resource(), "SELECT", "\u00a0").get_principal())
        with self.assertRaisesRegex(ValueError, "principal cannot be empty"):
            PermissionAssignment(table_resource(), "SELECT", "\x00")
        with self.assertRaisesRegex(ValueError, "access cannot be empty"):
            PermissionAssignment(table_resource(), " \t", "analyst")
        with self.assertRaisesRegex(ValueError, "Unknown access ' SELECT'"):
            PermissionAssignment(table_resource(), " SELECT", "analyst")
        self.assertEqual(" orders ", PermissionResource(
            ResourceType.TABLE, "sales", " orders ", None, None).get_table())

    def test_lengths_count_utf16_code_units(self):
        emoji = "\U0001F600"
        PermissionAssignment(
            table_resource(), "SELECT", emoji * (PermissionAssignment.MAX_PRINCIPAL_LENGTH // 2))
        with self.assertRaisesRegex(ValueError, "128"):
            PermissionAssignment(
                table_resource(), "SELECT",
                emoji * (PermissionAssignment.MAX_PRINCIPAL_LENGTH // 2 + 1))
        with self.assertRaisesRegex(ValueError, "at most 32 characters\\.$"):
            PermissionAssignment(table_resource(), emoji * 17, "analyst")

    def test_expire_time_follows_instant_parse(self):
        for accepted in ("2027-01-01T00:00:00Z", "2027-01-01T00:00:00.123Z",
                         "2027-01-01T00:00:00.123000000Z", "2027-01-01t00:00:00z",
                         "2027-01-01T24:00:00Z", "2016-12-31T23:59:60Z",
                         "0000-01-01T00:00:00Z", "9999-12-31T24:00:00Z", "+10000-01-01T00:00:00Z",
                         "-0001-01-01T00:00:00Z", "+1000000000-12-31T23:59:59Z",
                         "-1000000000-01-01T00:00:00Z", "2000-02-29T00:00:00Z"):
            PermissionAssignment(table_resource(), "SELECT", "analyst", expire_time=accepted)
        for rejected in ("tomorrow", "2027-01-01T00:00:00+08:00", "2027-01-01T00:00Z",
                         "2027-02-30T00:00:00Z", "2027-01-01 00:00:00Z",
                         "2027-01-01T00:00:00.1234567890Z",
                         "\uff12\uff10\uff12\uff17-01-01T00:00:00Z",
                         "10000-01-01T00:00:00Z", "+2027-01-01T00:00:00Z", "-0000-01-01T00:00:00Z",
                         "+1000000001-01-01T00:00:00Z", "-1000000001-12-31T23:59:59Z",
                         "1900-02-29T00:00:00Z", "+1000000000-12-31T24:00:00Z"):
            with self.assertRaisesRegex(ValueError, "ISO-8601 UTC instant"):
                PermissionAssignment(table_resource(), "SELECT", "analyst", expire_time=rejected)
        with self.assertRaisesRegex(ValueError, "millisecond"):
            PermissionAssignment(
                table_resource(), "SELECT", "analyst", expire_time="2027-01-01T00:00:00.1234Z")


if __name__ == '__main__':
    unittest.main()
