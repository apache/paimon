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

from pypaimon.api.api_request import DropPolicyRequest, PolicyRequest
from pypaimon.api.api_response import ListPoliciesResponse
from pypaimon.common.json_util import JSON
from pypaimon.management.column_mask import ColumnMask
from pypaimon.management.data_policy import DataPolicy
from pypaimon.management.list_policies_request import ListPoliciesRequest
from pypaimon.management.permission_resource import PermissionResource
from pypaimon.management.policy_type import PolicyType
from pypaimon.management.resource_type import ResourceType
from pypaimon.management.row_filter import RowFilter

PREDICATE_JSON = (
    '{"kind":"LEAF","transform":{"name":"FIELD_REF",'
    '"fieldRef":{"index":0,"name":"region","type":"STRING"}},'
    '"function":"EQUAL","literals":["APAC"]}')
TRANSFORM_JSON = (
    '{"name":"CONCAT","inputs":[{"index":0,"name":"region",'
    '"type":"STRING"},"****"]}')


def catalog_resource():
    return PermissionResource(ResourceType.CATALOG, None, None, None, None)


def table_resource():
    return PermissionResource(ResourceType.TABLE, "sales", "orders", None, None)


class PolicyManagementJsonTest(unittest.TestCase):

    def test_policy_definitions_round_trip(self):
        policy = DataPolicy.column_mask(
            table_resource(), ColumnMask("email", TRANSFORM_JSON), "analyst")

        round_trip = JSON.from_json(JSON.to_json(policy), DataPolicy)
        self.assertEqual(PolicyType.COLUMN_MASKING, round_trip.type())
        self.assertEqual(table_resource(), round_trip.get_resource())
        self.assertEqual("email", round_trip.get_column_mask().get_on_column())
        self.assertEqual(TRANSFORM_JSON, round_trip.get_column_mask().get_transform())
        self.assertEqual("analyst", round_trip.get_principal())

        request = PolicyRequest.from_policy(policy)
        wire = json.loads(JSON.to_json(request))
        self.assertEqual(2, len(wire))
        self.assertIsNotNone(wire["columnMask"])
        self.assertEqual("analyst", wire["principal"])
        self.assertEqual(table_resource(), request.policy(table_resource()).get_resource())

    def test_row_filter_round_trip(self):
        policy = DataPolicy.row_filter(table_resource(), RowFilter(PREDICATE_JSON), "analyst")

        round_trip = JSON.from_json(JSON.to_json(policy), DataPolicy)
        self.assertEqual(PolicyType.ROW_FILTER, round_trip.type())
        self.assertEqual(PREDICATE_JSON, round_trip.get_row_filter().get_predicate())
        self.assertIsNone(round_trip.get_column_mask())

    def test_policy_validation_and_payload_bounds(self):
        with self.assertRaisesRegex(ValueError, "predicate"):
            RowFilter(" ")
        with self.assertRaisesRegex(ValueError, "transform"):
            ColumnMask("email", " ")
        with self.assertRaisesRegex(ValueError, "exactly one"):
            DataPolicy(table_resource(), RowFilter(PREDICATE_JSON),
                       ColumnMask("email", TRANSFORM_JSON), "analyst")
        with self.assertRaisesRegex(ValueError, "TABLE"):
            DataPolicy.column_mask(catalog_resource(), ColumnMask("email", TRANSFORM_JSON), "analyst")
        with self.assertRaisesRegex(ValueError, "UTF-8 bytes"):
            RowFilter("p" * (RowFilter.MAX_PREDICATE_BYTES + 1))
        with self.assertRaisesRegex(ValueError, "UTF-8 bytes"):
            ColumnMask("email", "t" * (ColumnMask.MAX_TRANSFORM_BYTES + 1))

    def test_drop_policy_request_round_trip_and_identity_validation(self):
        request = DropPolicyRequest(PolicyType.COLUMN_MASKING, "analyst", "email")
        round_trip = JSON.from_json(JSON.to_json(request), DropPolicyRequest)

        self.assertEqual(PolicyType.COLUMN_MASKING, round_trip.get_type())
        self.assertEqual("analyst", round_trip.get_principal())
        self.assertEqual("email", round_trip.get_column())
        with self.assertRaisesRegex(ValueError, "column is required"):
            DropPolicyRequest(PolicyType.COLUMN_MASKING, "analyst", None)
        with self.assertRaisesRegex(ValueError, "cannot contain a column"):
            DropPolicyRequest(PolicyType.ROW_FILTER, "analyst", "email")

    def test_list_policies_validation_and_opaque_page_token(self):
        request = ListPoliciesRequest(
            table_resource(), PolicyType.COLUMN_MASKING, "analyst", "email", None, 25)

        self.assertEqual(" \t", request.with_page_token(" \t").get_page_token())
        with self.assertRaisesRegex(ValueError, "COLUMN_MASKING"):
            ListPoliciesRequest(table_resource(), None, None, "email", None, 25)
        with self.assertRaisesRegex(ValueError, "at most 1000"):
            ListPoliciesRequest(table_resource(), None, None, None, None, 1001)
        with self.assertRaisesRegex(ValueError, "TABLE"):
            ListPoliciesRequest(catalog_resource(), None, None, None, None, 25)

    def test_payload_bounds_count_utf8_bytes(self):
        RowFilter("\u20ac" * (RowFilter.MAX_PREDICATE_BYTES // 3))
        with self.assertRaisesRegex(ValueError, "UTF-8 bytes"):
            RowFilter("\u20ac" * (RowFilter.MAX_PREDICATE_BYTES // 3 + 1))
        with self.assertRaisesRegex(ValueError, "UTF-8 bytes"):
            ColumnMask("email", "\U0001F600" * (ColumnMask.MAX_TRANSFORM_BYTES // 4 + 1))
        RowFilter("\ud800" * RowFilter.MAX_PREDICATE_BYTES)

    def test_drop_identity_column_follows_java_string_trim(self):
        self.assertEqual(
            "\u00a0", DropPolicyRequest(PolicyType.COLUMN_MASKING, "analyst", "\u00a0").get_column())
        with self.assertRaisesRegex(ValueError, "cannot contain a column"):
            DropPolicyRequest(PolicyType.ROW_FILTER, "analyst", "\u00a0")
        with self.assertRaisesRegex(ValueError, "column is required"):
            DropPolicyRequest(PolicyType.COLUMN_MASKING, "analyst", "\x00")
        self.assertIsNone(DropPolicyRequest(PolicyType.ROW_FILTER, "analyst", "\x00").get_column())

    def test_drop_policy_request_type_is_case_sensitive(self):
        with self.assertRaisesRegex(ValueError, "column_masking"):
            JSON.from_json(
                '{"type":"column_masking","principal":"analyst","column":"email"}',
                DropPolicyRequest)
        with self.assertRaisesRegex(ValueError, "policy type cannot be null"):
            JSON.from_json('{"principal":"analyst"}', DropPolicyRequest)

    def test_listed_policies_are_validated(self):
        both = json.dumps({"policies": [{
            "resource": {"type": "TABLE", "database": "sales", "table": "orders"},
            "rowFilter": {"predicate": PREDICATE_JSON},
            "columnMask": {"onColumn": "email", "transform": TRANSFORM_JSON},
            "principal": "analyst"}]})
        with self.assertRaisesRegex(ValueError, "exactly one"):
            JSON.from_json(both, ListPoliciesResponse)


if __name__ == '__main__':
    unittest.main()
