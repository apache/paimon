"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import time

import pyarrow as pa

from pypaimon import Schema
from pypaimon.catalog.catalog_exception import (
    DatabaseNoPermissionException,
    TableNoPermissionException
)
from pypaimon.common.identifier import Identifier
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.snapshot.snapshot_commit import PartitionStatistics
from pypaimon.tests.rest.rest_base_test import RESTBaseTest


class RESTPermissionTest(RESTBaseTest):
    def setUp(self):
        super().setUp()
        self.pa_schema = pa.schema([
            ('user_id', pa.int64()),
            ('item_id', pa.int64()),
            ('behavior', pa.string()),
            ('dt', pa.string()),
        ])

    def test_get_database_no_permission(self):
        self.rest_catalog.create_database("forbidden_db", False)
        self.server.add_no_permission_database("forbidden_db")

        with self.assertRaises(DatabaseNoPermissionException) as context:
            self.rest_catalog.get_database("forbidden_db")

        self.assertEqual("forbidden_db", context.exception.database)

    def test_drop_database_no_permission(self):
        self.rest_catalog.create_database("forbidden_db_drop", False)
        self.server.add_no_permission_database("forbidden_db_drop")

        with self.assertRaises(DatabaseNoPermissionException) as context:
            self.rest_catalog.drop_database("forbidden_db_drop", False)

        self.assertEqual("forbidden_db_drop", context.exception.database)

    def test_list_tables_no_permission(self):
        self.rest_catalog.create_database("forbidden_db_list", False)
        self.server.add_no_permission_database("forbidden_db_list")

        with self.assertRaises(DatabaseNoPermissionException) as context:
            self.rest_catalog.list_tables("forbidden_db_list")

        self.assertEqual("forbidden_db_list", context.exception.database)

    def test_list_tables_paged_no_permission(self):
        self.rest_catalog.create_database("forbidden_db_list_paged", False)
        self.server.add_no_permission_database("forbidden_db_list_paged")

        with self.assertRaises(DatabaseNoPermissionException) as context:
            self.rest_catalog.list_tables_paged("forbidden_db_list_paged")

        self.assertEqual("forbidden_db_list_paged", context.exception.database)

    def test_get_table_no_permission(self):
        self.rest_catalog.create_database("test_db", True)
        schema = Schema.from_pyarrow_schema(self.pa_schema)
        self.rest_catalog.create_table("test_db.forbidden_table", schema, False)

        identifier = Identifier.create("test_db", "forbidden_table")
        self.server.add_no_permission_table(identifier)

        with self.assertRaises(TableNoPermissionException) as context:
            self.rest_catalog.get_table("test_db.forbidden_table")

        self.assertEqual("test_db.forbidden_table", context.exception.identifier.get_full_name())

    def test_drop_table_no_permission(self):
        self.rest_catalog.create_database("test_db_drop", True)
        schema = Schema.from_pyarrow_schema(self.pa_schema)
        self.rest_catalog.create_table("test_db_drop.forbidden_table", schema, False)

        identifier = Identifier.create("test_db_drop", "forbidden_table")
        self.server.add_no_permission_table(identifier)

        with self.assertRaises(TableNoPermissionException) as context:
            self.rest_catalog.drop_table("test_db_drop.forbidden_table", False)

        self.assertEqual("test_db_drop.forbidden_table", context.exception.identifier.get_full_name())

    def test_alter_table_no_permission(self):
        self.rest_catalog.create_database("test_db_alter", True)
        schema = Schema(
            fields=[
                DataField.from_dict({"id": 0, "name": "col1", "type": "STRING", "description": "field1"}),
            ],
            partition_keys=[],
            primary_keys=[],
            options={},
            comment=""
        )
        self.rest_catalog.create_table("test_db_alter.forbidden_table", schema, False)

        identifier = Identifier.create("test_db_alter", "forbidden_table")
        self.server.add_no_permission_table(identifier)

        with self.assertRaises(TableNoPermissionException) as context:
            self.rest_catalog.alter_table(
                "test_db_alter.forbidden_table",
                [SchemaChange.add_column("col2", AtomicType("INT"))],
                False
            )

        self.assertEqual("test_db_alter.forbidden_table", context.exception.identifier.get_full_name())

    def test_commit_snapshot_no_permission(self):
        self.rest_catalog.create_database("test_db_commit", True)
        schema = Schema.from_pyarrow_schema(self.pa_schema)
        self.rest_catalog.create_table("test_db_commit.forbidden_table", schema, False)

        identifier = Identifier.create("test_db_commit", "forbidden_table")
        self.server.add_no_permission_table(identifier)

        test_snapshot = Snapshot(
            version=3,
            id=1,
            schema_id=0,
            base_manifest_list="manifest-list-base",
            delta_manifest_list="manifest-list-delta",
            total_record_count=100,
            delta_record_count=100,
            commit_user="test_user",
            commit_identifier=1,
            commit_kind="APPEND",
            time_millis=int(time.time() * 1000)
        )
        test_statistics = [PartitionStatistics.create({"dt": "p1"}, 100, 1)]

        with self.assertRaises(TableNoPermissionException) as context:
            self.rest_catalog.commit_snapshot(
                identifier,
                "test-uuid",
                test_snapshot,
                test_statistics
            )

        self.assertEqual("test_db_commit.forbidden_table", context.exception.identifier.get_full_name())

    def test_permission_after_create(self):
        self.rest_catalog.create_database("test_perm_db", True)
        schema = Schema.from_pyarrow_schema(self.pa_schema)
        self.rest_catalog.create_table("test_perm_db.test_table", schema, False)

        table = self.rest_catalog.get_table("test_perm_db.test_table")
        self.assertEqual("test_perm_db.test_table", table.identifier.get_full_name())

        tables = self.rest_catalog.list_tables("test_perm_db")
        self.assertIn("test_table", tables)

        identifier = Identifier.create("test_perm_db", "test_table")
        self.server.add_no_permission_table(identifier)

        with self.assertRaises(TableNoPermissionException):
            self.rest_catalog.get_table("test_perm_db.test_table")

    def test_drop_table_ignore_not_exists_with_no_permission(self):
        self.rest_catalog.create_database("test_db_ignore", True)
        schema = Schema.from_pyarrow_schema(self.pa_schema)
        self.rest_catalog.create_table("test_db_ignore.forbidden_table", schema, False)

        identifier = Identifier.create("test_db_ignore", "forbidden_table")
        self.server.add_no_permission_table(identifier)

        with self.assertRaises(TableNoPermissionException):
            self.rest_catalog.drop_table("test_db_ignore.forbidden_table", True)

    def test_drop_database_ignore_not_exists_with_no_permission(self):
        self.rest_catalog.create_database("forbidden_db_ignore", False)
        self.server.add_no_permission_database("forbidden_db_ignore")

        with self.assertRaises(DatabaseNoPermissionException):
            self.rest_catalog.drop_database("forbidden_db_ignore", True)
