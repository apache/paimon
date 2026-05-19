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

"""End-to-end tests for the ``$schemas`` system table."""

import datetime
import json
import os
import shutil
import tempfile
import unittest

from pypaimon import CatalogFactory, Schema
from pypaimon.schema.data_types import DataField
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.table.system.schemas_table import SchemasTable


def _read(table):
    rb = table.new_read_builder()
    return rb.new_read().to_arrow(rb.new_scan().plan().splits())


class SchemasTableTest(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="schemas_sys_")
        warehouse = os.path.join(self.tmp, "warehouse")
        self.catalog = CatalogFactory.create({"warehouse": warehouse})
        self.catalog.create_database("db", False)
        fields = [
            DataField.from_dict({"id": 0, "name": "id", "type": "INT"}),
            DataField.from_dict({"id": 1, "name": "name", "type": "STRING"}),
        ]
        self.catalog.create_table(
            "db.t",
            Schema(fields=fields,
                   options={"user-flag": "alpha"},
                   comment="initial table"),
            False,
        )

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_schemas_table_loaded_via_catalog(self):
        table = self.catalog.get_table("db.t$schemas")
        self.assertIsInstance(table, SchemasTable)

    def test_schema_column_layout(self):
        table = self.catalog.get_table("db.t$schemas")
        row_type = table.row_type()
        self.assertEqual(
            ["schema_id", "fields", "partition_keys", "primary_keys",
             "options", "comment", "update_time"],
            [f.name for f in row_type.fields],
        )
        expected_nullability = [False, False, False, False, False, True, False]
        for field, expected in zip(row_type.fields, expected_nullability):
            self.assertEqual(expected, field.type.nullable,
                             "field {} nullability".format(field.name))
        self.assertEqual(["schema_id"], table.primary_keys())

    def test_lists_every_committed_schema_in_id_order(self):
        # Mutate the schema a couple of times so list_all() returns more
        # than one row.
        table = self.catalog.get_table("db.t")
        table.schema_manager.commit_changes(
            [SchemaChange.set_option("user-flag", "beta")])
        table.schema_manager.commit_changes(
            [SchemaChange.set_option("user-flag", "gamma")])

        arrow_table = _read(self.catalog.get_table("db.t$schemas"))
        self.assertEqual([0, 1, 2],
                         arrow_table.column("schema_id").to_pylist())

        fields_json = arrow_table.column("fields").to_pylist()[0]
        decoded = json.loads(fields_json)
        self.assertEqual(["id", "name"], [f["name"] for f in decoded])

        options_jsons = arrow_table.column("options").to_pylist()
        self.assertEqual("alpha", json.loads(options_jsons[0])["user-flag"])
        self.assertEqual("beta", json.loads(options_jsons[1])["user-flag"])
        self.assertEqual("gamma", json.loads(options_jsons[2])["user-flag"])

        comments = arrow_table.column("comment").to_pylist()
        self.assertEqual("initial table", comments[0])

        for ts in arrow_table.column("update_time").to_pylist():
            self.assertIsInstance(ts, datetime.datetime)

    def test_empty_arrays_serialize_to_json_lists(self):
        arrow_table = _read(self.catalog.get_table("db.t$schemas"))
        self.assertEqual("[]",
                         arrow_table.column("partition_keys").to_pylist()[0])
        self.assertEqual("[]",
                         arrow_table.column("primary_keys").to_pylist()[0])


if __name__ == "__main__":
    unittest.main()
