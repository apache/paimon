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

"""End-to-end tests for the ``$options`` system table."""

import os
import shutil
import tempfile
import unittest

from pypaimon import CatalogFactory, Schema
from pypaimon.schema.data_types import DataField
from pypaimon.table.system.options_table import OptionsTable


def _read(table):
    rb = table.new_read_builder()
    return rb.new_read().to_arrow(rb.new_scan().plan().splits())


class OptionsTableTest(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="opts_sys_")
        warehouse = os.path.join(self.tmp, "warehouse")
        self.catalog = CatalogFactory.create({"warehouse": warehouse})
        self.catalog.create_database("db", False)
        fields = [DataField.from_dict({"id": 0, "name": "v", "type": "INT"})]
        self.catalog.create_table(
            "db.t",
            Schema(fields=fields, options={
                "bucket": "2",
                "user-flag": "alpha",
            }),
            False,
        )

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_options_table_is_loaded_via_catalog(self):
        table = self.catalog.get_table("db.t$options")
        self.assertIsInstance(table, OptionsTable)

    def test_schema_column_layout(self):
        table = self.catalog.get_table("db.t$options")
        row_type = table.row_type()
        self.assertEqual(["key", "value"], [f.name for f in row_type.fields])
        for field in row_type.fields:
            self.assertFalse(field.type.nullable,
                             "{} should be NOT NULL".format(field.name))
        self.assertEqual(["key"], table.primary_keys())

    def test_read_returns_every_committed_option(self):
        table = self.catalog.get_table("db.t$options")
        arrow_table = _read(table)
        self.assertEqual(["key", "value"], arrow_table.schema.names)
        rendered = dict(zip(
            arrow_table.column("key").to_pylist(),
            arrow_table.column("value").to_pylist(),
        ))
        self.assertEqual("2", rendered["bucket"])
        self.assertEqual("alpha", rendered["user-flag"])

    def test_read_with_projection_keeps_requested_column_only(self):
        table = self.catalog.get_table("db.t$options")
        rb = table.new_read_builder().with_projection(["key"])
        arrow_table = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        self.assertEqual(["key"], arrow_table.schema.names)
        self.assertIn("bucket", arrow_table.column("key").to_pylist())


if __name__ == "__main__":
    unittest.main()
