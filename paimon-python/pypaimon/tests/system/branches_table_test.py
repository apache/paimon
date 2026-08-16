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

"""End-to-end tests for the ``$branches`` system table."""

import datetime
import os
import shutil
import tempfile
import time
import unittest

from pypaimon import CatalogFactory, Schema
from pypaimon.common.json_util import JSON
from pypaimon.schema.data_types import DataField
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.table.system.branches_table import BranchesTable


def _seed_snapshot(table):
    snapshot_dir = "{}/snapshot".format(table.table_path)
    table.file_io.mkdirs(snapshot_dir)
    snapshot = Snapshot(
        version=3,
        id=1,
        schema_id=0,
        base_manifest_list="base.avro",
        delta_manifest_list="delta.avro",
        total_record_count=0,
        delta_record_count=0,
        commit_user="seed",
        commit_identifier=1,
        commit_kind="APPEND",
        time_millis=int(time.time() * 1000),
    )
    table.file_io.try_to_write_atomic(
        "{}/snapshot-1".format(snapshot_dir), JSON.to_json(snapshot))
    table.file_io.try_to_write_atomic(
        "{}/LATEST".format(snapshot_dir), "1")
    table.file_io.try_to_write_atomic(
        "{}/EARLIEST".format(snapshot_dir), "1")


def _read(table):
    rb = table.new_read_builder()
    return rb.new_read().to_arrow(rb.new_scan().plan().splits())


class BranchesTableTest(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="branches_sys_")
        warehouse = os.path.join(self.tmp, "warehouse")
        self.catalog = CatalogFactory.create({"warehouse": warehouse})
        self.catalog.create_database("db", False)
        fields = [DataField.from_dict({"id": 0, "name": "v", "type": "INT"})]
        self.catalog.create_table("db.t", Schema(fields=fields), False)
        self.table = self.catalog.get_table("db.t")
        _seed_snapshot(self.table)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_branches_table_loaded_via_catalog(self):
        table = self.catalog.get_table("db.t$branches")
        self.assertIsInstance(table, BranchesTable)

    def test_schema_column_layout(self):
        table = self.catalog.get_table("db.t$branches")
        row_type = table.row_type()
        self.assertEqual(["branch_name", "create_time"],
                         [f.name for f in row_type.fields])
        for field in row_type.fields:
            self.assertFalse(field.type.nullable,
                             "{} should be NOT NULL".format(field.name))
        self.assertEqual(["branch_name"], table.primary_keys())

    def test_empty_when_no_branches_exist(self):
        arrow_table = _read(self.catalog.get_table("db.t$branches"))
        self.assertEqual(0, arrow_table.num_rows)
        self.assertEqual(["branch_name", "create_time"],
                         arrow_table.schema.names)

    def test_lists_every_branch_with_create_time(self):
        before = int(time.time() * 1000)
        self.table.branch_manager().create_branch("dev")
        self.table.branch_manager().create_branch("staging")
        after = int(time.time() * 1000)

        arrow_table = _read(self.catalog.get_table("db.t$branches"))
        names = arrow_table.column("branch_name").to_pylist()
        self.assertEqual({"dev", "staging"}, set(names))

        for ts in arrow_table.column("create_time").to_pylist():
            # PyArrow timestamp values surface as naive datetimes that
            # encode milliseconds since the UTC epoch; reattach the
            # timezone before converting back to compare with the
            # int(time.time() * 1000) bracket.
            self.assertIsInstance(ts, datetime.datetime)
            ms = int(ts.replace(
                tzinfo=datetime.timezone.utc).timestamp() * 1000)
            self.assertGreaterEqual(ms, before - 5000)
            self.assertLessEqual(ms, after + 5000)


if __name__ == "__main__":
    unittest.main()
