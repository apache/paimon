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

"""End-to-end tests for the ``$snapshots`` system table."""

import datetime
import os
import shutil
import tempfile
import unittest

from pypaimon import CatalogFactory, Schema
from pypaimon.common.json_util import JSON
from pypaimon.schema.data_types import DataField
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.table.system.snapshots_table import SnapshotsTable


_T0 = 1779100000000
_T1 = 1779100050000


def _write(table, snapshot):
    snapshot_dir = "{}/snapshot".format(table.table_path)
    table.file_io.mkdirs(snapshot_dir)
    table.file_io.try_to_write_atomic(
        "{}/snapshot-{}".format(snapshot_dir, snapshot.id),
        JSON.to_json(snapshot),
    )


def _seed_two_snapshots(table):
    snapshot_one = Snapshot(
        version=3,
        id=1,
        schema_id=0,
        base_manifest_list="base-1.avro",
        delta_manifest_list="delta-1.avro",
        total_record_count=10,
        delta_record_count=10,
        commit_user="alice",
        commit_identifier=100,
        commit_kind="APPEND",
        time_millis=_T0,
        watermark=7,
    )
    snapshot_two = Snapshot(
        version=3,
        id=2,
        schema_id=1,
        base_manifest_list="base-2.avro",
        delta_manifest_list="delta-2.avro",
        total_record_count=25,
        delta_record_count=15,
        commit_user="bob",
        commit_identifier=200,
        commit_kind="OVERWRITE",
        time_millis=_T1,
        changelog_manifest_list="cl-2.avro",
        changelog_record_count=3,
    )
    _write(table, snapshot_one)
    _write(table, snapshot_two)
    table.file_io.try_to_write_atomic(
        "{}/snapshot/EARLIEST".format(table.table_path), "1")
    table.file_io.try_to_write_atomic(
        "{}/snapshot/LATEST".format(table.table_path), "2")


def _read(table):
    rb = table.new_read_builder()
    return rb.new_read().to_arrow(rb.new_scan().plan().splits())


class SnapshotsTableTest(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="snapshots_sys_")
        warehouse = os.path.join(self.tmp, "warehouse")
        self.catalog = CatalogFactory.create({"warehouse": warehouse})
        self.catalog.create_database("db", False)
        fields = [DataField.from_dict({"id": 0, "name": "v", "type": "INT"})]
        self.catalog.create_table("db.t", Schema(fields=fields), False)
        self.table = self.catalog.get_table("db.t")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_snapshots_table_loaded_via_catalog(self):
        table = self.catalog.get_table("db.t$snapshots")
        self.assertIsInstance(table, SnapshotsTable)

    def test_schema_column_layout(self):
        table = self.catalog.get_table("db.t$snapshots")
        row_type = table.row_type()
        expected = [
            ("snapshot_id", False), ("schema_id", False),
            ("commit_user", False), ("commit_identifier", False),
            ("commit_kind", False), ("commit_time", False),
            ("base_manifest_list", False), ("delta_manifest_list", False),
            ("changelog_manifest_list", True),
            ("total_record_count", True), ("delta_record_count", True),
            ("changelog_record_count", True), ("watermark", True),
            ("next_row_id", True),
        ]
        self.assertEqual([n for n, _ in expected],
                         [f.name for f in row_type.fields])
        for field, (_, expected_nullable) in zip(row_type.fields, expected):
            self.assertEqual(expected_nullable, field.type.nullable,
                             "field {} nullability".format(field.name))
        self.assertEqual(["snapshot_id"], table.primary_keys())

    def test_empty_table_returns_no_rows(self):
        arrow_table = _read(self.catalog.get_table("db.t$snapshots"))
        self.assertEqual(0, arrow_table.num_rows)

    def test_lists_committed_snapshots_in_id_order(self):
        _seed_two_snapshots(self.table)

        arrow_table = _read(self.catalog.get_table("db.t$snapshots"))
        self.assertEqual([1, 2], arrow_table.column("snapshot_id").to_pylist())
        self.assertEqual([0, 1], arrow_table.column("schema_id").to_pylist())
        self.assertEqual(["alice", "bob"],
                         arrow_table.column("commit_user").to_pylist())
        self.assertEqual(["APPEND", "OVERWRITE"],
                         arrow_table.column("commit_kind").to_pylist())
        self.assertEqual([100, 200],
                         arrow_table.column("commit_identifier").to_pylist())
        self.assertEqual([10, 25],
                         arrow_table.column("total_record_count").to_pylist())
        self.assertEqual([10, 15],
                         arrow_table.column("delta_record_count").to_pylist())
        # Nullable columns: row 1 has watermark, row 2 has changelog.
        self.assertEqual([7, None],
                         arrow_table.column("watermark").to_pylist())
        self.assertEqual([None, "cl-2.avro"],
                         arrow_table.column("changelog_manifest_list").to_pylist())
        self.assertEqual([None, 3],
                         arrow_table.column("changelog_record_count").to_pylist())
        self.assertEqual([None, None],
                         arrow_table.column("next_row_id").to_pylist())

        times = arrow_table.column("commit_time").to_pylist()
        self.assertIsInstance(times[0], datetime.datetime)
        as_ms = [int(t.replace(tzinfo=datetime.timezone.utc).timestamp() * 1000)
                 for t in times]
        self.assertEqual([_T0, _T1], as_ms)


if __name__ == "__main__":
    unittest.main()
