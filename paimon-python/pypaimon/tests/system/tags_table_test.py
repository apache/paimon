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

"""End-to-end tests for the ``$tags`` system table."""

import datetime
import os
import shutil
import tempfile
import unittest

from pypaimon import CatalogFactory, Schema
from pypaimon.common.json_util import JSON
from pypaimon.schema.data_types import DataField
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.table.system.tags_table import TagsTable


_TAG_COMMIT_MS = 1779100000000


def _seed_snapshot(table):
    snapshot_dir = "{}/snapshot".format(table.table_path)
    table.file_io.mkdirs(snapshot_dir)
    snapshot = Snapshot(
        version=3,
        id=7,
        schema_id=0,
        base_manifest_list="base-7.avro",
        delta_manifest_list="delta-7.avro",
        total_record_count=42,
        delta_record_count=42,
        commit_user="seed",
        commit_identifier=7,
        commit_kind="APPEND",
        time_millis=_TAG_COMMIT_MS,
    )
    table.file_io.try_to_write_atomic(
        "{}/snapshot-7".format(snapshot_dir), JSON.to_json(snapshot))
    table.file_io.try_to_write_atomic(
        "{}/LATEST".format(snapshot_dir), "7")
    table.file_io.try_to_write_atomic(
        "{}/EARLIEST".format(snapshot_dir), "7")


def _read(table):
    rb = table.new_read_builder()
    return rb.new_read().to_arrow(rb.new_scan().plan().splits())


class TagsTableTest(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="tags_sys_")
        warehouse = os.path.join(self.tmp, "warehouse")
        self.catalog = CatalogFactory.create({"warehouse": warehouse})
        self.catalog.create_database("db", False)
        fields = [DataField.from_dict({"id": 0, "name": "v", "type": "INT"})]
        self.catalog.create_table("db.t", Schema(fields=fields), False)
        self.table = self.catalog.get_table("db.t")
        _seed_snapshot(self.table)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_tags_table_loaded_via_catalog(self):
        table = self.catalog.get_table("db.t$tags")
        self.assertIsInstance(table, TagsTable)

    def test_schema_column_layout(self):
        table = self.catalog.get_table("db.t$tags")
        row_type = table.row_type()
        self.assertEqual(
            ["tag_name", "snapshot_id", "schema_id", "commit_time",
             "record_count", "create_time", "time_retained"],
            [f.name for f in row_type.fields],
        )
        expected_nullability = [False, False, False, False, True, True, True]
        for field, expected in zip(row_type.fields, expected_nullability):
            self.assertEqual(expected, field.type.nullable,
                             "field {} nullability".format(field.name))
        self.assertEqual(["tag_name"], table.primary_keys())

    def test_empty_when_no_tags_created(self):
        arrow_table = _read(self.catalog.get_table("db.t$tags"))
        self.assertEqual(0, arrow_table.num_rows)

    def test_lists_created_tags_with_snapshot_metadata(self):
        self.table.create_tag("v1", snapshot_id=7)
        self.table.create_tag("v2", snapshot_id=7)

        arrow_table = _read(self.catalog.get_table("db.t$tags"))
        names = arrow_table.column("tag_name").to_pylist()
        self.assertEqual({"v1", "v2"}, set(names))

        snapshot_ids = arrow_table.column("snapshot_id").to_pylist()
        schema_ids = arrow_table.column("schema_id").to_pylist()
        record_counts = arrow_table.column("record_count").to_pylist()
        self.assertEqual([7, 7], snapshot_ids)
        self.assertEqual([0, 0], schema_ids)
        self.assertEqual([42, 42], record_counts)

        for ts in arrow_table.column("commit_time").to_pylist():
            self.assertIsInstance(ts, datetime.datetime)
            ms = int(ts.replace(
                tzinfo=datetime.timezone.utc).timestamp() * 1000)
            self.assertEqual(_TAG_COMMIT_MS, ms)

        # Until pypaimon Tag carries these fields they are surfaced as
        # None — same trade-off as FileSystemCatalog.get_tag.
        for value in arrow_table.column("create_time").to_pylist():
            self.assertIsNone(value)
        for value in arrow_table.column("time_retained").to_pylist():
            self.assertIsNone(value)


if __name__ == "__main__":
    unittest.main()
