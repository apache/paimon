#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

"""End-to-end tests for Paimon system tables via pypaimon SQL.

Exercises the `<table>$<system_name>` syntax handled by paimon-rust
DataFusion integration. A non-partitioned table with one snapshot is
created in setUpClass and queried by each test.
"""

import os
import tempfile
import unittest

import pyarrow as pa


WAREHOUSE = os.environ.get("PAIMON_TEST_WAREHOUSE")
TABLE_NAME = "sql_system_test_table"
TABLE_FQN = f"default.{TABLE_NAME}"
ROW_COUNT = 3


class SQLSystemTableTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        from pypaimon import CatalogFactory, Schema
        from pypaimon.schema.data_types import AtomicType, DataField
        from pypaimon.sql.sql_context import SQLContext

        cls._tmpdir = None
        if WAREHOUSE:
            cls.warehouse = WAREHOUSE
        else:
            cls._tmpdir = tempfile.TemporaryDirectory(prefix="paimon-sql-systest-")
            cls.warehouse = cls._tmpdir.name

        catalog = CatalogFactory.create({"warehouse": cls.warehouse})
        catalog.create_database("default", ignore_if_exists=True)
        catalog.drop_table(TABLE_FQN, ignore_if_not_exists=True)
        schema = Schema(
            fields=[
                DataField(0, "id", AtomicType("INT")),
                DataField(1, "name", AtomicType("STRING")),
            ],
            primary_keys=[],
            partition_keys=[],
            options={},
            comment="",
        )
        catalog.create_table(TABLE_FQN, schema, ignore_if_exists=False)

        table = catalog.get_table(TABLE_FQN)
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        try:
            pa_table = pa.table({
                "id": pa.array([1, 2, 3], type=pa.int32()),
                "name": pa.array(["alice", "bob", "carol"], type=pa.string()),
            })
            table_write.write_arrow(pa_table)
            table_commit.commit(table_write.prepare_commit())
        finally:
            table_write.close()
            table_commit.close()

        cls.ctx = SQLContext()
        cls.ctx.register_catalog("paimon", {"warehouse": cls.warehouse})
        cls.ctx.set_current_catalog("paimon")
        cls.ctx.set_current_database("default")

    @classmethod
    def tearDownClass(cls):
        from pypaimon import CatalogFactory
        catalog = CatalogFactory.create({"warehouse": cls.warehouse})
        catalog.drop_table(TABLE_FQN, ignore_if_not_exists=True)
        if cls._tmpdir is not None:
            cls._tmpdir.cleanup()

    def _query(self, system_name: str) -> pa.Table:
        return self.ctx.sql(f"SELECT * FROM {TABLE_NAME}${system_name}")

    def test_options_system_table(self):
        table = self._query("options")
        self.assertListEqual(table.schema.names, ["key", "value"])

    def test_schemas_system_table(self):
        table = self._query("schemas")
        self.assertListEqual(
            table.schema.names,
            ["schema_id", "fields", "partition_keys", "primary_keys",
             "options", "comment", "update_time"],
        )
        self.assertGreaterEqual(table.num_rows, 1, "should have at least one schema")
        ids = table.column("schema_id").to_pylist()
        self.assertEqual(sorted(ids), sorted(set(ids)), "schema_id should be unique")
        fields_json = table.column("fields").to_pylist()[0]
        self.assertIn("id", fields_json)
        self.assertIn("name", fields_json)

    def test_snapshots_system_table(self):
        table = self._query("snapshots")
        names = table.schema.names
        for required in (
            "snapshot_id", "schema_id", "commit_user", "commit_identifier",
            "commit_kind", "commit_time", "base_manifest_list",
            "delta_manifest_list", "total_record_count",
        ):
            self.assertIn(required, names)
        self.assertEqual(table.num_rows, 1, "single batch write should produce one snapshot")
        self.assertEqual(table.column("total_record_count").to_pylist()[0], ROW_COUNT)

    def test_tags_system_table_empty(self):
        table = self._query("tags")
        self.assertListEqual(
            table.schema.names,
            ["tag_name", "snapshot_id", "schema_id", "commit_time",
             "record_count", "create_time", "time_retained"],
        )
        self.assertEqual(table.num_rows, 0, "no tags created")

    def test_branches_system_table_empty(self):
        table = self._query("branches")
        self.assertListEqual(table.schema.names, ["branch_name", "create_time"])
        self.assertEqual(table.num_rows, 0, "implicit main branch is not listed")

    def test_manifests_system_table(self):
        table = self._query("manifests")
        for required in (
            "file_name", "file_size", "num_added_files",
            "num_deleted_files", "schema_id",
        ):
            self.assertIn(required, table.schema.names)
        self.assertGreaterEqual(table.num_rows, 1, "snapshot should have manifests")
        for size in table.column("file_size").to_pylist():
            self.assertGreater(size, 0)
        total_added = sum(table.column("num_added_files").to_pylist())
        self.assertGreaterEqual(total_added, 1, "single write should add at least one file")


if __name__ == "__main__":
    unittest.main()
