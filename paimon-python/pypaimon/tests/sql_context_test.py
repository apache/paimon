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

import os
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory


WAREHOUSE = os.environ.get("PAIMON_TEST_WAREHOUSE", "/tmp/paimon-warehouse")


class SQLContextTest(unittest.TestCase):

    _table_created = False

    def _create_catalog(self):
        return CatalogFactory.create({"warehouse": WAREHOUSE})

    def _create_sql_context(self):
        from pypaimon.sql.sql_context import SQLContext
        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": WAREHOUSE})
        ctx.set_current_catalog("paimon")
        ctx.set_current_database("default")
        return ctx

    @classmethod
    def setUpClass(cls):
        """Create the test table once before all tests in this class."""
        from pypaimon import Schema, CatalogFactory
        from pypaimon.schema.data_types import DataField, AtomicType

        catalog = CatalogFactory.create({"warehouse": WAREHOUSE})
        try:
            catalog.create_database("default", ignore_if_exists=True)
        except Exception:
            pass

        identifier = "default.sql_test_table"

        # Drop existing table to ensure clean state
        catalog.drop_table(identifier, ignore_if_not_exists=True)

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
        catalog.create_table(identifier, schema, ignore_if_exists=False)

        table = catalog.get_table(identifier)
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

    @classmethod
    def tearDownClass(cls):
        """Clean up the test table after all tests."""
        catalog = CatalogFactory.create({"warehouse": WAREHOUSE})
        catalog.drop_table("default.sql_test_table", ignore_if_not_exists=True)

    def test_sql_returns_table(self):
        ctx = self._create_sql_context()
        table = ctx.sql("SELECT id, name FROM sql_test_table ORDER BY id")
        self.assertIsInstance(table, pa.Table)
        self.assertEqual(table.num_rows, 3)
        self.assertEqual(table.column("id").to_pylist(), [1, 2, 3])
        self.assertEqual(table.column("name").to_pylist(), ["alice", "bob", "carol"])

    def test_sql_to_pandas(self):
        ctx = self._create_sql_context()
        table = ctx.sql("SELECT id, name FROM sql_test_table ORDER BY id")
        df = table.to_pandas()
        self.assertEqual(len(df), 3)
        self.assertListEqual(list(df.columns), ["id", "name"])

    def test_sql_with_filter(self):
        ctx = self._create_sql_context()
        table = ctx.sql("SELECT id, name FROM sql_test_table WHERE id > 1 ORDER BY id")
        self.assertEqual(table.num_rows, 2)
        self.assertEqual(table.column("id").to_pylist(), [2, 3])

    def test_sql_with_empty_result(self):
        ctx = self._create_sql_context()
        table = ctx.sql("SELECT id, name FROM sql_test_table WHERE id > 4 ORDER BY id")
        self.assertIsInstance(table, pa.Table)
        self.assertEqual(table.num_rows, 0)
        self.assertEqual(table.schema.names, ["id", "name"])

    def test_sql_with_aggregation(self):
        ctx = self._create_sql_context()
        table = ctx.sql("SELECT count(*) AS cnt FROM sql_test_table")
        self.assertEqual(table.column("cnt").to_pylist(), [3])

    def test_sql_two_part_reference(self):
        ctx = self._create_sql_context()
        table = ctx.sql("SELECT count(*) AS cnt FROM default.sql_test_table")
        self.assertEqual(table.column("cnt").to_pylist(), [3])

    def test_import_error_without_pypaimon_rust(self):
        """register_catalog should raise ImportError when pypaimon-rust is missing."""
        import unittest.mock as mock
        import builtins
        original_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "pypaimon_rust.datafusion" or name == "pypaimon_rust":
                raise ImportError("No module named 'pypaimon_rust'")
            return original_import(name, *args, **kwargs)

        from pypaimon.sql.sql_context import SQLContext
        ctx = SQLContext()
        with mock.patch("builtins.__import__", side_effect=mock_import):
            with self.assertRaises(ImportError) as cm:
                ctx.register_catalog("paimon", {"warehouse": WAREHOUSE})
            self.assertIn("pypaimon-rust", str(cm.exception))


if __name__ == "__main__":
    unittest.main()
