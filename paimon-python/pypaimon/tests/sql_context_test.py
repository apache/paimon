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

import os
import unittest

import pyarrow as pa

from pypaimon_rust.datafusion import SQLContext


WAREHOUSE = os.environ.get("PAIMON_TEST_WAREHOUSE", "/tmp/paimon-warehouse")


class SQLContextTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Create the test table once before all tests in this class."""
        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": WAREHOUSE})
        ctx.sql("DROP TABLE IF EXISTS sql_test_table")
        ctx.sql("CREATE TABLE sql_test_table (id INT, name STRING)")
        ctx.sql("INSERT INTO sql_test_table VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")

    @classmethod
    def tearDownClass(cls):
        """Clean up the test table after all tests."""
        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": WAREHOUSE})
        ctx.sql("DROP TABLE IF EXISTS sql_test_table")

    def _create_sql_context(self):
        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": WAREHOUSE})
        return ctx

    def test_sql_returns_table(self):
        ctx = self._create_sql_context()
        batches = ctx.sql("SELECT id, name FROM sql_test_table ORDER BY id")
        table = pa.Table.from_batches(batches)
        self.assertIsInstance(table, pa.Table)
        self.assertEqual(table.num_rows, 3)
        self.assertEqual(table.column("id").to_pylist(), [1, 2, 3])
        self.assertEqual(table.column("name").to_pylist(), ["alice", "bob", "carol"])

    def test_sql_to_pandas(self):
        ctx = self._create_sql_context()
        batches = ctx.sql("SELECT id, name FROM sql_test_table ORDER BY id")
        table = pa.Table.from_batches(batches)
        df = table.to_pandas()
        self.assertEqual(len(df), 3)
        self.assertListEqual(list(df.columns), ["id", "name"])

    def test_sql_with_filter(self):
        ctx = self._create_sql_context()
        batches = ctx.sql("SELECT id, name FROM sql_test_table WHERE id > 1 ORDER BY id")
        table = pa.Table.from_batches(batches)
        self.assertEqual(table.num_rows, 2)
        self.assertEqual(table.column("id").to_pylist(), [2, 3])

    def test_sql_with_empty_result(self):
        ctx = self._create_sql_context()
        batches = ctx.sql("SELECT id, name FROM sql_test_table WHERE id > 4 ORDER BY id")
        self.assertEqual(len(batches), 0)

    def test_sql_with_aggregation(self):
        ctx = self._create_sql_context()
        batches = ctx.sql("SELECT count(*) AS cnt FROM sql_test_table")
        table = pa.Table.from_batches(batches)
        self.assertEqual(table.column("cnt").to_pylist(), [3])

    def test_sql_two_part_reference(self):
        ctx = self._create_sql_context()
        batches = ctx.sql("SELECT count(*) AS cnt FROM default.sql_test_table")
        table = pa.Table.from_batches(batches)
        self.assertEqual(table.column("cnt").to_pylist(), [3])


if __name__ == "__main__":
    unittest.main()
