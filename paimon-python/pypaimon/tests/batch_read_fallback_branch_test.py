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

"""Batch-read coverage for the ``scan.fallback-branch`` table option.

Mirrors Java's ``FallbackReadFileStoreTable``: partitions that exist in
the main branch are read from main; partitions that exist only in the
fallback branch are read from the fallback branch.
"""

import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.schema.schema_change import SchemaChange


class BatchReadFallbackBranchTest(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="unittest_fallback_branch_")
        warehouse = os.path.join(self.temp_dir, "warehouse")
        os.makedirs(warehouse, exist_ok=True)
        self.catalog = CatalogFactory.create({"warehouse": warehouse})
        self.catalog.create_database("db", True)
        self.pa_schema = pa.schema([
            ("k", pa.int64()),
            ("p", pa.string()),
        ])
        self.catalog.create_table(
            "db.t",
            Schema.from_pyarrow_schema(self.pa_schema, partition_keys=["p"]),
            False,
        )

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _write(self, table_name, rows):
        wb = self.catalog.get_table(table_name).new_batch_write_builder()
        writer = wb.new_write()
        try:
            writer.write_arrow(pa.table(rows, schema=self.pa_schema))
            wb.new_commit().commit(writer.prepare_commit())
        finally:
            writer.close()

    def _read(self, table_name):
        rb = self.catalog.get_table(table_name).new_read_builder()
        rows = rb.new_read().to_arrow(rb.new_scan().plan().splits()).to_pylist()
        return sorted(rows, key=lambda row: (row["p"], row["k"]))

    def _set_fallback(self, branch):
        self.catalog.alter_table(
            "db.t", [SchemaChange.set_option("scan.fallback-branch", branch)])

    def test_batch_read_includes_fallback_only_partitions(self):
        """Partition that is missing from main is read from the fallback branch."""
        self.catalog.create_branch("db.t", "b")
        self._write("db.t", {"k": [1], "p": ["a"]})
        self._write("db.t$branch_b", {"k": [2], "p": ["b"]})
        self._set_fallback("b")

        self.assertEqual(
            [{"k": 1, "p": "a"}, {"k": 2, "p": "b"}],
            self._read("db.t"),
        )

    def test_main_partition_shadows_fallback(self):
        """A partition present in main is read from main, not the fallback."""
        self.catalog.create_branch("db.t", "b")
        self._write("db.t", {"k": [1], "p": ["a"]})
        # Same partition "a" exists on the fallback branch with a different row.
        self._write("db.t$branch_b", {"k": [99], "p": ["a"]})
        self._write("db.t$branch_b", {"k": [2], "p": ["b"]})
        self._set_fallback("b")

        self.assertEqual(
            [{"k": 1, "p": "a"}, {"k": 2, "p": "b"}],
            self._read("db.t"),
        )

    def test_no_fallback_option_reads_main_only(self):
        """Without the option the read stays on the main branch."""
        self.catalog.create_branch("db.t", "b")
        self._write("db.t", {"k": [1], "p": ["a"]})
        self._write("db.t$branch_b", {"k": [2], "p": ["b"]})

        self.assertEqual([{"k": 1, "p": "a"}], self._read("db.t"))


if __name__ == "__main__":
    unittest.main()
