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

"""Tests for the in-memory SystemReadBuilder / Scan / Read pipeline.

These exercise the duck-typed surface so callers can reach for
``rb.with_filter / with_projection / with_limit / new_scan / new_read``
exactly as they would on a regular data table.
"""

import types
import unittest

import pyarrow as pa

from pypaimon.common.identifier import Identifier
from pypaimon.schema.data_types import AtomicType, DataField, RowType
from pypaimon.table.system.system_table import SystemReadBuilder, SystemTable
from pypaimon.table.system.system_table_scan import SystemSplit, SystemTableScan
from pypaimon.table.system.system_table_read import SystemTableRead


_DUMMY_ROW_TYPE = RowType(False, [
    DataField(0, "c1", AtomicType("STRING", nullable=False)),
    DataField(1, "c2", AtomicType("BIGINT", nullable=False)),
])


class _DummySystemTable(SystemTable):
    """Returns a 3-row table for pipeline tests."""

    def system_table_name(self) -> str:
        return "dummy"

    def row_type(self) -> RowType:
        return _DUMMY_ROW_TYPE

    def _build_arrow_table(self):
        return pa.table({"c1": ["a", "b", "c"], "c2": [1, 2, 3]})


def _fake_base():
    return types.SimpleNamespace(
        identifier=Identifier.create("db", "t"),
        file_io=object(),
        table_path="/tmp/db/t",
    )


class SystemReadPipelineTest(unittest.TestCase):

    def setUp(self):
        self.table = _DummySystemTable(_fake_base())

    def _splits_and_read(self, rb):
        return rb.new_read(), rb.new_scan().plan().splits()

    def test_new_read_builder_returns_system_read_builder(self):
        self.assertIsInstance(self.table.new_read_builder(), SystemReadBuilder)

    def test_scan_produces_single_in_memory_split(self):
        rb = self.table.new_read_builder()
        splits = rb.new_scan().plan().splits()
        self.assertEqual(1, len(splits))
        self.assertIsInstance(splits[0], SystemSplit)
        self.assertEqual(3, splits[0].row_count)
        self.assertEqual([], splits[0].files)
        self.assertIsNone(splits[0].partition)
        self.assertEqual(-1, splits[0].bucket)

    def test_default_read_returns_full_arrow_table(self):
        rb = self.table.new_read_builder()
        read, splits = self._splits_and_read(rb)
        result = read.to_arrow(splits)
        self.assertEqual(["c1", "c2"], result.schema.names)
        self.assertEqual(3, result.num_rows)
        self.assertEqual(["a", "b", "c"], result.column("c1").to_pylist())
        self.assertEqual([1, 2, 3], result.column("c2").to_pylist())

    def test_with_projection_drops_columns(self):
        rb = self.table.new_read_builder().with_projection(["c1"])
        read, splits = self._splits_and_read(rb)
        result = read.to_arrow(splits)
        self.assertEqual(["c1"], result.schema.names)
        self.assertEqual(3, result.num_rows)

    def test_projection_silently_skips_unknown_columns(self):
        # Mirrors ReadBuilder.with_projection contract: unknown names
        # are dropped rather than failing eagerly.
        rb = self.table.new_read_builder().with_projection(["c2", "no_such"])
        read, splits = self._splits_and_read(rb)
        result = read.to_arrow(splits)
        self.assertEqual(["c2"], result.schema.names)

    def test_with_limit_truncates_rows(self):
        rb = self.table.new_read_builder().with_limit(1)
        read, splits = self._splits_and_read(rb)
        result = read.to_arrow(splits)
        self.assertEqual(1, result.num_rows)
        self.assertEqual("a", result.column("c1")[0].as_py())

    def test_filter_is_not_supported_yet(self):
        rb = self.table.new_read_builder().with_filter(object())
        read, splits = self._splits_and_read(rb)
        with self.assertRaises(NotImplementedError):
            read.to_arrow(splits)

    def test_to_pandas_returns_dataframe(self):
        rb = self.table.new_read_builder()
        read, splits = self._splits_and_read(rb)
        df = read.to_pandas(splits)
        self.assertEqual(3, len(df))
        self.assertEqual(["c1", "c2"], list(df.columns))

    def test_to_record_batch_iterator_yields_batches(self):
        rb = self.table.new_read_builder()
        read, splits = self._splits_and_read(rb)
        batches = list(read.to_record_batch_iterator(splits))
        self.assertEqual(3, sum(b.num_rows for b in batches))

    def test_to_iterator_yields_one_dict_per_row(self):
        rb = self.table.new_read_builder()
        read, splits = self._splits_and_read(rb)
        rows = list(read.to_iterator(splits))
        self.assertEqual(3, len(rows))
        self.assertEqual({"c1", "c2"}, set(rows[0].keys()))
        self.assertEqual("a", rows[0]["c1"])

    def test_read_type_reflects_current_projection(self):
        rb = self.table.new_read_builder()
        self.assertEqual(["c1", "c2"], [f.name for f in rb.read_type()])
        rb.with_projection(["c2"])
        self.assertEqual(["c2"], [f.name for f in rb.read_type()])

    def test_new_scan_returns_system_table_scan(self):
        rb = self.table.new_read_builder()
        self.assertIsInstance(rb.new_scan(), SystemTableScan)

    def test_new_read_returns_system_table_read(self):
        rb = self.table.new_read_builder()
        self.assertIsInstance(rb.new_read(), SystemTableRead)


if __name__ == "__main__":
    unittest.main()
